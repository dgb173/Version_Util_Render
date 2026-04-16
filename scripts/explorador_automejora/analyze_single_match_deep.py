#!/usr/bin/env python3
"""
Analisis profundo de un partido puntual desde precacheo/snapshot:
- Patrones H2H Col3 (prioritario)
- H2H estadio/general (movimiento + resultado)
- Comparativas indirectas
- Prev home / prev away (resultado + handicap + over/under)

Entrega dos pronosticos separados:
1) Over/Under
2) Ganador de handicap (favorito vs no favorito)
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


STAT_BLOCKS: Tuple[str, ...] = ("col3", "prev_home", "prev_away", "ind_left", "ind_right")
LEARNING_VERSION = 1
LEARN_DEFAULT_REL_PATH = "data/explorador_automejora_state.json"
CONVERSATION_PROFILE = "v4.4-volumen-mercado"

METRIC_WEIGHTS: Dict[str, float] = {
    "target_total": 2.00,
    "target_diff": 1.50,
    "danger_total": 1.75,
    "danger_diff": 1.25,
    "attacks_total": 0.90,
    "attacks_diff": 0.60,
    "shots_total": 0.75,
    "shots_diff": 0.50,
}

METRIC_SCALES: Dict[str, float] = {
    "target_total": 3.0,
    "target_diff": 2.5,
    "danger_total": 20.0,
    "danger_diff": 16.0,
    "attacks_total": 42.0,
    "attacks_diff": 34.0,
    "shots_total": 8.0,
    "shots_diff": 6.0,
}

DIAG_THRESHOLDS: Dict[str, float] = {
    "target_total": 3.0,
    "danger_total": 20.0,
    "attacks_total": 40.0,
}

AH05_CONTEXT_FIELDS: Tuple[str, ...] = (
    "fav_side",
    "movement_pair",
    "ou_band",
    "draw_risk_band",
    "col3_wdl",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analisis profundo de un partido desde patrones del explorador/precacheo."
    )
    parser.add_argument(
        "--project-root",
        default=".",
        help="Ruta del proyecto (contiene src/).",
    )
    parser.add_argument(
        "--date",
        default="today",
        help="Fecha objetivo YYYY-MM-DD o 'today' (zona Europe/Madrid).",
    )
    parser.add_argument(
        "--team-query",
        default="",
        help="Texto de equipo para localizar partido (ej: sydney).",
    )
    parser.add_argument(
        "--match-id",
        default="",
        help="ID exacto del partido (prioridad sobre --team-query).",
    )
    parser.add_argument(
        "--min-support",
        type=int,
        default=12,
        help="Minimo de historicos similares para considerar confianza alta.",
    )
    parser.add_argument(
        "--history-limit",
        type=int,
        default=0,
        help="Limite de historicos a cargar (0 = sin limite).",
    )
    parser.add_argument(
        "--min-similarity",
        type=float,
        default=32.0,
        help="Umbral minimo del score compuesto para aceptar un historico.",
    )
    parser.add_argument(
        "--max-similars",
        type=int,
        default=450,
        help="Maximo de historicos similares considerados para estadisticas.",
    )
    parser.add_argument(
        "--top-similars",
        type=int,
        default=15,
        help="Cantidad de similares a mostrar en el reporte.",
    )
    parser.add_argument(
        "--ah-max-gap",
        type=float,
        default=0.25,
        help="Gap maximo permitido entre |AH actual| y |AH historico|.",
    )
    parser.add_argument(
        "--ou-max-gap",
        type=float,
        default=0.75,
        help="Gap maximo permitido entre OU actual e historico.",
    )
    parser.add_argument(
        "--min-handicap-score",
        type=float,
        default=8.0,
        help="Score minimo del bloque handicap para aceptar historico.",
    )
    parser.add_argument(
        "--min-stats-blocks",
        type=int,
        default=2,
        help="Bloques minimos de stats con buena cercania.",
    )
    parser.add_argument(
        "--disable-learning",
        action="store_true",
        help="Desactiva calibracion y persistencia de autoaprendizaje.",
    )
    parser.add_argument(
        "--learning-state",
        default="",
        help="Ruta del estado JSON de autoaprendizaje (default: <project-root>/data/explorador_automejora_state.json).",
    )
    parser.add_argument(
        "--learning-strength",
        type=float,
        default=0.35,
        help="Intensidad base de calibracion por autoaprendizaje [0-1].",
    )
    parser.add_argument(
        "--learning-min-non-push",
        type=float,
        default=24.0,
        help="Minimo de muestra ponderada sin push para tomar fuerza completa de aprendizaje.",
    )
    parser.add_argument(
        "--conversation-strength",
        type=float,
        default=0.65,
        help="Fuerza de las reglas de conversacion (volumen>eficiencia) [0-1].",
    )
    parser.add_argument(
        "--conversation-learning-rate",
        type=float,
        default=0.35,
        help="Velocidad de ajuste de la calibracion OU por feedback [0-1].",
    )
    parser.add_argument(
        "--actual-score",
        default="",
        help="Resultado real opcional del partido objetivo (ej: 1:1) para retroalimentar aprendizaje.",
    )
    parser.add_argument(
        "--output-md",
        default="report_single_match_deep.md",
        help="Ruta markdown de salida.",
    )
    parser.add_argument(
        "--output-json",
        default="report_single_match_deep.json",
        help="Ruta json de salida.",
    )
    return parser.parse_args()


def _load_base_module() -> Any:
    folder = Path(__file__).resolve().parent
    script_path: Optional[Path] = None
    for name in ("generate_today_fav_over_report.py", "generate_today_underdog_over_report.py"):
        candidate = folder / name
        if candidate.exists():
            script_path = candidate
            break
    if script_path is None:
        raise RuntimeError(
            f"No se encontro script base en {folder} (fav/underdog)."
        )
    spec = importlib.util.spec_from_file_location(f"single_deep_base_{script_path.stem}", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"No se pudo cargar modulo base: {script_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _safe_handicap_parse(raw: Any) -> Optional[float]:
    if raw is None:
        return None
    text = str(raw).strip().replace(" ", "").replace(",", ".")
    if not text or text in {"-", "?", "N/A"}:
        return None
    try:
        return float(text)
    except Exception:
        pass
    if "/" in text:
        parts = text.split("/")
        if len(parts) != 2:
            return None
        try:
            p1 = float(parts[0])
            p2 = float(parts[1])
            if p1 < 0 and p2 > 0 and not parts[1].startswith("-"):
                p2 = -abs(p2)
            return (p1 + p2) / 2.0
        except Exception:
            return None
    return None


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", ".")
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _is_ah05_bucket(bucket_abs: Optional[float]) -> bool:
    if bucket_abs is None:
        return False
    return abs(float(bucket_abs) - 0.5) <= 1e-9


def _ou_band(ou_line: Optional[float]) -> str:
    if ou_line is None:
        return "UNK"
    if ou_line <= 2.25:
        return "LOW"
    if ou_line <= 2.75:
        return "MID"
    return "HIGH"


def _draw_risk_band(draw_bias: float) -> str:
    if draw_bias >= 0.66:
        return "HIGH"
    if draw_bias >= 0.33:
        return "MED"
    return "LOW"


def _new_learning_node() -> Dict[str, float]:
    return {
        "weighted_samples": 0.0,
        "weighted_non_push": 0.0,
        "weighted_fav_cover": 0.0,
        "weighted_und_cover": 0.0,
        "weighted_push": 0.0,
        "weighted_draw": 0.0,
    }


def _default_ou_calibration() -> Dict[str, Any]:
    return {
        "global": {
            "volume_over_boost": 1.0,
            "low_volume_under_boost": 1.0,
            "samples": 0,
            "hits": 0,
        },
        "contexts": {},
    }


def _merge_ou_calibration(raw: Any) -> Dict[str, Any]:
    base = _default_ou_calibration()
    if not isinstance(raw, dict):
        return base

    def _merge_node(node_raw: Any) -> Dict[str, Any]:
        out = {
            "volume_over_boost": 1.0,
            "low_volume_under_boost": 1.0,
            "samples": 0,
            "hits": 0,
        }
        if not isinstance(node_raw, dict):
            return out

        over_boost = _safe_float(node_raw.get("volume_over_boost"))
        under_boost = _safe_float(node_raw.get("low_volume_under_boost"))
        samples = int(_safe_float(node_raw.get("samples")) or 0)
        hits = int(_safe_float(node_raw.get("hits")) or 0)

        if over_boost is not None:
            out["volume_over_boost"] = _clamp(float(over_boost), 0.60, 1.80)
        if under_boost is not None:
            out["low_volume_under_boost"] = _clamp(float(under_boost), 0.60, 1.80)
        out["samples"] = max(0, samples)
        out["hits"] = max(0, hits)
        return out

    base["global"] = _merge_node(raw.get("global"))
    contexts_raw = raw.get("contexts")
    if isinstance(contexts_raw, dict):
        for key, node in contexts_raw.items():
            base["contexts"][str(key)] = _merge_node(node)
    return base


def _default_learning_state() -> Dict[str, Any]:
    return {
        "version": LEARNING_VERSION,
        "updated_at": "",
        "global_ah05": _new_learning_node(),
        "contexts": {},
        "ingested_targets": {},
        "feedback_log": {},
        "ou_calibration": _default_ou_calibration(),
        "ou_feedback_log": {},
    }


def _merge_learning_node(node_raw: Any) -> Dict[str, float]:
    out = _new_learning_node()
    if not isinstance(node_raw, dict):
        return out
    for key in out.keys():
        value = _safe_float(node_raw.get(key))
        if value is not None and value >= 0.0:
            out[key] = float(value)
    return out


def _load_learning_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return _default_learning_state()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return _default_learning_state()
    if not isinstance(raw, dict):
        return _default_learning_state()

    state = _default_learning_state()
    state["version"] = int(_safe_float(raw.get("version")) or LEARNING_VERSION)
    state["updated_at"] = str(raw.get("updated_at") or "")
    state["global_ah05"] = _merge_learning_node(raw.get("global_ah05"))

    contexts_raw = raw.get("contexts")
    if isinstance(contexts_raw, dict):
        for key, node in contexts_raw.items():
            state["contexts"][str(key)] = _merge_learning_node(node)

    ingested = raw.get("ingested_targets")
    if isinstance(ingested, dict):
        state["ingested_targets"] = {str(k): v for k, v in ingested.items()}

    feedback = raw.get("feedback_log")
    if isinstance(feedback, dict):
        state["feedback_log"] = {str(k): v for k, v in feedback.items()}
    state["ou_calibration"] = _merge_ou_calibration(raw.get("ou_calibration"))
    ou_feedback = raw.get("ou_feedback_log")
    if isinstance(ou_feedback, dict):
        state["ou_feedback_log"] = {str(k): v for k, v in ou_feedback.items()}
    return state


def _save_learning_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    state["version"] = LEARNING_VERSION
    state["updated_at"] = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _context_key(ctx: Dict[str, Any]) -> str:
    if not bool(ctx.get("is_ah05")):
        return "NON_AH05"
    chunks = [f"{name}={ctx.get(name, 'UNK')}" for name in AH05_CONTEXT_FIELDS]
    return "|".join(chunks)


def _infer_context(match: Any, prev_features: Dict[str, Any]) -> Dict[str, Any]:
    features = match.features if isinstance(getattr(match, "features", None), dict) else {}
    draw_votes: List[float] = []
    for key in ("col3_wdl", "stadium_wdl", "general_wdl", "ind_left_wdl", "ind_right_wdl"):
        val = features.get(key)
        if val is None:
            continue
        draw_votes.append(1.0 if val == "DRAW" else 0.0)
    for key in ("prev_home_wdl", "prev_away_wdl"):
        val = prev_features.get(key)
        if val is None:
            continue
        draw_votes.append(1.0 if val == "DRAW" else 0.0)

    draw_bias = (sum(draw_votes) / len(draw_votes)) if draw_votes else 0.0
    ou_band = _ou_band(match.ou)
    movement_pair = f"{features.get('stadium_mov_dir') or 'UNK'}|{features.get('general_mov_dir') or 'UNK'}"
    ah_bucket_abs = abs(float(match.ah_bucket_abs)) if match.ah_bucket_abs is not None else None
    is_ah05 = _is_ah05_bucket(ah_bucket_abs)

    return {
        "fav_side": match.fav_side,
        "ou_band": ou_band,
        "movement_pair": movement_pair,
        "col3_wdl": features.get("col3_wdl") or "UNK",
        "draw_bias": draw_bias,
        "draw_risk_band": _draw_risk_band(draw_bias),
        "draw_signals": len(draw_votes),
        "ah_bucket_abs": ah_bucket_abs,
        "is_ah05": is_ah05,
    }


def _context_similarity_multiplier(target_ctx: Dict[str, Any], hist_ctx: Dict[str, Any]) -> float:
    if not bool(target_ctx.get("is_ah05")):
        return 1.0
    score = 1.0
    if target_ctx.get("fav_side") == hist_ctx.get("fav_side"):
        score += 0.03
    if target_ctx.get("movement_pair") == hist_ctx.get("movement_pair"):
        score += 0.08
    if target_ctx.get("ou_band") == hist_ctx.get("ou_band"):
        score += 0.05
    if target_ctx.get("draw_risk_band") == hist_ctx.get("draw_risk_band"):
        score += 0.06
    if target_ctx.get("col3_wdl") != "UNK" and target_ctx.get("col3_wdl") == hist_ctx.get("col3_wdl"):
        score += 0.03
    return _clamp(score, 0.84, 1.24)


def _update_learning_node(node: Dict[str, float], fav_cover: Optional[bool], is_draw: bool, weight: float) -> None:
    if weight <= 0:
        return
    node["weighted_samples"] += weight
    if is_draw:
        node["weighted_draw"] += weight
    if fav_cover is None:
        node["weighted_push"] += weight
        return
    node["weighted_non_push"] += weight
    if fav_cover:
        node["weighted_fav_cover"] += weight
    else:
        node["weighted_und_cover"] += weight


def _learning_stats(node: Dict[str, float]) -> Dict[str, float]:
    non_push = max(0.0, float(node.get("weighted_non_push", 0.0)))
    fav_cover = max(0.0, float(node.get("weighted_fav_cover", 0.0)))
    und_cover = max(0.0, float(node.get("weighted_und_cover", 0.0)))
    weighted_samples = max(0.0, float(node.get("weighted_samples", 0.0)))
    weighted_draw = max(0.0, float(node.get("weighted_draw", 0.0)))

    fav_post = (fav_cover + 2.0) / (non_push + 4.0) if non_push > 0 else 0.5
    und_post = (und_cover + 2.0) / (non_push + 4.0) if non_push > 0 else 0.5
    draw_post = (weighted_draw + 1.0) / (weighted_samples + 2.0) if weighted_samples > 0 else 0.5
    return {
        "weighted_samples": weighted_samples,
        "weighted_non_push": non_push,
        "fav_cover_rate": fav_post,
        "und_cover_rate": und_post,
        "draw_rate": draw_post,
    }


def _blend_learning(
    state: Dict[str, Any],
    context_key: str,
    min_non_push: float,
) -> Dict[str, float]:
    context_node = _merge_learning_node(state.get("contexts", {}).get(context_key))
    global_node = _merge_learning_node(state.get("global_ah05"))

    c = _learning_stats(context_node)
    g = _learning_stats(global_node)

    c_rel = _clamp(c["weighted_non_push"] / max(1.0, float(min_non_push)), 0.0, 1.0)
    g_rel = _clamp(g["weighted_non_push"] / max(1.0, float(min_non_push) * 2.0), 0.0, 1.0)
    denom = c_rel + g_rel

    if denom <= 1e-9:
        fav = 0.5
        und = 0.5
        draw = 0.5
    else:
        fav = (c["fav_cover_rate"] * c_rel + g["fav_cover_rate"] * g_rel) / denom
        und = (c["und_cover_rate"] * c_rel + g["und_cover_rate"] * g_rel) / denom
        draw = (c["draw_rate"] * c_rel + g["draw_rate"] * g_rel) / denom

    reliability = _clamp(0.75 * c_rel + 0.25 * g_rel, 0.0, 1.0)
    return {
        "fav_cover_rate": fav,
        "und_cover_rate": und,
        "draw_rate": draw,
        "reliability": reliability,
        "context_non_push": c["weighted_non_push"],
        "global_non_push": g["weighted_non_push"],
    }


def _learning_state_path(project_root: Path, override: str) -> Path:
    if override:
        return Path(override).expanduser().resolve()
    return (project_root / LEARN_DEFAULT_REL_PATH).resolve()


def _ingest_target_similars(
    state: Dict[str, Any],
    target: Any,
    target_day: date,
    target_ctx: Dict[str, Any],
    sampled: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    info = {"applied": False, "run_key": "", "added_weight": 0.0, "rows": 0}
    if not bool(target_ctx.get("is_ah05")):
        return info
    context_key = _context_key(target_ctx)
    run_key = f"{target_day.isoformat()}::{target.match_id}::{context_key}"
    info["run_key"] = run_key

    ingested_targets = state.setdefault("ingested_targets", {})
    if run_key in ingested_targets:
        return info

    contexts = state.setdefault("contexts", {})
    context_node = contexts.setdefault(context_key, _new_learning_node())
    global_node = state.setdefault("global_ah05", _new_learning_node())

    added_weight = 0.0
    rows = 0
    for row in sampled:
        hist = row["hist"]
        hist_ctx = row.get("hist_context")
        if not isinstance(hist_ctx, dict):
            continue
        if not bool(hist_ctx.get("is_ah05")):
            continue
        sim_total = float(row.get("sim_total", 0.0))
        if sim_total <= 0:
            continue
        # Normalizar peso para que represente "calidad relativa" y no crezca descontrolado.
        w = _clamp(sim_total / 40.0, 0.35, 2.25)
        is_draw = (
            hist.final_home_goals is not None
            and hist.final_away_goals is not None
            and hist.final_home_goals == hist.final_away_goals
        )
        fav_cover = _favorite_ah_cover(hist)
        _update_learning_node(context_node, fav_cover, bool(is_draw), w)
        _update_learning_node(global_node, fav_cover, bool(is_draw), w * 0.60)
        rows += 1
        added_weight += w

    ingested_targets[run_key] = {
        "target_match_id": str(target.match_id),
        "context_key": context_key,
        "rows": rows,
        "added_weight": round(float(added_weight), 4),
        "at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }
    info["applied"] = rows > 0
    info["added_weight"] = added_weight
    info["rows"] = rows
    return info


def _apply_actual_feedback(
    state: Dict[str, Any],
    target: Any,
    target_ctx: Dict[str, Any],
    score_raw: str,
    base: Any,
) -> Dict[str, Any]:
    out = {"applied": False, "reason": "", "score": ""}
    if not bool(target_ctx.get("is_ah05")):
        out["reason"] = "not_ah05"
        return out
    parsed = base._parse_score(score_raw)
    if not parsed:
        out["reason"] = "invalid_score"
        return out
    hg, ag = parsed
    out["score"] = f"{hg}:{ag}"

    feedback_log = state.setdefault("feedback_log", {})
    match_key = str(target.match_id)
    if match_key in feedback_log:
        out["reason"] = "already_logged"
        return out

    if target.ah is None:
        out["reason"] = "missing_ah"
        return out

    line = abs(float(target.ah))
    fav_margin = (hg - ag) if target.fav_side == "HOME" else (ag - hg)
    diff = fav_margin - line
    if diff > 0.05:
        fav_cover = True
    elif diff < -0.05:
        fav_cover = False
    else:
        fav_cover = None

    is_draw = hg == ag
    context_key = _context_key(target_ctx)
    context_node = state.setdefault("contexts", {}).setdefault(context_key, _new_learning_node())
    global_node = state.setdefault("global_ah05", _new_learning_node())

    # Peso alto: feedback real del partido objetivo.
    _update_learning_node(context_node, fav_cover, is_draw, 14.0)
    _update_learning_node(global_node, fav_cover, is_draw, 7.0)

    feedback_log[match_key] = {
        "context_key": context_key,
        "score": out["score"],
        "fav_cover": fav_cover,
        "is_draw": is_draw,
        "recorded_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }
    out["applied"] = True
    return out

def _score_to_wdl(score_raw: Any, base: Any) -> Optional[str]:
    parsed = base._parse_score(score_raw)
    if not parsed:
        return None
    hg, ag = parsed
    if hg > ag:
        return "HOME_WIN"
    if ag > hg:
        return "AWAY_WIN"
    return "DRAW"


def _score_over(score_raw: Any, ou_line: Optional[float], base: Any) -> Optional[bool]:
    parsed = base._parse_score(score_raw)
    if not parsed or ou_line is None:
        return None
    hg, ag = parsed
    return (hg + ag) > ou_line


def _normalize_label(label: Any) -> str:
    text = str(label or "").strip().casefold()
    replacements = {
        "á": "a",
        "é": "e",
        "í": "i",
        "ó": "o",
        "ú": "u",
        "ü": "u",
    }
    for k, v in replacements.items():
        text = text.replace(k, v)
    return text


def _stats_from_node(node: Any) -> Dict[str, Optional[float]]:
    out: Dict[str, Optional[float]] = {
        "shots_total": None,
        "shots_diff": None,
        "target_total": None,
        "target_diff": None,
        "danger_total": None,
        "danger_diff": None,
        "attacks_total": None,
        "attacks_diff": None,
    }
    if not isinstance(node, dict):
        return out
    rows = node.get("stats_rows")
    if not isinstance(rows, list):
        return out

    for row in rows:
        if not isinstance(row, dict):
            continue
        label = _normalize_label(row.get("label"))
        home = _safe_float(row.get("home"))
        away = _safe_float(row.get("away"))
        if home is None or away is None:
            continue
        if "tiros a puerta" in label:
            out["target_total"] = home + away
            out["target_diff"] = home - away
            continue
        if label == "tiros":
            out["shots_total"] = home + away
            out["shots_diff"] = home - away
            continue
        if "ataques peligrosos" in label:
            out["danger_total"] = home + away
            out["danger_diff"] = home - away
            continue
        if label == "ataques":
            out["attacks_total"] = home + away
            out["attacks_diff"] = home - away
            continue
    return out


def _extract_stats_blocks(match: Any) -> Dict[str, Dict[str, Optional[float]]]:
    raw = match.raw if isinstance(getattr(match, "raw", None), dict) else {}
    comp = raw.get("comparativas_indirectas") if isinstance(raw.get("comparativas_indirectas"), dict) else {}
    blocks = {
        "col3": raw.get("h2h_col3"),
        "prev_home": raw.get("last_home_match"),
        "prev_away": raw.get("last_away_match"),
        "ind_left": comp.get("left") if isinstance(comp, dict) else None,
        "ind_right": comp.get("right") if isinstance(comp, dict) else None,
    }
    return {name: _stats_from_node(node) for name, node in blocks.items()}


def _parse_movement(raw: Any) -> Dict[str, Optional[float | str]]:
    out: Dict[str, Optional[float | str]] = {
        "start": None,
        "end": None,
        "delta": None,
        "dir": None,
    }
    if raw is None:
        return out
    text = str(raw).strip().replace("→", "->").replace(" ", "").replace(",", ".")
    if "->" not in text:
        return out
    parts = text.split("->")
    if len(parts) != 2:
        return out
    start = _safe_float(parts[0])
    end = _safe_float(parts[1])
    if start is None or end is None:
        return out
    delta = end - start
    if delta > 0.01:
        direction = "UP"
    elif delta < -0.01:
        direction = "DOWN"
    else:
        direction = "FLAT"
    out["start"] = start
    out["end"] = end
    out["delta"] = delta
    out["dir"] = direction
    return out


def _extract_market_movement(match: Any) -> Dict[str, Optional[float | str]]:
    raw = match.raw if isinstance(getattr(match, "raw", None), dict) else {}
    market = raw.get("market_analysis_data") if isinstance(raw.get("market_analysis_data"), dict) else {}
    out: Dict[str, Optional[float | str]] = {}
    for scope in ("stadium", "general"):
        node = market.get(scope) if isinstance(market, dict) else None
        parsed = _parse_movement(node.get("movement") if isinstance(node, dict) else None)
        out[f"{scope}_start"] = parsed["start"]
        out[f"{scope}_end"] = parsed["end"]
        out[f"{scope}_delta"] = parsed["delta"]
        out[f"{scope}_dir"] = parsed["dir"]
    return out


def _extract_prev_features(match: Any, base: Any) -> Dict[str, Any]:
    ou_line = match.ou
    raw = match.raw if isinstance(getattr(match, "raw", None), dict) else {}
    current_ah_abs = abs(float(match.ah)) if match.ah is not None else None

    out: Dict[str, Any] = {}
    prev_home = raw.get("last_home_match") if isinstance(raw.get("last_home_match"), dict) else {}
    prev_away = raw.get("last_away_match") if isinstance(raw.get("last_away_match"), dict) else {}

    def _gap_band(gap: Optional[float]) -> Optional[str]:
        if gap is None:
            return None
        if gap <= 0.25:
            return "VERY_CLOSE"
        if gap <= 0.50:
            return "CLOSE"
        if gap <= 1.00:
            return "MID"
        return "FAR"

    def _role_from_line(ah_home: Optional[float], team_is_home: bool) -> Optional[str]:
        if ah_home is None:
            return None
        if team_is_home:
            if ah_home > 0.01:
                return "FAV"
            if ah_home < -0.01:
                return "UND"
            return "EVEN"
        if ah_home < -0.01:
            return "FAV"
        if ah_home > 0.01:
            return "UND"
        return "EVEN"

    def _cover_on_own_line(score: Any, ah_home: Optional[float], team_is_home: bool) -> Tuple[Optional[str], Optional[float]]:
        parsed = base._parse_score(score)
        if not parsed or ah_home is None:
            return None, None
        hg, ag = parsed
        if team_is_home:
            val = (hg - ag) + float(ah_home)
            margin = hg - ag
        else:
            val = (ag - hg) - float(ah_home)
            margin = ag - hg
        if val > 0.05:
            res = "COVER"
        elif val < -0.05:
            res = "NO_COVER"
        else:
            res = "PUSH"
        return res, float(margin)

    def _encode(prefix: str, node: Dict[str, Any], team_is_home: bool) -> None:
        score = node.get("score")
        out[f"{prefix}_wdl"] = _score_to_wdl(score, base)
        out[f"{prefix}_over"] = _score_over(score, ou_line, base)
        ah_prev = _safe_handicap_parse(node.get("handicap_line_raw"))
        out[f"{prefix}_ah_raw"] = ah_prev
        out[f"{prefix}_team_ah"] = (-ah_prev if (ah_prev is not None and not team_is_home) else ah_prev)
        out[f"{prefix}_ah_abs"] = abs(float(ah_prev)) if ah_prev is not None else None
        if ah_prev is None:
            out[f"{prefix}_ah_bucket"] = None
        else:
            out[f"{prefix}_ah_bucket"] = abs(base._normalize_ah_bucket(ah_prev))
        out[f"{prefix}_role"] = _role_from_line(ah_prev, team_is_home=team_is_home)
        cover, margin = _cover_on_own_line(score, ah_prev, team_is_home=team_is_home)
        out[f"{prefix}_cover_own_line"] = cover
        out[f"{prefix}_score_margin"] = margin
        if ah_prev is not None and current_ah_abs is not None:
            gap = abs(abs(float(ah_prev)) - float(current_ah_abs))
        else:
            gap = None
        out[f"{prefix}_line_gap_current"] = gap
        out[f"{prefix}_line_gap_band"] = _gap_band(gap)
        out[f"{prefix}_present"] = bool(score)

    _encode("prev_home", prev_home, team_is_home=True)
    _encode("prev_away", prev_away, team_is_home=False)
    return out


def _extract_stat_sides(node: Dict[str, Any]) -> Dict[str, Optional[float]]:
    out: Dict[str, Optional[float]] = {
        "shots_home": None,
        "shots_away": None,
        "target_home": None,
        "target_away": None,
        "danger_home": None,
        "danger_away": None,
        "attacks_home": None,
        "attacks_away": None,
    }
    if not isinstance(node, dict):
        return out
    rows = node.get("stats_rows")
    if not isinstance(rows, list):
        return out

    for row in rows:
        if not isinstance(row, dict):
            continue
        label = _normalize_label(row.get("label"))
        home = _safe_float(row.get("home"))
        away = _safe_float(row.get("away"))
        if home is None or away is None:
            continue
        if "tiros a puerta" in label:
            out["target_home"] = home
            out["target_away"] = away
            continue
        if label == "tiros":
            out["shots_home"] = home
            out["shots_away"] = away
            continue
        if "ataques peligrosos" in label:
            out["danger_home"] = home
            out["danger_away"] = away
            continue
        if label == "ataques":
            out["attacks_home"] = home
            out["attacks_away"] = away
            continue
    return out


def _build_prev_profile(node: Any, team_is_home: bool, base: Any) -> Dict[str, Any]:
    profile: Dict[str, Any] = {
        "goals_for": None,
        "goals_against": None,
        "shots_for": None,
        "target_for": None,
        "danger_for": None,
        "attacks_for": None,
        "zero_goal_high_volume": False,
        "low_volume": False,
        "high_concede": False,
    }
    if not isinstance(node, dict):
        return profile

    parsed = base._parse_score(node.get("score"))
    if parsed:
        hg, ag = parsed
        if team_is_home:
            profile["goals_for"] = hg
            profile["goals_against"] = ag
        else:
            profile["goals_for"] = ag
            profile["goals_against"] = hg

    side_stats = _extract_stat_sides(node)
    side = "home" if team_is_home else "away"
    profile["shots_for"] = side_stats.get(f"shots_{side}")
    profile["target_for"] = side_stats.get(f"target_{side}")
    profile["danger_for"] = side_stats.get(f"danger_{side}")
    profile["attacks_for"] = side_stats.get(f"attacks_{side}")

    gf = profile.get("goals_for")
    shots = profile.get("shots_for") or 0.0
    target = profile.get("target_for") or 0.0
    danger = profile.get("danger_for") or 0.0
    attacks = profile.get("attacks_for") or 0.0
    ga = profile.get("goals_against")

    zero_goal_high_volume = bool(
        gf == 0
        and (shots >= 12.0 or target >= 4.0 or danger >= 30.0 or attacks >= 90.0)
    )
    low_volume = bool(
        (shots <= 8.0 and target <= 3.0 and attacks <= 70.0)
        and not zero_goal_high_volume
    )
    high_concede = bool(ga is not None and ga >= 2)

    profile["zero_goal_high_volume"] = zero_goal_high_volume
    profile["low_volume"] = low_volume
    profile["high_concede"] = high_concede
    return profile


def _conversation_ou_context_key(target: Any, signals: Dict[str, Any]) -> str:
    return (
        f"{target.fav_side}|{_ou_band(target.ou)}|"
        f"zg={signals.get('zero_goal_high_volume_count', 0)}|"
        f"lv={signals.get('low_volume_count', 0)}|"
        f"hc={signals.get('high_concede_count', 0)}"
    )


def _get_ou_calibration_node(state: Dict[str, Any], context_key: str) -> Dict[str, Any]:
    ou_cal = state.setdefault("ou_calibration", _default_ou_calibration())
    if not isinstance(ou_cal, dict):
        ou_cal = _default_ou_calibration()
        state["ou_calibration"] = ou_cal
    global_node = ou_cal.setdefault("global", _default_ou_calibration()["global"])
    if not isinstance(global_node, dict):
        global_node = _default_ou_calibration()["global"]
        ou_cal["global"] = global_node
    contexts = ou_cal.setdefault("contexts", {})
    if not isinstance(contexts, dict):
        contexts = {}
        ou_cal["contexts"] = contexts
    context_node = contexts.setdefault(context_key, {"volume_over_boost": 1.0, "low_volume_under_boost": 1.0, "samples": 0, "hits": 0})
    if not isinstance(context_node, dict):
        context_node = {"volume_over_boost": 1.0, "low_volume_under_boost": 1.0, "samples": 0, "hits": 0}
        contexts[context_key] = context_node
    return {"global": global_node, "context": context_node}


def _deduped_h2h_ou_votes(target: Any) -> Tuple[int, int, bool]:
    raw = target.raw if isinstance(getattr(target, "raw", None), dict) else {}
    features = target.features if isinstance(getattr(target, "features", None), dict) else {}

    def _node_key(name: str, node: Any) -> str:
        if not isinstance(node, dict):
            return name
        return str(
            node.get("match1_id")
            or node.get("match6_id")
            or node.get("match_id")
            or node.get("date1")
            or node.get("date6")
            or node.get("date")
            or name
        )

    votes: List[Tuple[str, Optional[bool]]] = [
        (_node_key("stadium", raw.get("h2h_stadium")), features.get("stadium_over")),
        (_node_key("general", raw.get("h2h_general")), features.get("general_over")),
        (_node_key("col3", raw.get("h2h_col3")), features.get("col3_over")),
    ]

    seen: set[str] = set()
    duplicated = False
    over_votes = 0
    under_votes = 0
    for key, vote in votes:
        if vote is None:
            continue
        if key in seen:
            duplicated = True
            continue
        seen.add(key)
        if vote is True:
            over_votes += 1
        elif vote is False:
            under_votes += 1
    return over_votes, under_votes, duplicated


def _conversation_ou_adjustment(
    target: Any,
    base: Any,
    learning_state: Dict[str, Any],
    strength: float,
) -> Dict[str, Any]:
    raw = target.raw if isinstance(getattr(target, "raw", None), dict) else {}
    prev_home = _build_prev_profile(raw.get("last_home_match"), team_is_home=True, base=base)
    prev_away = _build_prev_profile(raw.get("last_away_match"), team_is_home=False, base=base)
    prev_features = _extract_prev_features(target, base)
    movement_data = _extract_market_movement(target)

    profiles = [prev_home, prev_away]
    zero_count = sum(1 for p in profiles if p.get("zero_goal_high_volume"))
    low_count = sum(1 for p in profiles if p.get("low_volume"))
    high_concede_count = sum(1 for p in profiles if p.get("high_concede"))

    features = target.features if isinstance(getattr(target, "features", None), dict) else {}
    h2h_over_votes, h2h_under_votes, duplicated_h2h_votes = _deduped_h2h_ou_votes(target)
    movement_align_votes = sum(1 for k in ("stadium_mov_dir", "general_mov_dir") if features.get(k) in {"UP", "DOWN"})
    short_favorite_line = bool(target.ah is not None and abs(float(target.ah)) <= 0.25)
    big_favorite_line = bool(target.ah is not None and abs(float(target.ah)) >= 1.0)
    capped_total_line = bool(target.ou is not None and float(target.ou) <= 3.0)
    inverse_col3_draw = bool(features.get("col3_wdl") == "DRAW" and features.get("col3_over") is False)

    favorite_profile = prev_home if str(getattr(target, "fav_side", "")).upper() == "HOME" else prev_away
    underdog_profile = prev_away if str(getattr(target, "fav_side", "")).upper() == "HOME" else prev_home
    if str(getattr(target, "fav_side", "")).upper() == "HOME":
        favorite_recent_cover = prev_features.get("prev_home_cover_own_line") == "COVER"
        underdog_recent_failed = prev_features.get("prev_away_cover_own_line") == "NO_COVER"
        favorite_gap_current = _safe_float(prev_features.get("prev_home_line_gap_current"))
        underdog_gap_current = _safe_float(prev_features.get("prev_away_line_gap_current"))
        favorite_prev_ah_abs = _safe_float(prev_features.get("prev_home_ah_abs"))
        underdog_prev_ah_abs = _safe_float(prev_features.get("prev_away_ah_abs"))
    else:
        favorite_recent_cover = prev_features.get("prev_away_cover_own_line") == "COVER"
        underdog_recent_failed = prev_features.get("prev_home_cover_own_line") == "NO_COVER"
        favorite_gap_current = _safe_float(prev_features.get("prev_away_line_gap_current"))
        underdog_gap_current = _safe_float(prev_features.get("prev_home_line_gap_current"))
        favorite_prev_ah_abs = _safe_float(prev_features.get("prev_away_ah_abs"))
        underdog_prev_ah_abs = _safe_float(prev_features.get("prev_home_ah_abs"))
    favorite_recent_failed = bool(
        (favorite_profile.get("goals_against") or 0) >= 2
        or (
            favorite_profile.get("goals_for") is not None
            and favorite_profile.get("goals_against") is not None
            and float(favorite_profile.get("goals_for") or 0) < float(favorite_profile.get("goals_against") or 0)
        )
    )
    underdog_clean_recent = bool(
        underdog_profile.get("goals_against") == 0
        and (underdog_profile.get("goals_for") or 0) >= 1
    )
    favorite_recent_control = bool(
        (favorite_profile.get("goals_for") or 0) >= 3
        and (favorite_profile.get("goals_against") or 99) <= 1
        and (
            (favorite_profile.get("target_for") or 0) >= 6
            or (favorite_profile.get("shots_for") or 0) >= 16
            or (favorite_profile.get("danger_for") or 0) >= 50
        )
    )
    repricing_to_favorite = False
    stadium_delta = _safe_float(movement_data.get("stadium_delta"))
    general_delta = _safe_float(movement_data.get("general_delta"))
    if str(getattr(target, "fav_side", "")).upper() == "HOME":
        repricing_to_favorite = bool(
            (stadium_delta is not None and stadium_delta >= 1.25)
            or (general_delta is not None and general_delta >= 1.25)
        )
    else:
        repricing_to_favorite = bool(
            (stadium_delta is not None and stadium_delta <= -1.25)
            or (general_delta is not None and general_delta <= -1.25)
        )
    underdog_same_line_collapse = bool(
        underdog_recent_failed
        and underdog_gap_current is not None
        and underdog_gap_current <= 0.25
        and (underdog_profile.get("goals_against") or 0) - (underdog_profile.get("goals_for") or 0) >= 2
    )
    favorite_harder_line_pressure = bool(
        favorite_prev_ah_abs is not None
        and target.ah is not None
        and favorite_prev_ah_abs - abs(float(target.ah)) >= 1.5
        and (
            (favorite_profile.get("shots_for") or 0) >= 16
            or (favorite_profile.get("target_for") or 0) >= 6
            or (favorite_profile.get("danger_for") or 0) >= 50
        )
    )
    demolition_memory = bool(
        big_favorite_line
        and target.ou is not None
        and float(target.ou) >= 3.25
        and h2h_over_votes >= 2
    )

    base_over_boost = 0.0
    base_under_boost = 0.0

    if zero_count >= 1:
        base_over_boost += 4.0 + 2.0 * max(0, zero_count - 1)
    if high_concede_count >= 1:
        base_over_boost += 2.0 + 1.0 * max(0, high_concede_count - 1)
    if h2h_over_votes >= 2:
        base_over_boost += 1.5
    if target.ou is not None and float(target.ou) >= 2.75 and zero_count >= 1:
        base_over_boost += 1.0

    if low_count >= 2:
        base_under_boost += 3.0
    if low_count >= 2 and h2h_under_votes >= 2:
        base_under_boost += 2.0
    if target.ou is not None and float(target.ou) >= 3.25 and zero_count == 0 and low_count >= 1:
        base_under_boost += 1.5
    if (
        target.ou is not None
        and float(target.ou) <= 2.25
        and short_favorite_line
        and inverse_col3_draw
    ):
        base_under_boost += 1.5
        if favorite_recent_failed and underdog_clean_recent:
            base_under_boost += 2.5
    if (
        big_favorite_line
        and capped_total_line
        and h2h_over_votes >= 2
        and movement_align_votes >= 2
        and favorite_recent_cover
        and favorite_recent_control
    ):
        # AH alto + OU contenido suele ser firma de superioridad con techo, no de intercambio.
        base_under_boost += 2.5
        if underdog_recent_failed:
            base_under_boost += 2.0
    if target.ou is not None and float(target.ou) >= 2.75 and favorite_harder_line_pressure:
        base_over_boost += 2.5
    if target.ou is not None and float(target.ou) >= 2.50 and big_favorite_line and underdog_same_line_collapse:
        base_over_boost += 2.5
        if repricing_to_favorite:
            base_over_boost += 1.5
    if demolition_memory:
        base_over_boost += 2.0

    signals = {
        "zero_goal_high_volume_count": int(zero_count),
        "low_volume_count": int(low_count),
        "high_concede_count": int(high_concede_count),
        "h2h_over_votes": int(h2h_over_votes),
        "h2h_under_votes": int(h2h_under_votes),
        "duplicated_h2h_votes": bool(duplicated_h2h_votes),
        "movement_align_votes": int(movement_align_votes),
        "short_favorite_line": bool(short_favorite_line),
        "big_favorite_line": bool(big_favorite_line),
        "capped_total_line": bool(capped_total_line),
        "inverse_col3_draw": bool(inverse_col3_draw),
        "favorite_recent_cover": bool(favorite_recent_cover),
        "favorite_recent_failed": bool(favorite_recent_failed),
        "favorite_recent_control": bool(favorite_recent_control),
        "favorite_gap_current": favorite_gap_current,
        "favorite_prev_ah_abs": favorite_prev_ah_abs,
        "underdog_recent_failed": bool(underdog_recent_failed),
        "underdog_gap_current": underdog_gap_current,
        "underdog_prev_ah_abs": underdog_prev_ah_abs,
        "underdog_clean_recent": bool(underdog_clean_recent),
        "repricing_to_favorite": bool(repricing_to_favorite),
        "underdog_same_line_collapse": bool(underdog_same_line_collapse),
        "favorite_harder_line_pressure": bool(favorite_harder_line_pressure),
        "demolition_memory": bool(demolition_memory),
        "prev_home_profile": prev_home,
        "prev_away_profile": prev_away,
    }
    context_key = _conversation_ou_context_key(target, signals)
    cal_nodes = _get_ou_calibration_node(learning_state, context_key=context_key)
    g_node = cal_nodes["global"]
    c_node = cal_nodes["context"]

    g_over = _clamp(float(_safe_float(g_node.get("volume_over_boost")) or 1.0), 0.60, 1.80)
    g_under = _clamp(float(_safe_float(g_node.get("low_volume_under_boost")) or 1.0), 0.60, 1.80)
    c_over = _clamp(float(_safe_float(c_node.get("volume_over_boost")) or 1.0), 0.60, 1.80)
    c_under = _clamp(float(_safe_float(c_node.get("low_volume_under_boost")) or 1.0), 0.60, 1.80)
    c_samples = max(0, int(_safe_float(c_node.get("samples")) or 0))
    context_rel = _clamp(c_samples / 12.0, 0.0, 1.0)

    over_mult = (1.0 - context_rel) * g_over + context_rel * c_over
    under_mult = (1.0 - context_rel) * g_under + context_rel * c_under
    strength_clamped = _clamp(float(strength), 0.0, 1.0)

    over_adj = base_over_boost * over_mult * strength_clamped
    under_adj = base_under_boost * under_mult * strength_clamped
    net_delta = over_adj - under_adj

    narrative: List[str] = []
    if zero_count > 0:
        narrative.append(f"{zero_count} bloque(s) con 0 goles + volumen alto (regla VOLUMEN>EFICIENCIA).")
    if high_concede_count > 0:
        narrative.append(f"{high_concede_count} bloque(s) con fragilidad defensiva (2+ goles encajados).")
    if duplicated_h2h_votes:
        narrative.append("El H2H estadio/general es el mismo precedente: en OU se cuenta una sola vez.")
    if low_count >= 2 and h2h_under_votes >= 2:
        narrative.append("Ambos bloques con bajo volumen y H2H bajo: sesgo parcial hacia UNDER.")
    if short_favorite_line and inverse_col3_draw and favorite_recent_failed and underdog_clean_recent:
        narrative.append("Favorito corto tocado + Col3 inversa por empate + no favorito llegando de porteria a cero: sesgo parcial hacia UNDER.")
    if (
        big_favorite_line
        and capped_total_line
        and h2h_over_votes >= 2
        and movement_align_votes >= 2
        and favorite_recent_cover
        and favorite_recent_control
    ):
        narrative.append("AH alto + OU 3 contenido + favorito reciente dominante: la casa parece comprar superioridad con techo, no intercambio; sesgo parcial hacia UNDER/push.")
    if target.ou is not None and float(target.ou) >= 2.75 and favorite_harder_line_pressure:
        narrative.append("El favorito venia de una linea mucho mas dura con volumen muy alto: la bajada actual abre escenario de goleada y empuja el OVER.")
    if target.ou is not None and float(target.ou) >= 2.50 and big_favorite_line and underdog_same_line_collapse:
        narrative.append("El no favorito ya revento una linea igual o muy parecida: hay riesgo real de derrumbe y sesgo parcial hacia OVER.")
    if demolition_memory:
        narrative.append("Existe memoria de demolicion H2H y el OU sigue alto: no se debe enfriar artificialmente el partido.")
    if not narrative:
        narrative.append("Sin señal extrema de volumen: se mantiene el peso base de históricos.")

    return {
        "profile": CONVERSATION_PROFILE,
        "context_key": context_key,
        "signals": signals,
        "base_over_boost": round(base_over_boost, 3),
        "base_under_boost": round(base_under_boost, 3),
        "calibrated_over_boost": round(over_adj, 3),
        "calibrated_under_boost": round(under_adj, 3),
        "net_delta": round(net_delta, 3),
        "over_multiplier": round(over_mult, 3),
        "under_multiplier": round(under_mult, 3),
        "context_reliability": round(context_rel, 3),
        "narrative": narrative,
    }


def _apply_conversation_ou_feedback(
    state: Dict[str, Any],
    target: Any,
    conv: Dict[str, Any],
    score_raw: str,
    base: Any,
    learning_rate: float,
    predicted_pick_ou: str,
) -> Dict[str, Any]:
    out = {"applied": False, "reason": "", "actual_over": None}
    parsed = base._parse_score(score_raw)
    if not parsed:
        out["reason"] = "invalid_score"
        return out
    if target.ou is None:
        out["reason"] = "missing_ou"
        return out

    ou_feedback_log = state.setdefault("ou_feedback_log", {})
    match_key = str(target.match_id)
    if isinstance(ou_feedback_log, dict) and match_key in ou_feedback_log:
        out["reason"] = "already_logged"
        return out

    hg, ag = parsed
    actual_over = (hg + ag) > float(target.ou)
    out["actual_over"] = bool(actual_over)

    context_key = str(conv.get("context_key") or "unknown")
    nodes = _get_ou_calibration_node(state, context_key=context_key)
    g_node = nodes["global"]
    c_node = nodes["context"]

    lr = _clamp(float(learning_rate), 0.0, 1.0) * 0.08
    zero_count = int(conv.get("signals", {}).get("zero_goal_high_volume_count", 0))
    low_count = int(conv.get("signals", {}).get("low_volume_count", 0))
    dominance_under_signal = bool(
        conv.get("signals", {}).get("big_favorite_line")
        and conv.get("signals", {}).get("capped_total_line")
        and int(conv.get("signals", {}).get("h2h_over_votes", 0)) >= 2
        and int(conv.get("signals", {}).get("movement_align_votes", 0)) >= 2
        and conv.get("signals", {}).get("favorite_recent_cover")
        and conv.get("signals", {}).get("favorite_recent_control")
    )
    avalanche_over_signal = bool(
        conv.get("signals", {}).get("favorite_harder_line_pressure")
        or conv.get("signals", {}).get("underdog_same_line_collapse")
        or conv.get("signals", {}).get("demolition_memory")
    )
    predicted_is_over = str(predicted_pick_ou).strip().upper() == "OVER"
    ou_hit = (predicted_is_over and actual_over) or ((not predicted_is_over) and (not actual_over))

    for node in (g_node, c_node):
        node["samples"] = int(max(0, int(_safe_float(node.get("samples")) or 0) + 1))
        if ou_hit:
            node["hits"] = int(max(0, int(_safe_float(node.get("hits")) or 0) + 1))

    if zero_count > 0:
        step = lr * float(zero_count)
        for node in (g_node, c_node):
            cur = float(_safe_float(node.get("volume_over_boost")) or 1.0)
            cur = cur + step if actual_over else cur - step
            node["volume_over_boost"] = _clamp(cur, 0.60, 1.80)
    if avalanche_over_signal:
        step = lr * 1.25
        for node in (g_node, c_node):
            cur = float(_safe_float(node.get("volume_over_boost")) or 1.0)
            cur = cur + step if actual_over else cur - step
            node["volume_over_boost"] = _clamp(cur, 0.60, 1.80)
    if low_count >= 2:
        step = lr * float(low_count - 1)
        for node in (g_node, c_node):
            cur = float(_safe_float(node.get("low_volume_under_boost")) or 1.0)
            cur = cur + step if (not actual_over) else cur - step
            node["low_volume_under_boost"] = _clamp(cur, 0.60, 1.80)
    if dominance_under_signal:
        step = lr * 1.25
        for node in (g_node, c_node):
            cur = float(_safe_float(node.get("low_volume_under_boost")) or 1.0)
            cur = cur + step if (not actual_over) else cur - step
            node["low_volume_under_boost"] = _clamp(cur, 0.60, 1.80)

    if isinstance(ou_feedback_log, dict):
        ou_feedback_log[match_key] = {
            "actual_over": bool(actual_over),
            "predicted_pick_ou": str(predicted_pick_ou),
            "context_key": context_key,
            "recorded_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        }

    out["applied"] = True
    return out


def _prev_similarity(candidate_prev: Dict[str, Any], hist_prev: Dict[str, Any]) -> Dict[str, float]:
    score = 0.0
    max_score = 0.0

    def add_if_equal(key: str, weight: float) -> None:
        nonlocal score, max_score
        cv = candidate_prev.get(key)
        hv = hist_prev.get(key)
        if cv is None or hv is None:
            return
        max_score += weight
        if cv == hv:
            score += weight

    def add_numeric_closeness(key: str, scale: float, weight: float) -> None:
        nonlocal score, max_score
        cv = candidate_prev.get(key)
        hv = hist_prev.get(key)
        closeness = _metric_closeness(
            float(cv) if isinstance(cv, (int, float)) else None,
            float(hv) if isinstance(hv, (int, float)) else None,
            scale,
        )
        if closeness is None:
            return
        max_score += weight
        score += weight * closeness

    add_if_equal("prev_home_wdl", 1.0)
    add_if_equal("prev_away_wdl", 1.0)
    add_if_equal("prev_home_over", 0.75)
    add_if_equal("prev_away_over", 0.75)
    add_if_equal("prev_home_ah_bucket", 1.0)
    add_if_equal("prev_away_ah_bucket", 1.0)
    add_if_equal("prev_home_role", 1.25)
    add_if_equal("prev_away_role", 1.25)
    add_if_equal("prev_home_cover_own_line", 1.10)
    add_if_equal("prev_away_cover_own_line", 1.10)
    add_if_equal("prev_home_line_gap_band", 0.90)
    add_if_equal("prev_away_line_gap_band", 0.90)
    add_numeric_closeness("prev_home_ah_abs", scale=1.0, weight=1.00)
    add_numeric_closeness("prev_away_ah_abs", scale=1.0, weight=1.00)
    add_numeric_closeness("prev_home_line_gap_current", scale=0.50, weight=0.90)
    add_numeric_closeness("prev_away_line_gap_current", scale=0.50, weight=0.90)
    add_numeric_closeness("prev_home_score_margin", scale=2.0, weight=0.60)
    add_numeric_closeness("prev_away_score_margin", scale=2.0, weight=0.60)
    add_if_equal("prev_home_present", 0.25)
    add_if_equal("prev_away_present", 0.25)
    quality = (score / max_score) if max_score > 0 else 0.0
    return {
        "score": score,
        "quality": quality,
    }


def _favorite_ah_cover(match: Any) -> Optional[bool]:
    hg = match.final_home_goals
    ag = match.final_away_goals
    ah = match.ah
    fav_side = match.fav_side
    if hg is None or ag is None or ah is None:
        return None

    line = abs(float(ah))
    fav_margin = (hg - ag) if fav_side == "HOME" else (ag - hg)
    diff = fav_margin - line
    if diff > 0.05:
        return True
    if diff < -0.05:
        return False
    return None


def _find_target(candidates: Sequence[Any], team_query: str, match_id: str) -> Tuple[Optional[Any], List[Any]]:
    mid = str(match_id or "").strip()
    if mid:
        exact = [m for m in candidates if str(m.match_id) == mid]
        return (exact[0] if exact else None, exact)

    q = (team_query or "").strip().casefold()
    if not q:
        return (None, [])

    matched = [
        m
        for m in candidates
        if q in m.home.casefold() or q in m.away.casefold()
    ]
    matched.sort(
        key=lambda m: (
            m.kickoff or datetime.max,
            m.home,
            m.away,
        )
    )
    return (matched[0] if matched else None, matched)


def _weighted_rate(rows: Sequence[Tuple[Any, float]], predicate) -> float:
    if not rows:
        return 0.0
    total_w = sum(w for _, w in rows)
    if total_w <= 0:
        return 0.0
    num = sum(w for obj, w in rows if predicate(obj))
    return num / total_w


def _weighted_avg(values: Sequence[Tuple[float, float]]) -> Optional[float]:
    if not values:
        return None
    denom = sum(w for _, w in values)
    if denom <= 0:
        return None
    return sum(v * w for v, w in values) / denom


def _metric_closeness(v1: Optional[float], v2: Optional[float], scale: float) -> Optional[float]:
    if v1 is None or v2 is None or scale <= 0:
        return None
    closeness = 1.0 - abs(float(v1) - float(v2)) / scale
    if closeness < 0.0:
        return 0.0
    if closeness > 1.0:
        return 1.0
    return closeness


def _handicap_similarity(candidate: Any, hist: Any) -> Dict[str, Any]:
    if candidate.fav_side != hist.fav_side:
        return {
            "score": 0.0,
            "ah_gap": None,
            "bucket_match": False,
            "col3_gap": None,
            "valid": False,
        }
    if candidate.ah is None or hist.ah is None:
        return {
            "score": 0.0,
            "ah_gap": None,
            "bucket_match": False,
            "col3_gap": None,
            "valid": False,
        }

    ah_gap = abs(abs(float(candidate.ah)) - abs(float(hist.ah)))
    score = 2.0
    if ah_gap <= 0.01:
        score += 10.0
    elif ah_gap <= 0.25:
        score += 7.0
    elif ah_gap <= 0.50:
        score += 4.0

    bucket_match = False
    if candidate.ah_bucket_abs is not None and hist.ah_bucket_abs is not None:
        bucket_match = abs(float(candidate.ah_bucket_abs) - float(hist.ah_bucket_abs)) <= 1e-9
        if bucket_match:
            score += 2.5

    col3_gap = None
    c_col3 = candidate.features.get("col3_ah_bucket")
    h_col3 = hist.features.get("col3_ah_bucket")
    if c_col3 is not None and h_col3 is not None:
        col3_gap = abs(abs(float(c_col3)) - abs(float(h_col3)))
        if col3_gap <= 0.25:
            score += 2.0
        elif col3_gap <= 0.50:
            score += 0.75

    return {
        "score": score,
        "ah_gap": ah_gap,
        "bucket_match": bucket_match,
        "col3_gap": col3_gap,
        "valid": True,
    }


def _movement_similarity(candidate: Any, hist: Any) -> Dict[str, float]:
    score = 0.0
    possible = 0.0
    dir_hits = 0.0
    dir_total = 0.0
    numeric_samples: List[float] = []

    for key, weight in (
        ("stadium_mov_dir", 3.0),
        ("general_mov_dir", 3.0),
    ):
        cv = candidate.features.get(key)
        hv = hist.features.get(key)
        if cv is None or hv is None:
            continue
        possible += weight
        dir_total += 1.0
        if cv == hv:
            score += weight
            dir_hits += 1.0

    for key, weight in (
        ("stadium_wdl", 1.50),
        ("general_wdl", 1.50),
        ("stadium_over", 1.00),
        ("general_over", 1.00),
    ):
        cv = candidate.features.get(key)
        hv = hist.features.get(key)
        if cv is None or hv is None:
            continue
        possible += weight
        if cv == hv:
            score += weight

    c_mov = _extract_market_movement(candidate)
    h_mov = _extract_market_movement(hist)
    for scope in ("stadium", "general"):
        for metric, scale, weight in (
            ("end", 0.50, 2.00),
            ("delta", 0.50, 1.50),
        ):
            cv = c_mov.get(f"{scope}_{metric}")
            hv = h_mov.get(f"{scope}_{metric}")
            closeness = _metric_closeness(
                float(cv) if isinstance(cv, (int, float)) else None,
                float(hv) if isinstance(hv, (int, float)) else None,
                scale,
            )
            if closeness is None:
                continue
            possible += weight
            score += weight * closeness
            numeric_samples.append(closeness)

    quality = (score / possible) if possible > 0 else 0.0
    dir_rate = (dir_hits / dir_total) if dir_total > 0 else 0.0
    numeric_rate = (sum(numeric_samples) / len(numeric_samples)) if numeric_samples else 0.0
    return {
        "score": score,
        "quality": quality,
        "dir_rate": dir_rate,
        "numeric_rate": numeric_rate,
    }


def _stats_similarity(
    candidate_stats: Dict[str, Dict[str, Optional[float]]],
    hist_stats: Dict[str, Dict[str, Optional[float]]],
) -> Dict[str, Any]:
    block_norms: List[float] = []
    matched_blocks = 0
    compared_blocks = 0
    target_close: List[float] = []
    danger_close: List[float] = []
    attacks_close: List[float] = []

    for block in STAT_BLOCKS:
        c_block = candidate_stats.get(block, {})
        h_block = hist_stats.get(block, {})
        block_score = 0.0
        block_possible = 0.0

        for metric, weight in METRIC_WEIGHTS.items():
            closeness = _metric_closeness(c_block.get(metric), h_block.get(metric), METRIC_SCALES[metric])
            if closeness is None:
                continue
            block_possible += weight
            block_score += weight * closeness
            if metric == "target_total":
                target_close.append(closeness)
            elif metric == "danger_total":
                danger_close.append(closeness)
            elif metric == "attacks_total":
                attacks_close.append(closeness)

        if block_possible <= 0:
            continue
        compared_blocks += 1
        block_norm = block_score / block_possible
        block_norms.append(block_norm)
        if block_norm >= 0.48:
            matched_blocks += 1

    quality = (sum(block_norms) / len(block_norms)) if block_norms else 0.0
    score = quality * 10.0 + matched_blocks * 1.5
    return {
        "score": score,
        "quality": quality,
        "matched_blocks": matched_blocks,
        "compared_blocks": compared_blocks,
        "target_quality": (sum(target_close) / len(target_close)) if target_close else 0.0,
        "danger_quality": (sum(danger_close) / len(danger_close)) if danger_close else 0.0,
        "attacks_quality": (sum(attacks_close) / len(attacks_close)) if attacks_close else 0.0,
    }


def _coincidence_pct(candidate_value: Any, similars: Sequence[Tuple[Any, float]], extractor) -> Optional[float]:
    if candidate_value is None:
        return None
    if not similars:
        return None
    total = len(similars)
    hits = 0
    for hist, _ in similars:
        if extractor(hist) == candidate_value:
            hits += 1
    return (100.0 * hits / total) if total > 0 else None


def _pct_text(value: Optional[float]) -> str:
    if value is None:
        return "N/A"
    return f"{value:.1f}%"


def _pick_ou(p_over: float, p_under: float) -> str:
    if p_over >= 54.0:
        return "OVER"
    if p_under >= 54.0:
        return "UNDER"
    return "NO BET"


def _pick_ah(
    match: Any,
    p_fav_cover: float,
    p_und_cover: float,
    is_ah05: bool = False,
    draw_risk: float = 0.0,
) -> str:
    fav_strong = 57.0
    fav_light = 52.0
    und_strong = 57.0
    und_light = 52.0

    if is_ah05:
        # En AH 0.5, el empate castiga al favorito: elevar exigencia para ir con él.
        risk = _clamp(draw_risk, 0.0, 1.0)
        fav_strong += 2.5 * risk
        fav_light += 2.0 * risk
        und_strong -= 1.25 * risk
        und_light -= 0.75 * risk

    if p_fav_cover >= fav_strong:
        return f"{_side_text(match, favorite=True)} (fuerte)"
    if p_fav_cover >= fav_light:
        return f"{_side_text(match, favorite=True)} (ligero)"
    if p_und_cover >= und_strong:
        return f"{_side_text(match, favorite=False)} (fuerte)"
    if p_und_cover >= und_light:
        return f"{_side_text(match, favorite=False)} (ligero)"
    return "MUY PAREJO / NO BET"


def _confidence_label(edge_pct: float, quality: float, support: int, min_support: int) -> str:
    edge_norm = min(1.0, max(0.0, edge_pct / 35.0))
    quality_norm = min(1.0, max(0.0, quality))
    support_norm = min(1.0, max(0.0, support / 35.0))
    blended = 0.58 * edge_norm + 0.32 * quality_norm + 0.10 * support_norm

    if support < max(4, int(min_support * 0.60)):
        return "BAJA (muestra corta)"
    if blended >= 0.74:
        return "ALTA"
    if blended >= 0.60:
        return "MEDIA"
    return "BAJA"


def _side_text(match: Any, favorite: bool) -> str:
    if match.fav_side == "HOME":
        return "LOCAL (favorito)" if favorite else "VISITANTE (no favorito)"
    return "VISITANTE (favorito)" if favorite else "LOCAL (no favorito)"


def _format_dt(dt: Optional[datetime]) -> str:
    if not dt:
        return "-"
    return dt.strftime("%Y-%m-%d %H:%M")


def _stats_diagnostics(
    candidate_stats: Dict[str, Dict[str, Optional[float]]],
    sampled: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for block in STAT_BLOCKS:
        c_block = candidate_stats.get(block, {})
        block_diag: Dict[str, Any] = {}
        for metric in ("target_total", "danger_total", "attacks_total"):
            c_val = c_block.get(metric)
            if c_val is None:
                continue
            pairs: List[Tuple[float, float]] = []
            threshold = DIAG_THRESHOLDS[metric]
            for row in sampled:
                h_block = row["hist_stats"].get(block, {})
                h_val = h_block.get(metric)
                if h_val is None:
                    continue
                weight = float(row["sim_total"])
                pairs.append((float(h_val), weight))
            avg_hist = _weighted_avg(pairs)
            if avg_hist is None:
                continue
            denom = sum(w for _, w in pairs)
            close_w = sum(w for h_val, w in pairs if abs(float(h_val) - float(c_val)) <= threshold)
            close_pct = (100.0 * close_w / denom) if denom > 0 else 0.0
            block_diag[f"{metric}_value"] = round(float(c_val), 2)
            block_diag[f"{metric}_hist_avg"] = round(float(avg_hist), 2)
            block_diag[f"{metric}_close_pct"] = round(float(close_pct), 2)
        if block_diag:
            out[block] = block_diag
    return out


def _prev_handicap_snapshot(target: Any, prev_features: Dict[str, Any]) -> Dict[str, Any]:
    def _side(prefix: str) -> Dict[str, Any]:
        return {
            "ah_raw": prev_features.get(f"{prefix}_ah_raw"),
            "team_ah": prev_features.get(f"{prefix}_team_ah"),
            "ah_abs": prev_features.get(f"{prefix}_ah_abs"),
            "ah_bucket": prev_features.get(f"{prefix}_ah_bucket"),
            "role": prev_features.get(f"{prefix}_role"),
            "cover_own_line": prev_features.get(f"{prefix}_cover_own_line"),
            "score_margin": prev_features.get(f"{prefix}_score_margin"),
            "line_gap_current": prev_features.get(f"{prefix}_line_gap_current"),
            "line_gap_band": prev_features.get(f"{prefix}_line_gap_band"),
        }

    return {
        "current_match_ah": target.ah,
        "current_match_ah_abs": abs(float(target.ah)) if target.ah is not None else None,
        "prev_home": _side("prev_home"),
        "prev_away": _side("prev_away"),
    }


def render_markdown(payload: Dict[str, Any]) -> str:
    match = payload["match"]
    probs = payload["probabilities"]
    breakdown = payload["breakdown"]
    similar_rows = payload["top_similars"]
    prev_hc = payload.get("prev_handicap_variables") or {}
    ph = prev_hc.get("prev_home") if isinstance(prev_hc, dict) else {}
    pa = prev_hc.get("prev_away") if isinstance(prev_hc, dict) else {}
    if not isinstance(ph, dict):
        ph = {}
    if not isinstance(pa, dict):
        pa = {}

    lines = [
        f"# Analisis profundo: {match['home']} vs {match['away']}",
        "",
        f"- Fecha objetivo: {payload['date']}",
        f"- Kickoff detectado: {match['kickoff']}",
        f"- Match ID: {match['match_id']}",
        f"- Handicap actual: {match['ah']}",
        f"- OU actual: {match['ou_line']}",
        f"- Favorito por linea: {match['favorite_side']}",
        f"- Historicos similares usados: {payload['support']} (umbral solicitado: {payload['min_support']})",
        "",
        "## Pronostico",
        "",
        (
            f"- OU: **{payload['pick_ou']}** "
            f"(Over={probs['over']:.2f}%, Under={probs['under']:.2f}%, "
            f"confianza={payload['confidence_ou']})."
        ),
        (
            f"- Ganador Handicap: **{payload['pick_ah_winner']}** "
            f"(FavCover={probs['favorite_ah_cover']:.2f}%, "
            f"NoFavCover={probs['underdog_ah_cover']:.2f}%, Push={probs['ah_push']:.2f}%, "
            f"confianza={payload['confidence_ah']})."
        ),
        "",
        "## Diagnostico handicap y movimiento",
        "",
        f"- AH exacto: {breakdown['ah_exact_rate']}",
        f"- AH cercano (|gap|<=0.25): {breakdown['ah_gap_le_025_rate']}",
        f"- AH cercano (|gap|<=0.50): {breakdown['ah_gap_le_050_rate']}",
        f"- Direccion movimiento H2H: {breakdown['movement_dir_rate']}",
        f"- Movimiento numerico (endline/delta): {breakdown['movement_numeric_rate']}",
        "",
        "## Variables handicap de partidos previos",
        "",
    ]

    lines.extend(
        [
            f"- AH actual (absoluto): {prev_hc.get('current_match_ah_abs', 'N/A')}",
            f"- Prev Home | rol={ph.get('role', 'N/A')} | ah_raw={ph.get('ah_raw', 'N/A')} | "
            f"team_ah={ph.get('team_ah', 'N/A')} | bucket={ph.get('ah_bucket', 'N/A')} | "
            f"cover={ph.get('cover_own_line', 'N/A')} | gap={ph.get('line_gap_current', 'N/A')} ({ph.get('line_gap_band', 'N/A')})",
            f"- Prev Away | rol={pa.get('role', 'N/A')} | ah_raw={pa.get('ah_raw', 'N/A')} | "
            f"team_ah={pa.get('team_ah', 'N/A')} | bucket={pa.get('ah_bucket', 'N/A')} | "
            f"cover={pa.get('cover_own_line', 'N/A')} | gap={pa.get('line_gap_current', 'N/A')} ({pa.get('line_gap_band', 'N/A')})",
            "",
            "## Diagnostico stats (tiros/ataques)",
            "",
            f"- Calidad tiros a puerta: {breakdown['target_quality']}",
            f"- Calidad ataques peligrosos: {breakdown['danger_quality']}",
            f"- Calidad ataques: {breakdown['attacks_quality']}",
            f"- Bloques stats bien correlacionados: {breakdown['stats_blocks_match_rate']}",
            "",
        ]
    )

    lines.extend(
        [
        "## Lectura tecnica corta",
        "",
        payload["summary_text"],
        "",
        "## Lectura conversacion v4.4",
        "",
        ]
    )

    conv = payload.get("conversation_reading")
    if isinstance(conv, dict):
        lines.append(f"- Perfil aplicado: `{conv.get('profile', 'N/A')}`")
        lines.append(f"- Contexto OU: `{conv.get('context_key', 'N/A')}`")
        lines.append(
            f"- Ajuste OU por volumen/mercado: Over +{conv.get('calibrated_over_boost', 0)} "
            f"vs Under +{conv.get('calibrated_under_boost', 0)} "
            f"(delta neto={conv.get('net_delta', 0)})."
        )
        for note in conv.get("narrative", []):
            lines.append(f"- {note}")
    else:
        lines.append("- Sin capa conversacional activa.")

    lines.extend(
        [
            "",
        "## Top similares",
        "",
        "| # | Partido historico | Fecha | Marcador | AH | OU | Sim | AHcore | Mov | Stats | BlkStats | Over | FavCoverAH |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )

    for idx, row in enumerate(similar_rows, 1):
        lines.append(
            f"| {idx} | {row['home']} vs {row['away']} | {row['kickoff']} | "
            f"{row['score']} | {row['ah']} | {row['ou_line']} | {row['similarity']} | "
            f"{row['handicap_score']} | {row['movement_score']} | {row['stats_score']} | {row['stats_blocks_matched']} | "
            f"{row['over_hit']} | {row['favorite_ah_cover']} |"
        )

    if payload.get("stats_diagnostics"):
        lines.extend(
            [
                "",
                "## Stats por bloque",
                "",
            ]
        )
        for block, diag in payload["stats_diagnostics"].items():
            lines.append(f"- {block}: {json.dumps(diag, ensure_ascii=False)}")

    learning = payload.get("learning")
    if isinstance(learning, dict) and learning.get("enabled"):
        lines.extend(
            [
                "",
                "## Autoaprendizaje AH 0.5",
                "",
                f"- Contexto detectado: `{learning.get('context_key', 'N/A')}`",
                f"- Riesgo de empate del contexto: {learning.get('draw_risk_pct', 'N/A')}",
                f"- Fiabilidad del aprendizaje: {learning.get('reliability_pct', 'N/A')}",
                f"- Ajuste FavCover por aprendizaje: {learning.get('fav_cover_adjustment', 'N/A')}",
            ]
        )
        if learning.get("actual_feedback"):
            lines.append(f"- Feedback real aplicado: {learning.get('actual_feedback')}")
        elif learning.get("actual_feedback_reason"):
            lines.append(f"- Feedback real: {learning.get('actual_feedback_reason')}")

    feedback = payload.get("feedback")
    if isinstance(feedback, dict) and feedback.get("actual_score"):
        lines.extend(
            [
                "",
                "## Autopsia de feedback",
                "",
                f"- Resultado real: {feedback.get('actual_score')}",
                f"- Pick OU previo: {feedback.get('predicted_pick_ou')}",
                f"- OU real: {'OVER' if feedback.get('actual_over') else 'UNDER'}",
                f"- Pick AH previo: {feedback.get('predicted_pick_ah')}",
                f"- Handicap real: {feedback.get('actual_fav_cover_text')}",
                f"- Diagnostico: {feedback.get('diagnosis', 'N/A')}",
            ]
        )

    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    base = _load_base_module()

    project_root = Path(args.project_root).resolve()
    target_day: date = base._target_date(args.date)
    candidates, historical = base.load_project_data(project_root, int(args.history_limit))
    candidates_today = [m for m in candidates if base._match_on_day(m, target_day)]

    target_scope = "today_candidates"
    target, matched = _find_target(candidates_today, args.team_query, args.match_id)
    if target is None:
        target, matched = _find_target(candidates, args.team_query, args.match_id)
        if target is not None:
            target_scope = "all_candidates"
    if target is None:
        target, matched = _find_target(historical, args.team_query, args.match_id)
        if target is not None:
            target_scope = "historical"

    if target is None:
        raise SystemExit(
            f"[ERROR] No se encontro partido para team_query='{args.team_query}' "
            f"o match_id='{args.match_id}' en fecha {target_day.isoformat()} "
            f"(candidatos_hoy={len(candidates_today)}, candidatos_totales={len(candidates)}, historicos={len(historical)})."
        )

    target_prev = _extract_prev_features(target, base)
    target_stats = _extract_stats_blocks(target)
    target_ctx = _infer_context(target, target_prev)
    target_prev_handicap_vars = _prev_handicap_snapshot(target, target_prev)

    learning_enabled = not bool(args.disable_learning)
    learning_state_path = _learning_state_path(project_root, str(args.learning_state or ""))
    learning_state = _load_learning_state(learning_state_path) if learning_enabled else _default_learning_state()
    learning_context_key = _context_key(target_ctx)

    scored: List[Dict[str, Any]] = []
    for hist in historical:
        if hist.match_id == target.match_id:
            continue
        if hist.over_hit is None:
            continue
        if target.fav_side != hist.fav_side:
            continue
        if target.ah is None or hist.ah is None:
            continue

        ah_gap = abs(abs(float(target.ah)) - abs(float(hist.ah)))
        if ah_gap > float(args.ah_max_gap):
            continue
        if target.ou is not None and hist.ou is not None and abs(float(target.ou) - float(hist.ou)) > float(args.ou_max_gap):
            continue

        base_sim = float(base._feature_match_score(target, hist))
        if base_sim <= 0:
            continue

        handicap = _handicap_similarity(target, hist)
        if handicap["score"] < float(args.min_handicap_score):
            continue

        movement = _movement_similarity(target, hist)
        hist_prev = _extract_prev_features(hist, base)
        hist_ctx = _infer_context(hist, hist_prev)
        prev = _prev_similarity(target_prev, hist_prev)
        hist_stats = _extract_stats_blocks(hist)
        stats = _stats_similarity(target_stats, hist_stats)
        if stats["compared_blocks"] < 2:
            continue
        if stats["matched_blocks"] < int(args.min_stats_blocks):
            continue

        sim_total = (
            handicap["score"] * 2.20
            + movement["score"] * 1.80
            + base_sim * 1.20
            + prev["score"] * 0.90
            + stats["score"] * 1.60
        )
        context_multiplier = _context_similarity_multiplier(target_ctx, hist_ctx)
        sim_total *= context_multiplier
        if sim_total < float(args.min_similarity):
            continue

        scored.append(
            {
                "hist": hist,
                "sim_total": sim_total,
                "base_sim": base_sim,
                "handicap": handicap,
                "movement": movement,
                "prev": prev,
                "stats": stats,
                "hist_prev": hist_prev,
                "hist_stats": hist_stats,
                "hist_context": hist_ctx,
                "context_multiplier": context_multiplier,
            }
        )

    scored.sort(key=lambda x: x["sim_total"], reverse=True)
    sampled = scored[: max(1, int(args.max_similars))]
    support = len(sampled)
    min_support = max(1, int(args.min_support))

    if support <= 0:
        raise SystemExit("[ERROR] No hubo historicos tras filtros de handicap/movimiento/stats.")

    rows_sw = [(row["hist"], float(row["sim_total"])) for row in sampled]
    total_w = sum(w for _, w in rows_sw)
    if total_w <= 0:
        raise SystemExit("[ERROR] No hubo peso total positivo tras filtrar similares.")

    # OU
    p_over = 100.0 * _weighted_rate(rows_sw, lambda h: bool(h.over_hit))
    p_under = max(0.0, 100.0 - p_over)
    p_over_model_raw = p_over
    p_under_model_raw = p_under

    conversation_reading = _conversation_ou_adjustment(
        target=target,
        base=base,
        learning_state=learning_state,
        strength=float(args.conversation_strength),
    )
    p_over = _clamp(p_over + float(conversation_reading.get("net_delta", 0.0)), 0.0, 100.0)
    p_under = max(0.0, 100.0 - p_over)

    # Resultado favorito/no favorito/draw
    def _fav_result(hist: Any) -> str:
        if hist.final_home_goals is None or hist.final_away_goals is None:
            return "UNKNOWN"
        if hist.final_home_goals == hist.final_away_goals:
            return "DRAW"
        if hist.fav_side == "HOME":
            return "FAV_WIN" if hist.final_home_goals > hist.final_away_goals else "UND_WIN"
        return "FAV_WIN" if hist.final_away_goals > hist.final_home_goals else "UND_WIN"

    p_fav = 100.0 * _weighted_rate(rows_sw, lambda h: _fav_result(h) == "FAV_WIN")
    p_und = 100.0 * _weighted_rate(rows_sw, lambda h: _fav_result(h) == "UND_WIN")
    p_draw = max(0.0, 100.0 - p_fav - p_und)

    # Handicap cover favorito/no favorito
    ah_rows_non_push = [(h, w) for h, w in rows_sw if _favorite_ah_cover(h) is not None]
    p_fav_cover = 100.0 * _weighted_rate(ah_rows_non_push, lambda h: _favorite_ah_cover(h) is True)
    p_und_cover = max(0.0, 100.0 - p_fav_cover) if ah_rows_non_push else 0.0
    push_rate = 100.0 * _weighted_rate(rows_sw, lambda h: _favorite_ah_cover(h) is None)

    pick_ou = _pick_ou(p_over, p_under)
    p_fav_cover_raw = p_fav_cover
    p_und_cover_raw = p_und_cover
    learning_feedback_note = ""
    learning_feedback_reason = ""
    learning_reliability = 0.0
    learned_fav_rate = 0.5
    learned_draw_rate = 0.5
    draw_risk = float(target_ctx.get("draw_bias", 0.0))
    ingest_info: Dict[str, Any] = {"applied": False, "rows": 0, "added_weight": 0.0}
    ou_feedback_note = ""
    ou_feedback_reason = ""

    if learning_enabled and bool(target_ctx.get("is_ah05")):
        ingest_info = _ingest_target_similars(
            learning_state,
            target=target,
            target_day=target_day,
            target_ctx=target_ctx,
            sampled=sampled,
        )

        if str(args.actual_score or "").strip():
            fb = _apply_actual_feedback(
                learning_state,
                target=target,
                target_ctx=target_ctx,
                score_raw=str(args.actual_score).strip(),
                base=base,
            )
            if fb.get("applied"):
                learning_feedback_note = f"score={fb.get('score')}"
            else:
                learning_feedback_reason = str(fb.get("reason") or "no_aplicado")
        else:
            learning_feedback_reason = "sin_feedback_explicito"

        blend = _blend_learning(
            learning_state,
            context_key=learning_context_key,
            min_non_push=float(args.learning_min_non_push),
        )
        learning_reliability = float(blend["reliability"])
        learned_fav_rate = float(blend["fav_cover_rate"])
        learned_draw_rate = float(blend["draw_rate"])

        strength = _clamp(float(args.learning_strength), 0.0, 1.0) * learning_reliability
        learned_fav_pct = learned_fav_rate * 100.0
        p_fav_cover = (1.0 - strength) * p_fav_cover + strength * learned_fav_pct
        p_fav_cover = _clamp(p_fav_cover, 0.0, 100.0)
        p_und_cover = max(0.0, 100.0 - p_fav_cover)
        draw_risk = _clamp(0.60 * draw_risk + 0.40 * learned_draw_rate, 0.0, 1.0)

        _save_learning_state(learning_state_path, learning_state)

    actual_score_clean = str(args.actual_score or "").strip()
    if learning_enabled and actual_score_clean:
        ou_fb = _apply_conversation_ou_feedback(
            state=learning_state,
            target=target,
            conv=conversation_reading,
            score_raw=actual_score_clean,
            base=base,
            learning_rate=float(args.conversation_learning_rate),
            predicted_pick_ou=pick_ou,
        )
        if ou_fb.get("applied"):
            ou_feedback_note = f"actual_over={ou_fb.get('actual_over')}"
            _save_learning_state(learning_state_path, learning_state)
        else:
            ou_feedback_reason = str(ou_fb.get("reason") or "no_aplicado")
    elif learning_enabled:
        ou_feedback_reason = "sin_feedback_explicito"
    conversation_reading["ou_feedback_note"] = ou_feedback_note
    conversation_reading["ou_feedback_reason"] = ou_feedback_reason

    pick_ah_winner = _pick_ah(
        target,
        p_fav_cover,
        p_und_cover,
        is_ah05=bool(target_ctx.get("is_ah05")),
        draw_risk=draw_risk,
    )

    feedback_payload: Dict[str, Any] = {
        "actual_score": actual_score_clean,
        "predicted_pick_ou": pick_ou,
        "predicted_pick_ah": pick_ah_winner,
        "actual_over": None,
        "actual_fav_cover_text": "N/A",
        "diagnosis": "Sin feedback real cargado.",
        "ou_feedback_note": ou_feedback_note,
        "ou_feedback_reason": ou_feedback_reason,
    }
    if actual_score_clean:
        parsed_actual = base._parse_score(actual_score_clean)
        if parsed_actual:
            ah_actual_text = "N/A"
            if target.ou is not None:
                actual_over = (parsed_actual[0] + parsed_actual[1]) > float(target.ou)
                feedback_payload["actual_over"] = bool(actual_over)
            else:
                actual_over = None

            fav_cover_actual: Optional[bool] = None
            if target.ah is not None:
                fav_margin = (
                    parsed_actual[0] - parsed_actual[1]
                    if target.fav_side == "HOME"
                    else parsed_actual[1] - parsed_actual[0]
                )
                diff = float(fav_margin) - abs(float(target.ah))
                if diff > 0.05:
                    fav_cover_actual = True
                    ah_actual_text = "FAV_COVER"
                elif diff < -0.05:
                    fav_cover_actual = False
                    ah_actual_text = "NOFAV_COVER"
                else:
                    fav_cover_actual = None
                    ah_actual_text = "PUSH"
            feedback_payload["actual_fav_cover_text"] = ah_actual_text

            diagnosis_bits: List[str] = []
            if actual_over is not None:
                predicted_over = pick_ou == "OVER"
                predicted_under = pick_ou == "UNDER"
                ou_hit = (predicted_over and actual_over) or (predicted_under and (not actual_over))
                if ou_hit:
                    diagnosis_bits.append("Lectura OU correcta.")
                else:
                    if predicted_under and actual_over:
                        if int(conversation_reading.get("signals", {}).get("zero_goal_high_volume_count", 0)) > 0:
                            diagnosis_bits.append("Fallo OU: se quedo corto el sesgo de explosion por volumen.")
                        else:
                            diagnosis_bits.append("Fallo OU: el partido rompio por encima de la linea esperada.")
                    elif predicted_over and (not actual_over):
                        if int(conversation_reading.get("signals", {}).get("low_volume_count", 0)) >= 2:
                            diagnosis_bits.append("Fallo OU: bajo volumen real, faltaba penalizacion UNDER.")
                        else:
                            diagnosis_bits.append("Fallo OU: el ritmo ofensivo real fue menor al esperado.")
            if fav_cover_actual is not None:
                pick_is_fav = "(favorito)" in pick_ah_winner and "(no favorito)" not in pick_ah_winner
                pick_is_und = "(no favorito)" in pick_ah_winner
                ah_hit = (pick_is_fav and fav_cover_actual) or (pick_is_und and (not fav_cover_actual))
                if pick_is_fav or pick_is_und:
                    diagnosis_bits.append("Lectura AH correcta." if ah_hit else "Fallo AH frente al comportamiento real de la linea.")
            if not diagnosis_bits:
                diagnosis_bits.append("Feedback parcial: no se pudo evaluar todo el bloque.")
            feedback_payload["diagnosis"] = " ".join(diagnosis_bits)

    ah_exact_rate = 100.0 * _weighted_rate(
        [(row, row["sim_total"]) for row in sampled],
        lambda row: row["handicap"]["ah_gap"] is not None and row["handicap"]["ah_gap"] <= 0.01,
    )
    ah_gap_025_rate = 100.0 * _weighted_rate(
        [(row, row["sim_total"]) for row in sampled],
        lambda row: row["handicap"]["ah_gap"] is not None and row["handicap"]["ah_gap"] <= 0.25,
    )
    ah_gap_050_rate = 100.0 * _weighted_rate(
        [(row, row["sim_total"]) for row in sampled],
        lambda row: row["handicap"]["ah_gap"] is not None and row["handicap"]["ah_gap"] <= 0.50,
    )
    movement_dir_quality = _weighted_avg([(row["movement"]["dir_rate"], row["sim_total"]) for row in sampled]) or 0.0
    movement_numeric_quality = _weighted_avg([(row["movement"]["numeric_rate"], row["sim_total"]) for row in sampled]) or 0.0
    target_quality = _weighted_avg([(row["stats"]["target_quality"], row["sim_total"]) for row in sampled]) or 0.0
    danger_quality = _weighted_avg([(row["stats"]["danger_quality"], row["sim_total"]) for row in sampled]) or 0.0
    attacks_quality = _weighted_avg([(row["stats"]["attacks_quality"], row["sim_total"]) for row in sampled]) or 0.0
    stats_blocks_match_rate = _weighted_avg(
        [
            (
                row["stats"]["matched_blocks"] / max(1, row["stats"]["compared_blocks"]),
                row["sim_total"],
            )
            for row in sampled
        ]
    ) or 0.0

    quality_ou = 0.45 * (ah_gap_025_rate / 100.0) + 0.30 * movement_dir_quality + 0.25 * target_quality
    quality_ah = 0.50 * (ah_gap_025_rate / 100.0) + 0.30 * movement_numeric_quality + 0.20 * danger_quality
    if bool(target_ctx.get("is_ah05")) and p_fav_cover >= p_und_cover:
        quality_ah *= (1.0 - 0.20 * draw_risk)
    confidence_ou = _confidence_label(abs(p_over - p_under), quality_ou, support, min_support)
    confidence_ah = _confidence_label(abs(p_fav_cover - p_und_cover), quality_ah, support, min_support)

    # Desglose por bloques
    col3_keys = ("col3_wdl", "col3_over", "col3_ah_bucket", "col3_present")
    h2h_keys = ("stadium_wdl", "general_wdl", "stadium_over", "general_over", "stadium_mov_dir", "general_mov_dir")
    ind_keys = ("ind_left_wdl", "ind_right_wdl", "ind_left_over", "ind_right_over", "ind_left_present", "ind_right_present")

    def _block_rate(keys: Sequence[str], hist_getter) -> str:
        pcts: List[float] = []
        for k in keys:
            cval = hist_getter(target).get(k)
            pct = _coincidence_pct(cval, rows_sw, lambda h, kk=k: hist_getter(h).get(kk))
            if pct is not None:
                pcts.append(pct)
        if not pcts:
            return "N/A"
        return f"{sum(pcts)/len(pcts):.1f}%"

    col3_rate = _block_rate(col3_keys, lambda h: h.features)
    h2h_rate = _block_rate(h2h_keys, lambda h: h.features)
    ind_rate = _block_rate(ind_keys, lambda h: h.features)
    prev_keys = (
        "prev_home_wdl",
        "prev_away_wdl",
        "prev_home_over",
        "prev_away_over",
        "prev_home_ah_bucket",
        "prev_away_ah_bucket",
        "prev_home_role",
        "prev_away_role",
        "prev_home_cover_own_line",
        "prev_away_cover_own_line",
        "prev_home_line_gap_band",
        "prev_away_line_gap_band",
    )
    prev_rate = _block_rate(prev_keys, lambda h: _extract_prev_features(h, base))

    same_ah_bucket = _coincidence_pct(target.ah_bucket_abs, rows_sw, lambda h: h.ah_bucket_abs)
    close_ou = _coincidence_pct(
        True,
        rows_sw,
        lambda h: (
            (target.ou is not None and h.ou is not None and abs(float(target.ou) - float(h.ou)) <= 0.25)
        ),
    )

    similar_table: List[Dict[str, Any]] = []
    for row in sampled[: max(1, int(args.top_similars))]:
        hist = row["hist"]
        hg = hist.final_home_goals
        ag = hist.final_away_goals
        score = f"{hg}:{ag}" if hg is not None and ag is not None else "N/A"
        ah_cover = _favorite_ah_cover(hist)
        if ah_cover is True:
            ah_cover_txt = "FAV_COVER"
        elif ah_cover is False:
            ah_cover_txt = "NOFAV_COVER"
        else:
            ah_cover_txt = "PUSH"
        similar_table.append(
            {
                "match_id": hist.match_id,
                "home": hist.home,
                "away": hist.away,
                "kickoff": _format_dt(hist.kickoff),
                "score": score,
                "ah": hist.ah,
                "ou_line": hist.ou,
                "similarity": round(float(row["sim_total"]), 3),
                "base_similarity": round(float(row["base_sim"]), 3),
                "context_multiplier": round(float(row.get("context_multiplier", 1.0)), 3),
                "handicap_score": round(float(row["handicap"]["score"]), 3),
                "movement_score": round(float(row["movement"]["score"]), 3),
                "prev_similarity": round(float(row["prev"]["score"]), 3),
                "stats_score": round(float(row["stats"]["score"]), 3),
                "stats_blocks_matched": int(row["stats"]["matched_blocks"]),
                "over_hit": bool(hist.over_hit),
                "favorite_win": _fav_result(hist) == "FAV_WIN",
                "favorite_ah_cover": ah_cover_txt,
            }
        )

    stats_diag = _stats_diagnostics(target_stats, sampled)

    phv = target_prev_handicap_vars.get("prev_home", {}) if isinstance(target_prev_handicap_vars, dict) else {}
    pav = target_prev_handicap_vars.get("prev_away", {}) if isinstance(target_prev_handicap_vars, dict) else {}
    prev_h_txt = (
        f"PrevHome[{phv.get('role', 'N/A')}, cover={phv.get('cover_own_line', 'N/A')}, "
        f"gap={phv.get('line_gap_current', 'N/A')}({phv.get('line_gap_band', 'N/A')})]"
    )
    prev_a_txt = (
        f"PrevAway[{pav.get('role', 'N/A')}, cover={pav.get('cover_own_line', 'N/A')}, "
        f"gap={pav.get('line_gap_current', 'N/A')}({pav.get('line_gap_band', 'N/A')})]"
    )

    summary = (
        f"Se priorizo la correlacion de handicap: AH exacto={ah_exact_rate:.1f}% y "
        f"AH<=0.25={ah_gap_025_rate:.1f}%. El movimiento H2H acompana "
        f"(dir={movement_dir_quality*100.0:.1f}%, numerico={movement_numeric_quality*100.0:.1f}%). "
        f"Las stats de soporte muestran cercania en tiros a puerta={target_quality*100.0:.1f}% y "
        f"ataques peligrosos={danger_quality*100.0:.1f}%. "
        f"Previos AH: {prev_h_txt}; {prev_a_txt}. "
        f"Con soporte={support}, el modelo inclina OU hacia {pick_ou} y AH hacia {pick_ah_winner}. "
        f"Capa conversacion {CONVERSATION_PROFILE}: delta_OU={conversation_reading.get('net_delta', 0)} "
        f"(Over+{conversation_reading.get('calibrated_over_boost', 0)} / "
        f"Under+{conversation_reading.get('calibrated_under_boost', 0)})."
    )
    if bool(target_ctx.get("is_ah05")):
        summary += (
            f" Contexto AH0.5: draw_risk={draw_risk*100.0:.1f}% y "
            f"autolearn_reliability={learning_reliability*100.0:.1f}%."
        )

    payload: Dict[str, Any] = {
        "date": target_day.isoformat(),
        "project_root": str(project_root),
        "search": {
            "team_query": args.team_query,
            "match_id": args.match_id,
            "target_scope": target_scope,
            "matches_found": len(matched),
            "selected_match_id": str(target.match_id),
        },
        "model_config": {
            "min_similarity": float(args.min_similarity),
            "ah_max_gap": float(args.ah_max_gap),
            "ou_max_gap": float(args.ou_max_gap),
            "min_handicap_score": float(args.min_handicap_score),
            "min_stats_blocks": int(args.min_stats_blocks),
            "learning_enabled": bool(learning_enabled),
            "learning_state_path": str(learning_state_path),
            "learning_strength": float(args.learning_strength),
            "learning_min_non_push": float(args.learning_min_non_push),
            "conversation_profile": CONVERSATION_PROFILE,
            "conversation_strength": float(args.conversation_strength),
            "conversation_learning_rate": float(args.conversation_learning_rate),
        },
        "match": {
            "match_id": str(target.match_id),
            "home": target.home,
            "away": target.away,
            "kickoff": _format_dt(target.kickoff),
            "ah": target.ah,
            "ou_line": target.ou,
            "favorite_side": _side_text(target, favorite=True),
            "non_favorite_side": _side_text(target, favorite=False),
            "is_ah05": bool(target_ctx.get("is_ah05")),
            "context_draw_risk_band": target_ctx.get("draw_risk_band"),
            "context_movement_pair": target_ctx.get("movement_pair"),
        },
        "prev_handicap_variables": target_prev_handicap_vars,
        "support": support,
        "min_support": min_support,
        "pick_ou": pick_ou,
        "pick_ah_winner": pick_ah_winner,
        "confidence_ou": confidence_ou,
        "confidence_ah": confidence_ah,
        "probabilities": {
            "over_raw_model": round(p_over_model_raw, 2),
            "under_raw_model": round(p_under_model_raw, 2),
            "over": round(p_over, 2),
            "under": round(p_under, 2),
            "favorite_win": round(p_fav, 2),
            "underdog_win": round(p_und, 2),
            "draw": round(max(0.0, p_draw), 2),
            "favorite_ah_cover_raw": round(p_fav_cover_raw, 2),
            "underdog_ah_cover_raw": round(p_und_cover_raw, 2),
            "favorite_ah_cover": round(p_fav_cover, 2),
            "underdog_ah_cover": round(p_und_cover, 2),
            "ah_push": round(push_rate, 2),
        },
        "breakdown": {
            "ah_exact_rate": _pct_text(ah_exact_rate),
            "ah_gap_le_025_rate": _pct_text(ah_gap_025_rate),
            "ah_gap_le_050_rate": _pct_text(ah_gap_050_rate),
            "movement_dir_rate": _pct_text(movement_dir_quality * 100.0),
            "movement_numeric_rate": _pct_text(movement_numeric_quality * 100.0),
            "target_quality": _pct_text(target_quality * 100.0),
            "danger_quality": _pct_text(danger_quality * 100.0),
            "attacks_quality": _pct_text(attacks_quality * 100.0),
            "stats_blocks_match_rate": _pct_text(stats_blocks_match_rate * 100.0),
            "col3_match_rate": col3_rate,
            "market_h2h_match_rate": h2h_rate,
            "indirect_match_rate": ind_rate,
            "prev_match_rate": prev_rate,
            "same_ah_bucket_rate": _pct_text(same_ah_bucket),
            "close_ou_rate": _pct_text(close_ou),
            "draw_risk_context": _pct_text(draw_risk * 100.0),
            "learning_reliability": _pct_text(learning_reliability * 100.0),
            "learning_fav_cover_delta": _pct_text(p_fav_cover - p_fav_cover_raw),
            "conversation_delta_ou": f"{conversation_reading.get('net_delta', 0)} pts",
            "conversation_over_boost": f"{conversation_reading.get('calibrated_over_boost', 0)} pts",
            "conversation_under_boost": f"{conversation_reading.get('calibrated_under_boost', 0)} pts",
        },
        "learning": {
            "enabled": bool(learning_enabled and target_ctx.get("is_ah05")),
            "state_path": str(learning_state_path),
            "context_key": learning_context_key,
            "draw_risk_pct": _pct_text(draw_risk * 100.0),
            "reliability_pct": _pct_text(learning_reliability * 100.0),
            "learned_fav_cover_rate": _pct_text(learned_fav_rate * 100.0),
            "learned_draw_rate": _pct_text(learned_draw_rate * 100.0),
            "fav_cover_adjustment": _pct_text(p_fav_cover - p_fav_cover_raw),
            "ingested_similar_rows": int(ingest_info.get("rows", 0)),
            "ingest_added_weight": round(float(ingest_info.get("added_weight", 0.0)), 3),
            "actual_feedback": learning_feedback_note,
            "actual_feedback_reason": learning_feedback_reason,
        },
        "conversation_reading": conversation_reading,
        "feedback": feedback_payload,
        "stats_diagnostics": stats_diag,
        "summary_text": summary,
        "top_similars": similar_table,
    }

    md_text = render_markdown(payload)

    output_md = Path(args.output_md).resolve()
    output_json = Path(args.output_json).resolve()
    output_md.write_text(md_text, encoding="utf-8")
    output_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] Markdown: {output_md}")
    print(f"[OK] JSON: {output_json}")
    print(
        f"[INFO] target={target.home} vs {target.away} support={support} "
        f"OU={pick_ou} AH={pick_ah_winner} conv_delta={conversation_reading.get('net_delta', 0)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
