#!/usr/bin/env python3
"""
Genera un informe Top-N de partidos "Favorito + Over" para hoy
usando señales del ecosistema /explorador:
- H2H Estadio
- H2H General
- H2H Col3 (peso principal)
- Comparativas indirectas
"""

from __future__ import annotations

import argparse
import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo


MADRID_TZ = ZoneInfo("Europe/Madrid")
MAIN_PAGE_CACHE_KEY = "app_main_page_cache_v1"


@dataclass
class EncodedMatch:
    raw: Dict[str, Any]
    match_id: str
    home: str
    away: str
    kickoff: Optional[datetime]
    ah: Optional[float]
    ou: Optional[float]
    fav_side: str
    ah_bucket_abs: Optional[float]
    final_home_goals: Optional[int]
    final_away_goals: Optional[int]
    over_hit: Optional[bool]
    favorite_win: Optional[bool]
    favorite_over_hit: Optional[bool]
    features: Dict[str, Any]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Top de favoritos+over con patrones del explorador"
    )
    parser.add_argument(
        "--project-root",
        default=".",
        help="Ruta del proyecto (contiene carpeta src/). Default: directorio actual.",
    )
    parser.add_argument(
        "--date",
        default="today",
        help="Fecha objetivo YYYY-MM-DD o 'today' (zona Europe/Madrid).",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=15,
        help="Cantidad de picks a devolver.",
    )
    parser.add_argument(
        "--min-support",
        type=int,
        default=12,
        help="Mínimo de históricos similares para aceptar pick.",
    )
    parser.add_argument(
        "--history-limit",
        type=int,
        default=0,
        help="Limitar históricos cargados (0 = sin límite).",
    )
    parser.add_argument(
        "--output-md",
        default="report_favorito_over_hoy.md",
        help="Ruta de salida markdown.",
    )
    parser.add_argument(
        "--output-json",
        default="report_favorito_over_hoy.json",
        help="Ruta de salida json.",
    )
    return parser.parse_args()


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


def _parse_score(score: Any) -> Optional[Tuple[int, int]]:
    if score is None:
        return None
    text = str(score).strip()
    if not text or "?" in text:
        return None
    text = text.replace(" - ", ":").replace("-", ":")
    parts = text.split(":")
    if len(parts) != 2:
        return None
    try:
        return int(parts[0]), int(parts[1])
    except Exception:
        return None


def _normalize_ah_bucket(ah: float) -> float:
    """
    Igual criterio que pattern_search.normalize_ah_bucket:
    enteros se mantienen, decimales a .5 del mismo entero.
    """
    if abs(ah % 1.0) < 1e-9:
        return float(ah)
    sign = 1 if ah >= 0 else -1
    base = math.floor(abs(ah))
    return sign * (base + 0.5)


def _favorite_side(ah: Optional[float]) -> str:
    # Convención del proyecto:
    # ah > 0 => HOME favorito
    # ah < 0 => AWAY favorito
    # ah = 0 => NEUTRAL
    if ah is None:
        return "UNKNOWN"
    if ah > 0.01:
        return "HOME"
    if ah < -0.01:
        return "AWAY"
    return "NEUTRAL"


def _parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None

    normalized = text.replace("/", "-")
    if "T" in normalized:
        normalized = normalized.split(".")[0]

    formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%m-%d-%Y",
        "%m-%d-%Y %H:%M",
        "%m-%d-%Y %H:%M:%S",
        "%m/%d/%Y",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y %H:%M:%S",
    )
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            pass
        try:
            return datetime.strptime(normalized, fmt)
        except Exception:
            pass
    return None


def _extract_match_datetime(match: Dict[str, Any]) -> Optional[datetime]:
    # Caso común en precacheo: fecha y hora separadas
    m_date = match.get("match_date") or match.get("date")
    m_time = match.get("time")
    if m_date and m_time:
        t = str(m_time).strip()
        # Algunos registros vienen como "9:00:".
        while t.endswith(":"):
            t = t[:-1]
        combo = f"{m_date} {t}"
        dt = _parse_datetime(combo)
        if dt:
            return dt

    for key in ("start_time", "match_date", "date", "time_obj", "precacheo_date"):
        dt = _parse_datetime(match.get(key))
        if dt:
            return dt
    return None


def _movement_direction(movement: Any) -> Optional[str]:
    if movement is None:
        return None
    text = str(movement).strip().replace("→", "->").replace(" ", "").replace(",", ".")
    if "->" not in text:
        return None
    parts = text.split("->")
    if len(parts) != 2:
        return None
    start = _safe_float(parts[0])
    end = _safe_float(parts[1])
    if start is None or end is None:
        return None
    if end > start:
        return "UP"
    if end < start:
        return "DOWN"
    return "FLAT"


def _score_result_to_wdl(score: Any) -> Optional[str]:
    parsed = _parse_score(score)
    if not parsed:
        return None
    h, a = parsed
    if h > a:
        return "HOME_WIN"
    if a > h:
        return "AWAY_WIN"
    return "DRAW"


def _score_over(score: Any, line: Optional[float]) -> Optional[bool]:
    parsed = _parse_score(score)
    if not parsed or line is None:
        return None
    h, a = parsed
    return (h + a) > line


def _extract_market_h2h_features(match: Dict[str, Any], ou_line: Optional[float]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    market = match.get("market_analysis_data") or {}
    if isinstance(market, dict):
        stadium = market.get("stadium") or {}
        general = market.get("general") or {}

        st_res = stadium.get("result") or stadium.get("score")
        ge_res = general.get("result") or general.get("score")

        out["stadium_wdl"] = _score_result_to_wdl(st_res)
        out["general_wdl"] = _score_result_to_wdl(ge_res)
        out["stadium_over"] = _score_over(st_res, ou_line)
        out["general_over"] = _score_over(ge_res, ou_line)
        out["stadium_mov_dir"] = _movement_direction(stadium.get("movement"))
        out["general_mov_dir"] = _movement_direction(general.get("movement"))
    else:
        out["stadium_wdl"] = None
        out["general_wdl"] = None
        out["stadium_over"] = None
        out["general_over"] = None
        out["stadium_mov_dir"] = None
        out["general_mov_dir"] = None

    return out


def _extract_col3_features(match: Dict[str, Any], ou_line: Optional[float]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    col3 = match.get("h2h_col3") or {}
    if not isinstance(col3, dict) or col3.get("status") != "found":
        out["col3_present"] = False
        out["col3_wdl"] = None
        out["col3_over"] = None
        out["col3_ah_bucket"] = None
        return out

    home_goals = _safe_float(col3.get("goles_home"))
    away_goals = _safe_float(col3.get("goles_away"))
    if home_goals is None or away_goals is None:
        wdl = None
        over = None
    else:
        hg = int(home_goals)
        ag = int(away_goals)
        if hg > ag:
            wdl = "HOME_WIN"
        elif ag > hg:
            wdl = "AWAY_WIN"
        else:
            wdl = "DRAW"
        over = (hg + ag) > ou_line if ou_line is not None else None

    out["col3_present"] = True
    out["col3_wdl"] = wdl
    out["col3_over"] = over
    c_ah = _safe_float(col3.get("handicap") or col3.get("ah"))
    out["col3_ah_bucket"] = _normalize_ah_bucket(c_ah) if c_ah is not None else None
    return out


def _extract_indirect_features(match: Dict[str, Any], ou_line: Optional[float]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    comp = match.get("comparativas_indirectas") or {}
    left = comp.get("left") if isinstance(comp, dict) else None
    right = comp.get("right") if isinstance(comp, dict) else None

    def _encode(prefix: str, node: Any) -> None:
        if not isinstance(node, dict):
            out[f"{prefix}_wdl"] = None
            out[f"{prefix}_over"] = None
            out[f"{prefix}_present"] = False
            return
        score = node.get("score")
        out[f"{prefix}_wdl"] = _score_result_to_wdl(score)
        out[f"{prefix}_over"] = _score_over(score, ou_line)
        out[f"{prefix}_present"] = bool(score)

    _encode("ind_left", left)
    _encode("ind_right", right)
    return out


def encode_match(match: Dict[str, Any], historical: bool) -> Optional[EncodedMatch]:
    odds = match.get("main_match_odds") or {}
    ah = _safe_float(odds.get("ah_linea") if isinstance(odds, dict) else None)
    if ah is None:
        ah = _safe_float(match.get("handicap"))

    ou_line = _safe_float(odds.get("goals_linea") if isinstance(odds, dict) else None)
    if ou_line is None:
        ou_line = _safe_float(match.get("goals_linea"))
    if ou_line is None:
        ou_line = _safe_float(match.get("ou_line"))

    fav_side = _favorite_side(ah)
    if fav_side in ("UNKNOWN", "NEUTRAL"):
        return None

    ah_bucket_abs = None
    if ah is not None:
        ah_bucket_abs = abs(_normalize_ah_bucket(ah))

    home = str(match.get("home_name") or match.get("home_team") or "").strip()
    away = str(match.get("away_name") or match.get("away_team") or "").strip()
    match_id = str(match.get("match_id") or match.get("id") or "").strip()
    if not home or not away:
        return None

    score = match.get("final_score") or match.get("score")
    parsed_score = _parse_score(score)
    if historical and not parsed_score:
        return None

    if parsed_score:
        hg, ag = parsed_score
        over_hit = (hg + ag) > ou_line if ou_line is not None else None
        if fav_side == "HOME":
            fav_win = hg > ag
        else:
            fav_win = ag > hg
        fav_over_hit = bool(fav_win and over_hit) if over_hit is not None else None
    else:
        hg = ag = None
        over_hit = None
        fav_win = None
        fav_over_hit = None

    features: Dict[str, Any] = {}
    features.update(_extract_market_h2h_features(match, ou_line))
    features.update(_extract_col3_features(match, ou_line))
    features.update(_extract_indirect_features(match, ou_line))

    return EncodedMatch(
        raw=match,
        match_id=match_id,
        home=home,
        away=away,
        kickoff=_extract_match_datetime(match),
        ah=ah,
        ou=ou_line,
        fav_side=fav_side,
        ah_bucket_abs=ah_bucket_abs,
        final_home_goals=hg,
        final_away_goals=ag,
        over_hit=over_hit,
        favorite_win=fav_win,
        favorite_over_hit=fav_over_hit,
        features=features,
    )


def _feature_match_score(candidate: EncodedMatch, hist: EncodedMatch) -> float:
    if candidate.fav_side != hist.fav_side:
        return 0.0
    if candidate.ah_bucket_abs is None or hist.ah_bucket_abs is None:
        return 0.0
    if abs(candidate.ah_bucket_abs - hist.ah_bucket_abs) > 0.5:
        return 0.0

    # Si la línea OU existe en ambos, tolerar distancia máxima de 1.0
    if candidate.ou is not None and hist.ou is not None:
        if abs(candidate.ou - hist.ou) > 1.0:
            return 0.0

    score = 0.0

    def add_if_equal(key: str, weight: float) -> None:
        nonlocal score
        c = candidate.features.get(key)
        h = hist.features.get(key)
        if c is None or h is None:
            return
        if c == h:
            score += weight

    # Peso fuerte en H2H Col3
    add_if_equal("col3_wdl", 3.0)
    add_if_equal("col3_over", 2.0)
    add_if_equal("col3_ah_bucket", 1.25)
    add_if_equal("col3_present", 0.75)

    # H2H Estadio / General
    add_if_equal("stadium_wdl", 1.5)
    add_if_equal("general_wdl", 1.5)
    add_if_equal("stadium_over", 1.0)
    add_if_equal("general_over", 1.0)
    add_if_equal("stadium_mov_dir", 0.75)
    add_if_equal("general_mov_dir", 0.75)

    # Comparativas indirectas
    add_if_equal("ind_left_wdl", 1.0)
    add_if_equal("ind_right_wdl", 1.0)
    add_if_equal("ind_left_over", 0.75)
    add_if_equal("ind_right_over", 0.75)
    add_if_equal("ind_left_present", 0.25)
    add_if_equal("ind_right_present", 0.25)

    return score


def _format_dt(dt: Optional[datetime]) -> str:
    if not dt:
        return "-"
    return dt.strftime("%Y-%m-%d %H:%M")


def _today_in_madrid() -> date:
    return datetime.now(MADRID_TZ).date()


def _target_date(raw: str) -> date:
    if not raw or raw.lower() == "today":
        return _today_in_madrid()
    return datetime.strptime(raw, "%Y-%m-%d").date()


def _match_on_day(match: EncodedMatch, target: date) -> bool:
    if not match.kickoff:
        return False
    return match.kickoff.date() == target


def _candidate_reason(candidate: EncodedMatch, similars: List[Tuple[EncodedMatch, float]]) -> str:
    if not similars:
        return "Sin históricos suficientemente parecidos."

    key_features = (
        ("col3_wdl", "H2H Col3 resultado"),
        ("col3_over", "H2H Col3 over"),
        ("stadium_wdl", "H2H estadio resultado"),
        ("general_wdl", "H2H general resultado"),
        ("ind_left_wdl", "Indirecta izquierda resultado"),
        ("ind_right_wdl", "Indirecta derecha resultado"),
    )

    total = len(similars)
    chunks = []
    for key, label in key_features:
        cval = candidate.features.get(key)
        if cval is None:
            continue
        matches = 0
        for hist, _ in similars:
            if hist.features.get(key) == cval:
                matches += 1
        if matches == 0:
            continue
        pct = 100.0 * matches / total
        chunks.append(f"{label} coincide en {pct:.0f}%")

    if not chunks:
        return "Patrones comparables detectados, pero sin señales dominantes claras."
    return "; ".join(chunks[:3]) + "."


def build_report(
    today_candidates: List[EncodedMatch],
    historical: List[EncodedMatch],
    top_n: int,
    min_support: int,
) -> Dict[str, Any]:
    ranked: List[Dict[str, Any]] = []

    for cand in today_candidates:
        scored: List[Tuple[EncodedMatch, float]] = []
        for hist in historical:
            if hist.match_id == cand.match_id:
                continue
            if hist.favorite_over_hit is None:
                continue
            sim = _feature_match_score(cand, hist)
            if sim >= 3.0:
                scored.append((hist, sim))

        if not scored:
            continue

        scored.sort(key=lambda x: x[1], reverse=True)
        sampled = scored[:450]
        support = len(sampled)
        if support < min_support:
            continue

        w_total = sum(sim for _, sim in sampled)
        w_hits = sum(sim for hist, sim in sampled if hist.favorite_over_hit)
        if w_total <= 0:
            continue
        p_hat = w_hits / w_total

        avg_sim = w_total / support
        reliability = min(1.0, support / 80.0)
        ranking_score = p_hat * (0.7 + 0.3 * reliability)

        # Penalizar si no hay Col3 en candidato
        if not cand.features.get("col3_present"):
            ranking_score *= 0.9

        pick_side = "LOCAL" if cand.fav_side == "HOME" else "VISITANTE"

        ranked.append(
            {
                "match_id": cand.match_id,
                "home": cand.home,
                "away": cand.away,
                "kickoff": _format_dt(cand.kickoff),
                "favorite_side": pick_side,
                "ah": cand.ah,
                "ou_line": cand.ou,
                "probability_fav_over": round(p_hat * 100.0, 2),
                "support": support,
                "avg_similarity": round(avg_sim, 3),
                "ranking_score": round(ranking_score, 6),
                "reason": _candidate_reason(cand, sampled),
            }
        )

    ranked.sort(
        key=lambda r: (
            r["ranking_score"],
            r["probability_fav_over"],
            r["support"],
            r["avg_similarity"],
        ),
        reverse=True,
    )

    return {"top": ranked[:top_n], "all_ranked": ranked}


def render_markdown(report: Dict[str, Any], target_day: date) -> str:
    top = report.get("top", [])
    lines = [
        f"# Informe Favorito + Over ({target_day.isoformat()})",
        "",
        "Metodo:",
        "- Base historica: SQL del explorador (`state=historical`).",
        "- Candidatos: partidos de hoy en `precacheo`.",
        "- Ranking por similitud de patrones: H2H Estadio, H2H General, Comparativas indirectas y H2H Col3 (peso mayor).",
        "- Salida probabilistica; no garantiza acierto infalible.",
        "",
    ]

    if not top:
        lines.extend(
            [
                "No se encontraron picks con soporte suficiente para hoy.",
                "",
                "Sugerencia: asegurar que `/precacheo` tenga partidos de hoy scrapeados y volver a ejecutar.",
            ]
        )
        return "\n".join(lines)

    lines.extend(
        [
            "## Top 15 (o menos si no cumplen soporte)",
            "",
            "| # | Partido | Favorito | AH | OU | Prob Fav+Over | Soporte |",
            "|---|---|---|---:|---:|---:|---:|",
        ]
    )

    for idx, row in enumerate(top, 1):
        lines.append(
            f"| {idx} | {row['home']} vs {row['away']} | {row['favorite_side']} | "
            f"{row['ah']} | {row['ou_line']} | {row['probability_fav_over']}% | {row['support']} |"
        )

    lines.append("")
    lines.append("## Justificacion breve por pick")
    lines.append("")
    for idx, row in enumerate(top, 1):
        lines.append(
            f"{idx}. `{row['home']} vs {row['away']}` ({row['kickoff']}): "
            f"{row['reason']} Prob. estimada favorito+over: {row['probability_fav_over']}% "
            f"(soporte={row['support']}, similitud media={row['avg_similarity']})."
        )

    return "\n".join(lines)


def load_project_data(
    project_root: Path, history_limit: int
) -> Tuple[List[EncodedMatch], List[EncodedMatch]]:
    src_dir = project_root / "src"
    if not src_dir.exists():
        raise RuntimeError(f"No existe carpeta src en: {project_root}")

    import sys

    sys.path.insert(0, str(src_dir))
    from modules import data_manager, sql_store  # type: ignore

    # Candidatos enriquecidos (precacheo)
    precache_rows = data_manager.load_precacheo_matches()
    candidates: List[EncodedMatch] = []
    for m in precache_rows:
        enc = encode_match(m, historical=False)
        if enc:
            candidates.append(enc)

    # Históricos de explorador
    scan_limit = history_limit if history_limit and history_limit > 0 else None
    historical_rows = data_manager.load_explorer_matches(scan_limit=scan_limit)
    historical: List[EncodedMatch] = []
    for m in historical_rows:
        enc = encode_match(m, historical=True)
        if enc and enc.favorite_over_hit is not None:
            historical.append(enc)

    # Si no hay precache, fallback a upcoming snapshot (menos señales)
    if not candidates:
        snapshot = sql_store.get_json_state(
            MAIN_PAGE_CACHE_KEY, default={"upcoming_matches": []}
        )
        fallback_rows = []
        if isinstance(snapshot, dict):
            fallback_rows = snapshot.get("upcoming_matches", []) or []
        for m in fallback_rows:
            enc = encode_match(m, historical=False)
            if enc:
                candidates.append(enc)

    return candidates, historical


def main() -> int:
    args = parse_args()
    project_root = Path(args.project_root).resolve()
    target_day = _target_date(args.date)

    candidates, historical = load_project_data(project_root, args.history_limit)
    candidates_today = [m for m in candidates if _match_on_day(m, target_day)]

    report = build_report(
        today_candidates=candidates_today,
        historical=historical,
        top_n=max(1, int(args.top)),
        min_support=max(1, int(args.min_support)),
    )

    payload = {
        "date": target_day.isoformat(),
        "project_root": str(project_root),
        "candidates_total": len(candidates),
        "candidates_today": len(candidates_today),
        "historical_total": len(historical),
        "top_count": len(report.get("top", [])),
        "top": report.get("top", []),
    }

    md_text = render_markdown(report, target_day)

    output_md = Path(args.output_md).resolve()
    output_json = Path(args.output_json).resolve()
    output_md.write_text(md_text, encoding="utf-8")
    output_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] Markdown: {output_md}")
    print(f"[OK] JSON: {output_json}")
    print(
        f"[INFO] candidatos_hoy={payload['candidates_today']} "
        f"historicos={payload['historical_total']} top={payload['top_count']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
