#!/usr/bin/env python3
"""
Backtest walk-forward por handicap (no por liga) para validar rigor del sistema.

Evalua por bucket de handicap:
- Hit rate OU (OVER/UNDER)
- Hit rate AH (favorito vs no favorito)
- Cobertura de apuestas (cuantos picks vs no-bet)
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backtest por handicap del sistema explorador_automejora."
    )
    parser.add_argument("--project-root", default=".", help="Ruta del proyecto.")
    parser.add_argument(
        "--max-matches",
        type=int,
        default=1200,
        help="Maximo de partidos evaluados (los mas recientes). 0 = todos.",
    )
    parser.add_argument(
        "--min-history",
        type=int,
        default=600,
        help="Historico minimo requerido antes de empezar a evaluar.",
    )
    parser.add_argument("--min-support", type=int, default=12)
    parser.add_argument("--min-similarity", type=float, default=32.0)
    parser.add_argument("--max-similars", type=int, default=450)
    parser.add_argument("--ah-max-gap", type=float, default=0.25)
    parser.add_argument("--ou-max-gap", type=float, default=0.75)
    parser.add_argument("--min-handicap-score", type=float, default=8.0)
    parser.add_argument("--min-stats-blocks", type=int, default=2)
    parser.add_argument("--conversation-strength", type=float, default=0.65)
    parser.add_argument("--conversation-learning-rate", type=float, default=0.35)
    parser.add_argument(
        "--output-json",
        default="scripts/explorador_automejora/backtest_handicap.json",
        help="Salida JSON.",
    )
    parser.add_argument(
        "--output-md",
        default="scripts/explorador_automejora/backtest_handicap.md",
        help="Salida Markdown.",
    )
    return parser.parse_args()


def _load_module(path: Path, name: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"No se pudo cargar modulo: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _fmt_bucket(value: Optional[float]) -> str:
    if value is None:
        return "unknown"
    if abs(value) < 1e-9:
        return "0"
    text = f"{value:.2f}".rstrip("0").rstrip(".")
    return text


def _safe_float(v: Any) -> Optional[float]:
    try:
        return float(v)
    except Exception:
        return None


def _pick_ah_side(pick_text: str) -> Optional[str]:
    t = str(pick_text or "").upper()
    if "(NO FAVORITO)" in t:
        return "NOFAV"
    if "(FAVORITO)" in t and "(NO FAVORITO)" not in t:
        return "FAV"
    return None


def _parse_bucket_for_sort(bucket: str) -> float:
    v = _safe_float(bucket)
    if v is None:
        return 999.0
    return v


@dataclass
class EvalRow:
    match_id: str
    bucket: str
    support: int
    pick_ou: str
    ou_hit: Optional[bool]
    pick_ah: str
    ah_hit: Optional[bool]


def _weighted_rate(rows: Sequence[Tuple[Any, float]], predicate) -> float:
    if not rows:
        return 0.0
    denom = sum(w for _, w in rows)
    if denom <= 0:
        return 0.0
    num = sum(w for obj, w in rows if predicate(obj))
    return num / denom


def _build_markdown(payload: Dict[str, Any]) -> str:
    lines: List[str] = [
        "# Backtest por Handicap",
        "",
        f"- Partidos evaluados: {payload['overall']['tested_matches']}",
        f"- Picks OU: {payload['overall']['ou_bets']} (hit={payload['overall']['ou_hit_rate']}%)",
        f"- Picks AH: {payload['overall']['ah_bets']} (hit={payload['overall']['ah_hit_rate']}%)",
        f"- Doble pick (OU+AH): {payload['overall']['both_bets']} (hit conjunto={payload['overall']['both_hit_rate']}%)",
        "",
        "## Resultados por handicap",
        "",
        "| Handicap | Test | OU Bets | OU Hit% | AH Bets | AH Hit% | Doble Bets | Doble Hit% |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]

    for row in payload["by_handicap"]:
        lines.append(
            f"| {row['handicap_bucket']} | {row['tested']} | {row['ou_bets']} | {row['ou_hit_rate']} | "
            f"{row['ah_bets']} | {row['ah_hit_rate']} | {row['both_bets']} | {row['both_hit_rate']} |"
        )

    lines.extend(["", "## Config usada", "", "```json", json.dumps(payload["config"], ensure_ascii=False, indent=2), "```"])
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    project_root = Path(args.project_root).resolve()
    scripts_dir = Path(__file__).resolve().parent

    base = _load_module(scripts_dir / "generate_today_fav_over_report.py", "automejora_base_backtest")
    deep = _load_module(scripts_dir / "analyze_single_match_deep.py", "automejora_deep_backtest")

    _, historical = base.load_project_data(project_root, history_limit=0)
    rows = [
        m
        for m in historical
        if m.kickoff is not None
        and m.over_hit is not None
        and m.ah is not None
        and m.final_home_goals is not None
        and m.final_away_goals is not None
    ]
    rows.sort(key=lambda m: (m.kickoff, str(m.match_id)))

    if not rows:
        raise SystemExit("[ERROR] No hay historicos validos para backtest.")

    start_idx = max(0, int(args.min_history))
    if int(args.max_matches) > 0:
        start_idx = max(start_idx, len(rows) - int(args.max_matches))

    learning_state = deep._default_learning_state()
    eval_rows: List[EvalRow] = []

    for idx in range(start_idx, len(rows)):
        target = rows[idx]
        history = rows[:idx]
        if len(history) < int(args.min_history):
            continue

        target_prev = deep._extract_prev_features(target, base)
        target_stats = deep._extract_stats_blocks(target)
        target_ctx = deep._infer_context(target, target_prev)

        scored: List[Dict[str, Any]] = []
        for hist in history:
            if hist.match_id == target.match_id:
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

            handicap = deep._handicap_similarity(target, hist)
            if handicap["score"] < float(args.min_handicap_score):
                continue

            movement = deep._movement_similarity(target, hist)
            hist_prev = deep._extract_prev_features(hist, base)
            hist_ctx = deep._infer_context(hist, hist_prev)
            prev = deep._prev_similarity(target_prev, hist_prev)
            hist_stats = deep._extract_stats_blocks(hist)
            stats = deep._stats_similarity(target_stats, hist_stats)
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
            sim_total *= deep._context_similarity_multiplier(target_ctx, hist_ctx)
            if sim_total < float(args.min_similarity):
                continue

            scored.append(
                {
                    "hist": hist,
                    "sim_total": sim_total,
                }
            )

        if not scored:
            continue

        scored.sort(key=lambda x: x["sim_total"], reverse=True)
        sampled = scored[: max(1, int(args.max_similars))]
        support = len(sampled)
        if support < int(args.min_support):
            continue

        rows_sw = [(r["hist"], float(r["sim_total"])) for r in sampled]
        p_over = 100.0 * _weighted_rate(rows_sw, lambda h: bool(h.over_hit))
        p_under = max(0.0, 100.0 - p_over)

        conv = deep._conversation_ou_adjustment(
            target=target,
            base=base,
            learning_state=learning_state,
            strength=float(args.conversation_strength),
        )
        p_over = deep._clamp(p_over + float(conv.get("net_delta", 0.0)), 0.0, 100.0)
        p_under = max(0.0, 100.0 - p_over)
        pick_ou = deep._pick_ou(p_over, p_under)

        ah_rows_non_push = [(h, w) for h, w in rows_sw if deep._favorite_ah_cover(h) is not None]
        p_fav_cover = 100.0 * _weighted_rate(ah_rows_non_push, lambda h: deep._favorite_ah_cover(h) is True)
        p_und_cover = max(0.0, 100.0 - p_fav_cover) if ah_rows_non_push else 0.0
        pick_ah = deep._pick_ah(
            target,
            p_fav_cover,
            p_und_cover,
            is_ah05=bool(target_ctx.get("is_ah05")),
            draw_risk=float(target_ctx.get("draw_bias", 0.0)),
        )

        actual_over = bool(target.over_hit)
        ou_hit: Optional[bool] = None
        if pick_ou == "OVER":
            ou_hit = actual_over
        elif pick_ou == "UNDER":
            ou_hit = not actual_over

        actual_fav_cover = deep._favorite_ah_cover(target)
        pick_ah_side = _pick_ah_side(pick_ah)
        ah_hit: Optional[bool] = None
        if pick_ah_side is not None and actual_fav_cover is not None:
            ah_hit = (pick_ah_side == "FAV" and actual_fav_cover is True) or (
                pick_ah_side == "NOFAV" and actual_fav_cover is False
            )

        score_raw = f"{target.final_home_goals}:{target.final_away_goals}"
        deep._apply_conversation_ou_feedback(
            state=learning_state,
            target=target,
            conv=conv,
            score_raw=score_raw,
            base=base,
            learning_rate=float(args.conversation_learning_rate),
            predicted_pick_ou=pick_ou,
        )

        bucket = _fmt_bucket(base._normalize_ah_bucket(float(target.ah)))
        eval_rows.append(
            EvalRow(
                match_id=str(target.match_id),
                bucket=bucket,
                support=support,
                pick_ou=pick_ou,
                ou_hit=ou_hit,
                pick_ah=pick_ah,
                ah_hit=ah_hit,
            )
        )

    if not eval_rows:
        raise SystemExit("[ERROR] Sin partidos evaluados; relaja filtros de soporte/similitud.")

    by_bucket: Dict[str, Dict[str, Any]] = {}
    for row in eval_rows:
        agg = by_bucket.setdefault(
            row.bucket,
            {
                "tested": 0,
                "ou_bets": 0,
                "ou_hits": 0,
                "ah_bets": 0,
                "ah_hits": 0,
                "both_bets": 0,
                "both_hits": 0,
            },
        )
        agg["tested"] += 1
        if row.ou_hit is not None:
            agg["ou_bets"] += 1
            if row.ou_hit:
                agg["ou_hits"] += 1
        if row.ah_hit is not None:
            agg["ah_bets"] += 1
            if row.ah_hit:
                agg["ah_hits"] += 1
        if row.ou_hit is not None and row.ah_hit is not None:
            agg["both_bets"] += 1
            if row.ou_hit and row.ah_hit:
                agg["both_hits"] += 1

    bucket_rows: List[Dict[str, Any]] = []
    for bucket in sorted(by_bucket.keys(), key=_parse_bucket_for_sort):
        a = by_bucket[bucket]
        ou_hit_rate = round(100.0 * a["ou_hits"] / a["ou_bets"], 2) if a["ou_bets"] > 0 else None
        ah_hit_rate = round(100.0 * a["ah_hits"] / a["ah_bets"], 2) if a["ah_bets"] > 0 else None
        both_hit_rate = round(100.0 * a["both_hits"] / a["both_bets"], 2) if a["both_bets"] > 0 else None
        bucket_rows.append(
            {
                "handicap_bucket": bucket,
                "tested": a["tested"],
                "ou_bets": a["ou_bets"],
                "ou_hit_rate": ou_hit_rate,
                "ah_bets": a["ah_bets"],
                "ah_hit_rate": ah_hit_rate,
                "both_bets": a["both_bets"],
                "both_hit_rate": both_hit_rate,
            }
        )

    overall = {
        "tested_matches": len(eval_rows),
        "ou_bets": sum(r["ou_bets"] for r in by_bucket.values()),
        "ou_hits": sum(r["ou_hits"] for r in by_bucket.values()),
        "ah_bets": sum(r["ah_bets"] for r in by_bucket.values()),
        "ah_hits": sum(r["ah_hits"] for r in by_bucket.values()),
        "both_bets": sum(r["both_bets"] for r in by_bucket.values()),
        "both_hits": sum(r["both_hits"] for r in by_bucket.values()),
    }
    overall["ou_hit_rate"] = round(100.0 * overall["ou_hits"] / overall["ou_bets"], 2) if overall["ou_bets"] > 0 else None
    overall["ah_hit_rate"] = round(100.0 * overall["ah_hits"] / overall["ah_bets"], 2) if overall["ah_bets"] > 0 else None
    overall["both_hit_rate"] = round(100.0 * overall["both_hits"] / overall["both_bets"], 2) if overall["both_bets"] > 0 else None

    payload: Dict[str, Any] = {
        "config": {
            "max_matches": int(args.max_matches),
            "min_history": int(args.min_history),
            "min_support": int(args.min_support),
            "min_similarity": float(args.min_similarity),
            "max_similars": int(args.max_similars),
            "ah_max_gap": float(args.ah_max_gap),
            "ou_max_gap": float(args.ou_max_gap),
            "min_handicap_score": float(args.min_handicap_score),
            "min_stats_blocks": int(args.min_stats_blocks),
            "conversation_strength": float(args.conversation_strength),
            "conversation_learning_rate": float(args.conversation_learning_rate),
        },
        "overall": overall,
        "by_handicap": bucket_rows,
    }

    out_json = Path(args.output_json).resolve()
    out_md = Path(args.output_md).resolve()
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    out_md.write_text(_build_markdown(payload), encoding="utf-8")

    print(f"[OK] JSON: {out_json}")
    print(f"[OK] MD: {out_md}")
    print(
        f"[INFO] tested={overall['tested_matches']} "
        f"ou_hit={overall['ou_hit_rate']}% ah_hit={overall['ah_hit_rate']}%"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

