#!/usr/bin/env python3
"""Chronological backtest for the active Clave Dicotomica engine."""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from modules.clave_dicotomica import apply_key  # noqa: E402
from modules.housemind_ou import (  # noqa: E402
    get_match_date,
    merge_match_records,
    parse_score,
    safe_float,
    settle_ou_score,
)


def _json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _asian_split(line: float) -> Tuple[float, ...]:
    rounded = round(float(line) * 4.0) / 4.0
    floor_line = math.floor(rounded)
    fraction = round(rounded - floor_line, 2)
    if fraction == 0.25:
        return float(floor_line), float(floor_line) + 0.5
    if fraction == 0.75:
        return float(floor_line) + 0.5, float(floor_line) + 1.0
    return (rounded,)


def settle_favorite_margin(margin: int, handicap_magnitude: float) -> float:
    values = []
    for line in _asian_split(handicap_magnitude):
        residual = float(margin) - line
        values.append(1.0 if residual > 0 else -1.0 if residual < 0 else 0.0)
    return sum(values) / len(values)


def _score(match: Dict[str, Any], row_score: Any) -> Optional[Tuple[int, int]]:
    return (
        parse_score(match.get("final_score"))
        or parse_score(row_score)
        or parse_score(match.get("score"))
    )


def _exact_line(value: float) -> str:
    rounded = round(float(value) * 4.0) / 4.0
    return f"{rounded:+.2f}"


def _has_col3(match: Dict[str, Any]) -> bool:
    raw = match.get("h2h_col3") or {}
    if not isinstance(raw, dict):
        return False
    nested = raw.get("col3_data")
    col3 = nested if isinstance(nested, dict) else raw
    if col3.get("goles_home") is not None and col3.get("goles_away") is not None:
        return True
    return parse_score(col3.get("score") or col3.get("result")) is not None


def load_backtest_rows(db_path: Path) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    rows: List[Dict[str, Any]] = []
    audit: Counter = Counter()
    connection = sqlite3.connect(str(db_path))
    connection.row_factory = sqlite3.Row
    query = """
        SELECT match_id, match_date, handicap, score, payload_json, explorer_json
        FROM matches
        WHERE state = 'historical'
        ORDER BY match_id
    """
    try:
        for db_row in connection.execute(query):
            audit["historical_rows"] += 1
            match = merge_match_records(
                _json_dict(db_row["explorer_json"]),
                _json_dict(db_row["payload_json"]),
                {
                    "match_id": str(db_row["match_id"]),
                    "match_date": db_row["match_date"],
                    "handicap": db_row["handicap"],
                },
            )
            score = _score(match, db_row["score"])
            match_date = get_match_date(match)
            odds = match.get("main_match_odds")
            odds = odds if isinstance(odds, dict) else {}
            ah_line = safe_float(odds.get("ah_linea"))
            ou_line = safe_float(odds.get("goals_linea"))
            if score is None:
                audit["reject:no_score"] += 1
                continue
            if match_date is None:
                audit["reject:no_date"] += 1
                continue
            if ah_line is None or ou_line is None:
                audit["reject:no_lines"] += 1
                continue
            try:
                result = apply_key(match)
            except Exception as exc:
                audit[f"reject:engine_error:{type(exc).__name__}"] += 1
                continue

            favorite_margin = score[0] - score[1] if ah_line >= 0 else score[1] - score[0]
            favorite_settlement = settle_favorite_margin(favorite_margin, abs(ah_line))
            over_settlement = settle_ou_score(score[0] + score[1], ou_line, "OVER")
            ah_pick = result.get("ah")
            raw_ah_pick = result.get("raw_ah")
            ou_pick = result.get("ou")
            bookie_detector = result.get("bookie_detector") or {}
            ah_settlement = (
                favorite_settlement
                if ah_pick == "FAV_CUBRE"
                else -favorite_settlement
                if ah_pick == "DOG_CUBRE"
                else None
            )
            raw_ah_settlement = (
                favorite_settlement
                if raw_ah_pick == "FAV_CUBRE"
                else -favorite_settlement
                if raw_ah_pick == "DOG_CUBRE"
                else None
            )
            ou_settlement = (
                over_settlement
                if ou_pick == "OVER"
                else -over_settlement
                if ou_pick == "UNDER"
                else None
            )
            rows.append(
                {
                    "match_id": str(db_row["match_id"]),
                    "date": match_date,
                    "home": match.get("home_name") or match.get("home_team") or "",
                    "away": match.get("away_name") or match.get("away_team") or "",
                    "league": match.get("league_name") or match.get("league") or "",
                    "score": f"{score[0]}:{score[1]}",
                    "ah_line": ah_line,
                    "ah_line_exact": _exact_line(ah_line),
                    "ou_line": ou_line,
                    "ah_pick": ah_pick,
                    "core_ah": result.get("core_ah", ah_pick),
                    "raw_ah": raw_ah_pick,
                    "ou_pick": ou_pick,
                    "ah_settlement": ah_settlement,
                    "raw_ah_settlement": raw_ah_settlement,
                    "ou_settlement": ou_settlement,
                    "edge_ah": float(result.get("edge_AH") or 0.0),
                    "edge_ou": float(result.get("edge_OU") or 0.0),
                    "ah_fam": result.get("ah_fam") or "UNKNOWN",
                    "ou_fam": result.get("ou_fam") or "UNKNOWN",
                    "pressure": result.get("pressure") or "UNKNOWN",
                    "base_cover": result.get("base_cover") or "UNKNOWN",
                    "flags": list(result.get("flags") or []),
                    "mr_ah": list(result.get("mr_fav") or []) + list(result.get("mr_dog") or []),
                    "mr_ou": list(result.get("mr_over") or []) + list(result.get("mr_under") or []),
                    "quality": result.get("quality") or {},
                    "ah_gate_reasons": list(result.get("ah_gate_reasons") or []),
                    "core_prediction_tier_ah": result.get("core_prediction_tier_ah") or result.get("prediction_tier_ah"),
                    "prediction_tier_ah": result.get("prediction_tier_ah") or "NO_BET",
                    "expansion_ah_rule": result.get("expansion_ah_rule") or "CORE_OR_NONE",
                    "core_ah_gate_reasons": list(result.get("core_ah_gate_reasons") or result.get("ah_gate_reasons") or []),
                    "blocking_flags": list(result.get("blocking_flags") or []),
                    "production_ah_rules": list(result.get("production_ah_rules") or []),
                    "col3_available": "WITH_COL3" if _has_col3(match) else "WITHOUT_COL3",
                    "bookie_confirmation": result.get("bookie_confirmation") or "NO_DATA",
                    "bookie_aligned_signals": list(bookie_detector.get("aligned_signals") or []),
                    "bookie_conflicting_signals": list(bookie_detector.get("conflicting_signals") or []),
                    "col3_concordance": (
                        "COL3_AGREES"
                        if bookie_detector.get("col3_agrees") is True
                        else "COL3_CONFLICTS"
                        if bookie_detector.get("col3_agrees") is False
                        else "COL3_NO_BRANCH"
                    ),
                    "engine_version": result.get("engine_version") or "V6",
                }
            )
            audit["usable_rows"] += 1
    finally:
        connection.close()
    rows.sort(key=lambda row: (row["date"], row["match_id"]))
    return rows, dict(audit)


def wilson_lower(positive: int, total: int, z: float = 1.96) -> float:
    if total <= 0:
        return 0.0
    proportion = positive / total
    denominator = 1.0 + z * z / total
    center = proportion + z * z / (2.0 * total)
    spread = z * math.sqrt((proportion * (1.0 - proportion) + z * z / (4.0 * total)) / total)
    return (center - spread) / denominator


def metric(rows: Sequence[Dict[str, Any]], settlement_key: str) -> Dict[str, Any]:
    values = [row[settlement_key] for row in rows if row.get(settlement_key) is not None]
    positive = sum(value > 0 for value in values)
    negative = sum(value < 0 for value in values)
    pushes = sum(value == 0 for value in values)
    decided = positive + negative
    return {
        "rows": len(rows),
        "picks": len(values),
        "coverage": round(len(values) * 100.0 / max(len(rows), 1), 2),
        "positive": positive,
        "negative": negative,
        "pushes": pushes,
        "positive_rate": round(positive * 100.0 / decided, 2) if decided else None,
        "wilson_lower_95": round(wilson_lower(positive, decided) * 100.0, 2) if decided else None,
        "mean_settlement_even_odds": round(sum(values) / len(values), 5) if values else None,
    }


def _edge_band(value: float) -> str:
    magnitude = abs(float(value))
    if magnitude < 1.15:
        return "LT_1_15"
    if magnitude < 1.75:
        return "1_15_1_74"
    if magnitude < 2.5:
        return "1_75_2_49"
    if magnitude < 3.5:
        return "2_50_3_49"
    return "3_50_PLUS"


def grouped_metrics(
    rows: Sequence[Dict[str, Any]],
    group_key: str,
    settlement_key: str,
    minimum_picks: int = 1,
) -> List[Dict[str, Any]]:
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[str(row.get(group_key) or "UNKNOWN")].append(row)
    output = []
    for name, subset in groups.items():
        stats = metric(subset, settlement_key)
        if stats["picks"] < minimum_picks:
            continue
        output.append({"group": name, **stats})
    output.sort(key=lambda item: (-item["picks"], item["group"]))
    return output


def repeated_signal_metrics(
    rows: Sequence[Dict[str, Any]],
    signal_key: str,
    settlement_key: str,
    minimum_picks: int = 10,
) -> List[Dict[str, Any]]:
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        for signal in set(row.get(signal_key) or []):
            if row.get(settlement_key) is not None:
                groups[str(signal)].append(row)
    output = []
    for name, subset in groups.items():
        stats = metric(subset, settlement_key)
        if stats["picks"] < minimum_picks:
            continue
        output.append({"signal": name, **stats})
    output.sort(
        key=lambda item: (
            -float(item["wilson_lower_95"] or 0.0),
            -item["picks"],
            item["signal"],
        )
    )
    return output


def _temporal_folds(rows: Sequence[Dict[str, Any]], folds: int = 5) -> List[List[Dict[str, Any]]]:
    if not rows:
        return []
    output: List[List[Dict[str, Any]]] = []
    start = 0
    for fold_index in range(folds):
        target = len(rows) if fold_index == folds - 1 else int(len(rows) * (fold_index + 1) / folds)
        if target < len(rows):
            boundary_date = rows[max(start, target - 1)]["date"]
            while target < len(rows) and rows[target]["date"] == boundary_date:
                target += 1
        output.append(list(rows[start:target]))
        start = target
    return [fold for fold in output if fold]


def build_report(rows: Sequence[Dict[str, Any]], audit: Dict[str, int]) -> Dict[str, Any]:
    holdout_start = int(len(rows) * 0.80)
    if rows and holdout_start > 0:
        boundary_date = rows[holdout_start - 1]["date"]
        while holdout_start < len(rows) and rows[holdout_start]["date"] == boundary_date:
            holdout_start += 1
    holdout = list(rows[holdout_start:])
    enriched = []
    for row in rows:
        copied = dict(row)
        copied["edge_ah_band"] = _edge_band(row["edge_ah"])
        copied["edge_ou_band"] = _edge_band(row["edge_ou"])
        enriched.append(copied)
    holdout_enriched = enriched[holdout_start:]
    fold_rows = _temporal_folds(enriched, 5)
    engine_versions = Counter(row["engine_version"] for row in rows)
    return {
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "engine_versions": dict(engine_versions),
        "data_audit": audit,
        "range": {
            "start": rows[0]["date"].isoformat() if rows else None,
            "end": rows[-1]["date"].isoformat() if rows else None,
            "rows": len(rows),
        },
        "all": {
            "ah": metric(enriched, "ah_settlement"),
            "ou": metric(enriched, "ou_settlement"),
        },
        "holdout_20": {
            "range": {
                "start": holdout[0]["date"].isoformat() if holdout else None,
                "end": holdout[-1]["date"].isoformat() if holdout else None,
                "rows": len(holdout),
            },
            "ah": metric(holdout_enriched, "ah_settlement"),
            "ou": metric(holdout_enriched, "ou_settlement"),
            "ah_by_pick": grouped_metrics(holdout_enriched, "ah_pick", "ah_settlement"),
            "ah_by_tier": grouped_metrics(holdout_enriched, "prediction_tier_ah", "ah_settlement"),
            "ah_by_expansion_rule": grouped_metrics(holdout_enriched, "expansion_ah_rule", "ah_settlement"),
            "ou_by_pick": grouped_metrics(holdout_enriched, "ou_pick", "ou_settlement"),
            "ah_by_family": grouped_metrics(holdout_enriched, "ah_fam", "ah_settlement", 8),
            "ah_by_exact_line": grouped_metrics(holdout_enriched, "ah_line_exact", "ah_settlement", 5),
            "ah_by_col3": grouped_metrics(holdout_enriched, "col3_available", "ah_settlement"),
            "ah_by_col3_concordance": grouped_metrics(holdout_enriched, "col3_concordance", "ah_settlement"),
            "ah_by_bookie_confirmation": grouped_metrics(holdout_enriched, "bookie_confirmation", "ah_settlement"),
            "ou_by_family": grouped_metrics(holdout_enriched, "ou_fam", "ou_settlement", 8),
            "ou_by_col3": grouped_metrics(holdout_enriched, "col3_available", "ou_settlement"),
            "ah_by_edge": grouped_metrics(holdout_enriched, "edge_ah_band", "ah_settlement", 8),
            "ou_by_edge": grouped_metrics(holdout_enriched, "edge_ou_band", "ou_settlement", 8),
            "ah_micro_rules": repeated_signal_metrics(holdout_enriched, "mr_ah", "ah_settlement"),
            "ou_micro_rules": repeated_signal_metrics(holdout_enriched, "mr_ou", "ou_settlement"),
            "ah_flags": repeated_signal_metrics(holdout_enriched, "flags", "ah_settlement", 12),
            "ou_flags": repeated_signal_metrics(holdout_enriched, "flags", "ou_settlement", 12),
        },
        "temporal_folds": [
            {
                "fold": index + 1,
                "start": fold[0]["date"].isoformat(),
                "end": fold[-1]["date"].isoformat(),
                "ah": metric(fold, "ah_settlement"),
                "ou": metric(fold, "ou_settlement"),
            }
            for index, fold in enumerate(fold_rows)
        ],
    }


def markdown_report(report: Dict[str, Any]) -> str:
    holdout = report["holdout_20"]
    lines = [
        "# Backtest Clave Dicotomica",
        "",
        f"Motor: {report['engine_versions']}",
        f"Rango: {report['range']['start']} a {report['range']['end']} ({report['range']['rows']} partidos)",
        "",
        "## Holdout cronologico 20%",
        "",
        f"- AH: {holdout['ah']['positive_rate']}% | {holdout['ah']['picks']} picks | Wilson 95% {holdout['ah']['wilson_lower_95']}% | settlement medio {holdout['ah']['mean_settlement_even_odds']}",
        f"- O/U: {holdout['ou']['positive_rate']}% | {holdout['ou']['picks']} picks | Wilson 95% {holdout['ou']['wilson_lower_95']}% | settlement medio {holdout['ou']['mean_settlement_even_odds']}",
        "",
        "## Bloques temporales",
        "",
    ]
    for fold in report["temporal_folds"]:
        lines.append(
            f"- F{fold['fold']} {fold['start']}..{fold['end']}: "
            f"AH {fold['ah']['positive_rate']}% ({fold['ah']['picks']}) | "
            f"OU {fold['ou']['positive_rate']}% ({fold['ou']['picks']})"
        )
    lines.extend(["", "## Micro-reglas AH en holdout", ""])
    for row in holdout["ah_micro_rules"][:25]:
        lines.append(
            f"- {row['signal']}: {row['positive_rate']}% | n={row['picks']} | "
            f"Wilson={row['wilson_lower_95']}% | settlement={row['mean_settlement_even_odds']}"
        )
    lines.extend(["", "## AH por linea exacta en holdout", ""])
    lines.extend([
        "| Linea local | Picks | Acierto decidido | Push | Wilson 95% | Settlement medio |",
        "|---:|---:|---:|---:|---:|---:|",
    ])
    for row in holdout["ah_by_exact_line"]:
        lines.append(
            f"| {row['group']} | {row['picks']} | {row['positive_rate']}% | {row['pushes']} | "
            f"{row['wilson_lower_95']}% | {row['mean_settlement_even_odds']} |"
        )
    lines.extend(["", "## AH por nivel de publicacion", ""])
    for row in holdout["ah_by_tier"]:
        lines.append(
            f"- {row['group']}: {row['positive_rate']}% | n={row['picks']} | "
            f"Wilson={row['wilson_lower_95']}% | settlement={row['mean_settlement_even_odds']}"
        )
    lines.extend(["", "## Dependencia de Col3 en holdout", ""])
    for row in holdout["ah_by_col3"]:
        lines.append(
            f"- AH {row['group']}: {row['positive_rate']}% | n={row['picks']} | "
            f"Wilson={row['wilson_lower_95']}% | settlement={row['mean_settlement_even_odds']}"
        )
    for row in holdout["ou_by_col3"]:
        lines.append(
            f"- O/U {row['group']}: {row['positive_rate']}% | n={row['picks']} | "
            f"Wilson={row['wilson_lower_95']}% | settlement={row['mean_settlement_even_odds']}"
        )
    lines.extend(["", "## Confirmacion casa y concordancia Col3", ""])
    for row in holdout["ah_by_bookie_confirmation"]:
        lines.append(
            f"- Casa {row['group']}: {row['positive_rate']}% | n={row['picks']} | "
            f"Wilson={row['wilson_lower_95']}% | settlement={row['mean_settlement_even_odds']}"
        )
    for row in holdout["ah_by_col3_concordance"]:
        lines.append(
            f"- {row['group']}: {row['positive_rate']}% | n={row['picks']} | "
            f"Wilson={row['wilson_lower_95']}% | settlement={row['mean_settlement_even_odds']}"
        )
    lines.extend(["", "## Micro-reglas O/U en holdout", ""])
    for row in holdout["ou_micro_rules"][:25]:
        lines.append(
            f"- {row['signal']}: {row['positive_rate']}% | n={row['picks']} | "
            f"Wilson={row['wilson_lower_95']}% | settlement={row['mean_settlement_even_odds']}"
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Backtest cronologico de Clave Dicotomica.")
    parser.add_argument("--project-root", default=str(PROJECT_ROOT))
    parser.add_argument("--database", default="data/app_data.db")
    parser.add_argument("--output-json", default="data/clave_dicotomica/backtest.json")
    parser.add_argument("--output-md", default="data/clave_dicotomica/BACKTEST.md")
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    db_path = (project_root / args.database).resolve()
    output_json = (project_root / args.output_json).resolve()
    output_md = (project_root / args.output_md).resolve()
    rows, audit = load_backtest_rows(db_path)
    report = build_report(rows, audit)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(report, ensure_ascii=True, indent=2), encoding="utf-8")
    output_md.write_text(markdown_report(report), encoding="utf-8")
    print(f"[OK] {output_json}")
    print(f"[OK] {output_md}")
    print(
        "[HOLDOUT] "
        f"AH={report['holdout_20']['ah']['positive_rate']}%/{report['holdout_20']['ah']['picks']} "
        f"OU={report['holdout_20']['ou']['positive_rate']}%/{report['holdout_20']['ou']['picks']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
