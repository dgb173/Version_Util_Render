#!/usr/bin/env python3
"""Discover AH expansion rules without touching the final chronological test.

The script only studies raw Clave directions which pass every V7 production
guard except the exact-line whitelist.  Candidate conditions are born in the
first 60%, are checked in the next 20%, and the selected union is evaluated
once in the last 20%.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from backtest_clave import PROJECT_ROOT, _edge_band, load_backtest_rows, metric


LINE_GATE_REASON = "linea AH sin validacion cronologica suficiente"
MIN_DISCOVERY_PICKS = 30
MIN_VALIDATION_PICKS = 12
MIN_DISCOVERY_RATE = 56.0
MIN_VALIDATION_RATE = 55.0
MIN_DISCOVERY_MEAN = 0.08
MIN_VALIDATION_MEAN = 0.05
MAX_SELECTED_RULES = 8


Condition = Tuple[str, str, str]
Rule = Tuple[Condition, ...]


def _split_at_date(rows: Sequence[Dict[str, Any]], proportion: float) -> int:
    target = int(len(rows) * proportion)
    if target <= 0 or target >= len(rows):
        return target
    boundary = rows[target - 1]["date"]
    while target < len(rows) and rows[target]["date"] == boundary:
        target += 1
    return target


def chronological_parts(
    rows: Sequence[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    discovery_end = _split_at_date(rows, 0.60)
    validation_end = _split_at_date(rows, 0.80)
    return (
        list(rows[:discovery_end]),
        list(rows[discovery_end:validation_end]),
        list(rows[validation_end:]),
    )


def eligible(row: Dict[str, Any]) -> bool:
    return (
        row.get("raw_ah") in {"FAV_CUBRE", "DOG_CUBRE"}
        and row.get("raw_ah_settlement") is not None
        and row.get("core_ah") not in {"FAV_CUBRE", "DOG_CUBRE"}
        and row.get("core_ah_gate_reasons") == [LINE_GATE_REASON]
    )


def _condition(field: str, value: Any, op: str = "eq") -> Condition:
    return field, op, str(value)


def candidates_for(row: Dict[str, Any]) -> Iterable[Rule]:
    base: Rule = (
        _condition("ah_line_exact", row.get("ah_line_exact")),
        _condition("raw_ah", row.get("raw_ah")),
    )
    yield base

    scalar_features = {
        "ah_fam": row.get("ah_fam"),
        "pressure": row.get("pressure"),
        "base_cover": row.get("base_cover"),
        "bookie_confirmation": row.get("bookie_confirmation"),
        "col3_concordance": row.get("col3_concordance"),
        "edge_ah_band": _edge_band(float(row.get("edge_ah") or 0.0)),
    }
    for field, value in scalar_features.items():
        if value not in (None, "", "UNKNOWN", "NO_DATA"):
            yield base + (_condition(field, value),)

    repeated_features = (
        "mr_ah",
        "flags",
        "production_ah_rules",
        "bookie_aligned_signals",
        "bookie_conflicting_signals",
    )
    for field in repeated_features:
        for value in sorted(set(row.get(field) or [])):
            yield base + (_condition(field, value, "contains"),)


def matches(row: Dict[str, Any], rule: Rule) -> bool:
    for field, op, expected in rule:
        actual = row.get(field)
        if field == "edge_ah_band":
            actual = _edge_band(float(row.get("edge_ah") or 0.0))
        if op == "contains":
            if expected not in {str(value) for value in (actual or [])}:
                return False
        elif str(actual) != expected:
            return False
    return True


def subset(rows: Sequence[Dict[str, Any]], rule: Rule) -> List[Dict[str, Any]]:
    return [row for row in rows if matches(row, rule)]


def _passes(stats: Dict[str, Any], *, validation: bool) -> bool:
    min_picks = MIN_VALIDATION_PICKS if validation else MIN_DISCOVERY_PICKS
    min_rate = MIN_VALIDATION_RATE if validation else MIN_DISCOVERY_RATE
    min_mean = MIN_VALIDATION_MEAN if validation else MIN_DISCOVERY_MEAN
    return (
        stats["picks"] >= min_picks
        and stats["positive_rate"] is not None
        and stats["positive_rate"] >= min_rate
        and stats["mean_settlement_even_odds"] is not None
        and stats["mean_settlement_even_odds"] >= min_mean
    )


def _serialise_rule(rule: Rule) -> List[Dict[str, str]]:
    return [
        {"field": field, "op": op, "value": value}
        for field, op, value in rule
    ]


def _rule_name(rule: Rule) -> str:
    return " | ".join(
        f"{field}{'~' if op == 'contains' else '='}{value}"
        for field, op, value in rule
    )


def discover(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    discovery_all, validation_all, final_all = chronological_parts(rows)
    discovery = [row for row in discovery_all if eligible(row)]
    validation = [row for row in validation_all if eligible(row)]
    final_test = [row for row in final_all if eligible(row)]

    candidate_set = {
        rule
        for row in discovery
        for rule in candidates_for(row)
    }
    passed = []
    for rule in candidate_set:
        discovery_stats = metric(subset(discovery, rule), "raw_ah_settlement")
        if not _passes(discovery_stats, validation=False):
            continue
        validation_stats = metric(subset(validation, rule), "raw_ah_settlement")
        if not _passes(validation_stats, validation=True):
            continue
        passed.append((rule, discovery_stats, validation_stats))

    passed.sort(
        key=lambda item: (
            -min(item[1]["positive_rate"], item[2]["positive_rate"]),
            -item[2]["picks"],
            len(item[0]),
            _rule_name(item[0]),
        )
    )

    selected = []
    covered_validation_ids = set()
    for rule, discovery_stats, validation_stats in passed:
        incremental = [
            row for row in validation
            if row["match_id"] not in covered_validation_ids and matches(row, rule)
        ]
        incremental_stats = metric(incremental, "raw_ah_settlement")
        if incremental_stats["picks"] < 8:
            continue
        if not (
            (incremental_stats["positive_rate"] or 0.0) >= MIN_VALIDATION_RATE
            and (incremental_stats["mean_settlement_even_odds"] or -1.0) >= MIN_VALIDATION_MEAN
        ):
            continue
        selected.append(
            {
                "id": f"EXP_AH_{len(selected) + 1:02d}",
                "name": _rule_name(rule),
                "conditions": _serialise_rule(rule),
                "discovery": discovery_stats,
                "validation": validation_stats,
                "validation_incremental": incremental_stats,
                "_rule": rule,
            }
        )
        covered_validation_ids.update(row["match_id"] for row in incremental)
        if len(selected) >= MAX_SELECTED_RULES:
            break

    def union(part: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        rules = [item["_rule"] for item in selected]
        return [row for row in part if any(matches(row, rule) for rule in rules)]

    selected_union = {
        "discovery": metric(union(discovery), "raw_ah_settlement"),
        "validation": metric(union(validation), "raw_ah_settlement"),
        "final_test": metric(union(final_test), "raw_ah_settlement"),
    }
    for item in selected:
        rule = item["_rule"]
        item["final_test"] = metric(subset(final_test, rule), "raw_ah_settlement")
        del item["_rule"]

    return {
        "method": {
            "split": "60% discovery / 20% validation / 20% untouched final test",
            "eligibility": "raw AH passes every V7 guard except exact-line whitelist",
            "candidate_origin": "discovery only",
            "selection_uses_final_test": False,
            "thresholds": {
                "discovery": {
                    "minimum_picks": MIN_DISCOVERY_PICKS,
                    "minimum_positive_rate": MIN_DISCOVERY_RATE,
                    "minimum_mean_settlement": MIN_DISCOVERY_MEAN,
                },
                "validation": {
                    "minimum_picks": MIN_VALIDATION_PICKS,
                    "minimum_positive_rate": MIN_VALIDATION_RATE,
                    "minimum_mean_settlement": MIN_VALIDATION_MEAN,
                },
            },
        },
        "periods": {
            "discovery": {
                "from": str(discovery_all[0]["date"]) if discovery_all else None,
                "to": str(discovery_all[-1]["date"]) if discovery_all else None,
                "all_rows": len(discovery_all),
                "eligible_rows": len(discovery),
            },
            "validation": {
                "from": str(validation_all[0]["date"]) if validation_all else None,
                "to": str(validation_all[-1]["date"]) if validation_all else None,
                "all_rows": len(validation_all),
                "eligible_rows": len(validation),
            },
            "final_test": {
                "from": str(final_all[0]["date"]) if final_all else None,
                "to": str(final_all[-1]["date"]) if final_all else None,
                "all_rows": len(final_all),
                "eligible_rows": len(final_test),
            },
        },
        "candidates_generated": len(candidate_set),
        "candidates_passing_discovery_and_validation": len(passed),
        "selected_rules": selected,
        "selected_union": selected_union,
    }


def render_markdown(report: Dict[str, Any]) -> str:
    lines = [
        "# Descubrimiento de reglas de expansión AH",
        "",
        "El tramo final no participa en la creación ni en la selección de reglas.",
        "",
        "## Periodos",
        "",
    ]
    for name, period in report["periods"].items():
        lines.append(
            f"- **{name}:** {period['from']} → {period['to']} | "
            f"{period['all_rows']} partidos | {period['eligible_rows']} candidatos"
        )
    lines.extend(
        [
            "",
            f"Candidatas generadas: **{report['candidates_generated']}**. ",
            f"Superan discovery+validation: **{report['candidates_passing_discovery_and_validation']}**.",
            "",
            "## Reglas seleccionadas",
            "",
        ]
    )
    if not report["selected_rules"]:
        lines.append("Ninguna regla superó los umbrales prefijados.")
    for rule in report["selected_rules"]:
        lines.append(f"### {rule['id']} — {rule['name']}")
        for period in ("discovery", "validation", "final_test"):
            stats = rule[period]
            lines.append(
                f"- {period}: n={stats['picks']}, acierto={stats['positive_rate']}%, "
                f"Wilson95={stats['wilson_lower_95']}%, media={stats['mean_settlement_even_odds']}"
            )
        lines.append("")
    lines.extend(["## Unión sin duplicados", ""])
    for period, stats in report["selected_union"].items():
        lines.append(
            f"- **{period}:** n={stats['picks']}, acierto={stats['positive_rate']}%, "
            f"Wilson95={stats['wilson_lower_95']}%, media={stats['mean_settlement_even_odds']}"
        )
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=PROJECT_ROOT / "data" / "app_data.db")
    parser.add_argument(
        "--output-json",
        type=Path,
        default=PROJECT_ROOT / "data" / "clave_dicotomica" / "EXPANSION_RULES_DISCOVERY.json",
    )
    parser.add_argument(
        "--output-md",
        type=Path,
        default=PROJECT_ROOT / "data" / "clave_dicotomica" / "EXPANSION_RULES_DISCOVERY.md",
    )
    args = parser.parse_args()
    rows, audit = load_backtest_rows(args.db)
    report = discover(rows)
    report["audit"] = audit
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    args.output_md.write_text(render_markdown(report), encoding="utf-8")
    print(json.dumps({
        "usable_rows": len(rows),
        "selected_rules": len(report["selected_rules"]),
        "selected_union": report["selected_union"],
        "output_json": str(args.output_json),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
