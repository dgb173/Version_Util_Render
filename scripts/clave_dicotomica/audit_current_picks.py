#!/usr/bin/env python3
"""Count the currently publishable universal picks in precacheo."""

from __future__ import annotations

import json
import sqlite3
import sys
from collections import Counter
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from modules.clave_dicotomica import apply_key  # noqa: E402
from modules.housemind_ou import merge_match_records  # noqa: E402


def _dict(value):
    try:
        decoded = json.loads(value or "{}")
    except (TypeError, ValueError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def main() -> None:
    connection = sqlite3.connect(str(PROJECT_ROOT / "data" / "app_data.db"))
    connection.row_factory = sqlite3.Row
    selections = []
    counts = Counter()
    try:
        rows = connection.execute(
            """
            SELECT match_id, match_date, handicap, payload_json, explorer_json
            FROM matches WHERE state = 'precacheo' ORDER BY match_date, match_id
            """
        )
        for row in rows:
            counts["PRECACHEO_MATCHES"] += 1
            match = merge_match_records(
                _dict(row["explorer_json"]),
                _dict(row["payload_json"]),
                {
                    "match_id": str(row["match_id"]),
                    "match_date": row["match_date"],
                    "handicap": row["handicap"],
                },
            )
            result = apply_key(match)
            ah_tier = result.get("prediction_tier_ah")
            if ah_tier in {"PRODUCTION", "PRODUCTION_EXPANSION"}:
                counts["AH_TOTAL"] += 1
                counts[ah_tier] += 1
                selections.append(
                    {
                        "match_id": str(row["match_id"]),
                        "date": str(row["match_date"] or ""),
                        "home": match.get("home_name") or match.get("home_team") or "",
                        "away": match.get("away_name") or match.get("away_team") or "",
                        "market_line": (match.get("main_match_odds") or {}).get("ah_linea"),
                        "pick": result.get("ah_label"),
                        "tier": ah_tier,
                        "confidence": result.get("confidence_ah"),
                        "bookie_confirmation": result.get("bookie_confirmation"),
                        "expansion_rule": result.get("expansion_ah_rule"),
                    }
                )
            if result.get("prediction_tier_ou") == "PRODUCTION":
                counts["OU_TOTAL"] += 1
    finally:
        connection.close()

    output = {
        "precacheo_matches": counts["PRECACHEO_MATCHES"],
        "counts": dict(counts),
        "total_market_picks": counts["AH_TOTAL"] + counts["OU_TOTAL"],
        "expansion_selections": [
            selection for selection in selections
            if selection["tier"] == "PRODUCTION_EXPANSION"
        ],
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
