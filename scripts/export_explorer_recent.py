"""Export a bounded historical Explorer archive for stateless deployments."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "data" / "app_data.db"
DEFAULT_OUTPUT = ROOT / "data" / "explorer_recent.json"


def compact_for_archive(row: dict) -> dict:
    compact = dict(row)
    for key in (
        "market_analysis_html", "historical_matches_html",
        "pre_match_context", "recent_home_matches_all",
        "recent_away_matches_all", "recent_home_matches_same_league_specific",
        "recent_away_matches_same_league_specific",
        "recent_home_matches_same_league_general",
        "recent_away_matches_same_league_general",
    ):
        compact.pop(key, None)
    return compact


def export_recent(db_path: Path, output: Path, limit: int = 2000) -> int:
    if not db_path.exists():
        raise FileNotFoundError(db_path)
    rows = []
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """SELECT match_id, bucket, COALESCE(explorer_json, payload_json)
               FROM matches
               WHERE state = 'historical'
                 AND score IS NOT NULL
                 AND score NOT IN ('?:?', '??')
               ORDER BY match_date DESC, updated_at DESC, match_id DESC
               LIMIT ?""",
            (max(1, limit),),
        )
        for match_id, bucket, raw in cursor:
            try:
                row = json.loads(raw)
            except (TypeError, ValueError):
                continue
            if not isinstance(row, dict):
                continue
            score = str(row.get("final_score") or row.get("score") or "")
            if not score or "?" in score:
                continue
            # Keep structured analysis and match statistics.
            row = compact_for_archive(row)
            row["match_id"] = str(match_id)
            row["_explorer_bucket"] = bucket
            rows.append(row)

    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(rows, handle, ensure_ascii=False, separators=(",", ":"))
    temporary.replace(output)
    print(f"Explorer archive: {len(rows)} historical matches -> {output}")
    return len(rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--limit", type=int, default=2000)
    args = parser.parse_args()
    export_recent(args.db, args.output, args.limit)
