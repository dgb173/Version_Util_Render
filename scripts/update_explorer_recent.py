"""Fold newly finished precache analyses into the deployed Explorer archive."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.export_explorer_recent import compact_for_archive  # noqa: E402
sys.path.insert(0, str(ROOT / "src"))
from modules.data_manager import get_bucket_name  # noqa: E402


def _rows(path: Path):
    if not path.is_file():
        return []
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    return value if isinstance(value, list) else []


def _match_id(row):
    return str(row.get("match_id") or row.get("id") or "").strip()


def _score(row):
    value = str(row.get("final_score") or row.get("score") or "").strip()
    parts = value.replace("-", ":").split(":")
    return ":".join(parts) if len(parts) == 2 and all(part.isdigit() for part in parts) else None


def _date(value):
    text = str(value or "").strip()
    for pattern in ("%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y"):
        try:
            return dt.datetime.strptime(text[:10], pattern).date().isoformat()
        except ValueError:
            pass
    return ""


def update_archive(archive: Path, snapshot: Path, sources: list[Path], limit=2000):
    existing = {_match_id(row): row for row in _rows(archive) if isinstance(row, dict) and _match_id(row)}
    with snapshot.open("r", encoding="utf-8") as handle:
        main = json.load(handle)
    finished = {
        _match_id(row): row for row in main.get("finished_matches", [])
        if isinstance(row, dict) and _match_id(row) and _score(row)
    }
    added = 0
    for source in sources:
        for row in _rows(source):
            if not isinstance(row, dict):
                continue
            match_id = _match_id(row)
            final = finished.get(match_id)
            if not final or "h2h_col3" not in row:
                continue
            compact = compact_for_archive(row)
            compact["match_id"] = match_id
            compact["_explorer_bucket"] = get_bucket_name(
                compact.get("handicap") or (compact.get("main_match_odds") or {}).get("ah_linea")
            )
            compact["score"] = compact["final_score"] = _score(final)
            compact["match_date"] = compact["date"] = _date(
                final.get("match_date") or final.get("date") or row.get("match_date")
            )
            if not compact["match_date"]:
                continue
            if match_id not in existing:
                added += 1
            existing[match_id] = compact

    ordered = sorted(
        existing.values(),
        key=lambda row: (_date(row.get("match_date") or row.get("date")), _match_id(row)),
        reverse=True,
    )[:max(1, int(limit))]
    archive.parent.mkdir(parents=True, exist_ok=True)
    temporary = archive.with_suffix(archive.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(ordered, handle, ensure_ascii=False, separators=(",", ":"))
    temporary.replace(archive)
    print(f"Explorer archive: {len(ordered)} finished rows, {added} new")
    return added


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", type=Path, default=ROOT / "data/explorer_recent.json")
    parser.add_argument("--snapshot", type=Path, default=ROOT / "data.json")
    parser.add_argument("--source", type=Path, action="append")
    parser.add_argument("--limit", type=int, default=2000)
    args = parser.parse_args()
    update_archive(
        args.archive, args.snapshot,
        args.source or [ROOT / "data/data_precacheo.json", ROOT / "data/data_pending_results.json"],
        args.limit,
    )
