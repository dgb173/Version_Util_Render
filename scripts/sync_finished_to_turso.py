#!/usr/bin/env python
"""Sync the validated finished-cache artifacts to the Explorer database."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))


def collect_rows(results_dir: Path) -> list[dict]:
    from cloud_cache_pipeline import mid, quality_error

    paths = sorted(Path(results_dir).rglob("result_*.json"))
    if not paths:
        raise RuntimeError("No finished-cache result artifacts were found")

    rows: list[dict] = []
    seen: set[str] = set()
    for path in paths:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("kind") != "finished":
            raise ValueError(f"Unexpected cache kind in {path}")
        for row in payload.get("rows") or []:
            match_id = mid(row)
            error = quality_error(row, "finished")
            if error:
                raise ValueError(f"Invalid finished result {match_id}: {error}")
            if match_id in seen:
                raise ValueError(f"Duplicate finished result {match_id}")
            seen.add(match_id)
            rows.append(row)
    return rows


def sync_rows(rows: list[dict]) -> int:
    if not os.getenv("LIBSQL_URL", "").strip():
        raise RuntimeError("LIBSQL_URL is not configured")
    if not os.getenv("LIBSQL_AUTH_TOKEN", "").strip():
        raise RuntimeError("LIBSQL_AUTH_TOKEN is not configured")

    # GitHub Actions writes directly to Turso. It never downloads a local
    # replica or imports the large legacy JSON buckets.
    os.environ.setdefault("LIBSQL_REMOTE_ONLY", "1")
    os.environ.setdefault("SQL_BOOTSTRAP_MODE", "schema_only")
    os.environ.setdefault("DATA_LEGACY_SYNC", "0")

    from modules import data_manager, sql_store

    entries = [
        (
            row,
            data_manager.get_bucket_name((row.get("main_match_odds") or {}).get("ah_linea")),
            "historical",
        )
        for row in rows
    ]
    sql_store.upsert_matches(entries)
    return len(entries)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, default=Path("results"))
    args = parser.parse_args()

    rows = collect_rows(args.results)
    if not rows:
        print("No new finished matches to sync")
        return 0

    synced = sync_rows(rows)
    print(f"Synced {synced} finished matches to Turso")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
