#!/usr/bin/env python
"""Stream the legacy finished buckets into Turso without loading them in RAM."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
from typing import Iterator, Tuple

import ijson

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

HISTORICAL_BUCKETS = (
    "data_ah_0.json",
    "data_ah_0.5.json",
    "data_ah_1.5.json",
    "data_ah_2_plus.json",
    "data_minus_ah_0.5.json",
    "data_minus_ah_1.5.json",
    "data_minus_ah_2_plus.json",
    "data_unknown.json",
    "data_others.json",
    "data_cloud_league.json",
)


def iter_rows(data_dir: Path) -> Iterator[Tuple[dict, str]]:
    for bucket in HISTORICAL_BUCKETS:
        path = Path(data_dir) / bucket
        if not path.exists():
            continue
        with path.open("rb") as handle:
            # ``use_float=True`` prevents ijson from returning Decimal values,
            # which the standard JSON encoder used by sql_store cannot encode.
            for row in ijson.items(handle, "item", use_float=True):
                if not isinstance(row, dict):
                    continue
                match_id = str(row.get("match_id") or row.get("id") or "").strip()
                if not match_id:
                    continue
                row["match_id"] = match_id
                yield row, bucket


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, default=ROOT / "data")
    parser.add_argument("--batch-size", type=int, default=25)
    args = parser.parse_args()

    if not os.getenv("LIBSQL_URL", "").strip():
        raise RuntimeError("LIBSQL_URL is not configured")
    if not os.getenv("LIBSQL_AUTH_TOKEN", "").strip():
        raise RuntimeError("LIBSQL_AUTH_TOKEN is not configured")
    if not 1 <= args.batch_size <= 200:
        raise ValueError("batch-size must be between 1 and 200")

    os.environ.setdefault("LIBSQL_REMOTE_ONLY", "1")
    os.environ.setdefault("SQL_BOOTSTRAP_MODE", "schema_only")
    os.environ.setdefault("DATA_LEGACY_SYNC", "0")

    from modules import sql_store

    synced = 0
    batch = []
    for row, bucket in iter_rows(args.data):
        batch.append((row, bucket, "historical"))
        if len(batch) >= args.batch_size:
            sql_store.upsert_matches(batch)
            synced += len(batch)
            batch.clear()
            print(f"Synced {synced} historical matches", flush=True)

    if batch:
        sql_store.upsert_matches(batch)
        synced += len(batch)

    print(f"Backfill complete: {synced} historical matches available in Explorer")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
