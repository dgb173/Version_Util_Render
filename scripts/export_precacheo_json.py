#!/usr/bin/env python
import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from modules import sql_store  # noqa: E402


def export_bucket(bucket: str) -> None:
    out_path = sql_store.export_bucket_to_json(bucket)
    count = len(sql_store.fetch_matches(bucket=bucket))
    print(f"OK: {bucket} -> {out_path} ({count} partidos)")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Exporta snapshots de pre-cacheo desde SQL a JSON."
    )
    parser.add_argument(
        "--include-pending",
        action="store_true",
        help="Tambien exporta data_pending_results.json",
    )
    args = parser.parse_args()

    buckets = ["data_precacheo.json"]
    if args.include_pending:
        buckets.append("data_pending_results.json")

    for bucket in buckets:
        try:
            export_bucket(bucket)
        except Exception as exc:
            print(f"ERROR exportando {bucket}: {exc}")
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
