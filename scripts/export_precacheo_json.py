#!/usr/bin/env python
import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from modules import sql_store  # noqa: E402


def export_bucket(bucket: str) -> None:
    out_path = sql_store.export_bucket_to_json(bucket)
    with out_path.open("r", encoding="utf-8") as fh:
        exported = json.load(fh)
    count = len(exported) if isinstance(exported, list) else 0
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

    try:
        from build_precache_fast_store import main as build_fast_store
        build_fast_store()
    except Exception as exc:
        print(f"[AVISO] No se pudo regenerar precacheo_fast_store: {exc}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
