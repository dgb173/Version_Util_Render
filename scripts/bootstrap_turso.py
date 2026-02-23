#!/usr/bin/env python
import os
import sys
from pathlib import Path


def main() -> int:
    project_root = Path(__file__).resolve().parent.parent
    src_dir = project_root / "src"
    sys.path.insert(0, str(src_dir))

    libsql_url = os.getenv("LIBSQL_URL", "").strip()
    if not libsql_url:
        print("ERROR: falta LIBSQL_URL")
        return 1

    if not os.getenv("LIBSQL_AUTH_TOKEN", "").strip():
        print("ERROR: falta LIBSQL_AUTH_TOKEN")
        return 1

    # Force full import only for this one-off bootstrap.
    os.environ["SQL_BOOTSTRAP_MODE"] = "full"

    from modules import data_manager, sql_store  # noqa: E402

    print("== Turso bootstrap ==")
    print(f"LIBSQL_URL: {libsql_url}")
    print(f"DB local replica: {sql_store.get_db_path()}")
    print(f"Modo bootstrap: {os.getenv('SQL_BOOTSTRAP_MODE')}")

    data_manager.import_legacy_json_to_sql(reset_first=True)
    synced = sql_store.sync_replica()

    buckets = sql_store.fetch_distinct_buckets()
    total = len(sql_store.fetch_all_matches())

    print(f"Buckets: {len(buckets)}")
    print(f"Matches totales: {total}")
    print(f"Sync ejecutado: {synced}")
    print("Bootstrap completado.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
