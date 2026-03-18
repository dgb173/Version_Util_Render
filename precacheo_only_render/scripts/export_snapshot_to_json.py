#!/usr/bin/env python
import argparse
import json
import sqlite3
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "app_data.db"
DEFAULT_OUTPUTS = [PROJECT_ROOT / "data.json", PROJECT_ROOT / "data" / "data.json"]
DEFAULT_CACHE_KEY = "app_main_page_cache_v1"


def _load_snapshot(db_path: Path, cache_key: str):
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT value FROM kv_store WHERE key = ?",
            (cache_key,),
        ).fetchone()
    finally:
        conn.close()

    if not row or not row[0]:
        return None

    payload = json.loads(row[0])
    if isinstance(payload, str):
        payload = json.loads(payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Exporta el snapshot SQL (upcoming/finished) a data.json compatible."
    )
    parser.add_argument(
        "--db",
        default=str(DEFAULT_DB_PATH),
        help=f"Ruta DB SQLite (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--key",
        default=DEFAULT_CACHE_KEY,
        help=f"Clave en kv_store (default: {DEFAULT_CACHE_KEY})",
    )
    parser.add_argument(
        "--output",
        action="append",
        default=[],
        help="Ruta de salida JSON (repetible). Si se omite usa data.json y data/data.json",
    )
    args = parser.parse_args()

    db_path = Path(args.db).resolve()
    outputs = [Path(p).resolve() for p in args.output] if args.output else DEFAULT_OUTPUTS

    if not db_path.exists():
        print(f"ERROR: DB no existe: {db_path}")
        return 2

    snapshot = _load_snapshot(db_path, args.key)
    if snapshot is None:
        print(f"ERROR: No hay snapshot para key '{args.key}' en {db_path}")
        return 3

    if not isinstance(snapshot, dict):
        print("ERROR: Snapshot inválido (no es dict).")
        return 4

    upcoming = snapshot.get("upcoming_matches", [])
    finished = snapshot.get("finished_matches", [])

    for out in outputs:
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("w", encoding="utf-8") as fh:
            json.dump(snapshot, fh, ensure_ascii=False, indent=2)
        print(f"OK: {out}")

    print(f"Snapshot exportado: upcoming={len(upcoming)} finished={len(finished)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

