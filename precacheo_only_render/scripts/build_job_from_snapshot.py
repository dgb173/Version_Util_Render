import argparse
import json
import sqlite3
import sys
from pathlib import Path


def build_jobs(db_path: Path, cache_key: str, out_path: Path) -> int:
    if not db_path.exists():
        print(f"ERROR: No existe base SQL: {db_path}")
        return 2

    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT value FROM kv_store WHERE key = ?",
            (cache_key,),
        ).fetchone()
    finally:
        conn.close()

    if not row or not row[0]:
        print("ERROR: No hay snapshot de partidos en SQL. Ejecuta primero generar data.")
        return 3

    payload = json.loads(row[0])
    if isinstance(payload, str):
        payload = json.loads(payload)

    upcoming = payload.get("upcoming_matches", []) if isinstance(payload, dict) else []
    jobs = []
    seen = set()

    for match in upcoming:
        if not isinstance(match, dict):
            continue

        mid = match.get("id") or match.get("match_id")
        if mid is None:
            continue

        mid = str(mid).strip()
        if not mid or mid in seen:
            continue

        seen.add(mid)
        jobs.append(
            {
                "id": mid,
                "ah": str(match.get("handicap", "N/A")),
                "season": "json_snapshot",
                "league_id": "json_snapshot",
            }
        )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as fh:
        json.dump(jobs, fh, ensure_ascii=False, indent=2)

    print(f"Partidos exportados a JSON: {len(jobs)} -> {out_path}")
    if not jobs:
        return 4
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Genera temp_matches_job.json desde snapshot SQL.")
    parser.add_argument("--db", default="data/app_data.db", help="Ruta de la base SQLite")
    parser.add_argument("--cache-key", default="app_main_page_cache_v1", help="Clave en kv_store")
    parser.add_argument("--out", default="temp_matches_job.json", help="Ruta del JSON de salida")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return build_jobs(
        db_path=Path(args.db),
        cache_key=args.cache_key,
        out_path=Path(args.out),
    )


if __name__ == "__main__":
    sys.exit(main())
