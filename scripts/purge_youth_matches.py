#!/usr/bin/env python
"""Purga partidos y ligas juveniles/no apostables de la base de datos y archivos JSON."""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from modules.youth_filter import is_unbettable_youth_match, filter_youth_matches
from modules import sql_store


def purge_db_matches() -> int:
    db_path = PROJECT_ROOT / "data" / "app_data.db"
    if not db_path.exists():
        print(f"Base de datos no encontrada: {db_path}")
        return 0

    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute(
        "SELECT match_id, bucket, payload_json FROM matches WHERE bucket IN ('data_precacheo.json', 'data_pending_results.json')"
    )
    rows = cursor.fetchall()
    
    to_delete = []
    for mid, bucket, p_json in rows:
        try:
            payload = json.loads(p_json) if p_json else {}
        except Exception:
            payload = {}
        if is_unbettable_youth_match(payload):
            to_delete.append((bucket, str(mid)))

    if to_delete:
        cursor.executemany(
            "DELETE FROM matches WHERE bucket = ? AND match_id = ?",
            to_delete,
        )
        conn.commit()
    conn.close()
    print(f"[DB] Eliminados {len(to_delete)} partidos juveniles de matches (precacheo/pendientes).")
    return len(to_delete)


def purge_kv_snapshot() -> None:
    snapshot = sql_store.get_json_state("app_main_page_cache_v1", default=None)
    if isinstance(snapshot, dict):
        upcoming = snapshot.get("upcoming_matches", [])
        finished = snapshot.get("finished_matches", [])
        clean_up = filter_youth_matches(upcoming)
        clean_fin = filter_youth_matches(finished)
        diff_up = len(upcoming) - len(clean_up)
        diff_fin = len(finished) - len(clean_fin)
        snapshot["upcoming_matches"] = clean_up
        snapshot["finished_matches"] = clean_fin
        sql_store.set_json_state("app_main_page_cache_v1", snapshot)
        print(f"[KV_STORE] Snapshot purgado: {diff_up} próximos y {diff_fin} finalizados eliminados.")


def purge_json_file(file_path: Path) -> int:
    if not file_path.exists():
        return 0
    try:
        data = json.loads(file_path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"Error leyendo {file_path}: {exc}")
        return 0

    if isinstance(data, list):
        clean = filter_youth_matches(data)
        diff = len(data) - len(clean)
        if diff > 0:
            file_path.write_text(json.dumps(clean, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[{file_path.name}] Purgados {diff} partidos juveniles. Quedan {len(clean)}.")
        return diff
    elif isinstance(data, dict):
        diff_total = 0
        for k in ("upcoming_matches", "finished_matches", "matches"):
            if k in data and isinstance(data[k], list):
                original_len = len(data[k])
                data[k] = filter_youth_matches(data[k])
                diff_total += original_len - len(data[k])
        if diff_total > 0:
            file_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[{file_path.name}] Purgados {diff_total} partidos juveniles.")
        return diff_total
    return 0


def main():
    print("--- INICIANDO PURGA DE LIGAS Y PARTIDOS JUVENILES ---")
    purge_db_matches()
    purge_kv_snapshot()

    files = [
        PROJECT_ROOT / "data" / "data_precacheo.json",
        PROJECT_ROOT / "data" / "data_pending_results.json",
        PROJECT_ROOT / "data.json",
        PROJECT_ROOT / "data" / "data.json",
    ]
    for f in files:
        purge_json_file(f)

    # Regenerar fast store
    try:
        from build_precache_fast_store import main as build_fast_store
        build_fast_store()
        print("[FAST STORE] Reconstruido exitosamente sin partidos juveniles.")
    except Exception as exc:
        print(f"[AVISO] No se pudo reconstruir fast_store: {exc}")

    print("--- PURGA FINALIZADA CON ÉXITO ---")


if __name__ == "__main__":
    main()
