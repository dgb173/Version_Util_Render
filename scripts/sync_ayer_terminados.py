#!/usr/bin/env python
"""
scripts/sync_ayer_terminados.py

Actualiza de forma inmediata y masiva los marcadores de los partidos de ayer
utilizando el feed directo de resultados de NowGoal (/gf/data/finish/bf_en-idn.js).
Para los partidos que ya están pre-cacheados en SQLite, asigna el marcador final
y los pasa a 'historical' en el bucket correspondiente en cuestión de segundos.
"""
import datetime as dt
import json
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from modules import data_manager, nowgoal_fetcher, sql_store


def sync_ayer(export_json: bool = True):
    print("=" * 65)
    print("  ACTUALIZACIÓN DIRECTA DE RESULTADOS DE AYER (NOWGOAL -> SQLITE)")
    print("=" * 65)

    print("\n[1/3] Descargando feed de resultados terminados desde NowGoal...")
    content, odds = nowgoal_fetcher.fetch_finished_data_raw()
    if not content:
        print("[ERROR] No se pudo obtener el feed de resultados de NowGoal.")
        return 0

    matches = nowgoal_fetcher.parse_matches_from_bf_content(
        content,
        status_filter="finished",
        require_handicap=True,
        require_goal_line=False,
        odds_by_match=odds,
    )
    print(f"  Resultados analizados en NowGoal con hándicap: {len(matches)}")

    finished_scores = {}
    for m in matches:
        mid = str(m.get("id") or m.get("match_id") or "").strip()
        score = m.get("score") or m.get("final_score")
        if mid and score and score not in ("-:-", "??", "None"):
            finished_scores[mid] = score

    print("\n[2/3] Cruzando con la base de datos local (data/app_data.db)...")
    sql_store.ensure_bootstrap()
    conn = sqlite3.connect(sql_store.DB_PATH, timeout=60)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute(
        "SELECT match_id, bucket, state, handicap, score, payload_json FROM matches WHERE state IN ('precacheo', 'pending_results')"
    )
    pending_rows = cur.fetchall()

    to_upgrade = []
    for r in pending_rows:
        mid = str(r["match_id"])
        if mid in finished_scores:
            to_upgrade.append((r, finished_scores[mid]))

    print(f"  Partidos en precacheo/pendientes que ya finalizaron: {len(to_upgrade)}")

    if not to_upgrade:
        print("  [OK] No hay partidos pendientes de marcador en tu base de datos.")
        conn.close()
        return 0

    print(f"\n[3/3] Actualizando {len(to_upgrade)} partidos a 'historical' con marcador final...")
    changed_buckets = set()
    count = 0

    for row, final_score in to_upgrade:
        try:
            m_dict = json.loads(row["payload_json"])
            m_dict["score"] = final_score
            m_dict["final_score"] = final_score
            m_dict["match_id"] = str(row["match_id"])
            m_dict["status"] = -1

            ah = m_dict.get("handicap")
            if ah is None:
                ah = (m_dict.get("main_match_odds") or {}).get("ah_linea")

            target_bucket = data_manager.get_bucket_name(ah)
            if not target_bucket:
                target_bucket = "data_others.json"

            sql_store.upsert_match(
                m_dict,
                bucket=target_bucket,
                state="historical",
            )
            changed_buckets.add(target_bucket)
            count += 1
            if count % 100 == 0:
                print(f"    Procesados {count}/{len(to_upgrade)}...", flush=True)
        except Exception as exc:
            print(f"  [AVISO] Error al procesar ID {row['match_id']}: {exc}")

    conn.close()
    data_manager._clear_explorer_cache()
    print(f"  [ÉXITO] {count} partidos pasados a terminados ('historical') con éxito.")

    if export_json and changed_buckets:
        print(f"\nExportando {len(changed_buckets)} archivos JSON de buckets actualizados...")
        for b in sorted(changed_buckets):
            try:
                sql_store.export_bucket_to_json(b)
                print(f"  ✓ {b} actualizado en data/")
            except Exception as e:
                print(f"  ✗ Error exportando {b}: {e}")

    print("\n" + "=" * 65)
    print(f"  PROCESO COMPLETADO: {count} partidos de ayer listos en el Explorador.")
    print("=" * 65)
    return count


if __name__ == "__main__":
    export = "--no-export" not in sys.argv
    sync_ayer(export_json=export)
