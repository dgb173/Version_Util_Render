#!/usr/bin/env python
"""
Sanea y repara registros vacíos o mal clasificados en SQLite local (data/app_data.db).

Acciones que realiza:
1. Corrige 1.056 registros de data_precacheo_clean.json que estaban como 'historical'
   sin resultado (score ?:?), cambiándolos a 'precacheo' para que no ensucien el Explorador.
2. Elimina registros no válidos / de prueba (ej. 'EXP_AH_01' con handicap y score NULL).
3. Rellena las celdas 'match_date' vacías en partidos históricos usando la fecha de 'cached_at'.
4. Recupera el handicap de partidos en data_unknown.json cuyas cuotas sí estaban en el payload.

Uso:
  py scripts/limpiar_registros_vacios_sql.py --dry-run   # Modo simulación (no modifica)
  py scripts/limpiar_registros_vacios_sql.py             # Aplica los cambios
"""
from __future__ import annotations

import argparse
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

from modules import data_manager, sql_store  # noqa: E402


def sanear_base_datos(db_path: Path, dry_run: bool = False) -> dict[str, int]:
    conn = sqlite3.connect(db_path, timeout=30)
    cur = conn.cursor()

    stats = {
        "precacheo_clean_corregidos": 0,
        "registros_basura_eliminados": 0,
        "fechas_recuperadas": 0,
        "handicaps_recuperados": 0,
    }

    try:
        # 1. Corregir data_precacheo_clean.json con state='historical' y score vacío
        cur.execute("""
            SELECT count(*) FROM matches
            WHERE bucket = 'data_precacheo_clean.json' AND state = 'historical'
        """)
        clean_count = cur.fetchone()[0]
        stats["precacheo_clean_corregidos"] = clean_count
        if clean_count > 0:
            print(f"[1] data_precacheo_clean.json: {clean_count} partidos marcados como 'historical' sin score.")
            if not dry_run:
                cur.execute("""
                    UPDATE matches
                    SET state = 'precacheo'
                    WHERE bucket = 'data_precacheo_clean.json' AND state = 'historical'
                """)
                print(f"    -> Cambiados a state='precacheo' (ya no aparecerán como vacíos en el Explorador).")

        # 2. Eliminar registros de prueba / falsos
        cur.execute("""
            SELECT match_id FROM matches
            WHERE match_id LIKE 'EXP_%' OR match_id LIKE 'TEST_%'
               OR (handicap IS NULL AND score IS NULL AND match_date IS NULL AND bucket = 'data_pending_results.json')
        """)
        bad_ids = [r[0] for r in cur.fetchall()]
        stats["registros_basura_eliminados"] = len(bad_ids)
        if bad_ids:
            print(f"\n[2] Registros no-partido / pruebas encontrados: {bad_ids}")
            if not dry_run:
                for bid in bad_ids:
                    cur.execute("DELETE FROM matches WHERE match_id = ?", (bid,))
                print(f"    -> Eliminados {len(bad_ids)} registros no válidos.")

        # 3. Recuperar match_date vacíos usando cached_at
        cur.execute("""
            SELECT match_id, payload_json FROM matches
            WHERE match_date IS NULL OR trim(match_date) = ''
        """)
        rows_no_date = cur.fetchall()
        date_updates = []
        for mid, p_raw in rows_no_date:
            if not p_raw:
                continue
            try:
                p = json.loads(p_raw)
                cached_at = p.get("cached_at") or ""
                # Si cached_at tiene formato 'YYYY-MM-DD HH:MM:SS'
                if len(cached_at) >= 10 and "-" in cached_at[:10]:
                    date_val = cached_at[:10]
                    date_updates.append((date_val, mid))
            except Exception:
                continue

        stats["fechas_recuperadas"] = len(date_updates)
        if date_updates:
            print(f"\n[3] Fechas vacías: {len(rows_no_date)} encontradas, {len(date_updates)} recuperables desde cached_at.")
            if not dry_run:
                cur.executemany("UPDATE matches SET match_date = ? WHERE match_id = ?", date_updates)
                print(f"    -> Actualizadas {len(date_updates)} fechas vacías.")

        # 4. Recuperar handicap de data_unknown.json si existe en payload
        cur.execute("""
            SELECT match_id, payload_json FROM matches
            WHERE bucket = 'data_unknown.json' AND handicap IS NULL
        """)
        unknown_rows = cur.fetchall()
        ah_updates = []
        for mid, p_raw in unknown_rows:
            if not p_raw:
                continue
            try:
                p = json.loads(p_raw)
                odds = p.get("main_match_odds") or {}
                ah_str = p.get("handicap") or odds.get("ah_linea")
                if ah_str is not None and str(ah_str).strip() not in ("", "N/A", "null", "None"):
                    ah_val = float(ah_str)
                    bucket_name = data_manager.get_bucket_name(ah_val)
                    ah_updates.append((ah_val, bucket_name, mid))
            except Exception:
                continue

        stats["handicaps_recuperados"] = len(ah_updates)
        if ah_updates:
            print(f"\n[4] data_unknown.json sin handicap: {len(unknown_rows)} encontrados, {len(ah_updates)} recuperables del payload.")
            if not dry_run:
                cur.executemany("UPDATE matches SET handicap = ?, bucket = ? WHERE match_id = ?", ah_updates)
                print(f"    -> Recuperados {len(ah_updates)} handicaps y reasignados a sus buckets correctos.")

        if not dry_run:
            conn.commit()
            print("\n[OK] Cambios guardados correctamente en SQLite local.")
        else:
            print("\n[DRY-RUN] Simulación completa. No se ha modificado la base de datos.")

    finally:
        conn.close()

    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Simula sin modificar la base de datos")
    args = parser.parse_args()

    db_path = sql_store.DB_PATH
    print("=" * 65)
    print("  SANEAMIENTO DE REGISTROS VACÍOS EN SQLITE LOCAL")
    print(f"  Base de datos: {db_path}")
    print("=" * 65)

    stats = sanear_base_datos(db_path, dry_run=args.dry_run)
    print("\nResumen:")
    for k, v in stats.items():
        print(f"  - {k}: {v}")
    print("=" * 65)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
