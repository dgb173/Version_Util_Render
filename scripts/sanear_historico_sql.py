#!/usr/bin/env python
"""
scripts/sanear_historico_sql.py

Sanea la base de datos SQLite (data/app_data.db) para eliminar las filas
rotas o incompletas (celdas rosas 'cell-no-data' en el Explorador):

1. Reclasifica los partidos que están en state = 'historical' pero que carecen
   de análisis H2H profundo (sin last_home_match o last_away_match) a
   state = 'raw_result'. De esta forma se conservan en la base de datos pero
   no ensucian el Explorador ni generan celdas vacías.
2. Recupera las fechas faltantes en los registros históricos completos usando
   la columna match_date, cached_at, start_time o la fecha del último precedente.
3. Sincroniza payload_json y explorer_json para que la fecha siempre viaje al frontend.
"""
import json
import os
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT))

DB_PATH = PROJECT_ROOT / "data" / "app_data.db"


def _normalize_iso_date(value: str) -> str | None:
    if not value or value in ("N/A", "-", "?", "None", "null"):
        return None
    s = str(value).strip()
    if not s:
        return None
    if " " in s:
        s = s.split(" ")[0]
    if "T" in s:
        s = s.split("T")[0]
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s
    if "/" in s:
        parts = s.split("/")
        if len(parts) == 3:
            p0, p1, p2 = parts
            if len(p2) == 4:
                # MM/DD/YYYY or DD/MM/YYYY -> standard MM/DD/YYYY
                month, day, year = int(p0), int(p1), int(p2)
                if month > 12 >= day:
                    month, day = day, month
                return f"{year:04d}-{month:02d}-{day:02d}"
    return s[:10] if len(s) >= 10 else None


def sanear_base_datos(dry_run: bool = False):
    if not DB_PATH.exists():
        print(f"[ERROR] No se encontró la base de datos en {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()

        print("=" * 60)
        print("SANEAMIENTO DE REGISTROS HISTÓRICOS Y CELDAS VACÍAS")
        print("=" * 60)

        # ---------------------------------------------------------
        # PASO 1: Reclasificar registros sin H2H a 'raw_result'
        # ---------------------------------------------------------
        cur.execute(
            """
            SELECT match_id, bucket, payload_json
            FROM matches
            WHERE state = 'historical'
            """
        )
        all_hist = cur.fetchall()
        print(f"Total de registros con state = 'historical': {len(all_hist)}")

        sin_h2h = []
        for row in all_hist:
            mid = row["match_id"]
            try:
                data = json.loads(row["payload_json"])
            except Exception:
                sin_h2h.append(mid)
                continue

            lhm = data.get("last_home_match")
            lam = data.get("last_away_match")

            # Es incompleto si no tiene lhm o lam como diccionarios válidos
            if not isinstance(lhm, dict) or not lhm or not isinstance(lam, dict) or not lam:
                sin_h2h.append(mid)

        print(f"-> Registros incompletos (sin H2H / raw): {len(sin_h2h)}")
        print(f"-> Registros completos para Explorador: {len(all_hist) - len(sin_h2h)}")

        if not dry_run and sin_h2h:
            # Reclasificar en lotes
            for i in range(0, len(sin_h2h), 500):
                batch = sin_h2h[i : i + 500]
                placeholders = ",".join("?" * len(batch))
                conn.execute(
                    f"UPDATE matches SET state = 'raw_result' WHERE match_id IN ({placeholders})",
                    batch,
                )
            conn.commit()
            print(f"  [OK] Reclasificados {len(sin_h2h)} partidos a state = 'raw_result'.")

        # ---------------------------------------------------------
        # PASO 2: Recuperar fechas faltantes en los registros completos
        # ---------------------------------------------------------
        cur.execute(
            """
            SELECT match_id, match_date, payload_json, explorer_json
            FROM matches
            WHERE state = 'historical'
            """
        )
        hist_valid = cur.fetchall()
        print(f"\nRevisando fechas en {len(hist_valid)} partidos históricos activos...")

        actualizados_fecha = 0
        for row in hist_valid:
            mid = row["match_id"]
            col_date = row["match_date"]
            try:
                data = json.loads(row["payload_json"])
            except Exception:
                continue

            # Extraer fecha existente o deducir
            payload_date = data.get("match_date") or data.get("date")
            resolved_date = _normalize_iso_date(col_date) or _normalize_iso_date(payload_date)

            if not resolved_date:
                # Buscar en campos auxiliares
                for candidate in (
                    data.get("time_obj"),
                    data.get("start_time"),
                    data.get("cached_at"),
                    (data.get("last_away_match") or {}).get("date"),
                    (data.get("last_home_match") or {}).get("date"),
                ):
                    if candidate:
                        resolved_date = _normalize_iso_date(candidate)
                        if resolved_date:
                            break

            if not resolved_date:
                continue

            # Verificar si necesita actualización
            needs_update = False
            if col_date != resolved_date:
                needs_update = True

            if data.get("match_date") != resolved_date or data.get("date") != resolved_date:
                data["match_date"] = resolved_date
                data["date"] = resolved_date
                needs_update = True

            if needs_update and not dry_run:
                new_payload = json.dumps(data, ensure_ascii=False)
                # Actualizar también explorer_json si existe
                exp_json_str = None
                if row["explorer_json"]:
                    try:
                        exp_data = json.loads(row["explorer_json"])
                        exp_data["match_date"] = resolved_date
                        exp_data["date"] = resolved_date
                        exp_json_str = json.dumps(exp_data, ensure_ascii=False)
                    except Exception:
                        pass

                if exp_json_str:
                    conn.execute(
                        "UPDATE matches SET match_date = ?, payload_json = ?, explorer_json = ? WHERE match_id = ?",
                        (resolved_date, new_payload, exp_json_str, mid),
                    )
                else:
                    conn.execute(
                        "UPDATE matches SET match_date = ?, payload_json = ? WHERE match_id = ?",
                        (resolved_date, new_payload, mid),
                    )
                actualizados_fecha += 1

        if not dry_run:
            conn.commit()
            print(f"  [OK] Fechas normalizadas e inyectadas en {actualizados_fecha} partidos.")
        else:
            print(f"  [DRY-RUN] Se habrían normalizado {actualizados_fecha} fechas.")

        # Limpiar caché en memoria si data_manager está cargado
        try:
            from modules import data_manager
            data_manager._clear_explorer_cache()
            print("  [OK] Caché en memoria del explorador reiniciada.")
        except Exception:
            pass

        print("\n" + "=" * 60)
        print("SANEAMIENTO COMPLETADO EXITOSAMENTE")
        print("=" * 60)

    finally:
        conn.close()


if __name__ == "__main__":
    dry = "--dry-run" in sys.argv
    sanear_base_datos(dry_run=dry)
