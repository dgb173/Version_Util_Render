#!/usr/bin/env python
"""
scripts/import_terminados_local.py

Importa a SQLite local (data/app_data.db) EXCLUSIVAMENTE los partidos terminados
nuevos que el bot de GitHub Actions acaba de cachear en los archivos JSON
(data/data_ah_*.json, data/data_minus_ah_*.json, etc.) tras un 'git pull'.

NO conecta a Turso ni descarga registros masivos de la nube.
Solo busca partidos nuevos o con resultado actualizado en los archivos JSON locales
y exige que tengan análisis H2H completo (validate_explorer_match).
"""
from __future__ import annotations

import argparse
import gzip
import json
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from modules import data_manager, sql_store  # noqa: E402

DATA_DIR = PROJECT_ROOT / "data"
ARCHIVE_DIR = DATA_DIR / "cache_archive" / "finished"

AH_BUCKET_FILES = [
    "data_ah_0.json",
    "data_ah_0.5.json",
    "data_ah_1.5.json",
    "data_ah_2_plus.json",
    "data_minus_ah_0.5.json",
    "data_minus_ah_1.5.json",
    "data_minus_ah_2_plus.json",
    "data_others.json",
]


def _get_git_modified_json_files() -> List[Path]:
    """Detecta qué archivos JSON de datos fueron modificados en el último git pull."""
    try:
        res = subprocess.run(
            ["git", "diff", "HEAD@{1}", "HEAD", "--name-only", "data/"],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            check=False,
        )
        if res.returncode == 0 and res.stdout.strip():
            modified = []
            for line in res.stdout.strip().splitlines():
                p = PROJECT_ROOT / line.strip()
                if p.is_file() and (
                    (p.name.endswith(".json") and p.name.startswith("data_"))
                    or (ARCHIVE_DIR in p.parents and p.name.endswith((".json", ".json.gz")))
                ):
                    modified.append(p)
            if modified:
                return modified
    except Exception:
        pass

    # Si git diff falla o no hay reflog, buscar por fecha de modificación reciente (< 24 horas)
    recent_files = []
    now = time.time()
    for fname in AH_BUCKET_FILES:
        p = DATA_DIR / fname
        if p.is_file() and (now - p.stat().st_mtime) < 86400:
            recent_files.append(p)
    return recent_files or [DATA_DIR / f for f in AH_BUCKET_FILES if (DATA_DIR / f).is_file()]


def sync_terminados_from_json(dry_run: bool = False, full_scan: bool = False) -> int:
    """
    Lee los archivos JSON locales actualizados por el bot y extrae
    exclusivamente los partidos terminados nuevos o actualizados.
    """
    print("\n" + "=" * 60)
    print("  SINCRONIZANDO PARTIDOS TERMINADOS NUEVOS DEL BOT A SQLITE")
    print("=" * 60)

    sql_store.ensure_bootstrap()
    db_path = sql_store.DB_PATH

    # 1. Cargar el mapa de partidos existentes en SQLite local
    print("\n[1/3] Consultando partidos existentes en SQLite local...")
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        cur = conn.cursor()
        cur.execute("SELECT match_id, score, state FROM matches")
        local_matches = {str(r[0]): {"score": str(r[1] or ""), "state": str(r[2] or "")} for r in cur.fetchall()}
    finally:
        conn.close()

    print(f"  Partidos totales en SQLite local: {len(local_matches):,}")

    # 2. Determinar qué archivos JSON inspeccionar
    if full_scan:
        target_files = [DATA_DIR / f for f in AH_BUCKET_FILES if (DATA_DIR / f).is_file()]
        target_files += sorted(ARCHIVE_DIR.glob("*.json"))
        target_files += sorted(ARCHIVE_DIR.glob("*.json.gz"))
        print(f"\n[2/3] Modo escaneo completo de buckets ({len(target_files)} archivos)...")
    else:
        target_files = _get_git_modified_json_files()
        print(f"\n[2/3] Inspeccionando archivos JSON modificados por el bot ({len(target_files)} archivos):")
        for f in target_files:
            print(f"    - {f.name} ({f.stat().st_size / (1024*1024):.1f} MB)")

    # 3. Extraer partidos nuevos o actualizados que tengan H2H válido
    to_insert: List[Tuple[Dict[str, Any], str]] = []
    seen_ids: Set[str] = set()

    for file_path in target_files:
        if not file_path.is_file():
            continue

        is_archive = ARCHIVE_DIR in file_path.parents
        bucket_name = file_path.name
        try:
            opener = gzip.open if file_path.suffix == ".gz" else open
            with opener(file_path, "rt", encoding="utf-8") as fh:
                items = json.load(fh)
        except Exception as exc:
            print(f"  [ERROR] No se pudo leer {file_path.name}: {exc}")
            continue

        if is_archive and isinstance(items, dict):
            items = [items]
        if not isinstance(items, list):
            continue

        for m in items:
            if not isinstance(m, dict):
                continue

            mid = str(m.get("match_id") or m.get("id") or "").strip()
            if not mid or mid in seen_ids:
                continue

            # Exigir que sea un partido terminado con análisis H2H completo
            if not data_manager.validate_explorer_match(m):
                continue

            m["match_id"] = mid
            if is_archive:
                ah = m.get("handicap")
                if ah is None:
                    ah = (m.get("main_match_odds") or {}).get("ah_linea")
                bucket_name = data_manager.get_bucket_name(ah)
            new_score = str(m.get("score") or m.get("final_score") or "").strip()

            # Caso A: Partido no existe en SQLite local
            if mid not in local_matches:
                seen_ids.add(mid)
                to_insert.append((m, bucket_name))
            # Caso B: Partido existía pero estaba pendiente o sin marcador definitivo
            elif local_matches[mid]["state"] in ("precacheo", "pending_results") or local_matches[mid]["score"] in ("?:?", "??", "", "None"):
                seen_ids.add(mid)
                to_insert.append((m, bucket_name))

    print(f"\n[3/3] Partidos terminados nuevos o actualizados detectados: {len(to_insert)}")

    if not to_insert:
        print("  [OK] No hay partidos terminados nuevos pendientes de incorporar.")
        return 0

    if dry_run:
        print("  [DRY-RUN] No se guardan cambios en SQLite.")
        return len(to_insert)

    # 4. Insertar en SQLite local en lotes rápidos
    print(f"  Guardando {len(to_insert)} partidos en tu base SQLite local...")
    conn = sqlite3.connect(db_path, timeout=60)
    conn.row_factory = sqlite3.Row
    try:
        inserted = 0
        with conn:
            for match, bucket in to_insert:
                sql_store._upsert_match(conn, match, bucket=bucket, state="historical")
                inserted += 1
                if inserted % 200 == 0:
                    print(f"    Insertados {inserted}/{len(to_insert)}...", flush=True)

        print(f"  [EXITO] {inserted} partidos terminados incorporados a SQLite correctamente.")
    finally:
        conn.close()

    # Limpiar caché de explorador en memoria
    data_manager._clear_explorer_cache()
    return len(to_insert)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Importa partidos terminados del bot de GitHub a SQLite local."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simula la detección sin escribir en SQLite.",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Escanea todos los archivos JSON de data/ en vez de solo los modificados.",
    )
    args = parser.parse_args()

    count = sync_terminados_from_json(dry_run=args.dry_run, full_scan=args.full)
    print("\nProceso finalizado con éxito.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
