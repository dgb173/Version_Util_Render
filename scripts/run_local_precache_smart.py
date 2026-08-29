#!/usr/bin/env python
"""Precacheo Inteligente Local con Sincronización Cloud.

1. Sincroniza desde GitHub los partidos ya procesados y subidos por el bot.
2. Actualiza el listado de partidos del día.
3. Detecta automáticamente cuáles partidos ya están listos y cuáles faltan.
4. Procesa en tu máquina únicamente los partidos pendientes.
5. Genera el contexto previo de los nuevos partidos.
6. Exporta el precacheo a JSON y opcionalmente lo sube a GitHub/Render.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from modules import data_manager, pending_results_query, sql_store  # noqa: E402


TEMP_JOB_FILE = PROJECT_ROOT / "temp_matches_job_local.json"


def _run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    printable = " ".join(str(part) for part in cmd)
    print(f"\n$ {printable}", flush=True)
    return subprocess.run(cmd, cwd=str(PROJECT_ROOT), check=check)


def sync_cloud_data_to_local() -> int:
    """Descarga data_precacheo.json y data_pending_results.json desde GitHub e inserta en SQLite."""
    print("\n" + "=" * 60)
    print(" [PASO 1/5] Sincronizando partidos del bot de GitHub a SQLite local")
    print("=" * 60)

    try:
        subprocess.run(
            ["git", "fetch", "origin", "main"],
            cwd=str(PROJECT_ROOT),
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception as exc:
        print(f"[AVISO] No se pudo hacer fetch de origin/main: {exc}")
        return 0

    synced_count = 0

    # 1. Sincronizar data_precacheo.json
    try:
        raw_precache = subprocess.check_output(
            ["git", "show", "origin/main:data/data_precacheo.json"],
            cwd=str(PROJECT_ROOT),
            text=True,
            encoding="utf-8",
        )
        precache_rows = json.loads(raw_precache)
        if isinstance(precache_rows, list):
            valid_rows = [
                row for row in precache_rows
                if isinstance(row, dict) and (row.get("match_id") or row.get("id"))
            ]
            conn = sql_store._connect()
            try:
                for row in valid_rows:
                    sql_store._upsert_match(
                        conn,
                        row,
                        bucket="data_precacheo.json",
                        state="precacheo",
                    )
                conn.commit()
                synced_count = len(valid_rows)
                print(f"  [OK] Precacheo Cloud sincronizado: {synced_count} partidos integrados en SQLite.")
            finally:
                conn.close()
    except Exception as exc:
        print(f"[AVISO] No se pudo leer data_precacheo.json remoto: {exc}")

    # 2. Sincronizar data_pending_results.json
    try:
        raw_pending = subprocess.check_output(
            ["git", "show", "origin/main:data/data_pending_results.json"],
            cwd=str(PROJECT_ROOT),
            text=True,
            encoding="utf-8",
        )
        pending_rows = json.loads(raw_pending)
        if isinstance(pending_rows, list):
            valid_pending = [
                row for row in pending_rows
                if isinstance(row, dict) and (row.get("match_id") or row.get("id"))
            ]
            conn = sql_store._connect()
            try:
                for row in valid_pending:
                    sql_store._upsert_match(
                        conn,
                        row,
                        bucket="data_pending_results.json",
                        state="pending_results",
                    )
                conn.commit()
                print(f"  [OK] Pendientes Cloud sincronizados: {len(valid_pending)} partidos.")
            finally:
                conn.close()
    except Exception as exc:
        print(f"[AVISO] No se pudo leer data_pending_results.json remoto: {exc}")

    return synced_count


def update_day_snapshot() -> None:
    """Ejecuta el scraper de portada para tener los partidos del día al día."""
    print("\n" + "=" * 60)
    print(" [PASO 2/5] Actualizando snapshot de partidos de hoy")
    print("=" * 60)
    _run([sys.executable, str(PROJECT_ROOT / "scripts" / "run_scraper.py")])


def detect_missing_jobs(job_file: Path, force_full: bool = False) -> list[dict]:
    """Usa build_job_from_snapshot para seleccionar SOLO los partidos que no están en precacheo."""
    print("\n" + "=" * 60)
    print(" [PASO 3/5] Comparando partidos del día con la base local (filtrado inteligente)")
    print("=" * 60)

    db_path = sql_store.get_db_path()
    build_cmd = [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "build_job_from_snapshot.py"),
        "--db",
        str(db_path),
        "--cache-key",
        "app_main_page_cache_v1",
        "--out",
        str(job_file),
    ]
    if force_full:
        build_cmd.append("--include-existing")

    job_file.unlink(missing_ok=True)
    result = _run(build_cmd, check=False)

    if result.returncode != 0:
        raise RuntimeError(
            f"No se pudo construir la cola de Pre-Cacheo (código {result.returncode})."
        )

    if not job_file.exists():
        return []

    try:
        with job_file.open("r", encoding="utf-8") as f:
            jobs = json.load(f)
        if isinstance(jobs, list):
            return jobs
    except Exception as e:
        print(f"Error leyendo archivo de trabajos {job_file}: {e}")

    return []


def push_changes_to_render(custom_message: str = "") -> bool:
    """Exporta el precacheo a JSON y lo envía a origin/main."""
    print("\n" + "=" * 60)
    print(" Subiendo precacheo actualizado a GitHub / Render...")
    print("=" * 60)
    try:
        _run([sys.executable, str(PROJECT_ROOT / "scripts" / "export_precacheo_json.py"), "--include-pending"])
        subprocess.run(
            ["git", "add", "--", "data/data_precacheo.json", "data/data_pending_results.json", "data/league_extractions.json"],
            cwd=str(PROJECT_ROOT),
            check=True,
        )
        diff_res = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=str(PROJECT_ROOT))
        if diff_res.returncode == 0:
            print("  [INFO] No hay cambios nuevos en los archivos de precacheo para subir.")
            return True

        stamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
        msg = custom_message or f"chore: local precache update ({stamp})"
        _run(["git", "commit", "-m", msg, "--", "data/data_precacheo.json", "data/data_pending_results.json", "data/league_extractions.json"])

        print("  Haciendo push a origin main...")
        # No hacer rebase automático: esta carpeta puede contener otros
        # trabajos locales. Si main avanzó, el push falla sin tocar esos datos.
        subprocess.run(["git", "fetch", "origin", "main"], cwd=str(PROJECT_ROOT), check=False)
        push_res = subprocess.run(["git", "push", "origin", "HEAD:main"], cwd=str(PROJECT_ROOT), check=False)
        if push_res.returncode == 0:
            print("  [OK] Precacheo subido exitosamente a Render!")
            return True
        else:
            print("  [ERROR] El push ha fallado. Revisa tu conexión o permisos de git.")
            return False
    except Exception as exc:
        print(f"Error al subir cambios a GitHub: {exc}")
        return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Precacheo local inteligente con selección de partidos faltantes.")
    parser.add_argument("--workers", type=int, default=8, help="Número de workers en paralelo para scraping local.")
    parser.add_argument("--force-full", action="store_true", help="Forzar reanálisis de todos los partidos.")
    publish = parser.add_mutually_exclusive_group()
    publish.add_argument(
        "--push",
        action="store_true",
        help="Publicar explícitamente los JSON en GitHub/Render al terminar.",
    )
    publish.add_argument(
        "--no-push",
        action="store_true",
        help="Compatibilidad: no publicar (este es ahora el comportamiento por defecto).",
    )
    parser.add_argument("--message", type=str, default="", help="Mensaje personalizado de commit.")
    return parser.parse_args()


def print_health_summary() -> None:
    snapshot = sql_store.get_json_state("app_main_page_cache_v1", default={}) or {}
    snapshot_count = len(snapshot.get("upcoming_matches", [])) if isinstance(snapshot, dict) else 0
    future_ids = pending_results_query.fetch_upcoming_ids_from_sql(
        buckets=[data_manager.PRECACHEO_BUCKET, data_manager.PENDING_RESULTS_BUCKET],
        limit=500,
    )
    with sql_store._connect() as conn:
        historical_count = int(
            conn.execute("SELECT COUNT(*) FROM matches WHERE state = 'historical'").fetchone()[0]
        )

    print("\n" + "=" * 60)
    print(" RESUMEN DE SALUD LOCAL")
    print("=" * 60)
    print(f" Partidos próximos del snapshot: {snapshot_count}")
    print(f" Partidos próximos analizados visibles: {len(future_ids)}")
    print(f" Partidos históricos disponibles en Explorador: {historical_count}")
    print(" La app local leerá SQLite; el índice rápido queda reservado para Render.")
    print("=" * 60)


def main() -> int:
    args = parse_args()
    print("============================================================")
    print("        PRECACHEO INTELIGENTE LOCAL (NOWGOAL APP)           ")
    print("============================================================")

    # 1. Sincronizar partidos del bot
    sync_cloud_data_to_local()

    # 2. Actualizar snapshot de hoy
    update_day_snapshot()

    # 3. Detectar qué falta
    missing_jobs = detect_missing_jobs(TEMP_JOB_FILE, force_full=args.force_full)
    total_missing = len(missing_jobs)

    if total_missing == 0:
        print("\n" + "=" * 60)
        print(" [RESULTADO] Todos los partidos ya fueron precacheados por el bot!")
        print(" No hay partidos pendientes que requieran análisis local.")
        print("=" * 60)
        # Exportar JSON por si la sincronización del bot trajo novedades
        _run([sys.executable, str(PROJECT_ROOT / "scripts" / "export_precacheo_json.py"), "--include-pending"])
        if args.push:
            push_changes_to_render(args.message)
        print_health_summary()
        return 0

    print(f"\n -> Se detectaron {total_missing} partidos FALTANTES o pendientes de análisis.")
    print(f" -> Iniciando análisis local con {args.workers} workers...")

    # 4. Procesar faltantes en local
    print("\n" + "=" * 60)
    print(f" [PASO 4/5] Analizando {total_missing} partidos faltantes en local")
    print("=" * 60)
    runner_cmd = [
        sys.executable,
        str(PROJECT_ROOT / "background_runner.py"),
        "--job_file",
        str(TEMP_JOB_FILE),
        "--concurrency",
        str(args.workers),
        "--flush_every",
        "5",
    ]
    runner_result = _run(runner_cmd, check=False)
    if runner_result.returncode != 0:
        print(f"\n[ERROR] El análisis local terminó con código {runner_result.returncode}.")
        print("No se exportará ni publicará un snapshot incompleto.")
        return runner_result.returncode or 1

    # 5. Generar contexto previo
    print("\n" + "=" * 60)
    print(" [PASO 5/5] Calculando contexto previo para los nuevos partidos")
    print("=" * 60)
    context_cmd = [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "refresh_pre_match_context.py"),
        "--workers",
        str(min(args.workers, 6)),
        "--ttl-hours",
        "8",
    ]
    context_result = _run(context_cmd, check=False)
    if context_result.returncode != 0:
        print("[AVISO] Falló parte del contexto previo; los partidos analizados se conservan.")

    # Limpieza de archivo temporal
    try:
        TEMP_JOB_FILE.unlink(missing_ok=True)
    except OSError:
        pass

    # 6. Exportar y Push
    if args.push:
        push_changes_to_render(args.message)
    else:
        _run([sys.executable, str(PROJECT_ROOT / "scripts" / "export_precacheo_json.py"), "--include-pending"])
        print("\n[INFO] Exportación local completada. GitHub/Render no se han modificado.")

    print_health_summary()

    print("\n" + "=" * 60)
    print(" ¡PROCESO DE PRECACHEO LOCAL FINALIZADO CON ÉXITO!")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
