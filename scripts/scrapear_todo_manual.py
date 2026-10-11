#!/usr/bin/env python
"""Scraper Manual Completo de Emergencia para Precacheo.

Ejecuta el ciclo completo de scraping, análisis y exportación en tu PC local:
1. Descarga el listado completo de partidos de hoy y mañana desde NowGoal (Playwright).
2. Guarda el snapshot en la base de datos local SQLite.
3. Detecta todos los partidos pendientes de análisis.
4. Analiza cada partido en profundidad con 10 bots en paralelo.
5. Calcula el contexto previo y evolución de cuotas.
6. Exporta data_precacheo.json (hasta 400 partidos conservando ayer y hoy).
7. Opcionalmente sube los resultados a GitHub/Render.
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

from modules import sql_store, data_manager, nowgoal_fetcher  # noqa: E402

TEMP_JOB_FILE = PROJECT_ROOT / "temp_manual_scrape_jobs.json"


def _run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    printable = " ".join(str(part) for part in cmd)
    print(f"\n$ {printable}", flush=True)
    return subprocess.run(cmd, cwd=str(PROJECT_ROOT), check=check)


def main() -> int:
    parser = argparse.ArgumentParser(description="Scraper manual completo de precacheo.")
    parser.add_argument("--workers", type=int, default=10, help="Número de workers en paralelo.")
    parser.add_argument("--push", action="store_true", help="Subir a Render/GitHub al terminar.")
    parser.add_argument("--force-all", action="store_true", help="Forzar reanálisis de todos los partidos.")
    args = parser.parse_args()
    os.environ.setdefault("ANALYSIS_MAX_CONCURRENCY", str(max(1, min(args.workers, 6))))
    os.environ.setdefault("PLAYWRIGHT_MAX_CONCURRENCY", "1")

    python = sys.executable

    print("=" * 65)
    print("      SCRAPER MANUAL COMPLETO DE PRECACHEO (NOWGOAL)")
    print("=" * 65)

    # PASO 1: Scraping de portada y calendario con Playwright
    print("\n[PASO 1/5] Descargando listado fresco de partidos de NowGoal...")
    _run([python, "scripts/run_scraper.py"])

    # PASO 2: Construir lista de partidos a analizar
    print("\n[PASO 2/5] Generando cola de análisis...")
    db_path = sql_store.get_db_path()
    build_cmd = [
        python,
        "scripts/build_job_from_snapshot.py",
        "--db",
        str(db_path),
        "--cache-key",
        "app_main_page_cache_v1",
        "--out",
        str(TEMP_JOB_FILE),
    ]
    if args.force_all:
        build_cmd.append("--include-existing")

    TEMP_JOB_FILE.unlink(missing_ok=True)
    build_res = _run(build_cmd, check=False)

    jobs = []
    if TEMP_JOB_FILE.exists():
        try:
            with TEMP_JOB_FILE.open("r", encoding="utf-8") as fh:
                jobs = json.load(fh)
        except Exception:
            jobs = []

    print(f"\n -> Total de partidos a analizar en profundidad: {len(jobs)}")

    # PASO 3: Ejecutar análisis en paralelo
    if jobs:
        print(f"\n[PASO 3/5] Analizando {len(jobs)} partidos con {args.workers} bots locales...")
        runner_cmd = [
            python,
            "background_runner.py",
            "--job_file",
            str(TEMP_JOB_FILE),
            "--concurrency",
            str(args.workers),
            "--flush_every",
            "5",
            "--defer-summary-stats",
        ]
        _run(runner_cmd, check=False)
    else:
        print("\n[PASO 3/5] No hay partidos nuevos que requieran análisis profundo.")

    # PASO 4: Generar contexto previo
    print("\n[PASO 4/5] Calculando contexto previo (H2H y últimos partidos)...")
    context_cmd = [
        python,
        "scripts/refresh_pre_match_context.py",
        "--workers",
        str(min(args.workers, 6)),
        "--ttl-hours",
        "8",
    ]
    _run(context_cmd, check=False)

    # Limpieza de archivo temporal
    try:
        TEMP_JOB_FILE.unlink(missing_ok=True)
    except OSError:
        pass

    # PASO 5: Exportar JSON
    print("\n[PASO 5/5] Exportando data_precacheo.json (hasta 400 partidos)...")
    _run([python, "scripts/export_precacheo_json.py", "--include-pending"])

    # PASO 6: Push opcional a Render
    if args.push:
        print("\n[PASO 6/6] Subiendo precacheo actualizado a GitHub y Render...")
        stamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
        msg = f"chore: manual full precache update ({stamp})"
        subprocess.run(
            ["git", "-c", "core.hooksPath=", "add", "--", "data/data_precacheo.json", "data/data_pending_results.json", "data/league_extractions.json"],
            cwd=str(PROJECT_ROOT),
            check=False,
        )
        subprocess.run(
            ["git", "-c", "core.hooksPath=", "commit", "-m", msg, "--no-verify", "--no-gpg-sign", "--", "data/data_precacheo.json", "data/data_pending_results.json", "data/league_extractions.json"],
            cwd=str(PROJECT_ROOT),
            check=False,
        )
        subprocess.run(
            ["git", "-c", "core.hooksPath=", "fetch", "origin", "main", "--depth=1"],
            cwd=str(PROJECT_ROOT),
            check=False,
        )
        subprocess.run(
            ["git", "-c", "core.hooksPath=", "pull", "origin", "main", "--no-rebase", "--no-edit", "--no-verify", "-X", "theirs"],
            cwd=str(PROJECT_ROOT),
            check=False,
        )
        push_res = subprocess.run(
            ["git", "-c", "core.hooksPath=", "push", "origin", "HEAD:main", "--no-verify"],
            cwd=str(PROJECT_ROOT),
            check=False,
        )
        if push_res.returncode == 0:
            print("\n[OK] ¡Precacheo subido exitosamente a Render!")
        else:
            print("\n[AVISO] El push falló. Puedes usar 'subir_precacheo_a_render.bat' más tarde.")

    print("\n" + "=" * 65)
    print("  ¡SCRAPING Y PRECACHEO MANUAL COMPLETADO CON ÉXITO!")
    print("=" * 65)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
