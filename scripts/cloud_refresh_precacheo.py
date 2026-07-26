#!/usr/bin/env python
"""Actualiza el snapshot y el precacheo en un runner desechable de GitHub."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Iterable, Sequence


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_JOB_FILE = PROJECT_ROOT / "temp_matches_job_cloud.json"
SNAPSHOT_FILES = (
    PROJECT_ROOT / "data.json",
    PROJECT_ROOT / "data" / "data.json",
)
PRECACHE_FILE = PROJECT_ROOT / "data" / "data_precacheo.json"
PENDING_FILE = PROJECT_ROOT / "data" / "data_pending_results.json"


class RefreshError(RuntimeError):
    pass


def _run(command: Sequence[str], allowed_codes: Iterable[int] = (0,)) -> int:
    printable = " ".join(str(part) for part in command)
    print(f"\n$ {printable}", flush=True)
    completed = subprocess.run(
        [str(part) for part in command],
        cwd=PROJECT_ROOT,
        env=os.environ.copy(),
        check=False,
    )
    allowed = set(allowed_codes)
    if completed.returncode not in allowed:
        raise RefreshError(
            f"El comando terminó con código {completed.returncode}: {printable}"
        )
    return completed.returncode


def _load_json(path: Path):
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception as exc:
        raise RefreshError(f"JSON inválido o ilegible: {path}: {exc}") from exc


def _validate_outputs(max_json_bytes: int) -> dict:
    for path in (*SNAPSHOT_FILES, PRECACHE_FILE, PENDING_FILE):
        if not path.is_file():
            raise RefreshError(f"No se generó el archivo obligatorio: {path}")
        size = path.stat().st_size
        if size <= 2:
            raise RefreshError(f"El archivo generado está vacío: {path}")
        if size > max_json_bytes:
            raise RefreshError(
                f"{path.name} ocupa {size} bytes y supera el límite "
                f"configurado de {max_json_bytes} bytes"
            )

    root_snapshot = _load_json(SNAPSHOT_FILES[0])
    data_snapshot = _load_json(SNAPSHOT_FILES[1])
    if root_snapshot != data_snapshot:
        raise RefreshError("data.json y data/data.json no contienen el mismo snapshot")
    if not isinstance(root_snapshot, dict):
        raise RefreshError("El snapshot principal no es un objeto JSON")

    upcoming = root_snapshot.get("upcoming_matches")
    finished = root_snapshot.get("finished_matches")
    if not isinstance(upcoming, list) or not isinstance(finished, list):
        raise RefreshError("El snapshot no contiene listas upcoming/finished válidas")
    if not upcoming:
        raise RefreshError(
            "El scraper devolvió cero partidos próximos; se cancela el push "
            "para no reemplazar Render con un snapshot vacío"
        )

    precache = _load_json(PRECACHE_FILE)
    pending = _load_json(PENDING_FILE)
    if not isinstance(precache, list) or not precache:
        raise RefreshError("El precacheo generado no es una lista válida con partidos")
    if not isinstance(pending, list):
        raise RefreshError("Los pendientes generados no son una lista válida")

    return {
        "upcoming": len(upcoming),
        "finished": len(finished),
        "precache": len(precache),
        "pending": len(pending),
        "precache_bytes": PRECACHE_FILE.stat().st_size,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Equivalente cloud de generar data + análisis previo + exportar precacheo."
    )
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--force-full", action="store_true")
    parser.add_argument("--job-file", type=Path, default=DEFAULT_JOB_FILE)
    parser.add_argument("--max-json-bytes", type=int, default=99_000_000)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    workers = max(1, min(int(args.workers), 20))
    job_file = args.job_file.resolve()
    python = sys.executable

    required_env = (
        "APP_SQLITE_PATH",
        "APP_SQLITE_BOOTSTRAP_LOCK",
        "SQL_BOOTSTRAP_MODE",
        "SQL_BOOTSTRAP_ONLY_BUCKETS",
    )
    missing = [name for name in required_env if not os.getenv(name)]
    if missing:
        raise RefreshError(
            "Faltan variables obligatorias para el runner cloud: " + ", ".join(missing)
        )

    try:
        _run((python, "scripts/run_scraper.py"))
        _run((python, "scripts/export_snapshot_to_json.py"))

        build_command = [
            python,
            "scripts/build_job_from_snapshot.py",
            "--db",
            os.environ["APP_SQLITE_PATH"],
            "--cache-key",
            "app_main_page_cache_v1",
            "--out",
            str(job_file),
        ]
        if args.force_full:
            build_command.append("--include-existing")

        build_code = _run(build_command, allowed_codes=(0, 4))
        jobs = _load_json(job_file) if job_file.exists() else []
        if not isinstance(jobs, list):
            raise RefreshError("El archivo temporal de trabajos no es una lista JSON")

        analysis_code = 0
        if build_code == 4 or not jobs:
            print("\nNo hay partidos nuevos que analizar; se conserva el precacheo vigente.")
        else:
            analysis_code = _run(
                (
                    python,
                    "background_runner.py",
                    "--job_file",
                    str(job_file),
                    "--concurrency",
                    str(workers),
                    "--flush_every",
                    "5",
                ),
                allowed_codes=(0, 1),
            )
            if analysis_code == 1:
                print(
                    "\nAVISO: algunos partidos fallaron, pero se exportarán y publicarán "
                    "los análisis completados correctamente."
                )

        _run((python, "scripts/export_precacheo_json.py", "--include-pending"))
        summary = _validate_outputs(max_json_bytes=max(1, args.max_json_bytes))

        print("\n=== RESUMEN CLOUD PRECACHEO ===")
        print(f"Partidos próximos: {summary['upcoming']}")
        print(f"Partidos finalizados: {summary['finished']}")
        print(f"Partidos enviados a análisis: {len(jobs)}")
        print(f"Partidos en precacheo: {summary['precache']}")
        print(f"Pendientes: {summary['pending']}")
        print(f"Tamaño data_precacheo.json: {summary['precache_bytes']} bytes")
        print(f"Resultado del análisis: {analysis_code}")
        return 0
    except RefreshError as exc:
        print(f"\nERROR CLOUD PRECACHEO: {exc}", file=sys.stderr)
        return 1
    finally:
        try:
            job_file.unlink(missing_ok=True)
        except OSError:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
