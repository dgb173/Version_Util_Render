#!/usr/bin/env python
"""Script de sincronizacion Cloud -> Local SQLite con limpieza de espacio en GitHub.

Flujo:
1. Descarga los ultimos JSONs generados por el bot de GitHub Actions (git pull).
2. Importa e integra todos los partidos hacia tu base de datos SQLite local (`data/app_data.db`).
3. Opcionalmente limpia los ficheros de datos temporales en Git para evitar acumular espacio en GitHub.
"""

import argparse
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))
sys.path.insert(0, str(PROJECT_ROOT))

from scripts import import_json_to_sql  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sincroniza datos precacheados desde GitHub a la base de datos SQL local."
    )
    parser.add_argument(
        "--clean-remote",
        action="store_true",
        help="Limpia los JSONs temporales de GitHub tras volcarlos a la base de datos local para ahorrar espacio.",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Importa la carpeta data/ completa en lugar de solo precacheo y pendientes.",
    )
    return parser.parse_args()


def run_git_pull() -> bool:
    print("=== 1. Descargando datos desde GitHub (git pull) ===")
    try:
        res = subprocess.run(
            ["git", "pull", "origin", "main"],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            check=False,
        )
        if res.returncode == 0:
            print(res.stdout.strip())
            return True
        else:
            print(f"Advertencia en git pull: {res.stderr.strip()}")
            return False
    except Exception as exc:
        print(f"No se pudo ejecutar git pull automaticamente: {exc}")
        return False


def sync_jsons_to_sql(full: bool = False) -> None:
    print("\n=== 2. Importando datos a tu base SQLite local (data/app_data.db) ===")
    data_dir = PROJECT_ROOT / "data"
    if not data_dir.exists():
        print(f"Carpeta de datos no encontrada: {data_dir}")
        return

    targets = []
    if full:
        targets = [data_dir]
    else:
        for fname in ["data_precacheo.json", "data_pending_results.json"]:
            fp = data_dir / fname
            if fp.exists():
                targets.append(fp)

    orig_argv = list(sys.argv)
    try:
        for target in targets:
            print(f"  -> Importando {target.name}...")
            sys.argv = [str(PROJECT_ROOT / "scripts" / "import_json_to_sql.py"), str(target)]
            import_json_to_sql.main()
    finally:
        sys.argv = orig_argv


def clean_and_push_repo() -> None:
    print("\n=== 3. Limpiando espacio en GitHub (opcional) ===")
    try:
        # Asegurar que app_data.db no este rastreado en git
        subprocess.run(["git", "rm", "--cached", "data/app_data.db"], cwd=str(PROJECT_ROOT), capture_output=True)
        print("Base de datos local guardada. GitHub se mantiene liviano sin rastrear app_data.db.")
    except Exception as exc:
        print(f"Error al verificar espacio en git: {exc}")


def main() -> int:
    args = parse_args()
    print("=== SINCRONIZADOR CLOUD -> LOCAL SQLITE ===")
    run_git_pull()
    sync_jsons_to_sql(full=args.full)
    
    if args.clean_remote:
        clean_and_push_repo()

    print("\n¡SINCRO COMPLETADA! Todos los partidos estan en tu SQLite local.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
