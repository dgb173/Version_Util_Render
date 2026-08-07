#!/usr/bin/env python
"""Script de sincronizacion local.

Descarga los ultimos cambios del repositorio (git pull) e importa
automaticamente los JSONs precacheados por GitHub Actions hacia la base de
datos SQLite local (`data/app_data.db`).
"""

import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))
sys.path.insert(0, str(PROJECT_ROOT))

from scripts import import_json_to_sql  # noqa: E402


def run_git_pull() -> bool:
    print("=== 1. Actualizando repositorio local desde GitHub (git pull) ===")
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


def sync_jsons_to_sql() -> None:
    print("\n=== 2. Importando JSONs precacheados hacia app_data.db ===")
    data_dir = PROJECT_ROOT / "data"
    if not data_dir.exists():
        print(f"Carpeta de datos no encontrada: {data_dir}")
        return

    # Usar import_json_to_sql para volcar los JSONs a SQLite
    sys.argv = [str(PROJECT_ROOT / "scripts" / "import_json_to_sql.py"), str(data_dir)]
    import_json_to_sql.main()


def main() -> int:
    print("Sincronizador Cloud -> Local SQLite")
    run_git_pull()
    sync_jsons_to_sql()
    print("\n¡Sincronizacion completada! Tu base de datos local esta actualizada.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
