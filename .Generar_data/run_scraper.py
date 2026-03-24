import runpy
from pathlib import Path


def main():
    """
    Delega al scraper oficial SQL del proyecto para evitar desalineaciones
    con flujos legacy.
    """
    project_root = Path(__file__).resolve().parent.parent
    default_target = project_root / "scripts" / "run_scraper.py"
    target = default_target
    print(f"Ejecutando scraper SQL oficial: {target}")
    runpy.run_path(str(target), run_name="__main__")


if __name__ == "__main__":
    main()
