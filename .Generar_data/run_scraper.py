import runpy
from pathlib import Path


def main():
    """
    Delega al scraper oficial SQL del proyecto para evitar desalineaciones
    con flujos legacy.
    """
    project_root = Path(__file__).resolve().parent.parent
    precacheo_only_target = project_root / "precacheo_only_render" / "scripts" / "run_scraper.py"
    default_target = project_root / "scripts" / "run_scraper.py"

    target = precacheo_only_target if precacheo_only_target.exists() else default_target
    print(f"Ejecutando scraper SQL oficial: {target}")
    runpy.run_path(str(target), run_name="__main__")


if __name__ == "__main__":
    main()
