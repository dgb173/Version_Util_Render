import sys
from pathlib import Path

# Add project root to sys.path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from src.app import process_upcoming_matches_background
from src.modules import sql_store

if __name__ == "__main__":
    print("Iniciando 'Scrapear Pendientes' (Pre-Cacheo)...")
    print(f"DB SQL: {sql_store.get_db_path()}")

    # Leer workers de argumentos
    workers = 8
    if len(sys.argv) > 1:
        try:
            workers = int(sys.argv[1])
        except:
            pass

    print(f"Configuración: Workers={workers}")

    try:
        process_upcoming_matches_background(handicap_filter=None, goal_line_filter=None, workers=workers)
        print("Scraping de pendientes completado.")
    except Exception as e:
        print(f"Error durante el scraping: {e}")
