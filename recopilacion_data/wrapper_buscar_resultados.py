import sys
import os
from pathlib import Path

# Add project root to sys.path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from src.app import scrape_pending_results_background

if __name__ == "__main__":
    print("Iniciando 'Buscar Resultados (+2h)'...")
    try:
        scrape_pending_results_background()
        print("Búsqueda de resultados completada.")
    except Exception as e:
        print(f"Error durante la búsqueda de resultados: {e}")
