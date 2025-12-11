import sys
import os
from pathlib import Path

# Add project root to sys.path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from src.app import process_upcoming_matches_background

if __name__ == "__main__":
    print("Iniciando 'Scrapear Pendientes' (Pre-Cacheo)...")
    # Default parameters as used in the API endpoint
    # scrapeAllPending in html calls /api/precacheo_scrape_background with handicap filter if present
    # We will default to None for filters to scrape everything relevant
    try:
        process_upcoming_matches_background(handicap_filter=None, goal_line_filter=None, workers=8)
        print("Scraping de pendientes completado.")
    except Exception as e:
        print(f"Error durante el scraping: {e}")
