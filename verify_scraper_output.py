import sys
from pathlib import Path
import json

# Add src to path
sys.path.insert(0, str(Path(__file__).resolve().parent / 'src'))

try:
    from modules import estudio_scraper
    
    match_ids = ["2902305", "2789604"]
    
    for mid in match_ids:
        print(f"\nRunning analysis for match {mid}...")
        data = estudio_scraper.analizar_partido_completo(mid, force_refresh=True)
        
        if "error" in data:
            print(f"Error: {data['error']}")
        else:
            print(f"--- ODDS for {mid} ---")
            odds = data.get('main_match_odds', {})
            print(f"AH: {odds.get('ah_linea')}")
            print(f"O/U: {odds.get('goals_linea')}")

except Exception as e:
    print(f"An error occurred: {e}")
