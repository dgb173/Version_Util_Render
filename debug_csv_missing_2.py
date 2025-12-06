
import sys
import os
import json
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).resolve().parent / 'src'))

from modules.estudio_scraper import analizar_partido_completo

def debug_match_data(match_id):
    print(f"Analyzing match {match_id}...")
    try:
        data = analizar_partido_completo(match_id)
        
        debug_info = {
            "match_id": data.get("match_id"),
            "home_team": data.get("home_name"),
            "away_team": data.get("away_name"),
            "score": data.get("final_score"),
            "time": data.get("time"),
            "league": data.get("league_name"),
            "main_odds": data.get("main_match_odds"),
        }
        
        print(json.dumps(debug_info, indent=2, default=str))
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_match_data("2866818")
