
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
        
        # Dump the ENTIRE comparativas_indirectas structure to see what's going on
        debug_info = {
            "match_id": data.get("match_id"),
            "home_team": data.get("home_name"),
            "away_team": data.get("away_name"),
            "comparativas_indirectas": data.get("comparativas_indirectas"),
            "last_home_match": data.get("last_home_match"),
        }
        
        print(json.dumps(debug_info, indent=2, default=str))
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    # Problematic ID from user
    debug_match_data("2862244")
