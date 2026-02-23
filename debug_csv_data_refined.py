
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
        
        # Extract specific fields of interest based on NEW structure
        debug_info = {
            "match_id": data.get("match_id"),
            "time": data.get("time"),
            "comp_home_rival": data.get("comparativas_indirectas", {}).get("left", {}).get("rival_name"),
            "comp_away_rival": data.get("comparativas_indirectas", {}).get("right", {}).get("rival_name"),
            "h2h_col3_home": data.get("h2h_col3", {}).get("h2h_home_team_name") if data.get("h2h_col3") else None,
            "main_odds": data.get("main_match_odds"),
        }
        
        print(json.dumps(debug_info, indent=2, default=str))
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    # Try a few IDs
    ids = ["2904192", "2904239"]
    for mid in ids:
        debug_match_data(mid)
