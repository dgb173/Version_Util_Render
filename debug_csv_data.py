
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
        print(json.dumps(data, indent=2, default=str))
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    # Use a match ID that likely exists or was used in the user's screenshot
    # From screenshot: 2904192 (Hawassa City vs Sidama Bunna)
    match_id = "2904192" 
    debug_match_data(match_id)
