
import json
from pathlib import Path

def find_match():
    try:
        path = Path(r"c:\Users\Usuario\Desktop\Version-00-nueva\data.json")
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        upcoming = data.get('upcoming_matches', [])
        print(f"Total upcoming: {len(upcoming)}")
        
        for m in upcoming:
            if 'Francvaros' in m.get('home_team', '') or 'Ferencvaros' in m.get('home_team', ''):
                 print(f"Found: {m['home_team']} vs {m['away_team']} ID: {m['id']}")
    except Exception as e:
        print(f"Error: {e}")

find_match()
