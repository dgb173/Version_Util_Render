import json
import sys
import datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).resolve().parent / 'src'))

from modules.pattern_search import get_previous_match, find_similar_patterns

def load_data():
    data_path = Path('studied_matches/history.json')
    if not data_path.exists():
        data_path = Path('src/studied_matches/history.json')
        
    if not data_path.exists():
        # Fallback to absolute path based on user info if needed, or just try relative
        data_path = Path(r'c:\Users\Usuario\Desktop\V_buena\studied_matches\history.json')

    if not data_path.exists():
        print(f"ERROR: history.json not found at {data_path}")
        return []
        
    with open(data_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    # history.json is usually a list of dicts
    all_matches = data
        
    print(f"Loaded {len(all_matches)} matches from {data_path}")
    return all_matches

def test_get_previous_match():
    all_matches = load_data()
    
    # Test Case 1: Tatran Presov (Known to have history)
    team = "Tatran Presov"
    ref_date = "2025-11-29"
    print(f"\n--- Testing {team} before {ref_date} (Home) ---")
    
    prev = get_previous_match(team, ref_date, all_matches, required_venue='home')
    if prev:
        print("FOUND PREVIOUS MATCH:")
        print(json.dumps(prev['match'], indent=2, ensure_ascii=False))
    else:
        print("NO PREVIOUS MATCH FOUND")
        
    # Check if the match exists in data at all
    print(f"\nSearching for any match involving {team}...")
    found_any = False
    for m in all_matches:
        # Check keys
        h = m.get('home_team') or m.get('home_name')
        a = m.get('away_team') or m.get('away_name')
        
        if h and team in h:
            print(f"Found match for {team}. Keys: {list(m.keys())}")
            print(f"Sample data: {json.dumps(m, indent=2, ensure_ascii=False)}")
            break
    if not found_any:
        print(f"WARNING: {team} not found in data.json at all!")

    # Test Case 2: Soles de Sonora (From screenshot)
    team2 = "Soles de Sonora"
    print(f"\n--- Testing {team2} before {ref_date} (Home) ---")
    
    found_soles = False
    for m in all_matches:
        h = m.get('home_team') or m.get('home_name')
        if h and team2 in h:
            found_soles = True
            print(f"Found match for {team2}. Keys: {list(m.keys())}")
            if 'last_home_match' in m:
                print("Has 'last_home_match':", json.dumps(m['last_home_match'], indent=2, ensure_ascii=False))
            else:
                print("Does NOT have 'last_home_match'")
            break
            
    if not found_soles:
        print(f"WARNING: {team2} not found in history.json")

if __name__ == "__main__":
    test_get_previous_match()
