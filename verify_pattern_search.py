import json
import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path.cwd() / 'src'))

from modules.pattern_search import find_similar_patterns, normalize_ah_bucket, asian_result

def test_pattern_search():
    print("--- Testing Pattern Search Module ---")
    
    # 1. Test Normalization
    print("\n[Test] Normalization:")
    cases = [
        (-0.5, -0.5), (-0.25, -0.5), (-0.75, -0.5),
        (0.5, 0.5), (0.25, 0.5), (0.75, 0.5),
        (-2.0, -2.0), (-2.25, -2.5), (0.0, 0.0)
    ]
    for inp, exp in cases:
        res = normalize_ah_bucket(inp)
        print(f"  {inp} -> {res} (Expected: {exp}) {'OK' if res == exp else 'FAIL'}")

    # 2. Test Asian Result
    print("\n[Test] Asian Result:")
    # Home Win 2-1, AH -0.5 -> Win (Cover)
    r1 = asian_result(2, 1, -0.5)
    print(f"  2-1, AH -0.5 -> {r1['category']} (Expected: COVER)")
    
    # Home Win 1-0, AH -1.5 -> Loss (No Cover)
    r2 = asian_result(1, 0, -1.5)
    print(f"  1-0, AH -1.5 -> {r2['category']} (Expected: NO_COVER)")
    
    # Draw 1-1, AH -0.25 -> Half Loss (No Cover)
    r3 = asian_result(1, 1, -0.25)
    print(f"  1-1, AH -0.25 -> {r3['category']} (Expected: NO_COVER)")
    
    # Draw 1-1, AH +0.25 -> Half Win (Cover)
    r4 = asian_result(1, 1, 0.25)
    print(f"  1-1, AH +0.25 -> {r4['category']} (Expected: COVER)")

    # 3. Test Find Similar Patterns
    print("\n[Test] Find Similar Patterns:")
    
    # Load history
    json_path = Path('studied_matches/history.json')
    if not json_path.exists():
        print("  Skipping: history.json not found")
        return

    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    print(f"  Loaded {len(data)} matches from history.")
    
    # Mock upcoming match with previous AH context
    upcoming = {
        'ah_open_home': -0.5,
        'home_team': 'Test Home',
        'away_team': 'Test Away',
        'date': '2025-12-01',
        'prev_home_ah': -0.25, # Example: Home team came from a -0.25 game
        'prev_away_ah': 0.5    # Example: Away team came from a +0.5 game
    }
    
    print(f"\n[Test] Searching with Previous AH Context:")
    print(f"  Target AH: {upcoming['ah_open_home']}")
    print(f"  Target Prev Home AH: {upcoming['prev_home_ah']}")
    print(f"  Target Prev Away AH: {upcoming['prev_away_ah']}")

    results = find_similar_patterns(upcoming, data)
    print(f"  Found {len(results)} similar patterns.")
    
    if results:
        print("  Top 3 results (sorted by similarity):")
        for i, res in enumerate(results[:3]):
            score = res.get('similarity_score', 1)
            ph_match = res['prev_home']['matches_pattern'] if res['prev_home'] else False
            pa_match = res['prev_away']['matches_pattern'] if res['prev_away'] else False
            print(f"    #{i+1} Score: {score} | PrevHomeMatch: {ph_match} | PrevAwayMatch: {pa_match} | Date: {res['candidate']['date']}")

if __name__ == "__main__":
    test_pattern_search()
