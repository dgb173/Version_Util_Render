import json
from pathlib import Path
import sys

# Add src to path to import modules if needed, but we can just copy the logic for isolation
sys.path.append('src')

def parse_ah(ah_str):
    if not ah_str: return None
    try:
        return float(ah_str)
    except:
        return None

def debug_matching():
    data_path = Path("data.json")
    if not data_path.exists():
        print("data.json not found!")
        return

    with open(data_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    finished = data.get('finished_matches', [])
    print(f"Loaded {len(finished)} finished matches.")
    
    # Sample a few matches to see formats
    print("\n--- Sample Data ---")
    for i, m in enumerate(finished[:5]):
        print(f"ID: {m.get('id')}, Home: {m.get('home_team')}, AH: {m.get('handicap')}, OU: {m.get('goal_line')}")

    # Test Case 1: Pick a match and try to find it as a clone
    if not finished: return

    target_match = finished[0]
    target_ah = target_match.get('handicap') # e.g. "0.75"
    target_ou = target_match.get('goal_line') # e.g. "2.75"
    
    print(f"\n--- Testing Match Logic for Target: AH={target_ah}, OU={target_ou} ---")
    
    # Logic 1: AH Only (Current Implementation)
    matches_ah = [m for m in finished if m.get('handicap') == target_ah]
    print(f"Matches with AH {target_ah}: {len(matches_ah)}")
    
    # Logic 2: AH + OU (Proposed Fix)
    matches_strict = [m for m in finished if m.get('handicap') == target_ah and m.get('goal_line') == target_ou]
    print(f"Matches with AH {target_ah} AND OU {target_ou}: {len(matches_strict)}")
    
    # Check if there are matches with same AH but diff OU
    diff_ou = [m for m in matches_ah if m.get('goal_line') != target_ou]
    if diff_ou:
        print(f"Example of AH match but OU mismatch: ID {diff_ou[0].get('id')}, OU: {diff_ou[0].get('goal_line')}")
    else:
        print("All AH matches also matched OU (unlikely in large dataset).")

if __name__ == "__main__":
    debug_matching()
