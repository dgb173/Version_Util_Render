import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent / 'src'))

import json
from modules import specialist_validator
from modules.ml_predictor import extract_features

print("Loading Data...")
try:
    with open('data/data_precacheo.json', 'r', encoding='utf-8') as f:
        matches = json.load(f)
    print(f"Loaded {len(matches)} precacheo matches.")
except Exception as e:
    print(f"Error loading precacheo data: {e} (Falling back to data.json upcoming)")
    try:
        with open('data.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
        matches = data.get('upcoming_matches', [])
    except: matches = []

if not matches:
    print("No matches to test.")
    sys.exit(0)

# Load rules
specialist_validator.validator.load_rules()
print(f"Loaded {len(specialist_validator.validator.rules)} rule files.")

# Test first 5 matches
for i, m in enumerate(matches[:5]):
    print(f"\n--- Testing Match {i}: {m.get('home_team')} vs {m.get('away_team')} (AH: {m.get('main_match_odds', {}).get('ah_linea')}) ---")
    
    # 1. Extract Features using Validatror
    target = float(m.get('main_match_odds', {}).get('ah_linea', 0) or 0)
    print(f"  Target Line: {target}")
    feats = specialist_validator.validator.extract_features(m, target, 'AH')
    print("  Validator Features:")
    for k, v in feats.items():
        print(f"    {k}: {v}")

    # 2. Evaluate
    picks = specialist_validator.validator.evaluate_match(m)
    print(f"Picks Found: {len(picks)}")
    for p in picks:
        print(f"  ✅ {p['name']} -> {p['prediction']} ({p['accuracy']}%)")

print("\nDone.")
