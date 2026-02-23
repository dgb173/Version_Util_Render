import json
import os
import sys

# Load validation data
fpath = 'validation_data_39_2024-2025.json'
with open(fpath, 'r', encoding='utf-8') as f:
    data = json.load(f)

# Find a match with a clear result and AH
for m in data[:30]:
    score = m.get('score')
    ah = m.get('handicap')
    
    # Needs valid score and AH
    if score in ['?:?', '?-?'] or not ah: continue
    
    parts = score.split('-')
    hg = int(parts[0])
    ag = int(parts[1])
    
    try:
        ah_val = float(ah)
    except: continue
    
    print(f"Match: {m.get('home_team')} vs {m.get('away_team')}")
    print(f"Score: {score} (Diff: {hg-ag})")
    print(f"Handicap (Home): {ah_val}")
    
    # Calc Result
    res = (hg - ag) + ah_val
    outcome_home = "WIN" if res > 0.25 else ("LOSS" if res < -0.25 else "PUSH/HALF")
    
    print(f"Home Result with AH: {res} -> {outcome_home}")
    print("-" * 20)
    
    if outcome_home == "WIN":
        print("For a HOME prediction to be correct, Home must Cover.")
    else:
        print("For a HOME prediction to be correct, it would fail here.")
        
    print("\nSimulating Validator for this match...")
    
    # Import validator
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
    from modules.specialist_validator import validator
    
    picks = validator.evaluate_match(m)
    print(f"Picks Found: {len(picks)}")
    for p in picks:
        print(f" - Rule: {p['name']}")
        print(f" - Prediction: {p['prediction']}")
        print(f" - Rule Target Line: {p.get('target_line')}")
        
        # Check Hit
        is_hit = False
        if outcome_home == "WIN":
             is_hit = (p['prediction'] in ['HOME', 'LOCAL'])
        elif outcome_home == "LOSS":
             is_hit = (p['prediction'] in ['AWAY', 'VISITA'])
        
        print(f" -> IS HIT? {is_hit}")
    
    # Stop after 3 useful matches
    # break
