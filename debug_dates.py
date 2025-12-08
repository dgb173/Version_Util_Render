import json
import os

file_path = 'c:/Users/Usuario/Desktop/Version-00-nueva/data/data_minus_ah_1.5.json'

try:
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    print(f"Total matches: {len(data)}")
    
    # Check first 5 matches
    for i, m in enumerate(data[:5]):
        print(f"Match {i}: ID={m.get('match_id')}")
        print(f"  Keys: {list(m.keys())}")
        print(f"  match_date: {m.get('match_date')}")
        print(f"  date: {m.get('date')}")
        print(f"  cached_at: {m.get('cached_at')}")
        print("-" * 20)

    # Search for Soles de Sonora
    target = "Soles de Sonora"
    found = False
    for m in data:
        if m.get('home_name') == target or m.get('home_team') == target or m.get('away_name') == target or m.get('away_team') == target:
            print(f"FOUND TARGET: {target}")
            print(f"  ID: {m.get('match_id')}")
            print(f"  match_date: {m.get('match_date')}")
            print(f"  date: {m.get('date')}")
            print(f"  cached_at: {m.get('cached_at')}")
            found = True
            break
            
    if not found:
        print(f"Target {target} NOT FOUND in JSON.")

except Exception as e:
    print(f"Error: {e}")
