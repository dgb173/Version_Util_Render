
import json
import sys
import os

# Add src to path
sys.path.append(os.path.abspath('src'))

from modules.pattern_search import explore_matches

# Mock data loading
file_path = r"c:\Users\Usuario\Desktop\Version-00-nueva\data\data_minus_ah_1.5.json"
with open(file_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

# Run explore_matches
print("Running explore_matches...")
results = explore_matches(data, filters={'limit': 5})

print(f"\nFound {len(results)} matches.")
for i, res in enumerate(results):
    print(f"\n--- Match {i} ({res['match_id']}) ---")
    
    ph = res.get('prev_home', {})
    print(f"Prev Home Movement: '{ph.get('movement')}'")
    
    pa = res.get('prev_away', {})
    print(f"Prev Away Movement: '{pa.get('movement')}'")
    
    # Debug HTML content for Prev Home
    html = res.get('market_analysis_html')
    if html:
        if "movimiento" in html:
            print("Trace: 'movimiento' found in HTML.")
            # Print snippet around 'movimiento'
            idx = html.find("movimiento")
            print(f"Snippet: {html[idx:idx+100]}")
        else:
            print("Trace: 'movimiento' NOT found in HTML.")
    else:
        print("Trace: No market_analysis_html.")
