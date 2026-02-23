
import json
import glob
import os

data_dir = r"c:\Users\Usuario\Desktop\Version_Util_Render\data"
json_files = glob.glob(os.path.join(data_dir, "data*.json"))

print(f"Found {len(json_files)} json files.")

for f in json_files:
    try:
        with open(f, 'r', encoding='utf-8') as fp:
            data = json.load(fp)
            matches = []
            if isinstance(data, list):
                matches = data
            elif isinstance(data, dict):
                matches = data.get('upcoming_matches', []) + data.get('finished_matches', [])
            
            print(f"Checking {f} ({len(matches)} matches)")
            
            found_stats = False
            for m in matches:
                # Check different sources
                sources = ['last_home_match', 'last_away_match', 'h2h_general', 'h2h_stadium']
                for src in sources:
                    if m.get(src) and 'stats_rows' in m[src]:
                        print(f"Found stats_rows in {src} for match {m.get('id')}")
                        print(json.dumps(m[src]['stats_rows'], indent=2))
                        found_stats = True
                        break
                if found_stats:
                    break
            
            if found_stats:
                break
    except Exception as e:
        print(f"Error reading {f}: {e}")
