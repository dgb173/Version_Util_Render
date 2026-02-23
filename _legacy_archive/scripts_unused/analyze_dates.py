import json
import datetime
from pathlib import Path
from collections import Counter

DATA_FILE = Path(r"c:\Users\Usuario\Desktop\Version_Util_Render\data\data_precacheo.json")

def analyze():
    if not DATA_FILE.exists():
        print("File not found")
        return

    print(f"Reading {DATA_FILE}...")
    with open(DATA_FILE, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
        except Exception as e:
            print(f"Error loading: {e}")
            return

    print(f"Total matches: {len(data)}")
    
    dates = []
    for m in data:
        dates.append(m.get('match_date', 'N/A'))
    
    counts = Counter(dates)
    print("\nMatches by date (Top 20):")
    for date, count in counts.most_common(20):
        print(f"  {date}: {count}")

    # Check for specific old date from user's observation
    old_count = counts.get('12/25/2025', 0)
    print(f"\nMatches for 12/25/2025: {old_count}")

    # Identify formats
    print("\nDate format samples:")
    for d in list(counts.keys())[:10]:
        print(f"  '{d}'")

if __name__ == "__main__":
    analyze()
