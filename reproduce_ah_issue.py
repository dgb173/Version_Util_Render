import json
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.getcwd(), 'src'))

from modules.pattern_search import explore_matches, normalize_ah_bucket

def test_ah_filter():
    print("Loading history.json...")
    try:
        with open('studied_matches/history.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print("studied_matches/history.json not found.")
        return

    if not isinstance(data, list):
        print("history.json is not a list.")
        return

    print(f"Loaded {len(data)} matches.")

    # Test normalize_ah_bucket
    print("\nTesting normalize_ah_bucket:")
    test_vals = [2.0, 2.25, 2.5, 2.75, 3.0, -2.5, -2.75, -2.25]
    for v in test_vals:
        print(f"  {v} -> {normalize_ah_bucket(v)}")

    # Test explore_matches with 2.5
    print("\nTesting explore_matches with handicap=2.5:")
    filters = {'handicap': 2.5, 'limit': 10}
    results = explore_matches(data, filters)
    print(f"Found {len(results)} results.")
    for r in results:
        c = r['candidate']
        print(f"  Match: {c['home']} vs {c['away']}, AH: {c['ah_real']}, Bucket: {c['bucket']}")

    # Test explore_matches with -2.5
    print("\nTesting explore_matches with handicap=-2.5:")
    filters = {'handicap': -2.5, 'limit': 10}
    results = explore_matches(data, filters)
    print(f"Found {len(results)} results.")
    for r in results:
        c = r['candidate']
        print(f"  Match: {c['home']} vs {c['away']}, AH: {c['ah_real']}, Bucket: {c['bucket']}")

if __name__ == "__main__":
    test_ah_filter()
