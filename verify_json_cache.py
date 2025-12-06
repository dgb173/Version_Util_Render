import sys
import os
import json
from pathlib import Path
import shutil

# Add src to path
src_path = Path(__file__).resolve().parent / 'src'
sys.path.insert(0, str(src_path))

# Import app (this might print some init messages)
try:
    import app
except ImportError as e:
    print(f"Error importing app: {e}")
    sys.exit(1)

# Setup test paths
STUDIED_MATCHES_DIR = Path(__file__).resolve().parent / 'studied_matches'
JSON_FILE = STUDIED_MATCHES_DIR / 'history.json'

def setup():
    if JSON_FILE.exists():
        # Backup existing file
        shutil.copy(JSON_FILE, JSON_FILE.with_suffix('.json.bak'))
        print("Backed up existing history.json")
        # Clear it for test
        # os.remove(JSON_FILE) # Better not to remove, just read/write
        # But for verification of "creation", maybe we should start fresh or just check append.
        # Let's just check append.

def teardown():
    # Restore backup if needed, or just leave it.
    pass

def verify():
    print("--- Starting Verification ---")
    
    # 1. Create dummy match
    match_1 = {
        'match_id': 'TEST_001',
        'home_name': 'Test Home',
        'away_name': 'Test Away',
        'score': '1-0',
        'final_score': '1-0',
        'time': '12:00'
    }
    
    # 2. Save match 1
    print("Saving match TEST_001...")
    app.save_match_to_json(match_1)
    
    # 3. Verify it's in JSON
    if not JSON_FILE.exists():
        print("FAILURE: history.json was not created.")
        return

    with open(JSON_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    found = any(m.get('match_id') == 'TEST_001' for m in data)
    if found:
        print("SUCCESS: Match TEST_001 found in JSON.")
    else:
        print("FAILURE: Match TEST_001 NOT found in JSON.")
        return

    initial_count = len(data)

    # 4. Try to save duplicate
    print("Saving match TEST_001 again (duplicate)...")
    app.save_match_to_json(match_1)
    
    with open(JSON_FILE, 'r', encoding='utf-8') as f:
        data_after = json.load(f)
        
    if len(data_after) == initial_count:
        print("SUCCESS: Duplicate match was NOT added.")
    else:
        print(f"FAILURE: Duplicate match WAS added. Count went from {initial_count} to {len(data_after)}")

    # 5. Save different match
    match_2 = {
        'match_id': 'TEST_002',
        'home_name': 'Test Home 2',
        'away_name': 'Test Away 2'
    }
    print("Saving match TEST_002...")
    app.save_match_to_json(match_2)
    
    with open(JSON_FILE, 'r', encoding='utf-8') as f:
        data_final = json.load(f)
        
    if len(data_final) == initial_count + 1:
        print("SUCCESS: New match was added correctly.")
    else:
        print(f"FAILURE: New match count mismatch. Expected {initial_count + 1}, got {len(data_final)}")

    # Cleanup test data from json (optional, but good to keep clean)
    # We can remove the test entries
    cleaned_data = [m for m in data_final if m.get('match_id') not in ['TEST_001', 'TEST_002']]
    with open(JSON_FILE, 'w', encoding='utf-8') as f:
        json.dump(cleaned_data, f, ensure_ascii=False, indent=2)
    print("Cleaned up test data.")

if __name__ == "__main__":
    setup()
    verify()
    teardown()
