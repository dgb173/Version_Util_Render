
import sys
import json
import shutil
from pathlib import Path

# Setup path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'src'))
from modules import data_manager

# Config
TEST_FILE = "data_ah_test_clean.json"
DATA_DIR = data_manager.DATA_DIR
TEST_FILE_PATH = DATA_DIR / TEST_FILE

def setup_test_data():
    """Creates a test JSON file with valid and invalid entries."""
    data = [
        # Valid
        {
            "match_id": "valid_1",
            "score": "1-0",
            "last_home_match": {"match_id": "h1"},
            "last_away_match": {"match_id": "a1"}
        },
        # Invalid: Score ??
        {
            "match_id": "invalid_score_1",
            "score": "??",
            "last_home_match": {"match_id": "h1"},
            "last_away_match": {"match_id": "a1"}
        },
        # Invalid: Missing Home History
        {
            "match_id": "invalid_hist_1",
            "score": "1-0",
            # "last_home_match": ... missing
            "last_away_match": {"match_id": "a1"}
        },
        # Invalid: Home History is None
        {
            "match_id": "invalid_hist_2",
            "score": "1-0",
            "last_home_match": None,
            "last_away_match": {"match_id": "a1"}
        },
         # Invalid: Missing Away History
        {
            "match_id": "invalid_hist_3",
            "score": "1-0",
            "last_home_match": {"match_id": "h1"},
            # "last_away_match": ... missing
        },
         # Invalid: Empty Dict History
        {
            "match_id": "invalid_hist_4",
            "score": "1-0",
            "last_home_match": {},
            "last_away_match": {"match_id": "a1"}
        }
    ]
    
    with open(TEST_FILE_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
        
    print(f"Created test file with {len(data)} entries (1 valid, 5 invalid).")

def run_test():
    setup_test_data()
    
    print("Running cleaning...")
    removed, total = data_manager.clean_bucket(TEST_FILE)
    
    print(f"Removed: {removed}, Original Total: {total}")
    
    # Reload to verify
    with open(TEST_FILE_PATH, 'r', encoding='utf-8') as f:
        remaining_data = json.load(f)
        
    print(f"Remaining entries: {len(remaining_data)}")
    
    success = True
    if len(remaining_data) != 1:
        print("FAIL: Expected 1 remaining entry.")
        success = False
    
    if remaining_data[0]['match_id'] != "valid_1":
        print("FAIL: Remaining entry is wrong.")
        success = False
        
    if success:
        print("PASS: Valid match preserved, invalid ones removed.")
        
    # Cleanup
    if TEST_FILE_PATH.exists():
        TEST_FILE_PATH.unlink()

if __name__ == "__main__":
    run_test()
