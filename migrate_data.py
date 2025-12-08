import json
import os
from pathlib import Path
from src.modules import data_manager

OLD_DATA_FILE = "studied_matches/history.json"

def migrate():
    global OLD_DATA_FILE
    OLD_DATA_FILE = "studied_matches/history.json.bak"
    if not os.path.exists(OLD_DATA_FILE):
        print(f"{OLD_DATA_FILE} not found.")
        return

    print("Loading old data...")
    try:
        with open(OLD_DATA_FILE, 'r', encoding='utf-8') as f:
            old_data = json.load(f)
    except Exception as e:
        print(f"Error loading {OLD_DATA_FILE}: {e}")
        return

    print(f"Found {len(old_data)} matches. Starting migration...")
    
    count = 0
    skipped = 0
    
    for match in old_data:
        # data_manager.save_match handles filtering (AH 3/-3, Score ??) and bucket selection
        saved = data_manager.save_match(match)
        if saved:
            count += 1
        else:
            skipped += 1
            
        if (count + skipped) % 100 == 0:
            print(f"Processed {count + skipped}/{len(old_data)}...")

    print(f"Migration complete.")
    print(f"Transferred: {count}")
    print(f"Skipped (Filtered): {skipped}")
    
    # Rename old file to backup
    os.rename(OLD_DATA_FILE, "data_backup_legacy.json")
    print(f"Renamed {OLD_DATA_FILE} to data_backup_legacy.json")

if __name__ == "__main__":
    migrate()
