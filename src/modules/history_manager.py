import json
import os
import sys
import time
from pathlib import Path
import logging

# Cross-platform file locking
if sys.platform == 'win32':
    import msvcrt
else:
    import fcntl

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the path to history.json
BASE_DIR = Path(__file__).resolve().parent.parent
HISTORY_DIR = BASE_DIR / 'studied_matches'
HISTORY_FILE = HISTORY_DIR / 'history.json'

def _ensure_history_file():
    """Ensures the history file and directory exist with the correct structure."""
    if not HISTORY_DIR.exists():
        HISTORY_DIR.mkdir(parents=True)
    
    if not HISTORY_FILE.exists():
        initial_structure = {
            "pending": {},
            "cached": {}
        }
        with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(initial_structure, f, indent=2)
        return initial_structure
    return None

class FileLock:
    """Cross-platform context manager for file locking."""
    def __init__(self, file_path):
        self.file_path = file_path
        self.file_handle = None

    def __enter__(self):
        self.file_handle = open(self.file_path, 'r+')
        while True:
            try:
                if sys.platform == 'win32':
                    msvcrt.locking(self.file_handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    fcntl.flock(self.file_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except (IOError, OSError):
                time.sleep(0.1)
        return self.file_handle

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.file_handle.seek(0)
            if sys.platform == 'win32':
                msvcrt.locking(self.file_handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(self.file_handle.fileno(), fcntl.LOCK_UN)
        except (IOError, OSError):
            pass
        self.file_handle.close()

def load_history():
    """Loads the history data. Not thread-safe on its own, use with care or for read-only."""
    _ensure_history_file()
    try:
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return {"pending": {}, "cached": {}}

def save_history(data):
    """Saves the history data. Not thread-safe on its own."""
    _ensure_history_file()
    with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)

def add_pending_matches(season, league_id, match_data_list):
    """
    Adds match IDs (and optional AH data) to the pending list.
    Thread-safe / Process-safe using file locking.
    """
    _ensure_history_file()
    
    season = str(season)
    league_id = str(league_id)
    
    # Use lock for read-modify-write
    with FileLock(HISTORY_FILE) as f:
        try:
            f.seek(0)
            data = json.load(f)
        except json.JSONDecodeError:
            data = {"pending": {}, "cached": {}}
        
        if season not in data["pending"]:
            data["pending"][season] = {}
        
        if league_id not in data["pending"][season]:
            data["pending"][season][league_id] = []
        
        current_list = data["pending"][season][league_id]
        
        # Helper to get ID from item
        def get_id(item):
            return item['id'] if isinstance(item, dict) else str(item)
        
        # Get existing IDs for quick lookup
        existing_ids = set()
        for item in current_list:
            existing_ids.add(get_id(item))
            
        # Add only new IDs
        added_count = 0
        for item in match_data_list:
            mid_str = get_id(item)
            
            # Check if it's already in pending OR cached
            is_cached = False
            if season in data["cached"] and league_id in data["cached"][season]:
                if mid_str in data["cached"][season][league_id]:
                    is_cached = True
            
            if mid_str not in existing_ids and not is_cached:
                if isinstance(item, dict):
                    current_list.append(item)
                else:
                    current_list.append({'id': mid_str, 'ah': 'N/A'})
                
                existing_ids.add(mid_str)
                added_count += 1
        
        # Write back
        f.seek(0)
        f.truncate()
        json.dump(data, f, indent=2)
        
    return added_count

def move_to_cached(season, league_id, match_id):
    """
    Moves a match ID from pending to cached.
    Thread-safe / Process-safe using file locking.
    """
    _ensure_history_file()
    season = str(season)
    league_id = str(league_id)
    match_id = str(match_id)
    
    with FileLock(HISTORY_FILE) as f:
        try:
            f.seek(0)
            data = json.load(f)
        except json.JSONDecodeError:
            data = {"pending": {}, "cached": {}}
            
        # Remove from pending
        # Need to handle both string IDs and objects in pending list
        if season in data["pending"] and league_id in data["pending"][season]:
            pending_list = data["pending"][season][league_id]
            # Filter out the match_id
            new_list = [m for m in pending_list if (m['id'] if isinstance(m, dict) else m) != match_id]
            
            if len(new_list) != len(pending_list):
                data["pending"][season][league_id] = new_list
                
                # Clean up empty lists
                if not data["pending"][season][league_id]:
                    del data["pending"][season][league_id]
                if not data["pending"][season]:
                    del data["pending"][season]
        
        # Add to cached
        if season not in data["cached"]:
            data["cached"][season] = {}
        if league_id not in data["cached"][season]:
            data["cached"][season][league_id] = []
            
        if match_id not in data["cached"][season][league_id]:
            data["cached"][season][league_id].append(match_id)
            
        # Write back
        f.seek(0)
        f.truncate()
        json.dump(data, f, indent=2)

def get_pending_matches():
    """Returns the pending matches structure. Read-only, no lock needed for simple read."""
    return load_history().get("pending", {})
