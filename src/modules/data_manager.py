import json
import os
import threading
from pathlib import Path

# Config
DATA_DIR = Path(__file__).resolve().parent.parent.parent / 'data'
DATA_DIR.mkdir(exist_ok=True)

# Locks for each bucket file to ensure thread safety
_locks = {}
_global_lock = threading.Lock()

def get_bucket_name(ah_val):
    """
    Determines the filename bucket for a given AH value.
    Buckets:
    - 0: 0
    - 0.5: 0.25, 0.5, 0.75
    - -0.5: -0.25, -0.5, -0.75
    - 1.5: 1.0, 1.25, 1.5, 1.75
    - -1.5: -1.0, -1.25, -1.5, -1.75
    - 2_plus: >= 2.0
    - minus_2_plus: <= -2.0
    """
    if ah_val is None or ah_val == 'N/A':
        return "data_unknown.json"
    
    try:
        val = float(ah_val)
    except ValueError:
        return "data_unknown.json"

    # Filter out erroneous 3 / -3 as requested (though this function just returns bucket, 
    # filtering should happen before saving)
    
    if val == 0:
        return "data_ah_0.json"
    
    abs_val = abs(val)
    sign = "minus_" if val < 0 else ""
    
    if 0.25 <= abs_val <= 0.75:
        return f"data_{sign}ah_0.5.json"
    
    if 1.0 <= abs_val <= 1.75:
        return f"data_{sign}ah_1.5.json"
        
    if abs_val >= 2.0:
        return f"data_{sign}ah_2_plus.json"
        
    return "data_others.json" # Should not happen with standard AH

def get_file_lock(filename):
    with _global_lock:
        if filename not in _locks:
            _locks[filename] = threading.Lock()
        return _locks[filename]

def save_match(match_data):
    """
    Saves a single match to its appropriate JSON bucket.
    Thread-safe.
    """
    # 1. Clean/Validate
    ah = match_data.get('handicap')
    if ah is None:
        ah = match_data.get('main_match_odds', {}).get('ah_linea')
        
    score = match_data.get('score')
    if score is None:
        score = match_data.get('final_score')
    
    # Filter: AH 3 or -3
    if ah in [3, 3.0, '3', '3.0', -3, -3.0, '-3', '-3.0']:
        print(f"Skipping match {match_data.get('match_id')} with AH {ah}")
        return False
        
    # Filter: Score "??" -> Save to pending results
    if score == "??" or score == "?-?":
        print(f"Saving match {match_data.get('match_id')} to pending results (score {score})")
        bucket_name = "data_pending_results.json"
        # Skip the rest of the logic and save directly to this bucket
        file_path = DATA_DIR / bucket_name
        lock = get_file_lock(bucket_name)
        with lock:
            data = []
            if file_path.exists():
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                except json.JSONDecodeError:
                    data = []
            
            match_id = match_data.get('match_id')
            existing_idx = next((i for i, m in enumerate(data) if m.get('match_id') == match_id), -1)
            
            if existing_idx >= 0:
                data[existing_idx] = match_data
            else:
                data.append(match_data)
                
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        return True

    bucket_name = get_bucket_name(ah)
    file_path = DATA_DIR / bucket_name
    
    lock = get_file_lock(bucket_name)
    
    with lock:
        # Load existing
        data = []
        if file_path.exists():
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except json.JSONDecodeError:
                data = []
        
        # Update or Append
        match_id = match_data.get('match_id')
        existing_idx = next((i for i, m in enumerate(data) if m.get('match_id') == match_id), -1)
        
        if existing_idx >= 0:
            data[existing_idx] = match_data
        else:
            data.append(match_data)
            
        # Save
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            
    return True

def load_all_matches():
    """Loads matches from ALL buckets."""
    all_matches = []
    for file in DATA_DIR.glob("data_*.json"):
        try:
            with open(file, 'r', encoding='utf-8') as f:
                all_matches.extend(json.load(f))
        except:
            pass
    return all_matches

def load_matches_by_bucket(ah_filter):
    """
    Loads matches from the specific bucket(s) relevant to the filter.
    If ah_filter is 'all', loads everything.
    """
    if not ah_filter or ah_filter == 'all':
        return load_all_matches()
        
    # Determine which file this AH belongs to
    bucket = get_bucket_name(ah_filter)
    file_path = DATA_DIR / bucket
    
    if file_path.exists():
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return []
    return []

# --- Pre-Cacheo Functions ---
PRECACHEO_FILE = DATA_DIR / "data_precacheo.json"
_precacheo_lock = threading.Lock()

def save_precacheo_match(match_data):
    """Saves a match to the pre-cacheo JSON (upcoming matches without final result)."""
    with _precacheo_lock:
        data = []
        if PRECACHEO_FILE.exists():
            try:
                with open(PRECACHEO_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except json.JSONDecodeError:
                data = []
        
        match_id = match_data.get('match_id')
        existing_idx = next((i for i, m in enumerate(data) if m.get('match_id') == match_id), -1)
        
        if existing_idx >= 0:
            data[existing_idx] = match_data
        else:
            data.append(match_data)
            
        with open(PRECACHEO_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    return True

def load_precacheo_matches():
    """Loads all pre-cached matches."""
    if PRECACHEO_FILE.exists():
        try:
            with open(PRECACHEO_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return []
    return []

def remove_from_precacheo(match_id):
    """Removes a match from precacheo after it's finalized."""
    with _precacheo_lock:
        data = load_precacheo_matches()
        initial_len = len(data)
        data = [m for m in data if str(m.get('match_id')) != str(match_id)]
        
        if len(data) < initial_len:
            with open(PRECACHEO_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
    return True

def get_precacheo_match(match_id):
    """Gets a single match from precacheo by ID."""
    matches = load_precacheo_matches()
    for m in matches:
        if str(m.get('match_id')) == str(match_id):
            return m
    return None

def finalize_precacheo_batch(match_ids):
    """
    Finalizes a batch of matches:
    1. Reads precacheo ONCE.
    2. Identifies matches with results.
    3. Groups them by target bucket.
    4. Writes to buckets efficiently (locking each bucket once).
    5. Removes from precacheo ONCE.
    Returns: (count_success, count_failed, errors_list)
    """
    success_count = 0
    errors = []
    
    with _precacheo_lock:
        # 1. Load Precacheo
        if not PRECACHEO_FILE.exists():
            return 0, len(match_ids), ["Precacheo file not found"]
            
        try:
            with open(PRECACHEO_FILE, 'r', encoding='utf-8') as f:
                precacheo_data = json.load(f)
        except json.JSONDecodeError:
            return 0, len(match_ids), ["Precacheo file corrupt"]

        # Map for fast lookup
        precacheo_map = {str(m.get('match_id')): m for m in precacheo_data}
        
        matches_to_move = []
        ids_to_remove = set()
        
        # 2. Identify candidates
        for mid in match_ids:
            mid_str = str(mid)
            match = precacheo_map.get(mid_str)
            if not match:
                errors.append(f"Match {mid} not found in precacheo")
                continue
                
            # Check validity (has score)
            score = match.get('score') or match.get('final_score')
            if not score or score in ['??', '?-?', '? - ?']:
                errors.append(f"Match {mid} has no result ({score})")
                continue
                
            matches_to_move.append(match)
            ids_to_remove.add(mid_str)

        # 3. Group by bucket
        bucket_actions = {} # filename -> [matches]
        
        for m in matches_to_move:
            ah = m.get('handicap')
            if ah is None:
                ah = m.get('main_match_odds', {}).get('ah_linea')
            
            score = m.get('score') or m.get('final_score')
            if score == "??" or score == "?-?":
                 b_name = "data_pending_results.json"
            else:
                 b_name = get_bucket_name(ah)
                 
            if b_name not in bucket_actions:
                bucket_actions[b_name] = []
            bucket_actions[b_name].append(m)

        # 4. Write to buckets
        for filename, matches in bucket_actions.items():
            file_path = DATA_DIR / filename
            lock = get_file_lock(filename)
            
            with lock:
                # Load existing
                existing_data = []
                if file_path.exists():
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            existing_data = json.load(f)
                    except:
                        existing_data = []
                
                # Update/Append in memory
                existing_map = {str(item.get('match_id')): i for i, item in enumerate(existing_data)}
                
                for new_m in matches:
                    nid = str(new_m.get('match_id'))
                    if nid in existing_map:
                        existing_data[existing_map[nid]] = new_m
                    else:
                        existing_data.append(new_m)
                        existing_map[nid] = len(existing_data) - 1
                
                # Write back
                try:
                    with open(file_path, 'w', encoding='utf-8') as f:
                        json.dump(existing_data, f, indent=2, ensure_ascii=False)
                    success_count += len(matches)
                except Exception as e:
                    errors.append(f"Failed to write to {filename}: {str(e)}")

        # 5. Remove from Precacheo
        if ids_to_remove:
            new_precacheo_data = [m for m in precacheo_data if str(m.get('match_id')) not in ids_to_remove]
            try:
                with open(PRECACHEO_FILE, 'w', encoding='utf-8') as f:
                    json.dump(new_precacheo_data, f, indent=2, ensure_ascii=False)
            except Exception as e:
                errors.append(f"Failed to update precacheo file: {str(e)}")

    return success_count, len(match_ids) - success_count, errors

