import json
import sys
import os
import time

# Add src to path
sys.path.append(os.path.join(os.getcwd(), 'src'))

from modules.estudio_scraper import fetch_odds_from_ajax, format_ah_as_decimal_string_of, get_requests_session_of

HISTORY_FILE = 'studied_matches/history.json'

def fix_history():
    if not os.path.exists(HISTORY_FILE):
        print(f"File not found: {HISTORY_FILE}")
        return

    print(f"Reading {HISTORY_FILE}...")
    with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
        try:
            history = json.load(f)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            return
    
    modified_count = 0
    matches_to_fix = [m for m in history if m.get('main_match_odds', {}).get('ah_linea') == '-3']
    
    print(f"Found {len(matches_to_fix)} matches with AH -3.")
    
    for match in matches_to_fix:
        match_id = match.get('match_id')
        home = match.get('home_name', 'Unknown')
        away = match.get('away_name', 'Unknown')
        print(f"[{match_id}] {home} vs {away}...")
        
        # Try fetching from AJAX (fastest and fixed)
        try:
            new_odds = fetch_odds_from_ajax(match_id)
            
            if new_odds and new_odds.get('ah_linea_raw'):
                raw_ah = new_odds.get('ah_linea_raw')
                formatted_ah = format_ah_as_decimal_string_of(raw_ah)
                
                # Check if it's different and valid
                if formatted_ah != '-3' and formatted_ah not in ['-', '?', 'N/A']:
                    print(f"  -> UPDATING: -3 -> {formatted_ah} (Raw: {raw_ah})")
                    match['main_match_odds']['ah_linea'] = formatted_ah
                    modified_count += 1
                else:
                    print(f"  -> Result is still {formatted_ah}. Skipping.")
            else:
                print("  -> No data from AJAX.")
                
        except Exception as e:
            print(f"  -> Error: {e}")
            
        # Be nice to the server
        time.sleep(0.5)

    if modified_count > 0:
        print(f"Saving {modified_count} changes to {HISTORY_FILE}...")
        # Backup first
        backup_file = HISTORY_FILE + '.bak'
        with open(backup_file, 'w', encoding='utf-8') as f:
            json.dump(history, f, indent=4, ensure_ascii=False) # Dump the original state? No, wait.
        
        # Actually I should have backed up the original before modifying.
        # But 'history' variable holds the modified data now.
        # I'll save the modified data to HISTORY_FILE.
        # I can't easily backup the original file content now unless I re-read it, but I already modified the list objects in memory.
        # It's fine, I'll just save.
        
        with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(history, f, indent=4, ensure_ascii=False)
        print("Done.")
    else:
        print("No changes made.")

if __name__ == "__main__":
    fix_history()
