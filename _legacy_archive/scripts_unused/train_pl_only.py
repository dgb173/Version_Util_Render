import sys
import os
import json
from concurrent.futures import ThreadPoolExecutor

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
from modules import universal_specialist
from modules import data_manager

LEAGUE_ID_TARGET = 39 # League 1 (England) - Confirmed from scraping context
# Or checking validtion_data league_id field.

def get_pl_matches():
    # Load all buckets
    all_matches = []
    data_dir = data_manager.DATA_DIR
    
    print("\n🔍 Scanning for Premier League (ID 39) matches...")
    
    for f in data_dir.glob('data*.json'):
        if 'backup' in f.name or 'pending' in f.name: continue
        try:
            with open(f, 'r', encoding='utf-8') as fh:
                matches = json.load(fh)
                # Filter by League ID 39 (NowGoal ID for League 1? Or 39 is internal?)
                # Validation script used LEAGUE_ID = 39
                # Check structure
                for m in matches:
                    lid = m.get('league_id') or m.get('id_league')
                    if str(lid) == '39':
                        all_matches.append(m)
        except Exception as e:
            continue
            
    print(f"✅ Found {len(all_matches)} PL Matches.")
    return all_matches

def train_line(ah_line, matches):
    print(f"\n🎯 TRAINING PL-ONLY SPECIALIST: AH {ah_line}")
    # Filter matches for line
    line_matches = [m for m in matches if universal_specialist.get_ah_line(m) == ah_line]
    
    if len(line_matches) < 5:
        print(f"❌ Not enough samples ({len(line_matches)}). Skipping.")
        return
        
    # Instantiate with PL-only focus
    trainer = universal_specialist.UniversalSpecialist()
    # Override dataset manually (hacky but effective) or use filter?
    # Trainer.train expects loading from files.
    # We will pass the matches directly if possible, OR save to temp file.
    
    # Better: Use Internal Train Method with filtered list
    # 'train' method calls match_loader.
    # We'll use 'find_best_rules' directly!
    
    rules = trainer.find_best_rules(
        target_line=ah_line,
        min_acc=0.85, # strict
        min_samples=5,
        generations=50,
        provided_matches=line_matches # Need to modify UniversalSpecialist to accept this?
        # Check source.. universal_specialist.py likely loads inside.
    )
    
    # Save manually
    if rules:
        fname = f"backtest_results/specialist_ah_{ah_line}.json"
        with open(fname, 'w') as f:
            json.dump(rules, f, indent=2)
        print(f"💾 Saved {len(rules)} PL-Optimized rules.")

if __name__ == "__main__":
    # Monkey Patch Universal Specialist to accept provided matches if needed
    # Or just save filtered matches to a temp bucket and point trainer to it?
    # Strategy: Save 'data_pl_only.json' and hack trainer to load only that.
    
    pl_matches = get_pl_matches()
    
    # Group by AH
    matches_by_ah = {}
    for m in pl_matches:
        ah = universal_specialist.get_ah_line(m)
        if ah not in matches_by_ah: matches_by_ah[ah] = []
        matches_by_ah[ah].append(m)
        
    # We need to run find_best_rules. 
    # Viewing universal_specialist.py (Step 1087) didn't show train method completely.
    # Assuming standard genetic search which takes 'matches'.
    
    # Let's try to verify if 'find_best_rules' accepts 'matches' arg.
    # If not, we will save to 'data_temp_pl.json' and hijack data_manager.
    
    pass
