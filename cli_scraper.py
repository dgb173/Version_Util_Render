import sys
import os
import json
import time
import logging
import argparse
from pathlib import Path

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from modules import history_manager
from app import analizar_partido_completo, save_match_to_json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [Worker %(worker)s] - %(message)s')

# Re-use FileLock from history_manager for data.json safety
from modules.history_manager import FileLock

DATA_JSON_PATH = Path(__file__).resolve().parent / 'src' / 'data' / 'data.json'

def normalize_ah(value):
    """Normalize AH value to match filter logic (0.25/0.75 -> 0.5)."""
    if not value or value == 'N/A': return None
    try:
        float_val = float(value)
        abs_val = abs(float_val)
        base = int(abs_val)
        frac = abs_val - base
        
        bucket = base
        if abs(frac - 0.0) > 0.01: # If not 0.0
            bucket = base + 0.5
            
        return -bucket if float_val < 0 else bucket
    except ValueError:
        return None

from modules import data_manager

def save_match_safe(match_data):
    """Saves match data using data_manager (split buckets)."""
    # data_manager handles locking and buckets
    data_manager.save_match(match_data)

def run_scraper(worker_index, total_workers, ah_filter=None, job_file=None):
    logger = logging.getLogger(f"Worker-{worker_index}")
    extra = {'worker': worker_index}
    logger = logging.LoggerAdapter(logger, extra)
    
    print(f"Worker {worker_index}/{total_workers} started.")
    
    all_pending = []
    
    if job_file and os.path.exists(job_file):
        print(f"Worker {worker_index}: Loading matches from job file: {job_file}")
        try:
            with open(job_file, 'r') as f:
                all_pending = json.load(f)
        except Exception as e:
            print(f"Worker {worker_index}: Error loading job file: {e}")
            return
    else:
        # Fallback to loading from history (legacy mode)
        print(f"Worker {worker_index}: Loading from history (legacy mode)")
        pending_structure = history_manager.get_pending_matches()
        for season, leagues in pending_structure.items():
            for league_id, matches in leagues.items():
                for m in matches:
                    m_obj = m if isinstance(m, dict) else {'id': m, 'ah': 'N/A'}
                    m_obj['season'] = season
                    m_obj['league_id'] = league_id
                    all_pending.append(m_obj)
        
        # Filter by AH only in legacy mode (job file should already be filtered)
        if ah_filter and ah_filter != 'all':
            filtered_pending = []
            filter_val = float(ah_filter)
            for m in all_pending:
                norm_ah = normalize_ah(m.get('ah'))
                if norm_ah is not None and norm_ah == filter_val:
                    filtered_pending.append(m)
            all_pending = filtered_pending

    if not all_pending:
        print(f"Worker {worker_index}: No matches to process. Exiting.")
        return

    # Select matches for this worker
    my_matches = [m for i, m in enumerate(all_pending) if i % total_workers == worker_index]
    
    if not my_matches:
        print(f"Worker {worker_index}: No matches assigned (Total: {len(all_pending)}). Exiting.")
        return
        
    print(f"Worker {worker_index}: Processing {len(my_matches)} matches...")
    
    processed_count = 0
    for match in my_matches:
        match_id = match['id']
        # print(f"Worker {worker_index}: Scraping {match_id}...") # Reduce spam
        
        try:
            # Scrape
            match_data = analizar_partido_completo(match_id, force_refresh=True)
            
            if "error" in match_data:
                print(f"Worker {worker_index}: Error scraping {match_id}: {match_data['error']}")
                history_manager.move_to_cached(match['season'], match['league_id'], match_id)
                continue
            
            # Save
            save_match_safe(match_data)
            
            # Update History
            history_manager.move_to_cached(match['season'], match['league_id'], match_id)
            
            processed_count += 1
            if processed_count % 5 == 0:
                 print(f"Worker {worker_index}: Progress {processed_count}/{len(my_matches)}")
            
        except Exception as e:
            print(f"Worker {worker_index}: Critical error on {match_id}: {e}")
            try:
                history_manager.move_to_cached(match['season'], match['league_id'], match_id)
            except:
                pass

    print(f"Worker {worker_index}: Finished. Processed {processed_count} matches.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='CLI Scraper Worker')
    parser.add_argument('--index', type=int, required=True, help='Worker index (0-based)')
    parser.add_argument('--total', type=int, required=True, help='Total number of workers')
    parser.add_argument('--ah', type=str, default='all', help='AH Filter value (legacy)')
    parser.add_argument('--job_file', type=str, default=None, help='Path to JSON file with matches to scrape')
    
    args = parser.parse_args()
    
    run_scraper(args.index, args.total, args.ah, args.job_file)
