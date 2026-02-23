import sys
import os
print("--- VERSION 2 ---")
sys.path.append(os.path.join(os.getcwd(), 'src'))

import concurrent.futures
import time
import logging
from src.modules.estudio_scraper import analizar_partido_completo

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def scrape_match(match_id):
    try:
        start_time = time.time()
        logger.info(f"Starting scrape for {match_id}")
        # force_refresh=True to ensure we actually scrape and don't just hit cache
        data = analizar_partido_completo(match_id, force_refresh=True)
        duration = time.time() - start_time
        
        # Basic validation
        if not data:
            return {"id": match_id, "status": "failed", "reason": "No data returned"}
            
        keys_to_check = ["match_id", "home_name", "away_name"]
        missing = [k for k in keys_to_check if k not in data]
        
        if missing:
            return {"id": match_id, "status": "incomplete", "missing": missing}
            
        return {
            "id": match_id, 
            "status": "success", 
            "duration": duration, 
            "home": data.get("home_name"), 
            "away": data.get("away_name")
        }
        
    except Exception as e:
        return {"id": match_id, "status": "error", "error": str(e)}

def test_parallel_execution():
    # Test matches (taken from history.json pending list)
    test_ids = ["2587775", "2587776", "2587777", "2587778", "2587779"]
    
    print("--- Running Sequential Check on First Match ---")
    # force_refresh=True is critical
    data = analizar_partido_completo(test_ids[0], force_refresh=True)
    
    try:
        print(f"Full Data Returned Keys: {list(data.keys())}")
        print(f"Full Data Returned (Safe): {repr(data)}")
    except Exception as e:
        print(f"Error printing data: {e}")
    
    seq_res = scrape_match(test_ids[0])
    print(f"Sequential Result Wrapper: {seq_res}")
    
    if seq_res['status'] != 'success':
        print("Sequential check failed! Aborting parallel test.")
        return

    print(f"\n--- Starting Parallel Test with {len(test_ids)} matches (5 workers) ---")
    start_total = time.time()
    
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_id = {executor.submit(scrape_match, mid): mid for mid in test_ids}
        
        for future in concurrent.futures.as_completed(future_to_id):
            mid = future_to_id[future]
            try:
                res = future.result()
                results.append(res)
                print(f"Match {mid}: {res['status']} ({res.get('duration', 0):.2f}s)")
            except Exception as exc:
                print(f"Match {mid} generated an exception: {exc}")

    total_duration = time.time() - start_total
    print(f"\n--- Test Completed in {total_duration:.2f}s ---")
    
    success_count = sum(1 for r in results if r['status'] == 'success')
    print(f"Success: {success_count}/{len(test_ids)}")
    
    for r in results:
        if r['status'] != 'success':
            print(f"Failed: {r}")

if __name__ == "__main__":
    test_parallel_execution()
