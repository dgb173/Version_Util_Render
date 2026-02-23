
import sys
import os
import re
import json
import requests
import concurrent.futures
from pathlib import Path

# Setup paths
script_dir = Path(__file__).resolve().parent
project_dir = script_dir.parent
src_dir = project_dir / 'src'
sys.path.insert(0, str(src_dir))
sys.path.insert(0, str(project_dir))
os.chdir(project_dir)

from modules.estudio_scraper import analizar_partido_completo
from scripts.rule_applier import get_rule_applier

# Portugal 2 2024-2025
LIGA_URL = "https://football.nowgoal26.com/jsData/matchResult/2024-2025/s157_1787_en.js"
MAX_MATCHES = 150

def get_league_match_ids(url):
    try:
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0',
            'Referer': 'https://football.nowgoal26.com/',
        })
        resp = session.get(url, timeout=15)
        if resp.status_code != 200: return []
        matches_raw = re.findall(r'jh\["R_\d+"\]\s*=\s*\[(.*?)\];', resp.text, re.DOTALL)
        ids = []
        for round_data in matches_raw:
            m_list = re.findall(r'\[([^\[\]]+)\]', round_data)
            for m_raw in m_list:
                parts = m_raw.split(',')
                if len(parts) < 10: continue
                mid = parts[0].strip()
                score = parts[6].strip().strip("'")
                if score and '-' in score and score != '?-?':
                    ids.append(mid)
        return ids
    except: return []

def calculate_profit(match_data, is_home_fav, odds=1.8):
    final_score = match_data.get('final_score')
    if not final_score: return 0.0, 0.0
    try:
        parts = re.findall(r'\d+', final_score)
        gh, ga = int(parts[0]), int(parts[1])
        ah_str = match_data.get('main_match_odds', {}).get('ah_linea')
        ah = float(ah_str) if ah_str and ah_str != '-' else 0
        
        diff = gh - ga - ah
        if diff >= 0.5: outcome_h = 'WIN'
        elif diff == 0.25: outcome_h = 'HALF_WIN'
        elif diff == 0: outcome_h = 'PUSH'
        elif diff == -0.25: outcome_h = 'HALF_LOSS'
        else: outcome_h = 'LOSS'
        
        def prof(res):
            if res == 'WIN': return odds - 1
            if res == 'HALF_WIN': return (odds-1)/2
            if res == 'HALF_LOSS': return -0.5
            if res == 'LOSS': return -1
            return 0
            
        fav_p = prof(outcome_h) if is_home_fav else prof('LOSS' if outcome_h=='WIN' else 'WIN' if outcome_h=='LOSS' else 'HALF_LOSS' if outcome_h=='HALF_WIN' else 'HALF_WIN' if outcome_h=='HALF_LOSS' else 'PUSH')
        und_p = prof('LOSS' if outcome_h=='WIN' else 'WIN' if outcome_h=='LOSS' else 'HALF_LOSS' if outcome_h=='HALF_WIN' else 'HALF_WIN' if outcome_h=='HALF_LOSS' else 'PUSH') if is_home_fav else prof(outcome_h)
        
        return fav_p, und_p
    except: return 0.0, 0.0

def extract_worker(mid):
    try:
        data = analizar_partido_completo(str(mid))
        if not data: return None
        applier = get_rule_applier()
        feats = applier.get_match_features(data)
        if not feats: return None
        
        # Determine fav profit
        ah_str = data.get('main_match_odds', {}).get('ah_linea')
        ah_val = float(ah_str) if ah_str and ah_str!='-' else 0
        is_home_fav = ah_val >= 0
        fav_p, und_p = calculate_profit(data, is_home_fav)
        
        feats['fav_p'] = fav_p
        feats['und_p'] = und_p
        feats['mid'] = mid
        return feats
    except: return None

def main():
    ids = get_league_match_ids(LIGA_URL)[::-1][:MAX_MATCHES]
    print(f"Extracting features for {len(ids)} matches...")
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(extract_worker, mid): mid for mid in ids}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res: results.append(res)
            if len(results) % 10 == 0: print(f"Progress: {len(results)} matches")
            
    output_file = project_dir / "scripts" / "portugal2_features.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print(f"Done! Saved {len(results)} records to {output_file}")

if __name__ == "__main__":
    main()
