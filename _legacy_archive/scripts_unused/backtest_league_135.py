import sys
import os
import json
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add parent dir to path to import modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
from modules import league_scraper, estudio_scraper

# Add current dir to path to import mega_trainer logic
sys.path.append(os.path.dirname(__file__))
from mega_trainer import extract_mega_features, get_ah_result, get_ou_result

SEASON = "2024-2025"
LEAGUE_ID = 135 # Serie A
TARGET_SAMPLES = 20

def scrape_match_details(mid):
    try:
        # We need FULL details (stats, previous matches) for the patterns
        data = estudio_scraper.analizar_partido_completo(mid)
        # estudio_scraper returns a dict with 'id', 'score', 'features', etc.
        # But mega_trainer expects specific structure. 
        # Analyzing `estudio_scraper.py` (assumed) output usually matches `data.json` format.
        if data:
             # Ensure AH is float
            if 'handicap' in data:
                 data['ah'] = float(data['handicap'])
            return data
    except Exception as e:
        # print(f"Error scraping {mid}: {e}")
        pass
    return None

def load_validated_patterns():
    path = os.path.join(os.path.dirname(__file__), 'mega_patterns.json')
    if not os.path.exists(path):
        print("Error: No existen patrones validados. Ejecuta mega_trainer.py primero.")
        return [], []
    
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Filter only validated ones (same logic as generate_patterns_js.py)
    valid_ah = [p for p in data['ah_patterns'] 
            if p.get('test_accuracy', 0) >= 0.6 and p.get('test_samples', 0) >= 3]
            
    if len(valid_ah) < 5:
         print("Warning: Using Training-only patterns due to lack of validated ones")
         valid_ah = [p for p in data['ah_patterns'] if p['accuracy'] >= 0.8][:30]

    return valid_ah, data['ou_patterns']

def test_system():
    print(f"🎯 BACKTEST LIGA {LEAGUE_ID} (Serie A) - {SEASON}")
    print("=" * 60)
    
    # 1. Get IDs
    print(f"📋 Obteniendo IDs de la liga {LEAGUE_ID}...")
    result = league_scraper.extract_ids_by_params(SEASON, LEAGUE_ID)
    match_list = result.get('match_data', [])
    
    # Filter matches that have finished (have a score)
    # Usually `match_data` from league_scraper has basic info.
    # We want matches that HAVE a score to verify the prediction.
    # But league_scraper might return all matches.
    
    # Simply shuffle and take 100 IDs. We will check if they have results during scraping.
    all_ids = [m['id'] for m in match_list]
    print(f"✅ Total partidos encontrados: {len(all_ids)}")
    
    if len(all_ids) == 0:
        print("❌ No se encontraron partidos. Revisa el ID de liga o la conexión.")
        return

    # Shuffle
    random.shuffle(all_ids)
    selected_ids = all_ids[:min(len(all_ids), 150)] # Take a bit more to ensure 100 valid
    
    print(f"\n🔍 Scrapeando detalles de {len(selected_ids)} partidos para el test...")
    matches = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(scrape_match_details, mid): mid for mid in selected_ids}
        for i, future in enumerate(as_completed(futures)):
            m = future.result()
            if m:
                # Check for Valid Result
                score = m.get('final_score') or m.get('score')
                if score and ':' in str(score) and score not in ['?-?', '??']:
                    matches.append(m)
            
            print(f"   Scrapeados: {len(matches)}/{TARGET_SAMPLES}", end='\r')
            if len(matches) >= TARGET_SAMPLES:
                break
                
    matches = matches[:TARGET_SAMPLES]
    print(f"\n✅ {len(matches)} partidos válidos para el test.")
    
    if not matches:
        return

    # 2. Load Patterns
    patterns_ah, patterns_ou = load_validated_patterns()
    print(f"🧠 Cargados {len(patterns_ah)} patrones AH y {len(patterns_ou)} patrones O/U")
    
    # 3. Simulate
    print("\n🎲 Ejecutando simulación...")
    
    bets = 0
    hits = 0
    balance = 0
    staked = 0
    
    for m in matches:
        f = extract_mega_features(m)
        current_ah_res = get_ah_result(m)
        
        # Checking AH Patterns
        pick = None
        algo_name = ""
        
        # Logic matching generate_patterns_js.py priority
        for idx, p in enumerate(patterns_ah):
            # Check features
            match = True
            for feat in p['features']:
                if not f.get(feat, False):
                    match = False
                    break
            
            if match:
                pick = p['target']
                algo_name = f"ML_{idx} ({p.get('test_accuracy',0):.2f})"
                break # First match priority
        
        if pick:
            bets += 1
            staked += 1
            is_win = (pick == current_ah_res)
            
            # Simple odds simulation (1.90)
            payout = 1.90 if is_win else 0
            profit = payout - 1
            balance += profit
            
            icon = "✅" if is_win else "❌"
            # print(f"{icon} {m['home_team']} vs {m['away_team']} -> Pick: {pick} (Real: {current_ah_res}) [{algo_name}]")
            
            if is_win: hits += 1
            
    print("\n" + "="*60)
    print("📊 RESULTADOS DEL BACKTEST")
    print("="*60)
    print(f"Partidos Testeados: {len(matches)}")
    print(f"Apuestas Realizadas: {bets}")
    if bets > 0:
        win_rate = (hits / bets) * 100
        roi = (balance / staked) * 100
        print(f"Aciertos: {hits} ({win_rate:.1f}%)")
        print(f"ROI Estimado (cuota 1.90): {roi:.1f}%")
        print(f"Balance (u): {balance:.2f}u")
    else:
        print("No se encontraron setups (pattern match 0).")

if __name__ == "__main__":
    main_func = test_system
    main_func()
