import sys
import os
import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

from modules import league_scraper
from modules import estudio_scraper
from modules.specialist_validator import validator

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

LEAGUE_ID = 39
SEASON = '2024-2025'
DATA_FILE = f'validation_data_{LEAGUE_ID}_{SEASON}.json'

def get_accuracy_color(acc):
    if acc >= 85: return "\033[92m" # Green
    if acc >= 70: return "\033[93m" # Yellow
    return "\033[91m" # Red

def main():
    print(f"🚀 INICIANDO VALIDACIÓN PREMIER LEAGUE {SEASON} (ID: {LEAGUE_ID})")
    
    # 1. Get IDs
    print("📋 Extrayendo IDs de partidos...")
    matches_meta = league_scraper.extract_ids_by_params(SEASON, LEAGUE_ID)
    
    if "error" in matches_meta:
        print(f"❌ Error extracting IDs: {matches_meta['error']}")
        return

    match_list = matches_meta.get('match_data', [])
    ids = [m['id'] for m in match_list]
    print(f"✅ Se encontraron {len(ids)} partidos.")
    
    # 2. Scrape Data (or load from cache)
    matches_data = []
    
    if os.path.exists(DATA_FILE):
        print(f"📂 Cargando datos cacheados de {DATA_FILE}...")
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                matches_data = json.load(f)
            print(f"✅ Cargados {len(matches_data)} partidos.")
        except:
            print("⚠️ Error cargando cache, se descargará de nuevo.")
    
    # Identify missing IDs
    cached_ids = set(m['match_id'] for m in matches_data)
    missing_ids = [mid for mid in ids if mid not in cached_ids]
    
    if missing_ids:
        print(f"⬇️ Descargando {len(missing_ids)} partidos faltantes (Threads=10)...")
        
        def fetch_match(mid):
            try:
                # Use estudio_scraper to get FULL analysis data
                data = estudio_scraper.analizar_partido_completo(mid)
                return data
            except Exception as e:
                print(f"Error {mid}: {e}")
                return None

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(fetch_match, mid): mid for mid in missing_ids}
            completed = 0
            for future in as_completed(futures):
                res = future.result()
                if res and res.get('match_id'):
                    matches_data.append(res)
                completed += 1
                if completed % 10 == 0:
                    print(f"   Progress: {completed}/{len(missing_ids)} matches scraped...")
        
        # Save cache
        print(f"💾 Guardando datos en {DATA_FILE}...")
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(matches_data, f, indent=2)
            
    # 3. Validation Loop
    print("\n🔍 EJECUTANDO VALIDACIÓN DE SISTEMA...")
    print("="*60)
    
    hits = 0
    total_picks = 0
    ignored_conflicts = 0
    
    by_rule = {}
    
    for m in matches_data:
        # Simulate prediction
        # The validator ignores the current score naturally
        picks = validator.evaluate_match(m)
        
        if not picks:
            continue
            
        # Implement Conflict Guard Logic (Python Version)
        picks_ah = [p for p in picks if p['type'] == 'AH']
        final_pick = None
        
        if picks_ah:
             # Sort by accuracy
             picks_ah.sort(key=lambda x: x['accuracy'], reverse=True)
             
             # Group by prediction
             home_preds = [p for p in picks_ah if p['prediction'] in ['HOME', 'LOCAL']]
             away_preds = [p for p in picks_ah if p['prediction'] in ['AWAY', 'VISITA']]
             
             best_home = home_preds[0] if home_preds else None
             best_away = away_preds[0] if away_preds else None
             
             if best_home and best_away:
                 diff = abs(best_home['accuracy'] - best_away['accuracy'])
                 if diff < 5:
                     ignored_conflicts += 1
                     continue # Conflict!
                 else:
                     final_pick = best_home if best_home['accuracy'] > best_away['accuracy'] else best_away
             elif best_home:
                 final_pick = best_home
             elif best_away:
                 final_pick = best_away
        
        if final_pick:
             # Validate Result
             score_str = m.get('score', '0-0')
             if score_str == '?:?' or score_str == '': continue
             
             try:
                 parts = score_str.replace('-', ':').split(':')
                 hg = int(parts[0])
                 ag = int(parts[1])
                 
                 # Determine Winner for AH (Simplified Validation: Standard AH Win)
                 # Actually, we need to validate against the Handicap Line of the Rule?
                 # OR simply "Did Local Win?" if pick was Local?
                 # Specialist rules usually target AH win.
                 # Let's use the Rule's logic or a standard validator.
                 
                 # Simplest valid check: 
                 # Pick LOCAL -> Local wins or wins AH?
                 # Our rules are usually "Back Local AH -0.5" -> Local Win.
                 # "Back Local AH +0.5" -> Local Win or Draw.
                 
                 # Let's check the TARGET LINE of the rule.
                 target_ah = float(final_pick.get('target_line', 0))
                 
                 # Result from perspective of Home
                 # cover = (hg - ag) + target_ah > 0.25
                 
                 diff_score = (hg - ag) + target_ah
                 
                 is_hit = False
                 if diff_score > 0.25: # Cover
                     is_hit = (final_pick['prediction'] in ['HOME', 'LOCAL'])
                 elif diff_score < -0.25: # No Cover
                     is_hit = (final_pick['prediction'] in ['AWAY', 'VISITA'])
                 else:
                     # Push - Ignore from stats?
                     continue
                 
                 total_picks += 1
                 if is_hit:
                     hits += 1
                     
                 # Record by Rule
                 rname = final_pick['name']
                 if rname not in by_rule: by_rule[rname] = {'h':0, 't':0}
                 by_rule[rname]['t'] += 1
                 if is_hit: by_rule[rname]['h'] += 1
                     
             except: continue

    # Report
    print("="*60)
    print("📊 RESULTADOS VALIDACIÓN PREMIER LEAGUE 24/25")
    acc = (hits / total_picks * 100) if total_picks > 0 else 0
    color = get_accuracy_color(acc)
    
    print(f"Partidos Analizados: {len(matches_data)}")
    print(f"Predicciones Realizadas: {total_picks}")
    print(f"Conflictos Evitados: {ignored_conflicts}")
    print(f"Aciertos: {hits}")
    print(f"{color}PRECISIÓN GLOBAL: {acc:.2f}%\033[0m")
    print("="*60)
    
    print("🔝 TOP REGLAS:")
    sorted_rules = sorted(by_rule.items(), key=lambda x: (x[1]['h']/x[1]['t'] if x[1]['t']>0 else 0), reverse=True)
    for r, stats in sorted_rules[:10]:
        r_acc = (stats['h']/stats['t']*100)
        print(f"   {r}: {r_acc:.1f}% ({stats['h']}/{stats['t']})")

    # Save to data.json logic (Optional, based on user input)
    # To "auto-improve", we should append these to data.json so next training sees them.
    # We will do this if requested or imply it.
    
    if acc < 85:
        print("\n⚠️ Precisión por debajo del 85%. Se recomienda añadir estos datos al entrenamiento.")

if __name__ == "__main__":
    main()
