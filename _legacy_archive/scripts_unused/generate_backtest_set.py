
import json
import time
import sys
from pathlib import Path
import concurrent.futures

print("DEBUG: Script starting...", flush=True)

# Import modules from src
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'src'))

print("DEBUG: Importing history_manager...", flush=True)
from modules import history_manager
print("DEBUG: Importing estudio_scraper...", flush=True)
from modules import estudio_scraper
print("DEBUG: Imports complete.", flush=True)

DATA_DIR = Path(__file__).resolve().parent.parent / 'data'
TEST_INPUTS_FILE = DATA_DIR / 'testing_inputs.json'
TEST_RESULTS_FILE = DATA_DIR / 'testing_results.json'

def generate_backtest_datasets():
    print("--- Generando Dataset de Testing ---", flush=True)
    
    # 1. Get Pending Matches
    print("DEBUG: Getting pending matches...", flush=True)
    pending_structure = history_manager.get_pending_matches()
    match_ids = []
    
    for season, leagues in pending_structure.items():
        for league_id, matches in leagues.items():
            for m in matches:
                # Handle both dict and string formats in pending list
                if isinstance(m, dict):
                    match_ids.append(m.get('id'))
                else:
                    match_ids.append(m)
    
    # Optional: Limit for testing? No, user said "gran cantidad".
    # But let's verify if we have any
    if not match_ids:
        print("No hay partidos pendientes en el historial para generar el dataset.")
        return

    print(f"Se encontraron {len(match_ids)} IDs pendientes para procesar.")
    print("Iniciando scraping (esto puede tardar)...")

    scraped_data = []
    failed_count = 0
    
    # Multi-threaded scraping
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_id = {executor.submit(estudio_scraper.analizar_partido_completo, mid): mid for mid in match_ids}
        
        for i, future in enumerate(concurrent.futures.as_completed(future_to_id)):
            mid = future_to_id[future]
            try:
                data = future.result()
                if data:
                    scraped_data.append(data)
                else:
                    failed_count += 1
            except Exception as e:
                print(f"Error procesando {mid}: {e}")
                failed_count += 1
            
            if (i+1) % 10 == 0:
                print(f"Procesado {i+1}/{len(match_ids)}")

    print(f"\nScraping completado. Éxitos: {len(scraped_data)}, Fallos: {failed_count}")

    # 2. Split Data
    inputs = []
    results = {}
    
    for match in scraped_data:
        # Full copy for safety
        input_match = match.copy()
        
        # Extract Result
        score = match.get('score') or match.get('final_score')
        match_id = match.get('match_id')
        
        if score and score not in ['??', '?-?']:
            results[match_id] = score
            
            # Remove result from input to simulate "pre-match"
            # We remove strictly the final score fields
            keys_to_remove = ['score', 'final_score', 'res_raw']
            for k in keys_to_remove:
                if k in input_match:
                    del input_match[k]
                    
            # Note: We keep H2H and other stats as they are "pre-match" info relative to the game time
            inputs.append(input_match)
        else:
            # If no result, we can still use it as input for prediction (live test), 
            # but we can't verify it immediately.
            # User asked for "data testing con todos los datos... sin el resultado final"
            # So we include it in inputs, but it won't be in results.json
             inputs.append(input_match)

    # 3. Save Files
    with open(TEST_INPUTS_FILE, 'w', encoding='utf-8') as f:
        json.dump(inputs, f, indent=2, ensure_ascii=False)
        
    with open(TEST_RESULTS_FILE, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"\nGeneración Finalizada:")
    print(f" -> inputs: {TEST_INPUTS_FILE} ({len(inputs)} partidos)")
    print(f" -> results: {TEST_RESULTS_FILE} ({len(results)} partidos con resultado)")
    print("Listo para probar tus patrones.")

if __name__ == "__main__":
    generate_backtest_datasets()
