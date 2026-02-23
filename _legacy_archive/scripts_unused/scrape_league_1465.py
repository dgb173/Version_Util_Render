"""
Scrapea todos los partidos de la liga 1465 y los guarda para entrenamiento.
Luego entrena reglas específicas y valida hasta alcanzar 80% accuracy.
"""
import sys
import os
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
from modules import league_scraper, estudio_scraper

SEASON = "2024-2025"
LEAGUE_ID = 1465
OUTPUT_FILE = "training_data_1465.json"

def scrape_match(mid):
    try:
        data = estudio_scraper.analizar_partido_completo(mid)
        if data:
            data['league_id'] = LEAGUE_ID
            return data
    except Exception as e:
        pass
    return None

def main():
    print(f"🎯 SCRAPING LIGA {LEAGUE_ID} PARA ENTRENAMIENTO")
    print("=" * 60)
    
    # 1. Extraer IDs
    print("\n📋 Extrayendo IDs de partidos...")
    result = league_scraper.extract_ids_by_params(SEASON, LEAGUE_ID)
    match_data = result.get('match_data', [])
    ids = [str(m['id']) for m in match_data]
    print(f"✅ Encontrados {len(ids)} partidos")
    
    # 2. Scrapear todos (con threading)
    print(f"\n🔍 Scrapeando partidos (esto tardará unos minutos)...")
    
    all_matches = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(scrape_match, mid): mid for mid in ids}
        for i, future in enumerate(as_completed(futures)):
            result = future.result()
            if result:
                # Check it has a valid score
                score = result.get('final_score') or result.get('score')
                if score and score not in ['??', '?-?', '-']:
                    all_matches.append(result)
            if (i+1) % 20 == 0:
                print(f"   Procesados {i+1}/{len(ids)}... ({len(all_matches)} válidos)")
    
    print(f"\n✅ Scrapeados {len(all_matches)} partidos con datos válidos")
    
    # 3. Guardar
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(all_matches, f, indent=2, ensure_ascii=False)
    
    print(f"💾 Guardados en {OUTPUT_FILE}")
    print("\n🚀 Ahora ejecuta: py scripts/train_league_1465.py")

if __name__ == "__main__":
    main()
