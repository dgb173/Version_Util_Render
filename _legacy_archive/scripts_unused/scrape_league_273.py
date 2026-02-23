"""
Scraper de Liga 273 (A-League Australia) - Obtiene partidos con handicaps
Para backtest real sin overfitting.
"""

import sys
from pathlib import Path
import json
import requests
import re
import time
from datetime import datetime

# Headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://football.nowgoal26.com/"
}


def fetch_league_schedule(league_id: int, season: str) -> list:
    """Obtiene lista de partidos de una liga."""
    
    # URL del archivo JS con el schedule
    url = f"https://football.nowgoal26.com/jsData/matchResult/{season}/s{league_id}_en.js"
    
    print(f"Descargando schedule: {url}")
    
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        js_content = resp.text
    except Exception as e:
        print(f"Error: {e}")
        return []
    
    # Parsear diferentes formatos de JS
    matches = []
    
    # Buscar arrMatch o jh
    match_data = re.search(r'(?:var\s+)?(?:arrMatch|jh)\s*=\s*(\[[\s\S]*?\]);', js_content)
    if not match_data:
        # Intentar formato alternativo
        match_data = re.search(r'\[\s*\[.*?\][\s,\n]*\]', js_content, re.DOTALL)
    
    if not match_data:
        print("No se encontro estructura de datos")
        print(f"Primeros 500 chars: {js_content[:500]}")
        return []
    
    try:
        arr_str = match_data.group(1) if match_data.lastindex else match_data.group(0)
        arr_str = arr_str.replace("'", '"')
        arr_str = re.sub(r',\s*,', ',null,', arr_str)
        arr_str = re.sub(r'\[\s*,', '[null,', arr_str)
        arr_str = re.sub(r',\s*\]', ',null]', arr_str)
        
        raw_matches = json.loads(arr_str)
    except Exception as e:
        print(f"Error parseando: {e}")
        return []
    
    # Convertir a formato estandar
    for row in raw_matches:
        if not row or len(row) < 9:
            continue
        
        try:
            match = {
                'match_id': str(row[0]),
                'date': str(row[1]) if row[1] else '',
                'time': str(row[2]) if row[2] else '',
                'home_id': str(row[3]) if row[3] else '',
                'home_name': str(row[4]) if row[4] else '',
                'away_id': str(row[5]) if row[5] else '',
                'away_name': str(row[6]) if row[6] else '',
                'home_goals': int(row[7]) if row[7] is not None else None,
                'away_goals': int(row[8]) if row[8] is not None else None,
            }
            
            if match['home_goals'] is not None:
                matches.append(match)
        except:
            continue
    
    print(f"Encontrados {len(matches)} partidos con resultado")
    return matches


def fetch_match_odds(match_id: str) -> dict:
    """Obtiene odds AH de un partido."""
    
    # URL de odds de partido
    url = f"https://www.nowgoal26.com/analysis/{match_id}"
    
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return {}
        
        html = resp.text
        
        # Buscar AH en el HTML
        ah_match = re.search(r'AHLine["\']?\s*:\s*["\']?([-\d.]+)', html)
        if ah_match:
            return {'ah_linea': float(ah_match.group(1))}
        
        # Alternativa: buscar en tabla de odds
        ah_match = re.search(r'data-ah=["\']?([-\d.]+)', html)
        if ah_match:
            return {'ah_linea': float(ah_match.group(1))}
        
    except:
        pass
    
    return {}


def scrape_league(league_id: int = 273, season: str = "2024-2025", max_matches: int = 200):
    """Scrapea una liga completa."""
    
    print("=" * 60)
    print(f"SCRAPEANDO LIGA {league_id} - {season}")
    print("=" * 60)
    
    # Obtener lista de partidos
    matches = fetch_league_schedule(league_id, season)
    
    if not matches:
        print("No se encontraron partidos")
        return []
    
    # Limitar cantidad
    matches = matches[:max_matches]
    
    print(f"\nObteniendo odds de {len(matches)} partidos...")
    
    scraped = []
    for i, m in enumerate(matches):
        print(f"  [{i+1}/{len(matches)}] {m['home_name']} vs {m['away_name']}...", end='')
        
        odds = fetch_match_odds(m['match_id'])
        m['main_match_odds'] = odds
        
        if odds.get('ah_linea'):
            m['ah_line'] = odds['ah_linea']
            scraped.append(m)
            print(f" AH={odds['ah_linea']}")
        else:
            print(" sin AH")
        
        time.sleep(0.5)  # Rate limit
    
    print(f"\nTotal con odds AH: {len(scraped)}")
    return scraped


def main():
    base_path = Path(__file__).parent.parent
    
    # Scrapear liga 273
    matches = scrape_league(273, "2024-2025", max_matches=200)
    
    if not matches:
        print("No se obtuvieron partidos con odds")
        return
    
    # Guardar
    output = base_path / 'data' / 'league_273_with_odds.json'
    output.parent.mkdir(exist_ok=True)
    
    with open(output, 'w', encoding='utf-8') as f:
        json.dump(matches, f, indent=2, ensure_ascii=False)
    
    print(f"\nGuardado en: {output}")


if __name__ == '__main__':
    main()
