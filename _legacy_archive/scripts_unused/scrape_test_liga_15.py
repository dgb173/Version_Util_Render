#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Scraper y Backtest para Liga 15
Scrapea partidos jugados de la liga 15 y hace backtest con las reglas mega-entrenadas.
"""
import sys
import os
from pathlib import Path

# Setup path
script_dir = Path(__file__).resolve().parent
project_dir = script_dir.parent
src_dir = project_dir / 'src'
sys.path.insert(0, str(src_dir))
sys.path.insert(0, str(project_dir))
os.chdir(project_dir)

import requests
import re
import json
import time
import concurrent.futures

# Importar modulos
try:
    from modules import data_manager
    from modules.estudio_scraper import analizar_partido_completo
    print("Modulos importados correctamente")
except ImportError as e:
    print(f"Error importando modulos: {e}")
    sys.exit(1)

from scripts.rule_applier import RuleApplier


# Liga 15 config - Ajustar segun nowgoal
LIGA_CONFIG = {
    'name': 'Liga 15',
    'id': 15,
    'url': 'https://football.nowgoal26.com/jsData/matchResult/2024-2025/s15_en.js',
    'season': '2024-2025'
}


def get_played_matches(max_matches=100):
    """Obtiene partidos YA JUGADOS de la liga 15."""
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0',
        'Accept': '*/*',
        'Referer': 'https://football.nowgoal26.com/'
    })
    
    print(f"Descargando datos de Liga {LIGA_CONFIG['id']}...")
    print(f"URL: {LIGA_CONFIG['url']}")
    
    try:
        response = session.get(LIGA_CONFIG['url'], timeout=30)
        if response.status_code != 200:
            print(f"Error HTTP: {response.status_code}")
            return []
        
        js_content = response.text
        
        if 'DOCTYPE' in js_content or '<html' in js_content.lower():
            print("La URL retorna HTML, no JS. Probando URL alternativa...")
            # Probar URL alternativa
            alt_url = f"https://football.nowgoal26.com/jsData/match/result/{LIGA_CONFIG['season']}/{LIGA_CONFIG['id']}_en.js"
            response = session.get(alt_url, timeout=30)
            if response.status_code == 200:
                js_content = response.text
            else:
                print(f"URL alternativa tambien fallo: {response.status_code}")
                return []
        
        print(f"Contenido recibido: {len(js_content)} bytes")
        
    except Exception as e:
        print(f"Error descargando: {e}")
        return []
    
    # Parsear equipos
    teams = {}
    team_match = re.search(r'var arrTeam = \[(.*?)\];', js_content, re.DOTALL)
    if team_match:
        team_str = team_match.group(1)
        team_entries = re.findall(r'\[(\d+),\'[^\']*\',\'[^\']*\',\'([^\']+)\'', team_str)
        for tid, tname in team_entries:
            teams[int(tid)] = tname
    
    print(f"Equipos encontrados: {len(teams)}")
    
    # Parsear partidos
    matches = []
    
    # Buscar jh["R_X"]
    round_matches = re.findall(r'jh\["R_\d+"\]\s*=\s*\[(.*?)\];', js_content, re.DOTALL)
    
    for round_data in round_matches:
        matches_raw = re.findall(r'\[([^\[\]]+)\]', round_data)
        
        for match_raw in matches_raw:
            parts = match_raw.split(',')
            if len(parts) < 14:
                continue
            
            try:
                match_id = parts[0].strip()
                home_id = int(parts[4].strip())
                away_id = int(parts[5].strip())
                score = parts[6].strip().strip("'").replace("'", "")
                
                # Solo partidos con resultado
                if not score or score == '-' or ':' not in score:
                    continue
                
                home_name = teams.get(home_id, f"Team {home_id}")
                away_name = teams.get(away_id, f"Team {away_id}")
                
                matches.append({
                    'match_id': match_id,
                    'home_name': home_name,
                    'away_name': away_name,
                    'score': score
                })
                
                if len(matches) >= max_matches:
                    break
                    
            except Exception:
                continue
        
        if len(matches) >= max_matches:
            break
    
    print(f"Partidos con resultado: {len(matches)}")
    return matches


def scrape_match_details(match_id):
    """Scrapea detalles de un partido."""
    try:
        result = analizar_partido_completo(str(match_id))
        if result and not result.get('error'):
            return result
    except Exception as e:
        print(f"Error scrapeando {match_id}: {e}")
    return None


def parse_score(score_str):
    if not score_str or score_str in ['-', '?:?']:
        return None, None
    score_str = str(score_str).replace('-', ':')
    parts = score_str.split(':')
    if len(parts) != 2:
        return None, None
    try:
        return int(parts[0]), int(parts[1])
    except:
        return None, None


def settle_ah(home_g, away_g, ah_line, bet_side):
    diff = (home_g - away_g) + ah_line
    
    if bet_side in ['HOME', 'LOCAL']:
        if diff > 0.25: return 'WIN'
        elif diff > 0: return 'HALF_WIN'
        elif diff == 0: return 'PUSH'
        elif diff >= -0.25: return 'HALF_LOSS'
        else: return 'LOSS'
    else:
        diff = -diff
        if diff > 0.25: return 'WIN'
        elif diff > 0: return 'HALF_WIN'
        elif diff == 0: return 'PUSH'
        elif diff >= -0.25: return 'HALF_LOSS'
        else: return 'LOSS'


def main():
    print("=" * 70)
    print(f"SCRAPER Y BACKTEST - LIGA {LIGA_CONFIG['id']}")
    print("=" * 70)
    
    # Obtener lista de partidos
    matches = get_played_matches(max_matches=150)
    
    if not matches:
        print("\nNo se encontraron partidos. Puede que:")
        print("  - Nowgoal bloquee la request")
        print("  - El ID de liga sea incorrecto")
        print("  - El formato de URL haya cambiado")
        return
    
    # Cargar reglas
    print("\nCargando reglas mega-entrenadas...")
    applier = RuleApplier()
    print(f"Reglas cargadas: {len(applier.rules)}")
    
    # Scrapear detalles (con limite para no saturar)
    print(f"\nScrapeando detalles de {min(50, len(matches))} partidos...")
    
    detailed_matches = []
    scraped = 0
    
    for m in matches[:50]:  # Limitar a 50 para no tardar mucho
        print(f"  [{scraped+1}/50] {m['home_name']} vs {m['away_name']}...", end='', flush=True)
        
        details = scrape_match_details(m['match_id'])
        if details:
            details['score_from_list'] = m['score']
            detailed_matches.append(details)
            print(" OK")
            scraped += 1
        else:
            print(" skip")
        
        time.sleep(0.3)  # Rate limit
    
    print(f"\nPartidos con detalles: {len(detailed_matches)}")
    
    if not detailed_matches:
        print("No se pudieron scrapear detalles")
        return
    
    # Backtest
    print("\nEjecutando backtest...")
    
    STAKE = 5.0
    ODDS = 1.85
    
    picks = 0
    wins = 0
    losses = 0
    staked = 0.0
    returned = 0.0
    
    for m in detailed_matches:
        # Obtener AH
        odds = m.get('main_match_odds', {})
        ah_str = odds.get('ah_linea')
        if not ah_str or ah_str == '-':
            continue
        
        try:
            ah = float(ah_str)
        except:
            continue
        
        # Score
        score = m.get('final_score', m.get('score_from_list', ''))
        home_g, away_g = parse_score(score)
        if home_g is None:
            continue
        
        # Aplicar reglas
        pick = applier.get_best_pick(m)
        if not pick:
            continue
        
        picks += 1
        staked += STAKE
        
        outcome = settle_ah(home_g, away_g, ah, pick['pick'])
        
        if outcome == 'WIN':
            wins += 1
            returned += STAKE * ODDS
        elif outcome == 'HALF_WIN':
            wins += 1
            returned += STAKE + (STAKE * (ODDS - 1) / 2)
        elif outcome == 'PUSH':
            returned += STAKE
        elif outcome == 'HALF_LOSS':
            losses += 1
            returned += STAKE / 2
        else:
            losses += 1
    
    # Resultados
    profit = returned - staked
    roi = (profit / staked * 100) if staked > 0 else 0
    win_rate = (wins / picks * 100) if picks > 0 else 0
    
    print("\n" + "=" * 70)
    print(f"RESULTADOS LIGA {LIGA_CONFIG['id']}")
    print("=" * 70)
    print(f"Partidos scrapeados: {len(detailed_matches)}")
    print(f"Picks realizados: {picks}")
    print(f"Ganadas: {wins}")
    print(f"Perdidas: {losses}")
    print()
    print(f"Total apostado: EUR{staked:.2f}")
    print(f"Total retornado: EUR{returned:.2f}")
    print(f"PROFIT/LOSS: EUR{profit:+.2f}")
    print()
    print(f"WIN RATE: {win_rate:.1f}%")
    print(f"ROI: {roi:+.1f}%")


if __name__ == '__main__':
    main()
