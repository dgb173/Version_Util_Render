#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Scraper y Tester de una liga específica de nowgoal.
1. Scrapea los IDs de partidos terminados de una liga
2. Precachea cada partido usando el sistema existente
3. Aplica las reglas minadas y muestra resultados
"""
import sys
import os
import re
import json
import requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# Configurar paths
script_dir = Path(__file__).parent
project_dir = script_dir.parent
src_dir = project_dir / 'src'
sys.path.insert(0, str(src_dir))
sys.path.insert(0, str(project_dir))

# Importar módulos del proyecto
try:
    from modules.estudio_scraper import analizar_partido_completo
    from modules import data_manager
    print("✅ Módulos importados correctamente")
except ImportError as e:
    print(f"❌ Error importando módulos: {e}")
    sys.exit(1)

# Configuración
LIGA_ID = 118  # Israel Premier League
LIGA_URL = f'https://football.nowgoal26.com/jsData/matchResult/2024-2025/s{LIGA_ID}_en.js'
MAX_MATCHES = 50  # Partidos a testear
OUTPUT_FILE = project_dir / 'data' / 'backtest_liga_118.json'

def get_finished_matches():
    """Obtiene partidos YA TERMINADOS de la liga."""
    matches = []
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })
    
    print(f"🌍 Descargando datos de liga {LIGA_ID}...")
    try:
        response = session.get(LIGA_URL, timeout=15)
        if response.status_code != 200:
            print(f"❌ Error {response.status_code}")
            return []
        
        js_content = response.text
        
        # Parsear equipos
        teams = {}
        team_match = re.search(r'var arrTeam = \[(.*?)\];', js_content, re.DOTALL)
        if team_match:
            team_str = team_match.group(1)
            team_entries = re.findall(r'\[(\d+),\'[^\']*\',\'[^\']*\',\'([^\']+)\'', team_str)
            for tid, tname in team_entries:
                teams[int(tid)] = tname
        
        print(f"   Equipos encontrados: {len(teams)}")
        
        # Parsear partidos de todas las jornadas
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
                    score = parts[6].strip().strip("'")
                    
                    # Solo partidos con resultado (terminados)
                    clean_score = score.replace("'", "").strip()
                    if not clean_score or clean_score == '-' or ':' not in clean_score:
                        continue
                    
                    home_name = teams.get(home_id, f"Team {home_id}")
                    away_name = teams.get(away_id, f"Team {away_id}")
                    
                    matches.append({
                        'id': match_id,
                        'home': home_name,
                        'away': away_name,
                        'score': clean_score
                    })
                    
                except Exception as e:
                    continue
        
        print(f"   ✅ Partidos terminados encontrados: {len(matches)}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    
    return matches[-MAX_MATCHES:]  # Últimos N partidos

def precache_match(match_info):
    """Precachea un partido individual."""
    match_id = match_info['id']
    try:
        # Verificar si ya está precacheado
        existing = data_manager.get_precacheo_match(str(match_id))
        if existing and existing.get('market_analysis_data'):
            return {'id': match_id, 'status': 'exists', 'data': existing}
        
        # Scrapear
        result = analizar_partido_completo(str(match_id))
        if result and not result.get('error'):
            if 'match_id' not in result:
                result['match_id'] = str(match_id)
            data_manager.save_precacheo_match(result)
            return {'id': match_id, 'status': 'scraped', 'data': result}
        else:
            return {'id': match_id, 'status': 'error'}
            
    except Exception as e:
        return {'id': match_id, 'status': 'error', 'error': str(e)}

def apply_rules_to_match(match_data, rules):
    """Aplica las reglas minadas al partido."""
    from scripts.apply_mined_rules import extract_features
    
    features = extract_features(match_data)
    is_home_fav = features.get('is_home_fav', True)
    
    best_match = None
    best_roi = -999
    
    for rule in rules:
        conditions = rule.get('conditions', {})
        match_all = True
        for cond_key, cond_val in conditions.items():
            if features.get(cond_key) != cond_val:
                match_all = False
                break
        
        if match_all and rule['roi'] > best_roi:
            best_roi = rule['roi']
            pick = rule['pick']
            
            if pick == 'FAV':
                actual_pick = 'HOME' if is_home_fav else 'AWAY'
            else:
                actual_pick = 'AWAY' if is_home_fav else 'HOME'
            
            best_match = {
                'rule_name': rule['name'],
                'pick': actual_pick,
                'roi_expected': rule['roi']
            }
    
    return best_match

def calculate_result(home_goals, away_goals, ah_line, pick):
    """Calcula si el pick ganó."""
    is_home_fav = ah_line >= 0
    
    if is_home_fav:
        fav_diff = home_goals - away_goals
        handicap = ah_line
    else:
        fav_diff = away_goals - home_goals
        handicap = abs(ah_line)
    
    adjusted = fav_diff - handicap
    
    if abs(handicap % 0.5) == 0.25:
        line1 = handicap - 0.25
        line2 = handicap + 0.25
        adj1 = fav_diff - line1
        adj2 = fav_diff - line2
        r1 = 1 if adj1 > 0 else (-1 if adj1 < 0 else 0)
        r2 = 1 if adj2 > 0 else (-1 if adj2 < 0 else 0)
        fav_result = (r1 + r2) / 2
    else:
        fav_result = 1 if adjusted > 0 else (-1 if adjusted < 0 else 0)
    
    # Determinar resultado según pick
    if pick == 'HOME':
        if is_home_fav:
            return fav_result
        else:
            return -fav_result
    else:
        if not is_home_fav:
            return fav_result
        else:
            return -fav_result

def main():
    print("=" * 70)
    print("BACKTEST REAL - Liga 118 (Israel Premier League)")
    print("=" * 70)
    print()
    
    # 1. Obtener partidos terminados
    matches = get_finished_matches()
    if not matches:
        print("No se encontraron partidos")
        return
    
    print(f"\n📥 Precacheando {len(matches)} partidos...")
    
    # 2. Precachear cada partido
    precached = []
    for i, m in enumerate(matches, 1):
        print(f"   [{i}/{len(matches)}] {m['home']} vs {m['away']}...", end=" ")
        result = precache_match(m)
        if result['status'] in ['exists', 'scraped']:
            precached.append(result['data'])
            print(f"✅ {result['status']}")
        else:
            print(f"❌ {result.get('error', 'error')}")
    
    print(f"\n✅ Partidos precacheados: {len(precached)}")
    
    # 3. Cargar reglas minadas
    rules_file = project_dir / 'data' / 'mined_rules.json'
    if not rules_file.exists():
        print("❌ No se encontró mined_rules.json")
        return
    
    with open(rules_file, 'r', encoding='utf-8') as f:
        rules = json.load(f)
    print(f"📋 Reglas cargadas: {len(rules)}")
    
    # 4. Aplicar reglas y calcular resultados
    print("\n" + "=" * 70)
    print("RESULTADOS DEL BACKTEST")
    print("=" * 70)
    
    stake = 5
    odds = 1.85
    total_profit = 0
    total_bets = 0
    wins = 0
    losses = 0
    
    print(f"\n{'#':>2} | {'Partido':<40} | {'Score':>5} | {'AH':>5} | {'Pick':>4} | {'Res':>10} | {'P/L':>7}")
    print("-" * 90)
    
    for i, match in enumerate(precached, 1):
        # Obtener score y AH
        score = match.get('final_score')
        if not score or ':' not in score:
            continue
        
        try:
            parts = score.split(':')
            hg, ag = int(parts[0]), int(parts[1])
        except:
            continue
        
        odds_data = match.get('main_match_odds') or {}
        ah_raw = odds_data.get('ah_linea') or match.get('handicap')
        if ah_raw is None:
            continue
        try:
            ah = float(ah_raw)
        except:
            continue
        
        # Aplicar reglas
        pick_result = apply_rules_to_match(match, rules)
        if pick_result is None:
            continue
        
        pick = pick_result['pick']
        rule = pick_result['rule_name']
        
        # Calcular resultado
        result = calculate_result(hg, ag, ah, pick)
        
        if result > 0:
            if result == 1:
                profit = stake * (odds - 1)
                res_str = "WIN"
                wins += 1
            else:
                profit = stake * (odds - 1) / 2
                res_str = "HALF WIN"
                wins += 0.5
        elif result < 0:
            if result == -1:
                profit = -stake
                res_str = "LOSS"
                losses += 1
            else:
                profit = -stake / 2
                res_str = "HALF LOSS"
                losses += 0.5
        else:
            profit = 0
            res_str = "PUSH"
        
        total_profit += profit
        total_bets += 1
        
        home = match.get('home_name', 'Home')[:18]
        away = match.get('away_name', 'Away')[:18]
        emoji = "+" if profit > 0 else ("-" if profit < 0 else "=")
        
        print(f"{i:>2} | {home} vs {away:<18} | {score:>5} | {ah:>5} | {pick:>4} | {res_str:>10} | {emoji}{abs(profit):>6.2f}")
    
    print("-" * 90)
    print()
    print("=" * 50)
    print("RESUMEN FINAL")
    print("=" * 50)
    print(f"Partidos testeados: {len(precached)}")
    print(f"Partidos con pick: {total_bets}")
    print()
    if total_bets > 0:
        print(f"Ganadas: {wins} | Perdidas: {losses}")
        print(f"Win Rate: {wins/total_bets*100:.1f}%")
        print()
        print(f"💰 PROFIT TOTAL: {total_profit:.2f}€")
        print(f"📊 ROI: {total_profit/(total_bets*stake)*100:.1f}%")
    else:
        print("No se generaron picks.")

if __name__ == "__main__":
    main()
