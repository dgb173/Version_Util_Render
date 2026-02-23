#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Backtest REAL con partidos nuevos de Israel Premier League 2024-2025
IDs obtenidos de nowgoal - Rounds 22-26
"""
import sys
import json
from pathlib import Path

# Configurar paths
script_dir = Path(__file__).parent
project_dir = script_dir.parent
src_dir = project_dir / 'src'
sys.path.insert(0, str(src_dir))
sys.path.insert(0, str(project_dir))

# IDs de partidos extraídos de nowgoal (Rounds 22-26)
MATCH_IDS = [
    # Round 26
    {"id": "2640862", "home": "Maccabi Netanya", "away": "Ironi Tiberias", "score": "1-1"},
    {"id": "2640861", "home": "Maccabi Tel Aviv", "away": "Hapoel Haifa", "score": "2-0"},
    {"id": "2640863", "home": "Hapoel Bnei Sakhnin FC", "away": "Hapoel Jerusalem", "score": "0-2"},
    {"id": "2640859", "home": "Maccabi Bnei Raina", "away": "Hapoel Hadera", "score": "1-2"},
    {"id": "2640858", "home": "Hapoel Kiryat Shmona", "away": "Maccabi Petah Tikva FC", "score": "2-0"},
    {"id": "2640860", "home": "Maccabi Haifa", "away": "Ashdod MS", "score": "1-2"},
    {"id": "2640857", "home": "Beitar Jerusalem", "away": "Hapoel Beer Sheva", "score": "1-1"},
    # Round 25
    {"id": "2640852", "home": "Hapoel Jerusalem", "away": "Hapoel Kiryat Shmona", "score": "0-0"},
    {"id": "2640856", "home": "Ashdod MS", "away": "Maccabi Netanya", "score": "0-0"},
    {"id": "2640850", "home": "Hapoel Hadera", "away": "Maccabi Tel Aviv", "score": "2-3"},
    {"id": "2640854", "home": "Ironi Tiberias", "away": "Maccabi Bnei Raina", "score": "1-0"},
    {"id": "2640851", "home": "Hapoel Haifa", "away": "Hapoel Bnei Sakhnin FC", "score": "2-1"},
    {"id": "2640855", "home": "Maccabi Petah Tikva FC", "away": "Beitar Jerusalem", "score": "1-2"},
    {"id": "2640853", "home": "Hapoel Beer Sheva", "away": "Maccabi Haifa", "score": "3-3"},
    # Round 24
    {"id": "2640843", "home": "Beitar Jerusalem", "away": "Hapoel Jerusalem", "score": "1-1"},
    {"id": "2640846", "home": "Maccabi Haifa", "away": "Maccabi Petah Tikva FC", "score": "1-0"},
    {"id": "2640847", "home": "Maccabi Tel Aviv", "away": "Maccabi Bnei Raina", "score": "0-1"},
    {"id": "2640844", "home": "Ironi Tiberias", "away": "Ashdod MS", "score": "2-1"},
    {"id": "2640848", "home": "Maccabi Netanya", "away": "Hapoel Beer Sheva", "score": "1-2"},
    {"id": "2640849", "home": "Hapoel Bnei Sakhnin FC", "away": "Hapoel Hadera", "score": "0-1"},
    {"id": "2640753", "home": "Hapoel Haifa", "away": "Hapoel Kiryat Shmona", "score": "4-3"},
    # Round 23
    {"id": "2640839", "home": "Hapoel Beer Sheva", "away": "Ironi Tiberias", "score": "4-0"},
    {"id": "2640841", "home": "Maccabi Petah Tikva FC", "away": "Maccabi Netanya", "score": "0-1"},
    {"id": "2640836", "home": "Hapoel Hadera", "away": "Hapoel Kiryat Shmona", "score": "0-2"},
    {"id": "2640840", "home": "Maccabi Bnei Raina", "away": "Ashdod MS", "score": "1-4"},
    {"id": "2640837", "home": "Hapoel Haifa", "away": "Beitar Jerusalem", "score": "3-2"},
    {"id": "2640842", "home": "Maccabi Tel Aviv", "away": "Hapoel Bnei Sakhnin FC", "score": "3-1"},
    {"id": "2640748", "home": "Maccabi Haifa", "away": "Hapoel Jerusalem", "score": "3-3"},
    # Round 22
    {"id": "2640833", "home": "Maccabi Netanya", "away": "Hapoel Jerusalem", "score": "2-1"},
    {"id": "2640829", "home": "Beitar Jerusalem", "away": "Hapoel Hadera", "score": "0-0"},
    {"id": "2640830", "home": "Ironi Tiberias", "away": "Maccabi Petah Tikva FC", "score": "1-2"},
    {"id": "2640834", "home": "Hapoel Bnei Sakhnin FC", "away": "Maccabi Bnei Raina", "score": "2-0"},
    {"id": "2640835", "home": "Ashdod MS", "away": "Hapoel Beer Sheva", "score": "0-2"},
    {"id": "2640831", "home": "Hapoel Kiryat Shmona", "away": "Maccabi Tel Aviv", "score": "1-2"},
    {"id": "2640832", "home": "Maccabi Haifa", "away": "Hapoel Haifa", "score": "1-1"},
]

def main():
    # Importar módulos del proyecto
    try:
        from modules.estudio_scraper import analizar_partido_completo
        from modules import data_manager
        print("✅ Módulos importados")
    except ImportError as e:
        print(f"❌ Error importando: {e}")
        return
    
    print("=" * 80)
    print("BACKTEST REAL - Israel Premier League 2024-2025")
    print("=" * 80)
    print(f"Partidos a testear: {len(MATCH_IDS)}")
    print()
    
    # 1. Precachear partidos
    print("📥 Precacheando partidos...")
    precached = []
    
    for i, m in enumerate(MATCH_IDS, 1):
        print(f"   [{i:>2}/{len(MATCH_IDS)}] {m['home'][:15]} vs {m['away'][:15]}...", end=" ")
        
        try:
            # Verificar si ya está
            existing = data_manager.get_precacheo_match(m['id'])
            if existing and existing.get('market_analysis_data'):
                precached.append(existing)
                print("✅ (cache)")
                continue
            
            # Scrapear
            result = analizar_partido_completo(m['id'])
            if result and not result.get('error'):
                if 'match_id' not in result:
                    result['match_id'] = m['id']
                data_manager.save_precacheo_match(result)
                precached.append(result)
                print("✅ (scraped)")
            else:
                print("❌ error")
        except Exception as e:
            print(f"❌ {str(e)[:30]}")
    
    print(f"\n✅ Partidos precacheados: {len(precached)}")
    
    # 2. Cargar reglas
    rules_file = project_dir / 'data' / 'mined_rules.json'
    if not rules_file.exists():
        print("❌ No se encontró mined_rules.json")
        return
    
    with open(rules_file, 'r', encoding='utf-8') as f:
        rules = json.load(f)
    print(f"📋 Reglas cargadas: {len(rules)}")
    
    # 3. Importar funciones de aplicar reglas
    from scripts.apply_mined_rules import extract_features
    
    # 4. Backtest
    print("\n" + "=" * 80)
    print("RESULTADOS DEL BACKTEST")
    print("=" * 80)
    
    stake = 5
    odds = 1.85
    total_profit = 0
    total_bets = 0
    wins = 0
    losses = 0
    pushes = 0
    
    print(f"\n{'#':>2} | {'Partido':<35} | {'Score':>5} | {'AH':>5} | {'Pick':>4} | {'Res':>10} | {'P/L':>7}")
    print("-" * 90)
    
    for i, match in enumerate(precached, 1):
        # Obtener score
        score = match.get('final_score')
        if not score or '-' not in score and ':' not in score:
            continue
        
        try:
            score = score.replace('-', ':')
            parts = score.split(':')
            hg, ag = int(parts[0]), int(parts[1])
        except:
            continue
        
        # Obtener AH
        odds_data = match.get('main_match_odds') or {}
        ah_raw = odds_data.get('ah_linea') or match.get('handicap')
        if ah_raw is None:
            continue
        try:
            ah = float(ah_raw)
        except:
            continue
        
        # Extraer features
        features = extract_features(match)
        is_home_fav = features.get('is_home_fav', True)
        
        # Aplicar reglas
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
                pick_type = rule['pick']
                
                if pick_type == 'FAV':
                    pick = 'HOME' if is_home_fav else 'AWAY'
                else:
                    pick = 'AWAY' if is_home_fav else 'HOME'
                
                best_match = {
                    'rule_name': rule['name'],
                    'pick': pick
                }
        
        if best_match is None:
            continue
        
        pick = best_match['pick']
        rule_name = best_match['rule_name']
        
        # Calcular resultado
        if is_home_fav:
            fav_diff = hg - ag
            handicap = ah
        else:
            fav_diff = ag - hg
            handicap = abs(ah)
        
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
        
        # Resultado según pick
        if pick == 'HOME':
            result = fav_result if is_home_fav else -fav_result
        else:
            result = fav_result if not is_home_fav else -fav_result
        
        # Profit
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
            pushes += 1
        
        total_profit += profit
        total_bets += 1
        
        home = match.get('home_name', 'Home')[:15]
        away = match.get('away_name', 'Away')[:15]
        emoji = "+" if profit > 0 else ("-" if profit < 0 else "=")
        
        print(f"{i:>2} | {home} vs {away:<15} | {score:>5} | {ah:>5} | {pick:>4} | {res_str:>10} | {emoji}{abs(profit):>6.2f}")
    
    print("-" * 90)
    print()
    print("=" * 50)
    print("RESUMEN FINAL (DATOS NUEVOS - NO USADOS EN ENTRENAMIENTO)")
    print("=" * 50)
    print(f"Partidos precacheados: {len(precached)}")
    print(f"Partidos con pick: {total_bets}")
    print()
    if total_bets > 0:
        print(f"Ganadas: {wins} | Perdidas: {losses} | Push: {pushes}")
        print(f"Win Rate: {wins/total_bets*100:.1f}%")
        print()
        print(f"💰 PROFIT TOTAL: {total_profit:.2f}€")
        print(f"📊 ROI: {total_profit/(total_bets*stake)*100:.1f}%")
    else:
        print("No se generaron picks.")

if __name__ == "__main__":
    main()
