"""
Valida las reglas unificadas en una liga específica y calcula ganancias.
Liga: 157
Apuesta: 5€ por pick
Cuota: 1.8
"""
import sys
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
from modules import league_scraper, estudio_scraper

SEASON = "2023-2024"
LEAGUE_ID = 292
BET_AMOUNT = 5.0
ODDS = 1.8
RULES_FILE = "backtest_results/specialist_simple.json"

def parse_score(s):
    if not s: return None
    s = str(s).replace(':', '-')
    if '-' not in s: return None
    try:
        p = s.split('-')
        return int(p[0]), int(p[1])
    except:
        return None

def parse_ah(v):
    if not v or v == 'N/A': return None
    try: return float(v)
    except: return None

def get_ah_result(hg, ag, ah):
    diff = (hg - ag) + ah
    if diff > 0.25: return 'LOCAL'
    if diff < -0.25: return 'VISITA'
    return 'PUSH'

def parse_stats_rows(rows):
    result = {}
    if not rows: return result
    for r in rows:
        label = (r.get('label') or '').strip()
        try:
            h = float(r.get('home', 0) or 0)
            a = float(r.get('away', 0) or 0)
            result[label] = {'home': h, 'away': a, 'diff': h - a}
        except:
            pass
    return result

def extract_features(m):
    f = {}
    odds = m.get('main_match_odds', {})
    try:
        current_ah = parse_ah(odds.get('ah_linea', 0)) or 0.0
    except:
        return {}
    
    f['current_ah'] = current_ah
    f['context_fav_home'] = current_ah < 0
    f['fav_home_strong'] = current_ah <= -0.5
    f['fav_away_strong'] = current_ah >= 0.5
    
    # Rankings
    hs = m.get('home_standings', {}) or {}
    as_ = m.get('away_standings', {}) or {}
    try:
        hr = int(hs.get('ranking', 0) or 0) if str(hs.get('ranking', 0)) not in ['N/A', ''] else 0
        ar = int(as_.get('ranking', 0) or 0) if str(as_.get('ranking', 0)) not in ['N/A', ''] else 0
    except:
        hr, ar = 0, 0
    f['rank_diff'] = ar - hr if hr > 0 and ar > 0 else 0
    f['home_better_rank'] = hr < ar if hr > 0 and ar > 0 else False
    f['rank_close'] = abs(f['rank_diff']) <= 3
    
    # Col3
    col3 = m.get('h2h_col3', {}) or {}
    f['has_col3'] = col3.get('status') == 'found'
    col3_ah = parse_ah(col3.get('ah'))
    f['col3_ah'] = col3_ah if col3_ah else 0.0
    f['col3_ah_diff'] = current_ah - (col3_ah or 0)
    try:
        f['col3_goal_diff'] = int(col3.get('goles_home', 0) or 0) - int(col3.get('goles_away', 0) or 0)
    except:
        f['col3_goal_diff'] = 0
    f['col3_covered'] = False
    
    # Indirectas
    comp = m.get('comparativas_indirectas', {}) or {}
    left = comp.get('left', {}) or {}
    right = comp.get('right', {}) or {}
    
    left_ah = parse_ah(left.get('ah'))
    f['ind_left_ah'] = left_ah if left_ah else 0.0
    f['ind_left_ah_diff'] = current_ah - (left_ah or 0)
    f['ind_left_covered'] = False
    
    right_ah = parse_ah(right.get('ah'))
    f['ind_right_ah'] = right_ah if right_ah else 0.0
    f['ind_right_ah_diff'] = current_ah - (right_ah or 0)
    f['ind_right_covered'] = False
    
    # Prev home
    lhm = m.get('last_home_match') or {}
    lhm_ah = parse_ah(lhm.get('ah')) if lhm else None
    lhm_score = parse_score(lhm.get('score')) if lhm else None
    
    f['prev_home_ah'] = lhm_ah if lhm_ah else 0.0
    f['prev_home_ah_diff'] = current_ah - (lhm_ah or 0)
    f['line_moved_up_vs_prev_home'] = current_ah > lhm_ah if lhm_ah else False
    f['line_moved_down_vs_prev_home'] = current_ah < lhm_ah if lhm_ah else False
    
    if lhm_score:
        f['prev_home_goal_diff'] = lhm_score[0] - lhm_score[1]
        f['prev_home_won'] = lhm_score[0] > lhm_score[1]
        f['prev_home_lost'] = lhm_score[0] < lhm_score[1]
        f['prev_home_covered'] = False
        f['home_won_line_up'] = f['prev_home_won'] and f['line_moved_up_vs_prev_home']
    else:
        f['prev_home_goal_diff'] = 0
        f['prev_home_won'] = False
        f['prev_home_lost'] = False
        f['prev_home_covered'] = False
        f['home_won_line_up'] = False
    
    f['prev_home_da_diff'] = 0
    f['prev_home_dominated'] = False
    f['prev_home_was_dominated'] = False
    f['prev_home_shots_diff'] = 0
    f['prev_home_unlucky_loss'] = False
    
    # Prev away
    lam = m.get('last_away_match') or {}
    lam_ah = parse_ah(lam.get('ah')) if lam else None
    lam_score = parse_score(lam.get('score')) if lam else None
    
    f['prev_away_ah'] = lam_ah if lam_ah else 0.0
    f['prev_away_ah_diff'] = current_ah - (lam_ah or 0)
    f['line_moved_up_vs_prev_away'] = current_ah > lam_ah if lam_ah else False
    f['line_moved_down_vs_prev_away'] = current_ah < lam_ah if lam_ah else False
    
    if lam_score:
        f['prev_away_goal_diff'] = lam_score[1] - lam_score[0]
        f['prev_away_won'] = lam_score[1] > lam_score[0]
        f['prev_away_lost'] = lam_score[1] < lam_score[0]
        f['prev_away_covered'] = False
    else:
        f['prev_away_goal_diff'] = 0
        f['prev_away_won'] = False
        f['prev_away_lost'] = False
        f['prev_away_covered'] = False
    
    f['prev_away_da_diff'] = 0
    f['prev_away_dominated'] = False
    f['prev_away_was_dominated'] = False
    f['prev_away_shots_diff'] = 0
    f['prev_away_unlucky_loss'] = False
    
    f['dominance_diff'] = 0
    f['home_momentum'] = False
    f['away_momentum'] = False
    
    return f

def check_rule(conditions, features):
    for feat, op, val in conditions:
        if feat not in features: return False
        curr = features[feat]
        try:
            if op == '==' and curr != val: return False
            if op == '>' and not (curr > val): return False
            if op == '<' and not (curr < val): return False
            if op == '>=' and not (curr >= val): return False
            if op == '<=' and not (curr <= val): return False
        except: return False
    return True

def scrape_match(mid):
    try:
        return estudio_scraper.analizar_partido_completo(mid)
    except:
        return None

def main():
    print(f"🎯 VALIDACIÓN LIGA {LEAGUE_ID} (Temporada {SEASON})")
    print(f"💰 Apuesta: {BET_AMOUNT}€ por pick | Cuota: {ODDS}")
    print("=" * 60)
    
    # Load rules
    try:
        with open(RULES_FILE, 'r', encoding='utf-8') as f:
            rules = json.load(f)
        print(f"📋 Cargadas {len(rules)} reglas")
    except:
        print("❌ No se encontraron reglas. Ejecuta primero train_unified_all.py")
        return

    # Get match IDs
    print("\n📋 Extrayendo IDs de partidos...")
    result = league_scraper.extract_ids_by_params(SEASON, LEAGUE_ID)
    match_data = result.get('match_data', [])
    ids = [str(m['id']) for m in match_data]
    print(f"✅ Encontrados {len(ids)} partidos")

    # Scrape matches
    max_matches = min(150, len(ids))
    print(f"\n🔍 Scrapeando {max_matches} partidos...")
    
    matches = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(scrape_match, mid): mid for mid in ids[:max_matches]}
        for i, future in enumerate(as_completed(futures)):
            result = future.result()
            if result:
                matches.append(result)
            if (i+1) % 30 == 0:
                print(f"   Procesados {i+1}/{max_matches}...")
    
    print(f"✅ Scrapeados {len(matches)} partidos")
    
    # Validate
    print("\n🔄 Validando...")
    wins = 0
    losses = 0
    no_pick = 0
    
    for m in matches:
        score = parse_score(m.get('final_score') or m.get('score'))
        if not score: continue
        
        odds_data = m.get('main_match_odds', {})
        ah = parse_ah(odds_data.get('ah_linea'))
        if ah is None: continue
        
        result = get_ah_result(score[0], score[1], ah)
        if result == 'PUSH': continue
        
        feats = extract_features(m)
        if not feats: continue
        
        # Find matching rules
        matching_rules = []
        for r in rules:
            conds = r.get('conditions', [])
            if check_rule(conds, feats):
                matching_rules.append(r)
        
        if not matching_rules:
            no_pick += 1
            continue
        
        # Take best rule
        best = max(matching_rules, key=lambda x: x.get('accuracy', 0))
        prediction = best.get('prediction')
        
        if prediction == result:
            wins += 1
        else:
            losses += 1
    
    # Calculate
    total_bets = wins + losses
    total_staked = total_bets * BET_AMOUNT
    gross_wins = wins * BET_AMOUNT * ODDS
    net_profit = gross_wins - total_staked
    accuracy = (wins / total_bets * 100) if total_bets > 0 else 0
    roi = (net_profit / total_staked * 100) if total_staked > 0 else 0
    
    print("\n" + "=" * 60)
    print("📊 RESULTADOS")
    print("=" * 60)
    print(f"Partidos scrapeados: {len(matches)}")
    print(f"Partidos con pick: {total_bets}")
    print(f"Sin pick: {no_pick}")
    print(f"Aciertos: {wins}")
    print(f"Fallos: {losses}")
    print(f"ACCURACY: {accuracy:.1f}%")
    print()
    print(f"💵 APOSTADO: {total_staked:.2f}€")
    print(f"💰 GANADO (bruto): {gross_wins:.2f}€")
    print(f"📈 BENEFICIO NETO: {net_profit:+.2f}€")
    print(f"📊 YIELD (ROI): {roi:+.1f}%")
    
    if net_profit > 0:
        print(f"\n🎉 ¡HUBIERAS GANADO {net_profit:.2f}€!")
    else:
        print(f"\n😔 Hubieras perdido {abs(net_profit):.2f}€")

if __name__ == "__main__":
    main()
