"""
Valida las reglas mejoradas de liga 1465 y calcula ganancias.
"""
import json
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

DATA_FILE = "training_data_1465.json"
RULES_FILE = "backtest_results/specialist_league_1465.json"
BET_AMOUNT = 5.0
ODDS = 1.8

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

def extract_features(m):
    f = {}
    odds = m.get('main_match_odds', {})
    try:
        current_ah = parse_ah(odds.get('ah_linea', 0)) or 0.0
    except:
        return {}
    
    f['current_ah'] = current_ah
    f['context_fav_home'] = current_ah < 0
    
    # Rankings
    hs = m.get('home_standings', {})
    as_ = m.get('away_standings', {})
    try:
        hr = int(hs.get('ranking', 0) or 0) if str(hs.get('ranking', 0)) not in ['N/A', ''] else 0
        ar = int(as_.get('ranking', 0) or 0) if str(as_.get('ranking', 0)) not in ['N/A', ''] else 0
    except:
        hr, ar = 0, 0
    f['rank_diff'] = ar - hr if hr > 0 and ar > 0 else 0
    
    # Col3
    col3 = m.get('h2h_col3', {})
    f['has_col3'] = col3.get('status') == 'found'
    col3_ah = parse_ah(col3.get('ah'))
    f['col3_ah'] = col3_ah if col3_ah else 0.0
    f['col3_ah_diff'] = current_ah - (col3_ah or 0)
    try:
        f['col3_goal_diff'] = int(col3.get('goles_home', 0) or 0) - int(col3.get('goles_away', 0) or 0)
    except:
        f['col3_goal_diff'] = 0
    f['col3_covered'] = False
    
    # Indirect
    comp = m.get('comparativas_indirectas', {})
    left = comp.get('left', {})
    right = comp.get('right', {})
    f['ind_left_covered'] = False
    f['ind_right_covered'] = False
    f['ind_margin_diff'] = 0
    
    # Prev matches
    lhm = m.get('last_home_match', {})
    lam = m.get('last_away_match', {})
    f['prev_home_covered'] = False
    f['prev_away_covered'] = False
    f['prev_home_won'] = False
    f['prev_away_won'] = False
    
    return f

def main():
    print("🎯 VALIDACIÓN REGLAS MEJORADAS LIGA 1465")
    print(f"💰 Apuesta: {BET_AMOUNT}€ @ {ODDS}")
    print("=" * 60)
    
    # Load
    with open(DATA_FILE, 'r', encoding='utf-8') as f:
        matches = json.load(f)
    with open(RULES_FILE, 'r', encoding='utf-8') as f:
        rules = json.load(f)
    
    print(f"📊 Partidos: {len(matches)}")
    print(f"📋 Reglas: {len(rules)}")
    
    # Validate
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
    print(f"Partidos con pick: {total_bets}")
    print(f"Sin pick: {no_pick}")
    print(f"Aciertos: {wins}")
    print(f"Fallos: {losses}")
    print(f"ACCURACY: {accuracy:.1f}%")
    print()
    print(f"💵 APOSTADO: {total_staked:.2f}€")
    print(f"💰 GANADO (bruto): {gross_wins:.2f}€")
    print(f"📈 BENEFICIO NETO: {net_profit:+.2f}€")
    print(f"📊 ROI: {roi:+.1f}%")
    
    if net_profit > 0:
        print(f"\n🎉 ¡HUBIERAS GANADO {net_profit:.2f}€!")
    else:
        print(f"\n😔 Hubieras perdido {abs(net_profit):.2f}€")

if __name__ == "__main__":
    main()
