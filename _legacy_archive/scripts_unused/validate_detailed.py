"""
Validación DETALLADA - Muestra cada partido con predicción y resultado
Liga: 292 (2023-2024)
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
OUTPUT_FILE = "validation_detailed_292.json"

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
    diff = (hg - ag) - ah  # LOCAL favorito con HA positivo
    if diff > 0.25: return 'LOCAL'
    if diff < -0.25: return 'VISITA'
    return 'PUSH'

def extract_features(m):
    f = {}
    odds = m.get('main_match_odds', {})
    try:
        current_ah = parse_ah(odds.get('ah_linea', 0)) or 0.0
    except:
        return {}
    
    f['current_ah'] = current_ah
    f['context_fav_home'] = current_ah > 0  # HA positivo = LOCAL favorito
    f['fav_home_strong'] = current_ah >= 1.0  # LOCAL muy favorito
    f['fav_away_strong'] = current_ah <= -1.0  # VISITANTE muy favorito
    f['is_heavy_fav'] = abs(current_ah) >= 1.0
    f['underdog_home'] = current_ah <= -1.0
    f['underdog_away'] = current_ah >= 1.0
    
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
    
    # Prev matches
    lhm = m.get('last_home_match') or {}
    lhm_score = parse_score(lhm.get('score')) if lhm else None
    f['prev_home_won'] = lhm_score[0] > lhm_score[1] if lhm_score else False
    f['prev_home_lost'] = lhm_score[0] < lhm_score[1] if lhm_score else False
    
    lam = m.get('last_away_match') or {}
    lam_score = parse_score(lam.get('score')) if lam else None
    f['prev_away_won'] = lam_score[1] > lam_score[0] if lam_score else False
    f['prev_away_lost'] = lam_score[1] < lam_score[0] if lam_score else False
    
    f['home_momentum'] = f['prev_home_won']
    f['away_momentum'] = f['prev_away_won']
    f['both_won'] = f['prev_home_won'] and f['prev_away_won']
    f['both_lost'] = f['prev_home_lost'] and f['prev_away_lost']
    
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
    print(f"🎯 VALIDACIÓN DETALLADA LIGA {LEAGUE_ID} (Temporada {SEASON})")
    print("=" * 70)
    
    with open(RULES_FILE, 'r', encoding='utf-8') as f:
        rules = json.load(f)
    print(f"📋 Cargadas {len(rules)} reglas")

    print("\n📋 Extrayendo IDs de partidos...")
    result = league_scraper.extract_ids_by_params(SEASON, LEAGUE_ID)
    match_data = result.get('match_data', [])
    ids = [str(m['id']) for m in match_data]
    print(f"✅ Encontrados {len(ids)} partidos")

    # Scrape ALL matches
    print(f"\n🔍 Scrapeando TODOS los {len(ids)} partidos...")
    
    matches = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(scrape_match, mid): mid for mid in ids}
        for i, future in enumerate(as_completed(futures)):
            result = future.result()
            if result:
                matches.append(result)
            if (i+1) % 50 == 0:
                print(f"   Procesados {i+1}/{len(ids)}...")
    
    print(f"✅ Scrapeados {len(matches)} partidos")
    
    # Validate with details
    print("\n🔄 Validando y generando reporte detallado...")
    
    detailed_results = []
    wins = 0
    losses = 0
    no_pick = 0
    
    for m in matches:
        home = m.get('home_team', 'Unknown')
        away = m.get('away_team', 'Unknown')
        date = m.get('match_date', 'Unknown')
        
        score = parse_score(m.get('final_score') or m.get('score'))
        if not score: continue
        
        odds_data = m.get('main_match_odds', {})
        ah = parse_ah(odds_data.get('ah_linea'))
        if ah is None: continue
        
        actual_result = get_ah_result(score[0], score[1], ah)
        if actual_result == 'PUSH': continue
        
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
        rule_name = best.get('name', '')
        accuracy = best.get('accuracy', 0)
        
        is_correct = prediction == actual_result
        if is_correct:
            wins += 1
        else:
            losses += 1
        
        detail = {
            'date': date,
            'home': home,
            'away': away,
            'score': f"{score[0]}-{score[1]}",
            'ah': ah,
            'prediction': prediction,
            'actual': actual_result,
            'correct': is_correct,
            'rule': rule_name[:60],
            'rule_accuracy': accuracy
        }
        detailed_results.append(detail)
        
        # Print each result
        icon = "✅" if is_correct else "❌"
        print(f"{icon} {home} vs {away} | HA:{ah:+.2f} | Pred:{prediction} | Real:{actual_result} | Score:{score[0]}-{score[1]}")
    
    # Save results
    output_data = {
        'league_id': LEAGUE_ID,
        'season': SEASON,
        'total_matches': len(matches),
        'picks': wins + losses,
        'no_pick': no_pick,
        'wins': wins,
        'losses': losses,
        'accuracy': (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0,
        'details': detailed_results
    }
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    # Summary
    total_bets = wins + losses
    total_staked = total_bets * BET_AMOUNT
    gross_wins = wins * BET_AMOUNT * ODDS
    net_profit = gross_wins - total_staked
    accuracy = (wins / total_bets * 100) if total_bets > 0 else 0
    roi = (net_profit / total_staked * 100) if total_staked > 0 else 0
    
    print("\n" + "=" * 70)
    print("📊 RESUMEN")
    print("=" * 70)
    print(f"Partidos: {len(matches)} | Picks: {total_bets} | Sin pick: {no_pick}")
    print(f"Aciertos: {wins} | Fallos: {losses} | ACCURACY: {accuracy:.1f}%")
    print(f"💵 APOSTADO: {total_staked:.2f}€")
    print(f"💰 BENEFICIO: {net_profit:+.2f}€ | YIELD: {roi:+.1f}%")
    print(f"\n📄 Reporte guardado en: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
