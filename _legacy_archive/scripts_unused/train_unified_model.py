"""
MODELO UNIFICADO COMPLETO
=========================
Combina TODAS las features en un solo modelo:
1. Col3 Handicap + diferencia vs actual
2. Comparativas Indirectas + handicap + cobertura
3. Ataques Peligrosos + Tiros + Posesión
4. Movimiento de línea (subió/bajó)
5. Último resultado en estadio
6. Correlación: movimiento + resultado previo
"""
import json
import random
import copy
from pathlib import Path
from typing import Dict, List, Tuple, Optional

DATA_FILE = Path("training_data_1465.json")
RESULTS_DIR = Path("backtest_results")
RESULTS_DIR.mkdir(exist_ok=True)

# ==================== UTILS ====================

def parse_score(score_str) -> Optional[Tuple[int, int]]:
    if not score_str: return None
    s = str(score_str).replace(':', '-').replace(' ', '')
    if '-' not in s: return None
    try:
        p = s.split('-')
        return int(p[0]), int(p[1])
    except:
        return None

def parse_ah(ah_str):
    if not ah_str or ah_str == 'N/A': return None
    try:
        return float(ah_str)
    except:
        return None

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

# ==================== UNIFIED FEATURE EXTRACTION ====================

def extract_all_features(match: Dict) -> Dict:
    f = {}
    
    # === HANDICAP ACTUAL ===
    odds = match.get('main_match_odds', {})
    try:
        current_ah = parse_ah(odds.get('ah_linea', 0)) or 0.0
        current_ou = float(odds.get('goals_linea', 2.5) or 2.5)
    except:
        return {}
    
    f['current_ah'] = current_ah
    f['current_ou'] = current_ou
    f['context_fav_home'] = current_ah < 0
    
    # === RANKINGS ===
    hs = match.get('home_standings', {})
    as_ = match.get('away_standings', {})
    try:
        hr_val = hs.get('ranking', 0)
        ar_val = as_.get('ranking', 0)
        hr = int(hr_val) if hr_val and str(hr_val) not in ['N/A', '', 'None'] else 0
        ar = int(ar_val) if ar_val and str(ar_val) not in ['N/A', '', 'None'] else 0
    except:
        hr, ar = 0, 0
    f['rank_diff'] = ar - hr if hr > 0 and ar > 0 else 0
    f['home_better_rank'] = hr < ar if hr > 0 and ar > 0 else False
    
    # === H2H COL3 ===
    col3 = match.get('h2h_col3', {})
    f['has_col3'] = col3.get('status') == 'found'
    
    col3_ah = parse_ah(col3.get('ah'))
    f['col3_ah'] = col3_ah if col3_ah else 0.0
    f['col3_ah_diff'] = current_ah - (col3_ah or 0)
    
    try:
        gh = int(col3.get('goles_home', 0) or 0)
        ga = int(col3.get('goles_away', 0) or 0)
        f['col3_goal_diff'] = gh - ga
        if col3_ah is not None:
            f['col3_covered'] = get_ah_result(gh, ga, col3_ah) == 'LOCAL'
        else:
            f['col3_covered'] = False
    except:
        f['col3_goal_diff'] = 0
        f['col3_covered'] = False
    
    # === COMPARATIVAS INDIRECTAS ===
    comp = match.get('comparativas_indirectas', {})
    left = comp.get('left', {})
    right = comp.get('right', {})
    
    if left:
        left_ah = parse_ah(left.get('ah'))
        left_score = parse_score(left.get('score'))
        f['ind_left_ah'] = left_ah if left_ah else 0.0
        f['ind_left_ah_diff'] = current_ah - (left_ah or 0)
        if left_score and left_ah is not None:
            if left.get('localia') == 'H':
                f['ind_left_covered'] = get_ah_result(left_score[0], left_score[1], left_ah) == 'LOCAL'
            else:
                f['ind_left_covered'] = get_ah_result(left_score[1], left_score[0], -left_ah) == 'VISITA'
            f['ind_left_margin'] = left_score[0] - left_score[1]
        else:
            f['ind_left_covered'] = False
            f['ind_left_margin'] = 0
    else:
        f['ind_left_ah'] = 0.0
        f['ind_left_ah_diff'] = 0.0
        f['ind_left_covered'] = False
        f['ind_left_margin'] = 0
    
    if right:
        right_ah = parse_ah(right.get('ah'))
        right_score = parse_score(right.get('score'))
        f['ind_right_ah'] = right_ah if right_ah else 0.0
        f['ind_right_ah_diff'] = current_ah - (right_ah or 0)
        if right_score and right_ah is not None:
            f['ind_right_covered'] = get_ah_result(right_score[0], right_score[1], right_ah) == 'VISITA'
            f['ind_right_margin'] = right_score[0] - right_score[1]
        else:
            f['ind_right_covered'] = False
            f['ind_right_margin'] = 0
    else:
        f['ind_right_ah'] = 0.0
        f['ind_right_ah_diff'] = 0.0
        f['ind_right_covered'] = False
        f['ind_right_margin'] = 0
    
    f['ind_margin_diff'] = f['ind_left_margin'] - f['ind_right_margin']
    
    # === ÚLTIMO PARTIDO EN CASA (con estadísticas) ===
    lhm = match.get('last_home_match') or {}
    lhm_ah = parse_ah(lhm.get('ah')) if lhm else None
    lhm_score = parse_score(lhm.get('score')) if lhm else None
    
    f['prev_home_ah'] = lhm_ah if lhm_ah else 0.0
    f['prev_home_ah_diff'] = current_ah - (lhm_ah or 0)
    
    # Movimiento de línea: subió o bajó respecto al prev
    if lhm_ah is not None:
        f['line_moved_up_vs_prev_home'] = current_ah > lhm_ah
        f['line_moved_down_vs_prev_home'] = current_ah < lhm_ah
        f['line_same_as_prev_home'] = abs(current_ah - lhm_ah) < 0.1
    else:
        f['line_moved_up_vs_prev_home'] = False
        f['line_moved_down_vs_prev_home'] = False
        f['line_same_as_prev_home'] = False
    
    if lhm_score:
        f['prev_home_goal_diff'] = lhm_score[0] - lhm_score[1]
        f['prev_home_won'] = lhm_score[0] > lhm_score[1]
        f['prev_home_lost'] = lhm_score[0] < lhm_score[1]
        if lhm_ah is not None:
            f['prev_home_covered'] = get_ah_result(lhm_score[0], lhm_score[1], lhm_ah) == 'LOCAL'
        else:
            f['prev_home_covered'] = False
            
        # CORRELACIÓN: Ganó + línea subió = confianza
        f['home_won_line_up'] = f['prev_home_won'] and f['line_moved_up_vs_prev_home']
        f['home_lost_line_down'] = f['prev_home_lost'] and f['line_moved_down_vs_prev_home']
    else:
        f['prev_home_goal_diff'] = 0
        f['prev_home_won'] = False
        f['prev_home_lost'] = False
        f['prev_home_covered'] = False
        f['home_won_line_up'] = False
        f['home_lost_line_down'] = False
    
    # Stats del prev home
    if lhm.get('stats_rows'):
        stats = parse_stats_rows(lhm.get('stats_rows'))
        da = stats.get('Ataques Peligrosos', stats.get('Dangerous Attacks', {}))
        f['prev_home_da_diff'] = da.get('diff', 0)
        f['prev_home_dominated'] = da.get('diff', 0) > 10
        f['prev_home_was_dominated'] = da.get('diff', 0) < -10
        
        shots = stats.get('Tiros', stats.get('Shots', {}))
        f['prev_home_shots_diff'] = shots.get('diff', 0)
        
        # Unlucky loss
        f['prev_home_unlucky_loss'] = f['prev_home_lost'] and f['prev_home_da_diff'] > 15
    else:
        f['prev_home_da_diff'] = 0
        f['prev_home_dominated'] = False
        f['prev_home_was_dominated'] = False
        f['prev_home_shots_diff'] = 0
        f['prev_home_unlucky_loss'] = False
    
    # === ÚLTIMO PARTIDO FUERA (con estadísticas) ===
    lam = match.get('last_away_match') or {}
    lam_ah = parse_ah(lam.get('ah')) if lam else None
    lam_score = parse_score(lam.get('score')) if lam else None
    
    f['prev_away_ah'] = lam_ah if lam_ah else 0.0
    f['prev_away_ah_diff'] = current_ah - (lam_ah or 0)
    
    if lam_ah is not None:
        f['line_moved_up_vs_prev_away'] = current_ah > lam_ah
        f['line_moved_down_vs_prev_away'] = current_ah < lam_ah
    else:
        f['line_moved_up_vs_prev_away'] = False
        f['line_moved_down_vs_prev_away'] = False
    
    if lam_score:
        f['prev_away_goal_diff'] = lam_score[1] - lam_score[0]
        f['prev_away_won'] = lam_score[1] > lam_score[0]
        f['prev_away_lost'] = lam_score[1] < lam_score[0]
        if lam_ah is not None:
            f['prev_away_covered'] = get_ah_result(lam_score[0], lam_score[1], lam_ah) == 'VISITA'
        else:
            f['prev_away_covered'] = False
            
        # CORRELACIÓN
        f['away_won_line_up'] = f['prev_away_won'] and f['line_moved_up_vs_prev_away']
        f['away_lost_line_down'] = f['prev_away_lost'] and f['line_moved_down_vs_prev_away']
    else:
        f['prev_away_goal_diff'] = 0
        f['prev_away_won'] = False
        f['prev_away_lost'] = False
        f['prev_away_covered'] = False
        f['away_won_line_up'] = False
        f['away_lost_line_down'] = False
    
    if lam.get('stats_rows'):
        stats = parse_stats_rows(lam.get('stats_rows'))
        da = stats.get('Ataques Peligrosos', stats.get('Dangerous Attacks', {}))
        f['prev_away_da_diff'] = da.get('away', 0) - da.get('home', 0)
        f['prev_away_dominated'] = f['prev_away_da_diff'] > 10
        f['prev_away_was_dominated'] = f['prev_away_da_diff'] < -10
        
        shots = stats.get('Tiros', stats.get('Shots', {}))
        f['prev_away_shots_diff'] = shots.get('away', 0) - shots.get('home', 0)
        
        f['prev_away_unlucky_loss'] = f['prev_away_lost'] and f['prev_away_da_diff'] > 15
    else:
        f['prev_away_da_diff'] = 0
        f['prev_away_dominated'] = False
        f['prev_away_was_dominated'] = False
        f['prev_away_shots_diff'] = 0
        f['prev_away_unlucky_loss'] = False
    
    # === FEATURES COMBINADAS ===
    f['dominance_diff'] = f['prev_home_da_diff'] - f['prev_away_da_diff']
    f['home_momentum'] = f['prev_home_won'] and f['prev_home_dominated']
    f['away_momentum'] = f['prev_away_won'] and f['prev_away_dominated']
    f['home_vulnerable'] = f['prev_home_lost'] and f['prev_home_was_dominated']
    f['away_vulnerable'] = f['prev_away_lost'] and f['prev_away_was_dominated']
    
    return f

# ==================== GENETIC ALGORITHM ====================

class Rule:
    def __init__(self, conditions, prediction):
        self.conditions = conditions
        self.prediction = prediction
        self.stats = {'total': 0, 'correct': 0}
        
    def matches(self, features: Dict) -> bool:
        for feat, op, val in self.conditions:
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

    def evaluate(self, data_pool):
        t, c = 0, 0
        for feats, result in data_pool:
            if result == 'PUSH': continue
            if self.matches(feats):
                t += 1
                if self.prediction == result: c += 1
        self.stats = {'total': t, 'correct': c}
        
    @property
    def accuracy(self):
        return (self.stats['correct'] / self.stats['total'] * 100) if self.stats['total'] > 0 else 0

    @property
    def name(self):
        parts = []
        for feat, op, val in self.conditions:
            readable = feat.replace('_', ' ').title()
            if isinstance(val, bool):
                parts.append(f"[{readable} is {val}]")
            else:
                parts.append(f"[{readable} {op} {val}]")
        return " + ".join(parts)

def genetic_search(data_pool, features_meta, target_acc=80.0, generations=200, min_samples=5):
    population_size = 400
    population = []
    
    keys = list(features_meta.keys())
    for _ in range(population_size):
        conds = []
        for _ in range(random.randint(2, 6)):
            k = random.choice(keys)
            meta = features_meta[k]
            if meta['type'] == bool:
                val = random.choice([True, False])
                op = '=='
            elif meta['type'] in [int, float]:
                val = random.choice(meta['values']) if meta['values'] else 0
                op = random.choice(['>', '<', '>=', '<='])
            else: continue
            conds.append((k, op, val))
        population.append(Rule(conds, random.choice(['LOCAL', 'VISITA'])))

    best_rules = []

    for gen in range(generations):
        for r in population: r.evaluate(data_pool)
        
        survivors = [r for r in population if r.stats['total'] >= min_samples and r.accuracy >= (target_acc * 0.9)]
        survivors.sort(key=lambda r: (r.accuracy, r.stats['total']), reverse=True)
        survivors = survivors[:150]
        
        for s in survivors:
            if s.accuracy >= target_acc:
                if not any(b.name == s.name and b.prediction == s.prediction for b in best_rules):
                    best_rules.append(copy.deepcopy(s))

        next_gen = survivors[:]
        while len(next_gen) < population_size:
            if not survivors:
                conds = [(random.choice(keys), '>', 0) for _ in range(3)]
                next_gen.append(Rule(conds, random.choice(['LOCAL', 'VISITA'])))
                continue

            parent = random.choice(survivors)
            child_conds = copy.deepcopy(parent.conditions)
            
            if random.random() < 0.3 and child_conds:
                idx = random.randint(0, len(child_conds)-1)
                k = child_conds[idx][0]
                meta = features_meta.get(k, {})
                if meta.get('type') in [int, float] and meta.get('values'):
                    child_conds[idx] = (k, child_conds[idx][1], random.choice(meta['values']))
            
            if random.random() < 0.3:
                k = random.choice(keys)
                meta = features_meta[k]
                if meta['type'] == bool:
                    child_conds.append((k, '==', random.choice([True, False])))
                elif meta['type'] in [int, float] and meta['values']:
                    child_conds.append((k, random.choice(['>', '<']), random.choice(meta['values'])))

            if random.random() < 0.2 and len(child_conds) > 2:
                child_conds.pop(random.randint(0, len(child_conds)-1))
                
            next_gen.append(Rule(child_conds, parent.prediction))
            
        population = next_gen
        if gen % 25 == 0: print(f"   Gen {gen}: {len(best_rules)} reglas >= {target_acc}%")

    return best_rules

# ==================== MAIN ====================

def main():
    print("=" * 60)
    print("🎯 MODELO UNIFICADO COMPLETO")
    print("📊 ALL FEATURES: Col3 + Indirectas + Stats + Movimiento")
    print("=" * 60)
    
    with open(DATA_FILE, 'r', encoding='utf-8') as f:
        matches = json.load(f)
    print(f"\n📊 Cargados {len(matches)} partidos")
    
    dataset = []
    feature_values = {}
    
    for m in matches:
        score = parse_score(m.get('final_score') or m.get('score'))
        if not score: continue
        
        odds = m.get('main_match_odds', {})
        ah = parse_ah(odds.get('ah_linea', 0))
        if ah is None: continue
        
        result = get_ah_result(score[0], score[1], ah)
        if result == 'PUSH': continue
        
        feats = extract_all_features(m)
        if not feats: continue
        
        dataset.append((feats, result))
        
        for k, v in feats.items():
            if k not in feature_values:
                feature_values[k] = {'type': type(v), 'values': set()}
            if isinstance(v, (int, float)):
                feature_values[k]['values'].add(v)
            elif isinstance(v, bool):
                feature_values[k]['values'].add(v)
    
    print(f"   Partidos válidos: {len(dataset)}")
    print(f"   Features totales: {len(feature_values)}")
    
    for k in feature_values:
        feature_values[k]['values'] = sorted(list(feature_values[k]['values']))
    
    print("\n🔄 Entrenando MODELO UNIFICADO (target: 80%)...")
    rules = genetic_search(dataset, feature_values, target_acc=80.0, generations=200, min_samples=5)
    
    rules = [r for r in rules if r.stats['total'] >= 5]
    rules.sort(key=lambda r: (r.accuracy, r.stats['total']), reverse=True)
    
    print(f"\n🏆 REGLAS UNIFICADAS: {len(rules)}")
    for r in rules[:10]:
        print(f"   [{r.prediction}] {r.name}")
        print(f"      → Acc: {r.accuracy:.1f}% ({r.stats['correct']}/{r.stats['total']})")
    
    output_rules = []
    for r in rules:
        output_rules.append({
            'name': r.name,
            'prediction': r.prediction,
            'conditions': r.conditions,
            'accuracy': r.accuracy,
            'stats': r.stats,
            'type': 'AH',
            'model': 'UNIFIED'
        })
    
    out_file = RESULTS_DIR / 'specialist_unified.json'
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(output_rules, f, indent=2)
    
    print(f"\n💾 Guardadas {len(output_rules)} reglas en {out_file}")

if __name__ == "__main__":
    main()
