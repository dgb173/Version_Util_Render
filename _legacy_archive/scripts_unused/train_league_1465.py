"""
ENTRENAMIENTO MEJORADO PARA LIGA 1465
=====================================
Incluye las features clave:
1. Handicap de H2H Col3 y diferencia con actual
2. Comparativas indirectas con handicap y cobertura
3. Último partido casa/fuera con handicap
"""
import json
import random
import copy
from pathlib import Path
from datetime import datetime
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

# ==================== ENHANCED FEATURE EXTRACTION ====================

def extract_enhanced_features(match: Dict) -> Dict:
    f = {}
    
    # Current match handicap
    odds = match.get('main_match_odds', {})
    try:
        current_ah = parse_ah(odds.get('ah_linea', 0)) or 0.0
        current_ou = float(odds.get('goals_linea', 2.5) or 2.5)
    except:
        return {}
    
    f['current_ah'] = current_ah
    f['current_ou'] = current_ou
    
    # Rankings
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
    f['context_fav_home'] = current_ah < 0
    
    # === H2H COL3 (CRUCIAL) ===
    col3 = match.get('h2h_col3', {})
    f['has_col3'] = col3.get('status') == 'found'
    
    if f['has_col3']:
        # Handicap del partido H2H Col3
        col3_ah = parse_ah(col3.get('ah'))
        f['col3_ah'] = col3_ah if col3_ah else 0.0
        
        # Diferencia entre handicap Col3 y actual (CLAVE)
        if col3_ah is not None:
            f['col3_ah_diff'] = current_ah - col3_ah  # Positivo = handicap actual es mayor
        else:
            f['col3_ah_diff'] = 0.0
        
        # Goles del partido Col3
        try:
            gh = int(col3.get('goles_home', 0) or 0)
            ga = int(col3.get('goles_away', 0) or 0)
            f['col3_goal_diff'] = gh - ga
            
            # Si cubrió el handicap Col3
            if col3_ah is not None:
                col3_result = get_ah_result(gh, ga, col3_ah)
                f['col3_covered'] = col3_result == 'LOCAL'
            else:
                f['col3_covered'] = False
        except:
            f['col3_goal_diff'] = 0
            f['col3_covered'] = False
    else:
        f['col3_ah'] = 0.0
        f['col3_ah_diff'] = 0.0
        f['col3_goal_diff'] = 0
        f['col3_covered'] = False
    
    # === COMPARATIVAS INDIRECTAS (CRUCIAL) ===
    comp = match.get('comparativas_indirectas', {})
    left = comp.get('left', {})
    right = comp.get('right', {})
    
    # Left side (Home team indirect)
    if left:
        left_ah = parse_ah(left.get('ah'))
        left_score = parse_score(left.get('score'))
        f['ind_left_ah'] = left_ah if left_ah else 0.0
        f['ind_left_ah_diff'] = current_ah - (left_ah or 0)  # Diff vs current
        
        if left_score and left_ah is not None:
            # Si cubrió el handicap en el indirecto
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
    
    # Right side (Away team indirect)
    if right:
        right_ah = parse_ah(right.get('ah'))
        right_score = parse_score(right.get('score'))
        f['ind_right_ah'] = right_ah if right_ah else 0.0
        f['ind_right_ah_diff'] = current_ah - (right_ah or 0)
        
        if right_score and right_ah is not None:
            if right.get('localia') == 'A':
                f['ind_right_covered'] = get_ah_result(right_score[0], right_score[1], right_ah) == 'VISITA'
            else:
                f['ind_right_covered'] = get_ah_result(right_score[0], right_score[1], right_ah) == 'LOCAL'
            f['ind_right_margin'] = right_score[0] - right_score[1]
        else:
            f['ind_right_covered'] = False
            f['ind_right_margin'] = 0
    else:
        f['ind_right_ah'] = 0.0
        f['ind_right_ah_diff'] = 0.0
        f['ind_right_covered'] = False
        f['ind_right_margin'] = 0
    
    # Margen indirecto combinado
    f['ind_margin_diff'] = f['ind_left_margin'] - f['ind_right_margin']
    
    # === ÚLTIMO PARTIDO CASA/FUERA ===
    lhm = match.get('last_home_match', {})
    lam = match.get('last_away_match', {})
    
    # Home last match
    if lhm:
        lhm_ah = parse_ah(lhm.get('ah'))
        lhm_score = parse_score(lhm.get('score'))
        f['prev_home_ah'] = lhm_ah if lhm_ah else 0.0
        f['prev_home_ah_diff'] = current_ah - (lhm_ah or 0)
        
        if lhm_score:
            f['prev_home_goal_diff'] = lhm_score[0] - lhm_score[1]
            f['prev_home_won'] = lhm_score[0] > lhm_score[1]
            if lhm_ah is not None:
                f['prev_home_covered'] = get_ah_result(lhm_score[0], lhm_score[1], lhm_ah) == 'LOCAL'
            else:
                f['prev_home_covered'] = False
        else:
            f['prev_home_goal_diff'] = 0
            f['prev_home_won'] = False
            f['prev_home_covered'] = False
    else:
        f['prev_home_ah'] = 0.0
        f['prev_home_ah_diff'] = 0.0
        f['prev_home_goal_diff'] = 0
        f['prev_home_won'] = False
        f['prev_home_covered'] = False
    
    # Away last match
    if lam:
        lam_ah = parse_ah(lam.get('ah'))
        lam_score = parse_score(lam.get('score'))
        f['prev_away_ah'] = lam_ah if lam_ah else 0.0
        f['prev_away_ah_diff'] = current_ah - (lam_ah or 0)
        
        if lam_score:
            f['prev_away_goal_diff'] = lam_score[0] - lam_score[1]
            f['prev_away_won'] = lam_score[1] > lam_score[0]  # Away won
            if lam_ah is not None:
                f['prev_away_covered'] = get_ah_result(lam_score[0], lam_score[1], lam_ah) == 'VISITA'
            else:
                f['prev_away_covered'] = False
        else:
            f['prev_away_goal_diff'] = 0
            f['prev_away_won'] = False
            f['prev_away_covered'] = False
    else:
        f['prev_away_ah'] = 0.0
        f['prev_away_ah_diff'] = 0.0
        f['prev_away_goal_diff'] = 0
        f['prev_away_won'] = False
        f['prev_away_covered'] = False
    
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
                if op == '==':
                    if curr != val: return False
                elif op == '>':
                    if not (curr > val): return False
                elif op == '<':
                    if not (curr < val): return False
                elif op == '>=':
                    if not (curr >= val): return False
                elif op == '<=':
                    if not (curr <= val): return False
            except: return False
        return True

    def evaluate(self, data_pool):
        t, c = 0, 0
        for feats, result in data_pool:
            if result == 'PUSH': continue
            if self.matches(feats):
                t += 1
                if self.prediction == result:
                    c += 1
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

def genetic_search(data_pool, features_meta, target_acc=80.0, generations=100, min_samples=5):
    population_size = 300
    population = []
    
    keys = list(features_meta.keys())
    for _ in range(population_size):
        conds = []
        for _ in range(random.randint(2, 5)):
            k = random.choice(keys)
            meta = features_meta[k]
            if meta['type'] == bool:
                val = random.choice([True, False])
                op = '=='
            elif meta['type'] in [int, float]:
                val = random.choice(meta['values']) if meta['values'] else 0
                op = random.choice(['>', '<', '>=', '<='])
            else:
                continue
            conds.append((k, op, val))
        
        pred = random.choice(['LOCAL', 'VISITA'])
        population.append(Rule(conds, pred))

    best_rules = []

    for gen in range(generations):
        for r in population:
            r.evaluate(data_pool)
        
        survivors = [r for r in population if r.stats['total'] >= min_samples and r.accuracy >= (target_acc * 0.9)]
        survivors.sort(key=lambda r: (r.accuracy, r.stats['total']), reverse=True)
        survivors = survivors[:100]
        
        for s in survivors:
            if s.accuracy >= target_acc:
                found = False
                for b in best_rules:
                    if b.name == s.name and b.prediction == s.prediction:
                        found = True
                        break
                if not found:
                    best_rules.append(copy.deepcopy(s))

        next_gen = survivors[:]
        while len(next_gen) < population_size:
            if not survivors:
                conds = []
                for _ in range(random.randint(2, 4)):
                    k = random.choice(keys)
                    meta = features_meta[k]
                    if meta['type'] == bool:
                        val = random.choice([True, False])
                        op = '=='
                    elif meta['type'] in [int, float]:
                        val = random.choice(meta['values']) if meta['values'] else 0
                        op = random.choice(['>', '<'])
                    else:
                        continue
                    conds.append((k, op, val))
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
                    val = random.choice([True, False])
                    op = '=='
                elif meta['type'] in [int, float] and meta['values']:
                    val = random.choice(meta['values'])
                    op = random.choice(['>', '<'])
                else:
                    continue
                child_conds.append((k, op, val))

            if random.random() < 0.2 and len(child_conds) > 1:
                child_conds.pop(random.randint(0, len(child_conds)-1))
                
            next_gen.append(Rule(child_conds, parent.prediction))
            
        population = next_gen
        
        if gen % 20 == 0:
            print(f"   Gen {gen}: {len(best_rules)} reglas >= {target_acc}%")

    return best_rules

# ==================== MAIN ====================

def main():
    print("=" * 60)
    print("🎯 ENTRENAMIENTO MEJORADO LIGA 1465")
    print("🔑 Features: Col3 Handicap, Indirectas, Prev Matches")
    print("=" * 60)
    
    # Load data
    with open(DATA_FILE, 'r', encoding='utf-8') as f:
        matches = json.load(f)
    print(f"\n📊 Cargados {len(matches)} partidos")
    
    # Extract features and results
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
        
        feats = extract_enhanced_features(m)
        if not feats: continue
        
        dataset.append((feats, result))
        
        for k, v in feats.items():
            if k not in feature_values:
                feature_values[k] = {'type': type(v), 'values': set()}
            if isinstance(v, (int, float)):
                feature_values[k]['values'].add(v)
            elif isinstance(v, bool):
                feature_values[k]['values'].add(v)
    
    print(f"   Partidos válidos para entrenamiento: {len(dataset)}")
    print(f"   Features extraídas: {len(feature_values)}")
    
    # Convert sets to lists
    for k in feature_values:
        feature_values[k]['values'] = sorted(list(feature_values[k]['values']))
    
    # Train
    print("\n🔄 Entrenando (target: 80% accuracy)...")
    rules = genetic_search(dataset, feature_values, target_acc=80.0, generations=150, min_samples=5)
    
    # Filter and save
    rules = [r for r in rules if r.stats['total'] >= 5]
    rules.sort(key=lambda r: (r.accuracy, r.stats['total']), reverse=True)
    
    print(f"\n🏆 REGLAS ENCONTRADAS: {len(rules)}")
    for r in rules[:10]:
        print(f"   [{r.prediction}] {r.name}")
        print(f"      → Acc: {r.accuracy:.1f}% ({r.stats['correct']}/{r.stats['total']})")
    
    # Save
    output_rules = []
    for r in rules:
        output_rules.append({
            'name': r.name,
            'prediction': r.prediction,
            'conditions': r.conditions,
            'accuracy': r.accuracy,
            'stats': r.stats,
            'type': 'AH',
            'target_line': 0.5  # Apply to all
        })
    
    out_file = RESULTS_DIR / 'specialist_league_1465.json'
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(output_rules, f, indent=2)
    
    print(f"\n💾 Guardadas {len(output_rules)} reglas en {out_file}")
    
    # Quick validation
    if rules:
        hits = sum(1 for feats, res in dataset if any(r.matches(feats) and r.prediction == res for r in rules[:10]))
        total_with_pick = sum(1 for feats, _ in dataset if any(r.matches(feats) for r in rules[:10]))
        if total_with_pick > 0:
            print(f"\n📈 Validación rápida (top 10 reglas): {hits}/{total_with_pick} = {hits/total_with_pick*100:.1f}%")

if __name__ == "__main__":
    main()
