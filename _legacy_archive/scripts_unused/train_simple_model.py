"""
MODELO SIMPLIFICADO - Reglas más generales que apliquen a más partidos
======================================================================
Usa umbrales bajos y condiciones simples para maximizar cobertura.
"""
import json
import random
import copy
from pathlib import Path
from typing import Dict, List, Tuple, Optional

DATA_DIR = Path("data")
RESULTS_DIR = Path("backtest_results")

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
    diff = (hg - ag) - ah  # LOCAL favorito con HA positivo
    if diff > 0.25: return 'LOCAL'
    if diff < -0.25: return 'VISITA'
    return 'PUSH'

def extract_simple_features(m):
    """Extract simpler, more common features."""
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
    f['underdog_home'] = current_ah <= -1.0 # Local es underdog (Visita muy fav)
    f['underdog_away'] = current_ah >= 1.0 # Visita es underdog (Local muy fav)
    
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
    
    # Col3 - simplified
    col3 = m.get('h2h_col3', {}) or {}
    f['has_col3'] = col3.get('status') == 'found'
    
    # Prev home - simplified
    lhm = m.get('last_home_match') or {}
    lhm_score = parse_score(lhm.get('score')) if lhm else None
    f['prev_home_won'] = lhm_score[0] > lhm_score[1] if lhm_score else False
    f['prev_home_lost'] = lhm_score[0] < lhm_score[1] if lhm_score else False
    f['prev_home_draw'] = lhm_score[0] == lhm_score[1] if lhm_score else False
    
    # Prev away - simplified
    lam = m.get('last_away_match') or {}
    lam_score = parse_score(lam.get('score')) if lam else None
    f['prev_away_won'] = lam_score[1] > lam_score[0] if lam_score else False
    f['prev_away_lost'] = lam_score[1] < lam_score[0] if lam_score else False
    f['prev_away_draw'] = lam_score[1] == lam_score[0] if lam_score else False
    
    # Combined momentum patterns
    f['home_momentum'] = f['prev_home_won'] and not f['prev_away_won']
    f['away_momentum'] = f['prev_away_won'] and not f['prev_home_won']
    f['both_won'] = f['prev_home_won'] and f['prev_away_won']
    f['both_lost'] = f['prev_home_lost'] and f['prev_away_lost']
    
    return f

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
                if op == '!=' and curr == val: return False
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

def genetic_search(data_pool, features_meta, target_acc=75.0, generations=150, min_samples=30):
    """Use more lenient parameters for broader coverage."""
    population_size = 400
    population = []
    
    keys = list(features_meta.keys())
    for _ in range(population_size):
        conds = []
        for _ in range(random.randint(1, 3)):  # Fewer conditions = more general
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
        
        # More lenient filtering
        survivors = [r for r in population if r.stats['total'] >= min_samples and r.accuracy >= (target_acc * 0.95)]
        survivors.sort(key=lambda r: (r.accuracy, r.stats['total']), reverse=True)
        survivors = survivors[:150]
        
        for s in survivors:
            if s.accuracy >= target_acc:
                if not any(b.name == s.name and b.prediction == s.prediction for b in best_rules):
                    best_rules.append(copy.deepcopy(s))

        next_gen = survivors[:]
        while len(next_gen) < population_size:
            if not survivors:
                conds = [(random.choice(keys), '==', True)]
                next_gen.append(Rule(conds, random.choice(['LOCAL', 'VISITA'])))
                continue

            parent = random.choice(survivors)
            child_conds = copy.deepcopy(parent.conditions)
            
            if random.random() < 0.4 and child_conds:
                idx = random.randint(0, len(child_conds)-1)
                k = child_conds[idx][0]
                meta = features_meta.get(k, {})
                if meta.get('type') in [int, float] and meta.get('values'):
                    child_conds[idx] = (k, child_conds[idx][1], random.choice(meta['values']))
            
            if random.random() < 0.3 and len(child_conds) < 3:
                k = random.choice(keys)
                meta = features_meta[k]
                if meta['type'] == bool:
                    child_conds.append((k, '==', random.choice([True, False])))
                elif meta['type'] in [int, float] and meta['values']:
                    child_conds.append((k, random.choice(['>', '<']), random.choice(meta['values'])))

            if random.random() < 0.3 and len(child_conds) > 1:
                child_conds.pop(random.randint(0, len(child_conds)-1))
                
            next_gen.append(Rule(child_conds, parent.prediction))
            
        population = next_gen
        if gen % 25 == 0: print(f"   Gen {gen}: {len(best_rules)} reglas >= {target_acc}%")

    return best_rules

def main():
    print("=" * 60)
    print("🎯 MODELO SIMPLIFICADO - REGLAS GENERALES")
    print("📊 Target: 75% accuracy, mínimo 30 samples")
    print("=" * 60)
    
    all_matches = []
    for f in DATA_DIR.glob("data*.json"):
        if 'backup' in f.name or 'pending' in f.name or 'precacheo' in f.name:
            continue
        try:
            with open(f, 'r', encoding='utf-8') as fh:
                data = json.load(fh)
                if isinstance(data, list):
                    print(f"   📂 {f.name}: {len(data)} partidos")
                    all_matches.extend(data)
        except:
            pass
    
    print(f"\n📊 TOTAL: {len(all_matches)} partidos")
    
    dataset = []
    feature_values = {}
    
    for m in all_matches:
        score = parse_score(m.get('final_score') or m.get('score'))
        if not score: continue
        
        odds = m.get('main_match_odds', {})
        ah = parse_ah(odds.get('ah_linea', 0))
        if ah is None: continue
        
        result = get_ah_result(score[0], score[1], ah)
        if result == 'PUSH': continue
        
        feats = extract_simple_features(m)
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
    print(f"   Features: {len(feature_values)}")
    
    for k in feature_values:
        vals = list(feature_values[k]['values'])
        feature_values[k]['values'] = sorted(vals[:50]) if len(vals) > 50 else sorted(vals)
    
    print("\n🔄 Entrenando reglas SIMPLES (target: 65%, min 25 samples)...")
    rules = genetic_search(dataset, feature_values, target_acc=65.0, generations=300, min_samples=25)
    
    rules = [r for r in rules if r.stats['total'] >= 30]
    rules.sort(key=lambda r: (r.accuracy, r.stats['total']), reverse=True)
    
    print(f"\n🏆 REGLAS SIMPLES ENCONTRADAS: {len(rules)}")
    for r in rules[:15]:
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
            'model': 'SIMPLE'
        })
    
    out_file = RESULTS_DIR / 'specialist_simple.json'
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(output_rules, f, indent=2)
    
    print(f"\n💾 Guardadas {len(output_rules)} reglas en {out_file}")

if __name__ == "__main__":
    main()
