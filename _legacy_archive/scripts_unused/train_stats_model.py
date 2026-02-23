"""
MODELO 2: ENTRENAMIENTO CON ESTADÍSTICAS DE RENDIMIENTO
========================================================
Criterios basados en:
1. Ataques Peligrosos (prev home/away)
2. Tiros / Tiros a Puerta
3. Posesión
4. Dominancia estadística
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
    """Parse stats_rows into a dict."""
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

# ==================== STATS-BASED FEATURE EXTRACTION ====================

def extract_stats_features(match: Dict) -> Dict:
    f = {}
    
    # Current match handicap
    odds = match.get('main_match_odds', {})
    try:
        current_ah = parse_ah(odds.get('ah_linea', 0)) or 0.0
    except:
        return {}
    
    f['current_ah'] = current_ah
    f['context_fav_home'] = current_ah < 0
    
    # === ÚLTIMO PARTIDO EN CASA - ESTADÍSTICAS ===
    lhm = match.get('last_home_match', {})
    if lhm and lhm.get('stats_rows'):
        stats = parse_stats_rows(lhm.get('stats_rows'))
        
        # Ataques Peligrosos
        da = stats.get('Ataques Peligrosos', stats.get('Dangerous Attacks', {}))
        f['prev_home_dangerous_attacks_h'] = da.get('home', 0)
        f['prev_home_dangerous_attacks_a'] = da.get('away', 0)
        f['prev_home_dangerous_attacks_diff'] = da.get('diff', 0)
        
        # Dominancia (si tuvo más ataques peligrosos que el rival)
        f['prev_home_dominated'] = da.get('diff', 0) > 10
        f['prev_home_was_dominated'] = da.get('diff', 0) < -10
        
        # Tiros
        shots = stats.get('Tiros', stats.get('Shots', {}))
        f['prev_home_shots_diff'] = shots.get('diff', 0)
        
        # Tiros a Puerta
        shots_on = stats.get('Tiros a puerta', stats.get('Shots on Target', {}))
        f['prev_home_shots_on_diff'] = shots_on.get('diff', 0)
        
        # Ataques
        attacks = stats.get('Ataques', stats.get('Attacks', {}))
        f['prev_home_attacks_diff'] = attacks.get('diff', 0)
        
        # Posesión
        poss = stats.get('Posesión', stats.get('Possession', {}))
        f['prev_home_possession_h'] = poss.get('home', 50)
        f['prev_home_possession_diff'] = poss.get('diff', 0)
        
        # Score del partido previo
        lhm_score = parse_score(lhm.get('score'))
        if lhm_score:
            f['prev_home_won'] = lhm_score[0] > lhm_score[1]
            f['prev_home_lost'] = lhm_score[0] < lhm_score[1]
            f['prev_home_goal_diff'] = lhm_score[0] - lhm_score[1]
            # Unlucky loss: Dominó pero perdió
            f['prev_home_unlucky_loss'] = (lhm_score[0] < lhm_score[1]) and (da.get('diff', 0) > 15)
        else:
            f['prev_home_won'] = False
            f['prev_home_lost'] = False
            f['prev_home_goal_diff'] = 0
            f['prev_home_unlucky_loss'] = False
    else:
        f['prev_home_dangerous_attacks_h'] = 0
        f['prev_home_dangerous_attacks_a'] = 0
        f['prev_home_dangerous_attacks_diff'] = 0
        f['prev_home_dominated'] = False
        f['prev_home_was_dominated'] = False
        f['prev_home_shots_diff'] = 0
        f['prev_home_shots_on_diff'] = 0
        f['prev_home_attacks_diff'] = 0
        f['prev_home_possession_h'] = 50
        f['prev_home_possession_diff'] = 0
        f['prev_home_won'] = False
        f['prev_home_lost'] = False
        f['prev_home_goal_diff'] = 0
        f['prev_home_unlucky_loss'] = False
    
    # === ÚLTIMO PARTIDO FUERA - ESTADÍSTICAS ===
    lam = match.get('last_away_match', {})
    if lam and lam.get('stats_rows'):
        stats = parse_stats_rows(lam.get('stats_rows'))
        
        # Ataques Peligrosos (perspectiva del visitante)
        da = stats.get('Ataques Peligrosos', stats.get('Dangerous Attacks', {}))
        # Para el away, los valores están invertidos (away es "nuestro" equipo)
        f['prev_away_dangerous_attacks_h'] = da.get('home', 0)
        f['prev_away_dangerous_attacks_a'] = da.get('away', 0)
        f['prev_away_dangerous_attacks_diff'] = da.get('away', 0) - da.get('home', 0)  # Invertido
        
        f['prev_away_dominated'] = f['prev_away_dangerous_attacks_diff'] > 10
        f['prev_away_was_dominated'] = f['prev_away_dangerous_attacks_diff'] < -10
        
        # Tiros
        shots = stats.get('Tiros', stats.get('Shots', {}))
        f['prev_away_shots_diff'] = shots.get('away', 0) - shots.get('home', 0)
        
        # Tiros a Puerta
        shots_on = stats.get('Tiros a puerta', stats.get('Shots on Target', {}))
        f['prev_away_shots_on_diff'] = shots_on.get('away', 0) - shots_on.get('home', 0)
        
        # Ataques
        attacks = stats.get('Ataques', stats.get('Attacks', {}))
        f['prev_away_attacks_diff'] = attacks.get('away', 0) - attacks.get('home', 0)
        
        # Posesión
        poss = stats.get('Posesión', stats.get('Possession', {}))
        f['prev_away_possession_a'] = poss.get('away', 50)
        f['prev_away_possession_diff'] = poss.get('away', 0) - poss.get('home', 0)
        
        # Score
        lam_score = parse_score(lam.get('score'))
        if lam_score:
            f['prev_away_won'] = lam_score[1] > lam_score[0]
            f['prev_away_lost'] = lam_score[1] < lam_score[0]
            f['prev_away_goal_diff'] = lam_score[1] - lam_score[0]
            f['prev_away_unlucky_loss'] = (lam_score[1] < lam_score[0]) and (f['prev_away_dangerous_attacks_diff'] > 15)
        else:
            f['prev_away_won'] = False
            f['prev_away_lost'] = False
            f['prev_away_goal_diff'] = 0
            f['prev_away_unlucky_loss'] = False
    else:
        f['prev_away_dangerous_attacks_h'] = 0
        f['prev_away_dangerous_attacks_a'] = 0
        f['prev_away_dangerous_attacks_diff'] = 0
        f['prev_away_dominated'] = False
        f['prev_away_was_dominated'] = False
        f['prev_away_shots_diff'] = 0
        f['prev_away_shots_on_diff'] = 0
        f['prev_away_attacks_diff'] = 0
        f['prev_away_possession_a'] = 50
        f['prev_away_possession_diff'] = 0
        f['prev_away_won'] = False
        f['prev_away_lost'] = False
        f['prev_away_goal_diff'] = 0
        f['prev_away_unlucky_loss'] = False
    
    # === FEATURES COMBINADAS ===
    # Diferencia de dominancia entre los dos equipos
    f['dominance_diff'] = f['prev_home_dangerous_attacks_diff'] - f['prev_away_dangerous_attacks_diff']
    f['shots_dominance_diff'] = f['prev_home_shots_diff'] - f['prev_away_shots_diff']
    
    # Momentum: Quién viene mejor
    f['home_momentum'] = f['prev_home_won'] and f['prev_home_dominated']
    f['away_momentum'] = f['prev_away_won'] and f['prev_away_dominated']
    
    # Vulnerabilidad: Quién viene peor
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
    print("🎯 MODELO 2: ENTRENAMIENTO CON ESTADÍSTICAS")
    print("📊 Features: Ataques Peligrosos, Tiros, Posesión, Dominancia")
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
        
        feats = extract_stats_features(m)
        if not feats: continue
        
        # Only include if has stats data
        if feats.get('prev_home_dangerous_attacks_diff') != 0 or feats.get('prev_away_dangerous_attacks_diff') != 0:
            dataset.append((feats, result))
            
            for k, v in feats.items():
                if k not in feature_values:
                    feature_values[k] = {'type': type(v), 'values': set()}
                if isinstance(v, (int, float)):
                    feature_values[k]['values'].add(v)
                elif isinstance(v, bool):
                    feature_values[k]['values'].add(v)
    
    print(f"   Partidos CON estadísticas: {len(dataset)}")
    print(f"   Features extraídas: {len(feature_values)}")
    
    if len(dataset) < 10:
        print("❌ No hay suficientes partidos con estadísticas para entrenar")
        return
    
    # Convert sets to lists
    for k in feature_values:
        feature_values[k]['values'] = sorted(list(feature_values[k]['values']))
    
    # Train
    print("\n🔄 Entrenando (target: 80% accuracy)...")
    rules = genetic_search(dataset, feature_values, target_acc=80.0, generations=150, min_samples=5)
    
    # Filter and save
    rules = [r for r in rules if r.stats['total'] >= 5]
    rules.sort(key=lambda r: (r.accuracy, r.stats['total']), reverse=True)
    
    print(f"\n🏆 REGLAS STATS ENCONTRADAS: {len(rules)}")
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
            'model': 'STATS',
            'target_line': 0.5
        })
    
    out_file = RESULTS_DIR / 'specialist_stats_model.json'
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(output_rules, f, indent=2)
    
    print(f"\n💾 Guardadas {len(output_rules)} reglas en {out_file}")
    
    # Quick validation
    if rules:
        hits = sum(1 for feats, res in dataset if any(r.matches(feats) and r.prediction == res for r in rules[:10]))
        total_with_pick = sum(1 for feats, _ in dataset if any(r.matches(feats) for r in rules[:10]))
        if total_with_pick > 0:
            print(f"\n📈 Validación (top 10): {hits}/{total_with_pick} = {hits/total_with_pick*100:.1f}%")

if __name__ == "__main__":
    main()
