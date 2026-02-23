# scripts/auto_trainer.py
"""
SISTEMA DE AUTO-ENTRENAMIENTO CONTINUO
======================================
Ejecuta continuamente el algoritmo genético buscando reglas con:
- Precisión > 70%
- Mínimo 50 muestras

Guarda las mejores reglas y las aplica automáticamente.
"""

import json
import random
import copy
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Tuple, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
RESULTS_DIR = PROJECT_ROOT / 'backtest_results'
RESULTS_DIR.mkdir(exist_ok=True)

DATA_FILES = [
    DATA_DIR / 'data_ah_0.json',
    DATA_DIR / 'data_ah_0.5.json', 
    DATA_DIR / 'data_ah_1.5.json',
    DATA_DIR / 'data_ah_2_plus.json',
    DATA_DIR / 'data_minus_ah_0.5.json',
    DATA_DIR / 'data_minus_ah_1.5.json',
    DATA_DIR / 'data_minus_ah_2_plus.json',
]


def parse_score(score_str):
    if not score_str or ':' not in str(score_str):
        return None
    try:
        parts = str(score_str).replace('-', ':').split(':')
        return int(parts[0]), int(parts[1])
    except:
        return None


def get_ah_winner(home_goals, away_goals, ah_line):
    adjusted = (home_goals - away_goals) - ah_line
    if adjusted > 0.25:
        return 'LOCAL'
    elif adjusted < -0.25:
        return 'VISITA'
    return 'PUSH'


def get_ou_result(home_goals, away_goals, ou_line):
    total = home_goals + away_goals
    if total > ou_line + 0.25:
        return 'OVER'
    elif total < ou_line - 0.25:
        return 'UNDER'
    return 'PUSH'


def get_cover_result(home_goals, away_goals, ah_line, perspective='home'):
    if perspective == 'home':
        adjusted = (home_goals - away_goals) - ah_line
    else:
        adjusted = (away_goals - home_goals) + ah_line
    
    if adjusted > 0.25:
        return 'COVER'
    elif adjusted < -0.25:
        return 'NO_COVER'
    return 'PUSH'


def extract_features(match):
    features = {}
    
    main_odds = match.get('main_match_odds') or {}
    try:
        ah_line = float(main_odds.get('ah_linea', 0) or 0)
        ou_line = float(main_odds.get('goals_linea', 2.5) or 2.5)
    except:
        ah_line, ou_line = 0, 2.5
    
    features['ah_line'] = ah_line
    features['ou_line'] = ou_line
    features['ah_bucket'] = round(abs(ah_line) * 2) / 2  # 0, 0.5, 1, 1.5, 2...
    features['ha_fav'] = 'LOCAL' if ah_line > 0 else ('VISITA' if ah_line < 0 else 'NEUTRO')
    
    # Analizar cada fuente para coberturas
    cover_data = {}
    ou_data = {}
    
    sources = [
        ('prev_home', match.get('last_home_match'), 'home'),
        ('prev_away', match.get('last_away_match'), 'away'),
        ('h2h_stadium', match.get('h2h_stadium'), 'home'),
        ('h2h_general', match.get('h2h_general'), 'home'),
        ('h2h_col3', match.get('h2h_col3'), 'home'),
    ]
    
    comp = match.get('comparativas_indirectas') or {}
    if comp.get('left'):
        l = comp['left']
        sources.append(('ind_left', l, 'home' if l.get('localia') == 'H' else 'away'))
    if comp.get('right'):
        r = comp['right']
        sources.append(('ind_right', r, 'away' if r.get('localia') == 'A' else 'home'))
    
    for name, data, perspective in sources:
        if not data:
            continue
        
        # Obtener score
        score = None
        if data.get('goles_home') is not None:
            try:
                score = (int(data['goles_home']), int(data['goles_away']))
            except:
                pass
        elif data.get('score'):
            score = parse_score(data['score'])
        
        if not score:
            continue
        
        # Obtener handicap
        try:
            src_ah = float(data.get('handicap_line_raw') or data.get('handicap') or data.get('ah_line') or data.get('ah') or 0)
        except:
            src_ah = 0
        
        # Calcular cobertura
        cover = get_cover_result(score[0], score[1], src_ah, perspective)
        cover_data[name] = cover
        features[f'{name}_cover'] = cover
        
        # Calcular O/U
        total = score[0] + score[1]
        ou = 'OVER' if total > 2.5 else 'UNDER'
        ou_data[name] = ou
        features[f'{name}_ou'] = ou
    
    # Contadores
    covers = sum(1 for v in cover_data.values() if v == 'COVER')
    no_covers = sum(1 for v in cover_data.values() if v == 'NO_COVER')
    valid_covers = len([v for v in cover_data.values() if v in ['COVER', 'NO_COVER']])
    
    features['covers'] = covers
    features['no_covers'] = no_covers
    features['valid_sources'] = valid_covers
    features['cover_ratio'] = covers / valid_covers if valid_covers > 0 else 0.5
    
    overs = sum(1 for v in ou_data.values() if v == 'OVER')
    unders = sum(1 for v in ou_data.values() if v == 'UNDER')
    valid_ou = len(ou_data)
    
    features['overs'] = overs
    features['unders'] = unders
    features['valid_ou'] = valid_ou
    features['over_ratio'] = overs / valid_ou if valid_ou > 0 else 0.5
    
    # Rankings
    try:
        hr = int((match.get('home_standings') or {}).get('ranking', 0) or 0)
        ar = int((match.get('away_standings') or {}).get('ranking', 0) or 0)
        features['rank_diff'] = hr - ar
        features['has_ranks'] = bool(hr and ar)
    except:
        features['rank_diff'] = 0
        features['has_ranks'] = False
    
    return features


class Rule:
    def __init__(self, conditions, prediction):
        self.conditions = conditions  # List of (feat, op, val)
        self.prediction = prediction
        self.total = 0
        self.correct = 0
    
    def matches(self, features):
        for feat, op, val in self.conditions:
            fv = features.get(feat)
            if fv is None:
                return False
            try:
                if op == '==' and fv != val:
                    return False
                elif op == '>=' and fv < val:
                    return False
                elif op == '<=' and fv > val:
                    return False
                elif op == '>' and fv <= val:
                    return False
                elif op == '<' and fv >= val:
                    return False
            except:
                return False
        return True
    
    def accuracy(self):
        return self.correct / self.total * 100 if self.total > 0 else 0
    
    def score(self):
        # Balancear precisión y muestras
        if self.total < 30:
            return 0
        return self.accuracy() * min(1, self.total / 100)
    
    def __repr__(self):
        conds = ' & '.join([f"{c[0]}{c[1]}{c[2]}" for c in self.conditions])
        return f"IF {conds} -> {self.prediction} ({self.accuracy():.1f}%, n={self.total})"


def evaluate_rules(matches, rules):
    for r in rules:
        r.total = 0
        r.correct = 0
    
    for match in matches:
        score = match.get('final_score') or match.get('score')
        parsed = parse_score(score)
        if not parsed:
            continue
        
        home, away = parsed
        main_odds = match.get('main_match_odds') or {}
        try:
            ah = float(main_odds.get('ah_linea', 0) or 0)
            ou = float(main_odds.get('goals_linea', 2.5) or 2.5)
        except:
            continue
        
        ah_result = get_ah_winner(home, away, ah)
        ou_result = get_ou_result(home, away, ou)
        
        features = extract_features(match)
        
        for rule in rules:
            if not rule.matches(features):
                continue
            
            rule.total += 1
            
            if rule.prediction in ['LOCAL', 'VISITA'] and ah_result != 'PUSH':
                if rule.prediction == ah_result:
                    rule.correct += 1
            elif rule.prediction in ['OVER', 'UNDER'] and ou_result != 'PUSH':
                if rule.prediction == ou_result:
                    rule.correct += 1
    
    return rules


def mutate(rule, features_list):
    new_conds = list(rule.conditions)
    action = random.choice(['add', 'remove', 'modify', 'replace'])
    
    if action == 'add' and len(new_conds) < 6:
        feat = random.choice(features_list)
        cond = generate_condition(feat)
        if cond:
            new_conds.append(cond)
    elif action == 'remove' and len(new_conds) > 2:
        new_conds.pop(random.randint(0, len(new_conds) - 1))
    elif action == 'modify' and new_conds:
        idx = random.randint(0, len(new_conds) - 1)
        feat, op, val = new_conds[idx]
        if isinstance(val, (int, float)):
            val = val * random.uniform(0.7, 1.3)
            val = round(val, 2)
        new_conds[idx] = (feat, op, val)
    elif action == 'replace' and new_conds:
        idx = random.randint(0, len(new_conds) - 1)
        feat = random.choice(features_list)
        cond = generate_condition(feat)
        if cond:
            new_conds[idx] = cond
    
    return Rule(new_conds, rule.prediction)


def generate_condition(feat):
    if feat.endswith('_cover'):
        return (feat, '==', random.choice(['COVER', 'NO_COVER']))
    elif feat.endswith('_ou'):
        return (feat, '==', random.choice(['OVER', 'UNDER']))
    elif feat == 'ha_fav':
        return (feat, '==', random.choice(['LOCAL', 'VISITA', 'NEUTRO']))
    elif feat in ['covers', 'no_covers', 'overs', 'unders', 'valid_sources', 'valid_ou']:
        return (feat, random.choice(['>=', '<=']), random.randint(2, 6))
    elif feat in ['cover_ratio', 'over_ratio']:
        return (feat, random.choice(['>=', '<=']), round(random.uniform(0.2, 0.8), 2))
    elif feat == 'rank_diff':
        return (feat, random.choice(['>', '<']), random.choice([-5, -3, 3, 5]))
    elif feat == 'ah_bucket':
        return (feat, '==', random.choice([0, 0.5, 1, 1.5, 2]))
    return None


def generate_random_rule(features_list, prediction):
    n = random.randint(2, 5)
    conditions = []
    used = set()
    for _ in range(n):
        feat = random.choice(features_list)
        if feat in used:
            continue
        used.add(feat)
        cond = generate_condition(feat)
        if cond:
            conditions.append(cond)
    return Rule(conditions, prediction) if conditions else None


def load_matches():
    all_matches = []
    for f in DATA_FILES:
        if not f.exists():
            continue
        try:
            with open(f, 'r', encoding='utf-8') as fp:
                all_matches.extend(json.load(fp))
        except:
            continue
    return [m for m in all_matches if parse_score(m.get('final_score') or m.get('score'))]


def main():
    print("=" * 70)
    print("🚀 AUTO-TRAINER: Búsqueda continua de reglas ganadoras")
    print("=" * 70)
    print(f"Objetivo: Precisión > 70% con mínimo 50 muestras")
    print()
    
    matches = load_matches()
    print(f"Partidos cargados: {len(matches)}")
    
    features_list = [
        'prev_home_cover', 'prev_away_cover', 'h2h_stadium_cover',
        'h2h_general_cover', 'h2h_col3_cover', 'ind_left_cover', 'ind_right_cover',
        'covers', 'no_covers', 'valid_sources', 'cover_ratio',
        'prev_home_ou', 'prev_away_ou', 'h2h_stadium_ou',
        'h2h_general_ou', 'h2h_col3_ou', 'overs', 'unders', 'valid_ou', 'over_ratio',
        'ha_fav', 'rank_diff', 'ah_bucket'
    ]
    
    # Población inicial
    population = []
    for pred in ['LOCAL', 'VISITA', 'OVER', 'UNDER']:
        for _ in range(200):
            r = generate_random_rule(features_list, pred)
            if r:
                population.append(r)
    
    best_rules = {'AH': None, 'OU': None}
    best_scores = {'AH': 0, 'OU': 0}
    
    print(f"\nPoblación inicial: {len(population)} reglas")
    print("\n" + "-" * 70)
    
    for gen in range(500):  # 500 generaciones
        evaluate_rules(matches, population)
        
        # Filtrar válidas
        valid = [r for r in population if r.total >= 30]
        
        if not valid:
            # Regenerar población
            population = []
            for pred in ['LOCAL', 'VISITA', 'OVER', 'UNDER']:
                for _ in range(200):
                    r = generate_random_rule(features_list, pred)
                    if r:
                        population.append(r)
            continue
        
        # Ordenar
        valid.sort(key=lambda r: -r.score())
        
        # Verificar mejores
        for r in valid[:10]:
            if r.prediction in ['LOCAL', 'VISITA']:
                cat = 'AH'
            else:
                cat = 'OU'
            
            if r.accuracy() >= 65 and r.total >= 50:
                if r.accuracy() > best_scores[cat] or \
                   (r.accuracy() == best_scores[cat] and r.total > (best_rules[cat].total if best_rules[cat] else 0)):
                    best_scores[cat] = r.accuracy()
                    best_rules[cat] = copy.deepcopy(r)
                    print(f"\n🎯 Gen {gen+1} - NUEVA MEJOR {cat}:")
                    print(f"   {r}")
        
        # Selección
        survivors = valid[:50]
        
        # Nueva generación
        new_pop = list(survivors)
        
        # Mutación
        while len(new_pop) < 800:
            parent = random.choice(survivors)
            child = mutate(parent, features_list)
            new_pop.append(child)
        
        # Añadir aleatorios
        for pred in ['LOCAL', 'VISITA', 'OVER', 'UNDER']:
            for _ in range(20):
                r = generate_random_rule(features_list, pred)
                if r:
                    new_pop.append(r)
        
        population = new_pop
        
        if (gen + 1) % 50 == 0:
            print(f"\n📊 Generación {gen+1} - Estado:")
            if best_rules['AH']:
                print(f"   Mejor AH: {best_rules['AH']}")
            if best_rules['OU']:
                print(f"   Mejor OU: {best_rules['OU']}")
    
    # Guardar resultados
    print("\n" + "=" * 70)
    print("📊 RESULTADOS FINALES")
    print("=" * 70)
    
    results = {'timestamp': datetime.now().isoformat(), 'rules': []}
    
    if best_rules['AH']:
        print(f"\n🏆 MEJOR REGLA AH:")
        print(f"   {best_rules['AH']}")
        results['rules'].append({
            'type': 'AH',
            'conditions': [(c[0], c[1], c[2] if not isinstance(c[2], float) else round(c[2], 3)) for c in best_rules['AH'].conditions],
            'prediction': best_rules['AH'].prediction,
            'accuracy': best_rules['AH'].accuracy(),
            'samples': best_rules['AH'].total
        })
    
    if best_rules['OU']:
        print(f"\n🏆 MEJOR REGLA O/U:")
        print(f"   {best_rules['OU']}")
        results['rules'].append({
            'type': 'OU',
            'conditions': [(c[0], c[1], c[2] if not isinstance(c[2], float) else round(c[2], 3)) for c in best_rules['OU'].conditions],
            'prediction': best_rules['OU'].prediction,
            'accuracy': best_rules['OU'].accuracy(),
            'samples': best_rules['OU'].total
        })
    
    path = RESULTS_DIR / 'best_rules.json'
    with open(path, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\n💾 Guardado en: {path}")


if __name__ == '__main__':
    main()
