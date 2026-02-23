# scripts/market_movement_trainer.py
"""
ENTRENAMIENTO CON MOVIMIENTOS DE MERCADO Y COBERTURAS REALES
============================================================
Usa los datos REALES de:
1. market_analysis_data.stadium.movement (ej: "0 → 0.25")
2. market_analysis_data.stadium.is_covered
3. handicap_line_raw de cada partido previo
4. Cálculo de si cubrió el handicap en cada fuente

Este es el entrenamiento MÁS PRECISO posible con tus datos.
"""

import json
import re
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import random
import copy

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
RESULTS_DIR = PROJECT_ROOT / 'backtest_results'
RESULTS_DIR.mkdir(exist_ok=True)

DATA_FILES = list(DATA_DIR.glob('data*.json'))

GENERATIONS = 1500
POPULATION_SIZE = 2000


def parse_score(score_str) -> Optional[Tuple[int, int]]:
    if not score_str or ':' not in str(score_str):
        return None
    try:
        parts = str(score_str).replace('-', ':').split(':')
        return int(parts[0]), int(parts[1])
    except:
        return None


def parse_movement(movement_str) -> Optional[Tuple[float, float]]:
    """Parsea movimiento como "0 → 0.25" a (0, 0.25)"""
    if not movement_str:
        return None
    try:
        parts = movement_str.replace('→', '->').split('->')
        if len(parts) != 2:
            return None
        return float(parts[0].strip()), float(parts[1].strip())
    except:
        return None


def get_ah_winner(home_goals, away_goals, ah_line) -> str:
    adjusted = (home_goals - away_goals) - ah_line
    if adjusted > 0.25:
        return 'LOCAL'
    elif adjusted < -0.25:
        return 'VISITA'
    return 'PUSH'


def get_ou_result(home_goals, away_goals, ou_line) -> str:
    total = home_goals + away_goals
    if total > ou_line + 0.25:
        return 'OVER'
    elif total < ou_line - 0.25:
        return 'UNDER'
    return 'PUSH'


def did_cover(home_goals, away_goals, ah_line, is_home: bool) -> str:
    """Determina si el equipo cubrió su handicap."""
    if is_home:
        adjusted = (home_goals - away_goals) - ah_line
    else:
        adjusted = (away_goals - home_goals) + ah_line
    
    if adjusted > 0.25:
        return 'COVER'
    elif adjusted < -0.25:
        return 'NO_COVER'
    return 'PUSH'


def extract_market_features(match: Dict) -> Dict:
    """
    Extrae features basadas en MOVIMIENTOS DE MERCADO y COBERTURAS REALES.
    """
    features = {}
    
    # Datos básicos
    main_odds = match.get('main_match_odds') or {}
    try:
        ah_line = float(main_odds.get('ah_linea', 0) or 0)
        ou_line = float(main_odds.get('goals_linea', 2.5) or 2.5)
    except:
        ah_line, ou_line = 0, 2.5
    
    features['ah_line'] = ah_line
    features['ou_line'] = ou_line
    features['ah_bucket'] = round(abs(ah_line) * 2) / 2
    features['fav_market'] = 'LOCAL' if ah_line > 0 else ('AWAY' if ah_line < 0 else 'NEUTRAL')
    
    # === MARKET ANALYSIS DATA ===
    market_data = match.get('market_analysis_data') or {}
    
    # Stadium H2H
    stadium = market_data.get('stadium') or {}
    features['h2h_stadium_covered'] = stadium.get('is_covered', None)
    features['h2h_stadium_movement'] = stadium.get('movement', '')
    
    movement = parse_movement(stadium.get('movement'))
    if movement:
        features['h2h_stadium_line_before'] = movement[0]
        features['h2h_stadium_line_after'] = movement[1]
        features['h2h_stadium_line_change'] = movement[1] - movement[0]
        features['h2h_stadium_line_increased'] = movement[1] > movement[0]
        features['h2h_stadium_line_decreased'] = movement[1] < movement[0]
        # Cambio de favorito
        features['h2h_stadium_fav_changed'] = (movement[0] > 0) != (movement[1] > 0)
    else:
        features['h2h_stadium_line_change'] = 0
        features['h2h_stadium_line_increased'] = False
        features['h2h_stadium_line_decreased'] = False
        features['h2h_stadium_fav_changed'] = False
    
    # General H2H
    general = market_data.get('general') or {}
    features['h2h_general_covered'] = general.get('is_covered', None)
    features['h2h_general_movement'] = general.get('movement', '')
    
    movement_gen = parse_movement(general.get('movement'))
    if movement_gen:
        features['h2h_general_line_before'] = movement_gen[0]
        features['h2h_general_line_after'] = movement_gen[1]
        features['h2h_general_line_change'] = movement_gen[1] - movement_gen[0]
        features['h2h_general_line_increased'] = movement_gen[1] > movement_gen[0]
        features['h2h_general_line_decreased'] = movement_gen[1] < movement_gen[0]
    else:
        features['h2h_general_line_change'] = 0
        features['h2h_general_line_increased'] = False
        features['h2h_general_line_decreased'] = False
    
    # === COBERTURAS EN PARTIDOS PREVIOS ===
    cover_results = []
    ou_results = []
    
    def process_prev_match(data, is_home_team: bool, name: str):
        if not data:
            return
        
        score = parse_score(data.get('score'))
        if not score:
            return
        
        try:
            prev_ah = float(data.get('handicap_line_raw', 0) or 0)
        except:
            prev_ah = 0
        
        home_goals, away_goals = score
        
        # ¿Cubrió el handicap?
        cover = did_cover(home_goals, away_goals, prev_ah, is_home_team)
        cover_results.append(cover)
        features[f'{name}_cover'] = cover
        features[f'{name}_ah'] = prev_ah
        
        # O/U
        total = home_goals + away_goals
        ou = 'OVER' if total > 2.5 else 'UNDER'
        ou_results.append(ou)
        features[f'{name}_ou'] = ou
        features[f'{name}_goals'] = total
    
    # Prev Home (el local jugando de local)
    process_prev_match(match.get('last_home_match'), True, 'prev_home')
    
    # Prev Away (el visitante jugando de visitante)
    process_prev_match(match.get('last_away_match'), False, 'prev_away')
    
    # H2H Col3
    h2h_col3 = match.get('h2h_col3') or {}
    if h2h_col3.get('status') == 'found':
        try:
            h_goals = int(h2h_col3.get('goles_home', 0))
            a_goals = int(h2h_col3.get('goles_away', 0))
            col3_ah = float(h2h_col3.get('handicap', 0) or 0)
            
            cover = did_cover(h_goals, a_goals, col3_ah, True)
            cover_results.append(cover)
            features['h2h_col3_cover'] = cover
            features['h2h_col3_goals'] = h_goals + a_goals
            features['h2h_col3_ou'] = 'OVER' if h_goals + a_goals > 2.5 else 'UNDER'
            ou_results.append(features['h2h_col3_ou'])
        except:
            pass
    
    # === ESTADÍSTICAS AGREGADAS ===
    covers = sum(1 for c in cover_results if c == 'COVER')
    no_covers = sum(1 for c in cover_results if c == 'NO_COVER')
    valid = len([c for c in cover_results if c in ['COVER', 'NO_COVER']])
    
    features['covers'] = covers
    features['no_covers'] = no_covers
    features['cover_sources'] = valid
    features['cover_ratio'] = covers / valid if valid > 0 else 0.5
    features['all_covered'] = covers == valid and valid >= 2
    features['none_covered'] = no_covers == valid and valid >= 2
    
    # Consistency H2H
    features['h2h_both_covered'] = (features.get('h2h_stadium_covered') == True and 
                                     features.get('h2h_general_covered') == True)
    features['h2h_both_not_covered'] = (features.get('h2h_stadium_covered') == False and 
                                         features.get('h2h_general_covered') == False)
    features['h2h_mixed'] = (features.get('h2h_stadium_covered') != features.get('h2h_general_covered') and
                             features.get('h2h_stadium_covered') is not None)
    
    # O/U
    overs = sum(1 for o in ou_results if o == 'OVER')
    unders = sum(1 for o in ou_results if o == 'UNDER')
    
    features['overs'] = overs
    features['unders'] = unders
    features['ou_sources'] = len(ou_results)
    features['over_ratio'] = overs / len(ou_results) if ou_results else 0.5
    
    # === PATRONES ANTI-MERCADO ===
    # Mercado aumentó línea pero H2H no cubrió
    features['market_up_h2h_not_cover'] = (features.get('h2h_stadium_line_increased', False) and 
                                            features.get('h2h_stadium_covered') == False)
    
    # Mercado disminuyó línea pero H2H cubrió
    features['market_down_h2h_cover'] = (features.get('h2h_stadium_line_decreased', False) and 
                                          features.get('h2h_stadium_covered') == True)
    
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
    def __init__(self, conditions, prediction, rule_type='AH'):
        self.conditions = conditions
        self.prediction = prediction
        self.rule_type = rule_type
        self.total = 0
        self.correct = 0
    
    def matches(self, features):
        for feat, op, val in self.conditions:
            fv = features.get(feat)
            if isinstance(val, bool):
                if fv != val:
                    return False
                continue
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
    
    def __repr__(self):
        conds = ' & '.join([f"{c[0]}{c[1]}{c[2]}" for c in self.conditions])
        return f"[{self.rule_type}] IF {conds} -> {self.prediction} ({self.accuracy():.1f}%, n={self.total})"


def generate_condition(feat):
    if feat.endswith('_covered') or feat.endswith('_cover'):
        if 'covered' in feat:
            return (feat, '==', random.choice([True, False]))
        return (feat, '==', random.choice(['COVER', 'NO_COVER']))
    elif feat.endswith('_ou'):
        return (feat, '==', random.choice(['OVER', 'UNDER']))
    elif feat in ['h2h_stadium_line_increased', 'h2h_stadium_line_decreased', 
                  'h2h_general_line_increased', 'h2h_general_line_decreased',
                  'h2h_stadium_fav_changed', 'h2h_both_covered', 'h2h_both_not_covered',
                  'h2h_mixed', 'all_covered', 'none_covered', 'has_ranks',
                  'market_up_h2h_not_cover', 'market_down_h2h_cover']:
        return (feat, '==', True)
    elif feat == 'fav_market':
        return (feat, '==', random.choice(['LOCAL', 'AWAY', 'NEUTRAL']))
    elif feat in ['covers', 'no_covers', 'overs', 'unders', 'cover_sources', 'ou_sources']:
        return (feat, random.choice(['>=', '<=', '==']), random.randint(1, 5))
    elif 'ratio' in feat:
        return (feat, random.choice(['>=', '<=']), round(random.uniform(0.2, 0.8), 2))
    elif 'line_change' in feat:
        return (feat, random.choice(['>', '<', '>=', '<=']), round(random.uniform(-0.5, 0.5), 2))
    elif feat == 'ah_bucket':
        return (feat, '==', random.choice([0, 0.5, 1, 1.5, 2]))
    elif feat in ['prev_home_ah', 'prev_away_ah', 'h2h_stadium_line_before', 'h2h_stadium_line_after']:
        return (feat, random.choice(['>', '<']), round(random.uniform(-1, 1), 2))
    elif feat == 'rank_diff':
        return (feat, random.choice(['>', '<']), random.choice([-5, -3, 0, 3, 5]))
    elif feat in ['prev_home_goals', 'prev_away_goals', 'h2h_col3_goals']:
        return (feat, random.choice(['>=', '<=']), random.randint(1, 4))
    return None


def generate_random_rule(features_list, prediction, rule_type):
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
    return Rule(conditions, prediction, rule_type) if len(conditions) >= 2 else None


def mutate(rule, features_list):
    new_conds = list(rule.conditions)
    action = random.choice(['add', 'remove', 'modify', 'replace'])
    
    if action == 'add' and len(new_conds) < 6:
        feat = random.choice(features_list)
        cond = generate_condition(feat)
        if cond and not any(c[0] == feat for c in new_conds):
            new_conds.append(cond)
    elif action == 'remove' and len(new_conds) > 2:
        new_conds.pop(random.randint(0, len(new_conds) - 1))
    elif action == 'modify' and new_conds:
        idx = random.randint(0, len(new_conds) - 1)
        feat, op, val = new_conds[idx]
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            val = val * random.uniform(0.7, 1.3)
            val = round(val, 2) if isinstance(val, float) else int(val)
            new_conds[idx] = (feat, op, val)
    elif action == 'replace' and new_conds:
        idx = random.randint(0, len(new_conds) - 1)
        feat = random.choice(features_list)
        cond = generate_condition(feat)
        if cond:
            new_conds[idx] = cond
    
    return Rule(new_conds, rule.prediction, rule.rule_type)


def evaluate_rules(matches, rules):
    for r in rules:
        r.total = 0
        r.correct = 0
    
    for match in matches:
        score = match.get('final_score') or match.get('score')
        parsed = parse_score(score)
        if not parsed:
            continue
        
        home_goals, away_goals = parsed
        main_odds = match.get('main_match_odds') or {}
        
        try:
            ah = float(main_odds.get('ah_linea', 0) or 0)
            ou = float(main_odds.get('goals_linea', 2.5) or 2.5)
        except:
            continue
        
        ah_result = get_ah_winner(home_goals, away_goals, ah)
        ou_result = get_ou_result(home_goals, away_goals, ou)
        
        features = extract_market_features(match)
        
        for rule in rules:
            if not rule.matches(features):
                continue
            
            rule.total += 1
            
            if rule.rule_type == 'AH' and ah_result != 'PUSH':
                if rule.prediction == ah_result:
                    rule.correct += 1
            elif rule.rule_type == 'OU' and ou_result != 'PUSH':
                if rule.prediction == ou_result:
                    rule.correct += 1
    
    return rules


def load_all_matches():
    all_matches = []
    for f in DATA_FILES:
        if not f.exists():
            continue
        try:
            with open(f, 'r', encoding='utf-8') as fp:
                data = json.load(fp)
                if isinstance(data, list):
                    all_matches.extend(data)
        except Exception as e:
            print(f"  Error en {f.name}: {e}")
    return [m for m in all_matches if parse_score(m.get('final_score') or m.get('score'))]


def main():
    print("=" * 70)
    print("🎯 ENTRENAMIENTO CON MOVIMIENTOS DE MERCADO")
    print("=" * 70)
    print(f"Generaciones: {GENERATIONS}")
    print(f"Población: {POPULATION_SIZE}")
    print()
    
    print("📂 Cargando partidos...")
    matches = load_all_matches()
    print(f"   Total partidos: {len(matches)}")
    
    # Verificar cuántos tienen market_analysis_data
    with_market = sum(1 for m in matches if m.get('market_analysis_data'))
    print(f"   Con market_analysis_data: {with_market}")
    
    features_list = [
        'ah_bucket', 'fav_market',
        'h2h_stadium_covered', 'h2h_general_covered',
        'h2h_stadium_line_change', 'h2h_general_line_change',
        'h2h_stadium_line_increased', 'h2h_stadium_line_decreased',
        'h2h_general_line_increased', 'h2h_general_line_decreased',
        'h2h_stadium_fav_changed', 'h2h_both_covered', 'h2h_both_not_covered', 'h2h_mixed',
        'prev_home_cover', 'prev_away_cover', 'h2h_col3_cover',
        'prev_home_ou', 'prev_away_ou', 'h2h_col3_ou',
        'prev_home_goals', 'prev_away_goals', 'h2h_col3_goals',
        'covers', 'no_covers', 'cover_sources', 'cover_ratio',
        'overs', 'unders', 'ou_sources', 'over_ratio',
        'all_covered', 'none_covered',
        'market_up_h2h_not_cover', 'market_down_h2h_cover',
        'rank_diff', 'has_ranks',
        'prev_home_ah', 'prev_away_ah'
    ]
    
    # Población inicial
    population = []
    for _ in range(POPULATION_SIZE // 4):
        for pred in ['LOCAL', 'VISITA']:
            r = generate_random_rule(features_list, pred, 'AH')
            if r:
                population.append(r)
        for pred in ['OVER', 'UNDER']:
            r = generate_random_rule(features_list, pred, 'OU')
            if r:
                population.append(r)
    
    print(f"   Población inicial: {len(population)}")
    print("\n" + "-" * 70)
    
    best_rules = {'AH': [], 'OU': []}
    
    for gen in range(GENERATIONS):
        evaluate_rules(matches, population)
        
        # Buscar buenas reglas
        for r in population:
            if r.total < 30:
                continue
            
            acc = r.accuracy()
            cat = 'AH' if r.rule_type == 'AH' else 'OU'
            
            if acc >= 65 and r.total >= 40:
                is_new = not any(
                    abs(existing.accuracy() - acc) < 0.5 and
                    existing.prediction == r.prediction
                    for existing in best_rules[cat]
                )
                if is_new and len(best_rules[cat]) < 50:
                    best_rules[cat].append(copy.deepcopy(r))
                    emoji = "🔥" if acc >= 75 else "✅" if acc >= 70 else "📊"
                    print(f"\n{emoji} Gen {gen+1} - {cat}: {acc:.1f}% (n={r.total})")
                    print(f"   {r}")
        
        if (gen + 1) % 200 == 0:
            print(f"\n📊 Gen {gen+1}/{GENERATIONS}")
            print(f"   AH: {len(best_rules['AH'])} reglas")
            print(f"   OU: {len(best_rules['OU'])} reglas")
        
        # Evolución
        valid = [r for r in population if r.total >= 20 and r.accuracy() >= 50]
        if not valid:
            valid = population[:100]
        
        valid.sort(key=lambda r: -r.accuracy())
        survivors = valid[:300]
        
        new_pop = list(survivors)
        while len(new_pop) < POPULATION_SIZE:
            parent = random.choice(survivors)
            new_pop.append(mutate(parent, features_list))
        
        for _ in range(100):
            for pred in ['LOCAL', 'VISITA']:
                r = generate_random_rule(features_list, pred, 'AH')
                if r:
                    new_pop.append(r)
            for pred in ['OVER', 'UNDER']:
                r = generate_random_rule(features_list, pred, 'OU')
                if r:
                    new_pop.append(r)
        
        population = new_pop
    
    # Resultados
    print("\n" + "=" * 70)
    print("📊 RESULTADOS FINALES")
    print("=" * 70)
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'total_matches': len(matches),
        'with_market_data': with_market,
        'generations': GENERATIONS,
        'rules': {'AH': [], 'OU': []}
    }
    
    for cat in ['AH', 'OU']:
        rules = best_rules[cat]
        rules.sort(key=lambda r: -r.accuracy())
        
        print(f"\n🏆 MEJORES REGLAS {cat}: {len(rules)}")
        for i, r in enumerate(rules[:10], 1):
            emoji = "🔥" if r.accuracy() >= 75 else "✅" if r.accuracy() >= 70 else "📊"
            print(f"   {i}. {emoji} {r}")
        
        for r in rules[:30]:
            results['rules'][cat].append({
                'conditions': [(c[0], c[1], c[2] if not isinstance(c[2], float) else round(c[2], 3)) 
                              for c in r.conditions],
                'prediction': r.prediction,
                'accuracy': round(r.accuracy(), 2),
                'samples': r.total
            })
    
    path = RESULTS_DIR / 'market_rules.json'
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 Guardado en: {path}")
    
    total = len(best_rules['AH']) + len(best_rules['OU'])
    print(f"\n✅ {total} reglas encontradas!")


if __name__ == '__main__':
    main()
