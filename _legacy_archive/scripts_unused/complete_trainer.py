# scripts/complete_trainer.py
"""
ENTRENAMIENTO COMPLETO CON TODAS LAS FEATURES
==============================================
Incluye:
1. Ataques peligrosos y tiros a puerta (stats_rows)
2. Comparativas indirectas (left/right)
3. H2H Col3
4. Movimientos de mercado (market_analysis_data)
5. Coberturas de handicap (is_covered, handicap_line_raw)
6. Rankings

Genera reglas para:
- AH: LOCAL / VISITA
- O/U: OVER / UNDER
"""

import json
import random
import copy
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import re

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
RESULTS_DIR = PROJECT_ROOT / 'backtest_results'
RESULTS_DIR.mkdir(exist_ok=True)

DATA_FILES = list(DATA_DIR.glob('data*.json'))

GENERATIONS = 2000
POPULATION_SIZE = 3000
MIN_SAMPLES = 35
MIN_ACCURACY = 63

STATS_KEYS = ['Tiros', 'Tiros a Puerta', 'Ataques', 'Ataques Peligrosos']


def parse_score(score_str) -> Optional[Tuple[int, int]]:
    if not score_str or ':' not in str(score_str):
        return None
    try:
        parts = str(score_str).replace('-', ':').split(':')
        return int(parts[0]), int(parts[1])
    except:
        return None


def parse_stats_rows(stats_rows: list) -> Dict:
    result = {}
    if not stats_rows or not isinstance(stats_rows, list):
        return result
    for r in stats_rows:
        label = (r.get('label') or '').strip()
        try:
            result[label] = {
                'home': float(r.get('home', 0) or 0),
                'away': float(r.get('away', 0) or 0)
            }
        except:
            continue
    return result


def calculate_edge(stats_dict: Dict, home_perspective: bool = True) -> Dict:
    edges = {}
    for stat in STATS_KEYS:
        if stat not in stats_dict:
            continue
        if home_perspective:
            edges[stat] = stats_dict[stat]['home'] - stats_dict[stat]['away']
        else:
            edges[stat] = stats_dict[stat]['away'] - stats_dict[stat]['home']
    return edges


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
    if is_home:
        adjusted = (home_goals - away_goals) - ah_line
    else:
        adjusted = (away_goals - home_goals) + ah_line
    if adjusted > 0.25:
        return 'COVER'
    elif adjusted < -0.25:
        return 'NO_COVER'
    return 'PUSH'


def extract_all_features(match: Dict) -> Dict:
    """Extrae TODAS las features posibles."""
    f = {}
    
    # === DATOS BÁSICOS ===
    main_odds = match.get('main_match_odds') or {}
    try:
        ah_line = float(main_odds.get('ah_linea', 0) or 0)
        ou_line = float(main_odds.get('goals_linea', 2.5) or 2.5)
    except:
        ah_line, ou_line = 0, 2.5
    
    f['ah_line'] = ah_line
    f['ou_line'] = ou_line
    f['ah_bucket'] = round(abs(ah_line) * 2) / 2
    f['ou_bucket'] = round(ou_line * 2) / 2
    f['fav_market'] = 'LOCAL' if ah_line > 0 else ('AWAY' if ah_line < 0 else 'NEUTRAL')
    f['local_fav'] = ah_line > 0
    f['away_fav'] = ah_line < 0
    
    # === MARKET ANALYSIS DATA (Movimientos de mercado) ===
    market = match.get('market_analysis_data') or {}
    stadium_m = market.get('stadium') or {}
    general_m = market.get('general') or {}
    
    f['h2h_stadium_covered'] = stadium_m.get('is_covered')
    f['h2h_general_covered'] = general_m.get('is_covered')
    
    # Parsear movimiento
    if stadium_m.get('movement'):
        parts = stadium_m['movement'].replace('→', '->').split('->')
        if len(parts) == 2:
            try:
                before = float(parts[0].strip())
                after = float(parts[1].strip())
                f['stadium_line_change'] = after - before
                f['stadium_line_increased'] = after > before
                f['stadium_line_decreased'] = after < before
                f['stadium_fav_changed'] = (before > 0) != (after > 0)
            except:
                f['stadium_line_change'] = 0
    
    if general_m.get('movement'):
        parts = general_m['movement'].replace('→', '->').split('->')
        if len(parts) == 2:
            try:
                before = float(parts[0].strip())
                after = float(parts[1].strip())
                f['general_line_change'] = after - before
                f['general_line_increased'] = after > before
                f['general_line_decreased'] = after < before
            except:
                f['general_line_change'] = 0
    
    # === ESTADÍSTICAS DE ATAQUES PELIGROSOS ===
    all_edges = {'danger': [], 'sot': [], 'shots': [], 'attacks': []}
    cover_results = []
    ou_results = []
    goals_list = []
    
    def process_source(name, data, is_home: bool):
        if not data:
            return
        
        # Stats
        stats = parse_stats_rows(data.get('stats_rows', []))
        if stats:
            edges = calculate_edge(stats, is_home)
            if 'Ataques Peligrosos' in edges:
                all_edges['danger'].append(edges['Ataques Peligrosos'])
                f[f'{name}_edge_danger'] = edges['Ataques Peligrosos']
            if 'Tiros a Puerta' in edges:
                all_edges['sot'].append(edges['Tiros a Puerta'])
                f[f'{name}_edge_sot'] = edges['Tiros a Puerta']
            if 'Tiros' in edges:
                all_edges['shots'].append(edges['Tiros'])
            if 'Ataques' in edges:
                all_edges['attacks'].append(edges['Ataques'])
        
        # Score y cobertura
        score = parse_score(data.get('score'))
        if not score and data.get('goles_home') is not None:
            try:
                score = (int(data['goles_home']), int(data['goles_away']))
            except:
                pass
        
        if score:
            try:
                src_ah = float(data.get('handicap_line_raw') or data.get('handicap') or 
                              data.get('ah_line') or data.get('ah') or 0)
            except:
                src_ah = 0
            
            cover = did_cover(score[0], score[1], src_ah, is_home)
            cover_results.append(cover)
            f[f'{name}_cover'] = cover
            
            total = score[0] + score[1]
            ou = 'OVER' if total > 2.5 else 'UNDER'
            ou_results.append(ou)
            f[f'{name}_ou'] = ou
            f[f'{name}_goals'] = total
            goals_list.append(total)
    
    # Procesar todas las fuentes
    process_source('prev_home', match.get('last_home_match'), True)
    process_source('prev_away', match.get('last_away_match'), False)
    process_source('h2h_stadium', match.get('h2h_stadium'), True)
    process_source('h2h_general', match.get('h2h_general'), True)
    
    if match.get('h2h_col3') and match['h2h_col3'].get('status') == 'found':
        process_source('h2h_col3', match['h2h_col3'], True)
    
    # Comparativas indirectas
    comp = match.get('comparativas_indirectas') or {}
    if comp.get('left'):
        left_is_home = comp['left'].get('localia') == 'H'
        process_source('ind_left', comp['left'], left_is_home)
    if comp.get('right'):
        right_is_home = comp['right'].get('localia') != 'A'
        process_source('ind_right', comp['right'], right_is_home)
    
    # === EDGES AGREGADOS ===
    if all_edges['danger']:
        f['avg_edge_danger'] = sum(all_edges['danger']) / len(all_edges['danger'])
        f['edge_danger_positive'] = f['avg_edge_danger'] > 0
        f['edge_danger_strong_local'] = f['avg_edge_danger'] >= 5
        f['edge_danger_strong_away'] = f['avg_edge_danger'] <= -5
    else:
        f['avg_edge_danger'] = 0
    
    if all_edges['sot']:
        f['avg_edge_sot'] = sum(all_edges['sot']) / len(all_edges['sot'])
        f['edge_sot_positive'] = f['avg_edge_sot'] > 0
    else:
        f['avg_edge_sot'] = 0
    
    # === COBERTURAS ===
    covers = sum(1 for c in cover_results if c == 'COVER')
    no_covers = sum(1 for c in cover_results if c == 'NO_COVER')
    valid = len([c for c in cover_results if c in ['COVER', 'NO_COVER']])
    
    f['covers'] = covers
    f['no_covers'] = no_covers
    f['cover_sources'] = valid
    f['cover_ratio'] = covers / valid if valid > 0 else 0.5
    f['all_covered'] = covers == valid and valid >= 2
    f['none_covered'] = no_covers == valid and valid >= 2
    
    # H2H consistency
    f['h2h_both_covered'] = f.get('h2h_stadium_covered') == True and f.get('h2h_general_covered') == True
    f['h2h_both_not_covered'] = f.get('h2h_stadium_covered') == False and f.get('h2h_general_covered') == False
    f['h2h_mixed'] = f.get('h2h_stadium_covered') != f.get('h2h_general_covered') and f.get('h2h_stadium_covered') is not None
    
    # === O/U ===
    overs = sum(1 for o in ou_results if o == 'OVER')
    unders = sum(1 for o in ou_results if o == 'UNDER')
    
    f['overs'] = overs
    f['unders'] = unders
    f['ou_sources'] = len(ou_results)
    f['over_ratio'] = overs / len(ou_results) if ou_results else 0.5
    f['all_overs'] = overs == len(ou_results) and len(ou_results) >= 3
    f['all_unders'] = unders == len(ou_results) and len(ou_results) >= 3
    
    # Goles promedio
    if goals_list:
        f['avg_goals'] = sum(goals_list) / len(goals_list)
        f['high_scoring'] = f['avg_goals'] >= 3
        f['low_scoring'] = f['avg_goals'] <= 2
    else:
        f['avg_goals'] = 2.5
    
    # === RANKINGS ===
    try:
        hr = int((match.get('home_standings') or {}).get('ranking', 0) or 0)
        ar = int((match.get('away_standings') or {}).get('ranking', 0) or 0)
        f['rank_diff'] = hr - ar
        f['has_ranks'] = bool(hr and ar)
        f['home_top5'] = 0 < hr <= 5
        f['away_top5'] = 0 < ar <= 5
        f['home_bottom5'] = hr >= 15
        f['away_bottom5'] = ar >= 15
    except:
        f['rank_diff'] = 0
        f['has_ranks'] = False
    
    # === CONFLICTOS MERCADO VS STATS ===
    f['conflict_away_fav_local_stats'] = f.get('away_fav', False) and f.get('avg_edge_danger', 0) > 0 and f.get('avg_edge_sot', 0) > 0
    f['conflict_local_fav_away_stats'] = f.get('local_fav', False) and f.get('avg_edge_danger', 0) < 0 and f.get('avg_edge_sot', 0) < 0
    
    return f


# Lista de todas las features para generar condiciones
FEATURES_LIST = [
    'ah_bucket', 'ou_bucket', 'fav_market', 'local_fav', 'away_fav',
    'h2h_stadium_covered', 'h2h_general_covered', 'h2h_both_covered', 'h2h_both_not_covered', 'h2h_mixed',
    'stadium_line_change', 'stadium_line_increased', 'stadium_line_decreased', 'stadium_fav_changed',
    'general_line_change', 'general_line_increased', 'general_line_decreased',
    'prev_home_cover', 'prev_away_cover', 'h2h_col3_cover', 'ind_left_cover', 'ind_right_cover',
    'prev_home_ou', 'prev_away_ou', 'h2h_col3_ou', 'ind_left_ou', 'ind_right_ou',
    'prev_home_goals', 'prev_away_goals', 'h2h_col3_goals', 'ind_left_goals', 'ind_right_goals',
    'prev_home_edge_danger', 'prev_away_edge_danger', 'ind_left_edge_danger', 'ind_right_edge_danger',
    'prev_home_edge_sot', 'prev_away_edge_sot', 'ind_left_edge_sot', 'ind_right_edge_sot',
    'avg_edge_danger', 'avg_edge_sot', 'edge_danger_positive', 'edge_sot_positive',
    'edge_danger_strong_local', 'edge_danger_strong_away',
    'covers', 'no_covers', 'cover_sources', 'cover_ratio', 'all_covered', 'none_covered',
    'overs', 'unders', 'ou_sources', 'over_ratio', 'all_overs', 'all_unders',
    'avg_goals', 'high_scoring', 'low_scoring',
    'rank_diff', 'has_ranks', 'home_top5', 'away_top5', 'home_bottom5', 'away_bottom5',
    'conflict_away_fav_local_stats', 'conflict_local_fav_away_stats'
]


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
    if feat.endswith('_covered'):
        return (feat, '==', random.choice([True, False]))
    elif feat.endswith('_cover'):
        return (feat, '==', random.choice(['COVER', 'NO_COVER']))
    elif feat.endswith('_ou'):
        return (feat, '==', random.choice(['OVER', 'UNDER']))
    elif feat in ['local_fav', 'away_fav', 'h2h_both_covered', 'h2h_both_not_covered', 'h2h_mixed',
                  'stadium_line_increased', 'stadium_line_decreased', 'stadium_fav_changed',
                  'general_line_increased', 'general_line_decreased',
                  'all_covered', 'none_covered', 'all_overs', 'all_unders',
                  'high_scoring', 'low_scoring', 'has_ranks', 'home_top5', 'away_top5',
                  'edge_danger_positive', 'edge_sot_positive',
                  'edge_danger_strong_local', 'edge_danger_strong_away',
                  'conflict_away_fav_local_stats', 'conflict_local_fav_away_stats']:
        return (feat, '==', True)
    elif feat == 'fav_market':
        return (feat, '==', random.choice(['LOCAL', 'AWAY']))
    elif feat in ['covers', 'no_covers', 'overs', 'unders', 'cover_sources', 'ou_sources']:
        return (feat, random.choice(['>=', '<=', '==']), random.randint(1, 6))
    elif 'ratio' in feat:
        return (feat, random.choice(['>=', '<=']), round(random.uniform(0.2, 0.8), 2))
    elif 'line_change' in feat:
        return (feat, random.choice(['>', '<']), round(random.uniform(-0.5, 0.5), 2))
    elif 'edge' in feat:
        return (feat, random.choice(['>=', '<=', '>', '<']), round(random.uniform(-10, 10), 1))
    elif feat == 'ah_bucket':
        return (feat, '==', random.choice([0, 0.5, 1, 1.5, 2]))
    elif feat == 'ou_bucket':
        return (feat, '==', random.choice([2, 2.5, 3]))
    elif feat == 'avg_goals':
        return (feat, random.choice(['>=', '<=']), round(random.uniform(1.5, 4), 1))
    elif feat == 'rank_diff':
        return (feat, random.choice(['>', '<', '>=', '<=']), random.choice([-8, -5, -3, 0, 3, 5, 8]))
    elif feat.endswith('_goals'):
        return (feat, random.choice(['>=', '<=']), random.randint(1, 5))
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
        
        features = extract_all_features(match)
        
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
    print("🚀 ENTRENAMIENTO COMPLETO - TODAS LAS FEATURES")
    print("=" * 70)
    print(f"Generaciones: {GENERATIONS}")
    print(f"Población: {POPULATION_SIZE}")
    print(f"Min precisión: {MIN_ACCURACY}%")
    print(f"Min muestras: {MIN_SAMPLES}")
    print()
    print("Features incluidas:")
    print("  - Ataques peligrosos, tiros a puerta (edge)")
    print("  - Comparativas indirectas (left/right)")
    print("  - H2H Col3")
    print("  - Movimientos de mercado histórico")
    print("  - Coberturas de handicap")
    print("  - Over/Under")
    print()
    
    print("📂 Cargando partidos...")
    matches = load_all_matches()
    print(f"   Total partidos: {len(matches)}")
    
    # Poblar
    population = []
    for _ in range(POPULATION_SIZE // 4):
        for pred in ['LOCAL', 'VISITA']:
            r = generate_random_rule(FEATURES_LIST, pred, 'AH')
            if r:
                population.append(r)
        for pred in ['OVER', 'UNDER']:
            r = generate_random_rule(FEATURES_LIST, pred, 'OU')
            if r:
                population.append(r)
    
    print(f"   Población inicial: {len(population)}")
    print("\n" + "-" * 70)
    
    best_rules = {'AH': [], 'OU': []}
    
    for gen in range(GENERATIONS):
        evaluate_rules(matches, population)
        
        # Buscar buenas reglas
        for r in population:
            if r.total < MIN_SAMPLES:
                continue
            
            acc = r.accuracy()
            cat = 'AH' if r.rule_type == 'AH' else 'OU'
            
            if acc >= MIN_ACCURACY:
                is_new = not any(
                    abs(existing.accuracy() - acc) < 0.5 and
                    existing.prediction == r.prediction and
                    existing.rule_type == r.rule_type
                    for existing in best_rules[cat]
                )
                if is_new and len(best_rules[cat]) < 100:
                    best_rules[cat].append(copy.deepcopy(r))
                    emoji = "🔥" if acc >= 75 else "✅" if acc >= 70 else "📊"
                    print(f"\n{emoji} Gen {gen+1} - {cat} ({r.prediction}): {acc:.1f}% (n={r.total})")
                    # Mostrar condiciones en español
                    for c in r.conditions:
                        print(f"   {c[0]} {c[1]} {c[2]}")
        
        if (gen + 1) % 200 == 0:
            print(f"\n📊 Gen {gen+1}/{GENERATIONS}")
            print(f"   AH: {len(best_rules['AH'])} reglas")
            print(f"   OU: {len(best_rules['OU'])} reglas")
            if best_rules['AH']:
                top_ah = max(best_rules['AH'], key=lambda r: r.accuracy())
                print(f"   Top AH: {top_ah.accuracy():.1f}%")
            if best_rules['OU']:
                top_ou = max(best_rules['OU'], key=lambda r: r.accuracy())
                print(f"   Top OU: {top_ou.accuracy():.1f}%")
        
        # Evolución
        valid = [r for r in population if r.total >= 20 and r.accuracy() >= 50]
        if not valid:
            valid = population[:100]
        
        valid.sort(key=lambda r: -r.accuracy())
        survivors = valid[:400]
        
        new_pop = list(survivors)
        while len(new_pop) < POPULATION_SIZE:
            parent = random.choice(survivors)
            new_pop.append(mutate(parent, FEATURES_LIST))
        
        for _ in range(150):
            for pred in ['LOCAL', 'VISITA']:
                r = generate_random_rule(FEATURES_LIST, pred, 'AH')
                if r:
                    new_pop.append(r)
            for pred in ['OVER', 'UNDER']:
                r = generate_random_rule(FEATURES_LIST, pred, 'OU')
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
        'generations': GENERATIONS,
        'min_accuracy': MIN_ACCURACY,
        'min_samples': MIN_SAMPLES,
        'rules': {'AH': [], 'OU': []}
    }
    
    for cat in ['AH', 'OU']:
        rules = best_rules[cat]
        rules.sort(key=lambda r: -r.accuracy())
        
        print(f"\n🏆 MEJORES REGLAS {cat}: {len(rules)}")
        for i, r in enumerate(rules[:15], 1):
            emoji = "🔥" if r.accuracy() >= 75 else "✅" if r.accuracy() >= 70 else "📊"
            print(f"   {i}. {emoji} {r.prediction}: {r.accuracy():.1f}% (n={r.total})")
        
        for r in rules[:50]:
            results['rules'][cat].append({
                'conditions': [(c[0], c[1], c[2] if not isinstance(c[2], float) else round(c[2], 3)) 
                              for c in r.conditions],
                'prediction': r.prediction,
                'accuracy': round(r.accuracy(), 2),
                'samples': r.total
            })
    
    path = RESULTS_DIR / 'complete_rules.json'
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 Guardado en: {path}")
    
    total = len(best_rules['AH']) + len(best_rules['OU'])
    print(f"\n✅ {total} reglas totales!")
    print(f"   AH: {len(best_rules['AH'])}")
    print(f"   OU: {len(best_rules['OU'])}")


if __name__ == '__main__':
    main()
