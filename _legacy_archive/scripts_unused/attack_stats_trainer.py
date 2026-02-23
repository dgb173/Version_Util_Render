# scripts/attack_stats_trainer.py
"""
ENTRENAMIENTO BASADO EN ATAQUES PELIGROSOS Y COMPARATIVAS DIRECTAS
===================================================================
Enfocado en:
1. Ataques peligrosos (edge entre equipos)
2. Tiros a puerta (edge entre equipos)  
3. Comparativas DIRECTAS: ¿quién domina las stats?
4. Combinación con coberturas y movimientos de mercado

La idea es: si un equipo DOMINA en ataques peligrosos en múltiples
partidos históricos vs su rival (H2H, previos, indirectas), hay
una señal predictiva - INDEPENDIENTE del resultado final.
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

DATA_FILES = list(DATA_DIR.glob('data*.json'))

GENERATIONS = 2500
POPULATION_SIZE = 4000
MIN_SAMPLES = 30
MIN_ACCURACY = 65

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


def extract_attack_features(match: Dict) -> Dict:
    """
    Extrae SOLO features basadas en ataques peligrosos y comparativas directas.
    El edge indica QUIÉN DOMINA las stats (positivo = Local, negativo = Visita)
    """
    f = {}
    
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
    f['local_fav'] = ah_line > 0
    f['away_fav'] = ah_line < 0
    
    # EDGES de stats por fuente (positivo = local domina, negativo = visita domina)
    all_danger_edges = []
    all_sot_edges = []
    all_shots_edges = []
    all_attacks_edges = []
    
    def process_source(name: str, data: dict, home_perspective: bool):
        if not data:
            return
        
        stats = parse_stats_rows(data.get('stats_rows', []))
        if not stats:
            return
        
        # Calcular edges (diferencia) para cada stat
        for stat_key, edge_list in [
            ('Ataques Peligrosos', all_danger_edges),
            ('Tiros a Puerta', all_sot_edges),
            ('Tiros', all_shots_edges),
            ('Ataques', all_attacks_edges)
        ]:
            if stat_key in stats:
                home_val = stats[stat_key]['home']
                away_val = stats[stat_key]['away']
                
                if home_perspective:
                    edge = home_val - away_val
                else:
                    edge = away_val - home_val  # Invertir para que sea perspectiva del local actual
                
                edge_list.append(edge)
                f[f'{name}_edge_{stat_key.lower().replace(" ", "_")}'] = edge
                
                # Binarios útiles
                f[f'{name}_domina_{stat_key.lower().replace(" ", "_")}'] = edge > 0
    
    # Procesar cada fuente
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
    
    # === AGREGADOS DE EDGES ===
    if all_danger_edges:
        f['avg_edge_danger'] = sum(all_danger_edges) / len(all_danger_edges)
        f['sum_edge_danger'] = sum(all_danger_edges)
        f['sources_danger'] = len(all_danger_edges)
        f['local_domina_danger'] = f['avg_edge_danger'] > 0
        f['local_domina_danger_fuerte'] = f['avg_edge_danger'] >= 5
        f['visita_domina_danger_fuerte'] = f['avg_edge_danger'] <= -5
        f['danger_unanime_local'] = all(e > 0 for e in all_danger_edges) and len(all_danger_edges) >= 3
        f['danger_unanime_visita'] = all(e < 0 for e in all_danger_edges) and len(all_danger_edges) >= 3
        f['danger_mayoria_local'] = sum(1 for e in all_danger_edges if e > 0) >= len(all_danger_edges) * 0.7
        f['danger_mayoria_visita'] = sum(1 for e in all_danger_edges if e < 0) >= len(all_danger_edges) * 0.7
    else:
        f['avg_edge_danger'] = 0
        f['sources_danger'] = 0
    
    if all_sot_edges:
        f['avg_edge_sot'] = sum(all_sot_edges) / len(all_sot_edges)
        f['sum_edge_sot'] = sum(all_sot_edges)
        f['sources_sot'] = len(all_sot_edges)
        f['local_domina_sot'] = f['avg_edge_sot'] > 0
        f['local_domina_sot_fuerte'] = f['avg_edge_sot'] >= 3
        f['visita_domina_sot_fuerte'] = f['avg_edge_sot'] <= -3
        f['sot_unanime_local'] = all(e > 0 for e in all_sot_edges) and len(all_sot_edges) >= 3
        f['sot_unanime_visita'] = all(e < 0 for e in all_sot_edges) and len(all_sot_edges) >= 3
    else:
        f['avg_edge_sot'] = 0
        f['sources_sot'] = 0
    
    if all_shots_edges:
        f['avg_edge_shots'] = sum(all_shots_edges) / len(all_shots_edges)
        f['local_domina_shots'] = f['avg_edge_shots'] > 0
    
    if all_attacks_edges:
        f['avg_edge_attacks'] = sum(all_attacks_edges) / len(all_attacks_edges)
        f['local_domina_attacks'] = f['avg_edge_attacks'] > 0
    
    # === COMBINACIONES POTENTES ===
    f['local_domina_todo'] = (
        f.get('local_domina_danger', False) and 
        f.get('local_domina_sot', False) and
        f.get('local_domina_shots', False)
    )
    f['visita_domina_todo'] = (
        not f.get('local_domina_danger', True) and 
        not f.get('local_domina_sot', True) and
        not f.get('local_domina_shots', True)
    )
    
    # Anti-intuición: mercado dice uno, stats dicen otro
    f['anti_intuicion_local'] = f.get('away_fav', False) and f.get('local_domina_danger_fuerte', False)
    f['anti_intuicion_visita'] = f.get('local_fav', False) and f.get('visita_domina_danger_fuerte', False)
    
    # Rankings
    try:
        hr = int((match.get('home_standings') or {}).get('ranking', 0) or 0)
        ar = int((match.get('away_standings') or {}).get('ranking', 0) or 0)
        f['rank_diff'] = hr - ar
        f['has_ranks'] = bool(hr and ar)
    except:
        f['rank_diff'] = 0
        f['has_ranks'] = False
    
    # Market analysis
    market = match.get('market_analysis_data') or {}
    f['h2h_both_covered'] = market.get('stadium', {}).get('is_covered') == True and market.get('general', {}).get('is_covered') == True
    f['h2h_both_not_covered'] = market.get('stadium', {}).get('is_covered') == False and market.get('general', {}).get('is_covered') == False
    
    return f


# Features importantes para este entrenamiento
ATTACK_FEATURES = [
    # Edges promedios
    'avg_edge_danger', 'avg_edge_sot', 'avg_edge_shots', 'avg_edge_attacks',
    'sum_edge_danger', 'sum_edge_sot',
    'sources_danger', 'sources_sot',
    
    # Dominio binario
    'local_domina_danger', 'local_domina_sot', 'local_domina_shots', 'local_domina_attacks',
    'local_domina_danger_fuerte', 'visita_domina_danger_fuerte',
    'local_domina_sot_fuerte', 'visita_domina_sot_fuerte',
    
    # Unanimidad
    'danger_unanime_local', 'danger_unanime_visita',
    'danger_mayoria_local', 'danger_mayoria_visita',
    'sot_unanime_local', 'sot_unanime_visita',
    
    # Combinaciones
    'local_domina_todo', 'visita_domina_todo',
    'anti_intuicion_local', 'anti_intuicion_visita',
    
    # Por fuente individual
    'prev_home_domina_ataques_peligrosos', 'prev_away_domina_ataques_peligrosos',
    'h2h_stadium_domina_ataques_peligrosos', 'h2h_general_domina_ataques_peligrosos',
    'ind_left_domina_ataques_peligrosos', 'ind_right_domina_ataques_peligrosos',
    
    # Edges específicos
    'prev_home_edge_ataques_peligrosos', 'prev_away_edge_ataques_peligrosos',
    'h2h_stadium_edge_ataques_peligrosos', 'ind_left_edge_ataques_peligrosos',
    
    # Otros
    'ah_bucket', 'ou_bucket', 'local_fav', 'away_fav',
    'rank_diff', 'has_ranks',
    'h2h_both_covered', 'h2h_both_not_covered'
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
            if fv is None:
                return False
            try:
                if isinstance(val, bool):
                    if bool(fv) != val:
                        return False
                elif op == '==' and fv != val:
                    return False
                elif op == '>=' and float(fv) < float(val):
                    return False
                elif op == '<=' and float(fv) > float(val):
                    return False
                elif op == '>' and float(fv) <= float(val):
                    return False
                elif op == '<' and float(fv) >= float(val):
                    return False
            except:
                return False
        return True
    
    def accuracy(self):
        return self.correct / self.total * 100 if self.total > 0 else 0
    
    def to_string(self):
        conds = ' & '.join([f"{c[0]}{c[1]}{c[2]}" for c in self.conditions])
        return f"IF {conds} -> {self.prediction}"


def generate_condition(feat):
    if 'domina' in feat or 'unanime' in feat or 'mayoria' in feat or feat.startswith('anti_') or feat.endswith('_covered') or feat in ['local_fav', 'away_fav', 'has_ranks']:
        return (feat, '==', True)
    elif 'edge' in feat or feat.startswith('avg_') or feat.startswith('sum_'):
        threshold = round(random.uniform(-10, 10), 1)
        return (feat, random.choice(['>=', '<=', '>', '<']), threshold)
    elif feat == 'ah_bucket':
        return (feat, '==', random.choice([0, 0.5, 1, 1.5]))
    elif feat == 'ou_bucket':
        return (feat, '==', random.choice([2, 2.5, 3]))
    elif 'sources' in feat:
        return (feat, random.choice(['>=', '==']), random.randint(2, 5))
    elif feat == 'rank_diff':
        return (feat, random.choice(['>', '<', '>=', '<=']), random.choice([-5, -3, 0, 3, 5]))
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
            val = val + random.uniform(-2, 2)
            val = round(val, 1)
            new_conds[idx] = (feat, op, val)
    elif action == 'replace' and new_conds:
        idx = random.randint(0, len(new_conds) - 1)
        feat = random.choice(features_list)
        cond = generate_condition(feat)
        if cond:
            new_conds[idx] = cond
    
    return Rule(new_conds, rule.prediction, rule.rule_type)


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
    print("⚔️ ENTRENAMIENTO: ATAQUES PELIGROSOS + COMPARATIVAS DIRECTAS")
    print("=" * 70)
    print(f"Generaciones: {GENERATIONS}")
    print(f"Población: {POPULATION_SIZE}")
    print(f"Min precisión: {MIN_ACCURACY}%")
    print(f"Min muestras: {MIN_SAMPLES}")
    print()
    print("ENFOQUE:")
    print("  - Edges de Ataques Peligrosos (quién domina)")
    print("  - Edges de Tiros a Puerta")
    print("  - Unanimidad en stats (todas las fuentes de acuerdo)")
    print("  - Anti-intuición (mercado vs stats)")
    print()
    
    print("📂 Cargando partidos...")
    matches = load_all_matches()
    print(f"   Total partidos: {len(matches)}")
    
    # Contar partidos con stats
    with_stats = 0
    for m in matches:
        f = extract_attack_features(m)
        if f.get('sources_danger', 0) >= 2:
            with_stats += 1
    print(f"   Con >=2 fuentes de ataques peligrosos: {with_stats}")
    
    population = []
    for _ in range(POPULATION_SIZE // 4):
        for pred in ['LOCAL', 'VISITA']:
            r = generate_random_rule(ATTACK_FEATURES, pred, 'AH')
            if r:
                population.append(r)
        for pred in ['OVER', 'UNDER']:
            r = generate_random_rule(ATTACK_FEATURES, pred, 'OU')
            if r:
                population.append(r)
    
    print(f"   Población inicial: {len(population)}")
    print("\n" + "-" * 70)
    
    best_rules = {'AH': [], 'OU': []}
    
    for gen in range(GENERATIONS):
        # Evaluar
        for r in population:
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
            
            features = extract_attack_features(match)
            
            for rule in population:
                if not rule.matches(features):
                    continue
                
                rule.total += 1
                
                if rule.rule_type == 'AH' and ah_result != 'PUSH':
                    if rule.prediction == ah_result:
                        rule.correct += 1
                elif rule.rule_type == 'OU' and ou_result != 'PUSH':
                    if rule.prediction == ou_result:
                        rule.correct += 1
        
        # Buscar buenas reglas
        for r in population:
            if r.total < MIN_SAMPLES:
                continue
            
            acc = r.accuracy()
            cat = r.rule_type
            
            if acc >= MIN_ACCURACY:
                is_new = not any(
                    abs(existing.accuracy() - acc) < 1 and
                    existing.prediction == r.prediction and
                    len(existing.conditions) == len(r.conditions)
                    for existing in best_rules[cat]
                )
                if is_new and len(best_rules[cat]) < 80:
                    best_rules[cat].append(copy.deepcopy(r))
                    emoji = "🔥" if acc >= 75 else "✅" if acc >= 70 else "📊"
                    print(f"\n{emoji} Gen {gen+1} - {cat} ({r.prediction}): {acc:.1f}% (n={r.total})")
                    for c in r.conditions:
                        print(f"   {c[0]} {c[1]} {c[2]}")
        
        if (gen + 1) % 250 == 0:
            print(f"\n📊 Gen {gen+1}/{GENERATIONS}")
            print(f"   AH: {len(best_rules['AH'])} reglas")
            print(f"   OU: {len(best_rules['OU'])} reglas")
            if best_rules['AH']:
                top = max(best_rules['AH'], key=lambda r: r.accuracy())
                print(f"   Top AH: {top.accuracy():.1f}%")
            if best_rules['OU']:
                top = max(best_rules['OU'], key=lambda r: r.accuracy())
                print(f"   Top OU: {top.accuracy():.1f}%")
        
        # Evolución
        valid = [r for r in population if r.total >= 15 and r.accuracy() >= 50]
        if not valid:
            valid = population[:200]
        
        valid.sort(key=lambda r: -r.accuracy())
        survivors = valid[:600]
        
        new_pop = list(survivors)
        while len(new_pop) < POPULATION_SIZE:
            parent = random.choice(survivors)
            new_pop.append(mutate(parent, ATTACK_FEATURES))
        
        for _ in range(200):
            for pred in ['LOCAL', 'VISITA']:
                r = generate_random_rule(ATTACK_FEATURES, pred, 'AH')
                if r:
                    new_pop.append(r)
            for pred in ['OVER', 'UNDER']:
                r = generate_random_rule(ATTACK_FEATURES, pred, 'OU')
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
        'focus': 'ATAQUES_PELIGROSOS',
        'rules': {'AH': [], 'OU': []}
    }
    
    for cat in ['AH', 'OU']:
        rules = best_rules[cat]
        rules.sort(key=lambda r: -r.accuracy())
        
        print(f"\n🏆 MEJORES REGLAS {cat} (basadas en ataques): {len(rules)}")
        for i, r in enumerate(rules[:20], 1):
            emoji = "🔥" if r.accuracy() >= 75 else "✅" if r.accuracy() >= 70 else "📊"
            print(f"   {i}. {emoji} {r.prediction}: {r.accuracy():.1f}% (n={r.total})")
            for c in r.conditions:
                print(f"      - {c[0]} {c[1]} {c[2]}")
        
        for r in rules[:40]:
            results['rules'][cat].append({
                'conditions': [(c[0], c[1], c[2]) for c in r.conditions],
                'prediction': r.prediction,
                'accuracy': round(r.accuracy(), 2),
                'samples': r.total
            })
    
    path = RESULTS_DIR / 'attack_stats_rules.json'
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 Guardado en: {path}")


if __name__ == '__main__':
    main()
