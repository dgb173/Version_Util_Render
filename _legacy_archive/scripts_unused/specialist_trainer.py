# scripts/specialist_trainer.py
"""
SISTEMA ESPECIALISTA POR LÍNEA DE HANDICAP
===========================================
Entrena modelos SEPARADOS para cada línea de handicap:
- data_ah_0.json -> Especialista AH 0
- data_ah_0.5.json -> Especialista AH 0.5
- data_ah_1.5.json -> Especialista AH 1.5
- etc.

Objetivo: encontrar reglas con MÁXIMA PRECISIÓN posible
para cada línea específica.

Features usadas:
- Ataques peligrosos (edges)
- Tiros a puerta (edges)  
- Posiciones en clasificación
- Victorias/derrotas recientes
- Cuotas (AH line y su porqué)
- Coberturas históricas
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

# Archivos por línea de handicap
HANDICAP_FILES = {
    'AH_0': DATA_DIR / 'data_ah_0.json',
    'AH_0.5': DATA_DIR / 'data_ah_0.5.json',
    'AH_1.5': DATA_DIR / 'data_ah_1.5.json',
    'AH_2+': DATA_DIR / 'data_ah_2_plus.json',
    'AH_-0.5': DATA_DIR / 'data_minus_ah_0.5.json',
    'AH_-1.5': DATA_DIR / 'data_minus_ah_1.5.json',
    'AH_-2+': DATA_DIR / 'data_minus_ah_2_plus.json',
}

GENERATIONS = 3000
POPULATION_SIZE = 5000
MIN_SAMPLES = 25
MIN_ACCURACY = 75  # Buscamos >75%


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


def extract_specialist_features(match: Dict) -> Dict:
    """Extrae features completas incluyendo el PORQUÉ de la cuota."""
    f = {}
    
    main_odds = match.get('main_match_odds') or {}
    try:
        ah_line = float(main_odds.get('ah_linea', 0) or 0)
        ou_line = float(main_odds.get('goals_linea', 2.5) or 2.5)
    except:
        ah_line, ou_line = 0, 2.5
    
    f['ah_line'] = ah_line
    f['ou_line'] = ou_line
    f['local_fav'] = ah_line > 0
    f['away_fav'] = ah_line < 0
    f['neutral_line'] = ah_line == 0
    
    # ===== RANKINGS Y POSICIONES =====
    home_standings = match.get('home_standings') or {}
    away_standings = match.get('away_standings') or {}
    
    try:
        f['home_rank'] = int(home_standings.get('ranking', 0) or 0)
        f['away_rank'] = int(away_standings.get('ranking', 0) or 0)
        f['rank_diff'] = f['home_rank'] - f['away_rank']
        f['home_top3'] = 0 < f['home_rank'] <= 3
        f['home_top5'] = 0 < f['home_rank'] <= 5
        f['away_top3'] = 0 < f['away_rank'] <= 3
        f['away_top5'] = 0 < f['away_rank'] <= 5
        f['home_bottom5'] = f['home_rank'] >= 15
        f['away_bottom5'] = f['away_rank'] >= 15
        f['rank_close'] = abs(f['rank_diff']) <= 3
    except:
        f['home_rank'] = f['away_rank'] = f['rank_diff'] = 0
    
    # ===== VICTORIAS/DERROTAS RECIENTES =====
    # Extraer W-D-L de standings
    for prefix, standings in [('home', home_standings), ('away', away_standings)]:
        try:
            f[f'{prefix}_wins'] = int(standings.get('wins', 0) or 0)
            f[f'{prefix}_draws'] = int(standings.get('draws', 0) or 0)
            f[f'{prefix}_losses'] = int(standings.get('losses', 0) or 0)
            total = f[f'{prefix}_wins'] + f[f'{prefix}_draws'] + f[f'{prefix}_losses']
            f[f'{prefix}_win_rate'] = f[f'{prefix}_wins'] / total if total > 0 else 0.5
            f[f'{prefix}_loss_rate'] = f[f'{prefix}_losses'] / total if total > 0 else 0.5
        except:
            f[f'{prefix}_wins'] = f[f'{prefix}_draws'] = f[f'{prefix}_losses'] = 0
            f[f'{prefix}_win_rate'] = f[f'{prefix}_loss_rate'] = 0.5
    
    f['home_better_record'] = f.get('home_win_rate', 0) > f.get('away_win_rate', 0)
    f['away_better_record'] = f.get('away_win_rate', 0) > f.get('home_win_rate', 0)
    
    # ===== CUOTA - EL PORQUÉ =====
    # La cuota refleja: ranking + forma + H2H + factores externos
    # Anomalía: cuota no coincide con lo que sugieren ranking/forma
    f['cuota_vs_ranking'] = 'NORMAL'
    if f['home_rank'] > 0 and f['away_rank'] > 0:
        if f['home_rank'] < f['away_rank'] and f.get('away_fav'):
            f['cuota_vs_ranking'] = 'ANOMALIA_LOCAL'  # Local mejor rank pero visita favorita
        elif f['away_rank'] < f['home_rank'] and f.get('local_fav'):
            f['cuota_vs_ranking'] = 'ANOMALIA_VISITA'  # Visita mejor rank pero local favorita
    
    f['cuota_vs_forma'] = 'NORMAL'
    if f.get('home_better_record') and f.get('away_fav'):
        f['cuota_vs_forma'] = 'ANOMALIA_LOCAL'
    elif f.get('away_better_record') and f.get('local_fav'):
        f['cuota_vs_forma'] = 'ANOMALIA_VISITA'
    
    # ===== ATAQUES PELIGROSOS Y TIROS =====
    danger_edges = []
    sot_edges = []
    shots_edges = []
    cover_results = []
    ou_results = []
    
    def process_source(name, data, is_home_perspective):
        if not data:
            return
        
        stats = parse_stats_rows(data.get('stats_rows', []))
        
        for stat_key, edge_list in [
            ('Ataques Peligrosos', danger_edges),
            ('Tiros a Puerta', sot_edges),
            ('Tiros', shots_edges)
        ]:
            if stat_key in stats:
                h = stats[stat_key]['home']
                a = stats[stat_key]['away']
                edge = (h - a) if is_home_perspective else (a - h)
                edge_list.append(edge)
                f[f'{name}_edge_{stat_key.lower().replace(" ", "_")}'] = edge
        
        # Cover
        score = parse_score(data.get('score'))
        if score:
            try:
                src_ah = float(data.get('handicap_line_raw') or data.get('handicap') or 0)
            except:
                src_ah = 0
            cover = did_cover(score[0], score[1], src_ah, is_home_perspective)
            cover_results.append(cover)
            f[f'{name}_cover'] = cover
            
            total = score[0] + score[1]
            ou = 'OVER' if total > 2.5 else 'UNDER'
            ou_results.append(ou)
            f[f'{name}_ou'] = ou
            f[f'{name}_goals'] = total
    
    process_source('prev_home', match.get('last_home_match'), True)
    process_source('prev_away', match.get('last_away_match'), False)
    process_source('h2h_stadium', match.get('h2h_stadium'), True)
    process_source('h2h_general', match.get('h2h_general'), True)
    
    if match.get('h2h_col3') and match['h2h_col3'].get('status') == 'found':
        process_source('h2h_col3', match['h2h_col3'], True)
    
    comp = match.get('comparativas_indirectas') or {}
    if comp.get('left'):
        process_source('ind_left', comp['left'], comp['left'].get('localia') == 'H')
    if comp.get('right'):
        process_source('ind_right', comp['right'], comp['right'].get('localia') != 'A')
    
    # Agregados
    if danger_edges:
        f['avg_edge_danger'] = sum(danger_edges) / len(danger_edges)
        f['sum_edge_danger'] = sum(danger_edges)
        f['sources_danger'] = len(danger_edges)
        f['local_domina_danger'] = f['avg_edge_danger'] > 0
        f['local_domina_danger_fuerte'] = f['avg_edge_danger'] >= 5
        f['visita_domina_danger_fuerte'] = f['avg_edge_danger'] <= -5
        f['danger_unanime_local'] = all(e > 0 for e in danger_edges) and len(danger_edges) >= 3
        f['danger_unanime_visita'] = all(e < 0 for e in danger_edges) and len(danger_edges) >= 3
    else:
        f['avg_edge_danger'] = f['sum_edge_danger'] = f['sources_danger'] = 0
    
    if sot_edges:
        f['avg_edge_sot'] = sum(sot_edges) / len(sot_edges)
        f['sum_edge_sot'] = sum(sot_edges)
        f['sources_sot'] = len(sot_edges)
        f['local_domina_sot'] = f['avg_edge_sot'] > 0
    else:
        f['avg_edge_sot'] = f['sum_edge_sot'] = f['sources_sot'] = 0
    
    # Coberturas
    covers = sum(1 for c in cover_results if c == 'COVER')
    no_covers = sum(1 for c in cover_results if c == 'NO_COVER')
    valid = len([c for c in cover_results if c in ['COVER', 'NO_COVER']])
    
    f['covers'] = covers
    f['no_covers'] = no_covers
    f['cover_sources'] = valid
    f['cover_ratio'] = covers / valid if valid > 0 else 0.5
    f['all_covered'] = covers == valid and valid >= 2
    f['none_covered'] = no_covers == valid and valid >= 2
    
    # Market analysis
    market = match.get('market_analysis_data') or {}
    stadium_m = market.get('stadium') or {}
    general_m = market.get('general') or {}
    
    f['h2h_stadium_covered'] = stadium_m.get('is_covered')
    f['h2h_general_covered'] = general_m.get('is_covered')
    f['h2h_both_covered'] = f['h2h_stadium_covered'] == True and f['h2h_general_covered'] == True
    f['h2h_both_not_covered'] = f['h2h_stadium_covered'] == False and f['h2h_general_covered'] == False
    
    # Movimiento de línea
    if stadium_m.get('movement'):
        parts = stadium_m['movement'].replace('→', '->').split('->')
        if len(parts) == 2:
            try:
                before = float(parts[0].strip())
                after = float(parts[1].strip())
                f['line_change'] = after - before
                f['line_increased'] = after > before
                f['line_decreased'] = after < before
                f['fav_changed'] = (before > 0) != (after > 0)
            except:
                pass
    
    # ===== COMBINACIONES ESPECIALES =====
    f['anti_intuicion_local'] = f.get('away_fav', False) and f.get('local_domina_danger_fuerte', False)
    f['anti_intuicion_visita'] = f.get('local_fav', False) and f.get('visita_domina_danger_fuerte', False)
    
    f['local_domina_todo'] = (
        f.get('local_domina_danger', False) and 
        f.get('local_domina_sot', False) and
        f.get('home_better_record', False)
    )
    f['visita_domina_todo'] = (
        not f.get('local_domina_danger', True) and 
        not f.get('local_domina_sot', True) and
        f.get('away_better_record', False)
    )
    
    f['cuota_anomalia'] = f.get('cuota_vs_ranking') != 'NORMAL' or f.get('cuota_vs_forma') != 'NORMAL'
    
    return f


# Features por tipo
SPECIALIST_FEATURES = [
    # Rankings y posiciones
    'home_rank', 'away_rank', 'rank_diff', 'home_top3', 'home_top5', 'away_top3', 'away_top5',
    'home_bottom5', 'away_bottom5', 'rank_close',
    
    # Victorias/forma
    'home_wins', 'home_losses', 'away_wins', 'away_losses',
    'home_win_rate', 'away_win_rate', 'home_better_record', 'away_better_record',
    
    # Cuota/mercado
    'local_fav', 'away_fav', 'neutral_line', 'cuota_anomalia',
    'cuota_vs_ranking', 'cuota_vs_forma',
    'line_change', 'line_increased', 'line_decreased', 'fav_changed',
    
    # Ataques
    'avg_edge_danger', 'sum_edge_danger', 'sources_danger',
    'local_domina_danger', 'local_domina_danger_fuerte', 'visita_domina_danger_fuerte',
    'danger_unanime_local', 'danger_unanime_visita',
    'avg_edge_sot', 'sum_edge_sot', 'local_domina_sot',
    
    # Coberturas
    'covers', 'no_covers', 'cover_ratio', 'cover_sources',
    'all_covered', 'none_covered',
    'h2h_stadium_covered', 'h2h_general_covered', 'h2h_both_covered', 'h2h_both_not_covered',
    'prev_home_cover', 'prev_away_cover',
    
    # Combinaciones
    'anti_intuicion_local', 'anti_intuicion_visita',
    'local_domina_todo', 'visita_domina_todo',
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
                elif isinstance(val, str):
                    if str(fv) != val:
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


def generate_condition(feat):
    if feat in ['local_fav', 'away_fav', 'neutral_line', 'home_top3', 'home_top5', 'away_top3', 'away_top5',
                'home_bottom5', 'away_bottom5', 'rank_close', 'home_better_record', 'away_better_record',
                'local_domina_danger', 'local_domina_danger_fuerte', 'visita_domina_danger_fuerte',
                'danger_unanime_local', 'danger_unanime_visita', 'local_domina_sot',
                'all_covered', 'none_covered', 'h2h_both_covered', 'h2h_both_not_covered',
                'anti_intuicion_local', 'anti_intuicion_visita', 'local_domina_todo', 'visita_domina_todo',
                'line_increased', 'line_decreased', 'fav_changed', 'cuota_anomalia']:
        return (feat, '==', True)
    elif feat in ['cuota_vs_ranking', 'cuota_vs_forma']:
        return (feat, '==', random.choice(['NORMAL', 'ANOMALIA_LOCAL', 'ANOMALIA_VISITA']))
    elif feat in ['prev_home_cover', 'prev_away_cover', 'h2h_stadium_covered', 'h2h_general_covered']:
        return (feat, '==', random.choice(['COVER', 'NO_COVER', True, False]))
    elif 'edge' in feat or 'sum' in feat:
        return (feat, random.choice(['>=', '<=', '>', '<']), round(random.uniform(-10, 10), 1))
    elif 'ratio' in feat or 'rate' in feat:
        return (feat, random.choice(['>=', '<=']), round(random.uniform(0.2, 0.8), 2))
    elif 'rank' in feat:
        return (feat, random.choice(['>', '<', '>=', '<=']), random.choice([-8, -5, -3, 0, 3, 5, 8]))
    elif feat in ['covers', 'no_covers', 'cover_sources', 'sources_danger', 'sources_sot']:
        return (feat, random.choice(['>=', '<=', '==']), random.randint(1, 6))
    elif feat in ['home_wins', 'home_losses', 'away_wins', 'away_losses']:
        return (feat, random.choice(['>=', '<=']), random.randint(1, 10))
    elif 'line_change' in feat:
        return (feat, random.choice(['>', '<']), round(random.uniform(-0.5, 0.5), 2))
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
            val = round(val, 2)
            new_conds[idx] = (feat, op, val)
    elif action == 'replace' and new_conds:
        idx = random.randint(0, len(new_conds) - 1)
        feat = random.choice(features_list)
        cond = generate_condition(feat)
        if cond:
            new_conds[idx] = cond
    
    return Rule(new_conds, rule.prediction, rule.rule_type)


def train_specialist(handicap_name: str, file_path: Path) -> Dict:
    """Entrena especialista para una línea de handicap específica."""
    
    print(f"\n{'='*60}")
    print(f"🎯 ESPECIALISTA: {handicap_name}")
    print(f"{'='*60}")
    
    if not file_path.exists():
        print(f"   ⚠️ Archivo no encontrado: {file_path}")
        return {'handicap': handicap_name, 'rules': {'AH': [], 'OU': []}}
    
    # Cargar partidos
    with open(file_path, 'r', encoding='utf-8') as f:
        matches = json.load(f)
    
    matches = [m for m in matches if parse_score(m.get('final_score') or m.get('score'))]
    print(f"   Partidos con resultado: {len(matches)}")
    
    if len(matches) < 50:
        print(f"   ⚠️ Muy pocos partidos para entrenar")
        return {'handicap': handicap_name, 'rules': {'AH': [], 'OU': []}}
    
    # Poblar
    population = []
    for _ in range(POPULATION_SIZE // 4):
        for pred in ['LOCAL', 'VISITA']:
            r = generate_random_rule(SPECIALIST_FEATURES, pred, 'AH')
            if r:
                population.append(r)
        for pred in ['OVER', 'UNDER']:
            r = generate_random_rule(SPECIALIST_FEATURES, pred, 'OU')
            if r:
                population.append(r)
    
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
            
            main_odds = match.get('main_match_odds') or {}
            try:
                ah = float(main_odds.get('ah_linea', 0) or 0)
                ou = float(main_odds.get('goals_linea', 2.5) or 2.5)
            except:
                continue
            
            ah_result = get_ah_winner(parsed[0], parsed[1], ah)
            ou_result = get_ou_result(parsed[0], parsed[1], ou)
            
            features = extract_specialist_features(match)
            
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
                    existing.prediction == r.prediction
                    for existing in best_rules[cat]
                )
                if is_new and len(best_rules[cat]) < 50:
                    best_rules[cat].append(copy.deepcopy(r))
                    emoji = "🔥🔥" if acc >= 90 else "🔥" if acc >= 80 else "✅"
                    print(f"{emoji} Gen {gen+1} - {cat} ({r.prediction}): {acc:.1f}% (n={r.total})")
                    for c in r.conditions[:3]:
                        print(f"   {c[0]} {c[1]} {c[2]}")
        
        if (gen + 1) % 500 == 0:
            print(f"   Gen {gen+1}/{GENERATIONS} - AH: {len(best_rules['AH'])}, OU: {len(best_rules['OU'])}")
        
        # Evolución
        valid = [r for r in population if r.total >= 15 and r.accuracy() >= 50]
        if not valid:
            valid = population[:200]
        
        valid.sort(key=lambda r: -r.accuracy())
        survivors = valid[:600]
        
        new_pop = list(survivors)
        while len(new_pop) < POPULATION_SIZE:
            parent = random.choice(survivors)
            new_pop.append(mutate(parent, SPECIALIST_FEATURES))
        
        for _ in range(200):
            for pred in ['LOCAL', 'VISITA']:
                r = generate_random_rule(SPECIALIST_FEATURES, pred, 'AH')
                if r:
                    new_pop.append(r)
            for pred in ['OVER', 'UNDER']:
                r = generate_random_rule(SPECIALIST_FEATURES, pred, 'OU')
                if r:
                    new_pop.append(r)
        
        population = new_pop
    
    # Guardar resultados
    result = {
        'handicap': handicap_name,
        'total_matches': len(matches),
        'rules': {'AH': [], 'OU': []}
    }
    
    for cat in ['AH', 'OU']:
        rules = sorted(best_rules[cat], key=lambda r: -r.accuracy())
        print(f"\n   🏆 Top {cat}: {len(rules)} reglas")
        for r in rules[:10]:
            print(f"      {r.prediction}: {r.accuracy():.1f}% (n={r.total})")
        
        for r in rules[:20]:
            result['rules'][cat].append({
                'conditions': [(c[0], c[1], c[2]) for c in r.conditions],
                'prediction': r.prediction,
                'accuracy': round(r.accuracy(), 2),
                'samples': r.total
            })
    
    return result


def main():
    print("=" * 70)
    print("🎯 SISTEMA ESPECIALISTA POR LÍNEA DE HANDICAP")
    print("=" * 70)
    print(f"Generaciones por especialista: {GENERATIONS}")
    print(f"Población: {POPULATION_SIZE}")
    print(f"Min precisión: {MIN_ACCURACY}%")
    print(f"Min muestras: {MIN_SAMPLES}")
    print()
    
    all_results = {
        'timestamp': datetime.now().isoformat(),
        'config': {
            'generations': GENERATIONS,
            'population': POPULATION_SIZE,
            'min_accuracy': MIN_ACCURACY,
            'min_samples': MIN_SAMPLES
        },
        'specialists': {}
    }
    
    for handicap_name, file_path in HANDICAP_FILES.items():
        result = train_specialist(handicap_name, file_path)
        all_results['specialists'][handicap_name] = result
    
    # Guardar
    path = RESULTS_DIR / 'specialist_rules.json'
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)
    
    print("\n" + "=" * 70)
    print("📊 RESUMEN FINAL")
    print("=" * 70)
    
    total_ah = 0
    total_ou = 0
    for name, data in all_results['specialists'].items():
        ah = len(data['rules'].get('AH', []))
        ou = len(data['rules'].get('OU', []))
        total_ah += ah
        total_ou += ou
        if ah > 0 or ou > 0:
            print(f"   {name}: AH={ah}, OU={ou}")
    
    print(f"\n   TOTAL: {total_ah} reglas AH, {total_ou} reglas OU")
    print(f"   💾 Guardado en: {path}")


if __name__ == '__main__':
    main()
