# scripts/ou_expert_trainer.py
"""
🎯 SISTEMA EXPERTO OVER/UNDER - VERSIÓN PROFESIONAL
=====================================================
Sistema especializado para entrenar patrones de Over/Under
con features avanzadas y alta precisión.

Features incluidas:
- Goles promedio de cada equipo (casa/fuera)
- Goles en partidos previos (prev home, prev away, H2H)
- Tendencia Over/Under histórica
- Línea de goles y movimiento
- Comparativas indirectas
- Estadísticas de ataques peligrosos
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

# Archivos por línea de handicap (usaremos todos para O/U)
DATA_FILES = [
    DATA_DIR / 'data_ah_0.json',
    DATA_DIR / 'data_ah_0.5.json',
    DATA_DIR / 'data_ah_1.5.json',
    DATA_DIR / 'data_ah_2_plus.json',
    DATA_DIR / 'data_minus_ah_0.5.json',
    DATA_DIR / 'data_minus_ah_1.5.json',
    DATA_DIR / 'data_minus_ah_2_plus.json',
]

GENERATIONS = 5000
POPULATION_SIZE = 8000
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


def get_ou_result(home_goals, away_goals, ou_line) -> str:
    total = home_goals + away_goals
    if total > ou_line + 0.25:
        return 'OVER'
    elif total < ou_line - 0.25:
        return 'UNDER'
    return 'PUSH'


def extract_ou_features(match: Dict) -> Dict:
    """
    Extrae features especializadas para Over/Under.
    """
    f = {}
    
    main_odds = match.get('main_match_odds') or {}
    try:
        ou_line = float(main_odds.get('goals_linea', 2.5) or 2.5)
        ah_line = float(main_odds.get('ah_linea', 0) or 0)
    except:
        ou_line, ah_line = 2.5, 0
    
    f['ou_line'] = ou_line
    f['ou_bucket'] = round(ou_line * 2) / 2
    f['ah_line'] = ah_line
    f['ah_abs'] = abs(ah_line)
    
    # ===== STANDINGS Y GOLES PROMEDIO =====
    home_standings = match.get('home_standings') or {}
    away_standings = match.get('away_standings') or {}
    
    # Goles promedio del local en casa
    try:
        home_gf = int(home_standings.get('specific_gf', 0) or 0)
        home_gc = int(home_standings.get('specific_gc', 0) or 0)
        home_pj = int(home_standings.get('specific_pj', 1) or 1)
        f['home_goals_avg'] = (home_gf + home_gc) / home_pj if home_pj > 0 else 2.5
        f['home_gf_avg'] = home_gf / home_pj if home_pj > 0 else 1.0
        f['home_gc_avg'] = home_gc / home_pj if home_pj > 0 else 1.0
    except:
        f['home_goals_avg'] = 2.5
        f['home_gf_avg'] = f['home_gc_avg'] = 1.0
    
    # Goles promedio del visitante fuera
    try:
        away_gf = int(away_standings.get('specific_gf', 0) or 0)
        away_gc = int(away_standings.get('specific_gc', 0) or 0)
        away_pj = int(away_standings.get('specific_pj', 1) or 1)
        f['away_goals_avg'] = (away_gf + away_gc) / away_pj if away_pj > 0 else 2.5
        f['away_gf_avg'] = away_gf / away_pj if away_pj > 0 else 1.0
        f['away_gc_avg'] = away_gc / away_pj if away_pj > 0 else 1.0
    except:
        f['away_goals_avg'] = 2.5
        f['away_gf_avg'] = f['away_gc_avg'] = 1.0
    
    # Promedio combinado
    f['combined_goals_avg'] = (f['home_goals_avg'] + f['away_goals_avg']) / 2
    f['expected_goals'] = f['home_gf_avg'] + f['away_gf_avg']
    
    # Rankings
    try:
        f['home_rank'] = int(home_standings.get('ranking', 0) or 0)
        f['away_rank'] = int(away_standings.get('ranking', 0) or 0)
        f['rank_diff'] = f['home_rank'] - f['away_rank']
        f['rank_close'] = abs(f['rank_diff']) <= 3
    except:
        f['home_rank'] = f['away_rank'] = f['rank_diff'] = 0
    
    # ===== PREV HOME MATCH =====
    prev_home = match.get('last_home_match') or {}
    if prev_home:
        score = parse_score(prev_home.get('score'))
        if score:
            total = score[0] + score[1]
            f['prev_home_goals'] = total
            f['prev_home_over'] = total > 2.5
            f['prev_home_under'] = total <= 2
            f['prev_home_high'] = total >= 4
            f['prev_home_low'] = total <= 1
        
        # Stats de ataques
        stats = parse_stats_rows(prev_home.get('stats_rows', []))
        if 'Ataques Peligrosos' in stats:
            f['prev_home_attacks_total'] = stats['Ataques Peligrosos']['home'] + stats['Ataques Peligrosos']['away']
            f['prev_home_attacks_high'] = f['prev_home_attacks_total'] > 200
        if 'Tiros a Puerta' in stats:
            f['prev_home_sot_total'] = stats['Tiros a Puerta']['home'] + stats['Tiros a Puerta']['away']
            f['prev_home_sot_high'] = f['prev_home_sot_total'] > 10
    
    # ===== PREV AWAY MATCH =====
    prev_away = match.get('last_away_match') or {}
    if prev_away:
        score = parse_score(prev_away.get('score'))
        if score:
            total = score[0] + score[1]
            f['prev_away_goals'] = total
            f['prev_away_over'] = total > 2.5
            f['prev_away_under'] = total <= 2
            f['prev_away_high'] = total >= 4
            f['prev_away_low'] = total <= 1
        
        # Stats de ataques
        stats = parse_stats_rows(prev_away.get('stats_rows', []))
        if 'Ataques Peligrosos' in stats:
            f['prev_away_attacks_total'] = stats['Ataques Peligrosos']['home'] + stats['Ataques Peligrosos']['away']
            f['prev_away_attacks_high'] = f['prev_away_attacks_total'] > 200
        if 'Tiros a Puerta' in stats:
            f['prev_away_sot_total'] = stats['Tiros a Puerta']['home'] + stats['Tiros a Puerta']['away']
            f['prev_away_sot_high'] = f['prev_away_sot_total'] > 10
    
    # ===== H2H COL3 =====
    h2h_col3 = match.get('h2h_col3') or {}
    if h2h_col3.get('status') == 'found':
        try:
            h = int(h2h_col3.get('goles_home', 0) or 0)
            a = int(h2h_col3.get('goles_away', 0) or 0)
            total = h + a
            f['h2h_col3_goals'] = total
            f['h2h_col3_over'] = total > 2.5
            f['h2h_col3_under'] = total <= 2
            f['h2h_col3_high'] = total >= 4
            f['h2h_col3_low'] = total <= 1
        except:
            pass
    
    # ===== COMPARATIVAS INDIRECTAS =====
    comp = match.get('comparativas_indirectas') or {}
    
    ind_left = comp.get('left')
    if ind_left:
        score = parse_score(ind_left.get('score'))
        if score:
            total = score[0] + score[1]
            f['ind_left_goals'] = total
            f['ind_left_over'] = total > 2.5
            f['ind_left_under'] = total <= 2
            f['ind_left_high'] = total >= 4
    
    ind_right = comp.get('right')
    if ind_right:
        score = parse_score(ind_right.get('score'))
        if score:
            total = score[0] + score[1]
            f['ind_right_goals'] = total
            f['ind_right_over'] = total > 2.5
            f['ind_right_under'] = total <= 2
            f['ind_right_high'] = total >= 4
    
    # ===== MARKET ANALYSIS =====
    market = match.get('market_analysis_data') or {}
    stadium_m = market.get('stadium') or {}
    general_m = market.get('general') or {}
    
    f['h2h_stadium_covered'] = stadium_m.get('is_covered')
    f['h2h_general_covered'] = general_m.get('is_covered')
    f['h2h_both_covered'] = f['h2h_stadium_covered'] == True and f['h2h_general_covered'] == True
    f['h2h_none_covered'] = f['h2h_stadium_covered'] == False and f['h2h_general_covered'] == False
    
    # ===== PATRONES COMBINADOS =====
    # Ambos previos Over
    f['both_prev_over'] = f.get('prev_home_over', False) and f.get('prev_away_over', False)
    f['both_prev_under'] = f.get('prev_home_under', False) and f.get('prev_away_under', False)
    
    # All high scoring
    f['all_high_scoring'] = (
        f.get('prev_home_high', False) and 
        f.get('prev_away_high', False)
    )
    
    # All low scoring
    f['all_low_scoring'] = (
        f.get('prev_home_low', False) and 
        f.get('prev_away_low', False)
    )
    
    # Expected vs line
    if 'expected_goals' in f:
        f['expected_over_line'] = f['expected_goals'] > ou_line
        f['expected_under_line'] = f['expected_goals'] < ou_line - 0.5
        f['expected_far_over'] = f['expected_goals'] > ou_line + 1
        f['expected_far_under'] = f['expected_goals'] < ou_line - 1
    
    # Handicap indica partido cerrado o goleada
    f['tight_handicap'] = abs(ah_line) <= 0.5
    f['big_handicap'] = abs(ah_line) >= 1.5
    
    return f


# Features para O/U
OU_FEATURES = [
    # Línea
    'ou_line', 'ou_bucket', 'ah_abs', 'tight_handicap', 'big_handicap',
    
    # Promedios de goles
    'home_goals_avg', 'away_goals_avg', 'combined_goals_avg', 'expected_goals',
    'home_gf_avg', 'home_gc_avg', 'away_gf_avg', 'away_gc_avg',
    'expected_over_line', 'expected_under_line', 'expected_far_over', 'expected_far_under',
    
    # Rankings
    'rank_diff', 'rank_close',
    
    # Prev home
    'prev_home_goals', 'prev_home_over', 'prev_home_under', 'prev_home_high', 'prev_home_low',
    'prev_home_attacks_total', 'prev_home_attacks_high', 'prev_home_sot_total', 'prev_home_sot_high',
    
    # Prev away
    'prev_away_goals', 'prev_away_over', 'prev_away_under', 'prev_away_high', 'prev_away_low',
    'prev_away_attacks_total', 'prev_away_attacks_high', 'prev_away_sot_total', 'prev_away_sot_high',
    
    # H2H Col3
    'h2h_col3_goals', 'h2h_col3_over', 'h2h_col3_under', 'h2h_col3_high', 'h2h_col3_low',
    
    # Indirectas
    'ind_left_goals', 'ind_left_over', 'ind_left_under', 'ind_left_high',
    'ind_right_goals', 'ind_right_over', 'ind_right_under', 'ind_right_high',
    
    # Market
    'h2h_both_covered', 'h2h_none_covered',
    
    # Combinados
    'both_prev_over', 'both_prev_under', 'all_high_scoring', 'all_low_scoring',
]


class OUPattern:
    def __init__(self, conditions, pick):
        self.conditions = conditions  # [(feature, op, value), ...]
        self.pick = pick  # 'OVER' o 'UNDER'
        self.name = None
        
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
    
    def generate_name(self):
        cond_keys = [c[0] for c in self.conditions if c[2] == True]
        
        if 'all_high_scoring' in cond_keys or 'both_prev_over' in cond_keys:
            return 'GOLEADORES_HISTORICOS'
        elif 'all_low_scoring' in cond_keys or 'both_prev_under' in cond_keys:
            return 'DEFENSIVOS_HISTORICOS'
        elif 'h2h_col3_high' in cond_keys:
            return 'H2H_GOLEADOR'
        elif 'h2h_col3_low' in cond_keys:
            return 'H2H_DEFENSIVO'
        elif 'expected_far_over' in cond_keys:
            return 'ESPERADO_ALTO'
        elif 'expected_far_under' in cond_keys:
            return 'ESPERADO_BAJO'
        elif 'prev_home_attacks_high' in cond_keys and 'prev_away_attacks_high' in cond_keys:
            return 'ATAQUES_EXPLOSIVOS'
        elif 'h2h_both_covered' in cond_keys:
            return 'H2H_CUBIERTO'
        elif 'big_handicap' in cond_keys:
            return 'HANDICAP_GRANDE'
        elif 'tight_handicap' in cond_keys:
            return 'HANDICAP_CERRADO'
        
        return f'PATRON_OU_{len(self.conditions)}'
    
    def to_dict(self):
        return {
            'name': self.name or self.generate_name(),
            'pick': self.pick,
            'accuracy': round(self.accuracy(), 1),
            'samples': self.total,
            'conditions': [(c[0], c[1], c[2]) for c in self.conditions]
        }


def generate_condition(feat):
    if feat in ['prev_home_over', 'prev_home_under', 'prev_home_high', 'prev_home_low',
                'prev_away_over', 'prev_away_under', 'prev_away_high', 'prev_away_low',
                'h2h_col3_over', 'h2h_col3_under', 'h2h_col3_high', 'h2h_col3_low',
                'ind_left_over', 'ind_left_under', 'ind_left_high',
                'ind_right_over', 'ind_right_under', 'ind_right_high',
                'h2h_both_covered', 'h2h_none_covered',
                'both_prev_over', 'both_prev_under', 'all_high_scoring', 'all_low_scoring',
                'expected_over_line', 'expected_under_line', 'expected_far_over', 'expected_far_under',
                'tight_handicap', 'big_handicap', 'rank_close',
                'prev_home_attacks_high', 'prev_away_attacks_high',
                'prev_home_sot_high', 'prev_away_sot_high']:
        return (feat, '==', True)
    elif 'goals' in feat and 'avg' not in feat:
        return (feat, random.choice(['>=', '<=', '>', '<']), random.randint(1, 5))
    elif 'avg' in feat:
        return (feat, random.choice(['>=', '<=', '>', '<']), round(random.uniform(1.5, 4.0), 1))
    elif feat == 'ou_bucket':
        return (feat, '==', random.choice([2.0, 2.5, 3.0, 3.5]))
    elif feat == 'ah_abs':
        return (feat, random.choice(['>=', '<=', '>', '<']), round(random.uniform(0.25, 2.0), 2))
    elif feat == 'rank_diff':
        return (feat, random.choice(['>=', '<=', '>', '<']), random.randint(-8, 8))
    elif 'attacks' in feat or 'sot' in feat:
        return (feat, random.choice(['>=', '<=', '>', '<']), random.randint(100, 250))
    return None


def generate_random_pattern():
    n = random.randint(3, 5)
    conditions = []
    used = set()
    for _ in range(n):
        feat = random.choice(OU_FEATURES)
        if feat in used:
            continue
        used.add(feat)
        cond = generate_condition(feat)
        if cond:
            conditions.append(cond)
    
    if len(conditions) < 3:
        return None
    
    pick = random.choice(['OVER', 'UNDER'])
    return OUPattern(conditions, pick)


def mutate(pattern):
    new_conds = list(pattern.conditions)
    action = random.choice(['add', 'remove', 'modify', 'replace'])
    
    if action == 'add' and len(new_conds) < 6:
        feat = random.choice(OU_FEATURES)
        cond = generate_condition(feat)
        if cond and not any(c[0] == feat for c in new_conds):
            new_conds.append(cond)
    elif action == 'remove' and len(new_conds) > 3:
        new_conds.pop(random.randint(0, len(new_conds) - 1))
    elif action == 'modify' and new_conds:
        idx = random.randint(0, len(new_conds) - 1)
        feat, op, val = new_conds[idx]
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            val = val + random.uniform(-1, 1)
            val = round(val, 2)
            new_conds[idx] = (feat, op, val)
    elif action == 'replace' and new_conds:
        idx = random.randint(0, len(new_conds) - 1)
        feat = random.choice(OU_FEATURES)
        cond = generate_condition(feat)
        if cond:
            new_conds[idx] = cond
    
    new_pattern = OUPattern(new_conds, pattern.pick)
    
    # Ocasionalmente cambiar pick
    if random.random() < 0.1:
        new_pattern.pick = 'UNDER' if pattern.pick == 'OVER' else 'OVER'
    
    return new_pattern


def main():
    print("=" * 70)
    print("🎯 SISTEMA EXPERTO OVER/UNDER - VERSIÓN PROFESIONAL")
    print("=" * 70)
    print(f"Generaciones: {GENERATIONS}")
    print(f"Población: {POPULATION_SIZE}")
    print(f"Min precisión: {MIN_ACCURACY}%")
    print()
    
    # Cargar todos los partidos
    all_matches = []
    for file_path in DATA_FILES:
        if file_path.exists():
            with open(file_path, 'r', encoding='utf-8') as f:
                matches = json.load(f)
                all_matches.extend(matches)
                print(f"   Cargado: {file_path.name} ({len(matches)} partidos)")
    
    # Filtrar partidos con score válido
    all_matches = [m for m in all_matches if parse_score(m.get('final_score') or m.get('score'))]
    print(f"\n📊 Total partidos válidos: {len(all_matches)}")
    
    if not all_matches:
        print("❌ No hay partidos para entrenar")
        return
    
    # Poblar
    population = []
    for _ in range(POPULATION_SIZE):
        p = generate_random_pattern()
        if p:
            population.append(p)
    
    best_patterns = []
    
    for gen in range(GENERATIONS):
        # Reset
        for p in population:
            p.total = p.correct = 0
        
        for match in all_matches:
            score = match.get('final_score') or match.get('score')
            parsed = parse_score(score)
            if not parsed:
                continue
            
            main_odds = match.get('main_match_odds') or {}
            try:
                ou = float(main_odds.get('goals_linea', 2.5) or 2.5)
            except:
                continue
            
            ou_result = get_ou_result(parsed[0], parsed[1], ou)
            
            if ou_result == 'PUSH':
                continue
            
            features = extract_ou_features(match)
            
            for pattern in population:
                if not pattern.matches(features):
                    continue
                
                pattern.total += 1
                if pattern.pick == ou_result:
                    pattern.correct += 1
        
        # Buscar excelentes
        for p in population:
            if p.total < MIN_SAMPLES:
                continue
            
            acc = p.accuracy()
            
            if acc >= MIN_ACCURACY:
                is_new = not any(
                    abs(existing.accuracy() - acc) < 2 and
                    existing.pick == p.pick
                    for existing in best_patterns
                )
                if is_new and len(best_patterns) < 100:
                    p.name = p.generate_name()
                    best_patterns.append(copy.deepcopy(p))
                    emoji = "🔥🔥🔥" if acc >= 90 else "🔥🔥" if acc >= 85 else "🔥"
                    print(f"{emoji} Gen {gen+1} [{p.name}] {p.pick}: {acc:.1f}% (n={p.total})")
        
        if (gen + 1) % 500 == 0:
            print(f"   Gen {gen+1}/{GENERATIONS} - {len(best_patterns)} patrones")
        
        # Evolución
        valid = [p for p in population if p.total >= 10 and p.accuracy() >= 50]
        if not valid:
            valid = population[:300]
        
        valid.sort(key=lambda p: -p.accuracy())
        survivors = valid[:800]
        
        new_pop = list(survivors)
        while len(new_pop) < POPULATION_SIZE:
            parent = random.choice(survivors)
            new_pop.append(mutate(parent))
        
        for _ in range(300):
            p = generate_random_pattern()
            if p:
                new_pop.append(p)
        
        population = new_pop
    
    # Ordenar y guardar
    best_patterns.sort(key=lambda p: -p.accuracy())
    
    print(f"\n{'='*70}")
    print("🏆 RESUMEN FINAL")
    print("="*70)
    print(f"   Patrones encontrados: {len(best_patterns)}")
    
    over_patterns = [p for p in best_patterns if p.pick == 'OVER']
    under_patterns = [p for p in best_patterns if p.pick == 'UNDER']
    
    print(f"   OVER: {len(over_patterns)}")
    print(f"   UNDER: {len(under_patterns)}")
    print()
    
    print("🎯 TOP 10 PATRONES:")
    for i, p in enumerate(best_patterns[:10], 1):
        print(f"   {i}. [{p.name}] {p.pick}: {p.accuracy():.1f}% (n={p.total})")
    
    # Guardar
    results = {
        'timestamp': datetime.now().isoformat(),
        'version': '1.0',
        'total_matches': len(all_matches),
        'total_patterns': len(best_patterns),
        'patterns': [p.to_dict() for p in best_patterns[:50]]
    }
    
    path = RESULTS_DIR / 'ou_expert_patterns.json'
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ Guardado en: {path}")


if __name__ == '__main__':
    main()
