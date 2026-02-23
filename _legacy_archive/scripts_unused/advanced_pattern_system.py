# scripts/advanced_pattern_system.py
"""
SISTEMA AVANZADO DE PATRONES GANADORES
======================================
Analiza:
1. Coberturas de handicap previas (H2H y partidos anteriores)
2. Correlaciones entre resultados
3. Rachas y series de resultados
4. Combinaciones de múltiples condiciones
5. Auto-optimización genética de parámetros

Objetivo: Encontrar patrones con >80% de precisión
"""

import json
import random
import copy
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import itertools

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


# ==================== UTILIDADES ====================

def parse_score(score_str) -> Optional[Tuple[int, int]]:
    """Parsea score como '2:1' -> (2, 1)"""
    if not score_str or ':' not in str(score_str):
        return None
    try:
        parts = str(score_str).replace('-', ':').split(':')
        return int(parts[0]), int(parts[1])
    except:
        return None


def get_ah_cover_result(home_goals: int, away_goals: int, ah_line: float, perspective: str = 'home') -> str:
    """
    Calcula si cubrió el handicap.
    perspective: 'home' o 'away' - desde qué perspectiva evaluar
    Retorna: 'COVER', 'NO_COVER', o 'PUSH'
    """
    if perspective == 'home':
        adjusted = (home_goals - away_goals) - ah_line
    else:  # away perspective
        adjusted = (away_goals - home_goals) + ah_line
    
    if adjusted > 0.25:
        return 'COVER'
    elif adjusted < -0.25:
        return 'NO_COVER'
    return 'PUSH'


def get_ou_result(home_goals: int, away_goals: int, ou_line: float) -> str:
    """Calcula resultado Over/Under."""
    total = home_goals + away_goals
    if total > ou_line + 0.25:
        return 'OVER'
    elif total < ou_line - 0.25:
        return 'UNDER'
    return 'PUSH'


def get_ah_winner(home_goals: int, away_goals: int, ah_line: float) -> str:
    """Retorna 'LOCAL', 'VISITA', o 'PUSH'."""
    adjusted = (home_goals - away_goals) - ah_line
    if adjusted > 0.25:
        return 'LOCAL'
    elif adjusted < -0.25:
        return 'VISITA'
    return 'PUSH'


# ==================== EXTRACCIÓN DE FEATURES ====================

def extract_advanced_features(match: Dict) -> Dict:
    """
    Extrae features avanzadas basadas en coberturas de handicap.
    """
    features = {}
    
    # 1. Datos básicos del partido actual
    main_odds = match.get('main_match_odds') or {}
    try:
        ah_line = float(main_odds.get('ah_linea', 0) or 0)
        ou_line = float(main_odds.get('goals_linea', 2.5) or 2.5)
    except:
        ah_line, ou_line = 0, 2.5
    
    features['ah_line'] = ah_line
    features['ou_line'] = ou_line
    features['ha_fav'] = 'LOCAL' if ah_line > 0 else ('VISITA' if ah_line < 0 else 'NEUTRO')
    
    # 2. Prev Home - ¿El LOCAL cubrió su último partido de local?
    prev_home = match.get('last_home_match') or {}
    features['prev_home_cover'] = None
    features['prev_home_ou'] = None
    if prev_home.get('score'):
        parsed = parse_score(prev_home['score'])
        if parsed:
            try:
                prev_ah = float(prev_home.get('handicap_line_raw', 0) or 0)
                features['prev_home_cover'] = get_ah_cover_result(parsed[0], parsed[1], prev_ah, 'home')
                total = parsed[0] + parsed[1]
                features['prev_home_ou'] = 'OVER' if total > 2.5 else 'UNDER'
            except:
                pass
    
    # 3. Prev Away - ¿El VISITANTE cubrió su último partido de visitante?
    prev_away = match.get('last_away_match') or {}
    features['prev_away_cover'] = None
    features['prev_away_ou'] = None
    if prev_away.get('score'):
        parsed = parse_score(prev_away['score'])
        if parsed:
            try:
                prev_ah = float(prev_away.get('handicap_line_raw', 0) or 0)
                features['prev_away_cover'] = get_ah_cover_result(parsed[0], parsed[1], prev_ah, 'away')
                total = parsed[0] + parsed[1]
                features['prev_away_ou'] = 'OVER' if total > 2.5 else 'UNDER'
            except:
                pass
    
    # 4. H2H Stadium - ¿El LOCAL cubrió en el último H2H en este estadio?
    h2h_stadium = match.get('h2h_stadium') or {}
    features['h2h_stadium_cover'] = None
    features['h2h_stadium_ou'] = None
    if h2h_stadium.get('score') or h2h_stadium.get('goles_home') is not None:
        try:
            if h2h_stadium.get('goles_home') is not None:
                h_goals = int(h2h_stadium['goles_home'])
                a_goals = int(h2h_stadium['goles_away'])
            else:
                parsed = parse_score(h2h_stadium['score'])
                if parsed:
                    h_goals, a_goals = parsed
                else:
                    h_goals, a_goals = None, None
            
            if h_goals is not None:
                h2h_ah = float(h2h_stadium.get('handicap', 0) or 0)
                features['h2h_stadium_cover'] = get_ah_cover_result(h_goals, a_goals, h2h_ah, 'home')
                features['h2h_stadium_ou'] = 'OVER' if h_goals + a_goals > 2.5 else 'UNDER'
        except:
            pass
    
    # 5. H2H General
    h2h_general = match.get('h2h_general') or {}
    features['h2h_general_cover'] = None
    features['h2h_general_ou'] = None
    if h2h_general.get('score') or h2h_general.get('goles_home') is not None:
        try:
            if h2h_general.get('goles_home') is not None:
                h_goals = int(h2h_general['goles_home'])
                a_goals = int(h2h_general['goles_away'])
            else:
                parsed = parse_score(h2h_general['score'])
                if parsed:
                    h_goals, a_goals = parsed
                else:
                    h_goals, a_goals = None, None
            
            if h_goals is not None:
                h2h_ah = float(h2h_general.get('handicap', 0) or 0)
                features['h2h_general_cover'] = get_ah_cover_result(h_goals, a_goals, h2h_ah, 'home')
                features['h2h_general_ou'] = 'OVER' if h_goals + a_goals > 2.5 else 'UNDER'
        except:
            pass
    
    # 6. H2H Col3
    h2h_col3 = match.get('h2h_col3') or {}
    features['h2h_col3_cover'] = None
    features['h2h_col3_ou'] = None
    if h2h_col3.get('status') == 'found':
        try:
            h_goals = int(h2h_col3.get('goles_home', 0))
            a_goals = int(h2h_col3.get('goles_away', 0))
            h2h_ah = float(h2h_col3.get('handicap', 0) or 0)
            features['h2h_col3_cover'] = get_ah_cover_result(h_goals, a_goals, h2h_ah, 'home')
            features['h2h_col3_ou'] = 'OVER' if h_goals + a_goals > 2.5 else 'UNDER'
        except:
            pass
    
    # 7. Comparativas Indirectas
    comp = match.get('comparativas_indirectas') or {}
    
    # Indirecta Local
    ind_left = comp.get('left') or {}
    features['ind_left_cover'] = None
    features['ind_left_ou'] = None
    if ind_left.get('score'):
        parsed = parse_score(ind_left['score'])
        if parsed:
            try:
                ind_ah = float(ind_left.get('ah_line', 0) or ind_left.get('ah', 0) or 0)
                localia = ind_left.get('localia', 'H')
                perspective = 'home' if localia == 'H' else 'away'
                features['ind_left_cover'] = get_ah_cover_result(parsed[0], parsed[1], ind_ah, perspective)
                features['ind_left_ou'] = 'OVER' if parsed[0] + parsed[1] > 2.5 else 'UNDER'
            except:
                pass
    
    # Indirecta Visitante
    ind_right = comp.get('right') or {}
    features['ind_right_cover'] = None
    features['ind_right_ou'] = None
    if ind_right.get('score'):
        parsed = parse_score(ind_right['score'])
        if parsed:
            try:
                ind_ah = float(ind_right.get('ah_line', 0) or ind_right.get('ah', 0) or 0)
                localia = ind_right.get('localia', 'A')
                perspective = 'away' if localia == 'A' else 'home'
                features['ind_right_cover'] = get_ah_cover_result(parsed[0], parsed[1], ind_ah, perspective)
                features['ind_right_ou'] = 'OVER' if parsed[0] + parsed[1] > 2.5 else 'UNDER'
            except:
                pass
    
    # 8. Contadores de coberturas
    cover_sources = ['prev_home_cover', 'prev_away_cover', 'h2h_stadium_cover', 
                     'h2h_general_cover', 'h2h_col3_cover', 'ind_left_cover', 'ind_right_cover']
    
    local_covers = sum(1 for s in cover_sources if features.get(s) == 'COVER')
    local_no_covers = sum(1 for s in cover_sources if features.get(s) == 'NO_COVER')
    valid_sources = sum(1 for s in cover_sources if features.get(s) is not None)
    
    features['local_covers'] = local_covers
    features['local_no_covers'] = local_no_covers
    features['valid_cover_sources'] = valid_sources
    features['cover_ratio'] = local_covers / valid_sources if valid_sources > 0 else 0.5
    
    # 9. Contadores O/U
    ou_sources = ['prev_home_ou', 'prev_away_ou', 'h2h_stadium_ou', 
                  'h2h_general_ou', 'h2h_col3_ou', 'ind_left_ou', 'ind_right_ou']
    
    overs = sum(1 for s in ou_sources if features.get(s) == 'OVER')
    unders = sum(1 for s in ou_sources if features.get(s) == 'UNDER')
    valid_ou = sum(1 for s in ou_sources if features.get(s) is not None)
    
    features['overs'] = overs
    features['unders'] = unders
    features['valid_ou_sources'] = valid_ou
    features['over_ratio'] = overs / valid_ou if valid_ou > 0 else 0.5
    
    # 10. Consistencia (¿todos apuntan al mismo lado?)
    features['all_covers'] = local_covers == valid_sources and valid_sources >= 3
    features['all_no_covers'] = local_no_covers == valid_sources and valid_sources >= 3
    features['all_overs'] = overs == valid_ou and valid_ou >= 3
    features['all_unders'] = unders == valid_ou and valid_ou >= 3
    
    # 11. Rankings
    home_standings = match.get('home_standings') or {}
    away_standings = match.get('away_standings') or {}
    try:
        home_rank = int(home_standings.get('ranking', 0) or 0)
        away_rank = int(away_standings.get('ranking', 0) or 0)
        features['ranking_diff'] = home_rank - away_rank
        features['has_rankings'] = bool(home_rank and away_rank)
    except:
        features['ranking_diff'] = 0
        features['has_rankings'] = False
    
    return features


# ==================== REGLAS Y PATRONES ====================

class Rule:
    """Representa una regla de predicción."""
    def __init__(self, conditions: List[Tuple[str, str, any]], prediction: str):
        """
        conditions: lista de (feature_name, operator, value)
        prediction: 'LOCAL', 'VISITA', 'OVER', 'UNDER'
        """
        self.conditions = conditions
        self.prediction = prediction
        self.stats = {'total': 0, 'correct': 0}
    
    def matches(self, features: Dict) -> bool:
        """Evalúa si las features cumplen todas las condiciones."""
        for feat_name, op, value in self.conditions:
            feat_val = features.get(feat_name)
            if feat_val is None:
                return False
            
            if op == '==':
                if feat_val != value:
                    return False
            elif op == '>=':
                if feat_val < value:
                    return False
            elif op == '<=':
                if feat_val > value:
                    return False
            elif op == '>':
                if feat_val <= value:
                    return False
            elif op == '<':
                if feat_val >= value:
                    return False
            elif op == 'in':
                if feat_val not in value:
                    return False
        
        return True
    
    def accuracy(self) -> float:
        if self.stats['total'] == 0:
            return 0
        return self.stats['correct'] / self.stats['total'] * 100
    
    def __str__(self):
        conds = ' AND '.join([f"{c[0]}{c[1]}{c[2]}" for c in self.conditions])
        return f"IF {conds} THEN {self.prediction} (Acc: {self.accuracy():.1f}%, N={self.stats['total']})"


def generate_candidate_rules() -> List[Rule]:
    """Genera reglas candidatas basadas en diferentes hipótesis."""
    rules = []
    
    # === REGLAS PARA HANDICAP ===
    
    # Regla 1: Todos los H2H cubrieron -> LOCAL cubre
    rules.append(Rule([
        ('h2h_stadium_cover', '==', 'COVER'),
        ('h2h_general_cover', '==', 'COVER'),
        ('h2h_col3_cover', '==', 'COVER'),
    ], 'LOCAL'))
    
    # Regla 2: Todos los H2H NO cubrieron -> VISITA cubre
    rules.append(Rule([
        ('h2h_stadium_cover', '==', 'NO_COVER'),
        ('h2h_general_cover', '==', 'NO_COVER'),
        ('h2h_col3_cover', '==', 'NO_COVER'),
    ], 'VISITA'))
    
    # Regla 3: Prev Home + Prev Away ambos cubrieron -> LOCAL
    rules.append(Rule([
        ('prev_home_cover', '==', 'COVER'),
        ('prev_away_cover', '==', 'COVER'),
        ('ha_fav', '==', 'LOCAL'),
    ], 'LOCAL'))
    
    # Regla 4: Prev Home + Prev Away ambos NO cubrieron -> VISITA
    rules.append(Rule([
        ('prev_home_cover', '==', 'NO_COVER'),
        ('prev_away_cover', '==', 'NO_COVER'),
        ('ha_fav', '==', 'VISITA'),
    ], 'VISITA'))
    
    # Regla 5: Alta consistencia de coberturas LOCAL (>=5 de 7)
    for threshold in [5, 6, 7]:
        rules.append(Rule([
            ('local_covers', '>=', threshold),
            ('valid_cover_sources', '>=', threshold),
        ], 'LOCAL'))
    
    # Regla 6: Alta consistencia de NO coberturas (>=5 de 7)
    for threshold in [5, 6, 7]:
        rules.append(Rule([
            ('local_no_covers', '>=', threshold),
            ('valid_cover_sources', '>=', threshold),
        ], 'VISITA'))
    
    # Regla 7: HA favorito + H2H Col3 cubrió + Prev cubrió
    rules.append(Rule([
        ('ha_fav', '==', 'LOCAL'),
        ('h2h_col3_cover', '==', 'COVER'),
        ('prev_home_cover', '==', 'COVER'),
    ], 'LOCAL'))
    
    rules.append(Rule([
        ('ha_fav', '==', 'VISITA'),
        ('h2h_col3_cover', '==', 'NO_COVER'),
        ('prev_away_cover', '==', 'COVER'),
    ], 'VISITA'))
    
    # Regla 8: Ranking muy favorable + coberturas
    rules.append(Rule([
        ('ranking_diff', '<', -5),
        ('has_rankings', '==', True),
        ('local_covers', '>=', 3),
    ], 'LOCAL'))
    
    rules.append(Rule([
        ('ranking_diff', '>', 5),
        ('has_rankings', '==', True),
        ('local_no_covers', '>=', 3),
    ], 'VISITA'))
    
    # Regla 9: Cover ratio extremo
    rules.append(Rule([
        ('cover_ratio', '>=', 0.8),
        ('valid_cover_sources', '>=', 5),
    ], 'LOCAL'))
    
    rules.append(Rule([
        ('cover_ratio', '<=', 0.2),
        ('valid_cover_sources', '>=', 5),
    ], 'VISITA'))
    
    # === REGLAS PARA OVER/UNDER ===
    
    # Regla 10: Todos los partidos previos fueron OVER
    rules.append(Rule([
        ('all_overs', '==', True),
        ('valid_ou_sources', '>=', 4),
    ], 'OVER'))
    
    # Regla 11: Todos los partidos previos fueron UNDER
    rules.append(Rule([
        ('all_unders', '==', True),
        ('valid_ou_sources', '>=', 4),
    ], 'UNDER'))
    
    # Regla 12: Over ratio extremo
    for threshold in [0.8, 0.85, 0.9]:
        rules.append(Rule([
            ('over_ratio', '>=', threshold),
            ('valid_ou_sources', '>=', 5),
        ], 'OVER'))
    
    for threshold in [0.2, 0.15, 0.1]:
        rules.append(Rule([
            ('over_ratio', '<=', threshold),
            ('valid_ou_sources', '>=', 5),
        ], 'UNDER'))
    
    # Regla 13: H2H todos OVER/UNDER
    rules.append(Rule([
        ('h2h_stadium_ou', '==', 'OVER'),
        ('h2h_general_ou', '==', 'OVER'),
        ('h2h_col3_ou', '==', 'OVER'),
    ], 'OVER'))
    
    rules.append(Rule([
        ('h2h_stadium_ou', '==', 'UNDER'),
        ('h2h_general_ou', '==', 'UNDER'),
        ('h2h_col3_ou', '==', 'UNDER'),
    ], 'UNDER'))
    
    return rules


# ==================== EVALUACIÓN ====================

def evaluate_rules(matches: List[Dict], rules: List[Rule]) -> List[Rule]:
    """Evalúa todas las reglas contra los partidos."""
    
    for match in matches:
        # Obtener resultado real
        score = match.get('final_score') or match.get('score')
        parsed = parse_score(score)
        if not parsed:
            continue
        
        home_goals, away_goals = parsed
        
        main_odds = match.get('main_match_odds') or {}
        try:
            ah_line = float(main_odds.get('ah_linea', 0) or 0)
            ou_line = float(main_odds.get('goals_linea', 2.5) or 2.5)
        except:
            continue
        
        # Calcular resultados reales
        ah_winner = get_ah_winner(home_goals, away_goals, ah_line)
        ou_result = get_ou_result(home_goals, away_goals, ou_line)
        
        if ah_winner == 'PUSH' and ou_result == 'PUSH':
            continue
        
        # Extraer features
        features = extract_advanced_features(match)
        
        # Evaluar cada regla
        for rule in rules:
            if not rule.matches(features):
                continue
            
            rule.stats['total'] += 1
            
            # Verificar si acertó
            if rule.prediction in ['LOCAL', 'VISITA']:
                if ah_winner != 'PUSH' and rule.prediction == ah_winner:
                    rule.stats['correct'] += 1
            elif rule.prediction in ['OVER', 'UNDER']:
                if ou_result != 'PUSH' and rule.prediction == ou_result:
                    rule.stats['correct'] += 1
    
    return rules


# ==================== ALGORITMO GENÉTICO ====================

def mutate_rule(rule: Rule, all_features: List[str]) -> Rule:
    """Muta una regla aleatoriamente."""
    new_conditions = list(rule.conditions)
    mutation_type = random.choice(['add', 'remove', 'modify'])
    
    if mutation_type == 'add' and len(new_conditions) < 5:
        # Añadir nueva condición
        new_feat = random.choice(all_features)
        if 'cover' in new_feat:
            new_cond = (new_feat, '==', random.choice(['COVER', 'NO_COVER']))
        elif 'ou' in new_feat and 'ratio' not in new_feat:
            new_cond = (new_feat, '==', random.choice(['OVER', 'UNDER']))
        elif 'ratio' in new_feat:
            new_cond = (new_feat, random.choice(['>=', '<=']), random.uniform(0.3, 0.7))
        elif new_feat in ['local_covers', 'local_no_covers', 'overs', 'unders']:
            new_cond = (new_feat, '>=', random.randint(3, 6))
        else:
            return rule
        new_conditions.append(new_cond)
    
    elif mutation_type == 'remove' and len(new_conditions) > 1:
        new_conditions.pop(random.randint(0, len(new_conditions) - 1))
    
    elif mutation_type == 'modify' and new_conditions:
        idx = random.randint(0, len(new_conditions) - 1)
        feat, op, val = new_conditions[idx]
        if isinstance(val, (int, float)):
            val = val * random.uniform(0.8, 1.2)
        new_conditions[idx] = (feat, op, val)
    
    return Rule(new_conditions, rule.prediction)


def genetic_optimize(matches: List[Dict], generations: int = 50, population_size: int = 100) -> List[Rule]:
    """Optimiza reglas usando algoritmo genético."""
    
    print(f"\n🧬 Optimización genética ({generations} generaciones, población {population_size})")
    
    # Población inicial
    population = generate_candidate_rules()
    
    # Features disponibles
    all_features = [
        'prev_home_cover', 'prev_away_cover', 'h2h_stadium_cover', 
        'h2h_general_cover', 'h2h_col3_cover', 'ind_left_cover', 'ind_right_cover',
        'local_covers', 'local_no_covers', 'cover_ratio', 'valid_cover_sources',
        'prev_home_ou', 'prev_away_ou', 'h2h_stadium_ou', 'h2h_general_ou', 
        'h2h_col3_ou', 'overs', 'unders', 'over_ratio', 'valid_ou_sources',
        'ha_fav', 'ranking_diff', 'has_rankings'
    ]
    
    # Añadir reglas aleatorias
    for _ in range(population_size - len(population)):
        n_conditions = random.randint(2, 4)
        conditions = []
        for _ in range(n_conditions):
            feat = random.choice(all_features)
            if 'cover' in feat and 'local' not in feat:
                cond = (feat, '==', random.choice(['COVER', 'NO_COVER']))
            elif 'ou' in feat and 'ratio' not in feat and 'valid' not in feat:
                cond = (feat, '==', random.choice(['OVER', 'UNDER']))
            elif 'ratio' in feat:
                cond = (feat, random.choice(['>=', '<=']), round(random.uniform(0.3, 0.7), 2))
            elif feat in ['local_covers', 'local_no_covers', 'overs', 'unders', 'valid_cover_sources', 'valid_ou_sources']:
                cond = (feat, '>=', random.randint(3, 6))
            elif feat == 'ha_fav':
                cond = (feat, '==', random.choice(['LOCAL', 'VISITA']))
            elif feat == 'ranking_diff':
                cond = (feat, random.choice(['>', '<']), random.choice([-5, -3, 3, 5]))
            else:
                continue
            conditions.append(cond)
        
        if conditions:
            pred = random.choice(['LOCAL', 'VISITA', 'OVER', 'UNDER'])
            population.append(Rule(conditions, pred))
    
    best_ever = None
    best_accuracy = 0
    
    for gen in range(generations):
        # Evaluar población
        for rule in population:
            rule.stats = {'total': 0, 'correct': 0}
        population = evaluate_rules(matches, population)
        
        # Filtrar reglas con suficientes muestras
        valid_rules = [r for r in population if r.stats['total'] >= 20]
        
        if not valid_rules:
            # Mutar población si no hay reglas válidas
            population = [mutate_rule(r, all_features) for r in population]
            continue
        
        # Ordenar por precisión
        valid_rules.sort(key=lambda r: -r.accuracy())
        
        # Guardar mejor
        if valid_rules[0].accuracy() > best_accuracy and valid_rules[0].stats['total'] >= 30:
            best_accuracy = valid_rules[0].accuracy()
            best_ever = copy.deepcopy(valid_rules[0])
            print(f"  Gen {gen+1}: Nueva mejor regla - {best_accuracy:.1f}% ({best_ever.stats['total']} muestras)")
        
        # Selección: mantener top 30%
        survivors = valid_rules[:max(10, len(valid_rules) // 3)]
        
        # Reproducción
        new_population = list(survivors)
        while len(new_population) < population_size:
            parent = random.choice(survivors)
            child = mutate_rule(parent, all_features)
            new_population.append(child)
        
        population = new_population
    
    # Evaluación final
    for rule in population:
        rule.stats = {'total': 0, 'correct': 0}
    population = evaluate_rules(matches, population)
    
    # Filtrar y ordenar
    final_rules = [r for r in population if r.stats['total'] >= 30 and r.accuracy() >= 55]
    final_rules.sort(key=lambda r: -r.accuracy())
    
    return final_rules[:20]  # Top 20 reglas


# ==================== MAIN ====================

def load_all_matches() -> List[Dict]:
    all_matches = []
    for data_file in DATA_FILES:
        if not data_file.exists():
            continue
        try:
            with open(data_file, 'r', encoding='utf-8') as f:
                all_matches.extend(json.load(f))
        except:
            continue
    return all_matches


def main():
    print("=" * 70)
    print("🎯 SISTEMA AVANZADO DE PATRONES GANADORES")
    print("=" * 70)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Cargar datos
    print("📂 Cargando partidos...")
    matches = load_all_matches()
    
    # Filtrar partidos con resultado
    valid_matches = []
    for m in matches:
        score = m.get('final_score') or m.get('score')
        if parse_score(score):
            valid_matches.append(m)
    
    print(f"   Total: {len(matches)}, Con resultado: {len(valid_matches)}")
    
    # Generar y evaluar reglas predefinidas
    print("\n" + "=" * 70)
    print("📊 EVALUANDO REGLAS PREDEFINIDAS")
    print("=" * 70)
    
    rules = generate_candidate_rules()
    rules = evaluate_rules(valid_matches, rules)
    
    # Mostrar mejores reglas predefinidas
    good_rules = [r for r in rules if r.stats['total'] >= 30 and r.accuracy() >= 55]
    good_rules.sort(key=lambda r: -r.accuracy())
    
    print(f"\nReglas con >55% precisión (mínimo 30 muestras):")
    for rule in good_rules[:10]:
        emoji = "🔥" if rule.accuracy() >= 70 else "✅" if rule.accuracy() >= 60 else "📊"
        print(f"\n{emoji} {rule}")
    
    # Optimización genética
    print("\n" + "=" * 70)
    print("🧬 OPTIMIZACIÓN GENÉTICA")
    print("=" * 70)
    
    optimized_rules = genetic_optimize(valid_matches, generations=100, population_size=200)
    
    # Mostrar mejores reglas optimizadas
    print(f"\n✨ TOP REGLAS OPTIMIZADAS:")
    for i, rule in enumerate(optimized_rules[:10], 1):
        emoji = "🔥" if rule.accuracy() >= 70 else "✅" if rule.accuracy() >= 60 else "📊"
        print(f"\n{i}. {emoji} {rule}")
    
    # Guardar resultados
    results = {
        'timestamp': datetime.now().isoformat(),
        'total_matches': len(valid_matches),
        'predefined_rules': [
            {
                'conditions': [(c[0], c[1], str(c[2])) for c in r.conditions],
                'prediction': r.prediction,
                'accuracy': r.accuracy(),
                'total': r.stats['total'],
                'correct': r.stats['correct']
            }
            for r in good_rules[:20]
        ],
        'optimized_rules': [
            {
                'conditions': [(c[0], c[1], str(c[2]) if isinstance(c[2], float) else c[2]) for c in r.conditions],
                'prediction': r.prediction,
                'accuracy': r.accuracy(),
                'total': r.stats['total'],
                'correct': r.stats['correct']
            }
            for r in optimized_rules[:20]
        ]
    }
    
    results_path = RESULTS_DIR / 'advanced_rules.json'
    with open(results_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n💾 Resultados guardados en: {results_path}")
    
    # Resumen final
    print("\n" + "=" * 70)
    print("📌 RESUMEN FINAL")
    print("=" * 70)
    
    all_good = good_rules + optimized_rules
    best_ah = [r for r in all_good if r.prediction in ['LOCAL', 'VISITA']]
    best_ou = [r for r in all_good if r.prediction in ['OVER', 'UNDER']]
    
    if best_ah:
        best_ah.sort(key=lambda r: -r.accuracy())
        print(f"\n🏆 MEJOR REGLA AH: {best_ah[0]}")
    
    if best_ou:
        best_ou.sort(key=lambda r: -r.accuracy())
        print(f"\n🏆 MEJOR REGLA O/U: {best_ou[0]}")
    
    print("\n✅ Sistema avanzado completado!")


if __name__ == '__main__':
    main()
