# scripts/expert_ah_1_5.py
"""
SISTEMA EXPERTO AH 1.5 - PATRONES PROFESIONALES
=================================================
Sistema especializado en handicap 1.5 que analiza patrones COMPLEJOS:

1. RENDIMIENTO LOCAL VS FUERA
   - ¿El local es mejor en casa que fuera?
   - ¿El visitante es mejor fuera que en casa?

2. ATAQUES PELIGROSOS COMPARATIVOS
   - Edge de ataques en cada fuente
   - ¿Quién domina las stats incluso si pierde?

3. RESULTADOS PREVIOS CON MISMO O SIMILAR HANDICAP
   - ¿Cómo terminó el prev home con AH similar?
   - ¿El resultado fue por cuántos goles de margen?
   - ¿Cubrió o no cubrió? ¿Por margen amplio o justo?

4. COMPARATIVAS INDIRECTAS CRUZADAS
   - ¿Cómo le fue a cada equipo contra rivales comunes?
   - ¿Cubrieron el handicap en esas situaciones?

5. ENTENDER EL PORQUÉ DE LA CUOTA
   - ¿Qué dice el ranking sobre 1.5 goles?
   - ¿La cuota está justificada por el rendimiento?

SISTEMA RE-ENTRENABLE:
- Guarda predicciones hechas
- Cuando conocemos el resultado, evalúa el acierto
- Si falla, analiza QUÉ variable no consideró
- Añade nuevas variables dinámicamente
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

# Solo usamos data_ah_1.5.json para el experto 1.5
DATA_FILE = DATA_DIR / 'data_ah_1.5.json'
PREDICTIONS_FILE = RESULTS_DIR / 'expert_ah_1_5_predictions.json'
LEARNINGS_FILE = RESULTS_DIR / 'expert_ah_1_5_learnings.json'

GENERATIONS = 5000  # Más generaciones para mayor precisión
POPULATION_SIZE = 8000  # Mayor población
MIN_SAMPLES = 20  # Mínimo de muestras para validar regla
MIN_ACCURACY = 85  # Buscamos >85% - casi infalible


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
    """Determina quién cubre el handicap 1.5."""
    adjusted = (home_goals - away_goals) - ah_line
    if adjusted > 0.25:
        return 'LOCAL'  # Local cubre (gana por más de 1.5)
    elif adjusted < -0.25:
        return 'VISITA'  # Visita cubre
    return 'PUSH'


def get_margin(home_goals, away_goals) -> int:
    """Margen de victoria."""
    return abs(home_goals - away_goals)


def get_cover_type(home_goals, away_goals, ah_line, is_home_team) -> str:
    """
    Tipo de cobertura:
    - COVER_EASY: Cubrió con margen amplio
    - COVER_TIGHT: Cubrió justo
    - NO_COVER_CLOSE: No cubrió pero estuvo cerca
    - NO_COVER_CLEAR: No cubrió claramente
    """
    if is_home_team:
        adjusted = (home_goals - away_goals) - ah_line
    else:
        adjusted = (away_goals - home_goals) + ah_line
    
    if adjusted > 1:
        return 'COVER_EASY'
    elif adjusted > 0.25:
        return 'COVER_TIGHT'
    elif adjusted > -0.5:
        return 'NO_COVER_CLOSE'
    else:
        return 'NO_COVER_CLEAR'


def extract_professional_features(match: Dict) -> Dict:
    """
    Extrae features profesionales basadas en el análisis complejo del usuario.
    """
    f = {}
    
    main_odds = match.get('main_match_odds') or {}
    try:
        ah_line = float(main_odds.get('ah_linea', 1.5) or 1.5)
        ou_line = float(main_odds.get('goals_linea', 2.5) or 2.5)
    except:
        ah_line, ou_line = 1.5, 2.5
    
    f['ah_line'] = ah_line
    f['ou_line'] = ou_line
    f['local_fav'] = ah_line > 0
    f['away_fav'] = ah_line < 0
    
    # ===== 1. RANKINGS Y DIFERENCIA =====
    home_standings = match.get('home_standings') or {}
    away_standings = match.get('away_standings') or {}
    
    try:
        f['home_rank'] = int(home_standings.get('ranking', 0) or 0)
        f['away_rank'] = int(away_standings.get('ranking', 0) or 0)
        f['rank_diff'] = f['home_rank'] - f['away_rank']
        f['rank_diff_big'] = abs(f['rank_diff']) >= 5
        f['rank_diff_huge'] = abs(f['rank_diff']) >= 10
        f['local_better_rank'] = f['home_rank'] > 0 and f['home_rank'] < f['away_rank']
        f['visita_better_rank'] = f['away_rank'] > 0 and f['away_rank'] < f['home_rank']
    except:
        f['home_rank'] = f['away_rank'] = f['rank_diff'] = 0
    
    # ===== 2. RENDIMIENTO LOCAL VS FUERA (W-D-L) =====
    # Global
    try:
        f['home_wins_global'] = int(home_standings.get('wins', 0) or 0)
        f['home_draws_global'] = int(home_standings.get('draws', 0) or 0)
        f['home_losses_global'] = int(home_standings.get('losses', 0) or 0)
        total_home = f['home_wins_global'] + f['home_draws_global'] + f['home_losses_global']
        f['home_win_rate_global'] = f['home_wins_global'] / total_home if total_home > 0 else 0.5
        
        f['away_wins_global'] = int(away_standings.get('wins', 0) or 0)
        f['away_draws_global'] = int(away_standings.get('draws', 0) or 0)
        f['away_losses_global'] = int(away_standings.get('losses', 0) or 0)
        total_away = f['away_wins_global'] + f['away_draws_global'] + f['away_losses_global']
        f['away_win_rate_global'] = f['away_wins_global'] / total_away if total_away > 0 else 0.5
    except:
        pass
    
    # Específico (casa/fuera)
    try:
        # Local en casa
        home_spec = home_standings.get('specific', {}) or {}
        f['home_wins_home'] = int(home_spec.get('wins', 0) or 0)
        f['home_draws_home'] = int(home_spec.get('draws', 0) or 0)
        f['home_losses_home'] = int(home_spec.get('losses', 0) or 0)
        total = f['home_wins_home'] + f['home_draws_home'] + f['home_losses_home']
        f['home_win_rate_home'] = f['home_wins_home'] / total if total > 0 else 0.5
        f['home_unbeaten_home'] = f['home_losses_home'] == 0 and total > 0
        
        # Visitante fuera
        away_spec = away_standings.get('specific', {}) or {}
        f['away_wins_away'] = int(away_spec.get('wins', 0) or 0)
        f['away_draws_away'] = int(away_spec.get('draws', 0) or 0)
        f['away_losses_away'] = int(away_spec.get('losses', 0) or 0)
        total = f['away_wins_away'] + f['away_draws_away'] + f['away_losses_away']
        f['away_win_rate_away'] = f['away_wins_away'] / total if total > 0 else 0.5
        f['away_unbeaten_away'] = f['away_losses_away'] == 0 and total > 0
    except:
        pass
    
    # ¿El local es mejor en casa que fuera?
    f['home_better_at_home'] = f.get('home_win_rate_home', 0) > f.get('home_win_rate_global', 0)
    # ¿El visitante es mejor fuera que en casa?
    f['away_better_away'] = f.get('away_win_rate_away', 0) >= f.get('away_win_rate_global', 0)
    
    # ===== 3. ANÁLISIS PREV HOME (resultado y stats) =====
    prev_home = match.get('last_home_match') or {}
    if prev_home:
        score = parse_score(prev_home.get('score'))
        if score:
            f['prev_home_goals'] = score[0] + score[1]
            f['prev_home_margin'] = score[0] - score[1]
            f['prev_home_won'] = score[0] > score[1]
            f['prev_home_lost'] = score[0] < score[1]
            f['prev_home_drew'] = score[0] == score[1]
            
            # AH del partido previo
            try:
                prev_ah = float(prev_home.get('handicap_line_raw') or prev_home.get('handicap') or 0)
                f['prev_home_ah'] = prev_ah
                f['prev_home_ah_similar'] = abs(prev_ah - ah_line) < 0.5  # Similar al actual
                f['prev_home_cover_type'] = get_cover_type(score[0], score[1], prev_ah, True)
                f['prev_home_covered'] = f['prev_home_cover_type'].startswith('COVER')
                f['prev_home_covered_easy'] = f['prev_home_cover_type'] == 'COVER_EASY'
            except:
                pass
        
        # Stats del prev home
        stats = parse_stats_rows(prev_home.get('stats_rows', []))
        if 'Ataques Peligrosos' in stats:
            f['prev_home_danger_h'] = stats['Ataques Peligrosos']['home']
            f['prev_home_danger_a'] = stats['Ataques Peligrosos']['away']
            f['prev_home_danger_edge'] = f['prev_home_danger_h'] - f['prev_home_danger_a']
            f['prev_home_dominated_attacks'] = f['prev_home_danger_edge'] > 10
        if 'Tiros a Puerta' in stats:
            f['prev_home_sot_edge'] = stats['Tiros a Puerta']['home'] - stats['Tiros a Puerta']['away']
    
    # ===== 4. ANÁLISIS PREV AWAY (resultado y stats) =====
    prev_away = match.get('last_away_match') or {}
    if prev_away:
        score = parse_score(prev_away.get('score'))
        if score:
            f['prev_away_goals'] = score[0] + score[1]
            f['prev_away_margin'] = score[1] - score[0]  # Perspectiva visitante
            f['prev_away_won'] = score[1] > score[0]  # Visitante ganó
            f['prev_away_lost'] = score[1] < score[0]  # Visitante perdió
            f['prev_away_drew'] = score[0] == score[1]
            
            # ¿Perdió pero por poco? (Ej: 2-1 = perdió solo por 1)
            if f['prev_away_lost']:
                f['prev_away_close_loss'] = abs(score[0] - score[1]) == 1
                f['prev_away_big_loss'] = abs(score[0] - score[1]) >= 2
            
            # AH del partido previo
            try:
                prev_ah = float(prev_away.get('handicap_line_raw') or prev_away.get('handicap') or 0)
                f['prev_away_ah'] = prev_ah
                f['prev_away_ah_similar'] = abs(prev_ah - ah_line) < 0.5
                f['prev_away_cover_type'] = get_cover_type(score[0], score[1], prev_ah, False)
                f['prev_away_covered'] = f['prev_away_cover_type'].startswith('COVER')
            except:
                pass
        
        # Stats
        stats = parse_stats_rows(prev_away.get('stats_rows', []))
        if 'Ataques Peligrosos' in stats:
            f['prev_away_danger_edge'] = stats['Ataques Peligrosos']['away'] - stats['Ataques Peligrosos']['home']
            f['prev_away_dominated_attacks'] = f['prev_away_danger_edge'] > 10
    
    # ===== 5. H2H COL3 (enfrentamiento directo previo) =====
    h2h_col3 = match.get('h2h_col3') or {}
    if h2h_col3.get('status') == 'found':
        try:
            h = int(h2h_col3.get('goles_home', 0) or 0)
            a = int(h2h_col3.get('goles_away', 0) or 0)
            f['h2h_col3_goals'] = h + a
            f['h2h_col3_margin'] = h - a
            f['h2h_col3_high_scoring'] = (h + a) >= 3
            f['h2h_col3_draw'] = h == a
            
            try:
                col3_ah = float(h2h_col3.get('handicap') or 0)
                f['h2h_col3_ah'] = col3_ah
                f['h2h_col3_cover_type'] = get_cover_type(h, a, col3_ah, True)
            except:
                pass
        except:
            pass
    
    # ===== 6. COMPARATIVAS INDIRECTAS (contra rivales comunes) =====
    comp = match.get('comparativas_indirectas') or {}
    
    # Ind Local
    ind_left = comp.get('left') or {}
    if ind_left:
        score = parse_score(ind_left.get('score'))
        if score:
            left_is_home = ind_left.get('localia') == 'H'
            local_goals = score[0] if left_is_home else score[1]
            opp_goals = score[1] if left_is_home else score[0]
            f['ind_left_margin'] = local_goals - opp_goals
            f['ind_left_won'] = local_goals > opp_goals
            f['ind_left_goals'] = score[0] + score[1]
            
            try:
                left_ah = float(ind_left.get('ah_line') or ind_left.get('ah') or 0)
                f['ind_left_ah'] = left_ah
                f['ind_left_cover_type'] = get_cover_type(score[0], score[1], left_ah, left_is_home)
                f['ind_left_covered'] = f['ind_left_cover_type'].startswith('COVER')
            except:
                pass
        
        stats = parse_stats_rows(ind_left.get('stats_rows', []))
        if 'Ataques Peligrosos' in stats:
            left_is_home = ind_left.get('localia') == 'H'
            if left_is_home:
                f['ind_left_danger_edge'] = stats['Ataques Peligrosos']['home'] - stats['Ataques Peligrosos']['away']
            else:
                f['ind_left_danger_edge'] = stats['Ataques Peligrosos']['away'] - stats['Ataques Peligrosos']['home']
    
    # Ind Visitante
    ind_right = comp.get('right') or {}
    if ind_right:
        score = parse_score(ind_right.get('score'))
        if score:
            right_is_home = ind_right.get('localia') != 'A'
            away_goals = score[1] if right_is_home else score[0]
            opp_goals = score[0] if right_is_home else score[1]
            f['ind_right_margin'] = away_goals - opp_goals  # Perspectiva visitante actual
            f['ind_right_won'] = away_goals > opp_goals
            f['ind_right_goals'] = score[0] + score[1]
            f['ind_right_big_win'] = f['ind_right_margin'] >= 2
            
            try:
                right_ah = float(ind_right.get('ah_line') or ind_right.get('ah') or 0)
                f['ind_right_ah'] = right_ah
                f['ind_right_cover_type'] = get_cover_type(score[0], score[1], right_ah, not right_is_home)
                f['ind_right_covered'] = f['ind_right_cover_type'].startswith('COVER')
            except:
                pass
    
    # ===== 7. PATRONES CRUZADOS (lo que el usuario describe) =====
    # "El último rival del local con el mismo handicap empató en casa contra el rival del visitante"
    # Esto requiere analizar si hay coincidencias en las indirectas
    
    # Patrón: Local no gana pero domina stats
    f['local_dominates_but_not_wins'] = (
        f.get('prev_home_dominated_attacks', False) and 
        not f.get('prev_home_won', False)
    )
    
    # Patrón: Visitante perdió pero por poco
    f['away_close_loss_pattern'] = (
        f.get('prev_away_lost', False) and 
        f.get('prev_away_close_loss', False)
    )
    
    # Patrón: Ind right ganó ampliamente (visitante cubre fácil)
    f['ind_right_domination'] = (
        f.get('ind_right_won', False) and 
        f.get('ind_right_big_win', False)
    )
    
    # ===== 8. MARKET ANALYSIS =====
    market = match.get('market_analysis_data') or {}
    stadium_m = market.get('stadium') or {}
    general_m = market.get('general') or {}
    
    f['h2h_stadium_covered'] = stadium_m.get('is_covered')
    f['h2h_general_covered'] = general_m.get('is_covered')
    f['h2h_both_covered'] = f['h2h_stadium_covered'] == True and f['h2h_general_covered'] == True
    
    if stadium_m.get('movement'):
        parts = stadium_m['movement'].replace('→', '->').split('->')
        if len(parts) == 2:
            try:
                before = float(parts[0].strip())
                after = float(parts[1].strip())
                f['line_change'] = after - before
                f['line_increased'] = after > before
                f['line_decreased'] = after < before
            except:
                pass
    
    # ===== 9. COMBINACIONES ESPECIALES =====
    # Patrón del usuario: local mejor en casa + domina ataques + visitante perdió por poco
    f['user_pattern_1'] = (
        f.get('home_better_at_home', False) and
        f.get('prev_home_dominated_attacks', False) and
        f.get('away_close_loss_pattern', False)
    )
    
    # Patrón: visitante dominante
    f['away_dominant_pattern'] = (
        f.get('visita_better_rank', False) and
        f.get('ind_right_domination', False) and
        f.get('prev_away_covered', False)
    )
    
    return f


# Features para el experto AH 1.5
EXPERT_FEATURES = [
    # Rankings
    'rank_diff', 'rank_diff_big', 'rank_diff_huge', 'local_better_rank', 'visita_better_rank',
    
    # Rendimiento casa/fuera
    'home_win_rate_home', 'home_unbeaten_home', 'home_better_at_home',
    'away_win_rate_away', 'away_unbeaten_away', 'away_better_away',
    
    # Prev Home
    'prev_home_won', 'prev_home_lost', 'prev_home_drew', 'prev_home_margin',
    'prev_home_covered', 'prev_home_covered_easy', 'prev_home_ah_similar',
    'prev_home_danger_edge', 'prev_home_dominated_attacks', 'prev_home_sot_edge',
    
    # Prev Away
    'prev_away_won', 'prev_away_lost', 'prev_away_drew', 'prev_away_margin',
    'prev_away_covered', 'prev_away_close_loss', 'prev_away_big_loss', 'prev_away_ah_similar',
    'prev_away_danger_edge', 'prev_away_dominated_attacks',
    
    # H2H Col3
    'h2h_col3_goals', 'h2h_col3_margin', 'h2h_col3_high_scoring', 'h2h_col3_draw',
    
    # Indirectas
    'ind_left_margin', 'ind_left_won', 'ind_left_covered', 'ind_left_danger_edge',
    'ind_right_margin', 'ind_right_won', 'ind_right_covered', 'ind_right_big_win',
    
    # Patrones complejos
    'local_dominates_but_not_wins', 'away_close_loss_pattern', 'ind_right_domination',
    'user_pattern_1', 'away_dominant_pattern',
    
    # Market
    'h2h_both_covered', 'line_change', 'line_increased', 'line_decreased',
    
    # Favorito
    'local_fav', 'away_fav',
]


class Rule:
    def __init__(self, conditions, prediction):
        self.conditions = conditions
        self.prediction = prediction  # 'LOCAL' o 'VISITA'
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
    
    def to_dict(self):
        return {
            'conditions': [(c[0], c[1], c[2]) for c in self.conditions],
            'prediction': self.prediction,
            'accuracy': round(self.accuracy(), 2),
            'samples': self.total
        }


def generate_condition(feat):
    if feat in ['local_fav', 'away_fav', 'local_better_rank', 'visita_better_rank',
                'home_unbeaten_home', 'home_better_at_home', 'away_unbeaten_away', 'away_better_away',
                'prev_home_won', 'prev_home_lost', 'prev_home_drew', 'prev_home_covered', 'prev_home_covered_easy',
                'prev_home_ah_similar', 'prev_home_dominated_attacks',
                'prev_away_won', 'prev_away_lost', 'prev_away_drew', 'prev_away_covered',
                'prev_away_close_loss', 'prev_away_big_loss', 'prev_away_ah_similar', 'prev_away_dominated_attacks',
                'h2h_col3_high_scoring', 'h2h_col3_draw', 'h2h_both_covered',
                'ind_left_won', 'ind_left_covered', 'ind_right_won', 'ind_right_covered', 'ind_right_big_win',
                'local_dominates_but_not_wins', 'away_close_loss_pattern', 'ind_right_domination',
                'user_pattern_1', 'away_dominant_pattern', 'rank_diff_big', 'rank_diff_huge',
                'line_increased', 'line_decreased']:
        return (feat, '==', True)
    elif 'margin' in feat:
        return (feat, random.choice(['>=', '<=', '>', '<']), random.choice([-2, -1, 0, 1, 2, 3]))
    elif 'edge' in feat:
        return (feat, random.choice(['>=', '<=', '>', '<']), round(random.uniform(-15, 15), 0))
    elif 'rate' in feat:
        return (feat, random.choice(['>=', '<=']), round(random.uniform(0.3, 0.7), 2))
    elif 'rank_diff' in feat:
        return (feat, random.choice(['>', '<', '>=', '<=']), random.choice([-8, -5, -3, 0, 3, 5, 8]))
    elif 'goals' in feat:
        return (feat, random.choice(['>=', '<=']), random.randint(1, 4))
    elif 'line_change' in feat:
        return (feat, random.choice(['>', '<']), round(random.uniform(-0.5, 0.5), 2))
    return None


def generate_random_rule(prediction):
    n = random.randint(3, 6)  # 3-6 condiciones para patrones complejos
    conditions = []
    used = set()
    for _ in range(n):
        feat = random.choice(EXPERT_FEATURES)
        if feat in used:
            continue
        used.add(feat)
        cond = generate_condition(feat)
        if cond:
            conditions.append(cond)
    return Rule(conditions, prediction) if len(conditions) >= 3 else None


def mutate(rule):
    new_conds = list(rule.conditions)
    action = random.choice(['add', 'remove', 'modify', 'replace'])
    
    if action == 'add' and len(new_conds) < 7:
        feat = random.choice(EXPERT_FEATURES)
        cond = generate_condition(feat)
        if cond and not any(c[0] == feat for c in new_conds):
            new_conds.append(cond)
    elif action == 'remove' and len(new_conds) > 3:
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
        feat = random.choice(EXPERT_FEATURES)
        cond = generate_condition(feat)
        if cond:
            new_conds[idx] = cond
    
    return Rule(new_conds, rule.prediction)


def load_matches():
    if not DATA_FILE.exists():
        print(f"❌ Archivo no encontrado: {DATA_FILE}")
        return []
    
    with open(DATA_FILE, 'r', encoding='utf-8') as f:
        matches = json.load(f)
    
    return [m for m in matches if parse_score(m.get('final_score') or m.get('score'))]


def main():
    print("=" * 70)
    print("🎯 EXPERTO AH 1.5 - PATRONES PROFESIONALES")
    print("=" * 70)
    print(f"Archivo: {DATA_FILE.name}")
    print(f"Generaciones: {GENERATIONS}")
    print(f"Población: {POPULATION_SIZE}")
    print(f"Min precisión: {MIN_ACCURACY}%")
    print(f"Min muestras: {MIN_SAMPLES}")
    print()
    
    matches = load_matches()
    print(f"📂 Partidos AH 1.5 con resultado: {len(matches)}")
    
    if len(matches) < 50:
        print("⚠️ Muy pocos partidos para entrenar")
        return
    
    # Crear población
    population = []
    for _ in range(POPULATION_SIZE // 2):
        for pred in ['LOCAL', 'VISITA']:
            r = generate_random_rule(pred)
            if r:
                population.append(r)
    
    print(f"   Población inicial: {len(population)}")
    print("\n" + "-" * 70)
    
    best_rules = []
    
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
                ah = float(main_odds.get('ah_linea', 1.5) or 1.5)
            except:
                continue
            
            ah_result = get_ah_winner(parsed[0], parsed[1], ah)
            if ah_result == 'PUSH':
                continue
            
            features = extract_professional_features(match)
            
            for rule in population:
                if not rule.matches(features):
                    continue
                
                rule.total += 1
                if rule.prediction == ah_result:
                    rule.correct += 1
        
        # Buscar reglas excelentes
        for r in population:
            if r.total < MIN_SAMPLES:
                continue
            
            acc = r.accuracy()
            
            if acc >= MIN_ACCURACY:
                is_new = not any(
                    abs(existing.accuracy() - acc) < 2 and
                    existing.prediction == r.prediction and
                    len(existing.conditions) == len(r.conditions)
                    for existing in best_rules
                )
                if is_new and len(best_rules) < 100:
                    best_rules.append(copy.deepcopy(r))
                    emoji = "🔥🔥🔥" if acc >= 95 else "🔥🔥" if acc >= 90 else "🔥"
                    print(f"\n{emoji} Gen {gen+1} - {r.prediction}: {acc:.1f}% (n={r.total})")
                    for c in r.conditions:
                        print(f"   {c[0]} {c[1]} {c[2]}")
        
        if (gen + 1) % 500 == 0:
            print(f"\n📊 Gen {gen+1}/{GENERATIONS} - {len(best_rules)} reglas encontradas")
            if best_rules:
                top = max(best_rules, key=lambda r: r.accuracy())
                print(f"   Top: {top.prediction} {top.accuracy():.1f}% (n={top.total})")
        
        # Evolución
        valid = [r for r in population if r.total >= 10 and r.accuracy() >= 50]
        if not valid:
            valid = population[:300]
        
        valid.sort(key=lambda r: -r.accuracy())
        survivors = valid[:800]
        
        new_pop = list(survivors)
        while len(new_pop) < POPULATION_SIZE:
            parent = random.choice(survivors)
            new_pop.append(mutate(parent))
        
        for _ in range(300):
            for pred in ['LOCAL', 'VISITA']:
                r = generate_random_rule(pred)
                if r:
                    new_pop.append(r)
        
        population = new_pop
    
    # Guardar resultados
    print("\n" + "=" * 70)
    print("📊 RESULTADOS FINALES - EXPERTO AH 1.5")
    print("=" * 70)
    
    best_rules.sort(key=lambda r: -r.accuracy())
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'handicap': 'AH_1.5',
        'total_matches': len(matches),
        'rules': [r.to_dict() for r in best_rules[:50]]
    }
    
    path = RESULTS_DIR / 'expert_ah_1_5_rules.json'
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\n🏆 MEJORES REGLAS ({len(best_rules)} encontradas):")
    for i, r in enumerate(best_rules[:20], 1):
        emoji = "🔥🔥🔥" if r.accuracy() >= 95 else "🔥🔥" if r.accuracy() >= 90 else "🔥"
        print(f"\n{i}. {emoji} {r.prediction}: {r.accuracy():.1f}% (n={r.total})")
        for c in r.conditions:
            print(f"      {c[0]} {c[1]} {c[2]}")
    
    print(f"\n💾 Guardado en: {path}")


if __name__ == '__main__':
    main()
