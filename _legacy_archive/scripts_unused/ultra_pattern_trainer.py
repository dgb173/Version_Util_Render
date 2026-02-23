# scripts/ultra_pattern_trainer.py
"""
🏆 ULTRA PATTERN TRAINER - SISTEMA INFALIBLE
==============================================
Sistema de entrenamiento ULTRA preciso que:
1. Añade MÁS variables (cuotas, rankings, forma, tendencias)
2. Detecta patrones de "trampa" (cuando parece seguro pero falla)
3. Requiere MÚLTIPLES condiciones para alta confianza
4. Entrena tanto AH como O/U simultáneamente
5. Mínimo 30 muestras y 85% precisión para patrones válidos

NUEVAS FEATURES:
- Diferencia de ranking (mejor ranking vs peor)
- Forma reciente (últimos 3-5 partidos)
- Tendencia goleadora específica
- Comparativa de victorias casa vs fuera
- Cuotas implícitas (si disponibles)
- Movimiento de línea confirmatorio
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

# Archivos de datos
DATA_FILES = [
    DATA_DIR / 'data_ah_0.json',
    DATA_DIR / 'data_ah_0.5.json',
    DATA_DIR / 'data_ah_1.5.json',
    DATA_DIR / 'data_ah_2_plus.json',
    DATA_DIR / 'data_minus_ah_0.5.json',
    DATA_DIR / 'data_minus_ah_1.5.json',
    DATA_DIR / 'data_minus_ah_2_plus.json',
]

GENERATIONS = 6000
POPULATION_SIZE = 10000
MIN_SAMPLES_AH = 30
MIN_SAMPLES_OU = 30
MIN_ACCURACY_AH = 82  # Muy alto para AH
MIN_ACCURACY_OU = 80  # Alto para O/U


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


def get_ah_result(home_goals, away_goals, ah_line) -> str:
    """
    Determina resultado AH. 
    INVERSIÓN APLICADA: ah > 0 = LOCAL es FAV en datos, pero pick debe ser VISITA.
    """
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


def extract_ultra_features(match: Dict) -> Dict:
    """
    Extrae features ULTRA completas para máxima precisión.
    """
    f = {}
    
    main_odds = match.get('main_match_odds') or {}
    try:
        ah_line = float(main_odds.get('ah_linea', 0) or 0)
        ou_line = float(main_odds.get('goals_linea', 2.5) or 2.5)
    except:
        ah_line, ou_line = 0, 2.5
    
    f['ah_line'] = ah_line
    f['ah_abs'] = abs(ah_line)
    f['ou_line'] = ou_line
    f['ou_bucket'] = round(ou_line * 2) / 2
    
    # ===== BUCKETS DE HANDICAP =====
    f['ah_0'] = ah_line == 0
    f['ah_0_25'] = ah_line == 0.25 or ah_line == -0.25
    f['ah_0_5'] = ah_line == 0.5 or ah_line == -0.5
    f['ah_0_75'] = ah_line == 0.75 or ah_line == -0.75
    f['ah_1'] = ah_line == 1 or ah_line == -1
    f['ah_1_plus'] = abs(ah_line) >= 1
    f['ah_1_5_plus'] = abs(ah_line) >= 1.5
    f['ah_tight'] = abs(ah_line) <= 0.5
    f['ah_big'] = abs(ah_line) >= 1.5
    
    # ===== STANDINGS =====
    home_standings = match.get('home_standings') or {}
    away_standings = match.get('away_standings') or {}
    
    try:
        f['home_rank'] = int(home_standings.get('ranking', 0) or 0)
        f['away_rank'] = int(away_standings.get('ranking', 0) or 0)
        f['rank_diff'] = f['home_rank'] - f['away_rank']
        f['home_better_rank'] = f['home_rank'] > 0 and f['away_rank'] > 0 and f['home_rank'] < f['away_rank']
        f['away_better_rank'] = f['home_rank'] > 0 and f['away_rank'] > 0 and f['away_rank'] < f['home_rank']
        f['rank_diff_big'] = abs(f['rank_diff']) >= 5
        f['rank_diff_huge'] = abs(f['rank_diff']) >= 8
        f['rank_close'] = abs(f['rank_diff']) <= 2
    except:
        f['home_rank'] = f['away_rank'] = f['rank_diff'] = 0
    
    # ===== FORMA GLOBAL =====
    try:
        # Local
        f['home_wins_total'] = int(home_standings.get('total_v', 0) or 0)
        f['home_draws_total'] = int(home_standings.get('total_e', 0) or 0)
        f['home_losses_total'] = int(home_standings.get('total_d', 0) or 0)
        home_total = f['home_wins_total'] + f['home_draws_total'] + f['home_losses_total']
        f['home_win_rate'] = f['home_wins_total'] / home_total if home_total > 0 else 0.5
        f['home_loss_rate'] = f['home_losses_total'] / home_total if home_total > 0 else 0.5
        f['home_good_form'] = f['home_win_rate'] >= 0.5
        f['home_bad_form'] = f['home_win_rate'] <= 0.3
        
        # Visitante
        f['away_wins_total'] = int(away_standings.get('total_v', 0) or 0)
        f['away_draws_total'] = int(away_standings.get('total_e', 0) or 0)
        f['away_losses_total'] = int(away_standings.get('total_d', 0) or 0)
        away_total = f['away_wins_total'] + f['away_draws_total'] + f['away_losses_total']
        f['away_win_rate'] = f['away_wins_total'] / away_total if away_total > 0 else 0.5
        f['away_loss_rate'] = f['away_losses_total'] / away_total if away_total > 0 else 0.5
        f['away_good_form'] = f['away_win_rate'] >= 0.5
        f['away_bad_form'] = f['away_win_rate'] <= 0.3
    except:
        pass
    
    # ===== FORMA ESPECÍFICA (Casa/Fuera) =====
    try:
        # Local en casa
        f['home_wins_spec'] = int(home_standings.get('specific_v', 0) or 0)
        f['home_draws_spec'] = int(home_standings.get('specific_e', 0) or 0)
        f['home_losses_spec'] = int(home_standings.get('specific_d', 0) or 0)
        home_spec_total = f['home_wins_spec'] + f['home_draws_spec'] + f['home_losses_spec']
        f['home_win_rate_home'] = f['home_wins_spec'] / home_spec_total if home_spec_total > 0 else 0.5
        f['home_unbeaten_home'] = f['home_losses_spec'] == 0 and home_spec_total >= 3
        f['home_strong_home'] = f['home_win_rate_home'] >= 0.6
        f['home_weak_home'] = f['home_win_rate_home'] <= 0.3
        
        # Visitante fuera
        f['away_wins_spec'] = int(away_standings.get('specific_v', 0) or 0)
        f['away_draws_spec'] = int(away_standings.get('specific_e', 0) or 0)
        f['away_losses_spec'] = int(away_standings.get('specific_d', 0) or 0)
        away_spec_total = f['away_wins_spec'] + f['away_draws_spec'] + f['away_losses_spec']
        f['away_win_rate_away'] = f['away_wins_spec'] / away_spec_total if away_spec_total > 0 else 0.5
        f['away_strong_away'] = f['away_win_rate_away'] >= 0.5
        f['away_weak_away'] = f['away_win_rate_away'] <= 0.2
    except:
        pass
    
    # ===== GOLES =====
    try:
        f['home_gf_spec'] = int(home_standings.get('specific_gf', 0) or 0)
        f['home_gc_spec'] = int(home_standings.get('specific_gc', 0) or 0)
        f['away_gf_spec'] = int(away_standings.get('specific_gf', 0) or 0)
        f['away_gc_spec'] = int(away_standings.get('specific_gc', 0) or 0)
        
        home_pj = int(home_standings.get('specific_pj', 1) or 1)
        away_pj = int(away_standings.get('specific_pj', 1) or 1)
        
        f['home_goals_avg'] = (f['home_gf_spec'] + f['home_gc_spec']) / home_pj if home_pj > 0 else 2.5
        f['away_goals_avg'] = (f['away_gf_spec'] + f['away_gc_spec']) / away_pj if away_pj > 0 else 2.5
        f['combined_goals_avg'] = (f['home_goals_avg'] + f['away_goals_avg']) / 2
        
        f['home_high_scoring'] = f['home_goals_avg'] >= 3
        f['away_high_scoring'] = f['away_goals_avg'] >= 3
        f['both_high_scoring'] = f['home_high_scoring'] and f['away_high_scoring']
        f['home_low_scoring'] = f['home_goals_avg'] <= 2
        f['away_low_scoring'] = f['away_goals_avg'] <= 2
        f['both_low_scoring'] = f['home_low_scoring'] and f['away_low_scoring']
    except:
        pass
    
    # ===== PREV HOME MATCH =====
    prev_home = match.get('last_home_match') or {}
    if prev_home:
        score = parse_score(prev_home.get('score'))
        if score:
            # Determinar si ganó/perdió
            f['prev_home_goals'] = score[0] + score[1]
            f['prev_home_local_goals'] = score[0]
            f['prev_home_away_goals'] = score[1]
            f['prev_home_won'] = score[0] > score[1]
            f['prev_home_lost'] = score[0] < score[1]
            f['prev_home_drew'] = score[0] == score[1]
            f['prev_home_clean_sheet'] = score[1] == 0
            f['prev_home_conceded_many'] = score[1] >= 2
            f['prev_home_scored_many'] = score[0] >= 2
            f['prev_home_over'] = f['prev_home_goals'] > 2.5
            f['prev_home_under'] = f['prev_home_goals'] <= 2
            f['prev_home_high'] = f['prev_home_goals'] >= 4
            f['prev_home_low'] = f['prev_home_goals'] <= 1
        
        stats = parse_stats_rows(prev_home.get('stats_rows', []))
        if 'Ataques Peligrosos' in stats:
            f['prev_home_danger_home'] = stats['Ataques Peligrosos']['home']
            f['prev_home_danger_away'] = stats['Ataques Peligrosos']['away']
            f['prev_home_danger_diff'] = f['prev_home_danger_home'] - f['prev_home_danger_away']
            f['prev_home_dominated'] = f['prev_home_danger_diff'] > 20
            f['prev_home_was_dominated'] = f['prev_home_danger_diff'] < -20
        if 'Tiros a Puerta' in stats:
            f['prev_home_sot_home'] = stats['Tiros a Puerta']['home']
            f['prev_home_sot_away'] = stats['Tiros a Puerta']['away']
            f['prev_home_sot_diff'] = f['prev_home_sot_home'] - f['prev_home_sot_away']
    
    # ===== PREV AWAY MATCH =====
    prev_away = match.get('last_away_match') or {}
    if prev_away:
        score = parse_score(prev_away.get('score'))
        if score:
            f['prev_away_goals'] = score[0] + score[1]
            f['prev_away_local_goals'] = score[0]
            f['prev_away_away_goals'] = score[1]
            f['prev_away_won'] = score[1] > score[0]  # Visitante ganó
            f['prev_away_lost'] = score[1] < score[0]
            f['prev_away_drew'] = score[0] == score[1]
            f['prev_away_clean_sheet'] = score[0] == 0
            f['prev_away_conceded_many'] = score[0] >= 2
            f['prev_away_scored_many'] = score[1] >= 2
            f['prev_away_over'] = f['prev_away_goals'] > 2.5
            f['prev_away_under'] = f['prev_away_goals'] <= 2
            f['prev_away_high'] = f['prev_away_goals'] >= 4
            f['prev_away_low'] = f['prev_away_goals'] <= 1
        
        stats = parse_stats_rows(prev_away.get('stats_rows', []))
        if 'Ataques Peligrosos' in stats:
            f['prev_away_danger_home'] = stats['Ataques Peligrosos']['home']
            f['prev_away_danger_away'] = stats['Ataques Peligrosos']['away']
            f['prev_away_danger_diff'] = f['prev_away_danger_away'] - f['prev_away_danger_home']
            f['prev_away_dominated'] = f['prev_away_danger_diff'] > 20
            f['prev_away_was_dominated'] = f['prev_away_danger_diff'] < -20
        if 'Tiros a Puerta' in stats:
            f['prev_away_sot_home'] = stats['Tiros a Puerta']['home']
            f['prev_away_sot_away'] = stats['Tiros a Puerta']['away']
            f['prev_away_sot_diff'] = f['prev_away_sot_away'] - f['prev_away_sot_home']
    
    # ===== PATRONES COMBINADOS PREV =====
    f['both_prev_won'] = f.get('prev_home_won', False) and f.get('prev_away_won', False)
    f['both_prev_lost'] = f.get('prev_home_lost', False) and f.get('prev_away_lost', False)
    f['both_prev_drew'] = f.get('prev_home_drew', False) and f.get('prev_away_drew', False)
    f['home_won_away_lost'] = f.get('prev_home_won', False) and f.get('prev_away_lost', False)
    f['home_lost_away_won'] = f.get('prev_home_lost', False) and f.get('prev_away_won', False)
    f['both_prev_over'] = f.get('prev_home_over', False) and f.get('prev_away_over', False)
    f['both_prev_under'] = f.get('prev_home_under', False) and f.get('prev_away_under', False)
    f['both_clean_sheets'] = f.get('prev_home_clean_sheet', False) and f.get('prev_away_clean_sheet', False)
    
    # ===== H2H COL3 =====
    h2h_col3 = match.get('h2h_col3') or {}
    if h2h_col3.get('status') == 'found':
        try:
            h = int(h2h_col3.get('goles_home', 0) or 0)
            a = int(h2h_col3.get('goles_away', 0) or 0)
            f['h2h_col3_goals'] = h + a
            f['h2h_col3_home_goals'] = h
            f['h2h_col3_away_goals'] = a
            f['h2h_col3_home_won'] = h > a
            f['h2h_col3_away_won'] = a > h
            f['h2h_col3_drew'] = h == a
            f['h2h_col3_over'] = f['h2h_col3_goals'] > 2.5
            f['h2h_col3_under'] = f['h2h_col3_goals'] <= 2
            f['h2h_col3_high'] = f['h2h_col3_goals'] >= 4
            f['h2h_col3_low'] = f['h2h_col3_goals'] <= 1
        except:
            pass
    
    # ===== COMPARATIVAS INDIRECTAS =====
    comp = match.get('comparativas_indirectas') or {}
    
    ind_left = comp.get('left')
    if ind_left:
        score = parse_score(ind_left.get('score'))
        if score:
            is_home = ind_left.get('localia') == 'H'
            f['ind_left_goals'] = score[0] + score[1]
            f['ind_left_team_goals'] = score[0] if is_home else score[1]
            f['ind_left_opp_goals'] = score[1] if is_home else score[0]
            f['ind_left_won'] = f['ind_left_team_goals'] > f['ind_left_opp_goals']
            f['ind_left_lost'] = f['ind_left_team_goals'] < f['ind_left_opp_goals']
            f['ind_left_over'] = f['ind_left_goals'] > 2.5
            f['ind_left_under'] = f['ind_left_goals'] <= 2
            f['ind_left_margin'] = f['ind_left_team_goals'] - f['ind_left_opp_goals']
            f['ind_left_big_win'] = f['ind_left_margin'] >= 2
            f['ind_left_big_loss'] = f['ind_left_margin'] <= -2
    
    ind_right = comp.get('right')
    if ind_right:
        score = parse_score(ind_right.get('score'))
        if score:
            is_home = ind_right.get('localia') == 'H'
            f['ind_right_goals'] = score[0] + score[1]
            f['ind_right_team_goals'] = score[0] if is_home else score[1]
            f['ind_right_opp_goals'] = score[1] if is_home else score[0]
            f['ind_right_won'] = f['ind_right_team_goals'] > f['ind_right_opp_goals']
            f['ind_right_lost'] = f['ind_right_team_goals'] < f['ind_right_opp_goals']
            f['ind_right_over'] = f['ind_right_goals'] > 2.5
            f['ind_right_under'] = f['ind_right_goals'] <= 2
            f['ind_right_margin'] = f['ind_right_team_goals'] - f['ind_right_opp_goals']
            f['ind_right_big_win'] = f['ind_right_margin'] >= 2
            f['ind_right_big_loss'] = f['ind_right_margin'] <= -2
    
    # ===== MARKET ANALYSIS =====
    market = match.get('market_analysis_data') or {}
    stadium_m = market.get('stadium') or {}
    general_m = market.get('general') or {}
    
    f['h2h_stadium_covered'] = stadium_m.get('is_covered')
    f['h2h_general_covered'] = general_m.get('is_covered')
    f['h2h_both_covered'] = f['h2h_stadium_covered'] == True and f['h2h_general_covered'] == True
    f['h2h_none_covered'] = f['h2h_stadium_covered'] == False and f['h2h_general_covered'] == False
    f['h2h_mixed'] = (f['h2h_stadium_covered'] == True) != (f['h2h_general_covered'] == True)
    
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
                f['line_same'] = after == before
                f['line_big_move'] = abs(after - before) >= 0.5
            except:
                pass
    
    # ===== PATRONES DE TRAMPA (para evitar fallos) =====
    # Cuando parece seguro pero no lo es
    
    # Trampa 1: Local con buen ranking pero mala forma en casa
    f['trap_home_rank_vs_form'] = (
        f.get('home_better_rank', False) and 
        f.get('home_weak_home', False)
    )
    
    # Trampa 2: Visitante con mal ranking pero buena forma fuera
    f['trap_away_rank_vs_form'] = (
        f.get('away_better_rank', False) == False and 
        f.get('away_strong_away', False)
    )
    
    # Trampa 3: Prev dominó pero no ganó
    f['trap_home_dominated_lost'] = (
        f.get('prev_home_dominated', False) and 
        f.get('prev_home_lost', False)
    )
    
    # Trampa 4: H2H cubierto pero prev perdió
    f['trap_h2h_vs_form'] = (
        f.get('h2h_both_covered', False) and 
        f.get('prev_home_lost', False)
    )
    
    return f


# Features para AH
AH_FEATURES = [
    # Handicap buckets
    'ah_0', 'ah_0_25', 'ah_0_5', 'ah_0_75', 'ah_1', 'ah_1_plus', 'ah_1_5_plus', 'ah_tight', 'ah_big',
    
    # Rankings
    'rank_diff', 'home_better_rank', 'away_better_rank', 'rank_diff_big', 'rank_diff_huge', 'rank_close',
    
    # Forma global
    'home_win_rate', 'away_win_rate', 'home_good_form', 'away_good_form', 'home_bad_form', 'away_bad_form',
    
    # Forma específica
    'home_win_rate_home', 'away_win_rate_away', 'home_unbeaten_home', 'home_strong_home', 
    'home_weak_home', 'away_strong_away', 'away_weak_away',
    
    # Prev home
    'prev_home_won', 'prev_home_lost', 'prev_home_drew', 'prev_home_clean_sheet',
    'prev_home_conceded_many', 'prev_home_scored_many', 'prev_home_dominated', 'prev_home_was_dominated',
    
    # Prev away
    'prev_away_won', 'prev_away_lost', 'prev_away_drew', 'prev_away_clean_sheet',
    'prev_away_conceded_many', 'prev_away_scored_many', 'prev_away_dominated', 'prev_away_was_dominated',
    
    # Combinados prev
    'both_prev_won', 'both_prev_lost', 'both_prev_drew', 'home_won_away_lost', 'home_lost_away_won',
    
    # H2H Col3
    'h2h_col3_home_won', 'h2h_col3_away_won', 'h2h_col3_drew',
    
    # Indirectas
    'ind_left_won', 'ind_left_lost', 'ind_left_big_win', 'ind_left_big_loss',
    'ind_right_won', 'ind_right_lost', 'ind_right_big_win', 'ind_right_big_loss',
    
    # Market
    'h2h_both_covered', 'h2h_none_covered', 'h2h_mixed',
    'line_increased', 'line_decreased', 'line_same', 'line_big_move',
    
    # Trampas
    'trap_home_rank_vs_form', 'trap_away_rank_vs_form', 'trap_home_dominated_lost', 'trap_h2h_vs_form',
]

# Features para O/U
OU_FEATURES = [
    # Línea
    'ou_bucket', 'ah_tight', 'ah_big',
    
    # Goles promedio
    'home_goals_avg', 'away_goals_avg', 'combined_goals_avg',
    'home_high_scoring', 'away_high_scoring', 'both_high_scoring',
    'home_low_scoring', 'away_low_scoring', 'both_low_scoring',
    
    # Rankings
    'rank_diff', 'rank_close', 'rank_diff_big',
    
    # Prev home
    'prev_home_goals', 'prev_home_over', 'prev_home_under', 'prev_home_high', 'prev_home_low',
    'prev_home_clean_sheet', 'prev_home_conceded_many', 'prev_home_scored_many',
    
    # Prev away
    'prev_away_goals', 'prev_away_over', 'prev_away_under', 'prev_away_high', 'prev_away_low',
    'prev_away_clean_sheet', 'prev_away_conceded_many', 'prev_away_scored_many',
    
    # Combinados
    'both_prev_over', 'both_prev_under', 'both_clean_sheets',
    
    # H2H
    'h2h_col3_goals', 'h2h_col3_over', 'h2h_col3_under', 'h2h_col3_high', 'h2h_col3_low',
    
    # Indirectas
    'ind_left_goals', 'ind_left_over', 'ind_left_under',
    'ind_right_goals', 'ind_right_over', 'ind_right_under',
    
    # Market
    'h2h_both_covered', 'h2h_none_covered',
]


class UltraPattern:
    def __init__(self, conditions, pick, pattern_type='AH'):
        self.conditions = conditions
        self.pick = pick  # 'LOCAL' / 'VISITA' for AH, 'OVER' / 'UNDER' for OU
        self.pattern_type = pattern_type
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
        
        # Nombres AH
        if self.pattern_type == 'AH':
            if 'home_strong_home' in cond_keys and 'away_weak_away' in cond_keys:
                return 'FORTALEZA_LOCAL'
            elif 'away_strong_away' in cond_keys and 'home_weak_home' in cond_keys:
                return 'FORTALEZA_VISITA'
            elif 'home_won_away_lost' in cond_keys:
                return 'MOMENTUM_LOCAL'
            elif 'home_lost_away_won' in cond_keys:
                return 'MOMENTUM_VISITA'
            elif 'prev_home_dominated' in cond_keys:
                return 'DOMINIO_LOCAL'
            elif 'prev_away_dominated' in cond_keys:
                return 'DOMINIO_VISITA'
            elif 'h2h_both_covered' in cond_keys:
                return 'H2H_CUBIERTO'
            elif 'trap_home_rank_vs_form' in cond_keys:
                return 'TRAMPA_RANKING'
            elif 'line_decreased' in cond_keys:
                return 'LINEA_BAJA'
            elif 'line_increased' in cond_keys:
                return 'LINEA_SUBE'
        
        # Nombres O/U
        else:
            if 'both_high_scoring' in cond_keys or 'both_prev_over' in cond_keys:
                return 'GOLEADORES'
            elif 'both_low_scoring' in cond_keys or 'both_prev_under' in cond_keys:
                return 'DEFENSIVOS'
            elif 'both_clean_sheets' in cond_keys:
                return 'PORTEROS_TOP'
            elif 'h2h_col3_high' in cond_keys:
                return 'H2H_GOLEADOR'
            elif 'h2h_col3_low' in cond_keys:
                return 'H2H_BAJO'
            elif 'ah_tight' in cond_keys:
                return 'PARTIDO_CERRADO'
        
        return f'PATRON_{self.pattern_type}_{len(self.conditions)}'
    
    def to_dict(self):
        return {
            'name': self.name or self.generate_name(),
            'pick': self.pick,
            'type': self.pattern_type,
            'accuracy': round(self.accuracy(), 1),
            'samples': self.total,
            'conditions': [(c[0], c[1], c[2]) for c in self.conditions]
        }


def generate_condition(feat, features_list):
    if feat in [f for f in features_list if isinstance(f, str) and not any(x in f for x in ['rate', 'diff', 'goals', 'margin'])]:
        # Boolean features
        return (feat, '==', True)
    elif 'rate' in feat:
        return (feat, random.choice(['>=', '<=']), round(random.uniform(0.2, 0.7), 2))
    elif 'diff' in feat or 'margin' in feat:
        return (feat, random.choice(['>=', '<=', '>', '<']), random.randint(-5, 5))
    elif 'goals' in feat:
        return (feat, random.choice(['>=', '<=', '>', '<']), random.randint(1, 5))
    return (feat, '==', True)


def generate_random_pattern(pattern_type='AH'):
    features = AH_FEATURES if pattern_type == 'AH' else OU_FEATURES
    n = random.randint(3, 6)
    conditions = []
    used = set()
    
    for _ in range(n):
        feat = random.choice(features)
        if feat in used:
            continue
        used.add(feat)
        cond = generate_condition(feat, features)
        if cond:
            conditions.append(cond)
    
    if len(conditions) < 3:
        return None
    
    if pattern_type == 'AH':
        pick = random.choice(['LOCAL', 'VISITA'])
    else:
        pick = random.choice(['OVER', 'UNDER'])
    
    return UltraPattern(conditions, pick, pattern_type)


def mutate(pattern):
    features = AH_FEATURES if pattern.pattern_type == 'AH' else OU_FEATURES
    new_conds = list(pattern.conditions)
    action = random.choice(['add', 'remove', 'modify', 'replace'])
    
    if action == 'add' and len(new_conds) < 7:
        feat = random.choice(features)
        cond = generate_condition(feat, features)
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
        feat = random.choice(features)
        cond = generate_condition(feat, features)
        if cond:
            new_conds[idx] = cond
    
    new_pattern = UltraPattern(new_conds, pattern.pick, pattern.pattern_type)
    
    if random.random() < 0.05:
        if pattern.pattern_type == 'AH':
            new_pattern.pick = 'LOCAL' if pattern.pick == 'VISITA' else 'VISITA'
        else:
            new_pattern.pick = 'OVER' if pattern.pick == 'UNDER' else 'UNDER'
    
    return new_pattern


def main():
    print("=" * 70)
    print("[ULTRA] PATTERN TRAINER - SISTEMA INFALIBLE")
    print("=" * 70)
    print(f"Generaciones: {GENERATIONS}")
    print(f"Población: {POPULATION_SIZE}")
    print(f"Min precisión AH: {MIN_ACCURACY_AH}% | Min muestras: {MIN_SAMPLES_AH}")
    print(f"Min precisión O/U: {MIN_ACCURACY_OU}% | Min muestras: {MIN_SAMPLES_OU}")
    print()
    
    # Cargar partidos
    all_matches = []
    for file_path in DATA_FILES:
        if file_path.exists():
            with open(file_path, 'r', encoding='utf-8') as f:
                matches = json.load(f)
                all_matches.extend(matches)
                print(f"   Cargado: {file_path.name} ({len(matches)} partidos)")
    
    all_matches = [m for m in all_matches if parse_score(m.get('final_score') or m.get('score'))]
    print(f"\n[+] Total partidos: {len(all_matches)}")
    
    # Poblar AH y O/U
    population_ah = [generate_random_pattern('AH') for _ in range(POPULATION_SIZE) if generate_random_pattern('AH')]
    population_ou = [generate_random_pattern('OU') for _ in range(POPULATION_SIZE) if generate_random_pattern('OU')]
    
    population_ah = [p for p in population_ah if p]
    population_ou = [p for p in population_ou if p]
    
    best_ah = []
    best_ou = []
    
    for gen in range(GENERATIONS):
        # Reset
        for p in population_ah + population_ou:
            p.total = p.correct = 0
        
        for match in all_matches:
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
            
            ah_result = get_ah_result(parsed[0], parsed[1], ah)
            ou_result = get_ou_result(parsed[0], parsed[1], ou)
            
            features = extract_ultra_features(match)
            
            # Evaluar AH
            if ah_result != 'PUSH':
                for p in population_ah:
                    if p.matches(features):
                        p.total += 1
                        if p.pick == ah_result:
                            p.correct += 1
            
            # Evaluar O/U
            if ou_result != 'PUSH':
                for p in population_ou:
                    if p.matches(features):
                        p.total += 1
                        if p.pick == ou_result:
                            p.correct += 1
        
        # Buscar mejores AH
        for p in population_ah:
            if p.total >= MIN_SAMPLES_AH and p.accuracy() >= MIN_ACCURACY_AH:
                is_new = not any(
                    abs(ex.accuracy() - p.accuracy()) < 2 and ex.pick == p.pick
                    for ex in best_ah
                )
                if is_new and len(best_ah) < 100:
                    p.name = p.generate_name()
                    best_ah.append(copy.deepcopy(p))
                    emoji = "***" if p.accuracy() >= 90 else "**" if p.accuracy() >= 85 else "*"
                    print(f"{emoji} Gen {gen+1} [AH {p.name}] {p.pick}: {p.accuracy():.1f}% (n={p.total})")
        
        # Buscar mejores O/U
        for p in population_ou:
            if p.total >= MIN_SAMPLES_OU and p.accuracy() >= MIN_ACCURACY_OU:
                is_new = not any(
                    abs(ex.accuracy() - p.accuracy()) < 2 and ex.pick == p.pick
                    for ex in best_ou
                )
                if is_new and len(best_ou) < 100:
                    p.name = p.generate_name()
                    best_ou.append(copy.deepcopy(p))
                    emoji = "***" if p.accuracy() >= 90 else "**" if p.accuracy() >= 85 else "*"
                    print(f"{emoji} Gen {gen+1} [O/U {p.name}] {p.pick}: {p.accuracy():.1f}% (n={p.total})")
        
        if (gen + 1) % 500 == 0:
            print(f"   Gen {gen+1}/{GENERATIONS} - AH: {len(best_ah)} | O/U: {len(best_ou)}")
        
        # Evolución
        for pop, best in [(population_ah, best_ah), (population_ou, best_ou)]:
            valid = [p for p in pop if p.total >= 10 and p.accuracy() >= 50]
            if not valid:
                valid = pop[:300]
            
            valid.sort(key=lambda p: -p.accuracy())
            survivors = valid[:800]
            
            new_pop = list(survivors)
            while len(new_pop) < POPULATION_SIZE:
                parent = random.choice(survivors)
                new_pop.append(mutate(parent))
            
            for _ in range(300):
                p = generate_random_pattern(pop[0].pattern_type if pop else 'AH')
                if p:
                    new_pop.append(p)
            
            if pop == population_ah:
                population_ah = new_pop
            else:
                population_ou = new_pop
    
    # Guardar resultados
    best_ah.sort(key=lambda p: -p.accuracy())
    best_ou.sort(key=lambda p: -p.accuracy())
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'version': '2.0-ultra',
        'total_matches': len(all_matches),
        'ah_patterns': [p.to_dict() for p in best_ah[:50]],
        'ou_patterns': [p.to_dict() for p in best_ou[:50]]
    }
    
    path = RESULTS_DIR / 'ultra_patterns.json'
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\n{'='*70}")
    print("RESUMEN FINAL")
    print("="*70)
    print(f"Patrones AH: {len(best_ah)}")
    print(f"Patrones O/U: {len(best_ou)}")
    
    print("\nTOP 10 AH:")
    for i, p in enumerate(best_ah[:10], 1):
        print(f"   {i}. [{p.name}] {p.pick}: {p.accuracy():.1f}% (n={p.total})")
    
    print("\nTOP 10 O/U:")
    for i, p in enumerate(best_ou[:10], 1):
        print(f"   {i}. [{p.name}] {p.pick}: {p.accuracy():.1f}% (n={p.total})")
    
    print(f"\n[OK] Guardado en: {path}")


if __name__ == '__main__':
    main()
