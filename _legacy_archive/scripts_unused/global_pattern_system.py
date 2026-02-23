# scripts/global_pattern_system.py
"""
🏆 SISTEMA GLOBAL DE PATRONES - VERSIÓN PROFESIONAL
=====================================================
Sistema unificado que entrena TODOS los handicaps y genera:
- Patrones con NOMBRES DESCRIPTIVOS
- Predicción COMBINADA AH + O/U
- Basado en FAVORITO (no local/visita)
- Clave: HANDICAP INICIAL

Ejemplo de salida:
{
    "pattern": "DOMINATOR_PERDEDOR",
    "description": "FAV domina stats pero perdió último, NO_FAV perdió por poco",
    "ah_pick": "FAV",
    "ou_pick": "UNDER", 
    "accuracy_ah": 90.5,
    "accuracy_ou": 78.2,
    "accuracy_combo": 72.1,
    "samples": 25,
    "conditions": [...]
}
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

GENERATIONS = 4000
POPULATION_SIZE = 6000
MIN_SAMPLES = 20
MIN_ACCURACY = 80  # Buscamos >80%

# Nombres de patrones basados en las condiciones
PATTERN_NAMES = {
    # Patrones de dominio de stats
    ('fav_dominated_attacks', 'nofav_close_loss'): 'DOMINATOR_PERDEDOR',
    ('fav_dominated_attacks', 'fav_won_prev'): 'DOMINATOR_GANADOR',
    ('fav_dominated_sot', 'fav_covered_prev'): 'TIRADOR_CERTERO',
    
    # Patrones de línea
    ('line_decreased',): 'LINEA_BAJISTA',
    ('line_increased',): 'LINEA_ALCISTA',
    ('line_decreased', 'ind_right_covered'): 'LINEA_BAJISTA_INDIRECTA',
    
    # Patrones de forma
    ('fav_better_at_home', 'nofav_worse_away'): 'FORTALEZA_LOCAL',
    ('nofav_better_away', 'fav_worse_home'): 'FORTALEZA_VISITA',
    
    # Patrones de cobertura
    ('h2h_both_covered',): 'COBERTURA_HISTORICA',
    ('fav_covered_easy_prev',): 'COBERTURA_FACIL',
    ('fav_covered_tight_prev',): 'COBERTURA_JUSTA',
    
    # Patrones O/U
    ('prev_home_over', 'prev_away_over'): 'GOLEADORES_HISTORICOS',
    ('prev_home_under', 'prev_away_under'): 'DEFENSIVOS_HISTORICOS',
    ('h2h_high_scoring',): 'CLASICO_GOLEADOR',
    
    # Patrones anti-intuición
    ('fav_dominated_attacks', 'fav_lost_prev'): 'ANTI_RESULTADO',
    ('nofav_better_rank', 'fav_dominated_attacks'): 'ANTI_RANKING',
    
    # Patrones de ranking
    ('rank_diff_huge', 'fav_unbeaten_home'): 'GIGANTE_INVICTO',
    ('rank_close', 'h2h_both_covered'): 'DUELO_PAREJO_CUBIERTO',
}


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


def get_fav_result(home_goals, away_goals, ah_line) -> str:
    """
    Determina si el FAVORITO cubrió el handicap.
    ah_line > 0 -> Local es favorito (da ventaja)
    ah_line < 0 -> Visita es favorito (recibe ventaja)
    """
    adjusted = (home_goals - away_goals) - ah_line
    if adjusted > 0.25:
        return 'FAV' if ah_line > 0 else 'NO_FAV'
    elif adjusted < -0.25:
        return 'NO_FAV' if ah_line > 0 else 'FAV'
    return 'PUSH'


def get_ou_result(home_goals, away_goals, ou_line) -> str:
    total = home_goals + away_goals
    if total > ou_line + 0.25:
        return 'OVER'
    elif total < ou_line - 0.25:
        return 'UNDER'
    return 'PUSH'


def get_cover_type(home_goals, away_goals, ah_line, is_fav_home: bool) -> str:
    """Tipo de cobertura para el favorito."""
    if is_fav_home:
        adjusted = (home_goals - away_goals) - ah_line
    else:
        adjusted = (away_goals - home_goals) + ah_line
    
    if adjusted > 1:
        return 'COVER_EASY'
    elif adjusted > 0.25:
        return 'COVER_TIGHT'
    elif adjusted > -0.5:
        return 'NO_COVER_CLOSE'
    return 'NO_COVER_CLEAR'


def extract_global_features(match: Dict) -> Dict:
    """
    Extrae features GLOBALES basadas en FAV/NO_FAV (no local/visita).
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
    
    # Determinar quién es el favorito
    fav_is_home = ah_line > 0
    f['fav_is_home'] = fav_is_home
    
    # ===== RANKINGS - Perspectiva FAV/NO_FAV =====
    home_standings = match.get('home_standings') or {}
    away_standings = match.get('away_standings') or {}
    
    fav_standings = home_standings if fav_is_home else away_standings
    nofav_standings = away_standings if fav_is_home else home_standings
    
    try:
        f['fav_rank'] = int(fav_standings.get('ranking', 0) or 0)
        f['nofav_rank'] = int(nofav_standings.get('ranking', 0) or 0)
        f['rank_diff'] = f['fav_rank'] - f['nofav_rank']  # Negativo = FAV mejor ranking
        f['fav_better_rank'] = f['fav_rank'] > 0 and f['fav_rank'] < f['nofav_rank']
        f['nofav_better_rank'] = f['nofav_rank'] > 0 and f['nofav_rank'] < f['fav_rank']
        f['rank_diff_big'] = abs(f['rank_diff']) >= 5
        f['rank_diff_huge'] = abs(f['rank_diff']) >= 10
        f['rank_close'] = abs(f['rank_diff']) <= 3
    except:
        f['fav_rank'] = f['nofav_rank'] = f['rank_diff'] = 0
    
    # ===== RENDIMIENTO GLOBAL Y ESPECÍFICO =====
    try:
        # FAV global
        f['fav_wins'] = int(fav_standings.get('wins', 0) or 0)
        f['fav_losses'] = int(fav_standings.get('losses', 0) or 0)
        total = f['fav_wins'] + int(fav_standings.get('draws', 0) or 0) + f['fav_losses']
        f['fav_win_rate'] = f['fav_wins'] / total if total > 0 else 0.5
        
        # NO_FAV global
        f['nofav_wins'] = int(nofav_standings.get('wins', 0) or 0)
        f['nofav_losses'] = int(nofav_standings.get('losses', 0) or 0)
        total = f['nofav_wins'] + int(nofav_standings.get('draws', 0) or 0) + f['nofav_losses']
        f['nofav_win_rate'] = f['nofav_wins'] / total if total > 0 else 0.5
        
        # Específico (casa/fuera)
        fav_spec = fav_standings.get('specific', {}) or {}
        nofav_spec = nofav_standings.get('specific', {}) or {}
        
        f['fav_wins_spec'] = int(fav_spec.get('wins', 0) or 0)
        f['fav_losses_spec'] = int(fav_spec.get('losses', 0) or 0)
        total = f['fav_wins_spec'] + int(fav_spec.get('draws', 0) or 0) + f['fav_losses_spec']
        f['fav_win_rate_spec'] = f['fav_wins_spec'] / total if total > 0 else 0.5
        f['fav_unbeaten_home'] = f['fav_losses_spec'] == 0 and total > 0 if fav_is_home else False
        
        f['nofav_wins_spec'] = int(nofav_spec.get('wins', 0) or 0)
        f['nofav_losses_spec'] = int(nofav_spec.get('losses', 0) or 0)
        total = f['nofav_wins_spec'] + int(nofav_spec.get('draws', 0) or 0) + f['nofav_losses_spec']
        f['nofav_win_rate_spec'] = f['nofav_wins_spec'] / total if total > 0 else 0.5
        f['nofav_unbeaten_away'] = f['nofav_losses_spec'] == 0 and total > 0 if not fav_is_home else False
        
        # Comparaciones
        f['fav_better_at_home'] = fav_is_home and f['fav_win_rate_spec'] > f['fav_win_rate']
        f['nofav_better_away'] = not fav_is_home and f['nofav_win_rate_spec'] >= f['nofav_win_rate']
        f['fav_worse_home'] = fav_is_home and f['fav_win_rate_spec'] < f['fav_win_rate']
        f['nofav_worse_away'] = not fav_is_home and f['nofav_win_rate_spec'] < f['nofav_win_rate']
    except:
        pass
    
    # ===== PREV MATCH DEL FAV (según si es local o visita) =====
    prev_fav = match.get('last_home_match') if fav_is_home else match.get('last_away_match')
    prev_nofav = match.get('last_away_match') if fav_is_home else match.get('last_home_match')
    
    if prev_fav:
        score = parse_score(prev_fav.get('score'))
        if score:
            fav_goals = score[0] if fav_is_home else score[1]
            opp_goals = score[1] if fav_is_home else score[0]
            
            f['fav_won_prev'] = fav_goals > opp_goals
            f['fav_lost_prev'] = fav_goals < opp_goals
            f['fav_drew_prev'] = fav_goals == opp_goals
            f['fav_prev_margin'] = fav_goals - opp_goals
            f['fav_prev_goals'] = score[0] + score[1]
            
            # O/U
            f['prev_fav_over'] = (score[0] + score[1]) > 2.5
            f['prev_fav_under'] = (score[0] + score[1]) <= 2.5
            
            # Cobertura
            try:
                prev_ah = float(prev_fav.get('handicap_line_raw') or prev_fav.get('handicap') or 0)
                f['fav_prev_ah'] = prev_ah
                f['fav_ah_similar'] = abs(prev_ah - abs(ah_line)) < 0.5
                f['fav_cover_type'] = get_cover_type(score[0], score[1], prev_ah, fav_is_home)
                f['fav_covered_prev'] = f['fav_cover_type'].startswith('COVER')
                f['fav_covered_easy_prev'] = f['fav_cover_type'] == 'COVER_EASY'
                f['fav_covered_tight_prev'] = f['fav_cover_type'] == 'COVER_TIGHT'
            except:
                pass
        
        # Stats del FAV
        stats = parse_stats_rows(prev_fav.get('stats_rows', []))
        if 'Ataques Peligrosos' in stats:
            if fav_is_home:
                f['fav_danger_edge'] = stats['Ataques Peligrosos']['home'] - stats['Ataques Peligrosos']['away']
            else:
                f['fav_danger_edge'] = stats['Ataques Peligrosos']['away'] - stats['Ataques Peligrosos']['home']
            f['fav_dominated_attacks'] = f['fav_danger_edge'] > 10
        if 'Tiros a Puerta' in stats:
            if fav_is_home:
                f['fav_sot_edge'] = stats['Tiros a Puerta']['home'] - stats['Tiros a Puerta']['away']
            else:
                f['fav_sot_edge'] = stats['Tiros a Puerta']['away'] - stats['Tiros a Puerta']['home']
            f['fav_dominated_sot'] = f['fav_sot_edge'] > 3
    
    # ===== PREV MATCH DEL NO_FAV =====
    if prev_nofav:
        score = parse_score(prev_nofav.get('score'))
        if score:
            nofav_goals = score[1] if fav_is_home else score[0]
            opp_goals = score[0] if fav_is_home else score[1]
            
            f['nofav_won_prev'] = nofav_goals > opp_goals
            f['nofav_lost_prev'] = nofav_goals < opp_goals
            f['nofav_drew_prev'] = nofav_goals == opp_goals
            f['nofav_prev_margin'] = nofav_goals - opp_goals
            f['nofav_close_loss'] = f['nofav_lost_prev'] and abs(nofav_goals - opp_goals) == 1
            f['nofav_big_loss'] = f['nofav_lost_prev'] and abs(nofav_goals - opp_goals) >= 2
            
            # O/U
            f['prev_nofav_over'] = (score[0] + score[1]) > 2.5
            f['prev_nofav_under'] = (score[0] + score[1]) <= 2.5
            
            # Cobertura
            try:
                prev_ah = float(prev_nofav.get('handicap_line_raw') or prev_nofav.get('handicap') or 0)
                f['nofav_ah_similar'] = abs(prev_ah - abs(ah_line)) < 0.5
                f['nofav_covered_prev'] = get_cover_type(score[0], score[1], prev_ah, not fav_is_home).startswith('COVER')
            except:
                pass
    
    # ===== H2H COL3 =====
    h2h_col3 = match.get('h2h_col3') or {}
    if h2h_col3.get('status') == 'found':
        try:
            h = int(h2h_col3.get('goles_home', 0) or 0)
            a = int(h2h_col3.get('goles_away', 0) or 0)
            f['h2h_col3_goals'] = h + a
            f['h2h_high_scoring'] = (h + a) >= 3
            f['h2h_low_scoring'] = (h + a) <= 1
            f['h2h_col3_margin'] = (h - a) if fav_is_home else (a - h)
        except:
            pass
    
    # ===== COMPARATIVAS INDIRECTAS =====
    comp = match.get('comparativas_indirectas') or {}
    
    # Ind del FAV
    ind_fav = comp.get('left') if fav_is_home else comp.get('right')
    if ind_fav:
        score = parse_score(ind_fav.get('score'))
        if score:
            is_home = ind_fav.get('localia') == 'H'
            fav_goals = score[0] if is_home else score[1]
            opp_goals = score[1] if is_home else score[0]
            f['ind_fav_margin'] = fav_goals - opp_goals
            f['ind_fav_won'] = fav_goals > opp_goals
            f['ind_fav_goals'] = score[0] + score[1]
            f['ind_fav_over'] = (score[0] + score[1]) > 2.5
            
            try:
                ind_ah = float(ind_fav.get('ah_line') or ind_fav.get('ah') or 0)
                f['ind_fav_covered'] = get_cover_type(score[0], score[1], ind_ah, is_home).startswith('COVER')
            except:
                pass
    
    # Ind del NO_FAV
    ind_nofav = comp.get('right') if fav_is_home else comp.get('left')
    if ind_nofav:
        score = parse_score(ind_nofav.get('score'))
        if score:
            is_home = ind_nofav.get('localia') != 'A'
            nofav_goals = score[1] if is_home else score[0]
            opp_goals = score[0] if is_home else score[1]
            f['ind_nofav_margin'] = nofav_goals - opp_goals
            f['ind_nofav_won'] = nofav_goals > opp_goals
            f['ind_nofav_goals'] = score[0] + score[1]
            f['ind_nofav_big_win'] = f['ind_nofav_margin'] >= 2
            f['ind_nofav_over'] = (score[0] + score[1]) > 2.5
            
            try:
                ind_ah = float(ind_nofav.get('ah_line') or ind_nofav.get('ah') or 0)
                f['ind_nofav_covered'] = get_cover_type(score[0], score[1], ind_ah, not is_home).startswith('COVER')
            except:
                pass
    
    # ===== MARKET ANALYSIS =====
    market = match.get('market_analysis_data') or {}
    stadium_m = market.get('stadium') or {}
    general_m = market.get('general') or {}
    
    f['h2h_stadium_covered'] = stadium_m.get('is_covered')
    f['h2h_general_covered'] = general_m.get('is_covered')
    f['h2h_both_covered'] = f['h2h_stadium_covered'] == True and f['h2h_general_covered'] == True
    f['h2h_both_not_covered'] = f['h2h_stadium_covered'] == False and f['h2h_general_covered'] == False
    
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
    
    # ===== PATRONES COMBINADOS =====
    # Patrón del usuario: FAV domina pero no gana, NO_FAV perdió por poco
    f['pattern_dominator_perdedor'] = (
        f.get('fav_dominated_attacks', False) and
        f.get('fav_lost_prev', False) and
        f.get('nofav_close_loss', False)
    )
    
    # Patrón: Anti-resultado (FAV domina stats pero pierde)
    f['pattern_anti_resultado'] = (
        f.get('fav_dominated_attacks', False) and
        f.get('fav_lost_prev', False)
    )
    
    # Patrón: Goleadores históricos
    f['pattern_goleadores'] = (
        f.get('prev_fav_over', False) and
        f.get('prev_nofav_over', False)
    )
    
    # Patrón: Defensivos históricos
    f['pattern_defensivos'] = (
        f.get('prev_fav_under', False) and
        f.get('prev_nofav_under', False)
    )
    
    return f


# Features globales
GLOBAL_FEATURES = [
    # Rankings
    'rank_diff', 'fav_better_rank', 'nofav_better_rank', 'rank_diff_big', 'rank_diff_huge', 'rank_close',
    
    # Rendimiento
    'fav_win_rate', 'nofav_win_rate', 'fav_win_rate_spec', 'nofav_win_rate_spec',
    'fav_better_at_home', 'nofav_better_away', 'fav_unbeaten_home', 'nofav_unbeaten_away',
    'fav_worse_home', 'nofav_worse_away',
    
    # Prev FAV
    'fav_won_prev', 'fav_lost_prev', 'fav_drew_prev', 'fav_prev_margin',
    'fav_covered_prev', 'fav_covered_easy_prev', 'fav_covered_tight_prev', 'fav_ah_similar',
    'fav_danger_edge', 'fav_dominated_attacks', 'fav_sot_edge', 'fav_dominated_sot',
    'prev_fav_over', 'prev_fav_under',
    
    # Prev NO_FAV
    'nofav_won_prev', 'nofav_lost_prev', 'nofav_drew_prev', 'nofav_prev_margin',
    'nofav_close_loss', 'nofav_big_loss', 'nofav_covered_prev', 'nofav_ah_similar',
    'prev_nofav_over', 'prev_nofav_under',
    
    # H2H
    'h2h_col3_goals', 'h2h_high_scoring', 'h2h_low_scoring', 'h2h_col3_margin',
    'h2h_both_covered', 'h2h_both_not_covered',
    
    # Indirectas
    'ind_fav_margin', 'ind_fav_won', 'ind_fav_covered', 'ind_fav_over',
    'ind_nofav_margin', 'ind_nofav_won', 'ind_nofav_covered', 'ind_nofav_big_win', 'ind_nofav_over',
    
    # Market
    'line_change', 'line_increased', 'line_decreased',
    
    # Patrones combinados
    'pattern_dominator_perdedor', 'pattern_anti_resultado',
    'pattern_goleadores', 'pattern_defensivos',
]


class Pattern:
    def __init__(self, conditions, ah_pick, ou_pick=None):
        self.conditions = conditions  # [(feature, op, value), ...]
        self.ah_pick = ah_pick  # 'FAV' o 'NO_FAV'
        self.ou_pick = ou_pick  # 'OVER', 'UNDER', o None
        self.name = None  # Se genera después
        
        self.ah_total = 0
        self.ah_correct = 0
        self.ou_total = 0
        self.ou_correct = 0
        self.combo_total = 0
        self.combo_correct = 0
    
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
    
    def ah_accuracy(self):
        return self.ah_correct / self.ah_total * 100 if self.ah_total > 0 else 0
    
    def ou_accuracy(self):
        return self.ou_correct / self.ou_total * 100 if self.ou_total > 0 else 0
    
    def combo_accuracy(self):
        return self.combo_correct / self.combo_total * 100 if self.combo_total > 0 else 0
    
    def generate_name(self):
        """Genera nombre descriptivo basado en condiciones."""
        cond_keys = tuple(sorted([c[0] for c in self.conditions if c[2] == True]))
        
        # Buscar en patrones predefinidos
        for pattern_key, name in PATTERN_NAMES.items():
            if all(k in cond_keys for k in pattern_key):
                return name
        
        # Generar nombre genérico
        if 'fav_dominated_attacks' in cond_keys:
            return 'ATAQUE_DOMINANTE'
        elif 'line_decreased' in cond_keys:
            return 'LINEA_BAJISTA'
        elif 'line_increased' in cond_keys:
            return 'LINEA_ALCISTA'
        elif 'h2h_both_covered' in cond_keys:
            return 'COBERTURA_H2H'
        elif 'fav_covered_prev' in cond_keys:
            return 'COBERTURA_RECIENTE'
        elif 'ind_fav_covered' in cond_keys:
            return 'INDIRECTA_CUBIERTA'
        elif 'pattern_goleadores' in cond_keys:
            return 'GOLEADORES'
        elif 'pattern_defensivos' in cond_keys:
            return 'DEFENSIVOS'
        
        return 'PATRON_' + str(len(self.conditions))
    
    def to_dict(self):
        return {
            'name': self.name or self.generate_name(),
            'ah_pick': self.ah_pick,
            'ou_pick': self.ou_pick,
            'accuracy_ah': round(self.ah_accuracy(), 1),
            'accuracy_ou': round(self.ou_accuracy(), 1) if self.ou_pick else None,
            'accuracy_combo': round(self.combo_accuracy(), 1) if self.ou_pick else None,
            'samples': self.ah_total,
            'conditions': [(c[0], c[1], c[2]) for c in self.conditions]
        }


def generate_condition(feat):
    if feat in ['fav_better_rank', 'nofav_better_rank', 'rank_diff_big', 'rank_diff_huge', 'rank_close',
                'fav_better_at_home', 'nofav_better_away', 'fav_unbeaten_home', 'nofav_unbeaten_away',
                'fav_worse_home', 'nofav_worse_away',
                'fav_won_prev', 'fav_lost_prev', 'fav_drew_prev', 'fav_covered_prev',
                'fav_covered_easy_prev', 'fav_covered_tight_prev', 'fav_ah_similar',
                'fav_dominated_attacks', 'fav_dominated_sot',
                'nofav_won_prev', 'nofav_lost_prev', 'nofav_drew_prev', 'nofav_covered_prev',
                'nofav_close_loss', 'nofav_big_loss', 'nofav_ah_similar',
                'h2h_high_scoring', 'h2h_low_scoring', 'h2h_both_covered', 'h2h_both_not_covered',
                'ind_fav_won', 'ind_fav_covered', 'ind_nofav_won', 'ind_nofav_covered', 'ind_nofav_big_win',
                'line_increased', 'line_decreased',
                'prev_fav_over', 'prev_fav_under', 'prev_nofav_over', 'prev_nofav_under',
                'ind_fav_over', 'ind_nofav_over',
                'pattern_dominator_perdedor', 'pattern_anti_resultado',
                'pattern_goleadores', 'pattern_defensivos']:
        return (feat, '==', True)
    elif 'margin' in feat:
        return (feat, random.choice(['>=', '<=', '>', '<']), random.choice([-2, -1, 0, 1, 2, 3]))
    elif 'edge' in feat:
        return (feat, random.choice(['>=', '<=', '>', '<']), round(random.uniform(-10, 15), 0))
    elif 'rate' in feat:
        return (feat, random.choice(['>=', '<=']), round(random.uniform(0.3, 0.7), 2))
    elif 'rank_diff' in feat:
        return (feat, random.choice(['>', '<']), random.choice([-5, -3, 0, 3, 5]))
    elif 'goals' in feat:
        return (feat, random.choice(['>=', '<=']), random.randint(1, 4))
    elif 'line_change' in feat:
        return (feat, random.choice(['>', '<']), round(random.uniform(-0.3, 0.3), 2))
    return None


def generate_random_pattern():
    n = random.randint(3, 5)
    conditions = []
    used = set()
    for _ in range(n):
        feat = random.choice(GLOBAL_FEATURES)
        if feat in used:
            continue
        used.add(feat)
        cond = generate_condition(feat)
        if cond:
            conditions.append(cond)
    
    if len(conditions) < 3:
        return None
    
    ah_pick = random.choice(['FAV', 'NO_FAV'])
    ou_pick = random.choice(['OVER', 'UNDER', None])
    
    return Pattern(conditions, ah_pick, ou_pick)


def mutate(pattern):
    new_conds = list(pattern.conditions)
    action = random.choice(['add', 'remove', 'modify', 'replace'])
    
    if action == 'add' and len(new_conds) < 6:
        feat = random.choice(GLOBAL_FEATURES)
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
        feat = random.choice(GLOBAL_FEATURES)
        cond = generate_condition(feat)
        if cond:
            new_conds[idx] = cond
    
    new_pattern = Pattern(new_conds, pattern.ah_pick, pattern.ou_pick)
    
    # Ocasionalmente cambiar O/U
    if random.random() < 0.1:
        new_pattern.ou_pick = random.choice(['OVER', 'UNDER', None])
    
    return new_pattern


def train_handicap(handicap_name: str, file_path: Path) -> Dict:
    """Entrena patrones para un handicap específico."""
    
    print(f"\n{'='*60}")
    print(f"🎯 {handicap_name}")
    print(f"{'='*60}")
    
    if not file_path.exists():
        print(f"   ⚠️ Archivo no encontrado")
        return {'handicap': handicap_name, 'patterns': []}
    
    with open(file_path, 'r', encoding='utf-8') as f:
        matches = json.load(f)
    
    matches = [m for m in matches if parse_score(m.get('final_score') or m.get('score'))]
    print(f"   Partidos: {len(matches)}")
    
    if len(matches) < 50:
        print(f"   ⚠️ Muy pocos partidos")
        return {'handicap': handicap_name, 'patterns': []}
    
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
            p.ah_total = p.ah_correct = 0
            p.ou_total = p.ou_correct = 0
            p.combo_total = p.combo_correct = 0
        
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
            
            ah_result = get_fav_result(parsed[0], parsed[1], ah)
            ou_result = get_ou_result(parsed[0], parsed[1], ou)
            
            if ah_result == 'PUSH':
                continue
            
            features = extract_global_features(match)
            
            for pattern in population:
                if not pattern.matches(features):
                    continue
                
                pattern.ah_total += 1
                if pattern.ah_pick == ah_result:
                    pattern.ah_correct += 1
                
                if pattern.ou_pick and ou_result != 'PUSH':
                    pattern.ou_total += 1
                    if pattern.ou_pick == ou_result:
                        pattern.ou_correct += 1
                    
                    pattern.combo_total += 1
                    if pattern.ah_pick == ah_result and pattern.ou_pick == ou_result:
                        pattern.combo_correct += 1
        
        # Buscar excelentes
        for p in population:
            if p.ah_total < MIN_SAMPLES:
                continue
            
            acc = p.ah_accuracy()
            
            if acc >= MIN_ACCURACY:
                is_new = not any(
                    abs(existing.ah_accuracy() - acc) < 2 and
                    existing.ah_pick == p.ah_pick and
                    existing.ou_pick == p.ou_pick
                    for existing in best_patterns
                )
                if is_new and len(best_patterns) < 60:
                    p.name = p.generate_name()
                    best_patterns.append(copy.deepcopy(p))
                    emoji = "🔥🔥🔥" if acc >= 95 else "🔥🔥" if acc >= 90 else "🔥"
                    ou_str = f" + {p.ou_pick} ({p.ou_accuracy():.0f}%)" if p.ou_pick else ""
                    print(f"{emoji} Gen {gen+1} [{p.name}] {p.ah_pick}{ou_str}: {acc:.1f}% (n={p.ah_total})")
        
        if (gen + 1) % 500 == 0:
            print(f"   Gen {gen+1}/{GENERATIONS} - {len(best_patterns)} patrones")
        
        # Evolución
        valid = [p for p in population if p.ah_total >= 10 and p.ah_accuracy() >= 50]
        if not valid:
            valid = population[:300]
        
        valid.sort(key=lambda p: -p.ah_accuracy())
        survivors = valid[:600]
        
        new_pop = list(survivors)
        while len(new_pop) < POPULATION_SIZE:
            parent = random.choice(survivors)
            new_pop.append(mutate(parent))
        
        for _ in range(200):
            p = generate_random_pattern()
            if p:
                new_pop.append(p)
        
        population = new_pop
    
    # Ordenar y devolver
    best_patterns.sort(key=lambda p: -p.ah_accuracy())
    
    print(f"\n   🏆 {len(best_patterns)} patrones encontrados")
    for p in best_patterns[:5]:
        ou_str = f" + {p.ou_pick} ({p.ou_accuracy():.0f}%)" if p.ou_pick else ""
        print(f"      [{p.name}] {p.ah_pick}{ou_str}: {p.ah_accuracy():.1f}%")
    
    return {
        'handicap': handicap_name,
        'total_matches': len(matches),
        'patterns': [p.to_dict() for p in best_patterns[:30]]
    }


def main():
    print("=" * 70)
    print("🏆 SISTEMA GLOBAL DE PATRONES - VERSIÓN PROFESIONAL")
    print("=" * 70)
    print(f"Generaciones: {GENERATIONS}")
    print(f"Población: {POPULATION_SIZE}")
    print(f"Min precisión: {MIN_ACCURACY}%")
    print()
    print("FEATURES:")
    print("  ✅ Basado en FAV/NO_FAV (no local/visita)")
    print("  ✅ Ataques peligrosos y tiros a puerta")
    print("  ✅ Predicción combinada AH + O/U")
    print("  ✅ Patrones con nombres descriptivos")
    print()
    
    all_results = {
        'timestamp': datetime.now().isoformat(),
        'version': '2.0',
        'handicaps': {}
    }
    
    for handicap_name, file_path in HANDICAP_FILES.items():
        result = train_handicap(handicap_name, file_path)
        all_results['handicaps'][handicap_name] = result
    
    # Guardar
    path = RESULTS_DIR / 'global_patterns.json'
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)
    
    # Resumen
    print("\n" + "=" * 70)
    print("📊 RESUMEN GLOBAL")
    print("=" * 70)
    
    total_patterns = 0
    for name, data in all_results['handicaps'].items():
        n = len(data.get('patterns', []))
        total_patterns += n
        if n > 0:
            top = data['patterns'][0]
            print(f"   {name}: {n} patrones - Top: [{top['name']}] {top['ah_pick']} {top['accuracy_ah']}%")
    
    print(f"\n   TOTAL: {total_patterns} patrones")
    print(f"   💾 Guardado en: {path}")


if __name__ == '__main__':
    main()
