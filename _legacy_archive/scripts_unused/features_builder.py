"""
Features Builder - Transforma el JSON de partidos en un DataFrame 
con features normalizadas para generación de reglas.

Convención:
- +X (ah_line >= 0): LOCAL es favorito
- -X (ah_line < 0): VISITANTE es favorito
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import re


def parse_score(score_str: str) -> Tuple[Optional[int], Optional[int]]:
    """Parsea score como '2:1' o '2-1' en (home, away)."""
    if not score_str or score_str in ['-', '?:?', '']:
        return None, None
    score_str = score_str.replace('-', ':')
    parts = score_str.split(':')
    if len(parts) != 2:
        return None, None
    try:
        return int(parts[0]), int(parts[1])
    except:
        return None, None


def parse_ah_line(ah_str: str) -> Optional[float]:
    """Parsea línea AH."""
    if not ah_str or ah_str == '-':
        return None
    try:
        return float(ah_str)
    except:
        return None


def parse_movement(mov_str: str) -> Tuple[Optional[float], Optional[float]]:
    """Parsea movimiento '0.5 → -0.25' en (open, close)."""
    if not mov_str or '→' not in mov_str:
        return None, None
    parts = mov_str.replace('→', '->').split('->')
    if len(parts) != 2:
        return None, None
    try:
        return float(parts[0].strip()), float(parts[1].strip())
    except:
        return None, None


def categorize_zone(line_mag: float) -> str:
    """Categoriza la zona según magnitud de la línea."""
    if line_mag < 0.5:
        return 'PICK_EM'  # 0, 0.25
    elif line_mag < 1.0:
        return 'MUST_WIN'  # 0.5, 0.75
    elif line_mag < 2.0:
        return 'WIN_BY_1'  # 1.0, 1.25, 1.5, 1.75
    elif line_mag < 3.0:
        return 'WIN_BY_2'  # 2.0, 2.25, 2.5, 2.75
    else:
        return 'WIN_BY_3+'


def settle_ah(home_goals: int, away_goals: int, ah_line: float) -> Tuple[str, float]:
    """
    Calcula el settlement de AH desde perspectiva HOME.
    +ah_line significa LOCAL recibe ventaja.
    
    Retorna: (outcome, payout_score)
    - W: Win (+1.0)
    - HW: Half Win (+0.5)
    - P: Push (0.0)
    - HL: Half Loss (-0.5)
    - L: Loss (-1.0)
    """
    # Diferencia ajustada = (home - away) + ah_line
    diff = (home_goals - away_goals) + ah_line
    
    if diff > 0.25:
        return 'W', 1.0
    elif diff > 0:
        return 'HW', 0.5
    elif diff == 0:
        return 'P', 0.0
    elif diff >= -0.25:
        return 'HL', -0.5
    else:
        return 'L', -1.0


def settle_ou(total_goals: int, ou_line: float) -> Tuple[str, float]:
    """
    Calcula el settlement de O/U.
    
    Retorna: (outcome, payout_score) para OVER
    """
    diff = total_goals - ou_line
    
    if diff > 0.25:
        return 'OVER_W', 1.0
    elif diff > 0:
        return 'OVER_HW', 0.5
    elif diff == 0:
        return 'PUSH', 0.0
    elif diff >= -0.25:
        return 'UNDER_HW', 0.5
    else:
        return 'UNDER_W', 1.0


def extract_stats_diff(stats_rows: List[Dict]) -> Dict[str, int]:
    """Extrae diferencias de stats (home - away)."""
    result = {
        'shots_diff': 0,
        'sot_diff': 0,
        'attacks_diff': 0,
        'danger_diff': 0
    }
    
    if not stats_rows:
        return result
    
    for stat in stats_rows:
        label = stat.get('label', '').lower()
        try:
            h = int(stat.get('home', 0))
            a = int(stat.get('away', 0))
            diff = h - a
        except:
            continue
            
        if 'tiros a puerta' in label or 'shots on target' in label:
            result['sot_diff'] = diff
        elif 'tiros' in label or 'shots' in label:
            result['shots_diff'] = diff
        elif 'peligrosos' in label or 'dangerous' in label:
            result['danger_diff'] = diff
        elif 'ataques' in label or 'attacks' in label:
            result['attacks_diff'] = diff
            
    return result


def process_match(match: Dict) -> Optional[Dict]:
    """Procesa un partido y extrae todas las features."""
    
    # Datos básicos
    match_id = match.get('match_id')
    final_score = match.get('final_score', '')
    home_goals, away_goals = parse_score(final_score)
    
    if home_goals is None or away_goals is None:
        return None  # Sin resultado, no se puede usar para training
    
    # AH line
    odds = match.get('main_match_odds', {})
    ah_line = parse_ah_line(odds.get('ah_linea'))
    ou_line = parse_ah_line(odds.get('goals_linea'))
    
    if ah_line is None:
        return None  # Sin línea AH, no útil
    
    # Features base
    features = {
        'match_id': match_id,
        'home_name': match.get('home_name', ''),
        'away_name': match.get('away_name', ''),
        'league': match.get('league_name', ''),
        'match_date': match.get('match_date', ''),
        'home_goals': home_goals,
        'away_goals': away_goals,
        'total_goals': home_goals + away_goals,
        'goal_diff': home_goals - away_goals,
        
        # AH Core
        'ah_line': ah_line,
        'fav_side': 'HOME' if ah_line >= 0 else 'AWAY',
        'line_mag': abs(ah_line),
        'ticks': int(abs(ah_line) / 0.25),
        'zone': categorize_zone(abs(ah_line)),
        
        # O/U Core
        'ou_line': ou_line if ou_line else 2.5,
    }
    
    # Settlement
    ah_outcome, ah_payout = settle_ah(home_goals, away_goals, ah_line)
    features['ah_outcome'] = ah_outcome
    features['ah_payout'] = ah_payout
    features['ah_fav_covered'] = 1 if ah_payout > 0 else (0 if ah_payout == 0 else -1)
    
    if ou_line:
        ou_outcome, ou_payout = settle_ou(home_goals + away_goals, ou_line)
        features['ou_outcome'] = ou_outcome
        features['ou_payout'] = ou_payout
    else:
        features['ou_outcome'] = None
        features['ou_payout'] = None
    
    # Movement (from market_analysis_data)
    mkt = match.get('market_analysis_data', {})
    stadium_mov = mkt.get('stadium', {}).get('movement', '')
    ah_open, ah_close = parse_movement(stadium_mov)
    if ah_open is not None:
        features['ah_open'] = ah_open
        features['ah_close'] = ah_close
        features['ah_delta'] = ah_close - ah_open
        features['delta_ticks'] = int((ah_close - ah_open) / 0.25)
    else:
        features['ah_open'] = ah_line
        features['ah_close'] = ah_line
        features['ah_delta'] = 0
        features['delta_ticks'] = 0
    
    # H2H Stadium Cover
    h2h_stadium = match.get('h2h_stadium', {})
    h2h_stadium_res = h2h_stadium.get('res1')
    h2h_stadium_ah = parse_ah_line(h2h_stadium.get('ah1'))
    if h2h_stadium_res and h2h_stadium_ah is not None:
        h, a = parse_score(h2h_stadium_res)
        if h is not None:
            _, payout = settle_ah(h, a, ah_line)  # Usar línea actual
            features['h2h_stadium_cover'] = 'COVER' if payout > 0 else ('PUSH' if payout == 0 else 'NO_COVER')
        else:
            features['h2h_stadium_cover'] = None
    else:
        features['h2h_stadium_cover'] = None
    
    # H2H General Cover
    h2h_general = match.get('h2h_general', {})
    h2h_general_res = h2h_general.get('res1')
    if h2h_general_res:
        h, a = parse_score(h2h_general_res)
        if h is not None:
            _, payout = settle_ah(h, a, ah_line)
            features['h2h_general_cover'] = 'COVER' if payout > 0 else ('PUSH' if payout == 0 else 'NO_COVER')
        else:
            features['h2h_general_cover'] = None
    else:
        features['h2h_general_cover'] = None
    
    # Prev Home Stats
    prev_home = match.get('last_home_match') or {}
    prev_home_stats = extract_stats_diff(prev_home.get('stats_rows', []))
    features['prev_home_shots_diff'] = prev_home_stats['shots_diff']
    features['prev_home_sot_diff'] = prev_home_stats['sot_diff']
    features['prev_home_danger_diff'] = prev_home_stats['danger_diff']
    features['prev_home_attacks_diff'] = prev_home_stats['attacks_diff']
    
    # Prev Home Result
    prev_home_score = prev_home.get('score')
    if prev_home_score:
        h, a = parse_score(prev_home_score)
        if h is not None:
            features['prev_home_wdl'] = 'W' if h > a else ('L' if h < a else 'D')
        else:
            features['prev_home_wdl'] = None
    else:
        features['prev_home_wdl'] = None
    
    # Prev Away Stats
    prev_away = match.get('last_away_match') or {}
    prev_away_stats = extract_stats_diff(prev_away.get('stats_rows', []))
    # Invertir para perspectiva visitante
    features['prev_away_shots_diff'] = -prev_away_stats['shots_diff']
    features['prev_away_sot_diff'] = -prev_away_stats['sot_diff']
    features['prev_away_danger_diff'] = -prev_away_stats['danger_diff']
    features['prev_away_attacks_diff'] = -prev_away_stats['attacks_diff']
    
    # Prev Away Result
    prev_away_score = prev_away.get('score')
    if prev_away_score:
        h, a = parse_score(prev_away_score)
        if h is not None:
            features['prev_away_wdl'] = 'W' if a > h else ('L' if a < h else 'D')
        else:
            features['prev_away_wdl'] = None
    else:
        features['prev_away_wdl'] = None
    
    # Indirectas
    ind = match.get('comparativas_indirectas') or {}
    ind_left = ind.get('left') or {}
    ind_right = ind.get('right') or {}
    
    # Ind Local
    features['ind_local_localia'] = ind_left.get('localia')
    features['ind_local_ah'] = parse_ah_line(ind_left.get('ah_line'))
    ind_left_stats = extract_stats_diff(ind_left.get('stats_rows', []))
    features['ind_local_danger_diff'] = ind_left_stats['danger_diff']
    
    # Ind Visitante
    features['ind_visitante_localia'] = ind_right.get('localia')
    features['ind_visitante_ah'] = parse_ah_line(ind_right.get('ah_line'))
    ind_right_stats = extract_stats_diff(ind_right.get('stats_rows', []))
    features['ind_visitante_danger_diff'] = -ind_right_stats['danger_diff']
    
    # Rankings
    home_standings = match.get('home_standings', {})
    away_standings = match.get('away_standings', {})
    try:
        features['home_rank'] = int(home_standings.get('ranking', 99))
    except:
        features['home_rank'] = 99
    try:
        features['away_rank'] = int(away_standings.get('ranking', 99))
    except:
        features['away_rank'] = 99
    features['rank_diff'] = features['home_rank'] - features['away_rank']
    
    return features


def build_features_dataframe(json_path: str) -> pd.DataFrame:
    """Construye DataFrame de features desde JSON."""
    
    with open(json_path, 'r', encoding='utf-8') as f:
        matches = json.load(f)
    
    print(f"📊 Procesando {len(matches)} partidos...")
    
    features_list = []
    for match in matches:
        features = process_match(match)
        if features:
            features_list.append(features)
    
    df = pd.DataFrame(features_list)
    
    print(f"✅ Features extraídas: {len(df)} partidos válidos")
    print(f"   Columnas: {len(df.columns)}")
    
    return df


def discretize_features(df: pd.DataFrame) -> pd.DataFrame:
    """Discretiza features numéricas en bins para generación de reglas."""
    
    df = df.copy()
    
    # Danger diff bins
    def danger_bin(x):
        if pd.isna(x) or x == 0:
            return 'neutro'
        elif x > 20:
            return 'muy_pos'
        elif x > 5:
            return 'pos'
        elif x < -20:
            return 'muy_neg'
        elif x < -5:
            return 'neg'
        return 'neutro'
    
    for col in ['prev_home_danger_diff', 'prev_away_danger_diff', 'ind_local_danger_diff', 'ind_visitante_danger_diff']:
        if col in df.columns:
            df[f'{col}_bin'] = df[col].apply(danger_bin)
    
    # Ticks bins
    df['ticks_bin'] = pd.cut(df['ticks'], bins=[-1, 2, 4, 6, 8, 100], labels=['low', 'mid', 'high', 'very_high', 'extreme'])
    
    # Delta ticks bins
    df['delta_bin'] = pd.cut(df['delta_ticks'], bins=[-100, -2, 0, 2, 100], labels=['strong_drop', 'drop', 'rise', 'strong_rise'])
    
    return df


if __name__ == '__main__':
    import sys
    
    # Path por defecto
    json_path = Path(__file__).parent.parent / 'training_data_1465.json'
    if len(sys.argv) > 1:
        json_path = Path(sys.argv[1])
    
    if not json_path.exists():
        print(f"❌ No se encontró: {json_path}")
        sys.exit(1)
    
    df = build_features_dataframe(str(json_path))
    df = discretize_features(df)
    
    # Guardar
    output_path = Path(__file__).parent.parent / 'data' / 'features_matrix.csv'
    output_path.parent.mkdir(exist_ok=True)
    df.to_csv(output_path, index=False)
    
    print(f"\n💾 Guardado en: {output_path}")
    print(f"\n📈 Estadísticas:")
    print(f"   - Total partidos: {len(df)}")
    print(f"   - AH Favorable cubierto: {(df['ah_payout'] > 0).sum()} ({(df['ah_payout'] > 0).mean()*100:.1f}%)")
    print(f"   - Zonas: {df['zone'].value_counts().to_dict()}")
