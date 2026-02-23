"""
Features Builder v2 - Integración Unificada

Combina todos los builders (expectation, dominance, bridge) para crear
el feature set completo usado en el minado de patrones.

Incluye:
- Features de mercado actual
- Expectativas históricas por fuente
- Dominancia por fuente
- Puentes consolidados
- Gates y flags
- Discretización para reglas
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from bs4 import BeautifulSoup

from .settle_asian import (
    settle_ah, settle_ou, get_ah_family, get_ou_family, 
    get_favorite_side, calculate_profit, safe_float
)
from .gates import (
    validate_match_data, safe_int, is_da_valid, get_outlier_flag
)
from .expectation_builder import (
    extract_all_expectations, summarize_expectations, parse_score
)
from .dominance_builder import (
    extract_all_dominance, summarize_dominance, get_dominance_consensus
)
from .bridge_builder import build_all_bridges


def parse_ah_line(ah_str) -> Optional[float]:
    """Parsea línea AH de string."""
    if ah_str is None or ah_str == '-' or ah_str == '':
        return None
    try:
        return float(ah_str)
    except (ValueError, TypeError):
        return None


def parse_movement(mov_str: str) -> Tuple[Optional[float], Optional[float]]:
    """Parsea movimiento '0.5 → -0.25' en (open, close)."""
    if not mov_str or '→' not in mov_str and '->' not in mov_str:
        return None, None
    mov_str = mov_str.replace('→', '->')
    parts = mov_str.split('->')
    if len(parts) != 2:
        return None, None
    try:
        return float(parts[0].strip()), float(parts[1].strip())
    except (ValueError, TypeError):
        return None, None


def get_movement_direction(ah_open: float, ah_close: float) -> str:
    """Determina dirección del movimiento."""
    diff = ah_close - ah_open
    if abs(diff) < 0.1:
        return 'SAME'
    elif diff > 0:
        return 'UP'  # Línea subió (favorito se debilitó)
    else:
        return 'DOWN'  # Línea bajó (favorito se fortaleció)


def build_match_features(match: Dict) -> Optional[Dict[str, Any]]:
    """
    Construye todas las features para un partido.
    
    Args:
        match: Dict con datos completos del partido
        
    Returns:
        Dict con todas las features, o None si datos insuficientes
    """
    features = {}
    
    # ==================== DATOS BÁSICOS ====================
    match_id = match.get('match_id', '')
    features['match_id'] = match_id
    features['home_name'] = match.get('home_name', '')
    features['away_name'] = match.get('away_name', '')
    features['league'] = match.get('league_name', match.get('liga', ''))
    features['match_date'] = match.get('match_date', match.get('date', ''))
    
    # ==================== RESULTADO ====================
    final_score = match.get('final_score', '')
    home_goals, away_goals = parse_score(final_score)
    features['has_result'] = home_goals is not None
    features['home_goals'] = home_goals
    features['away_goals'] = away_goals
    features['total_goals'] = (home_goals or 0) + (away_goals or 0)
    features['goal_diff'] = (home_goals or 0) - (away_goals or 0)
    
    # ==================== MERCADO ACTUAL ====================
    odds = match.get('main_match_odds', {})
    
    # AH Line
    ah_line = parse_ah_line(odds.get('ah_linea'))
    if ah_line is None:
        # Intentar otros campos
        ah_line = safe_float(match.get('ah', 0))
    
    features['current_ah'] = ah_line
    features['ah_family'] = get_ah_family(ah_line) if ah_line is not None else None
    features['fav_side'] = get_favorite_side(ah_line) if ah_line is not None else None
    features['line_mag'] = abs(ah_line) if ah_line is not None else None
    
    # O/U Line
    ou_line = parse_ah_line(odds.get('goals_linea'))
    if ou_line is None:
        ou_line = safe_float(match.get('ou', 2.5))
    
    features['current_ou'] = ou_line
    features['ou_family'] = get_ou_family(ou_line) if ou_line else None
    
    # Cuotas
    features['ah_odds_home'] = safe_float(odds.get('ah_home', 1.80))
    features['ah_odds_away'] = safe_float(odds.get('ah_away', 1.80))
    features['ou_odds_over'] = safe_float(odds.get('goals_over', 1.80))
    features['ou_odds_under'] = safe_float(odds.get('goals_under', 1.80))
    
    # Movimiento de mercado
    mkt_data = match.get('market_analysis_data', {})
    stadium_mov = (mkt_data.get('stadium') or {}).get('movement', '')
    general_mov = (mkt_data.get('general') or {}).get('movement', '')
    
    ah_open, ah_close = parse_movement(stadium_mov)
    features['ah_open'] = ah_open
    features['ah_close'] = ah_close
    if ah_open is not None and ah_close is not None:
        features['ah_delta'] = ah_close - ah_open
        features['movement_dir'] = get_movement_direction(ah_open, ah_close)
    else:
        features['ah_delta'] = 0
        features['movement_dir'] = 'NONE'
    
    # ==================== GATES Y VALIDACIÓN ====================
    validation = validate_match_data(match)
    features['da_ok_count'] = validation['da_ok_count']
    features['has_prev_home'] = validation['prev_home_ok']
    features['has_prev_away'] = validation['prev_away_ok']
    features['has_h2h'] = validation['h2h_col3_ok']
    features['has_indirectas'] = validation['ind_left_ok'] or validation['ind_right_ok']
    
    # ==================== EXPECTATIVAS (CLAVE) ====================
    expectations = extract_all_expectations(match)
    exp_summary = summarize_expectations(expectations)
    
    # Features consolidadas de expectativas
    features['exp_sources'] = exp_summary['sources_with_data']
    features['exp_fav_count'] = exp_summary['fav_count']
    features['exp_dog_count'] = exp_summary['dog_count']
    features['exp_cover_own_rate'] = exp_summary['cover_own_rate']
    features['exp_cover_current_rate'] = exp_summary['cover_current_rate']
    features['exp_avg_strength'] = exp_summary['avg_exp_strength']
    features['exp_beat_count'] = exp_summary['beat_expectation_count']
    features['exp_failed_count'] = exp_summary['failed_expectation_count']
    
    # Features por fuente de expectativa
    for source_name, exp_data in expectations.items():
        prefix = source_name.replace('_exp', '')
        features[f'{prefix}_exp_line'] = exp_data.get('line_past')
        features[f'{prefix}_exp_was_fav'] = exp_data.get('was_favorite')
        features[f'{prefix}_exp_strength_bin'] = exp_data.get('exp_strength_bin')
        features[f'{prefix}_exp_cover_own'] = exp_data.get('cover_own')
        features[f'{prefix}_exp_cover_current'] = exp_data.get('cover_current')
        features[f'{prefix}_exp_beat'] = exp_data.get('beat_expectation')
        features[f'{prefix}_exp_failed'] = exp_data.get('failed_expectation')
    
    # ==================== DOMINANCIA ====================
    dominance = extract_all_dominance(match)
    dom_summary = summarize_dominance(dominance)
    
    features['dom_sources'] = dom_summary['sources_with_data']
    features['dom_home_count'] = dom_summary['home_dominant_count']
    features['dom_away_count'] = dom_summary['away_dominant_count']
    features['dom_avg_dSOT'] = dom_summary['avg_dSOT']
    features['dom_avg_dDA'] = dom_summary['avg_dDA']
    features['dom_consistent'] = dom_summary['consistent_dominance']
    features['dom_consensus'] = get_dominance_consensus(dominance)
    
    # Features por fuente de dominancia
    for source_name, dom_data in dominance.items():
        prefix = source_name.replace('_dom', '')
        features[f'{prefix}_dSOT'] = dom_data.get('dSOT')
        features[f'{prefix}_dSOT_bin'] = dom_data.get('dSOT_bin')
        features[f'{prefix}_dDA'] = dom_data.get('dDA')
        features[f'{prefix}_dDA_bin'] = dom_data.get('dDA_bin')
        features[f'{prefix}_DA_ok'] = dom_data.get('DA_ok')
        features[f'{prefix}_tempo_bin'] = dom_data.get('tempo_bin')
        features[f'{prefix}_quality'] = dom_data.get('quality')
    
    # ==================== PUENTES ====================
    bridges = build_all_bridges(match)
    
    features['bridge_count'] = bridges['bridge_count']
    features['bridge_M_gap_mean'] = bridges['M_gap_mean']
    features['bridge_D_gap_SOT_mean'] = bridges['D_gap_SOT_mean']
    features['bridge_D_gap_DA_mean'] = bridges['D_gap_DA_mean']
    features['bridge_contradiction_rate'] = bridges['contradiction_rate']
    features['bridge_consensus_rate'] = bridges['consensus_rate']
    features['bridge_consensus_bin'] = bridges['consensus_bin']
    features['bridge_direction'] = bridges['overall_direction']
    
    # ==================== RANKINGS ====================
    home_st = match.get('home_standings') or {}
    away_st = match.get('away_standings') or {}
    
    h_rank = safe_int(home_st.get('ranking', 99)) or 99
    a_rank = safe_int(away_st.get('ranking', 99)) or 99
    
    features['home_rank'] = h_rank
    features['away_rank'] = a_rank
    features['rank_diff'] = h_rank - a_rank
    features['home_top5'] = 0 < h_rank <= 5
    features['away_top5'] = 0 < a_rank <= 5
    features['home_bottom5'] = h_rank >= 15
    features['away_bottom5'] = a_rank >= 15
    
    # ==================== SETTLEMENT (si hay resultado) ====================
    if home_goals is not None and away_goals is not None and ah_line is not None:
        # IMPORTANTE: NowGoal usa ah_line positivo = Local Favorito
        # settle_ah usa ah_line negativo = Local Favorito
        # Por tanto, negamos ah_line para calcular correctamente
        ah_outcome, ah_profit = settle_ah(home_goals, away_goals, -ah_line)
        features['ah_outcome'] = ah_outcome
        features['ah_profit'] = ah_profit
        
        # ¿Quién cubrió?
        if ah_profit > 0:
            features['ah_covered'] = 'HOME'
        elif ah_profit < 0:
            features['ah_covered'] = 'AWAY'
        else:
            features['ah_covered'] = 'PUSH'
        
        # Settlement O/U
        total = home_goals + away_goals
        if ou_line:
            over_outcome, over_profit = settle_ou(total, ou_line, 'OVER')
            features['ou_outcome'] = 'OVER' if over_profit > 0 else ('UNDER' if over_profit < 0 else 'PUSH')
            features['ou_profit'] = over_profit
            
    # ==================== DATA H2H (COL 3) & HISTÓRICO ====================
    # Extraer datos de market_analysis_data si existe
    market_data = match.get('market_analysis_data', {})
    
    # 1. H2H Col 3 (General)
    h2h_gen = market_data.get('general', {})
    features['H2H_Last_Covered'] = str(h2h_gen.get('is_covered', 'unknown')) # 'True', 'False', 'unknown'
    features['H2H_Last_Eval'] = h2h_gen.get('evaluation', 'NONE') # CUBIERTO / NO CUBIERTO / PUSH
    
    # Detectar cambio de línea respecto al H2H
    h2h_mov = h2h_gen.get('movement', '')
    h2h_open, h2h_close = parse_movement(h2h_mov)
    if h2h_close is not None and features.get('current_ah') is not None:
        # Comparar línea actual con la del H2H
        # Si current_ah > h2h_close, el favorito es MÁS favorito ahora
        features['AH_vs_H2H'] = features['current_ah'] - h2h_close
    else:
        features['AH_vs_H2H'] = 0
        
    # 2. H2H Estadio
    h2h_stad = market_data.get('stadium', {})
    features['H2H_Stad_Covered'] = str(h2h_stad.get('is_covered', 'unknown'))
    
    # 3. Features Booleanas de H2H para patrones AH
    features['H2H_Driver_Covered'] = features['H2H_Last_Covered'] == 'True'
    features['H2H_Driver_Failed'] = features['H2H_Last_Covered'] == 'False'
    features['H2H_Line_Higher'] = features['AH_vs_H2H'] > 0.1
    features['H2H_Line_Lower'] = features['AH_vs_H2H'] < -0.1
    
    # 4. Features H2H para O/U
    h2h_res = h2h_gen.get('result', '')
    if h2h_res and ':' in h2h_res:
        hg, ag = parse_score(h2h_res)
        if hg is not None and ag is not None:
            h2h_total = hg + ag
            features['H2H_Last_Goals'] = h2h_total
            if features.get('current_ou') is not None:
                features['H2H_Over_Line'] = h2h_total > features['current_ou']
                features['H2H_Under_Line'] = h2h_total < features['current_ou']
            else:
                features['H2H_Over_Line'] = False
                features['H2H_Under_Line'] = False
    else:
        features['H2H_Over_Line'] = False
        features['H2H_Under_Line'] = False

    # ==================== HISTORIAL DETALLADO (Parsed from HTML) ====================
    # Analisis de los "red boxes": Como rindio el equipo con handicaps similares recientemente
    hist_html = match.get('historical_matches_html', '')
    current_ah = features.get('current_ah')
    
    hist_same_ah_wins = 0
    hist_same_ah_count = 0
    hist_same_ah_profit = 0.0
    
    if hist_html and current_ah is not None:
        try:
            soup = BeautifulSoup(hist_html, 'html.parser')
            # Buscar tablas de local y visitante
            tables = soup.find_all('table')
            
            # Asumimos que la primera tabla es Home y segunda es Away (por estructura visual comun)
            # Pero mejor verificar nombres si es posible. Por simplicidad, tomamos tabla Home.
            if tables:
                home_table = tables[0]
                rows = home_table.find_all('tr')[1:] # Skip header
                
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) < 6: continue
                    
                    # Extraer AH (ultima columna o cercana)
                    # En el HTML vi: <span ..>0.5</span>
                    ah_cell_text = cols[-1].get_text(strip=True)
                    if not ah_cell_text or ah_cell_text == '-': continue
                    
                    hist_ah = parse_ah_line(ah_cell_text)
                    if hist_ah is None: continue
                    
                    # Verificar si es "Similar" (mismo signo y magnitud cercana)
                    # Tolerancia de 0.25
                    if abs(hist_ah - current_ah) <= 0.25:
                        
                        # Extraer Resultado
                        res_text = cols[3].get_text(strip=True) # Res col index 3
                        hg_hist, ag_hist = parse_score(res_text)
                        
                        if hg_hist is not None and ag_hist is not None:
                            # Calcular Profit con el AH de ESE partido
                            # Asumimos que la tabla muestra AH desde perspectiva del local
                            # Negamos hist_ah si settle_ah espera Home Fav como negativo (depende de tu implementacion)
                            # settle_ah: ah_line < 0 => Home Fav.
                            # NowGoal: ah_line < 0 => Away Fav? No, NowGoal ah usually positive/negative relative to home?
                            # En el HTML vimos "-1", "-0.25". Si Home era favorito (-1), en settle_ah seria -1.
                            
                            # CUIDADO: parse_ah_line devuelve float. Si en HTML dice "-0.25", es -0.25.
                            # Si en HTML dice "0.25", es 0.25 (Away Fav?)
                            
                            outcome, profit = settle_ah(hg_hist, ag_hist, -hist_ah) # Asumiendo convencion local
                            
                            hist_same_ah_count += 1
                            hist_same_ah_profit += profit
                            if profit > 0:
                                hist_same_ah_wins += 1
                                
        except Exception:
            pass # Fail silent parsing
            
    features['HIST_SameAH_Count'] = hist_same_ah_count
    features['HIST_SameAH_WinRate'] = (hist_same_ah_wins / hist_same_ah_count) if hist_same_ah_count > 0 else 0
    features['HIST_SameAH_HighProfit'] = hist_same_ah_profit > 1.5
    features['HIST_SameAH_HighLoss'] = hist_same_ah_profit < -1.5

    return features


def discretize_feature(value, bins: List[Tuple[float, str]]) -> str:
    """
    Discretiza un valor numérico en un bin.
    
    Args:
        value: Valor a discretizar
        bins: Lista de (umbral, label) ordenados ascendentemente
        
    Returns:
        Label del bin correspondiente
    """
    if value is None:
        return 'unknown'
    
    for threshold, label in bins:
        if value <= threshold:
            return label
    return bins[-1][1] if bins else 'unknown'


def discretize_features(features: Dict) -> Dict:
    """
    Discretiza features numéricas en bins para generación de reglas.
    """
    f = features.copy()
    
    # Line magnitude bins
    if f.get('line_mag') is not None:
        mag = f['line_mag']
        if mag < 0.25:
            f['line_mag_bin'] = 'pickem'
        elif mag < 0.75:
            f['line_mag_bin'] = 'low'
        elif mag < 1.25:
            f['line_mag_bin'] = 'mid'
        else:
            f['line_mag_bin'] = 'high'
    
    # DA ok count bins
    da_ok = f.get('da_ok_count', 0)
    if da_ok <= 1:
        f['da_ok_bin'] = 'low'
    elif da_ok == 2:
        f['da_ok_bin'] = 'mid'
    else:
        f['da_ok_bin'] = 'high'
    
    # Exp cover rate bins
    cover_rate = f.get('exp_cover_current_rate')
    if cover_rate is not None:
        if cover_rate < 0.4:
            f['exp_cover_bin'] = 'low'
        elif cover_rate < 0.6:
            f['exp_cover_bin'] = 'mid'
        else:
            f['exp_cover_bin'] = 'high'
    
    # Movement delta bins
    delta = f.get('ah_delta', 0)
    if delta < -0.25:
        f['movement_bin'] = 'strong_drop'
    elif delta < 0:
        f['movement_bin'] = 'drop'
    elif delta > 0.25:
        f['movement_bin'] = 'strong_rise'
    elif delta > 0:
        f['movement_bin'] = 'rise'
    else:
        f['movement_bin'] = 'stable'
    
    # ============================================================
    # DERIVED BOOLEAN FEATURES (usadas por patrones entrenados)
    # ============================================================
    
    # --- Triangulación / Indirectas ---
    bridge_da = f.get('bridge_D_gap_DA_mean', 0) or 0
    bridge_m = f.get('bridge_M_gap_mean', 0) or 0
    f['TRIANG_Home_Dom'] = bridge_da > 5
    f['TRIANG_Away_Weak'] = bridge_da > 10
    f['IND_Strong_DA'] = bridge_da > 3
    f['IND_Market_Home'] = bridge_m < -0.1
    
    # --- Historial AH (Red Box Features) ---
    hist_count = f.get('HIST_SameAH_Count', 0)
    hist_wr = f.get('HIST_SameAH_WinRate', 0.0)
    hist_profit = f.get('HIST_SameAH_HighProfit', False)
    
    f['HIST_SameAH_Available'] = hist_count >= 3
    f['HIST_SameAH_Strong'] = hist_count >= 3 and hist_wr >= 0.65
    f['HIST_SameAH_Sniper'] = hist_count >= 4 and hist_wr >= 0.8  # 4/5 wins
    f['HIST_SameAH_Weak'] = hist_count >= 3 and hist_wr <= 0.35
    f['HIST_SameAH_Profit'] = hist_profit
    
    # Aliases para compatibilidad
    f['TRIANG_Home_Dominant'] = f['TRIANG_Home_Dom']
    f['IND_Home_Strong_DA'] = f['IND_Strong_DA']
    
    # --- Stats Partido Anterior ---
    prev_dda = f.get('prev_home_dDA', 0) or 0
    prev_dsot = f.get('prev_home_dSOT', 0) or 0
    f['PREV_Dom_DA'] = prev_dda > 15
    f['PREV_Dom_SOT'] = prev_dsot > 3
    f['PREV_Solid'] = prev_dda > 0
    # Aliases
    f['PREV_Home_Dominant_DA'] = f['PREV_Dom_DA']
    f['PREV_Home_Dominant_SOT'] = f['PREV_Dom_SOT']
    f['PREV_Home_Solid'] = f['PREV_Solid']
    
    # --- Falso Perdedor ---
    lost_prev = f.get('prev_home_exp_failed', False)
    f['FALSE_LOSER'] = lost_prev and (prev_dda > 10)
    f['FALSE_LOSER_Home'] = f['FALSE_LOSER']
    
    # --- Smart Money ---
    mov_dir = f.get('movement_dir', 'NONE')
    ah_delta = f.get('ah_delta', 0) or 0
    f['MONEY_Home'] = mov_dir == 'DOWN'
    f['MONEY_Strong'] = (mov_dir == 'DOWN') and (abs(ah_delta) >= 0.25)
    f['MONEY_With_Home'] = f['MONEY_Home']
    
    # --- Benchmark Tags ---
    current_ah = f.get('current_ah', 0) or 0
    prev_line = f.get('prev_home_exp_line', current_ah) or current_ah
    ah_gap = current_ah - prev_line
    f['TAG_Mejora'] = ah_gap > 0.1
    f['TAG_Empeora'] = ah_gap < -0.1
    f['TAG_Iguala'] = abs(ah_gap) <= 0.1
    
    # --- Expectativas ---
    exp_rate = f.get('exp_cover_own_rate', 0.5) or 0.5
    f['EXP_Reliable'] = exp_rate > 0.6
    f['EXP_Unreliable'] = exp_rate < 0.4
    f['EXP_Home_Reliable'] = f['EXP_Reliable']
    f['EXP_Home_Unreliable'] = f['EXP_Unreliable']
    
    # --- Contexto ---
    f['CTX_Urgency'] = f.get('home_bottom5', False)
    f['CTX_Home_Top'] = f.get('home_top5', False)
    f['CTX_Away_Bottom'] = f.get('away_bottom5', False)
    rank_diff = f.get('rank_diff', 0) or 0
    f['CTX_Derby'] = abs(rank_diff) < 3
    
    # --- Triggers Combinados ---
    f['TRIGGER_Sniper'] = f['FALSE_LOSER'] and f['MONEY_Home']
    f['TRIGGER_Trap'] = f['TAG_Empeora'] and f['PREV_Dom_DA']
    
    return f


def load_all_training_data(data_dir: str) -> List[Dict]:
    """
    Carga TODOS los archivos de datos de entrenamiento y los combina.
    
    Args:
        data_dir: Directorio con los archivos JSON
        
    Returns:
        Lista combinada de todos los partidos
    """
    matches = []
    data_path = Path(data_dir)
    
    if not data_path.exists():
        print(f"[ERROR] Directorio no encontrado: {data_dir}")
        return []
    
    # Archivos a cargar (combinamos todos)
    json_files = list(data_path.glob('*.json'))
    
    print(f"[INFO] Encontrados {len(json_files)} archivos JSON en {data_dir}")
    
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if isinstance(data, list):
                matches.extend(data)
            elif isinstance(data, dict):
                if 'partidos' in data:
                    matches.extend(data['partidos'])
                elif 'matches' in data:
                    matches.extend(data['matches'])
            
            print(f"  [OK] {json_file.name}: {len(data) if isinstance(data, list) else 'dict'}")
        except Exception as e:
            print(f"  [ERROR] Error en {json_file.name}: {e}")
    
    print(f"\n[INFO] Total partidos cargados: {len(matches)}")
    return matches


def build_training_dataframe(matches: List[Dict]) -> 'pd.DataFrame':
    """
    Construye DataFrame de features desde lista de partidos.
    Solo incluye partidos con resultado para entrenamiento.
    
    Returns:
        DataFrame con todas las features
    """
    import pandas as pd
    
    features_list = []
    skipped = 0
    
    for match in matches:
        features = build_match_features(match)
        if features and features.get('has_result') and features.get('current_ah') is not None:
            features = discretize_features(features)
            features_list.append(features)
        else:
            skipped += 1
    
    print(f"[OK] Partidos procesados: {len(features_list)}")
    print(f"[SKIP] Partidos sin resultado o sin AH: {skipped}")
    
    return pd.DataFrame(features_list)


# Función main para testing
if __name__ == '__main__':
    import sys
    
    # Path por defecto
    project_root = Path(__file__).parent.parent.parent
    data_dir = project_root / 'data'
    
    # Cargar datos
    matches = load_all_training_data(str(data_dir))
    
    if len(matches) < 10:
        print("[ERROR] Muy pocos partidos para testing")
        sys.exit(1)
    
    # Procesar un partido de ejemplo
    sample = matches[0]
    features = build_match_features(sample)
    
    print("\n📋 Features de ejemplo:")
    for k, v in sorted(features.items()):
        if v is not None:
            print(f"  {k}: {v}")
