"""
Gates de Calidad de Datos

Implementa los filtros/gates necesarios para asegurar que los datos
son válidos antes de usar las features derivadas de ellos.

Regla principal: DA_total >= 35 (Ataques Peligrosos)
"""

from typing import Dict, List, Optional, Tuple

# Constantes de validación
MIN_DA_TOTAL = 35
MIN_DA_TEAM = 35
MAX_DA_TOTAL = 150  # Valores mayores son outliers
MAX_SOT_SINGLE = 20  # SOT > 20 es outlier


def safe_int(val, default: int = 0) -> int:
    """Convierte a int de forma segura."""
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def safe_float(val, default: float = 0.0) -> float:
    """Convierte a float de forma segura."""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def is_da_valid(da_home: Optional[int], da_away: Optional[int]) -> bool:
    """
    Gate de ataques peligrosos.
    
    Regla: DA_total >= 35
    
    Args:
        da_home: DA del equipo local
        da_away: DA del equipo visitante
        
    Returns:
        True si los datos de DA son válidos para usar
    """
    da_h = safe_int(da_home, 0)
    da_a = safe_int(da_away, 0)
    total = da_h + da_a
    
    return total >= MIN_DA_TOTAL


def is_da_robust(da_home: Optional[int], da_away: Optional[int]) -> bool:
    """
    Gate robusto de DA: ambos equipos tienen DA significativo.
    
    Regla: DA_team >= 35 AND DA_total >= 70
    """
    da_h = safe_int(da_home, 0)
    da_a = safe_int(da_away, 0)
    
    return da_h >= MIN_DA_TEAM and da_a >= MIN_DA_TEAM


def is_shots_valid(shots: Optional[int], sot: Optional[int]) -> bool:
    """
    Sanidad de tiros: shots >= SOT siempre.
    
    Returns:
        True si los datos son coherentes
    """
    if shots is None or sot is None:
        return True  # Sin datos, no aplicamos gate
    
    s = safe_int(shots, 0)
    t = safe_int(sot, 0)
    
    return s >= t


def get_outlier_flag(da_home: Optional[int] = None, 
                     da_away: Optional[int] = None,
                     sot_home: Optional[int] = None,
                     sot_away: Optional[int] = None,
                     shots_home: Optional[int] = None,
                     shots_away: Optional[int] = None) -> int:
    """
    Detecta valores absurdos en las estadísticas.
    
    Returns:
        1 si hay outliers, 0 si todo está bien
    """
    da_h = safe_int(da_home, 0)
    da_a = safe_int(da_away, 0)
    sot_h = safe_int(sot_home, 0)
    sot_a = safe_int(sot_away, 0)
    shots_h = safe_int(shots_home, 0)
    shots_a = safe_int(shots_away, 0)
    
    # DA total > 150 es outlier
    if da_h + da_a > MAX_DA_TOTAL:
        return 1
    
    # SOT > 20 para un equipo es outlier
    if sot_h > MAX_SOT_SINGLE or sot_a > MAX_SOT_SINGLE:
        return 1
    
    # Shots < SOT es incoherente
    if shots_h < sot_h or shots_a < sot_a:
        return 1
    
    return 0


def extract_stats_from_rows(stats_rows: List[Dict]) -> Dict[str, int]:
    """
    Extrae estadísticas de un array de stats_rows.
    
    Returns:
        dict con shots_h, shots_a, sot_h, sot_a, attacks_h, attacks_a, da_h, da_a
    """
    result = {
        'shots_h': 0, 'shots_a': 0,
        'sot_h': 0, 'sot_a': 0,
        'attacks_h': 0, 'attacks_a': 0,
        'da_h': 0, 'da_a': 0
    }
    
    if not stats_rows:
        return result
    
    for stat in stats_rows:
        label = str(stat.get('label', '')).lower()
        home_val = safe_int(stat.get('home', 0))
        away_val = safe_int(stat.get('away', 0))
        
        if 'tiros a puerta' in label or 'shots on target' in label:
            result['sot_h'] = home_val
            result['sot_a'] = away_val
        elif 'tiros' in label or 'shots' in label:
            result['shots_h'] = home_val
            result['shots_a'] = away_val
        elif 'peligrosos' in label or 'dangerous' in label:
            result['da_h'] = home_val
            result['da_a'] = away_val
        elif 'ataques' in label or 'attacks' in label:
            result['attacks_h'] = home_val
            result['attacks_a'] = away_val
    
    return result


def compute_da_ok_for_source(stats_rows: List[Dict]) -> bool:
    """
    Verifica si una fuente tiene DA válido.
    
    Returns:
        True si DA_total >= 35 para esta fuente
    """
    stats = extract_stats_from_rows(stats_rows)
    return is_da_valid(stats['da_h'], stats['da_a'])


def compute_da_ok_count(sources: Dict[str, List[Dict]]) -> int:
    """
    Cuenta cuántas fuentes tienen DA válido.
    
    Args:
        sources: dict con {source_name: stats_rows}
        e.g., {'prev_home': [...], 'prev_away': [...], 'h2h_col3': [...]}
        
    Returns:
        int (0-N) indicando cuántas fuentes tienen DA válido
    """
    count = 0
    for source_name, stats_rows in sources.items():
        if compute_da_ok_for_source(stats_rows):
            count += 1
    return count


def validate_match_data(match: Dict) -> Dict[str, bool]:
    """
    Valida todos los datos de un partido.
    
    Returns:
        dict con flags de validación por fuente
    """
    result = {
        'has_result': False,
        'has_ah': False,
        'prev_home_ok': False,
        'prev_away_ok': False,
        'h2h_col3_ok': False,
        'ind_left_ok': False,
        'ind_right_ok': False,
        'da_ok_count': 0
    }
    
    # Resultado
    final_score = match.get('final_score', '')
    result['has_result'] = bool(final_score and ':' in str(final_score))
    
    # AH
    odds = match.get('main_match_odds', {})
    ah = odds.get('ah_linea')
    result['has_ah'] = ah is not None and ah != '-'
    
    # Sources
    sources_stats = {}
    
    # Prev Home
    prev_home = match.get('last_home_match') or {}
    ph_stats = prev_home.get('stats_rows', [])
    result['prev_home_ok'] = compute_da_ok_for_source(ph_stats)
    if ph_stats:
        sources_stats['prev_home'] = ph_stats
    
    # Prev Away
    prev_away = match.get('last_away_match') or {}
    pa_stats = prev_away.get('stats_rows', [])
    result['prev_away_ok'] = compute_da_ok_for_source(pa_stats)
    if pa_stats:
        sources_stats['prev_away'] = pa_stats
    
    # H2H Col3
    h2h_col3 = match.get('h2h_col3') or {}
    h2h_stats = h2h_col3.get('stats_rows', [])
    result['h2h_col3_ok'] = compute_da_ok_for_source(h2h_stats)
    if h2h_stats:
        sources_stats['h2h_col3'] = h2h_stats
    
    # Indirectas
    comp_ind = match.get('comparativas_indirectas') or {}
    ind_left = comp_ind.get('left') or {}
    ind_right = comp_ind.get('right') or {}
    
    il_stats = ind_left.get('stats_rows', [])
    result['ind_left_ok'] = compute_da_ok_for_source(il_stats)
    if il_stats:
        sources_stats['ind_left'] = il_stats
    
    ir_stats = ind_right.get('stats_rows', [])
    result['ind_right_ok'] = compute_da_ok_for_source(ir_stats)
    if ir_stats:
        sources_stats['ind_right'] = ir_stats
    
    result['da_ok_count'] = compute_da_ok_count(sources_stats)
    
    return result
