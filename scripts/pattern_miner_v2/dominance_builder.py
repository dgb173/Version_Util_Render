"""
Dominance Builder - Features de dominancia y calidad

Extrae diferencias de stats (dDA, dSOT, dShots) y métricas de calidad
para cada fuente de datos.

Regla de 3: Usar diferencias, no valores absolutos.
"""

from typing import Dict, List, Any, Optional
from .gates import extract_stats_from_rows, safe_int, is_da_valid


def get_dsot_bin(dsot: int) -> str:
    """
    Categoriza diferencia de SOT en bins.
    
    - neg_strong: ≤ -3
    - neg: -2 a -1
    - neutral: 0 a +1
    - pos: +2 a +3
    - pos_strong: ≥ +4
    """
    if dsot <= -3:
        return 'neg_strong'
    elif dsot <= -1:
        return 'neg'
    elif dsot <= 1:
        return 'neutral'
    elif dsot <= 3:
        return 'pos'
    else:
        return 'pos_strong'


def get_dda_bin(dda: int) -> str:
    """
    Categoriza diferencia de DA en bins.
    
    - neg_strong: ≤ -30
    - neg: -29 a -10
    - neutral: -9 a +9
    - pos: +10 a +29
    - pos_strong: ≥ +30
    """
    if dda <= -30:
        return 'neg_strong'
    elif dda <= -10:
        return 'neg'
    elif dda <= 9:
        return 'neutral'
    elif dda <= 29:
        return 'pos'
    else:
        return 'pos_strong'


def get_tempo_bin(total: int) -> str:
    """
    Categoriza ritmo total (para O/U).
    
    - low: < 50
    - mid: 50-80
    - high: > 80
    """
    if total < 50:
        return 'low'
    elif total <= 80:
        return 'mid'
    else:
        return 'high'


def extract_dominance_from_stats(
    stats_rows: List[Dict],
    team_is_home: bool = True,
    da_gate: bool = True
) -> Dict[str, Any]:
    """
    Extrae features de dominancia de un set de stats.
    
    Args:
        stats_rows: Lista de stats [{label, home, away}, ...]
        team_is_home: Si el equipo que analizamos era local
        da_gate: Si aplicar gate de DA (ignorar DA si total < 35)
        
    Returns:
        Dict con:
        - dSOT: int (diferencia desde perspectiva del equipo)
        - dSOT_bin: str
        - dShots: int
        - dAttacks: int
        - dDA: int (si DA_ok)
        - dDA_bin: str (si DA_ok)
        - SOT_total: int
        - DA_total: int
        - DA_ok: bool
        - quality: float (SOT/Shots si hay shots)
        - tempo_bin: str
    """
    result = {
        'dSOT': None,
        'dSOT_bin': None,
        'dShots': None,
        'dAttacks': None,
        'dDA': None,
        'dDA_bin': None,
        'SOT_total': None,
        'DA_total': None,
        'DA_ok': False,
        'quality': None,
        'tempo_bin': None,
        'has_data': False
    }
    
    if not stats_rows:
        return result
    
    stats = extract_stats_from_rows(stats_rows)
    
    # Calcular diferencias desde perspectiva del equipo
    if team_is_home:
        dSOT = stats['sot_h'] - stats['sot_a']
        dShots = stats['shots_h'] - stats['shots_a']
        dAttacks = stats['attacks_h'] - stats['attacks_a']
        dDA = stats['da_h'] - stats['da_a']
        team_shots = stats['shots_h']
        team_sot = stats['sot_h']
    else:
        dSOT = stats['sot_a'] - stats['sot_h']
        dShots = stats['shots_a'] - stats['shots_h']
        dAttacks = stats['attacks_a'] - stats['attacks_h']
        dDA = stats['da_a'] - stats['da_h']
        team_shots = stats['shots_a']
        team_sot = stats['sot_a']
    
    SOT_total = stats['sot_h'] + stats['sot_a']
    DA_total = stats['da_h'] + stats['da_a']
    
    # Gate de DA
    DA_ok = is_da_valid(stats['da_h'], stats['da_a']) if da_gate else True
    
    # Calidad de ataque
    quality = team_sot / team_shots if team_shots > 0 else None
    
    result = {
        'dSOT': dSOT,
        'dSOT_bin': get_dsot_bin(dSOT),
        'dShots': dShots,
        'dAttacks': dAttacks,
        'dDA': dDA if DA_ok else None,
        'dDA_bin': get_dda_bin(dDA) if DA_ok else None,
        'SOT_total': SOT_total,
        'DA_total': DA_total if DA_ok else None,
        'DA_ok': DA_ok,
        'quality': quality,
        'tempo_bin': get_tempo_bin(DA_total) if DA_ok else None,
        'has_data': True
    }
    
    return result


def extract_all_dominance(match: Dict) -> Dict[str, Dict]:
    """
    Extrae dominancia de todas las fuentes disponibles.
    
    Returns:
        Dict con dominancia por fuente:
        - prev_home_dom
        - prev_away_dom
        - h2h_col3_dom
        - ind_left_dom
        - ind_right_dom
    """
    result = {}
    
    home_name = match.get('home_name', '')
    away_name = match.get('away_name', '')
    
    # Prev Home
    prev_home = match.get('last_home_match') or {}
    ph_stats = prev_home.get('stats_rows', [])
    # En prev_home, el equipo local actual jugaba de local
    result['prev_home_dom'] = extract_dominance_from_stats(ph_stats, team_is_home=True)
    
    # Prev Away
    prev_away = match.get('last_away_match') or {}
    pa_stats = prev_away.get('stats_rows', [])
    # En prev_away, el equipo visitante actual jugaba de visitante
    result['prev_away_dom'] = extract_dominance_from_stats(pa_stats, team_is_home=False)
    
    # H2H Col3
    h2h_col3 = match.get('h2h_col3') or {}
    h2h_stats = h2h_col3.get('stats_rows', [])
    result['h2h_col3_dom'] = extract_dominance_from_stats(h2h_stats, team_is_home=True)
    
    # Indirectas
    comp_ind = match.get('comparativas_indirectas') or {}
    
    # Indirecta izquierda (Home vs Rival común)
    ind_left = comp_ind.get('left') or {}
    il_stats = ind_left.get('stats_rows', [])
    # Determinar si el equipo home actual era home en la indirecta
    il_home_team = ind_left.get('home_team', '')
    il_is_home = home_name.lower() in il_home_team.lower() if home_name and il_home_team else True
    result['ind_left_dom'] = extract_dominance_from_stats(il_stats, team_is_home=il_is_home)
    
    # Indirecta derecha (Rival común vs Away)
    ind_right = comp_ind.get('right') or {}
    ir_stats = ind_right.get('stats_rows', [])
    # Determinar si el equipo away actual era away en la indirecta
    ir_away_team = ind_right.get('away_team', '')
    ir_is_away = away_name.lower() in ir_away_team.lower() if away_name and ir_away_team else True
    result['ind_right_dom'] = extract_dominance_from_stats(ir_stats, team_is_home=not ir_is_away)
    
    return result


def summarize_dominance(dominance: Dict[str, Dict]) -> Dict[str, Any]:
    """
    Crea un resumen de dominancia consolidado.
    
    Returns:
        Dict con:
        - sources_with_data: int
        - home_dominant_count: int (cuántas fuentes muestran dSOT > 0 para home)
        - away_dominant_count: int
        - avg_dSOT: float
        - avg_dDA: float (solo fuentes con DA_ok)
        - da_ok_count: int
        - consistent_dominance: bool (todas las fuentes apuntan al mismo lado)
    """
    sources_with_data = 0
    home_dominant = 0
    away_dominant = 0
    dsot_list = []
    dda_list = []
    da_ok_count = 0
    
    for source_name, dom_data in dominance.items():
        if not dom_data.get('has_data'):
            continue
        
        sources_with_data += 1
        
        dsot = dom_data.get('dSOT', 0)
        if dsot is not None:
            dsot_list.append(dsot)
            if dsot > 0:
                home_dominant += 1
            elif dsot < 0:
                away_dominant += 1
        
        if dom_data.get('DA_ok'):
            da_ok_count += 1
            dda = dom_data.get('dDA')
            if dda is not None:
                dda_list.append(dda)
    
    avg_dSOT = sum(dsot_list) / len(dsot_list) if dsot_list else None
    avg_dDA = sum(dda_list) / len(dda_list) if dda_list else None
    
    # Dominancia consistente: todas apuntan al mismo lado
    if sources_with_data > 0:
        consistent = (home_dominant == sources_with_data) or (away_dominant == sources_with_data)
    else:
        consistent = False
    
    return {
        'sources_with_data': sources_with_data,
        'home_dominant_count': home_dominant,
        'away_dominant_count': away_dominant,
        'avg_dSOT': avg_dSOT,
        'avg_dDA': avg_dDA,
        'da_ok_count': da_ok_count,
        'consistent_dominance': consistent
    }


def get_dominance_consensus(dominance: Dict[str, Dict]) -> str:
    """
    Determina el consenso de dominancia.
    
    Returns:
        'HOME', 'AWAY', o 'MIXED'
    """
    summary = summarize_dominance(dominance)
    
    if summary['sources_with_data'] == 0:
        return 'NO_DATA'
    
    if summary['consistent_dominance']:
        if summary['home_dominant_count'] > 0:
            return 'HOME'
        else:
            return 'AWAY'
    
    # Mayoría
    if summary['home_dominant_count'] > summary['away_dominant_count']:
        return 'HOME_LEAN'
    elif summary['away_dominant_count'] > summary['home_dominant_count']:
        return 'AWAY_LEAN'
    else:
        return 'MIXED'
