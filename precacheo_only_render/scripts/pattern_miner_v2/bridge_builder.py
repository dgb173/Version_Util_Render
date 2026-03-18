"""
Bridge Builder - Comparativas indirectas y puentes

Un puente conecta A y B a través de un rival común C:
- A vs C (indirecta izquierda o H2H)
- B vs C (indirecta derecha)

Features clave:
- M_gap: Gap de expectativa del mercado (línea A - línea B)
- D_gap: Gap de dominancia (dSOT_A - dSOT_B)
- Contradicción: cuando mercado y dominancia apuntan a lados opuestos
- Consenso: % de puentes que apuntan al mismo lado
"""

from typing import Dict, List, Any, Optional, Tuple
from .gates import safe_float, safe_int
from .expectation_builder import extract_expectation_from_past_match
from .dominance_builder import extract_dominance_from_stats


def get_m_gap_bin(m_gap: float) -> str:
    """
    Categoriza el gap de mercado.
    
    - home_strong: < -0.5 (mercado ve HOME mucho mejor)
    - home_better: -0.5 a -0.1
    - neutral: -0.1 a +0.1
    - away_better: +0.1 a +0.5
    - away_strong: > +0.5 (mercado ve AWAY mucho mejor)
    """
    if m_gap < -0.5:
        return 'home_strong'
    elif m_gap < -0.1:
        return 'home_better'
    elif m_gap <= 0.1:
        return 'neutral'
    elif m_gap <= 0.5:
        return 'away_better'
    else:
        return 'away_strong'


def get_d_gap_bin(d_gap: float) -> str:
    """
    Categoriza el gap de dominancia.
    
    - home_strong: > +3 (HOME dominó mucho más en su puente)
    - home_better: +1 a +3
    - neutral: -1 a +1
    - away_better: -3 a -1
    - away_strong: < -3
    """
    if d_gap > 3:
        return 'home_strong'
    elif d_gap > 1:
        return 'home_better'
    elif d_gap >= -1:
        return 'neutral'
    elif d_gap >= -3:
        return 'away_better'
    else:
        return 'away_strong'


def get_consensus_bin(rate: float) -> str:
    """
    Categoriza tasa de consenso.
    
    - low: 0-49%
    - mid: 50-79%
    - high: 80-100%
    """
    if rate < 0.5:
        return 'low'
    elif rate < 0.8:
        return 'mid'
    else:
        return 'high'


def extract_bridge_features(
    ind_left: Dict,
    ind_right: Dict,
    current_ah: float,
    home_name: str,
    away_name: str
) -> Dict[str, Any]:
    """
    Extrae features de un puente (comparativa indirecta).
    
    El puente conecta:
    - Home vs Rival (ind_left)
    - Rival vs Away (ind_right)
    
    Args:
        ind_left: Dict con datos de indirecta izquierda
        ind_right: Dict con datos de indirecta derecha
        current_ah: Línea AH actual
        home_name: Nombre equipo local
        away_name: Nombre equipo visitante
        
    Returns:
        Dict con:
        - is_valid: bool
        - M_gap: float (gap de líneas: line_left - line_right)
        - M_gap_bin: str
        - D_gap_SOT: float (gap de dSOT)
        - D_gap_DA: float (gap de dDA, si ambos DA_ok)
        - contradiction_flag: bool
        - home_advantage: bool (si ambos gaps favorecen home)
        - away_advantage: bool (si ambos gaps favorecen away)
    """
    result = {
        'is_valid': False,
        'M_gap': None,
        'M_gap_bin': None,
        'D_gap_SOT': None,
        'D_gap_SOT_bin': None,
        'D_gap_DA': None,
        'D_gap_DA_bin': None,
        'contradiction_flag': False,
        'home_advantage': False,
        'away_advantage': False
    }
    
    if not ind_left or not ind_right:
        return result
    
    # Obtener líneas de AH de cada indirecta
    left_ah = safe_float(ind_left.get('ah_line', ind_left.get('ah', 0)))
    right_ah = safe_float(ind_right.get('ah_line', ind_right.get('ah', 0)))
    
    # Obtener stats
    left_stats = ind_left.get('stats_rows', [])
    right_stats = ind_right.get('stats_rows', [])
    
    if not left_stats and not right_stats:
        return result
    
    # Extraer dominancia
    # Para left: Home actual jugó, determinar si era home o away
    left_home_team = ind_left.get('home_team', '')
    left_is_home = home_name.lower() in left_home_team.lower() if home_name and left_home_team else True
    left_dom = extract_dominance_from_stats(left_stats, team_is_home=left_is_home)
    
    # Para right: Away actual jugó
    right_away_team = ind_right.get('away_team', '')
    right_is_away = away_name.lower() in right_away_team.lower() if away_name and right_away_team else True
    right_dom = extract_dominance_from_stats(right_stats, team_is_home=not right_is_away)
    
    # Calcular M_gap (gap de mercado)
    # Si left_ah es la línea para Home en su partido indirecto
    # Necesitamos ajustar según perspectiva
    # Simplificación: usar las líneas tal cual y calcular diferencia
    # left_ah negativo = Home era favorito en left
    # right_ah positivo = Away del right era favorito (que es nuestro Away)
    
    # Ajustar para perspectiva del equipo:
    # - left_ah desde perspectiva de Home actual
    # - right_ah desde perspectiva de Away actual
    
    if left_is_home:
        home_line = left_ah
    else:
        home_line = -left_ah  # Invertir si era away
    
    if right_is_away:
        away_line = -right_ah  # Away recibe la inversa de la línea del home
    else:
        away_line = right_ah
    
    M_gap = home_line - away_line  # Negativo = Home tenía mejor línea (era más favorito)
    
    # Calcular D_gap (gap de dominancia)
    left_dSOT = left_dom.get('dSOT', 0) or 0
    right_dSOT = right_dom.get('dSOT', 0) or 0
    D_gap_SOT = left_dSOT - right_dSOT  # Positivo = Home dominó más
    
    # D_gap para DA
    D_gap_DA = None
    if left_dom.get('DA_ok') and right_dom.get('DA_ok'):
        left_dDA = left_dom.get('dDA', 0) or 0
        right_dDA = right_dom.get('dDA', 0) or 0
        D_gap_DA = left_dDA - right_dDA
    
    # Contradicción: mercado y dominancia apuntan a lados opuestos
    # M_gap < 0 = mercado favorece Home
    # D_gap > 0 = dominancia favorece Home
    m_favors_home = M_gap < -0.1
    m_favors_away = M_gap > 0.1
    d_favors_home = D_gap_SOT > 1
    d_favors_away = D_gap_SOT < -1
    
    contradiction = (m_favors_home and d_favors_away) or (m_favors_away and d_favors_home)
    
    # Ventajas claras
    home_advantage = (M_gap < -0.1) and (D_gap_SOT > 0)
    away_advantage = (M_gap > 0.1) and (D_gap_SOT < 0)
    
    result = {
        'is_valid': True,
        'M_gap': M_gap,
        'M_gap_bin': get_m_gap_bin(M_gap),
        'D_gap_SOT': D_gap_SOT,
        'D_gap_SOT_bin': get_d_gap_bin(D_gap_SOT),
        'D_gap_DA': D_gap_DA,
        'D_gap_DA_bin': get_d_gap_bin(D_gap_DA) if D_gap_DA is not None else None,
        'contradiction_flag': contradiction,
        'home_advantage': home_advantage,
        'away_advantage': away_advantage
    }
    
    return result


def build_all_bridges(match: Dict) -> Dict[str, Any]:
    """
    Construye todos los puentes disponibles para un partido.
    
    Returns:
        Dict con:
        - bridges: List de bridge features
        - bridge_count: int
        - M_gap_mean: float
        - D_gap_SOT_mean: float
        - D_gap_DA_mean: float (si hay)
        - contradiction_rate: float
        - consensus_rate: float
        - consensus_bin: str
        - overall_direction: str ('HOME', 'AWAY', 'MIXED')
    """
    result = {
        'bridges': [],
        'bridge_count': 0,
        'M_gap_mean': None,
        'D_gap_SOT_mean': None,
        'D_gap_DA_mean': None,
        'contradiction_rate': None,
        'consensus_rate': None,
        'consensus_bin': None,
        'overall_direction': 'NO_DATA'
    }
    
    # Obtener datos
    odds = match.get('main_match_odds', {})
    current_ah = safe_float(odds.get('ah_linea', 0))
    home_name = match.get('home_name', '')
    away_name = match.get('away_name', '')
    
    # Obtener comparativas indirectas
    comp_ind = match.get('comparativas_indirectas') or {}
    ind_left = comp_ind.get('left') or {}
    ind_right = comp_ind.get('right') or {}
    
    bridges = []
    
    # Puente 1: Indirectas estándar
    if ind_left and ind_right:
        bridge1 = extract_bridge_features(ind_left, ind_right, current_ah, home_name, away_name)
        if bridge1['is_valid']:
            bridge1['source'] = 'indirect_main'
            bridges.append(bridge1)
    
    # Puente 2: H2H Stadium vs último del oponente
    h2h_stadium = match.get('h2h_stadium') or {}
    if h2h_stadium.get('res1') or h2h_stadium.get('score'):
        # Usar H2H como "left" y prev_away como "right"
        prev_away = match.get('last_away_match') or {}
        if prev_away.get('score'):
            bridge2 = extract_bridge_features(h2h_stadium, prev_away, current_ah, home_name, away_name)
            if bridge2['is_valid']:
                bridge2['source'] = 'h2h_vs_prevaway'
                bridges.append(bridge2)
    
    # Puente 3: Prev Home vs Prev Away (comparativa directa de form reciente)
    prev_home = match.get('last_home_match') or {}
    prev_away = match.get('last_away_match') or {}
    if prev_home.get('score') and prev_away.get('score'):
        bridge3 = extract_bridge_features(prev_home, prev_away, current_ah, home_name, away_name)
        if bridge3['is_valid']:
            bridge3['source'] = 'prev_comparison'
            bridges.append(bridge3)
    
    if not bridges:
        return result
    
    # Calcular estadísticas consolidadas
    m_gaps = [b['M_gap'] for b in bridges if b['M_gap'] is not None]
    d_gaps_sot = [b['D_gap_SOT'] for b in bridges if b['D_gap_SOT'] is not None]
    d_gaps_da = [b['D_gap_DA'] for b in bridges if b['D_gap_DA'] is not None]
    contradictions = [1 if b['contradiction_flag'] else 0 for b in bridges]
    
    # Consenso: cuántos puentes apuntan al mismo lado
    home_signals = sum(1 for b in bridges if b['home_advantage'])
    away_signals = sum(1 for b in bridges if b['away_advantage'])
    total_signals = home_signals + away_signals
    
    if total_signals > 0:
        consensus_rate = max(home_signals, away_signals) / total_signals
    else:
        consensus_rate = 0.5
    
    # Dirección general
    if home_signals > away_signals:
        overall_direction = 'HOME'
    elif away_signals > home_signals:
        overall_direction = 'AWAY'
    else:
        overall_direction = 'MIXED'
    
    result = {
        'bridges': bridges,
        'bridge_count': len(bridges),
        'M_gap_mean': sum(m_gaps) / len(m_gaps) if m_gaps else None,
        'D_gap_SOT_mean': sum(d_gaps_sot) / len(d_gaps_sot) if d_gaps_sot else None,
        'D_gap_DA_mean': sum(d_gaps_da) / len(d_gaps_da) if d_gaps_da else None,
        'contradiction_rate': sum(contradictions) / len(contradictions) if contradictions else None,
        'consensus_rate': consensus_rate,
        'consensus_bin': get_consensus_bin(consensus_rate),
        'overall_direction': overall_direction
    }
    
    return result
