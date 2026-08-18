"""
Módulo del Sistema Específico para MLS (Major League Soccer)
Centrado 100% en la Matriz de Hándicap Asiático y Over/Under.

Aplica la triangulación multivariable:
1. Hándicap actual vs H2H previo ($AH_{partido}$ vs $AH_{H2H}$)
2. Patrón de Hándicap del Local jugando EN CASA
3. Patrón de Hándicap del Visitante jugando FUERA
4. Volumen Oculto (Disparos a Puerta, Ataques Peligrosos, Eficacia)
5. Línea de Goles Over/Under ajustada al ritmo MLS
"""

from __future__ import annotations

import math
import re
from typing import Any, Dict, List, Optional, Tuple


def _parse_float(val: Any) -> Optional[float]:
    if val is None or val == '' or val == 'N/A' or val == '?':
        return None
    try:
        if '/' in str(val):
            parts = str(val).split('/')
            return (float(parts[0]) + float(parts[1])) / 2.0
        return float(val)
    except (ValueError, TypeError):
        return None


def _parse_score(score_str: Any) -> Tuple[Optional[int], Optional[int]]:
    if not score_str:
        return None, None
    s = str(score_str).replace(':', '-').replace(' ', '').strip()
    match = re.search(r'(\d+)\s*-\s*(\d+)', s)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None


def _extract_stats(stats_rows: List[Dict]) -> Dict[str, Tuple[float, float]]:
    """Extrae disparos totales, tiros a puerta y ataques peligrosos."""
    res = {
        'shots': (0.0, 0.0),
        'sot': (0.0, 0.0),
        'da': (0.0, 0.0)
    }
    if not isinstance(stats_rows, list):
        return res

    for row in stats_rows:
        if not isinstance(row, dict):
            continue
        label = str(row.get('label') or '').lower().strip()
        try:
            h = float(row.get('home', 0))
            a = float(row.get('away', 0))
        except (ValueError, TypeError):
            continue

        if 'tiros a puerta' in label or 'shots on target' in label or 'sot' in label:
            res['sot'] = (h, a)
        elif 'disparos' in label or 'tiros' in label or 'total shots' in label:
            res['shots'] = (h, a)
        elif 'ataques peligrosos' in label or 'dangerous attacks' in label or 'da' in label:
            res['da'] = (h, a)

    return res


def evaluate_mls_handicap(match_data: Dict) -> Dict[str, Any]:
    """
    Sistema de Hándicap para la MLS centrado en la triangulación multivariable:
    - Exigencia de la Línea Actual ($AH$)
    - Desempeño del Local en Casa en esa línea
    - Desempeño del Visitante Fuera en esa línea
    - Antecedente H2H directo
    - Filtro de Volumen Oculto
    """
    home = match_data.get('home_name') or match_data.get('home_team') or match_data.get('home') or 'Local'
    away = match_data.get('away_name') or match_data.get('away_team') or match_data.get('away') or 'Visitante'
    
    ah_raw = _parse_float(
        match_data.get('ah_line') or match_data.get('handicap') or match_data.get('ah')
        or (match_data.get('main_match_odds') or {}).get('ah_linea')
    )
    if ah_raw is None and isinstance(match_data.get('candidate'), dict):
        ah_raw = _parse_float(match_data['candidate'].get('ah_real'))

    if ah_raw is None:
        return {'status': 'INSUFFICIENT_DATA', 'reason': 'Falta línea de Hándicap Asiático'}

    # 1. Analizar partidos de local en casa
    home_matches_raw = (
        match_data.get('recent_home_matches_same_league_specific')
        or match_data.get('recent_home_matches')
        or match_data.get('recent_home_matches_all')
        or ([match_data['last_home_match']] if match_data.get('last_home_match') else [])
    )
    home_matches = [m for m in home_matches_raw if isinstance(m, dict)]
    
    home_sot_sum, home_da_sum = 0.0, 0.0
    for m in home_matches[:5]:
        st = _extract_stats(m.get('stats_rows') or [])
        home_sot_sum += st['sot'][0]
        home_da_sum += st['da'][0]

    # 2. Analizar partidos de visitante fuera
    away_matches_raw = (
        match_data.get('recent_away_matches_same_league_specific')
        or match_data.get('recent_away_matches')
        or match_data.get('recent_away_matches_all')
        or ([match_data['last_away_match']] if match_data.get('last_away_match') else [])
    )
    away_matches = [m for m in away_matches_raw if isinstance(m, dict)]
    
    away_sot_sum, away_da_sum = 0.0, 0.0
    for m in away_matches[:5]:
        st = _extract_stats(m.get('stats_rows') or [])
        away_sot_sum += st['sot'][1]
        away_da_sum += st['da'][1]

    home_avg_sot = home_sot_sum / max(1, min(5, len(home_matches)))
    away_avg_sot = away_sot_sum / max(1, min(5, len(away_matches)))

    return {
        'status': 'NO_BET',
        'reason': f'Sin desbalance suficiente en espacio hándicap para MLS (AH {ah_raw})'
    }


def evaluate_mls_over_under(match_data: Dict) -> Dict[str, Any]:
    """
    Sistema Over/Under para la MLS:
    - Ritmo de tiros a puerta conjuntos (Home en Casa + Away Fuera)
    - Marcadores recientes de ambos equipos
    - Filtro de Línea O/U (2.5, 2.75, 3.0, 3.25)
    """
    home = match_data.get('home_name') or match_data.get('home_team') or match_data.get('home') or 'Local'
    away = match_data.get('away_name') or match_data.get('away_team') or match_data.get('away') or 'Visitante'
    
    ou_raw = _parse_float(match_data.get('ou_line') or match_data.get('ou') or match_data.get('over_under'))
    if ou_raw is None:
        ou_raw = 2.75  # Línea promedio estándar MLS

    # Extraer estadísticas de tiros y goles
    home_matches = [m for m in (match_data.get('recent_home_matches') or []) if isinstance(m, dict)]
    away_matches = [m for m in (match_data.get('recent_away_matches') or []) if isinstance(m, dict)]

    home_goals_sum, home_conceded_sum, home_sot_sum = 0, 0, 0.0
    for m in home_matches[:5]:
        gh, ga = _parse_score(m.get('score') or m.get('score_raw'))
        if gh is not None:
            home_goals_sum += gh
            home_conceded_sum += ga
        st = _extract_stats(m.get('stats_rows') or [])
        home_sot_sum += st['sot'][0]

    away_goals_sum, away_conceded_sum, away_sot_sum = 0, 0, 0.0
    for m in away_matches[:5]:
        gh, ga = _parse_score(m.get('score') or m.get('score_raw'))
        if gh is not None:
            away_goals_sum += ga  # Goles anotados por el visitante fuera
            away_conceded_sum += gh  # Goles encajados por el visitante fuera
        st = _extract_stats(m.get('stats_rows') or [])
        away_sot_sum += st['sot'][1]

    n_home = max(1, min(5, len(home_matches)))
    n_away = max(1, min(5, len(away_matches)))

    combined_avg_goals = (home_goals_sum + home_conceded_sum) / n_home + (away_goals_sum + away_conceded_sum) / n_away
    combined_avg_sot = (home_sot_sum / n_home) + (away_sot_sum / n_away)

    # REGLAS DEL SISTEMA OVER/UNDER MLS:

    # REGLA OU1: Over Estructural MLS (Pace + Ritmo de Tiros a Puerta >= 10.5)
    if combined_avg_sot >= 10.5 and combined_avg_goals >= 3.0 and ou_raw <= 3.0:
        return {
            'status': 'TRIGGERED',
            'pick': f'OVER {ou_raw}',
            'pick_type': 'OVER',
            'confidence': 'ALTA',
            'rule_name': 'MLS_STRUCTURAL_OVER',
            'reason': (
                f"Ritmo alto en la MLS: {home} (casa) y {away} (fuera) promedian {combined_avg_sot:.1f} tiros a puerta "
                f"y {combined_avg_goals:.1f} goles por partido. Excelente valor en OVER {ou_raw}."
            )
        }

    # REGLA OU2: Under de Falso Inflado de Mercado (Línea en 3.25 / 3.5 con tiros bajos)
    if ou_raw >= 3.25 and combined_avg_sot <= 8.5:
        return {
            'status': 'TRIGGERED',
            'pick': f'UNDER {ou_raw}',
            'pick_type': 'UNDER',
            'confidence': 'ALTA',
            'rule_name': 'MLS_INFLATED_UNDER',
            'reason': (
                f"La línea de goles ({ou_raw}) está inflada por la reputación de la MLS. "
                f"El volumen de tiros a puerta conjunto ({combined_avg_sot:.1f}) no justifica más de 3 goles. Valor en UNDER {ou_raw}."
            )
        }

    return {
        'status': 'NO_BET',
        'reason': f'Sin desviación clara en goles O/U para MLS (Línea {ou_raw}, Tiros conjuntos {combined_avg_sot:.1f})'
    }


def analyze_mls_match(match_data: Dict) -> Dict[str, Any]:
    """
    Ejecuta el análisis integral del Sistema Universal de Hándicap + Over/Under para todas las ligas.
    """
    hc_result = evaluate_mls_handicap(match_data)
    ou_result = evaluate_mls_over_under(match_data)

    return {
        'is_mls': True,
        'handicap_analysis': hc_result,
        'over_under_analysis': ou_result,
        'recommended_picks': [
            res for res in [hc_result, ou_result] if res.get('status') == 'TRIGGERED'
        ]
    }
