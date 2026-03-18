"""
Expectation Builder - LA CLAVE del sistema

Extrae features de "expectativa del mercado" para cada partido pasado.
El handicap de un partido pasado representa lo que el mercado esperaba.

Para cada fuente (PrevHome, PrevAway, H2H Col3, Indirectas):
- line_past: Línea AH del partido pasado
- was_favorite: Si el equipo era favorito
- expectation_strength: abs(línea) - cuánto exigía el mercado
- cover_state: Si cubrió su propia línea
- cover_current: Si cubriría la línea actual
"""

from typing import Dict, Optional, Tuple, Any
from .settle_asian import settle_ah, get_favorite_side, safe_float
from .gates import safe_int


def parse_score(score_str: str) -> Tuple[Optional[int], Optional[int]]:
    """Parsea score como '2:1' o '2-1' en (home, away)."""
    if not score_str or score_str in ['-', '?:?', '']:
        return None, None
    score_str = str(score_str).replace('-', ':')
    parts = score_str.split(':')
    if len(parts) != 2:
        return None, None
    try:
        return int(parts[0]), int(parts[1])
    except (ValueError, TypeError):
        return None, None


def get_expectation_strength_bin(exp_strength: float) -> str:
    """
    Categoriza la fuerza de expectativa en bins.
    
    - low: < 0.5 (partido cerrado)
    - mid: 0.5 - 1.0 (favorito claro)
    - high: > 1.0 (favorito muy fuerte)
    """
    if exp_strength < 0.5:
        return 'low'
    elif exp_strength <= 1.0:
        return 'mid'
    else:
        return 'high'


def extract_expectation_from_past_match(
    past_match: Dict,
    current_ah: float,
    team_name: str,
    is_team_current_home: bool
) -> Dict[str, Any]:
    """
    Extrae features de expectativa del mercado de un partido pasado.
    
    Args:
        past_match: Dict con datos del partido pasado (score, ah, home_team, away_team)
        current_ah: Línea AH del partido actual
        team_name: Nombre del equipo que analizamos
        is_team_current_home: Si el equipo es local en el partido actual
        
    Returns:
        Dict con features de expectativa:
        - line_past: float
        - was_favorite: bool
        - exp_strength: float
        - exp_strength_bin: str
        - goal_diff: int (desde perspectiva del equipo)
        - cover_own: str (W/HW/P/HL/L si cubrió su propia línea)
        - cover_current: str (si cubriría la línea actual)
        - beat_expectation: bool (underdog que ganó)
        - failed_expectation: bool (favorito que perdió)
        - profit_own: float
        - profit_current: float
    """
    result = {
        'line_past': None,
        'was_favorite': None,
        'exp_strength': None,
        'exp_strength_bin': None,
        'goal_diff': None,
        'cover_own': None,
        'cover_current': None,
        'beat_expectation': False,
        'failed_expectation': False,
        'profit_own': None,
        'profit_current': None,
        'has_data': False
    }
    
    if not past_match:
        return result
    
    # Obtener score
    score_str = past_match.get('score', '') or past_match.get('res1', '') or ''
    home_goals, away_goals = parse_score(score_str)
    
    if home_goals is None:
        return result
    
    # Obtener AH pasado
    ah_past = safe_float(past_match.get('ah', past_match.get('ah_line', past_match.get('ah1', 0))))
    
    # Determinar si el equipo era local o visitante en el partido pasado
    past_home_team = past_match.get('home_team', '')
    past_away_team = past_match.get('away_team', '')
    
    # Intentar determinar la posición del equipo en el partido pasado
    team_was_home = False
    if team_name:
        team_name_lower = team_name.lower()
        if past_home_team and team_name_lower in past_home_team.lower():
            team_was_home = True
        elif past_away_team and team_name_lower in past_away_team.lower():
            team_was_home = False
        else:
            # Fallback: usar la lógica de is_team_current_home
            # Para prev_home el equipo actual-home jugaba de local
            # Para prev_away el equipo actual-away jugaba de visitante
            team_was_home = is_team_current_home
    else:
        team_was_home = is_team_current_home
    
    # Calcular goal_diff desde perspectiva del equipo
    if team_was_home:
        team_goals = home_goals
        opp_goals = away_goals
        # AH desde perspectiva del equipo: si era home, usa ah_past directamente
        team_ah = ah_past
    else:
        team_goals = away_goals
        opp_goals = home_goals
        # Si era away, invierte el AH
        team_ah = -ah_past
    
    goal_diff = team_goals - opp_goals
    
    # ¿Era favorito? (team_ah < 0 significa que daba ventaja = favorito)
    was_favorite = team_ah < 0
    
    # Fuerza de expectativa
    exp_strength = abs(team_ah)
    
    # Calcular cover de su propia línea
    # Desde perspectiva HOME del partido pasado
    cover_own, _ = settle_ah(home_goals, away_goals, ah_past)
    
    # Si el equipo era away, invertir el outcome
    if not team_was_home:
        # Invertir: W <-> L, HW <-> HL
        cover_map = {'W': 'L', 'L': 'W', 'HW': 'HL', 'HL': 'HW', 'P': 'P'}
        cover_own = cover_map.get(cover_own, cover_own)
    
    # Calcular cover de la línea actual
    # Aplicamos la línea actual al resultado pasado
    # Pero debemos ajustar según la perspectiva
    if is_team_current_home:
        # El equipo es HOME en partido actual
        # Aplicar current_ah (que es desde perspectiva HOME actual)
        if team_was_home:
            # Mismo rol, podemos aplicar directamente
            cover_current, profit_current = settle_ah(home_goals, away_goals, current_ah)
        else:
            # El equipo era AWAY en pasado pero es HOME en actual
            # Invertir goles para simular como si fuera home
            cover_current, profit_current = settle_ah(away_goals, home_goals, current_ah)
    else:
        # El equipo es AWAY en partido actual
        if team_was_home:
            # Era HOME en pasado, es AWAY en actual
            cover_current, profit_current = settle_ah(home_goals, away_goals, current_ah)
            # Invertir resultado porque estamos viendo desde perspectiva away
            cover_map = {'W': 'L', 'L': 'W', 'HW': 'HL', 'HL': 'HW', 'P': 'P'}
            cover_current = cover_map.get(cover_current, cover_current)
            profit_current = -profit_current if cover_current != 'P' else 0
        else:
            # Era AWAY en pasado, es AWAY en actual
            cover_current, _ = settle_ah(home_goals, away_goals, current_ah)
            cover_map = {'W': 'L', 'L': 'W', 'HW': 'HL', 'HL': 'HW', 'P': 'P'}
            cover_current = cover_map.get(cover_current, cover_current)
    
    # Beat expectation: underdog que ganó
    beat_expectation = not was_favorite and goal_diff > 0
    
    # Failed expectation: favorito que perdió
    failed_expectation = was_favorite and goal_diff < 0
    
    result = {
        'line_past': team_ah,
        'was_favorite': was_favorite,
        'exp_strength': exp_strength,
        'exp_strength_bin': get_expectation_strength_bin(exp_strength),
        'goal_diff': goal_diff,
        'cover_own': cover_own,
        'cover_current': cover_current,
        'beat_expectation': beat_expectation,
        'failed_expectation': failed_expectation,
        'has_data': True
    }
    
    return result


def extract_all_expectations(match: Dict) -> Dict[str, Dict]:
    """
    Extrae expectativas de todas las fuentes disponibles.
    
    Returns:
        Dict con expectativas por fuente:
        - prev_home_exp
        - prev_away_exp
        - h2h_col3_exp
        - ind_left_exp
        - ind_right_exp
    """
    result = {}
    
    # Obtener AH actual
    odds = match.get('main_match_odds', {})
    current_ah = safe_float(odds.get('ah_linea', 0))
    
    home_name = match.get('home_name', '')
    away_name = match.get('away_name', '')
    
    # Prev Home (último partido en casa del equipo local)
    prev_home = match.get('last_home_match') or {}
    result['prev_home_exp'] = extract_expectation_from_past_match(
        prev_home, current_ah, home_name, is_team_current_home=True
    )
    
    # Prev Away (último partido fuera del equipo visitante)
    prev_away = match.get('last_away_match') or {}
    result['prev_away_exp'] = extract_expectation_from_past_match(
        prev_away, current_ah, away_name, is_team_current_home=False
    )
    
    # H2H Stadium (partido directo home vs away en este estadio)
    h2h_stadium = match.get('h2h_stadium') or {}
    result['h2h_stadium_exp'] = extract_expectation_from_past_match(
        h2h_stadium, current_ah, home_name, is_team_current_home=True
    )
    
    # H2H General
    h2h_general = match.get('h2h_general') or {}
    result['h2h_general_exp'] = extract_expectation_from_past_match(
        h2h_general, current_ah, home_name, is_team_current_home=True
    )
    
    # H2H Col3
    h2h_col3 = match.get('h2h_col3') or {}
    result['h2h_col3_exp'] = extract_expectation_from_past_match(
        h2h_col3, current_ah, home_name, is_team_current_home=True
    )
    
    # Indirectas
    comp_ind = match.get('comparativas_indirectas') or {}
    
    # Indirecta izquierda (Home vs Rival común)
    ind_left = comp_ind.get('left') or {}
    result['ind_left_exp'] = extract_expectation_from_past_match(
        ind_left, current_ah, home_name, is_team_current_home=True
    )
    
    # Indirecta derecha (Rival común vs Away)
    ind_right = comp_ind.get('right') or {}
    result['ind_right_exp'] = extract_expectation_from_past_match(
        ind_right, current_ah, away_name, is_team_current_home=False
    )
    
    return result


def summarize_expectations(expectations: Dict[str, Dict]) -> Dict[str, Any]:
    """
    Crea un resumen de expectativas para features consolidadas.
    
    Returns:
        Dict con:
        - sources_with_data: int
        - fav_count: int (cuántas veces era favorito)
        - dog_count: int (cuántas veces era underdog)
        - cover_own_rate: float (tasa de cobertura propia)
        - cover_current_rate: float (tasa de cobertura con línea actual)
        - avg_exp_strength: float
        - beat_expectation_count: int
        - failed_expectation_count: int
    """
    sources_with_data = 0
    fav_count = 0
    dog_count = 0
    covers_own = []
    covers_current = []
    exp_strengths = []
    beat_count = 0
    failed_count = 0
    
    for source_name, exp_data in expectations.items():
        if not exp_data.get('has_data'):
            continue
        
        sources_with_data += 1
        
        if exp_data.get('was_favorite'):
            fav_count += 1
        else:
            dog_count += 1
        
        cover_own = exp_data.get('cover_own')
        if cover_own in ['W', 'HW']:
            covers_own.append(1)
        elif cover_own in ['L', 'HL']:
            covers_own.append(0)
        # Push no cuenta
        
        cover_current = exp_data.get('cover_current')
        if cover_current in ['W', 'HW']:
            covers_current.append(1)
        elif cover_current in ['L', 'HL']:
            covers_current.append(0)
        
        if exp_data.get('exp_strength') is not None:
            exp_strengths.append(exp_data['exp_strength'])
        
        if exp_data.get('beat_expectation'):
            beat_count += 1
        if exp_data.get('failed_expectation'):
            failed_count += 1
    
    return {
        'sources_with_data': sources_with_data,
        'fav_count': fav_count,
        'dog_count': dog_count,
        'cover_own_rate': sum(covers_own) / len(covers_own) if covers_own else None,
        'cover_current_rate': sum(covers_current) / len(covers_current) if covers_current else None,
        'avg_exp_strength': sum(exp_strengths) / len(exp_strengths) if exp_strengths else None,
        'beat_expectation_count': beat_count,
        'failed_expectation_count': failed_count
    }
