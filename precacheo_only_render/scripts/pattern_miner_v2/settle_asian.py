"""
Liquidación Asiática Exacta (AH y O/U)

Implementa la liquidación correcta incluyendo cuartos:
- x.25 = (x.0 + x.5) / 2
- x.75 = (x.5 + x+1.0) / 2

Esto es imprescindible para que el ROI del patrón sea real (half-win/half-loss/push).
"""

from typing import Tuple, Literal

OutcomeType = Literal['W', 'HW', 'P', 'HL', 'L']


def safe_float(val, default: float = 0.0) -> float:
    """Convierte a float de forma segura."""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def settle_ah(home_goals: int, away_goals: int, ah_line: float) -> Tuple[OutcomeType, float]:
    """
    Calcula el settlement de AH desde perspectiva HOME.
    
    Convención:
    - ah_line > 0: HOME recibe ventaja (es underdog)
    - ah_line < 0: HOME da ventaja (es favorito)
    - ah_line = 0: Pick'em (empate = push)
    
    Args:
        home_goals: Goles del equipo local
        away_goals: Goles del equipo visitante
        ah_line: Línea de handicap asiático (desde perspectiva HOME)
        
    Returns:
        Tuple[outcome, payout_score]:
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


def settle_ou(total_goals: int, ou_line: float, side: str = 'OVER') -> Tuple[OutcomeType, float]:
    """
    Calcula el settlement de O/U.
    
    Args:
        total_goals: Total de goles en el partido
        ou_line: Línea de Over/Under
        side: 'OVER' o 'UNDER'
        
    Returns:
        Tuple[outcome, payout_score] desde perspectiva del side elegido
    """
    diff = total_goals - ou_line
    
    if side == 'OVER':
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
    else:  # UNDER
        if diff < -0.25:
            return 'W', 1.0
        elif diff < 0:
            return 'HW', 0.5
        elif diff == 0:
            return 'P', 0.0
        elif diff <= 0.25:
            return 'HL', -0.5
        else:
            return 'L', -1.0


def calculate_profit(odds: float, outcome: OutcomeType, stake: float = 1.0) -> float:
    """
    Calcula el profit real considerando half-win/half-loss.
    
    Args:
        odds: Cuota decimal (e.g., 1.80)
        outcome: Resultado del settle ('W', 'HW', 'P', 'HL', 'L')
        stake: Cantidad apostada (default: 1.0)
        
    Returns:
        Profit (puede ser negativo)
    """
    if outcome == 'W':
        return stake * (odds - 1)
    elif outcome == 'HW':
        return stake * (odds - 1) / 2
    elif outcome == 'P':
        return 0.0
    elif outcome == 'HL':
        return -stake / 2
    else:  # L
        return -stake


def settle_ah_with_profit(home_goals: int, away_goals: int, ah_line: float, 
                          odds: float = 1.80, stake: float = 1.0) -> Tuple[OutcomeType, float]:
    """
    Settle AH y calcula profit en un solo paso.
    
    Returns:
        Tuple[outcome, profit]
    """
    outcome, _ = settle_ah(home_goals, away_goals, ah_line)
    profit = calculate_profit(odds, outcome, stake)
    return outcome, profit


def settle_ou_with_profit(total_goals: int, ou_line: float, side: str = 'OVER',
                          odds: float = 1.80, stake: float = 1.0) -> Tuple[OutcomeType, float]:
    """
    Settle O/U y calcula profit en un solo paso.
    
    Returns:
        Tuple[outcome, profit]
    """
    outcome, _ = settle_ou(total_goals, ou_line, side)
    profit = calculate_profit(odds, outcome, stake)
    return outcome, profit


def calculate_roi(profits: list, stake: float = 1.0) -> float:
    """
    Calcula ROI de una lista de profits.
    
    ROI = sum(profits) / (n * stake)
    
    Args:
        profits: Lista de profits (positivos y negativos)
        stake: Cantidad apostada por apuesta
        
    Returns:
        ROI como decimal (0.20 = 20%)
    """
    if not profits:
        return 0.0
    return sum(profits) / (len(profits) * stake)


def get_breakdown(outcomes: list) -> dict:
    """
    Genera breakdown de resultados.
    
    Returns:
        dict con conteo de W, HW, P, HL, L
    """
    breakdown = {'W': 0, 'HW': 0, 'P': 0, 'HL': 0, 'L': 0}
    for outcome in outcomes:
        if outcome in breakdown:
            breakdown[outcome] += 1
    return breakdown


# Funciones helper para interpretar AH
def get_favorite_side(ah_line: float) -> str:
    """
    Determina quién es favorito según la línea AH.
    
    Convención del usuario:
    - AH = 0: Favorito visitante (empate = push, DNB visitante)
    - AH > 0 (e.g., +1): Favorito local
    - AH < 0 (e.g., -1): Favorito visitante
    
    Returns:
        'HOME', 'AWAY', o 'NEUTRAL'
    """
    if ah_line > 0:
        return 'HOME'  # Local recibe ventaja = Local es underdog = AWAY favorito? 
        # Wait, revisando: si ah_line > 0, HOME recibe goles, así que HOME es underdog
        # Pero la convención del usuario dice AH = +1 -> favorito local
        # Esto parece contradictorio. Revisemos el informe:
        # "AH = +1 → favorito local"
        # Esto significa que cuando el AH mostrado es +1, el LOCAL es favorito.
        # Pero en terminología estándar, +1 para HOME significa que HOME recibe 1 gol de ventaja.
        # El usuario tiene una convención específica que debemos respetar.
        return 'HOME'
    elif ah_line < 0:
        return 'AWAY'
    else:
        return 'AWAY'  # AH = 0 -> favorito visitante según convención


def get_ah_family(ah_line: float) -> str:
    """
    Clasifica la línea AH en una familia.
    
    Familias:
    - H0: Pick'em (0)
    - H0.5: ±0.25, ±0.5, ±0.75 (agrupados por petición usuario)
    - H1.0: ±1.0
    - H1.25_1.75: Familia "2+ goles"
    - H2.0_plus: ±2.0 o más
    """
    mag = abs(ah_line)
    
    if mag < 0.01:
        return 'H0'
    elif mag <= 0.75: # Agrupa 0.25, 0.5 y 0.75
        return 'H0.5'
    elif mag <= 1.0:
        return 'H1.0'
    elif mag <= 1.75:
        return 'H1.25_1.75'
    else:
        return 'H2.0_plus'


def get_ou_family(ou_line: float) -> str:
    """
    Clasifica la línea O/U en una familia.
    """
    if ou_line <= 2.25:
        return 'OU2.0_2.25'
    elif ou_line <= 2.5:
        return 'OU2.5'
    elif ou_line <= 3.0:
        return 'OU2.75_3.0'
    else:
        return 'OU3.0_plus'
