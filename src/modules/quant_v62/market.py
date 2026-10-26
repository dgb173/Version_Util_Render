"""Exact quarter-goal arithmetic. Lines are canonical for the selected side."""
from decimal import Decimal


def quarters(value):
    q = Decimal(str(value)) * 4
    if not q.is_finite() or q != q.to_integral_value():
        raise ValueError('La línea debe ser un múltiplo exacto de 0.25')
    return int(q)


def strength_q(raw_home_ah, venue):
    if venue not in ('home', 'away', 'neutral'):
        raise ValueError('Perspectiva no válida')
    if venue == 'neutral':
        raise ValueError('En campo neutral debe declararse home/away administrativo')
    return quarters(raw_home_ah) * (1 if venue == 'home' else -1)


def legs(q):
    if type(q) is not int:
        raise ValueError('Se requieren cuartos enteros')
    return (q - 1, q + 1) if q % 2 else (q,)


def settle(market, line_q, home_goals, away_goals, odds=2.0):
    if market not in ('AH_HOME', 'AH_AWAY', 'OVER', 'UNDER'):
        raise ValueError('Mercado desconocido')
    if any(type(g) is not int or g < 0 for g in (home_goals, away_goals)):
        raise ValueError('Marcador no válido')
    price = Decimal(str(odds))
    if not price.is_finite() or price <= 1:
        raise ValueError('Cuota decimal no válida')
    outcomes = []
    for q in legs(line_q):
        if market == 'AH_HOME': x = 4 * (home_goals - away_goals) + q
        elif market == 'AH_AWAY': x = 4 * (away_goals - home_goals) + q
        elif market == 'OVER': x = 4 * (home_goals + away_goals) - q
        else: x = q - 4 * (home_goals + away_goals)
        outcomes.append(1 if x > 0 else -1 if x < 0 else 0)
    profit = sum((price - 1 if x == 1 else Decimal(x)) for x in outcomes) / len(outcomes)
    label = {1:'WIN', .5:'HALF_WIN', 0:'PUSH', -.5:'HALF_LOSS', -1:'LOSS'}[sum(outcomes)/len(outcomes)]
    return {'status':label, 'profit_per_unit':float(profit), 'legs_q':list(legs(line_q))}
