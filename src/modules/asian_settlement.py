"""Liquidación por unidad de AH/O/U con cuotas decimales verificadas."""

import math


def split_line(line):
    value = float(line)
    if not math.isfinite(value) or abs(value * 4 - round(value * 4)) > 1e-8:
        raise ValueError("La línea debe ser finita y múltiplo de 0.25")
    low = math.floor(value * 2) / 2
    return (low, low + 0.5) if abs(value - low) > 1e-8 else (value,)


def settlement(residual_value, line):
    """Resultado a cuota 2: -1, -0.5, 0, 0.5 o 1."""
    return sum(1 if residual_value > part else -1 if residual_value < part else 0
               for part in split_line(line)) / len(split_line(line))


def unit_profit(settled, decimal_odds):
    price = float(decimal_odds)
    if not math.isfinite(price) or price <= 1:
        raise ValueError("La cuota decimal debe ser finita y mayor de 1")
    if settled not in {-1, -0.5, 0, 0.5, 1}:
        raise ValueError("Liquidación asiática inválida")
    return settled * (price - 1) if settled > 0 else float(settled)
