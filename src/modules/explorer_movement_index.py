"""Shared movement parsing for the Explorer's compact historical index."""
import re


def bucket(value):
    value = float(value)
    absolute = abs(value)
    base = int(absolute)
    decimal = absolute - base
    number = base + 0.5 if 0.24 <= decimal <= 0.76 else (base if decimal < 0.24 else base + 1)
    return number if value >= 0 else -number


def movement_pair(row, section):
    data = row.get('market_analysis_data') or {}
    node = data.get(section) or {} if isinstance(data, dict) else {}
    movement = str(node.get('movement') or '').replace(',', '.')
    if not movement:
        html = row.get('market_analysis_html') or ''
        marker = 'Análisis del Precedente en Este Estadio' if section == 'stadium' else 'Análisis del H2H General Más Reciente'
        start = html.find(marker)
        if start >= 0:
            block = html[start:]
            if section == 'stadium':
                next_marker = block.find('Análisis del H2H General Más Reciente')
                if next_marker >= 0:
                    block = block[:next_marker]
            found = re.search(r'movimiento:.*?>\s*([+-]?\d*\.?\d+)\s*(?:→|->|➜)\s*([+-]?\d*\.?\d+)', block)
            if found:
                return tuple(bucket(value) for value in found.groups())
    found = re.search(r'([+-]?\d*\.?\d+)\s*(?:→|->|➜|\ba\b|\bto\b)\s*([+-]?\d*\.?\d+)', movement)
    return tuple(bucket(value) for value in found.groups()) if found else (None, None)


def index_row(row):
    return (str(row['match_id']), *movement_pair(row, 'stadium'), *movement_pair(row, 'general'))
