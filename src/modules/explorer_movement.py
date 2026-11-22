"""Server-side Explorer movement filters shared by SQL and local index search."""
import re


def matches(result, filters):
    def bucket(value):
        value = float(value)
        absolute = abs(value)
        base = int(absolute)
        decimal = absolute - base
        number = base + 0.5 if 0.24 <= decimal <= 0.76 else (base if decimal < 0.24 else base + 1)
        return number if value >= 0 else -number

    for section in ('stadium', 'general'):
        start = filters.get(f'h2h_{section}_start_ah') or []
        end = filters.get(f'h2h_{section}_end_ah') or []
        if not start and not end:
            continue
        movement = str((result.get(f'h2h_{section}') or {}).get('movement') or '').replace(',', '.')
        found = re.search(r'([+-]?\d*\.?\d+)\s*(?:→|->|\ba\b|\bto\b)\s*([+-]?\d*\.?\d+)', movement)
        if not found:
            return False
        for actual, requested in zip(found.groups(), (start, end)):
            if not requested:
                continue
            actual_bucket = bucket(actual)
            values = requested if isinstance(requested, list) else [requested]
            if not any((actual_bucket >= 2.5 if bucket(v) >= 2.5 else actual_bucket <= -2.5 if bucket(v) <= -2.5 else actual_bucket == bucket(v)) for v in values):
                return False
    return True
