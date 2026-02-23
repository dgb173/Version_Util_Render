import math
import re


def _parse_number_clean(value: str):
    if value is None:
        return None

    text = str(value).strip()
    text = text.replace("−", "-")
    text = text.replace(",", ".")
    text = text.replace("+", "")
    text = text.replace(" ", "")

    if not re.match(r"^[+-]?\d+(?:\.\d+)?$", text):
        return None

    try:
        return float(text)
    except ValueError:
        return None


def _parse_handicap_to_float(text: str):
    if text is None:
        return None

    raw = str(text).strip()
    if "/" in raw:
        parts = [p for p in re.split(r"/", raw) if p]
        if not parts:
            return None
        values = []
        for part in parts:
            parsed = _parse_number_clean(part)
            if parsed is None:
                return None
            values.append(parsed)
        return sum(values) / len(values)

    return _parse_number_clean(raw.replace("+", ""))


def _bucket_to_half(value: float) -> float:
    if value is None:
        return None

    if value == 0:
        return 0.0

    sign = -1.0 if value < 0 else 1.0
    abs_value = abs(value)
    base = math.floor(abs_value + 1e-9)
    frac = abs_value - base

    def close(a, b):
        return abs(a - b) < 1e-6

    if close(frac, 0.0):
        bucket = float(base)
    elif close(frac, 0.5) or close(frac, 0.25) or close(frac, 0.75):
        bucket = base + 0.5
    else:
        bucket = round(abs_value * 2) / 2.0
        bucket_frac = bucket - math.floor(bucket)
        if close(bucket_frac, 0.0) and (
            abs(abs_value - (math.floor(bucket) + 0.25)) < 0.26
            or abs(abs_value - (math.floor(bucket) + 0.75)) < 0.26
        ):
            bucket = math.floor(bucket) + 0.5

    return sign * bucket


def normalize_handicap_to_half_bucket_str(text: str):
    parsed = _parse_handicap_to_float(text)
    if parsed is None:
        return None

    bucket = _bucket_to_half(parsed)
    if bucket is None:
        return None

    return f"{bucket:.1f}"
