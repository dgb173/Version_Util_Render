import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


RED_CARD_LABEL = "Tarjetas Rojas"

_STRICT_RED_MARKERS = (
    "rcard",
    "redcard",
    "red-card",
    "red_card",
    "cardred",
    "card-red",
    "card_red",
)
_TEXT_RED_MARKERS = (
    "red card",
    "red cards",
    "tarjeta roja",
    "tarjetas rojas",
)
_RED_SRC_RE = re.compile(r"(^|[/_.-])red(card)?\d*\.(gif|png|jpg|jpeg|webp)$", re.I)


def _attr_to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple, set)):
        return " ".join(str(v) for v in value if v is not None)
    return str(value)


def _has_red_card_marker(tag: Any) -> bool:
    if tag is None or not hasattr(tag, "attrs"):
        return False

    class_raw = tag.get("class")
    class_values = []
    if isinstance(class_raw, (list, tuple, set)):
        class_values = [str(item).lower() for item in class_raw]
    elif class_raw is not None:
        class_values = str(class_raw).lower().split()

    tag_name = str(getattr(tag, "name", "") or "").lower()
    if "red" in class_values and tag_name not in {"td", "tr"} and _explicit_count_from_tag(tag) is not None:
        return True

    class_blob = " ".join(class_values)
    id_blob = _attr_to_text(tag.get("id")).lower()
    src_blob = _attr_to_text(tag.get("src")).lower()
    strict_blob = " ".join([class_blob, id_blob, src_blob])
    if any(marker in strict_blob for marker in _STRICT_RED_MARKERS):
        return True

    if src_blob and _RED_SRC_RE.search(src_blob):
        return True

    text_attrs = (
        tag.get("title"),
        tag.get("alt"),
        tag.get("aria-label"),
        tag.get("data-title"),
        tag.get("data-original-title"),
        tag.get("data-bs-title"),
    )
    text_blob = " ".join(_attr_to_text(v).lower() for v in text_attrs)
    return any(marker in text_blob for marker in _TEXT_RED_MARKERS)


def _explicit_count_from_tag(tag: Any) -> Optional[int]:
    parts = []
    try:
        parts.append(tag.get_text(" ", strip=True))
    except Exception:
        pass

    if hasattr(tag, "attrs"):
        for attr in (
            "title",
            "alt",
            "aria-label",
            "data-title",
            "data-original-title",
            "data-bs-title",
            "src",
        ):
            parts.append(_attr_to_text(tag.get(attr)))

    blob = " ".join(parts)
    for raw in re.findall(r"\d+", blob):
        try:
            value = int(raw)
        except ValueError:
            continue
        if 0 < value <= 5:
            return value
    return None


def extract_red_card_count_from_cell(cell: Any) -> Optional[str]:
    """
    Extract a red-card count from a Nowgoal team cell.

    The site has used several variants over time: spans with rcard/red-card
    classes, image icons, or title/alt labels. If an icon has no explicit
    number, one icon is counted as one red card.
    """
    if cell is None:
        return None

    elements = [cell]
    try:
        elements.extend(cell.find_all(True))
    except Exception:
        pass

    matched = []
    matched_ids = set()
    for element in elements:
        if not _has_red_card_marker(element):
            continue
        try:
            if any(id(parent) in matched_ids for parent in element.parents):
                continue
        except Exception:
            pass
        matched.append(element)
        matched_ids.add(id(element))

    total = 0
    for element in matched:
        explicit = _explicit_count_from_tag(element)
        total += explicit if explicit is not None else 1

    return str(total) if total > 0 else None


def normalize_red_card_value(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bool):
        return "1" if value else None
    if isinstance(value, (int, float)):
        return str(int(value)) if value > 0 else None

    text = str(value).strip()
    if not text or text in {"-", "N/A", "n/a", "None", "null"}:
        return None

    match = re.search(r"\d+", text)
    if not match:
        return None
    count = int(match.group(0))
    return str(count) if count > 0 else None


def is_red_card_row(row: Any) -> bool:
    if not isinstance(row, dict):
        return False
    if row.get("is_red_card"):
        return True
    label = str(row.get("label") or "").strip().lower()
    return label in {
        "tarjetas rojas",
        "tarjeta roja",
        "red cards",
        "red card",
    }


def append_red_card_stats(
    stats_rows: Any,
    home_red: Any = None,
    away_red: Any = None,
) -> List[Dict[str, Any]]:
    rows = list(stats_rows) if isinstance(stats_rows, list) else []
    home_count = normalize_red_card_value(home_red)
    away_count = normalize_red_card_value(away_red)

    if not home_count and not away_count:
        return rows

    red_row = {
        "label": RED_CARD_LABEL,
        "home": home_count or "0",
        "away": away_count or "0",
        "is_red_card": True,
    }

    for index, row in enumerate(rows):
        if is_red_card_row(row):
            updated = dict(row)
            updated.update(red_row)
            rows[index] = updated
            return rows

    rows.append(red_row)
    return rows


def _first_red_pair(
    block: Dict[str, Any],
    key_pairs: Sequence[Tuple[str, str]],
) -> Tuple[Optional[str], Optional[str]]:
    for home_key, away_key in key_pairs:
        home_count = normalize_red_card_value(block.get(home_key))
        away_count = normalize_red_card_value(block.get(away_key))
        if home_count or away_count:
            return home_count, away_count
    return None, None


def _apply_red_row(
    block: Any,
    key_pairs: Sequence[Tuple[str, str]],
) -> None:
    if not isinstance(block, dict):
        return
    home_count, away_count = _first_red_pair(block, key_pairs)
    if home_count or away_count:
        block["stats_rows"] = append_red_card_stats(
            block.get("stats_rows") or [],
            home_count,
            away_count,
        )


def normalize_red_card_stats_payload(match_data: Any) -> Any:
    if not isinstance(match_data, dict):
        return match_data

    standard_pairs = (("home_red", "away_red"),)
    stadium_pairs = (
        ("home_red_stadium", "away_red_stadium"),
        ("home_red", "away_red"),
    )
    general_pairs = (
        ("home_red_gen", "away_red_gen"),
        ("home_red", "away_red"),
    )

    _apply_red_row(match_data.get("last_home_match"), standard_pairs)
    _apply_red_row(match_data.get("last_away_match"), standard_pairs)
    _apply_red_row(match_data.get("h2h_col3"), standard_pairs)
    _apply_red_row(match_data.get("h2h_stadium"), stadium_pairs)
    _apply_red_row(match_data.get("h2h_general"), general_pairs)

    indirect = match_data.get("comparativas_indirectas")
    if isinstance(indirect, dict):
        _apply_red_row(indirect.get("left"), standard_pairs)
        _apply_red_row(indirect.get("right"), standard_pairs)

    recent = match_data.get("recent_indirect_full")
    if isinstance(recent, dict):
        _apply_red_row(recent.get("last_home"), standard_pairs)
        _apply_red_row(recent.get("last_away"), standard_pairs)
        _apply_red_row(recent.get("h2h_col3"), standard_pairs)

    return match_data
