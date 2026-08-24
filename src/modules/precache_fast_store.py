"""Read the deploy-time Pre-Cacheo snapshot without depending on stale Turso rows."""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
FAST_DIR = PROJECT_ROOT / "data" / ".precacheo_fast"
INDEX_FILE = FAST_DIR / "index.json"

_INDEX_LOCK = threading.Lock()
_INDEX_MTIME_NS = -1
_INDEX_ROWS: List[Dict[str, Any]] = []


def available() -> bool:
    return INDEX_FILE.is_file()


def _load_index() -> List[Dict[str, Any]]:
    global _INDEX_MTIME_NS, _INDEX_ROWS

    if not INDEX_FILE.is_file():
        return []
    try:
        mtime_ns = INDEX_FILE.stat().st_mtime_ns
    except OSError:
        return []

    with _INDEX_LOCK:
        if mtime_ns == _INDEX_MTIME_NS:
            return list(_INDEX_ROWS)
        try:
            with INDEX_FILE.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except Exception:
            return []
        _INDEX_ROWS = [row for row in payload if isinstance(row, dict)] if isinstance(payload, list) else []
        _INDEX_MTIME_NS = mtime_ns
        return list(_INDEX_ROWS)


def _matches_handicap(value: Any, selected_values: Optional[Sequence[str]]) -> bool:
    selected = []
    for raw in selected_values or []:
        try:
            selected.append(float(raw))
        except (TypeError, ValueError):
            continue
    if not selected:
        return True
    try:
        handicap = float(value)
    except (TypeError, ValueError):
        return False

    for selected_value in selected:
        magnitude = abs(selected_value)
        if magnitude >= 2.49:
            if (selected_value > 0 and handicap >= 2.24) or (selected_value < 0 and handicap <= -2.24):
                return True
        elif abs(magnitude - 2.0) < 0.01:
            center = 2.0 if selected_value > 0 else -2.0
            if center - 0.01 <= handicap <= center + 0.01:
                return True
        elif abs(magnitude - 1.5) < 0.1:
            low, high = ((1.24, 1.76) if selected_value > 0 else (-1.76, -1.24))
            if low <= handicap <= high:
                return True
        elif abs(magnitude - 1.0) < 0.1:
            if selected_value - 0.1 <= handicap <= selected_value + 0.1:
                return True
        elif abs(magnitude - 0.5) < 0.1:
            low, high = ((0.24, 0.76) if selected_value > 0 else (-0.76, -0.24))
            if low <= handicap <= high:
                return True
        elif magnitude < 0.1:
            if -0.1 <= handicap <= 0.1:
                return True
        elif selected_value - 0.01 <= handicap <= selected_value + 0.01:
            return True
    return False


def load_headers(handicap_buckets: Optional[Sequence[str]] = None) -> List[Dict[str, Any]]:
    return [
        dict(row)
        for row in _load_index()
        if _matches_handicap(row.get("handicap"), handicap_buckets)
    ]


def _payload_path(match_id: str) -> Path:
    import hashlib

    digest = hashlib.sha256(str(match_id).encode("utf-8")).hexdigest()
    return FAST_DIR / f"{digest}.json"


def get_payload(match_id: str) -> Optional[Dict[str, Any]]:
    path = _payload_path(str(match_id))
    if not path.is_file():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def load_payloads(match_ids: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    output: Dict[str, Dict[str, Any]] = {}
    for raw_id in match_ids or []:
        match_id = str(raw_id).strip()
        if not match_id or match_id in output:
            continue
        payload = get_payload(match_id)
        if payload:
            output[match_id] = payload
    return output


def load_all_payloads() -> List[Dict[str, Any]]:
    return list(load_payloads([row.get("match_id") for row in _load_index()]).values())
