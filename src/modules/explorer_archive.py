"""Read the committed historical fallback when Render's ephemeral SQL is empty."""

from __future__ import annotations

import json
import threading
from pathlib import Path

ARCHIVE_PATH = Path(__file__).resolve().parents[2] / "data" / "explorer_recent.json"
_lock = threading.Lock()
_rows = None


def load_page(bucket=None, limit=150, offset=0):
    global _rows
    if _rows is None:
        with _lock:
            if _rows is None:
                if not ARCHIVE_PATH.is_file():
                    _rows = []
                else:
                    with ARCHIVE_PATH.open("r", encoding="utf-8") as handle:
                        loaded = json.load(handle)
                    _rows = [row for row in loaded if isinstance(row, dict)]
    candidates = _rows
    if bucket:
        candidates = [row for row in candidates if row.get("_explorer_bucket") == bucket]
    start = max(0, int(offset or 0))
    stop = start + max(1, int(limit or 150))
    return candidates[start:stop]
