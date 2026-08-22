"""Build a compact, indexed Pre-Cacheo store for the Render runtime."""

from __future__ import annotations

import gc
import hashlib
import json
import sys
from pathlib import Path

try:
    import ijson
except ImportError:
    ijson = None

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from modules.sql_store import _build_explorer_payload  # noqa: E402

DATA_DIR = PROJECT_ROOT / "data"
FAST_DIR = DATA_DIR / ".precacheo_fast"
INDEX_FILE = FAST_DIR / "index.json"
SOURCE_FILES = (
    DATA_DIR / "data_precacheo.json",
    DATA_DIR / "data_pending_results.json",
)
EXTRA_KEYS = (
    "time",
    "start_time",
    "is_neutral_venue",
    "goal_line",
    "state",
    "h2h_stadium",
    "h2h_general",
    "h2h_col3",
    "last_home_match",
    "last_away_match",
    "comparativas_indirectas",
    "pre_match_context",
    "market_analysis_data",
    "recent_home_matches",
    "recent_away_matches",
    "recent_away_matches_all",
)


def payload_path(match_id: str) -> Path:
    digest = hashlib.sha256(str(match_id).encode("utf-8")).hexdigest()
    return FAST_DIR / f"{digest}.json"


def compact_row(row: dict) -> dict:
    compact = _build_explorer_payload(row)
    compact.pop("market_analysis_html", None)
    compact.pop("historical_matches_html", None)
    for key in EXTRA_KEYS:
        value = row.get(key)
        if value not in (None, ""):
            compact[key] = value
    return compact


def main() -> None:
    existing_sources = [source for source in SOURCE_FILES if source.exists()]
    if INDEX_FILE.exists() and existing_sources:
        newest_source = max(source.stat().st_mtime_ns for source in existing_sources)
        if INDEX_FILE.stat().st_mtime_ns >= newest_source:
            print("Pre-Cacheo fast store is already current")
            return

    FAST_DIR.mkdir(parents=True, exist_ok=True)
    headers_by_id: dict[str, dict] = {}
    written = 0

    for source in SOURCE_FILES:
        if not source.exists():
            continue
        if ijson is not None:
            with source.open("rb") as handle:
                rows_iter = ijson.items(handle, "item", use_float=True)
                for row in rows_iter:
                    if not isinstance(row, dict):
                        continue
                    match_id = str(row.get("match_id") or row.get("id") or "").strip()
                    if not match_id:
                        continue
                    compact = compact_row(row)
                    compact["match_id"] = match_id
                    compact["id"] = match_id
                    with payload_path(match_id).open("w", encoding="utf-8") as output:
                        json.dump(compact, output, ensure_ascii=False, separators=(",", ":"))

                    odds = compact.get("main_match_odds") if isinstance(compact.get("main_match_odds"), dict) else {}
                    handicap = compact.get("handicap")
                    if handicap in (None, ""):
                        handicap = odds.get("ah_linea")
                    headers_by_id[match_id] = {
                        "match_id": match_id,
                        "handicap": handicap,
                        "score": compact.get("score") or compact.get("final_score"),
                        "match_date": compact.get("match_date") or compact.get("date"),
                        "start_time": compact.get("start_time"),
                        "time": compact.get("time"),
                    }
                    written += 1
                    if written % 100 == 0: gc.collect()
        else:
            with source.open("r", encoding="utf-8") as handle:
                try:
                    loaded = json.load(handle)
                except Exception:
                    loaded = []
                if isinstance(loaded, dict):
                    loaded = list(loaded.values())
                for row in (loaded if isinstance(loaded, list) else []):
                    if not isinstance(row, dict):
                        continue
                    match_id = str(row.get("match_id") or row.get("id") or "").strip()
                    if not match_id:
                        continue
                    compact = compact_row(row)
                    compact["match_id"] = match_id
                    compact["id"] = match_id
                    with payload_path(match_id).open("w", encoding="utf-8") as output:
                        json.dump(compact, output, ensure_ascii=False, separators=(",", ":"))

                    odds = compact.get("main_match_odds") if isinstance(compact.get("main_match_odds"), dict) else {}
                    handicap = compact.get("handicap")
                    if handicap in (None, ""):
                        handicap = odds.get("ah_linea")
                    headers_by_id[match_id] = {
                        "match_id": match_id,
                        "handicap": handicap,
                        "score": compact.get("score") or compact.get("final_score"),
                        "match_date": compact.get("match_date") or compact.get("date"),
                        "start_time": compact.get("start_time"),
                        "time": compact.get("time"),
                    }
                    written += 1
                    if written % 100 == 0: gc.collect()

    with INDEX_FILE.open("w", encoding="utf-8") as output:
        json.dump(list(headers_by_id.values()), output, ensure_ascii=False, separators=(",", ":"))
    print(f"Built fast Pre-Cacheo store: {len(headers_by_id)} matches ({written} rows read)")


if __name__ == "__main__":
    main()
