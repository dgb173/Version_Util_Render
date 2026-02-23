#!/usr/bin/env python
import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from modules import data_manager  # noqa: E402
from modules import sql_store  # noqa: E402


def _iter_input_files(path: Path) -> Iterable[Path]:
    if path.is_file():
        yield path
        return
    for p in path.rglob("*.json"):
        if p.is_file():
            yield p


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _infer_bucket_from_filename(filename: str) -> Optional[str]:
    name = filename.lower()
    if name.startswith("data_") and name.endswith(".json"):
        return filename
    return None


def _normalize_match_id(match: Dict[str, Any]) -> Optional[str]:
    mid = match.get("match_id")
    if mid in (None, ""):
        mid = match.get("id")
    if mid in (None, ""):
        return None
    return str(mid)


def _pending_score(score: Optional[str]) -> bool:
    if score is None:
        return True
    s = str(score).strip()
    return s in {"??", "?-?", "?:?", "? : ?", "? - ?"}


def _infer_bucket_from_match(match: Dict[str, Any]) -> Tuple[str, str]:
    score = match.get("score") or match.get("final_score")
    if _pending_score(score):
        return ("data_pending_results.json", "pending_results")
    ah = match.get("handicap")
    if ah is None:
        ah = (match.get("main_match_odds") or {}).get("ah_linea")
    return (data_manager.get_bucket_name(ah), "historical")


def _bucket_state_for_bucket(bucket: str) -> str:
    if bucket == "data_precacheo.json":
        return "precacheo"
    if bucket == "data_pending_results.json":
        return "pending_results"
    return "historical"


def _upsert_match(match: Dict[str, Any], bucket: str, state: str) -> bool:
    mid = _normalize_match_id(match)
    if not mid:
        return False
    match["match_id"] = mid
    sql_store.upsert_match(match, bucket=bucket, state=state)
    return True


def _import_list(items: List[Any], bucket: Optional[str], strict: bool) -> Tuple[int, int]:
    imported = 0
    skipped = 0
    for item in items:
        if not isinstance(item, dict):
            skipped += 1
            continue
        if strict and bucket and bucket not in {"data_precacheo.json", "data_pending_results.json"}:
            if not data_manager.validate_explorer_match(item):
                skipped += 1
                continue
        if bucket:
            state = _bucket_state_for_bucket(bucket)
            ok = _upsert_match(item, bucket=bucket, state=state)
        else:
            inferred_bucket, inferred_state = _infer_bucket_from_match(item)
            ok = _upsert_match(item, bucket=inferred_bucket, state=inferred_state)
        if ok:
            imported += 1
        else:
            skipped += 1
    return imported, skipped


def import_file(path: Path, strict: bool) -> Tuple[int, int]:
    bucket = _infer_bucket_from_filename(path.name)
    payload = _load_json(path)
    if isinstance(payload, list):
        return _import_list(payload, bucket=bucket, strict=strict)
    if isinstance(payload, dict):
        # Support {upcoming_matches: [...], finished_matches: [...]}, etc.
        imported = 0
        skipped = 0
        for key, value in payload.items():
            if isinstance(value, list):
                imp, sk = _import_list(value, bucket=bucket, strict=strict)
                imported += imp
                skipped += sk
        # If dict is a single match object
        if imported == 0 and ("match_id" in payload or "id" in payload):
            inferred_bucket, inferred_state = _infer_bucket_from_match(payload)
            if _upsert_match(payload, bucket=inferred_bucket, state=inferred_state):
                imported += 1
            else:
                skipped += 1
        return imported, skipped
    return 0, 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Import JSON matches into SQL (dedupe by match_id).")
    parser.add_argument(
        "path",
        nargs="?",
        default="incoming_matches",
        help="File or directory containing JSON to import (default: incoming_matches)",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Skip explorer-invalid matches (missing prev_home/prev_away, etc.)",
    )
    args = parser.parse_args()

    sql_store.ensure_bootstrap()

    root = Path(args.path).resolve()
    if not root.exists():
        print(f"Path not found: {root}")
        return 2

    total_imported = 0
    total_skipped = 0
    files = sorted(_iter_input_files(root))
    if not files:
        print(f"No .json files found under: {root}")
        return 0

    for fp in files:
        try:
            imp, sk = import_file(fp, strict=args.strict)
            total_imported += imp
            total_skipped += sk
            print(f"{fp.name}: imported={imp} skipped={sk}")
        except Exception as exc:
            total_skipped += 1
            print(f"{fp.name}: ERROR {exc}")

    print(f"TOTAL imported={total_imported} skipped={total_skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

