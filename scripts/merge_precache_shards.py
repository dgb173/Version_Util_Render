#!/usr/bin/env python
"""Fusiona resultados independientes de precacheo sin escrituras Git concurrentes."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def _load_rows(path: Path) -> list[dict]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return []
    if not isinstance(payload, list):
        raise ValueError(f"{path} no contiene una lista JSON")
    return [row for row in payload if isinstance(row, dict)]


def _match_id(row: dict) -> str:
    return str(row.get("match_id") or row.get("id") or "").strip()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--shards-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=400)
    args = parser.parse_args()

    merged: dict[str, dict] = {}
    order: list[str] = []

    def consume(rows: list[dict]) -> None:
        for row in rows:
            match_id = _match_id(row)
            if not match_id:
                continue
            if match_id not in merged:
                order.append(match_id)
            merged[match_id] = row

    consume(_load_rows(args.base))
    shard_files = sorted(args.shards_dir.rglob("results_*.json"))
    for path in shard_files:
        consume(_load_rows(path))

    # Los resultados nuevos reemplazan al registro base y se colocan primero.
    new_ids = []
    for path in shard_files:
        for row in _load_rows(path):
            match_id = _match_id(row)
            if match_id and match_id not in new_ids:
                new_ids.append(match_id)
    new_id_set = set(new_ids)
    final_order = new_ids + [match_id for match_id in order if match_id not in new_id_set]
    rows = [merged[match_id] for match_id in final_order[: max(1, int(args.limit))]]

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Fusionados {len(shard_files)} shards: {len(new_ids)} nuevos, {len(rows)} totales")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
