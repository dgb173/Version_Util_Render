#!/usr/bin/env python
"""Divide una cola de precacheo de forma estable para una matriz de GitHub Actions."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--shards", type=int, default=8)
    args = parser.parse_args()

    jobs = json.loads(args.input.read_text(encoding="utf-8")) if args.input.exists() else []
    if not isinstance(jobs, list):
        raise SystemExit("La cola de entrada debe ser una lista JSON")

    shard_count = max(1, min(int(args.shards), 16))
    buckets = [[] for _ in range(shard_count)]
    seen = set()
    for job in jobs:
        if not isinstance(job, dict):
            continue
        match_id = str(job.get("id") or job.get("match_id") or "").strip()
        if not match_id or match_id in seen:
            continue
        seen.add(match_id)
        # Round-robin evita que un shard concentre una liga o franja horaria lenta.
        buckets[(len(seen) - 1) % shard_count].append(job)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    for index, bucket in enumerate(buckets):
        (args.output_dir / f"shard_{index}.json").write_text(
            json.dumps(bucket, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    manifest = {"jobs": len(seen), "shards": shard_count, "sizes": [len(x) for x in buckets]}
    (args.output_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(manifest, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
