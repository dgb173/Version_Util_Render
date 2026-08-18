"""Carga en SQL las fases previas UEFA y genera su catalogo de patrones."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from modules import uefa_qualifying  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--competitions", default=",".join(uefa_qualifying.COMPETITIONS))
    parser.add_argument("--seasons", default=",".join(uefa_qualifying.DEFAULT_SEASONS))
    parser.add_argument("--company-id", type=int, default=8)
    args = parser.parse_args()
    competitions = [value.strip() for value in args.competitions.split(",") if value.strip()]
    seasons = [value.strip() for value in args.seasons.split(",") if value.strip()]
    result = uefa_qualifying.ingest_history(competitions, seasons, args.company_id)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if result.get("errors") else 0


if __name__ == "__main__":
    raise SystemExit(main())
