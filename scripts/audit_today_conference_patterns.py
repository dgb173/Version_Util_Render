"""Audita similares estrictos AH/O-U/stats para los partidos Conference de una fecha."""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace


SKILL_SCRIPT = Path.home() / ".codex/skills/informe-dual-handicap-over/scripts/generate_today_dual_strict_report.py"


def load_report_module():
    spec = importlib.util.spec_from_file_location("conference_pattern_audit", SKILL_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"No se pudo cargar {SKILL_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    root = Path(args.project_root).resolve()
    sys.path.insert(0, str(root / "src"))
    report = load_report_module()
    base, deep = report._load_skill_modules()
    target_day = report.date.fromisoformat(args.date)
    _, historical = base.load_project_data(root, 0)
    visible = report._load_today_visible_candidate_rows(root, target_day)
    strict = SimpleNamespace(
        min_support=12,
        min_similarity=32.0,
        ah_max_gap=0.25,
        ou_max_gap=0.75,
        min_handicap_score=8.0,
        min_stats_blocks=2,
        max_similars=450,
        top_similars=6,
    )

    output = []
    for raw in visible:
        league = str(raw.get("league_name") or raw.get("competition_name") or "").lower()
        league_id = str(raw.get("league_id") or raw.get("competition_id") or "")
        if league_id != "2187" and "conference" not in league:
            continue
        candidate = base.encode_match(raw, historical=False)
        if candidate is None:
            continue
        evaluated = report.evaluate_candidate(candidate, historical, base, deep, strict)
        item = {
            "match_id": str(candidate.match_id),
            "home": candidate.home,
            "away": candidate.away,
            "ah": candidate.ah,
            "ou": candidate.ou,
            "strict": evaluated,
        }
        output.append(item)

    print(json.dumps({"date": args.date, "historical": len(historical), "matches": output}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
