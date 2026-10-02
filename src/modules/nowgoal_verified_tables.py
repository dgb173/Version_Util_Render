"""Bundled, source-traced league tables for competitions with a verified NowGoal ID."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict

from . import sofascore_context as sofa

_DATA = Path(__file__).resolve().parents[2] / "data" / "nowgoal_verified_tables.json"


@lru_cache(maxsize=1)
def _tables():
    return json.loads(_DATA.read_text(encoding="utf-8"))


def get_context(home_name: str, away_name: str, league_name: str, league_id: Any) -> Dict[str, Any]:
    lid = str(league_id or "")
    if not lid.isdigit():
        return {"available": False, "reason": "league_not_linked", "views": {}}
    try:
        table = _tables().get(lid) or {}
        if not table or sofa._similarity(league_name, table.get("league")) < .85:
            return {"available": False, "reason": "league_not_linked", "views": {}}
        views = table.get("views") or {}
        rows = views.get("total") or []
        if not rows:
            return {"available": False, "reason": "standings_not_available", "views": {}}

        def best_id(name: str, excluded: Any = None):
            ranked = sorted((
                (sofa._similarity(name, row.get("team")), row.get("team_id"))
                for row in rows if row.get("team_id") is not None and str(row.get("team_id")) != str(excluded)
            ), key=lambda item: item[0], reverse=True)
            return ranked[0][1] if ranked and ranked[0][0] >= .75 else None

        home_id = best_id(home_name)
        away_id = best_id(away_name, home_id)
        return {
            "available": True, "cached": True, "source": "NowGoal (copia verificada)",
            "fetched_at": table.get("captured_at") or datetime.now(timezone.utc).isoformat(),
            "tournament": table.get("league") or league_name,
            "season": table.get("season") or "",
            "home_team_id": home_id, "away_team_id": away_id,
            "home_name": home_name, "away_name": away_name,
            "views": views, "ou": {"views": {}, "matches_analyzed": 0, "tables": {}},
            "external_links": {"nowgoal": table.get("source_url")},
        }
    except (OSError, ValueError, TypeError, KeyError):
        return {"available": False, "reason": "snapshot_unavailable", "views": {}}
