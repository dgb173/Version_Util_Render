"""Verified standings from SofaScore's server-rendered public tournament pages."""
from __future__ import annotations

import json
import re
import threading
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Dict

from . import sofascore_context as sofa

_ALIASES = Path(__file__).resolve().parents[2] / "data" / "sofascore_league_aliases.json"
_SNAPSHOTS = Path(__file__).resolve().parents[2] / "data" / "sofascore_public_snapshots"
_cache: Dict[int, tuple[float, Dict[str, Any]]] = {}
_lock = threading.Lock()


def _public_page(tournament_id: int) -> Dict[str, Any]:
    with _lock:
        entry = _cache.get(tournament_id)
        if entry and time.time() - entry[0] < 300:
            return entry[1]
    page = {}
    try:
        url = f"https://www.sofascore.com/football/tournament/x/x/{tournament_id}"
        response = sofa._http_session().get(
            url, timeout=sofa.REQUEST_TIMEOUT_SECONDS, verify=sofa.VERIFY_SSL
        )
        response.raise_for_status()
        script = re.search(r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', response.text, re.S)
        if script:
            page = (json.loads(unescape(script.group(1))).get("props") or {}).get("pageProps") or {}
    except Exception:
        pass
    if not page:
        snapshot_path = _SNAPSHOTS / f"{tournament_id}.json"
        if snapshot_path.exists():
            snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
            page = dict(snapshot.get("page") or {})
            page["_snapshot_captured_at"] = snapshot.get("captured_at")
    if str((page.get("uniqueTournament") or {}).get("id")) != str(tournament_id):
        return {}
    with _lock:
        _cache[tournament_id] = (time.time(), page)
    return page


def _team_score(name: str, candidate: str) -> float:
    requested = sofa._normalize_name(name)
    listed = sofa._normalize_name(candidate)
    reserve = re.fullmatch(r"(.+) (?:b|ii|2|reserves?)", requested)
    if reserve:
        listed_reserve = re.fullmatch(r"(.+) (?:b|ii|2|reserves?)", listed)
        if listed_reserve and listed_reserve.group(1) == reserve.group(1):
            return 1.0
        if not listed_reserve:
            return min(sofa._similarity(name, candidate), .55)
    return sofa._similarity(name, candidate)


def get_context(home_name: str, away_name: str, league_name: str, goal_line: Any = 2.5) -> Dict[str, Any]:
    """Return a public-page table only for a curated, typed tournament mapping."""
    try:
        aliases = json.loads(_ALIASES.read_text(encoding="utf-8"))
        mapping = aliases.get(sofa._normalize_name(league_name)) or {}
        if not mapping.get("is_unique") or not str(mapping.get("id", "")).isdigit():
            return {"available": False, "reason": "competition_not_resolved", "views": {}}
        tournament_id = int(mapping["id"])
        page = _public_page(tournament_id)
        season = (page.get("seasons") or [{}])[0]
        views = {}
        for table in page.get("standings") or []:
            kind = table.get("type")
            if kind in ("total", "home", "away"):
                rows = sofa._flatten_standings({"standings": [table]})
                if rows:
                    views.setdefault(kind, []).extend(rows)
        if not views.get("total"):
            return {"available": False, "reason": "standings_not_available", "views": {}}
        rows = views["total"]

        def team_id(name: str, exclude: Any = None):
            ranked = sorted((
                (max(_team_score(name, row.get("team") or ""),
                     _team_score(name, row.get("short_name") or "")), row.get("team_id"))
                for row in rows if str(row.get("team_id")) != str(exclude)
            ), key=lambda item: item[0], reverse=True)
            return ranked[0][1] if ranked and ranked[0][0] >= .75 else None

        home_id = team_id(home_name)
        away_id = team_id(away_name, home_id)
        snapshot_at = page.get("_snapshot_captured_at")
        return {
            "available": True, "cached": bool(snapshot_at),
            "source": "SofaScore (copia verificada)" if snapshot_at else "SofaScore",
            "fetched_at": snapshot_at or datetime.now(timezone.utc).isoformat(),
            "tournament": (page.get("uniqueTournament") or {}).get("name") or league_name,
            "season": season.get("name") or season.get("year") or "",
            "tournament_id": tournament_id, "season_id": season.get("id"),
            "home_team_id": home_id, "away_team_id": away_id,
            "home_name": home_name, "away_name": away_name, "views": views,
            "ou": {"line": goal_line, "views": {}, "matches_analyzed": 0, "tables": {}},
            "external_links": {"sofascore": f"https://www.sofascore.com/football/tournament/x/x/{tournament_id}"},
        }
    except Exception:
        return {"available": False, "reason": "provider_unavailable", "views": {}}
