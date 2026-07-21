"""Catalogo y analisis de colocacion de cuotas en fases previas UEFA."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import requests

from . import sql_store
from .league_handicap_scraper import (
    BASE_URL,
    DEFAULT_COMPANY_ID,
    _discover_league,
    _get_text,
    parse_round_odds,
)


COMPETITIONS: Dict[str, str] = {
    "103": "UEFA Champions League",
    "113": "UEFA Europa League",
    "2187": "UEFA Conference League",
}
DEFAULT_SEASONS = ("2023-2024", "2024-2025", "2025-2026")
QUALIFYING_TOKENS = ("qual", "prelim", "playoff", "play-off")


def _nowgoal_date_to_madrid(value: Any) -> str:
    """Convierte la fecha cruda del calendario Nowgoal (Asia/Shanghai) a Madrid."""
    raw = str(value or "").strip()
    if not raw:
        return raw
    try:
        parsed = datetime.strptime(raw, "%Y-%m-%d %H:%M")
        localized = parsed.replace(tzinfo=ZoneInfo("Asia/Shanghai")).astimezone(ZoneInfo("Europe/Madrid"))
        return localized.strftime("%Y-%m-%d %H:%M")
    except (ValueError, TypeError):
        return raw


def _line_to_float(value: Any) -> Optional[float]:
    if value in (None, "", "-"):
        return None
    text = str(value).strip().replace(" ", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        pass
    parts = text.split("/")
    if len(parts) == 2:
        try:
            return (float(parts[0]) + float(parts[1])) / 2
        except ValueError:
            return None
    return None


def _score(value: Any) -> Optional[Tuple[int, int]]:
    match = re.fullmatch(r"\s*(\d+)\s*[-:]\s*(\d+)\s*", str(value or ""))
    return (int(match.group(1)), int(match.group(2))) if match else None


def _stage_order(name: str) -> int:
    value = str(name or "").lower()
    if "prelim" in value:
        return 0
    if "qual" in value:
        number = re.search(r"(\d+)", value)
        return int(number.group(1)) if number else 1
    if value in {"round 1", "round 2", "round 3"}:
        return int(value[-1])
    if "play" in value:
        return 4
    return 99


def _is_qualifying_stage(name: str, competition_id: str = "") -> bool:
    value = str(name or "").lower()
    if str(competition_id) == "103" and value in {"round 1", "round 2", "round 3"}:
        return True
    return any(token in value for token in QUALIFYING_TOKENS) and "knockout" not in value


def _iter_stage_rows(raw: Any) -> Iterable[List[Any]]:
    """Admite calendarios planos, eliminatorias agrupadas y el formato R_* antiguo."""
    if isinstance(raw, dict):
        for value in raw.values():
            yield from _iter_stage_rows(value)
        return
    if not isinstance(raw, list):
        return
    if len(raw) >= 8 and str(raw[0]).isdigit() and str(raw[1]).isdigit():
        yield raw
        return
    for item in raw:
        if isinstance(item, list) and len(item) >= 5 and isinstance(item[4], list):
            yield from _iter_stage_rows(item[4])
        else:
            yield from _iter_stage_rows(item)


def _extract_catalog_rows(
    data: Dict[str, Any],
    competition_id: str,
    season: str,
    odds: Dict[str, Dict[str, float]],
    company_id: int,
) -> List[Dict[str, Any]]:
    team_rows = data.get("TeamList") or data.get("TeamInfo") or []
    teams = {str(row[0]): str(row[1]) for row in team_rows if isinstance(row, list) and len(row) > 1}
    cup_kinds = data.get("CupKindList") or []
    stage_map = {
        str(row[0]): str(row[2])
        for row in cup_kinds
        if isinstance(row, list) and len(row) > 2
    }
    schedule = data.get("ScheduleList") or {}
    competition_name = COMPETITIONS.get(str(competition_id)) or (
        str((data.get("LeagueInfo") or [None, f"UEFA {competition_id}"])[1])
    )
    source_url = f"{BASE_URL}/league/{season}/{competition_id}"
    output: List[Dict[str, Any]] = []

    for key, raw_stage in schedule.items():
        stage_id_match = re.search(r"(\d+)", str(key))
        stage_id = stage_id_match.group(1) if stage_id_match else str(key)
        stage_name = stage_map.get(stage_id, str(key))
        if not _is_qualifying_stage(stage_name, competition_id):
            continue
        for row in _iter_stage_rows(raw_stage):
            if len(row) < 8:
                continue
            match_id = str(row[0])
            page_odds = odds.get(match_id) or {}
            visible_ah = page_odds.get("visible_ah")
            ah_line = visible_ah if visible_ah is not None else _line_to_float(row[8] if len(row) > 8 else None)
            output.append(
                {
                    "match_id": match_id,
                    "competition_id": str(competition_id),
                    "competition_name": competition_name,
                    "competition_type": "uefa_qualifying",
                    "season": season,
                    "stage_id": stage_id,
                    "stage_name": stage_name,
                    "stage_order": _stage_order(stage_name),
                    "match_date": _nowgoal_date_to_madrid(row[3]),
                    "source_match_date": str(row[3]),
                    "home_team_id": str(row[4]),
                    "home_team": teams.get(str(row[4]), str(row[4])),
                    "away_team_id": str(row[5]),
                    "away_team": teams.get(str(row[5]), str(row[5])),
                    "score": str(row[6] or "-"),
                    "half_time_score": str(row[7] or "-"),
                    "source_state": row[2],
                    "ah_line": ah_line,
                    "ou_line": _line_to_float(row[10] if len(row) > 10 else None),
                    "home_odds_decimal": (
                        page_odds.get("home_odds_hk") + 1 if page_odds.get("home_odds_hk") is not None else None
                    ),
                    "away_odds_decimal": (
                        page_odds.get("away_odds_hk") + 1 if page_odds.get("away_odds_hk") is not None else None
                    ),
                    "company_id": int(company_id),
                    "source_url": source_url,
                    "deep_status": "catalogued",
                }
            )
    return output


def fetch_season_catalog(
    competition_id: str,
    season: str,
    company_id: int = DEFAULT_COMPANY_ID,
    session: Optional[requests.Session] = None,
) -> List[Dict[str, Any]]:
    competition_id = str(competition_id)
    if competition_id not in COMPETITIONS:
        raise ValueError(f"Competicion UEFA no soportada: {competition_id}")
    session = session or requests.Session()
    discovered_season, data_url = _discover_league(session, competition_id, str(season))
    data = json.loads(_get_text(session, data_url))
    odds_url = (
        f"{BASE_URL}/ajax/LeagueOddsAjax?sclassId={competition_id}"
        f"&subSclassId=0&matchSeason={discovered_season}&round=1"
    )
    odds = parse_round_odds(_get_text(session, odds_url), company_id)
    return _extract_catalog_rows(data, competition_id, discovered_season, odds, company_id)


def _mirror_catalogue_in_matches(rows: Sequence[Dict[str, Any]]) -> int:
    """Crea una fila ligera en matches sin pisar analisis profundos ya existentes."""
    mirrored = 0
    for row in rows:
        match_id = str(row["match_id"])
        existing = sql_store.get_match(match_id)
        payload = {
            "match_id": match_id,
            "league_id": row["competition_id"],
            "league_name": row["competition_name"],
            "competition_type": "uefa_qualifying",
            "competition_stage": row["stage_name"],
            "competition_stage_id": row["stage_id"],
            "season": row["season"],
            "home_name": row["home_team"],
            "away_name": row["away_team"],
            "match_date": row["match_date"],
            "score": row["score"],
            "final_score": row["score"],
            "handicap": row["ah_line"],
            "main_match_odds": {
                "ah_linea": row["ah_line"],
                "goals_linea": row["ou_line"],
                "home_odds_decimal": row["home_odds_decimal"],
                "away_odds_decimal": row["away_odds_decimal"],
            },
            "uefa_qualifying_catalogue": True,
        }
        if existing is not None:
            if not existing.get("uefa_qualifying_catalogue"):
                continue
            payload = {**existing, **payload}
        sql_store.upsert_match(payload, bucket="data_uefa_qualifying.json", state="historical")
        mirrored += 1
    return mirrored


def ingest_history(
    competition_ids: Sequence[str] = tuple(COMPETITIONS),
    seasons: Sequence[str] = DEFAULT_SEASONS,
    company_id: int = DEFAULT_COMPANY_ID,
) -> Dict[str, Any]:
    session = requests.Session()
    details: List[Dict[str, Any]] = []
    all_rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, str]] = []
    for competition_id in competition_ids:
        for season in seasons:
            try:
                rows = fetch_season_catalog(competition_id, season, company_id, session=session)
                all_rows.extend(rows)
                details.append({"competition_id": str(competition_id), "season": str(season), "matches": len(rows)})
            except Exception as exc:
                errors.append({"competition_id": str(competition_id), "season": str(season), "error": str(exc)})
    saved = sql_store.upsert_uefa_qualifying_matches(all_rows)
    mirrored = _mirror_catalogue_in_matches(all_rows)
    return {
        "saved": saved,
        "mirrored_in_matches": mirrored,
        "details": details,
        "errors": errors,
    }


def _favorite_cover(row: Dict[str, Any]) -> Optional[bool]:
    score = _score(row.get("score"))
    ah = row.get("ah_line")
    if score is None or ah is None or abs(float(ah)) < 1e-9:
        return None
    home_goals, away_goals = score
    margin = home_goals - away_goals if float(ah) > 0 else away_goals - home_goals
    return margin > abs(float(ah))


def _home_cover(row: Dict[str, Any]) -> Optional[bool]:
    score = _score(row.get("score"))
    ah = row.get("ah_line")
    if score is None or ah is None:
        return None
    return score[0] - score[1] > float(ah)


def _ah_bucket(value: Any) -> str:
    if value is None:
        return "Sin linea"
    number = float(value)
    rounded = round(number * 4) / 4
    if rounded >= 2:
        return "+2 o mas"
    if rounded <= -2:
        return "-2 o menos"
    return f"{rounded:+g}"


def _price_band(row: Dict[str, Any]) -> str:
    ah = row.get("ah_line")
    if ah is None or abs(float(ah)) < 1e-9:
        return "Linea pareja"
    favorite_price = row.get("home_odds_decimal") if float(ah) > 0 else row.get("away_odds_decimal")
    if favorite_price is None:
        return "Sin precio"
    price = float(favorite_price)
    if price <= 1.85:
        return "Favorito protegido"
    if price <= 1.95:
        return "Precio equilibrado"
    return "Favorito caro"


def analyze_rows(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    finished = [row for row in rows if _score(row.get("score")) is not None]
    home_team_stats: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {"matches": 0, "wins": 0, "draws": 0, "losses": 0, "gf": 0, "ga": 0, "covers": 0, "cover_samples": 0}
    )
    for row in finished:
        home, away = _score(row["score"]) or (0, 0)
        stats = home_team_stats[row["home_team"]]
        stats["matches"] += 1
        stats["gf"] += home
        stats["ga"] += away
        if home > away:
            stats["wins"] += 1
        elif home == away:
            stats["draws"] += 1
        else:
            stats["losses"] += 1
        covered = _home_cover(row)
        if covered is not None:
            stats["cover_samples"] += 1
            stats["covers"] += int(covered)

    profiles: Dict[str, Dict[str, Any]] = {}
    for team, stats in home_team_stats.items():
        matches = stats["matches"]
        win_rate = stats["wins"] / matches if matches else 0
        cover_rate = stats["covers"] / stats["cover_samples"] if stats["cover_samples"] else None
        if matches < 3:
            label = "Muestra corta"
        elif win_rate >= 0.55 or (cover_rate is not None and cover_rate >= 0.58):
            label = "Duro en casa"
        elif win_rate <= 0.30 and (cover_rate is None or cover_rate <= 0.42):
            label = "Peor en casa"
        else:
            label = "Local neutro"
        profiles[team] = {
            **stats,
            "win_rate": round(win_rate * 100, 1),
            "cover_rate": round(cover_rate * 100, 1) if cover_rate is not None else None,
            "avg_goal_diff": round((stats["gf"] - stats["ga"]) / matches, 2) if matches else 0,
            "label": label,
        }

    groups: Dict[Tuple[str, str, str, str, str], Dict[str, Any]] = defaultdict(
        lambda: {"matches": 0, "home_wins": 0, "favorite_covers": 0, "favorite_samples": 0, "overs": 0, "ou_samples": 0}
    )
    enriched_rows: List[Dict[str, Any]] = []
    for row in rows:
        copy = dict(row)
        profile = profiles.get(row.get("home_team"), {"label": "Sin muestra"})
        copy["home_profile"] = profile
        copy["ah_bucket"] = _ah_bucket(row.get("ah_line"))
        copy["price_band"] = _price_band(row)
        score = _score(row.get("score"))
        if score:
            key = (
                row["competition_name"], row["stage_name"], copy["ah_bucket"],
                copy["price_band"], profile["label"],
            )
            group = groups[key]
            group["matches"] += 1
            group["home_wins"] += int(score[0] > score[1])
            favorite_cover = _favorite_cover(row)
            if favorite_cover is not None:
                group["favorite_samples"] += 1
                group["favorite_covers"] += int(favorite_cover)
            if row.get("ou_line") is not None:
                group["ou_samples"] += 1
                group["overs"] += int(sum(score) > float(row["ou_line"]))
        enriched_rows.append(copy)

    patterns: List[Dict[str, Any]] = []
    for key, values in groups.items():
        if values["matches"] < 4:
            continue
        competition, stage, ah_bucket, price_band, home_profile = key
        patterns.append(
            {
                "competition": competition,
                "stage": stage,
                "ah_bucket": ah_bucket,
                "price_band": price_band,
                "home_profile": home_profile,
                "matches": values["matches"],
                "home_win_rate": round(values["home_wins"] * 100 / values["matches"], 1),
                "favorite_cover_rate": round(values["favorite_covers"] * 100 / values["favorite_samples"], 1)
                if values["favorite_samples"] else None,
                "over_rate": round(values["overs"] * 100 / values["ou_samples"], 1)
                if values["ou_samples"] else None,
                "confidence": "alta" if values["matches"] >= 30 else "media" if values["matches"] >= 12 else "baja",
            }
        )
    patterns.sort(key=lambda item: (item["matches"], item.get("favorite_cover_rate") or 0), reverse=True)
    teams = [{"team": team, **values} for team, values in profiles.items()]
    teams.sort(key=lambda item: (item["label"] == "Duro en casa", item["matches"], item["win_rate"]), reverse=True)
    return {
        "rows": enriched_rows,
        "patterns": patterns,
        "home_teams": teams,
        "summary": {
            "catalogued": len(rows),
            "finished": len(finished),
            "competitions": len({row.get("competition_id") for row in rows}),
            "seasons": len({row.get("season") for row in rows}),
            "deep_enriched": sum(1 for row in rows if row.get("deep_status") == "enriched"),
        },
    }


def load_analysis(
    competition_ids: Optional[Sequence[str]] = None,
    seasons: Optional[Sequence[str]] = None,
    stages: Optional[Sequence[str]] = None,
    limit: int = 5000,
) -> Dict[str, Any]:
    rows = sql_store.fetch_uefa_qualifying_matches(competition_ids, seasons, stages, limit)
    return analyze_rows(rows)


def _competition_from_precache(row: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    league_id = str(row.get("league_id") or "").strip()
    if league_id in COMPETITIONS:
        return league_id, COMPETITIONS[league_id]
    name = str(row.get("league_name") or row.get("league") or "").lower()
    if "conference" in name or "uefa ecl" in name:
        return "2187", COMPETITIONS["2187"]
    if "champions league" in name or "uefa cl" in name:
        return "103", COMPETITIONS["103"]
    if "europa league" in name or "uefa el" in name:
        return "113", COMPETITIONS["113"]
    return None


def load_precache_upcoming() -> List[Dict[str, Any]]:
    """Devuelve los próximos UEFA ya enriquecidos por el flujo normal de Pre-Cacheo."""
    precache_rows = sql_store.fetch_matches(state="precacheo", limit=5000)
    historical = analyze_rows(sql_store.fetch_uefa_qualifying_matches(limit=20000))
    profiles = {row["team"].lower(): row for row in historical.get("home_teams", [])}
    output: List[Dict[str, Any]] = []
    for row in precache_rows:
        competition = _competition_from_precache(row)
        if not competition:
            continue
        competition_id, competition_name = competition
        odds = row.get("main_match_odds") or {}
        ah_line = _line_to_float(
            odds.get("ah_linea") if odds.get("ah_linea") is not None else row.get("handicap")
        )
        ou_line = _line_to_float(odds.get("goals_linea"))
        home_team = str(row.get("home_name") or row.get("home_team") or "")
        away_team = str(row.get("away_name") or row.get("away_team") or "")
        if ah_line is None or abs(ah_line) < 1e-9:
            favorite = "Parejo"
        else:
            favorite = home_team if ah_line > 0 else away_team
        output.append(
            {
                "match_id": str(row.get("match_id") or row.get("id") or ""),
                "competition_id": competition_id,
                "competition_name": competition_name,
                "match_date": row.get("match_date") or row.get("date"),
                "time": row.get("time") or row.get("start_time") or "",
                "home_team": home_team,
                "away_team": away_team,
                "ah_line": ah_line,
                "ou_line": ou_line,
                "favorite": favorite,
                "home_profile": profiles.get(home_team.lower(), {"label": "Sin muestra"}),
                "home_rank": (row.get("home_standings") or {}).get("ranking"),
                "away_rank": (row.get("away_standings") or {}).get("ranking"),
                "last_home": row.get("last_home_match") or {},
                "last_away": row.get("last_away_match") or {},
                "precacheo_date": row.get("precacheo_date"),
            }
        )
    output.sort(key=lambda item: (str(item.get("match_date") or ""), str(item.get("time") or ""), item["match_id"]))
    return output


def load_explorer_cards(
    competition_ids: Optional[Sequence[str]] = None,
    seasons: Optional[Sequence[str]] = None,
    stages: Optional[Sequence[str]] = None,
    page: int = 1,
    per_page: int = 20,
) -> Dict[str, Any]:
    """Construye fichas con la misma forma de datos que el Explorador general."""
    from .pattern_search import explore_matches

    catalogue = sql_store.fetch_uefa_qualifying_matches(
        competition_ids=competition_ids,
        seasons=seasons,
        stages=stages,
        limit=20000,
    )
    meta_by_id = {str(row["match_id"]): row for row in catalogue}
    full_rows = sql_store.fetch_matches_by_ids(list(meta_by_id), limit=20000)
    cards = explore_matches(
        full_rows,
        filters={"limit": 20000, "include_stats": True, "analyze_all": True},
    )
    for card in cards:
        match_id = str(card.get("match_id") or "")
        meta = meta_by_id.get(match_id) or {}
        card["competition_meta"] = {
            "competition_id": meta.get("competition_id"),
            "competition_name": meta.get("competition_name"),
            "season": meta.get("season"),
            "stage_name": meta.get("stage_name"),
            "deep_status": meta.get("deep_status"),
        }
    cards.sort(
        key=lambda card: str((card.get("candidate") or {}).get("date") or ""),
        reverse=True,
    )
    safe_per_page = max(5, min(int(per_page or 20), 50))
    total = len(cards)
    pages = max(1, (total + safe_per_page - 1) // safe_per_page)
    safe_page = max(1, min(int(page or 1), pages))
    start = (safe_page - 1) * safe_per_page
    return {
        "cards": cards[start:start + safe_per_page],
        "total": total,
        "page": safe_page,
        "per_page": safe_per_page,
        "pages": pages,
    }
