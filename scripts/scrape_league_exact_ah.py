"""Scrapea una liga de NowGoal por la linea AH visible y la guarda en SQL.

La pagina de liga carga el calendario desde un JSON y las cuotas visibles desde
``/ajax/LeagueOddsAjax``. Este script reproduce esa misma seleccion antes de
delegar el analisis y la persistencia a los modulos normales de la aplicacion.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import re
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
import urllib3


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))
sys.path.insert(0, str(PROJECT_ROOT))

from modules import data_manager, sql_store  # noqa: E402
from modules.estudio_scraper import analizar_partido_completo  # noqa: E402


BASE_URL = "https://football.nowgoal26.com"
DEFAULT_COMPANY_ID = 8
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
    ),
    "Referer": f"{BASE_URL}/",
}

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--league-id", required=True)
    parser.add_argument("--season", default="", help="Se autodetecta si se omite")
    parser.add_argument("--ah", required=True, type=float, help="Linea exacta visible, p. ej. -1")
    parser.add_argument("--company-id", type=int, default=DEFAULT_COMPANY_ID)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true", help="Reanaliza IDs ya presentes en SQL")
    return parser.parse_args()


def get_text(session: requests.Session, url: str) -> str:
    response = session.get(url, headers=HEADERS, timeout=30, verify=False)
    response.raise_for_status()
    return response.text.lstrip("\ufeff")


def discover_league(session: requests.Session, league_id: str, season: str) -> tuple[str, str]:
    suffix = f"/{season}/{league_id}" if season else f"/{league_id}"
    page_url = f"{BASE_URL}/league{suffix}"
    html = get_text(session, page_url)

    season_match = re.search(r'const\s+_season\s*=\s*"([^"]+)"', html)
    path_match = re.search(r'const\s+_dataPath\s*=\s*"([^"]+)"', html)
    if not season_match or not path_match:
        raise RuntimeError("NowGoal no expuso _season/_dataPath en la pagina de liga")

    discovered_season = season_match.group(1)
    if season and discovered_season != season:
        raise RuntimeError(f"Temporada solicitada {season}, pero la pagina devolvio {discovered_season}")
    return discovered_season, urljoin(BASE_URL, path_match.group(1))


def flatten_schedule(data: dict[str, Any]) -> tuple[dict[str, dict[str, Any]], list[tuple[str, str]]]:
    teams = {str(row[0]): row[1] for row in data.get("TeamInfo", []) if len(row) > 1}
    schedule = data.get("ScheduleList") or {}
    matches: dict[str, dict[str, Any]] = {}
    rounds: list[tuple[str, str]] = []

    for schedule_key, round_map in schedule.items():
        if not isinstance(round_map, dict):
            continue
        sub_id = schedule_key.removeprefix("sub_") if schedule_key.startswith("sub_") else "0"
        for round_key, rows in round_map.items():
            round_match = re.fullmatch(r"R_(.+)", str(round_key))
            if not round_match or not isinstance(rows, list):
                continue
            round_value = round_match.group(1)
            rounds.append((sub_id, round_value))
            for row in rows:
                if not isinstance(row, list) or len(row) < 8:
                    continue
                match_id = str(row[0])
                matches[match_id] = {
                    "id": match_id,
                    "round": round_value,
                    "date": row[3],
                    "home": teams.get(str(row[4]), str(row[4])),
                    "away": teams.get(str(row[5]), str(row[5])),
                    "score": row[6] or "-",
                    "state": row[2],
                }
    return matches, sorted(set(rounds), key=lambda item: (item[0], int(item[1]) if item[1].isdigit() else item[1]))


ODDS_RE = re.compile(r'oddsData\["L_(\d+)"\]\s*=\s*(\[\[.*?\]\]);')


def parse_round_odds(text: str, company_id: int) -> dict[str, dict[str, float]]:
    output: dict[str, dict[str, float]] = {}
    for match_id, raw_rows in ODDS_RE.findall(text):
        try:
            rows = json.loads(raw_rows)
        except json.JSONDecodeError:
            continue
        selected = next((row for row in rows if len(row) >= 4 and int(row[0]) == company_id), None)
        if not selected:
            continue
        output[match_id] = {
            "home_odds_hk": float(selected[1]),
            "ah": float(selected[2]),
            "away_odds_hk": float(selected[3]),
        }
    return output


def select_match_ids(
    session: requests.Session,
    league_id: str,
    season: str,
    rounds: list[tuple[str, str]],
    target_ah: float,
    company_id: int,
) -> dict[str, dict[str, float]]:
    selected: dict[str, dict[str, float]] = {}
    for sub_id, round_value in rounds:
        url = (
            f"{BASE_URL}/ajax/LeagueOddsAjax?sclassId={league_id}"
            f"&subSclassId=0&matchSeason={season}&round={round_value}"
        )
        odds = parse_round_odds(get_text(session, url), company_id)
        for match_id, values in odds.items():
            if abs(values["ah"] - target_ah) < 1e-9:
                selected[match_id] = values
    return selected


def scrape_one(match: dict[str, Any], league_id: str, force: bool) -> dict[str, Any]:
    match_id = match["id"]
    existing = sql_store.get_match(match_id)
    if existing and not force:
        return {"id": match_id, "status": "exists"}

    try:
        result = analizar_partido_completo(match_id, force_refresh=force, check_odds_early=False)
        if isinstance(result, tuple):
            result = result[0]
        if not isinstance(result, dict) or result.get("error"):
            return {"id": match_id, "status": "error", "error": (result or {}).get("error", "sin datos")}

        result["match_id"] = match_id
        result.setdefault("league_id", str(league_id))
        result["league_page_visible_ah"] = match["visible_ah"]
        result["league_page_company_id"] = match["company_id"]
        saved = data_manager.save_match(result)
        if not saved:
            return {"id": match_id, "status": "filtered", "error": "filtros de historial/AH de data_manager"}
        return {"id": match_id, "status": "saved"}
    except Exception as exc:  # pragma: no cover - diagnostico operativo
        return {"id": match_id, "status": "error", "error": str(exc)}


def main() -> int:
    args = parse_args()
    session = requests.Session()
    season, data_url = discover_league(session, str(args.league_id), args.season)
    league_data = json.loads(get_text(session, data_url))
    match_map, rounds = flatten_schedule(league_data)
    selected_odds = select_match_ids(
        session,
        str(args.league_id),
        season,
        rounds,
        args.ah,
        args.company_id,
    )

    selected: list[dict[str, Any]] = []
    for match_id, visible in selected_odds.items():
        base = dict(match_map.get(match_id) or {"id": match_id})
        base["visible_ah"] = visible["ah"]
        base["company_id"] = args.company_id
        base["home_odds_decimal"] = visible["home_odds_hk"] + 1
        base["away_odds_decimal"] = visible["away_odds_hk"] + 1
        selected.append(base)
    selected.sort(key=lambda row: (row.get("date", ""), row["id"]))

    print(f"Liga {args.league_id} | temporada {season} | AH exacto {args.ah:g}")
    print(f"Partidos encontrados: {len(selected)}")
    for row in selected:
        print(
            f"  {row['id']} | {row.get('date', '-')} | {row.get('home', '-')} vs "
            f"{row.get('away', '-')} | {row.get('score', '-')} | "
            f"{row.get('home_odds_decimal', 0):.2f} {row['visible_ah']:g} "
            f"{row.get('away_odds_decimal', 0):.2f}"
        )

    if args.dry_run:
        return 0

    results: list[dict[str, Any]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {
            executor.submit(scrape_one, row, str(args.league_id), args.force): row["id"]
            for row in selected
        }
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            results.append(result)
            suffix = f" | {result.get('error')}" if result.get("error") else ""
            print(f"[{result['status'].upper()}] {result['id']}{suffix}")

    summary: dict[str, int] = {}
    for result in results:
        summary[result["status"]] = summary.get(result["status"], 0) + 1
    print("Resumen:", json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 1 if summary.get("error") else 0


if __name__ == "__main__":
    raise SystemExit(main())
