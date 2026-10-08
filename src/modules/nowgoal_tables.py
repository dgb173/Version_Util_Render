"""Tablas publicadas por NowGoal, vinculadas por el ID del feed de partidos."""
import copy
import json
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

from . import sofascore_context as sofa
from .league_scraper import _walk_schedule_matches
from .nowgoal_fetcher import get_requests_session

ROOT = Path(__file__).resolve().parents[2]
BASE = 'https://football.nowgoal26.com'
_lock = threading.Lock()
_cache = {}


def resolve_league(league_name, home_name=None, away_name=None):
    from collections import Counter
    target_league = str(league_name or '').strip().casefold()
    target_home = str(home_name or '').strip().casefold()
    target_away = str(away_name or '').strip().casefold()

    candidates = [
        ROOT / 'data' / 'data_precacheo.json',
        ROOT / 'data' / 'data.json',
        ROOT / 'data.json',
    ]

    id_counter = Counter()

    for path in candidates:
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding='utf-8-sig'))
            rows = payload.values() if isinstance(payload, dict) else payload
            for row in rows:
                if not isinstance(row, dict):
                    continue
                row_league = str(row.get('league_name') or row.get('league') or '').strip().casefold()
                row_home = str(row.get('home_name') or row.get('home_team') or '').strip().casefold()
                row_away = str(row.get('away_name') or row.get('away_team') or '').strip().casefold()

                context = row.get('pre_match_context') or {}
                lid = (
                    row.get('league_id')
                    or context.get('league_id')
                    or (context.get('current') or {}).get('league_id')
                )
                if not lid or not str(lid).isdigit():
                    continue

                lid_str = str(lid)

                # Coincidencia por equipos
                if target_home and target_away and (
                    (target_home in row_home or row_home in target_home) and
                    (target_away in row_away or row_away in target_away)
                ):
                    return lid_str

                # Coincidencia por nombre de liga
                if target_league and (target_league == row_league or target_league in row_league or row_league in target_league):
                    id_counter[lid_str] += 1
        except Exception:
            continue

    if id_counter:
        return id_counter.most_common(1)[0][0]

    return None


def _get(url, as_json=False):
    if urlparse(url).netloc != urlparse(BASE).netloc:
        raise ValueError('Unexpected data host')
    response = get_requests_session().get(url, timeout=15, verify=False)
    response.raise_for_status()
    text = response.content.decode('utf-8-sig')
    return json.loads(text) if as_json else text


def _rows(node, group=''):
    if isinstance(node, dict):
        for key, value in node.items():
            yield from _rows(value, f'{group}/{key}'.strip('/'))
    elif isinstance(node, list):
        if node and not isinstance(node[0], (list, dict)):
            yield group, node
        else:
            for value in node:
                yield from _rows(value, group)


def normalize_tables(data):
    teams = {str(row[0]): row[1] for row in data.get('TeamInfo', []) if len(row) > 1}
    groups = {f'sub_{row[0]}': row[1] for row in data.get('SubLeagueInfo', []) if len(row) > 1}
    views = {}
    for view, key in [('total', 'TotalScoreList'), ('home', 'HomeScoreList'), ('away', 'GuestScoreList')]:
        output = []
        for group, row in _rows(data.get(key, {})):
            if len(row) < (17 if view == 'total' else 15):
                continue
            if view == 'total':
                pos, tid, pj, w, d, l, gf, ga, gd, points = [row[i] for i in (1, 2, 4, 5, 6, 7, 8, 9, 10, 16)]
            else:
                pos, tid, pj, w, d, l, gf, ga, gd, points = [row[i] for i in (0, 1, 2, 3, 4, 5, 6, 7, 8, 14)]
            if str(tid) not in teams:
                continue
            output.append(dict(position=pos, team_id=tid, team=teams[str(tid)], short_name=teams[str(tid)],
                               group=groups.get(group, group), matches=pj, wins=w, draws=d, losses=l,
                               scores_for=gf, scores_against=ga, goal_difference=gd, points=points, promotion=''))
        views[view] = output
    return views, teams


def get_context(home_name, away_name, league_name='', league_id=None, season_id=None, goal_line=2.5, **unused):
    lid = str(league_id or resolve_league(league_name) or '')
    if not lid.isdigit():
        return {'available': False, 'reason': 'league_not_linked', 'views': {}}
    season = str(season_id or '')
    if season and not re.fullmatch(r'\d{4}(?:-\d{4})?', season):
        return {'available': False, 'reason': 'invalid_season', 'views': {}}
    key = (lid, season)
    with _lock:
        entry = _cache.get(key)
    cache_file = ROOT / 'data' / 'league_tables' / f'{lid}_{season or "latest"}.json'
    if not entry and cache_file.exists():
        try:
            entry = json.loads(cache_file.read_text(encoding='utf-8'))
        except (OSError, ValueError):
            entry = None
    if entry and time.time() - entry[0] < 3600:
        data, seasons, url = entry[1:]
    else:
        url = f'{BASE}/league/{season + "/" if season else ""}{lid}'
        try:
            page = _get(url)
            match = re.search(r'const\s+_dataPath\s*=\s*"([^"]+)"', page)
            if not match:
                return {'available': False, 'reason': 'standings_not_available', 'source_url': url, 'views': {}}
            data = _get(urljoin(BASE, match[1]), True)
            if str((data.get('LeagueInfo') or [''])[0]) != lid:
                raise ValueError('League mismatch')
            actual_season = str(data['LeagueInfo'][2])
            if season and season != actual_season:
                raise ValueError('Season mismatch')
            try:
                seasons = _get(f'{BASE}/jsData/leagueSeason/sea{lid}.json', True).get('SeasonList', [])
            except Exception:
                seasons = [actual_season]
            with _lock:
                _cache[key] = (time.time(), data, seasons, url)
                cache_file.parent.mkdir(parents=True, exist_ok=True)
                cache_file.write_text(json.dumps(_cache[key], ensure_ascii=False), encoding='utf-8')
        except Exception as exc:
            return {'available': False, 'reason': 'alternative_unavailable', 'source_url': url, 'views': {}}
    views, teams = normalize_tables(data)
    if not views['total']:
        return {'available': False, 'reason': 'standings_not_available', 'source_url': url, 'views': {}}
    def team_id(name):
        exact = [tid for tid, text in teams.items() if text.casefold() == name.casefold()]
        if exact:
            return exact[0]
        ranked = sorted(((sofa._similarity(name, text), tid) for tid, text in teams.items()), reverse=True)
        return ranked[0][1] if ranked and ranked[0][0] >= .85 else None
    hid, aid = team_id(home_name), team_id(away_name)
    events, seen = [], set()
    for sub, group, row in _walk_schedule_matches(data.get('ScheduleList', {})):
        if str(row[0]) in seen or row[2] != -1:
            continue
        score = re.fullmatch(r'(\d+)\s*[-:]\s*(\d+)', str(row[6]))
        if not score:
            continue
        seen.add(str(row[0]))
        events.append({'id': row[0], 'status': {'type': 'finished'},
                       'homeTeam': {'id': row[4], 'name': teams.get(str(row[4]), str(row[4]))},
                       'awayTeam': {'id': row[5], 'name': teams.get(str(row[5]), str(row[5]))},
                       'homeScore': {'current': int(score[1])}, 'awayScore': {'current': int(score[2])}})
    try:
        line = float(goal_line)
    except (ValueError, TypeError):
        line = 2.5
    tables = {}
    for value in sorted({1.5, 2.5, 3.5, 4.5, line}):
        ou_views = sofa._build_ou_views(events, value)
        tables[f'{value:g}'] = {'line': value, 'views': ou_views, 'signal': sofa._ou_signal(ou_views, hid, aid)}
    selected = tables[f'{line:g}']
    return {'available': True, 'source': 'NowGoal', 'source_url': url, 'provider': 'nowgoal',
            'fetched_at': datetime.now(timezone.utc).isoformat(), 'tournament': data['LeagueInfo'][1],
            'tournament_id': None, 'nowgoal_league_id': lid, 'season_id': data['LeagueInfo'][2],
            'season': data['LeagueInfo'][2], 'seasons': [{'id': s, 'name': s, 'year': s} for s in seasons],
            'home_name': home_name, 'away_name': away_name, 'home_team_id': hid, 'away_team_id': aid,
            'views': views, 'ou': {**selected, 'tables': tables, 'matches_analyzed': len(events),
                                 'complete': True, 'scope': 'Resultados de la temporada completa, todas las fases'},
            'external_links': {}}
