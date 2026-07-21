from datetime import datetime, timezone

from src.modules import sofascore_context as sofa


def _event(home="Real Madrid", away="Athletic Club", timestamp=1779562800):
    return {
        "homeTeam": {"id": 2829, "name": home},
        "awayTeam": {"id": 2825, "name": away},
        "startTimestamp": timestamp,
        "tournament": {
            "name": "LaLiga",
            "uniqueTournament": {"id": 8, "name": "LaLiga"},
        },
        "season": {"id": 77559, "name": "LaLiga 25/26"},
    }


def test_select_team_result_rejects_other_sports():
    payload = {
        "results": [
            {
                "type": "team",
                "entity": {
                    "id": 99,
                    "name": "Real Madrid",
                    "sport": {"slug": "basketball"},
                },
            },
            {
                "type": "team",
                "entity": {
                    "id": 2829,
                    "name": "Real Madrid",
                    "sport": {"slug": "football"},
                },
            },
        ]
    }
    assert sofa._select_team_result(payload, "Real Madrid")["id"] == 2829


def test_select_team_result_respects_women_marker():
    payload = {
        "results": [
            {"type": "team", "entity": {
                "id": 1, "name": "South Hobart", "gender": "M",
                "sport": {"slug": "football"},
            }},
            {"type": "team", "entity": {
                "id": 2, "name": "South Hobart", "gender": "F",
                "sport": {"slug": "football"},
            }},
        ]
    }

    assert sofa._select_team_result(payload, "South Hobart (W)")["id"] == 2


def test_parse_date_accepts_precacheo_month_day_format():
    assert sofa._parse_date("7/19/2026").date().isoformat() == "2026-07-19"


def test_feed_aliases_match_sofascore_team_names():
    assert sofa._similarity("LAN Thurston", "Launceston United") > 0.95
    assert sofa._team_search_queries("Robina City Blue") == [
        "Robina City Blue", "robina city blue", "robina city"
    ]


def test_build_ou_views_calculates_total_home_and_away():
    events = [
        {
            "id": 1,
            "status": {"type": "finished"},
            "homeTeam": {"id": 10, "name": "Local"},
            "awayTeam": {"id": 20, "name": "Visitante"},
            "homeScore": {"current": 3},
            "awayScore": {"current": 1},
        },
        {
            "id": 2,
            "status": {"type": "finished"},
            "homeTeam": {"id": 20, "name": "Visitante"},
            "awayTeam": {"id": 10, "name": "Local"},
            "homeScore": {"current": 1},
            "awayScore": {"current": 0},
        },
    ]

    views = sofa._build_ou_views(events, 2.5)
    local_total = next(row for row in views["total"] if row["team_id"] == 10)
    visitor_home = next(row for row in views["home"] if row["team_id"] == 20)

    assert local_total["matches"] == 2
    assert local_total["over"] == 1
    assert local_total["under"] == 1
    assert local_total["over_pct"] == 50.0
    assert visitor_home["under"] == 1


def test_full_context_retries_search_without_women_suffix(monkeypatch):
    sofa._memory_cache.clear()
    monkeypatch.setattr(sofa, "sql_store", None)
    queries = []

    def fake_get(path, params=None):
        if path == "/search/all":
            queries.append(params["q"])
            if params["q"] == "South Hobart (W)":
                return {"results": []}
            return {"results": [{"type": "team", "entity": {
                "id": 2, "name": "South Hobart", "gender": "F",
                "sport": {"slug": "football"},
            }}]}
        if "/events/next/" in path:
            event = _event("South Hobart", "Launceston United")
            event["homeTeam"]["id"] = 2
            return {"events": [event]}
        if "/events/last/" in path:
            return {"events": []}
        if "/standings/" in path:
            return {"standings": [{"rows": [{
                "position": 1, "team": {"id": 2, "name": "South Hobart"},
                "matches": 1, "wins": 1, "draws": 0, "losses": 0,
                "scoresFor": 2, "scoresAgainst": 0, "points": 3,
            }]}]}
        raise AssertionError(path)

    monkeypatch.setattr(sofa, "_api_get", fake_get)
    result = sofa.get_league_table_context(
        "South Hobart (W)", "Launceston United (W)", "Tasmania Women", "7/19/2026"
    )

    assert result["available"] is True
    assert queries == ["South Hobart (W)", "south hobart"]


def test_select_event_uses_both_teams_and_competition():
    wrong = _event(away="Barcelona")
    correct = _event()
    selected = sofa._select_event(
        [wrong, correct],
        "Real Madrid",
        "Athletic Bilbao",
        "Spain La Liga",
        datetime.fromtimestamp(1779562800, tz=timezone.utc).strftime("%Y-%m-%d"),
    )
    assert selected is correct


def test_flatten_standings_keeps_goals_and_points():
    payload = {
        "standings": [{
            "name": "LaLiga 25/26",
            "rows": [{
                "position": 2,
                "team": {"id": 2829, "name": "Real Madrid"},
                "matches": 38,
                "wins": 27,
                "draws": 5,
                "losses": 6,
                "scoresFor": 77,
                "scoresAgainst": 35,
                "points": 86,
            }],
        }]
    }
    rows = sofa._flatten_standings(payload)
    assert rows == [{
        "group": "LaLiga 25/26",
        "position": 2,
        "team_id": 2829,
        "team": "Real Madrid",
        "short_name": "Real Madrid",
        "matches": 38,
        "wins": 27,
        "draws": 5,
        "losses": 6,
        "scores_for": 77,
        "scores_against": 35,
        "goal_difference": 42,
        "points": 86,
        "promotion": "",
    }]


def test_full_context_normalizes_all_available_views(monkeypatch):
    sofa._memory_cache.clear()
    monkeypatch.setattr(sofa, "sql_store", None)

    search_payload = {
        "results": [{
            "type": "team",
            "entity": {
                "id": 2829,
                "name": "Real Madrid",
                "sport": {"slug": "football"},
            },
        }]
    }
    table_payload = {
        "standings": [{
            "name": "LaLiga 25/26",
            "rows": [
                {"position": 1, "team": {"id": 2829, "name": "Real Madrid"},
                 "matches": 1, "wins": 1, "draws": 0, "losses": 0,
                 "scoresFor": 3, "scoresAgainst": 1, "points": 3},
                {"position": 2, "team": {"id": 2825, "name": "Athletic Club"},
                 "matches": 1, "wins": 0, "draws": 0, "losses": 1,
                 "scoresFor": 1, "scoresAgainst": 3, "points": 0},
            ],
        }]
    }

    def fake_get(path, params=None):
        if path == "/search/all":
            return search_payload
        if "/events/next/" in path:
            return {"events": [_event()]}
        if "/events/last/" in path:
            return {"events": []}
        if "/standings/" in path:
            return table_payload
        raise AssertionError(path)

    monkeypatch.setattr(sofa, "_api_get", fake_get)
    result = sofa.get_league_table_context(
        "Real Madrid", "Athletic Club", "LaLiga", "2026-05-24"
    )

    assert result["available"] is True
    assert set(result["views"]) == {"total", "home", "away"}
    assert result["views"]["total"][0]["scores_for"] == 3
    assert result["home_team_id"] == 2829
    assert result["away_team_id"] == 2825
