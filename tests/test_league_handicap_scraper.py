import json

from modules import league_handicap_scraper
from modules.league_handicap_scraper import (
    parse_league_reference,
    parse_round_odds,
    sanitize_selected_matches,
)


def test_parse_league_reference_accepts_id_and_urls():
    assert parse_league_reference("1453") == ("1453", "")
    assert parse_league_reference("https://football.nowgoal26.com/league/1453") == ("1453", "")
    assert parse_league_reference("https://football.nowgoal26.com/league/2026/1453") == ("1453", "2026")


def test_parse_round_odds_uses_selected_company():
    payload = (
        'oddsData["L_2944012"]='
        '[[3,0.79,-0.5,0.91],[8,0.98,-1,0.83],[35,0.97,-1,0.81]];'
    )
    assert parse_round_odds(payload, company_id=8) == {
        "2944012": {
            "home_odds_hk": 0.98,
            "visible_ah": -1.0,
            "away_odds_hk": 0.83,
        }
    }


def test_sanitize_selected_matches_deduplicates_and_rejects_invalid_rows():
    matches = sanitize_selected_matches(
        [
            {"id": "2944012", "visible_ah": -1},
            {"id": "2944012", "visible_ah": -1},
            {"id": "sin-id", "visible_ah": -1},
            {"id": "2944013", "visible_ah": "N/A"},
        ],
        company_id=8,
    )
    assert matches == [
        {
            "id": "2944012",
            "visible_ah": -1.0,
            "company_id": 8,
            "home": "",
            "away": "",
            "date": "",
            "round": "",
            "sub_id": "0",
            "sub_name": "",
        }
    ]


def test_sanitize_selected_matches_accepts_match_without_visible_ah():
    matches = sanitize_selected_matches(
        [{"id": "2944014", "visible_ah": None, "home": "Local", "away": "Visitante"}],
        company_id=8,
    )
    assert matches[0]["visible_ah"] is None


def test_full_league_preview_keeps_schedule_matches_without_company_odds(monkeypatch):
    league_data = {
        "LeagueInfo": [381, "Liga de prueba"],
        "SubLeagueInfo": [[4, "League"]],
        "TeamInfo": [[10, "Local"], [20, "Visitante"]],
        "ScheduleList": {
            "sub_4": {
                "R_1": [["3000001", 0, -1, "2025-01-01", 10, 20, "2-1", 0]],
            }
        },
    }
    requested_urls = []

    monkeypatch.setattr(
        league_handicap_scraper,
        "_discover_league",
        lambda session, league_id, season: ("2025", "schedule-url"),
    )

    def fake_get_text(session, url):
        requested_urls.append(url)
        return json.dumps(league_data) if url == "schedule-url" else ""

    monkeypatch.setattr(league_handicap_scraper, "_get_text", fake_get_text)
    monkeypatch.setattr(league_handicap_scraper.sql_store, "get_match", lambda match_id: None)

    result = league_handicap_scraper.preview_league_handicap("381", target_ah=None)

    assert result["target_ah"] is None
    assert result["matches"][0]["id"] == "3000001"
    assert result["matches"][0]["visible_ah"] is None
    assert result["matches"][0]["sub_id"] == "4"
    assert result["matches"][0]["sub_name"] == "League"
    assert any("subSclassId=4" in url for url in requested_urls)
