import json

from src.modules import sofascore_context as sofa


def test_public_page_requires_matching_tournament_id(monkeypatch):
    class Response:
        text = '<script id="__NEXT_DATA__" type="application/json">' + json.dumps({
            "props": {"pageProps": {"uniqueTournament": {"id": 999}, "standings": []}}
        }) + '</script>'

        def raise_for_status(self):
            pass

    class Session:
        def get(self, *args, **kwargs):
            return Response()

    monkeypatch.setattr(sofa, "_http_session", lambda: Session())
    assert sofa._public_tournament_page(29341) == {}


def test_bangalore_alias_and_public_page_returns_both_teams(monkeypatch):
    sofa._memory_cache.clear()
    monkeypatch.setattr(sofa, "sql_store", None)
    monkeypatch.setattr(sofa, "_resolve_team", lambda *args: None)
    monkeypatch.setattr(sofa, "_fetch_tournament_seasons", lambda *args: ([], True))
    monkeypatch.setattr(sofa, "_fetch_tournament_standings_views", lambda *args, **kwargs: {})
    monkeypatch.setattr(sofa, "_fetch_tournament_events", lambda *args, **kwargs: [])
    monkeypatch.setattr(sofa, "_public_tournament_page", lambda tid: {
        "uniqueTournament": {"id": 29341, "name": "BDFA Super Division League"},
        "seasons": [{"id": 103218, "name": "2026", "year": "2026"}],
        "standings": [{"type": "total", "rows": [
            {"position": 11, "team": {"id": 1125613, "name": "Rebels FC"}, "points": 5},
            {"position": 13, "team": {"id": 1155429, "name": "Stride Sports FC"}, "points": 5},
        ]}],
    })
    result = sofa.get_league_table_context(
        "Rebels FC", "Stride", "India Bangalore Super Division", "10/02/2026"
    )
    assert result["available"] is True
    assert result["season_id"] == 103218
    assert result["home_team_id"] == 1125613
    assert result["away_team_id"] == 1155429
    assert [r["position"] for r in result["views"]["total"]] == [11, 13]
