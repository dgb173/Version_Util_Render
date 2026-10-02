import json

from src.modules import sofascore_public_page as public


def test_public_page_rejects_wrong_tournament(monkeypatch):
    class Response:
        text = '<script id="__NEXT_DATA__">' + json.dumps({
            "props": {"pageProps": {"uniqueTournament": {"id": 1}, "standings": []}}
        }) + '</script>'

        def raise_for_status(self):
            pass

    class Session:
        def get(self, *args, **kwargs):
            return Response()

    monkeypatch.setattr(public.sofa, "_http_session", lambda: Session())
    public._cache.clear()
    assert public._public_page(29341) == {}


def test_bdfa_standings_match_distinct_teams(monkeypatch):
    monkeypatch.setattr(public, "_public_page", lambda tid: {
        "uniqueTournament": {"id": 29341, "name": "BDFA Super Division League"},
        "seasons": [{"id": 103218, "name": "2026"}],
        "standings": [{"type": "total", "rows": [
            {"position": 1, "team": {"id": 1, "name": "FC Bengaluru United"}},
            {"position": 2, "team": {"id": 2, "name": "Bengaluru FC II"}},
            {"position": 3, "team": {"id": 3, "name": "Bangalore City FC"}},
        ]}],
    })
    result = public.get_context("Bengaluru B", "Bangalore City", "India Bangalore Super Division")
    assert result["available"] is True
    assert result["home_team_id"] == 2
    assert result["away_team_id"] == 3
    assert len(result["views"]["total"]) == 3
