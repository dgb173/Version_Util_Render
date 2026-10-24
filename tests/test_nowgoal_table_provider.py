from src.modules import nowgoal_table_provider


def test_resolve_league_id():
    assert nowgoal_table_provider.resolve_league_id("USA Major League Soccer", "21") == "21"
    assert nowgoal_table_provider.resolve_league_id("USA Major League Soccer", None) == "21"
    assert nowgoal_table_provider.resolve_league_id("Categoria Primera A", "250") == "250"


def test_table_provider_returns_full_standings():
    result = nowgoal_table_provider.get_league_table_context(
        "Seattle Sounders", "Sporting Kansas City", "USA Major League Soccer", "21"
    )
    assert result["available"] is True
    assert result["source"] == "NowGoal"
    assert len(result["views"]["total"]) >= 20
    assert result["home_team_id"] is not None
    assert result["away_team_id"] is not None
    assert result["home_team_id"] != result["away_team_id"]


def test_table_provider_fallback_h2h():
    fallback = nowgoal_table_provider.build_h2h_standings_fallback(
        home_name="Team A",
        away_name="Team B",
        league_name="Test Cup",
        home_standings={"ranking": "1", "total_pj": "5", "total_v": "3", "total_e": "1", "total_d": "1", "total_gf": "8", "total_gc": "4"},
        away_standings={"ranking": "2", "total_pj": "5", "total_v": "2", "total_e": "2", "total_d": "1", "total_gf": "6", "total_gc": "5"},
    )
    assert fallback["available"] is True
    assert fallback["partial"] is True
    assert len(fallback["views"]["total"]) == 2
    assert fallback["views"]["total"][0]["team"] == "Team A"


def test_api_sofascore_league_table_endpoint():
    from src import app as app_module
    client = app_module.app.test_client()
    resp = client.post("/api/sofascore/league-table", json={
        "home_name": "Seattle Sounders",
        "away_name": "Sporting Kansas City",
        "league_name": "USA Major League Soccer",
        "nowgoal_league_id": "21",
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["available"] is True
    assert len(data["views"]["total"]) >= 20
