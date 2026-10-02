from src.modules import nowgoal_verified_tables


def test_verified_snapshot_requires_matching_league_id_and_name():
    assert not nowgoal_verified_tables.get_context(
        "Seattle Sounders", "Sporting Kansas City", "Another league", "21"
    )["available"]
    assert not nowgoal_verified_tables.get_context(
        "Seattle Sounders", "Sporting Kansas City", "USA Major League Soccer", "not-an-id"
    )["available"]


def test_verified_snapshot_has_full_table_and_distinct_match_teams():
    result = nowgoal_verified_tables.get_context(
        "Seattle Sounders", "Sporting Kansas City", "USA Major League Soccer", "21"
    )
    assert result["available"] is True
    assert result["source"] == "NowGoal (copia verificada)"
    assert len(result["views"]["total"]) == 30
    assert result["home_team_id"] != result["away_team_id"]
