from src.modules import league_market_tracker as tracker


def test_flatten_calendar_finds_nested_playoff_matches_without_wrapper_rows():
    data = {
        "TeamInfo": [[10, "Local"], [20, "Visitante"]],
        "ScheduleList": {
            "sub_1": {"R_1": [[9001, 381, -1, "2025-01-01 12:00", 10, 20, "2-1", "1-0"]]},
            "sub_2": {"R_1": [[10, 20, 2, 1, [9002, 381, -1, "2025-02-01 12:00", 20, 10, "0-0", "0-0"]]]},
        },
    }

    rows = tracker._flatten_calendar(data, "381")

    assert [row["match_id"] for row in rows] == ["9001", "9002"]
    assert rows[1]["home_team"] == "Visitante"


def test_standings_context_uses_only_matches_played_before_current_one():
    rows = [
        {"match_id": "1", "home_team_id": "a", "away_team_id": "b", "home_score": 3, "away_score": 1},
        {"match_id": "2", "home_team_id": "b", "away_team_id": "a", "home_score": 0, "away_score": 0},
    ]

    context = tracker._standings_context(rows)

    assert context["1"]["home"]["played"] == 0
    assert context["2"]["away"]["played"] == 1
    assert context["2"]["away"]["points"] == 3
    assert context["2"]["league_goal_avg_before"] == 4


def test_movement_band_requires_quarter_goal_change():
    assert tracker._movement_band(.25) == "line_up"
    assert tracker._movement_band(-.25) == "line_down"
    assert tracker._movement_band(.2) == "stable"


def test_mixed_summary_persists_both_ah_and_ou_markets():
    match = {"league_id": "381", "season": "2025", "match_id": "1"}
    summary = [{
        "cid": 8, "cn": "Bet365",
        "ah": {"f": {"u": ".9", "g": "-.5", "d": ".9"}, "l": {"u": ".8", "g": "0", "d": "1"}},
        "ou": {"f": {"u": ".9", "g": "3.25", "d": ".9"}, "l": {"u": ".95", "g": "3", "d": ".85"}},
    }]

    rows, _ = tracker._rows_from_summary(match, summary)

    assert {(row["market"], row["observed_at"], row["line"]) for row in rows} == {
        ("AH", "opening", -.5), ("AH", "closing", 0.0),
        ("OU", "opening", 3.25), ("OU", "closing", 3.0),
    }
