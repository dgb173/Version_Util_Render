from modules.pattern_search import explore_matches


def _candidate(match_id, ah, score, previous):
    home = f"Home {match_id}"
    away = f"Away {match_id}"
    row = {
        "match_id": match_id,
        "home_name": home,
        "away_name": away,
        "match_date": "2026-09-01 12:00",
        "score": score,
        "main_match_odds": {"ah_linea": ah, "goals_linea": 3.0},
    }
    if ah > 0:
        row["last_home_match"] = {
            "date": "2026-08-20",
            "home_team": home,
            "away_team": "Previous rival",
            **previous,
        }
    else:
        row["last_away_match"] = {
            "date": "2026-08-20",
            "home_team": "Previous rival",
            "away_team": away,
            **previous,
        }
    return row


def test_home_favorite_previous_cover_uses_previous_handicap_and_movement():
    row = _candidate(
        "home-fav",
        0.5,
        "2:1",
        {"score": "4:2", "handicap_line_raw": 1.5},
    )

    result = explore_matches(
        [row],
        {
            "exact_handicap": 0.5,
            "favorite_side": "HOME",
            "prev_favorite_ah": 1.5,
            "prev_favorite_result": "COVER",
            "prev_favorite_movement": "DOWN",
        },
    )

    assert len(result) == 1
    previous = result[0]["favorite_previous"]
    assert previous["team_market_line"] == 1.5
    assert previous["cover_result"] == "COVER"
    assert previous["movement"] == "1.5 -> 0.5"
    assert previous["movement_direction"] == "DOWN"


def test_previous_cover_is_not_recomputed_with_current_half_goal_line():
    # A 1-0 win covers today's -0.5, but did NOT cover the actual previous -1.5.
    row = _candidate(
        "short-win",
        0.5,
        "2:0",
        {"score": "1:0", "handicap_line_raw": 1.5},
    )

    wrong_filter = explore_matches(
        [row],
        {"prev_favorite_result": "COVER"},
    )
    correct_filter = explore_matches(
        [row],
        {"prev_favorite_result": "NO_COVER"},
    )

    assert wrong_filter == []
    assert len(correct_filter) == 1
    assert correct_filter[0]["favorite_previous"]["cover_result"] == "NO_COVER"


def test_away_favorite_previous_line_is_oriented_to_the_favorite():
    row = _candidate(
        "away-fav",
        -0.5,
        "0:1",
        {"score": "0:2", "handicap_line_raw": -1.5},
    )

    result = explore_matches(
        [row],
        {
            "favorite_side": "AWAY",
            "prev_favorite_ah": 1.5,
            "prev_favorite_result": "COVER",
            "prev_favorite_movement": "DOWN",
        },
    )

    assert len(result) == 1
    previous = result[0]["favorite_previous"]
    assert previous["team_is_home"] is False
    assert previous["team_market_line"] == 1.5
    assert previous["cover_result"] == "COVER"
    assert previous["movement"] == "1.5 -> 0.5"


def test_legacy_previous_result_filter_now_compares_actual_cover():
    row = _candidate(
        "legacy",
        0.5,
        "2:1",
        {"score": "1:0", "handicap_line_raw": 1.5},
    )

    assert explore_matches([row], {"prev_home_wdl": "COVER"}) == []
    assert len(explore_matches([row], {"prev_home_wdl": "NO_COVER"})) == 1
