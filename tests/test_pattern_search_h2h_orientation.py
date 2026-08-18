from modules.pattern_search import explore_matches


def test_general_h2h_keeps_real_historical_ah_sign_when_venues_are_reversed():
    rows = [
        {
            "match_id": "2850607",
            "home_name": "Ludogorets Razgrad",
            "away_name": "FK Shkendija 79",
            "match_date": "2025-08-28 19:30",
            "score": "2-1",
            "handicap": 1.25,
            "main_match_odds": {"ah_linea": 1.25, "goals_linea": 2.75},
        },
        {
            "match_id": "2850603",
            "home_name": "FK Shkendija 79",
            "away_name": "Ludogorets Razgrad",
            "match_date": "2025-08-21 20:00",
            "score": "2-1",
            "handicap": -0.5,
            "main_match_odds": {"ah_linea": -0.5, "goals_linea": 2.25},
        },
    ]

    result = explore_matches(rows, {"limit": 10, "include_stats": True})
    current = next(row for row in result if row["match_id"] == "2850607")
    general = current["h2h_general"]

    assert general["movement"] == "-0.5 -> 1.25"
    assert general["historical_ah"] == -0.5
    assert general["is_reversed"] is True
    assert general["home_team"] == "FK Shkendija 79"
    assert general["away_team"] == "Ludogorets Razgrad"
