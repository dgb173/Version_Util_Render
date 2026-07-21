from src.modules import league_evolution_learning as learning


def test_favorite_movement_is_normalized_for_home_and_away():
    assert learning._fav_move(.5, .75, "HOME") == "TO_FAVORITE"
    assert learning._fav_move(-.5, -.75, "AWAY") == "TO_FAVORITE"
    assert learning._fav_move(.75, .5, "HOME") == "TO_DOG"
    assert learning._fav_move(-.75, -.5, "AWAY") == "TO_DOG"


def test_decision_requires_multiple_patterns_and_support():
    assert learning._decision({"probability": 70, "patterns": [{}], "effective_sample": 100}, "OVER", "UNDER")["pick"] == "NO BET"
    accepted = learning._decision({"probability": 61, "patterns": [{}, {}], "effective_sample": 80}, "OVER", "UNDER")
    assert accepted["pick"] == "OVER"


def test_pattern_training_never_reads_outcome_as_feature():
    tokens = {name: "X" for spec in learning.PATTERN_SPECS for name in spec}
    rows = [{"tokens": tokens, "fav_cover": index % 2 == 0, "over": index % 3 == 0, "combo": False}
            for index in range(20)]

    patterns = learning._build_patterns(rows)

    assert patterns
    assert all("fav_cover" not in pattern["spec"] and "over" not in pattern["spec"] for pattern in patterns.values())
