from scripts.universal_autotrainer.features import (
    _asian_profit_from_margin,
    _ou_profit,
    build_feature_row,
)
from scripts.universal_autotrainer.predict import predict_match


class _ConstantModel:
    def __init__(self, value):
        self.value = value

    def predict(self, frame):
        return [self.value] * len(frame)


def test_asian_quarter_settlement_project_convention():
    # AH actual +0.75 = local favorito; ganar por 1 produce media victoria.
    assert _asian_profit_from_margin(1, 0.75) == 0.5
    assert _asian_profit_from_margin(2, 0.75) == 1.0
    assert _asian_profit_from_margin(0, 0.25) == -0.5
    # AH actual -0.75 = visitante favorito; empate cubre el lado local.
    assert _asian_profit_from_margin(0, -0.75) == 1.0


def test_ou_quarter_settlement():
    assert _ou_profit(3, 3.25) == -0.5
    assert _ou_profit(4, 3.25) == 1.0
    assert _ou_profit(3, 3.0) == 0.0


def test_grotta_style_inflation_and_common_market_are_detected():
    match = {
        "match_id": "grotta-synthetic",
        "match_date": "2026-07-14",
        "home_name": "Grotta",
        "away_name": "Grindavik",
        "league_name": "Iceland D1",
        "main_match_odds": {"ah_linea": "0.5", "goals_linea": "3.25"},
        "home_standings": {
            "ranking": "8", "specific_pj": "6", "specific_v": "2", "specific_e": "1", "specific_d": "3",
            "specific_gf": "8", "specific_gc": "8",
        },
        "away_standings": {
            "ranking": "10", "specific_pj": "6", "specific_v": "1", "specific_e": "3", "specific_d": "2",
            "specific_gf": "8", "specific_gc": "9",
        },
        "last_home_match": {
            "date": "2026-07-03", "home_team": "Grotta", "away_team": "HK", "score": "2:1",
            "handicap_line_raw": "-0.5",
            "stats_rows": [
                {"label": "Tiros", "home": "15", "away": "18"},
                {"label": "Ataques Peligrosos", "home": "46", "away": "67"},
            ],
        },
        "last_away_match": {
            "date": "2026-07-10", "home_team": "Njardvik", "away_team": "Grindavik", "score": "5:4",
            "handicap_line_raw": "0.5",
            "stats_rows": [{"label": "Ataques Peligrosos", "home": "48", "away": "30"}],
        },
        "comparativas_indirectas": {
            "left": {"date": "2026-05-31", "home_team": "Njardvik", "away_team": "Grotta", "score": "1:2", "ah_line": "1"},
            "right": {"date": "2026-06-21", "home_team": "HK", "away_team": "Grindavik", "score": "2:2", "ah_line": "0.75"},
        },
    }
    row = build_feature_row(match, include_targets=False)
    assert row is not None
    assert row["flag_home_result_inflation"] == 1.0
    assert row["flag_weak_home_condition"] == 1.0
    assert row["flag_hidden_resistant_away"] == 1.0
    # Grotta era valorado a -1 desde su perspectiva; Grindavik a -0.75: el puente prepartido valora mejor al visitante.
    assert row["ind_gap_line_strength"] == -0.25
    assert row["flag_common_market_away"] == 1.0
    assert row["flag_ou_inflated_recent_score"] == 1.0


def test_future_historical_block_is_ignored():
    match = {
        "match_id": "future-leak",
        "match_date": "2026-01-01",
        "home_name": "A",
        "away_name": "B",
        "main_match_odds": {"ah_linea": "0.5", "goals_linea": "2.5"},
        "last_home_match": {
            "date": "2026-01-02", "home_team": "A", "away_team": "C", "score": "8:0", "handicap_line_raw": "3"
        },
    }
    row = build_feature_row(match, include_targets=False)
    assert row is not None
    assert "prev_home_margin" not in row


def test_untouched_test_gate_forces_no_bet():
    match = {
        "match_id": "guardrail", "match_date": "2026-07-14", "home_name": "A", "away_name": "B",
        "main_match_odds": {"ah_linea": "0.5", "goals_linea": "2.5"},
    }
    artifact = {
        "feature_columns": ["current_ah", "current_ou"],
        "ah_model": _ConstantModel(0.5), "ou_model": _ConstantModel(-0.5),
        "ah_threshold": 0.1, "ou_threshold": 0.1,
        "ah_enabled": False, "ou_enabled": False, "calibration": {},
    }
    result = predict_match(match, artifact)
    assert result["ah"]["candidate"] == "A -0.5"
    assert result["ah"]["pick"] == "NO BET"
    assert result["ou"]["candidate"] == "UNDER 2.5"
    assert result["ou"]["pick"] == "NO BET"
