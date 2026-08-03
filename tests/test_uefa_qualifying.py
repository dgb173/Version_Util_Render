from modules.uefa_qualifying import _competition_from_precache, _extract_catalog_rows, _line_to_float, _nowgoal_date_to_madrid, analyze_rows
from modules.sql_store import _build_explorer_payload


def test_ratio_lines_are_normalized():
    assert _line_to_float("0.5/1") == 0.75
    assert _line_to_float("2/2.5") == 2.25


def test_nowgoal_asian_timestamp_is_displayed_in_madrid_time():
    assert _nowgoal_date_to_madrid("2025-08-22 02:00") == "2025-08-21 20:00"


def test_extracts_only_qualifying_stages_and_nested_ties():
    data = {
        "LeagueInfo": [2187, "UEFA Conference League"],
        "CupKindList": [[10, 0, "Qualifi 1"], [20, 0, "League Round"]],
        "TeamList": [[1, "Local"], [2, "Visitante"]],
        "ScheduleList": {
            "G10": [[1, 2, 2, 1, [[999, 2187, -1, "2025-07-10 20:00", 1, 2, "2-0", "1-0", "0.5", "0", "2.5"]]]],
            "G20A": [[1000, 2187, -1, "2025-10-10 20:00", 1, 2, "1-1", "0-0", "0", "0", "2.5"]],
        },
    }
    rows = _extract_catalog_rows(data, "2187", "2025-2026", {}, 8)
    assert len(rows) == 1
    assert rows[0]["stage_name"] == "Qualifi 1"
    assert rows[0]["home_team"] == "Local"


def test_home_strength_profiles_are_exposed():
    rows = []
    for index in range(4):
        rows.append({
            "match_id": str(index), "competition_id": "2187", "competition_name": "Conference",
            "season": "2025-2026", "stage_name": "Qualifi 1", "home_team": "Fortin",
            "away_team": f"Rival {index}", "score": "2-0", "ah_line": 0.5, "ou_line": 2.5,
            "deep_status": "catalogued",
        })
    analysis = analyze_rows(rows)
    assert analysis["home_teams"][0]["label"] == "Duro en casa"
    assert analysis["patterns"][0]["matches"] == 4


def test_precache_competition_can_be_inferred_from_name():
    assert _competition_from_precache({"league_name": "UEFA Conference League"})[0] == "2187"
    assert _competition_from_precache({"league_name": "UEFA Europa League"})[0] == "113"
    assert _competition_from_precache({"league_name": "UEFA Champions League"})[0] == "103"
    assert _competition_from_precache({"league_name": "Vietnam Championship U21"}) is None


def test_explorer_payload_keeps_progression_stats_and_reference_ids():
    payload = _build_explorer_payload({
        "match_id": "100",
        "stats_rows": [{"label": "Ataques Peligrosos", "home": "54", "away": "57"}],
        "last_home_match": {"match_id": "99", "score": "1-0", "stats_rows": []},
        "comparativas_indirectas": {
            "left": {"match_id": "98", "score": "2-0", "stats_rows": []},
        },
    })
    assert payload["stats_rows"][0]["label"] == "Ataques Peligrosos"
    assert payload["last_home_match"]["match_id"] == "99"
    assert payload["comparativas_indirectas"]["left"]["match_id"] == "98"


def test_explorer_payload_preserves_same_league_provenance():
    payload = _build_explorer_payload({
        "match_id": "100",
        "last_home_match": {
            "match_id": "99",
            "league_id_hist": "136",
            "history_scope": "same_league",
            "is_different_league": False,
        },
        "comparativas_indirectas": {
            "left": {
                "match_id": "98",
                "league_id_hist": "136",
                "history_scope": "same_league",
                "is_different_league": False,
            },
        },
    })
    assert payload["last_home_match"]["league_id_hist"] == "136"
    assert payload["last_home_match"]["is_different_league"] is False
    assert payload["comparativas_indirectas"]["left"]["history_scope"] == "same_league"
    assert payload["comparativas_indirectas"]["left"]["is_different_league"] is False
