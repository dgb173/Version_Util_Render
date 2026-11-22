import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import app as flask_app_mod
from modules import data_manager, ficha_completa, team_recent_history


def test_team_recent_history_calc_ah_evaluation_sign_and_splits():
    # Home FAV (+0.5 / +0.75 / +1.5 en NowGoal => local cede goles)
    assert team_recent_history._calc_ah_evaluation("2:0", "0.5", True)["code"] == "COVER"
    assert team_recent_history._calc_ah_evaluation("1:0", "0.75", True)["code"] == "HALF_WIN"
    assert team_recent_history._calc_ah_evaluation("1:0", "1.5", True)["code"] == "NO_COVER"
    assert team_recent_history._calc_ah_evaluation("1:0", "1.0", True)["code"] == "PUSH"

    # Away FAV (-0.25 / -0.75 => visitante cede goles)
    assert team_recent_history._calc_ah_evaluation("0:1", "-0.25", False)["code"] == "COVER"
    assert team_recent_history._calc_ah_evaluation("0:1", "-0.75", False)["code"] == "HALF_WIN"
    assert team_recent_history._calc_ah_evaluation("1:1", "-0.25", False)["code"] == "HALF_LOSS"

    # Away DOG (+0.5 / +0.75 => visitante recibe goles)
    assert team_recent_history._calc_ah_evaluation("1:1", "0.5", False)["code"] == "COVER"
    assert team_recent_history._calc_ah_evaluation("1:0", "0.5", False)["code"] == "NO_COVER"
    assert team_recent_history._calc_ah_evaluation("1:0", "0.75", False)["code"] == "HALF_LOSS"

    # PK
    assert team_recent_history._calc_ah_evaluation("1:1", "0", True)["code"] == "PUSH"


def test_get_team_recent_history_enrichment_and_v2_cache(monkeypatch):
    parent_match = {
        "match_id": "990101",
        "home_name": "Alpha FC",
        "away_name": "Beta FC",
        "recent_home_matches_all": [
            {
                "matchIndex": "501",
                "date": "2026-11-10",
                "league_id": "L1",
                "home": "Alpha FC",
                "away": "Gamma",
                "score": "2:0",
                "ahLine": "1.5",
                "ouLine": "2.5",
            },
            {
                "matchIndex": "502",
                "date": "2026-11-05",
                "league_id": "L1",
                "home": "Delta",
                "away": "Alpha FC",
                "score": "1:1",
                "ahLine": "0.5",
                "ouLine": "2.25",
            },
            {
                "matchIndex": "503",
                "date": "2026-11-01",
                "league_id": "L1",
                "home": "Alpha FC",
                "away": "Epsilon",
                "score": "1:0",
                "ahLine": "1.0",
                "ouLine": "2.0",
            },
        ],
        "recent_away_matches_all": [],
    }

    written_cache = {}
    monkeypatch.setattr(data_manager, "get_precacheo_match", lambda mid: parent_match if str(mid) == "990101" else None)
    monkeypatch.setattr(team_recent_history, "_read_cache", lambda: {})
    monkeypatch.setattr(team_recent_history, "_write_cache", lambda c: written_cache.update(c))

    res = team_recent_history.get_team_recent_history("Alpha FC", parent_match_id="990101", opponent_name="Beta FC")
    assert res["status"] == "success"
    assert any(k.startswith("v2|") for k in written_cache)

    m1 = next(m for m in res["matches"] if m["match_id"] == "501")
    m2 = next(m for m in res["matches"] if m["match_id"] == "502")
    m3 = next(m for m in res["matches"] if m["match_id"] == "503")

    assert m1["team_ah"] == 1.5 and m1["team_role"] == "FAV"
    assert m1["ah_eval"]["code"] == "COVER" and m1["ou_res"] == "UNDER"
    assert m1["goals_for"] == 2 and m1["goals_against"] == 0

    assert m2["team_ah"] == -0.5 and m2["team_role"] == "DOG"
    assert m2["ah_eval"]["code"] == "COVER" and m2["ou_res"] == "UNDER"

    assert m3["ah_eval"]["code"] == "PUSH" and m3["ou_res"] == "UNDER"

    stats = res["stats_all"]
    assert stats["cover_wins"] == 2 and stats["cover_push"] == 1 and stats["cover_losses"] == 0
    assert stats["cover_pct"] == 100.0
    assert stats["fav_n"] == 2 and stats["fav_cover_wins"] == 1 and stats["fav_cover_pct"] == 100.0
    assert stats["dog_n"] == 1 and stats["dog_cover_wins"] == 1 and stats["dog_cover_pct"] == 100.0
    assert stats["overs"] == 0 and stats["unders"] == 3 and stats["over_pct"] == 0.0
    assert stats["goals_for_avg"] == 1.33 and stats["goals_against_avg"] == 0.33


def test_build_ficha_completa_payload_h2h_history_dedup_and_persp(monkeypatch):
    from modules import estudio_scraper

    # Evitar llamadas reales de red en tests unitarios y validar hidratacion en vivo -> h2h_stadium
    monkeypatch.setattr(
        estudio_scraper,
        "analizar_contexto_previo_rapido",
        lambda match_id, **kwargs: {
            "current": {
                "home_name": "Gamba Osaka",
                "away_name": "Tokyo Verdy",
                "league_id": "2872",
                "home_matches": [
                    {"matchIndex": "701", "date": "2026-11-16", "league_id": "2872", "home": "Gamba Osaka", "away": "Shimizu", "score": "2:0", "ahLine": "1", "ouLine": "2.5"},
                    {"matchIndex": "702", "date": "2026-11-01", "league_id": "2872", "home": "Kashima", "away": "Gamba Osaka", "score": "1:1", "ahLine": "0.5", "ouLine": "2.75"},
                ],
                "away_matches": [
                    {"matchIndex": "703", "date": "2026-11-15", "league_id": "2872", "home": "Tokyo Verdy", "away": "Shimizu", "score": "1:2", "ahLine": "0.25", "ouLine": "2.75"},
                ],
            },
            "h2h_data": {
                "all_matches": [
                    {
                        "matchIndex": "601",
                        "date": "2025-10-10",
                        "home": "Gamba Osaka",
                        "away": "Tokyo Verdy",
                        "score": "2:1",
                        "ah": "0.25",
                        "ou": "2.5",
                    },
                    {
                        "matchIndex": "600",
                        "date": "2024-05-03",
                        "home": "Tokyo Verdy",
                        "away": "Gamba Osaka",
                        "score": "0:2",
                        "ah": "-0.25",
                        "ou": "2.25",
                    },
                    {
                        "matchIndex": "599",
                        "date": "2023-08-20",
                        "home": "Tokyo Verdy",
                        "away": "Gamba Osaka",
                        "score": "1:0",
                        "ah": "0.5",
                        "ou": "2.75",
                    },
                ]
            },
        },
    )

    payload = {
        "match_id": "990202",
        "home_name": "Gamba Osaka",
        "away_name": "Tokyo Verdy",
        "league_id": "2872",
        "league_name": "J-League",
        "time": "2026-11-22",
        "handicap": "0.5",
        "goal_line": "2.75",
        "h2h_stadium": {
            "match1_id": "601",
            "date1": "2025-10-10",
            "res1": "2:1",
            "ah1": "0.25",
            "ou1": "2.5",
            "h2h_stad_home": "Gamba Osaka",
            "h2h_stad_away": "Tokyo Verdy",
        },
        "h2h_general": {
            "match6_id": "601",
            "date6": "2025-10-10",
            "res6": "2:1",
            "ah6": "0.25",
            "ou6": "2.5",
            "h2h_gen_home": "Gamba Osaka",
            "h2h_gen_away": "Tokyo Verdy",
        },
    }

    res = ficha_completa.build_ficha_completa_payload(payload, "990202", force_refresh=False)
    assert res["hydrated_from_live"] is True
    assert res["target"]["fav_side"] == "HOME"

    # Normalized row perspective checks
    h_rows = res["tables"]["home_all"]
    assert h_rows[0]["team_role"] == "FAV"
    assert h_rows[0]["ah_delta_vs_target"] == 0.5
    assert h_rows[1]["team_role"] == "DOG"
    assert res["tables"]["summary"]["home_all"]["fav_n"] == 1
    assert res["tables"]["summary"]["home_all"]["dog_n"] == 1

    # H2H History deduplicated & sorted newest -> oldest
    h2h = res["h2h_history"]
    assert [x["match_id"] for x in h2h] == ["601", "600", "599"]

    e_2025, e_2024, e_2023 = h2h
    assert e_2025["year"] == "2025" and e_2025["is_same_stadium"] is True
    assert e_2025["home_persp_ah"] == 0.25
    assert e_2025["ah_delta_vs_today"] == 0.25 and e_2025["ah_mov_dir_vs_today"] == "SUBE"
    assert e_2025["ah_delta_vs_prev"] == 0.0
    assert e_2025["ou_delta_vs_today"] == 0.25 and e_2025["ou_res"] == "OVER" and e_2025["ou_res_vs_today"] == "OVER"
    assert e_2025["cover_own"] == "COVER" and e_2025["cover_today"] == "COVER"

    # Inverted venue 2024: Tokyo Verdy (Home) vs Gamba Osaka (Away), raw AH -0.25 => Gamba (Local today) gave 0.25
    assert e_2024["is_same_stadium"] is False
    assert e_2024["home_persp_ah"] == 0.25
    assert e_2024["wdl_local"] == "W"
    assert e_2024["cover_own"] == "COVER" and e_2024["cover_today"] == "COVER"
    assert e_2024["ah_delta_vs_prev"] == 0.75  # 0.25 - (-0.5)

    # Inverted venue 2023: Tokyo Verdy (Home) vs Gamba Osaka (Away), raw AH +0.5 => Gamba was DOG +0.5 (-0.5 persp)
    assert e_2023["is_same_stadium"] is False
    assert e_2023["home_persp_ah"] == -0.5
    assert e_2023["ah_delta_vs_today"] == 1.0 and e_2023["ah_mov_dir_vs_today"] == "SUBE"
    assert e_2023["wdl_local"] == "L"
    assert e_2023["cover_own"] == "NO_COVER" and e_2023["cover_today"] == "NO_COVER"


def test_ficha_completa_template_and_mov_endpoint(monkeypatch):
    client = flask_app_mod.app.test_client()
    resp = client.get("/ficha-completa/3114552")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)

    assert 'id="fc-btn-tags"' not in html
    assert 'id="fc-tags-slot"' not in html
    assert 'data-tab="h2h"' in html
    assert 'id="fc-tab-h2h-count"' in html
    assert 'id="fc-inspector-clear-all"' in html
    assert html.index('data-tab="teams"') < html.index('data-tab="common"')

    stored_match = {
        "match_id": "3114552",
        "main_match_odds": {"ah_linea": "0.5", "goals_ou": "3.25"},
        "h2h_stadium": {
            "match1_id": "601",
            "date1": "2025-09-12",
            "res1": "1:1",
            "ah1": "0.25",
            "ou1": "2.75",
            "h2h_stad_home": "Gamba Osaka",
            "h2h_stad_away": "Tokyo Verdy",
            "all_matches": [
                {"matchIndex": "601", "date": "2025-09-12", "home": "Gamba Osaka", "away": "Tokyo Verdy", "score": "1:1", "ah": "0.25", "ou": "2.75"},
                {"matchIndex": "600", "date": "2024-04-02", "home": "Tokyo Verdy", "away": "Gamba Osaka", "score": "0:2", "ah": "-0.5", "ou": "2.5"},
            ],
        },
        "h2h_general": {
            "match6_id": "600",
            "date6": "2024-04-02",
            "res6": "0:2",
            "ah6": "-0.5",
            "ou6": "2.5",
            "h2h_gen_home": "Tokyo Verdy",
            "h2h_gen_away": "Gamba Osaka",
        },
    }
    monkeypatch.setattr(data_manager, "get_precacheo_match", lambda mid: stored_match if str(mid) == "3114552" else None)

    mov_resp = client.post(
        "/api/precacheo_h2h_mov_historico",
        json={
            "match_id": "3114552",
            "row_home": "Gamba Osaka",
            "row_away": "Tokyo Verdy",
            "row_ah": "0.5",
            "row_ou": "3.25",
            "row_score": "?:?",
        },
    )
    assert mov_resp.status_code == 200
    body = mov_resp.get_json()
    assert body["status"] == "success"
    assert body["stadium"]["movement"] == "0.25 → 0.5"
    assert body["stadium"]["ou_movement"] == "2.75 → 3.25"
    assert body["general"]["movement"] == "0.5 → 0.5"
    assert len(body["history"]) == 2
    assert body["history"][0]["year"] == "2025"
