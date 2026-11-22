import pytest
from bs4 import BeautifulSoup

from src.modules import estudio_scraper, pattern_search_v2


def test_extract_h2h_data_of_includes_ou1_and_ou6():
    html = """
    <table id="table_v3">
        <tr id="tr3_1" index="2002" name="League" vs="0">
            <td>L1</td>
            <td><span name="timeData" data-t="2026-05-16 18:00:00">16-05-26</span></td>
            <td>Team B</td>
            <td><span class="fscore_3">1-2</span></td>
            <td>Team A</td>
            <td></td><td></td><td></td><td></td><td></td><td></td>
            <td data-o="-0.25">-0.25</td>
        </tr>
        <tr id="tr3_2" index="2001" name="League" vs="0">
            <td>L1</td>
            <td><span name="timeData" data-t="2025-02-01 18:00:00">01-02-25</span></td>
            <td>Team A</td>
            <td><span class="fscore_3">3-0</span></td>
            <td>Team B</td>
            <td></td><td></td><td></td><td></td><td></td><td></td>
            <td data-o="0.5">0.5</td>
        </tr>
    </table>
    """
    soup = BeautifulSoup(html, "html.parser")
    odds_map = {
        "2001": {"ah_initial": "0.5", "ou_initial": "2.75"},
        "2002": {"ah_initial": "-0.25", "ou_initial": "2.25"},
    }
    res = estudio_scraper.extract_h2h_data_of(soup, "Team A", "Team B", odds_map=odds_map)
    assert res["match1_id"] == "2001"
    assert res["ou1"] == "2.75"
    assert res["match6_id"] == "2002"
    assert res["ou6"] == "2.25"


def test_extract_h2h_ou_from_h2h_raw_all_strategies(monkeypatch):
    monkeypatch.setattr(pattern_search_v2, "_OU_BY_MATCH_ID", {"9991": 3.5}, raising=False)

    # 1) Direct ou1/ou6
    h2h_direct = {"ou1": "2.5", "match1_id": "1001", "ou6": "3.0", "match6_id": "1002"}
    assert pattern_search_v2._extract_h2h_ou_from_h2h_raw(h2h_direct, False) == (2.5, "1001")
    assert pattern_search_v2._extract_h2h_ou_from_h2h_raw(h2h_direct, True) == (3.0, "1002")

    # 2) Via all_matches matchIndex, and fallback date+score
    h2h_am = {
        "match1_id": "3001",
        "date1": "2025-04-10",
        "match6_id": "",
        "date6": "2025-09-20",
        "all_matches": [
            {"matchIndex": "3001", "date": "2025-04-10", "score": "2:0", "ouLine": "2.25"},
            {"matchIndex": "3002", "date": "2025-09-20", "score": "1:1", "ouLine": "-"},
            {"matchIndex": "3003", "date": "2025-09-20", "score": "3:1", "ouLine": "2.75"},
        ],
    }
    assert pattern_search_v2._extract_h2h_ou_from_h2h_raw(h2h_am, False) == (2.25, "3001")
    assert pattern_search_v2._extract_h2h_ou_from_h2h_raw(
        h2h_am, True, fallback_date="2025-09-20", fallback_score="3:1"
    ) == (2.75, "3003")

    # 3) Fallback _OU_BY_MATCH_ID
    h2h_map = {"match1_id": "9991"}
    assert pattern_search_v2._extract_h2h_ou_from_h2h_raw(h2h_map, False) == (3.5, "9991")


def test_enrich_card_ou_enriches_h2h_own_and_fallback_and_verdict(monkeypatch):
    monkeypatch.setattr(pattern_search_v2, "_OU_BY_MATCH_ID", {}, raising=False)
    card = {
        "match_id": "5000",
        "candidate": {"ou_line": "3.25", "ah_real": "1.5", "score": "2:1"},
        "prev_home": {"match_id": "4001", "score": "2:2", "ah": "0", "ou": "2.75"},
        "prev_away": {"match_id": "4002", "score": "1:2", "ah": "0.75", "ou": "2.5"},
        "h2h_stadium": {"movement": "1 -> 1.5", "score": "2:0", "date": "2025-04-16"},
        "h2h_general": {"movement": "-1 -> 1.5", "score": "1:4", "date": "2026-05-17"},
    }
    h2h_raw = {
        "match1_id": "2001",
        "date1": "2025-04-16",
        "match6_id": "2002",
        "date6": "2026-05-17",
        "all_matches": [
            {"matchIndex": "2001", "date": "2025-04-16", "score": "2:0", "ouLine": "2.5"},
            # h2h_general has no own ouLine -> should fall back to curr_ou (3.25)
            {"matchIndex": "2002", "date": "2026-05-17", "score": "1:4", "ouLine": "-"},
        ],
    }

    enriched = pattern_search_v2._enrich_card_ou(card, h2h_raw=h2h_raw)

    st = enriched["h2h_stadium"]
    assert st["ou"] == "2.5"
    assert st["ou_delta"] == 0.75
    assert st["ou_mov_dir"] == "SUBE"
    assert st["ou_movement"] == "2.5 → 3.25"
    assert st["ou_res"] == "UNDER"
    assert st["ou_ref_source"] == "own"
    assert st["ou_res_vs_curr"] == "UNDER"

    gen = enriched["h2h_general"]
    assert gen.get("ou") is None
    assert gen.get("ou_mov_dir") is None
    assert gen["ou_res"] == "OVER"  # 5 > 3.25 fallback
    assert gen["ou_ref_source"] == "curr"

    feat = pattern_search_v2._extract_card_features(enriched)
    assert feat["h2h_stadium"]["ou_delta"] == 0.75
    assert feat["h2h_stadium"]["ou_mov_dir"] == "SUBE"
    assert feat["h2h_stadium"]["ou_res"] == "UNDER"
    assert feat["h2h_general"]["ou_res"] == "OVER"

    ou_verdict = pattern_search_v2._build_ou_verdict([], [], feat)
    assert "H2H Est: 2.5→3.25 (+0.75, U)" in ou_verdict["target_ou_mov_text"]
    assert "H2H Gen: OVER" in ou_verdict["target_ou_mov_text"]


def test_score_deep_similarity_omits_team_tag_reasons_and_divergences():
    target_feat = {"ah": 0.5, "ah_bucket": 0.5, "ou": 2.5}
    cand_feat = {"ah": 0.5, "ah_bucket": 0.5, "ou": 2.5}
    target_prof = {
        "home_stats": {"all": {"win_pct": 60}, "venue": {"win_pct": 70}},
        "away_stats": {"all": {"win_pct": 30}, "venue": {"win_pct": 20}},
        "home_tags": [{"code": "FORTIN_CASA", "label": "Fortín Casa"}],
        "away_tags": [{"code": "MAL_VISITANTE", "label": "Mal Visitante"}],
        "matchup_tags": [
            {"code": "DOBLE_FUERZA_FAV", "label": "Doble Fuerza Fav"},
            {"code": "CHOQUE_DE_DUROS", "label": "Choque de Duros"},
        ],
        "common_rivals": {"count": 4, "edge": "HOME"},
    }
    cand_prof = {
        "profile_quality": "full",
        "home_stats": {"all": {"win_pct": 60}, "venue": {"win_pct": 70}},
        "away_stats": {"all": {"win_pct": 30}, "venue": {"win_pct": 20}},
        "home_tags": [{"code": "FORTIN_CASA", "label": "Fortín Casa"}],
        "away_tags": [{"code": "MAL_VISITANTE", "label": "Mal Visitante"}],
        "matchup_tags": [{"code": "DOBLE_FUERZA_FAV", "label": "Doble Fuerza Fav"}],
        "common_rivals": {"count": 4, "edge": "HOME"},
    }
    scored = pattern_search_v2._score_deep_similarity(
        target_feat, cand_feat, target_prof, cand_prof, pattern_tier=1, ficha_hydrated=True
    )
    assert "Doble Fuerza Fav" not in scored["matched_reasons"]
    assert not any("Choque de Duros" in d or "Doble Fuerza" in d for d in scored["divergences"])
    assert "Comunes =" in scored["matched_reasons"]
