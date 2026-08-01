from bs4 import BeautifulSoup

from modules import estudio_scraper, sql_store
from scripts import build_job_from_snapshot
from scripts import cloud_extract_league


def _row(match_id, league_id, date, home, away, score, table_number="1"):
    return f"""
    <tr id="tr{table_number}_{match_id}" name="{league_id}" index="{match_id}">
      <td>League</td>
      <td><span name="timeData" data-t="{date} 10:00:00"></span></td>
      <td><a onclick="team(1)">{home}</a></td>
      <td><span class="fscore_{table_number}">{score}</span></td>
      <td><a onclick="team(2)">{away}</a></td>
      <td></td><td></td><td></td><td></td><td></td><td></td>
      <td></td><td></td><td></td><td></td>
    </tr>
    """


def _soup(rows, odds_rows):
    return BeautifulSoup(
        f"""
        <html><body>
          <table id="table_v1">{''.join(rows)}</table>
          <script>var Vs_hOdds = {odds_rows};</script>
        </body></html>
        """,
        "lxml",
    )


def test_extracts_ah_and_over_under_from_vs_hodds():
    soup = _soup([], "[[100,8,'0.91','-0.5','0.95','0.88','-0.75','1.02','2.5','3']]")

    market = estudio_scraper.extract_vs_market_odds(soup)

    assert market["100"] == {
        "bookmaker_id": 8,
        "ah_initial": "-0.5",
        "ah_final": "-0.75",
        "ou_initial": "2.5",
        "ou_final": "3",
    }
    assert estudio_scraper.extract_vs_odds(soup) == {"100": "-0.5"}


def test_uses_same_league_general_as_yellow_fallback():
    rows = [
        _row("101", "99", "2026-07-30", "Alpha", "Other League", "2-0"),
        _row("100", "10", "2026-07-20", "Rival", "Alpha", "1-3"),
    ]
    soup = _soup(
        rows,
        "[[100,8,'0.9','0.25','0.9','0.9','0','0.9','2.5','2.75'],"
        "[101,8,'0.9','-0.5','0.9','0.9','-0.25','0.9','3.5','3.25']]",
    )
    market = estudio_scraper.extract_vs_market_odds(soup)

    result = estudio_scraper.extract_last_match_in_league_of(
        soup, "table_v1", "Alpha", "10", True, market,
    )

    assert result["match_id"] == "100"
    assert result["is_general_fallback"] is True
    assert result["is_different_league"] is False
    assert result["history_scope"] == "same_league_general_fallback"
    assert result["subject_is_home"] is False
    assert result["over_under_line_raw"] == "2.5"
    assert result["over_under_result"] == "OVER"


def test_keeps_same_league_specific_as_primary():
    soup = _soup(
        [_row("100", "10", "2026-07-20", "Alpha", "Rival", "1-1")],
        "[[100,8,'0.9','-0.5','0.9','0.9','-0.25','0.9','2','2.25']]",
    )
    market = estudio_scraper.extract_vs_market_odds(soup)

    result = estudio_scraper.extract_last_match_in_league_of(
        soup, "table_v1", "Alpha", "10", True, market,
    )

    assert result["match_id"] == "100"
    assert result["is_general_fallback"] is False
    assert result["history_scope"] == "same_league_specific"
    assert result["over_under_result"] == "PUSH"


def test_never_falls_back_to_another_league():
    soup = _soup(
        [_row("101", "99", "2026-07-30", "Alpha", "Rival", "2-0")],
        "[[101,8,'0.9','-0.5','0.9','0.9','-0.25','0.9','2.5','3']]",
    )
    market = estudio_scraper.extract_vs_market_odds(soup)

    assert estudio_scraper.extract_last_match_in_league_of(
        soup, "table_v1", "Alpha", "10", True, market,
    ) is None


def test_over_under_summary_uses_only_matches_with_valid_market():
    stats = estudio_scraper.calculate_over_under_stats(
        [
            {"ou_result": "OVER"},
            {"ou_result": "OVER"},
            {"ou_result": "UNDER"},
            {"ou_result": "PUSH"},
            {"ou_result": "N/A"},
        ],
        "same_league_general",
    )

    assert stats["total"] == 4
    assert stats["over_pct"] == 50.0
    assert stats["under_pct"] == 25.0
    assert stats["push_pct"] == 25.0


def test_explorer_payload_keeps_fallback_and_over_under_metadata():
    payload = sql_store._build_explorer_payload(
        {
            "match_id": "500",
            "last_home_match": {
                "match_id": "100",
                "over_under_line_raw": "2.5",
                "over_under_result": "OVER",
                "history_scope": "same_league_general_fallback",
                "is_general_fallback": True,
                "is_different_league": False,
            },
            "home_ou_stats_general": {"total": 8, "over_pct": 62.5},
        }
    )

    assert payload["last_home_match"]["over_under_line_raw"] == "2.5"
    assert payload["last_home_match"]["over_under_result"] == "OVER"
    assert payload["last_home_match"]["is_general_fallback"] is True
    assert payload["home_ou_stats_general"]["total"] == 8


def test_old_precache_is_refreshed_once_for_new_history_format():
    old = {"last_home_match": {"match_id": "1"}, "market_analysis_html": "ready"}
    current = {**old, "history_data_version": 2}

    assert build_job_from_snapshot._looks_like_complete_precache(old) is False
    assert build_job_from_snapshot._looks_like_complete_precache(current) is True


def test_cloud_league_forces_upgrade_of_old_history(monkeypatch):
    force_values = []
    monkeypatch.setattr(
        cloud_extract_league.sql_store,
        "get_match",
        lambda _match_id: {"match_id": "100", "history_data_version": 1},
    )
    monkeypatch.setattr(cloud_extract_league, "_store_in_cloud_bucket", lambda _mid: True)

    def fake_scrape(match, league_id, force=False):
        force_values.append(force)
        return {"id": match["id"], "status": "saved", "bucket": "old.json"}

    monkeypatch.setattr(cloud_extract_league, "scrape_match_to_sql", fake_scrape)

    result = cloud_extract_league._process_match({"id": "100"}, "10", force=False)

    assert force_values == [True]
    assert result["bucket"] == cloud_extract_league.CLOUD_BUCKET
