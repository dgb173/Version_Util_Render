from src.modules import estudio_scraper as es
from bs4 import BeautifulSoup


def test_match_info_keeps_escaped_apostrophe_in_team_name():
    soup = BeautifulSoup(
        "<script>var _matchInfo = {hId:parseInt('1'),gId:parseInt('2'),"
        "sclassId:parseInt('783'),hName:'Hapoel Tirat Karmel',"
        "gName:'Hapoel Beit She\\'any',lName:'ISR LA'};</script>",
        "html.parser",
    )

    assert es.get_team_league_info_from_script_of(soup)[4] == "Hapoel Beit She'any"


def test_history_team_key_accepts_nowgoal_quote_variants():
    assert es._history_team_key("Hapoel Beit She'any") == es._history_team_key('Hapoel Beit She"any')
    assert es._history_team_key("Hapoel Beit She＇any") == es._history_team_key("Hapoel Beit She'any")
    assert es._history_team_key("Hapoel Beit She&#39;any") == es._history_team_key("Hapoel Beit She'any")


def test_youth_and_adult_team_are_not_merged():
    assert es._normalize_team_name('Hapoel U19') != es._normalize_team_name('Hapoel')


def test_same_team_quotes_do_not_reverse_recent_home_perspective():
    row = {'home': "Hapoel Beit She'any", 'away': 'Other', 'score_raw': '2-0'}
    result = es._public_last_match(row, general_fallback=False, team_name="Hapoel Beit She＇any")
    assert result['subject_is_home'] is True


def test_recurrent_pickem_line_accepts_numeric_zero_and_quote_variants():
    rows = [{'date':'2026-10-01','home':"Hapoel Beit She'any",'away':'Other','score':'2:0','ahLine':0}]
    summary = es._summarize_similar_handicaps(rows,"Hapoel Beit She＇any",0)
    assert summary['samples'] == 1


def test_away_history_survives_wide_apostrophe_in_header():
    cells = ('<td>ISR LA</td><td><span name="timeData" data-t="2026-09-23 12:00:00"></span></td>'
             '<td><a onclick="team(1)">FC Tzeirey Tamra</a></td>'
             '<td><span class="fscore_2">1-0</span></td>'
             '<td><a onclick="team(2)">Hapoel Beit She\'any</a></td>'
             + '<td></td>' * 6 + '<td data-o="0.5"></td>')
    soup = BeautifulSoup('<table id="table_v2"><tr id="tr2_0" index="123" title="783">' + cells + '</tr></table>', 'html.parser')
    rows = es.extract_recent_matches(soup, 'table_v2', "Hapoel Beit She＇any", '783', False)
    assert len(rows) == 1
    assert rows[0]['matchIndex'] == '123'
    assert rows[0]['score'] == '1:0'


def _prepare_fast_scraper(monkeypatch, neutral):
    calls = []
    monkeypatch.setattr(es, "_load_main_match_soup", lambda match_id: object())
    monkeypatch.setattr(
        es,
        "get_team_league_info_from_script_of",
        lambda soup: ("10", "20", "1635", "Equipo Casa", "Equipo Fuera", "Liga Prueba"),
    )
    monkeypatch.setattr(es, "_is_neutral_league", lambda league: neutral)
    monkeypatch.setattr(es, "extract_vs_odds", lambda soup: {})
    if hasattr(es, "extract_vs_market_odds"):
        monkeypatch.setattr(es, "extract_vs_market_odds", lambda soup: {})

    def recent(soup, table_id, team, league_id, is_home, odds_map, limit, is_neutral_venue=False):
        calls.append({
            "table": table_id,
            "league_id": league_id,
            "is_home": is_home,
            "limit": limit,
            "neutral": is_neutral_venue,
        })
        return [{'date': '2026-07-01', 'home': 'Equipo Casa', 'away': 'Equipo Fuera', 'score': '1:0'}]

    monkeypatch.setattr(es, "extract_recent_matches", recent)
    monkeypatch.setattr(es, "extract_h2h_data_of", lambda *args, **kwargs: {})
    monkeypatch.setattr(es, "extract_previous_h2h_context", lambda data: None)
    monkeypatch.setattr(es, "extract_match_date_of", lambda soup: "2026-08-05")
    return calls


def test_normal_context_uses_all_leagues_but_keeps_home_away_venue(monkeypatch):
    calls = _prepare_fast_scraper(monkeypatch, neutral=False)

    result = es.analizar_contexto_previo_rapido("123", current_ah="0.5", current_goal_line="2.5")

    assert result["context_data_version"] == 2
    assert result["current"]["is_neutral_venue"] is False
    assert [call["league_id"] for call in calls] == [None] * 4
    assert [call["neutral"] for call in calls] == [False, False, True, True]
    assert [call["is_home"] for call in calls] == [True, False, True, False]
    assert [call["limit"] for call in calls] == [100] * 4
    assert result['current']['away_matches_all']


def test_neutral_context_uses_every_venue_and_every_league(monkeypatch):
    calls = _prepare_fast_scraper(monkeypatch, neutral=True)

    result = es.analizar_contexto_previo_rapido("123", current_ah="0.5", current_goal_line="2.5")

    assert result["current"]["is_neutral_venue"] is True
    assert [call["league_id"] for call in calls] == [None] * 4
    assert [call["neutral"] for call in calls] == [True] * 4


def test_saved_neutral_flag_survives_runner_without_render_config(monkeypatch):
    calls = _prepare_fast_scraper(monkeypatch, neutral=False)

    result = es.analizar_contexto_previo_rapido(
        "123",
        current_ah="0.5",
        current_goal_line="2.5",
        is_neutral_venue=True,
    )

    assert result["current"]["is_neutral_venue"] is True
    assert [call["neutral"] for call in calls] == [True] * 4


def test_neutral_league_comes_from_saved_user_configuration(monkeypatch):
    monkeypatch.setattr(
        es.sql_store,
        "get_json_state",
        lambda key, default=None: {"neutras_nombres": ["Liga Prueba"]},
    )

    assert es._is_neutral_league("liga prueba") is True
    assert es._is_neutral_league("Otra liga") is False
