from src.modules import estudio_scraper as es


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
        return []

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
    assert [call["league_id"] for call in calls] == [None, None]
    assert [call["neutral"] for call in calls] == [False, False]
    assert [call["is_home"] for call in calls] == [True, False]
    assert [call["limit"] for call in calls] == [100, 100]


def test_neutral_context_uses_every_venue_and_every_league(monkeypatch):
    calls = _prepare_fast_scraper(monkeypatch, neutral=True)

    result = es.analizar_contexto_previo_rapido("123", current_ah="0.5", current_goal_line="2.5")

    assert result["current"]["is_neutral_venue"] is True
    assert [call["league_id"] for call in calls] == [None, None]
    assert [call["neutral"] for call in calls] == [True, True]


def test_saved_neutral_flag_survives_runner_without_render_config(monkeypatch):
    calls = _prepare_fast_scraper(monkeypatch, neutral=False)

    result = es.analizar_contexto_previo_rapido(
        "123",
        current_ah="0.5",
        current_goal_line="2.5",
        is_neutral_venue=True,
    )

    assert result["current"]["is_neutral_venue"] is True
    assert [call["neutral"] for call in calls] == [True, True]


def test_neutral_league_comes_from_saved_user_configuration(monkeypatch):
    monkeypatch.setattr(
        es.sql_store,
        "get_json_state",
        lambda key, default=None: {"neutras_nombres": ["Liga Prueba"]},
    )

    assert es._is_neutral_league("liga prueba") is True
    assert es._is_neutral_league("Otra liga") is False
