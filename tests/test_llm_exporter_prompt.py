from modules import llm_exporter


def test_copy_prompt_excludes_analysis_request_and_internal_ids():
    match = {
        "match_id": "2997200",
        "state": "precacheo",
        "bucket": "data_precacheo.json",
        "home_name": "Equipo Local",
        "away_name": "Equipo Visitante",
        "league_name": "Liga de prueba",
        "match_date": "2026-08-03",
        "handicap": 1.25,
        "main_match_odds": {"goals_linea": 3.25},
        "home_standings": {"ranking": 6, "total_v": 1, "total_e": 2, "total_d": 3},
        "away_standings": {"ranking": 7, "total_v": 0, "total_e": 1, "total_d": 5},
    }

    prompt = llm_exporter.generate_llm_prompt(match)

    assert "Partido: Equipo Local vs Equipo Visitante" in prompt
    assert "ANÁLISIS SOLICITADO" not in prompt
    assert "| Equipo | Pos | Registro | Pts | AH cubierto | Tendencia O/U |" not in prompt
    assert "REGLAS DE CÁLCULO" not in prompt
    assert "Queda PROHIBIDO responder NO BET" not in prompt
    assert "2997200" not in prompt
    assert "data_precacheo.json" not in prompt
    assert "Estado:" not in prompt
    assert "Bucket:" not in prompt


def test_no_cover_text_is_not_misclassified_as_cover():
    match = {
        "home_name": "Local",
        "away_name": "Visitante",
        "handicap": 1.0,
        "h2h_stadium": {"match1_id": "internal", "res1": "0:1"},
        "market_analysis_data": {
            "stadium": {"movement": "0 → 1", "evaluation": "NO CUBIERTO"}
        },
    }

    prompt = llm_exporter.generate_llm_prompt(match)

    assert "Movimiento: 0 → 1 (NO CUBRIÓ)" in prompt
