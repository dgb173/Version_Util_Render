from src.modules.context_txt_exporter import format_match_bundle, format_pre_match_context


def _match():
    return {
        "match_id": "123",
        "home_name": "Equipo Casa",
        "away_name": "Equipo Fuera",
        "pre_match_context": {
            "current": {
                "date": "2026-08-05",
                "home_name": "Equipo Casa",
                "away_name": "Equipo Fuera",
                "home_matches": [
                    {"date": "2026-08-01", "home": "Equipo Casa", "away": "Rival A", "score": "2:0", "ahLine": "0.5"},
                ],
                "away_matches": [
                    {"date": "2026-08-02", "home": "Rival B", "away": "Equipo Fuera", "score": "1:1", "ahLine": "0"},
                ],
                "similar_ah": {
                    "home": {"target_line": 0.5, "threshold": 0.25, "samples": 4, "wins": 3, "draws": 0, "losses": 1, "cover_pct": 75, "goals_for_avg": 2, "goals_against_avg": 1},
                    "away": {"target_line": -0.5, "threshold": 0.25, "samples": 3, "wins": 1, "draws": 1, "losses": 1, "cover_pct": 50, "goals_for_avg": 1, "goals_against_avg": 1.3},
                    "correlation": {"label": "Correlación favorable al local", "confidence": "ORIENTATIVA"},
                },
            },
            "previous": {
                "date": "2026-05-01",
                "score": "1:0",
                "home_name": "Equipo Fuera",
                "away_name": "Equipo Casa",
                "home_matches": [],
                "away_matches": [],
            },
        },
    }


def test_formats_complete_context_as_plain_text():
    text = format_pre_match_context(_match())

    assert "CONTEXTO PREVIO (CASA VS FUERA · MISMA LIGA)" in text
    assert "PARTIDO ACTUAL — Equipo Casa vs Equipo Fuera — 2026-08-05" in text
    assert "2026-08-01 | Equipo Casa 2:0 Rival A | AH 0.5" in text
    assert "Resumen: V 1 | E 0 | D 0" in text
    assert "HÁNDICAPS SIMILARES" in text
    assert "LOCALÍAS INVERTIDAS" in text


def test_bundle_puts_llm_prompt_below_context():
    text = format_match_bundle(_match(), "MI PROMPT LLM", 1, 100)

    assert "PARTIDO 1/100 · ID 123" in text
    assert text.index("CONTEXTO PREVIO") < text.index("PROMPT LLM COMPLETO")
    assert text.index("PROMPT LLM COMPLETO") < text.index("MI PROMPT LLM")
