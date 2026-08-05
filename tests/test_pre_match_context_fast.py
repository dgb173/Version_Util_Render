from src.modules.estudio_scraper import (
    _correlate_home_away_handicaps,
    _summarize_similar_handicaps,
)


def test_similar_handicaps_respects_team_perspective_and_cover():
    matches = [
        {"date": "2026-01-01", "home": "Equipo A", "away": "Rival 1", "score": "2:0", "ahLine": "0.5"},
        {"date": "2026-01-02", "home": "Equipo A", "away": "Rival 2", "score": "1:0", "ahLine": "0.75"},
        {"date": "2026-01-03", "home": "Equipo A", "away": "Rival 3", "score": "0:1", "ahLine": "0.5"},
    ]

    summary = _summarize_similar_handicaps(matches, "Equipo A", 0.5, 2.5)

    assert summary["samples"] == 3
    assert summary["covers"] == 2
    assert summary["fails"] == 1
    assert summary["overs"] == 0
    assert summary["unders"] == 3


def test_similar_handicaps_inverts_line_for_away_team():
    matches = [
        {"date": "2026-02-01", "home": "Rival 1", "away": "Equipo B", "score": "0:1", "ahLine": "-0.5"},
        {"date": "2026-02-02", "home": "Rival 2", "away": "Equipo B", "score": "1:1", "ahLine": "-0.25"},
        {"date": "2026-02-03", "home": "Rival 3", "away": "Equipo B", "score": "2:0", "ahLine": "-0.5"},
    ]

    summary = _summarize_similar_handicaps(matches, "Equipo B", 0.5)

    assert summary["samples"] == 3
    assert {row["historical_line"] for row in summary["matches"]} == {0.25, 0.5}


def test_handicap_correlation_requires_samples_and_favors_stronger_side():
    strong = {"samples": 5, "cover_pct": 80, "wins": 4, "draws": 0}
    weak = {"samples": 5, "cover_pct": 20, "wins": 1, "draws": 1}

    result = _correlate_home_away_handicaps(strong, weak)

    assert result["status"] == "HOME"
    assert result["confidence"] == "ORIENTATIVA"
    assert result["gap"] > 12
