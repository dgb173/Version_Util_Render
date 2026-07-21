import importlib
import json
from pathlib import Path


app_module = importlib.import_module("src.app")


def test_sofascore_endpoint_returns_service_payload(monkeypatch):
    expected = {
        "available": True,
        "tournament": "Liga de prueba",
        "views": {"total": [{"team": "Local"}]},
    }
    monkeypatch.setattr(
        app_module.sofascore_context,
        "get_league_table_context",
        lambda **kwargs: {**expected, "received": kwargs},
    )

    response = app_module.app.test_client().post(
        "/api/sofascore/league-table",
        json={
            "home_name": "Local",
            "away_name": "Visitante",
            "league_name": "Liga de prueba",
            "match_date": "2026-07-18",
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["available"] is True
    assert payload["received"]["home_name"] == "Local"
    assert payload["received"]["away_name"] == "Visitante"


def test_sofascore_endpoint_retries_a_transient_provider_failure(monkeypatch):
    calls = []

    def fake_context(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            return {"available": False, "reason": "provider_unavailable", "views": {}}
        return {"available": True, "views": {"total": [{"team": "Local"}]}}

    monkeypatch.setattr(app_module.sofascore_context, "get_league_table_context", fake_context)
    monkeypatch.setattr(app_module.time, "sleep", lambda _seconds: None)

    response = app_module.app.test_client().post(
        "/api/sofascore/league-table",
        json={"home_name": "Local", "away_name": "Visitante", "match_date": "2026-07-20"},
    )

    assert response.status_code == 200
    assert response.get_json()["available"] is True
    assert len(calls) == 2


def test_study_panel_contains_table_button_and_modal(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    match = json.loads((root / "sample_rich_match.json").read_text(encoding="utf-8"))
    monkeypatch.setattr(app_module, "analizar_partido_completo", lambda *args, **kwargs: dict(match))
    monkeypatch.setattr(app_module, "save_match_to_json", lambda payload: True)

    response = app_module.app.test_client().get("/api/estudio_panel/12345")

    assert response.status_code == 200
    html = response.get_json()["html"]
    assert "league-table-trigger" in html
    assert 'id="leagueTableModal"' in html
    assert "league_table_modal.js" in html


def test_precacheo_replaces_market_correlations_with_league_table_button():
    root = Path(__file__).resolve().parents[1]
    html = (root / "src" / "templates" / "precacheo.html").read_text(encoding="utf-8")
    script = (root / "src" / "static" / "js" / "league_table_modal.js").read_text(encoding="utf-8")

    assert "league-table-trigger--compact" in html
    assert 'id="leagueTableModal"' in html
    assert "showMarketCorrelations" not in html
    assert "analysis: 'Lectura AH / O-U'" in script
    assert "total: 'General'" in script
    assert "home: 'En casa'" in script
    assert "away: 'Fuera'" in script
    assert "renderOuTable" in script
    assert "renderAnalysis" in script
    assert "buildHandicapDiagnosis" in script
    assert 'data-favorite-last-rival' in html
    assert "data-ou-line" in script
    assert "openStatusModal(button, 'loading')" in script
    assert "button.remove()" not in script
