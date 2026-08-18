import app as app_module
from modules import llm_exporter


def test_export_all_seasons_groups_rounds_and_marks_missing(monkeypatch):
    extractions = {
        "current": {
            "extraction_id": "current",
            "league_id": "235",
            "league_name": "TAS Premier League",
            "label": "Tasmania 2025",
            "season": "2025",
            "matches": [
                {
                    "id": "101",
                    "round": "1",
                    "home": "Equipo A",
                    "away": "Equipo B",
                    "status": "exists",
                },
                {
                    "id": "102",
                    "round": "2",
                    "home": "Equipo C",
                    "away": "Equipo D",
                    "status": "missing",
                },
            ],
        },
        "older": {
            "extraction_id": "older",
            "league_id": "235",
            "league_name": "TAS Premier League",
            "label": "Tasmania 2024",
            "season": "2024",
            "matches": [
                {
                    "id": "99",
                    "round": "1",
                    "home": "Equipo E",
                    "away": "Equipo F",
                    "status": "exists",
                }
            ],
        },
    }

    monkeypatch.setattr(
        app_module.league_extraction_registry,
        "get_extraction",
        lambda extraction_id: extractions.get(str(extraction_id)),
    )
    monkeypatch.setattr(
        app_module.league_extraction_registry,
        "list_extractions",
        lambda: [
            {"extraction_id": "current", "league_id": "235"},
            {"extraction_id": "older", "league_id": "235"},
        ],
    )
    monkeypatch.setattr(
        app_module.sql_store,
        "get_match",
        lambda match_id: {"match_id": match_id} if str(match_id) in {"99", "101"} else None,
    )
    monkeypatch.setattr(
        llm_exporter,
        "generate_llm_prompt",
        lambda match: f"DATOS COMPLETOS DEL PARTIDO {match['match_id']}",
    )

    response = app_module.app.test_client().get(
        "/api/league-extractions/current/export-all-seasons"
    )
    text = response.get_data(as_text=True)

    assert response.status_code == 200
    assert response.headers["X-League-Seasons"] == "2"
    assert response.headers["X-League-Rounds"] == "3"
    assert response.headers["X-League-Matches"] == "3"
    assert response.headers["X-League-Complete-Matches"] == "2"
    assert text.index("TEMPORADA 2024") < text.index("TEMPORADA 2025")
    assert "JORNADA 1 · 1 PARTIDOS" in text
    assert "JORNADA 2 · 1 PARTIDOS" in text
    assert "ETIQUETA: TODO" in text
    assert "DATOS COMPLETOS DEL PARTIDO 101" in text
    assert "DATOS COMPLETOS NO DISPONIBLES EN LA BASE SQL." in text


def test_resume_missing_league_starts_first_500_and_leaves_the_rest(monkeypatch):
    extraction = {
        "extraction_id": "mls",
        "league_id": "165",
        "league_name": "USA Major League Soccer",
        "season": "2025",
        "company_id": 8,
        "target_ah": None,
        "matches": [
            {"id": str(index), "round": str((index % 34) + 1)}
            for index in range(1, 621)
        ],
    }

    monkeypatch.setattr(
        app_module.league_extraction_registry,
        "get_extraction",
        lambda extraction_id: extraction if str(extraction_id) == "mls" else None,
    )
    monkeypatch.setattr(
        app_module.sql_store,
        "get_match",
        lambda match_id: {"match_id": match_id} if int(match_id) <= 20 else None,
    )

    started_threads = []

    class DummyThread:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def start(self):
            started_threads.append(self.kwargs)

    monkeypatch.setattr(app_module.threading, "Thread", DummyThread)
    with app_module.LEAGUE_AH_JOBS_LOCK:
        app_module.LEAGUE_AH_JOBS.clear()

    response = app_module.app.test_client().post(
        "/api/league-extractions/mls/scrape-missing",
        json={"limit": 500, "workers": 4},
    )
    payload = response.get_json()

    assert response.status_code == 202
    assert payload["status"] == "started"
    assert payload["total"] == 500
    assert payload["remaining_before"] == 600
    assert payload["remaining_after_batch"] == 100
    assert len(started_threads) == 1
    assert len(started_threads[0]["args"][2]) == 500
    assert started_threads[0]["args"][2][0]["id"] == "21"

    with app_module.LEAGUE_AH_JOBS_LOCK:
        app_module.LEAGUE_AH_JOBS.clear()
