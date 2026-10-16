import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from modules import nowgoal_fetcher


def test_finished_odds_xml_is_merged_into_result_rows():
    content = (
        "B[0]=[30,'Test League'];"
        "A[0]=[123,30,1,2,'Home','Away','2026-08-31 19:00:00','',-1,2,1,0,0,0,0,0,0,'','',null,'False',null,'',0,'',null,'False',0,0,0,null,0];"
    )
    odds = nowgoal_fetcher._parse_finished_odds_xml(
        "<?xml version='1.0'?><c><match><m>123,1,-0.75,0.9,0.9,2,2,3,4,3,2.5,0.9,0.9</m></match></c>"
    )

    matches = nowgoal_fetcher.parse_matches_from_bf_content(
        content,
        status_filter="finished",
        odds_by_match=odds,
    )

    assert len(matches) == 1
    assert matches[0]["handicap"] == "-0.75"
    assert matches[0]["goal_line"] == "2.5"
    assert matches[0]["league"] == "Test League"


def test_results_feed_excludes_postponed_cancelled_and_pending_rows():
    rows = "".join(
        f"A[{index}]=[{100 + index},30,1,2,'H','A','2026-08-31 19:00:00','',{status},0,0];"
        for index, status in enumerate((-1, -10, -11, -14))
    )
    odds = {
        str(100 + index): {"handicap": "0.5", "goal_line": "2.5"}
        for index in range(4)
    }

    matches = nowgoal_fetcher.parse_matches_from_bf_content(
        "B[0]=[30,'League'];" + rows,
        status_filter="finished",
        odds_by_match=odds,
    )

    assert [match["id"] for match in matches] == ["100"]


def test_finished_cache_requires_numeric_handicap_and_both_venue_forms():
    import app

    assert app._match_has_handicap({"handicap": "0"})
    assert app._match_has_handicap({"main_match_odds": {"ah_linea": "-0.25"}})
    assert not app._match_has_handicap({"handicap": "N/A"})
    assert not app._match_has_handicap({"handicap": ""})

    complete = {
        "recent_home_matches_same_league_specific": [{"match_id": "1"}],
        "recent_away_matches_same_league_specific": [{"match_id": "2"}],
    }
    assert app._has_required_recent_venue_form(complete)
    assert not app._has_required_recent_venue_form({**complete, "recent_away_matches_same_league_specific": []})


def test_worker_does_not_save_when_one_venue_form_is_missing(monkeypatch):
    import app

    monkeypatch.setattr(
        app,
        "analizar_partido_completo",
        lambda *_args, **_kwargs: {
            "match_id": "123",
            "main_match_odds": {},
            "recent_home_matches_same_league_specific": [{"match_id": "1"}],
            "recent_away_matches_same_league_specific": [],
        },
    )
    saved = []
    monkeypatch.setattr(app, "save_match_to_json", lambda data: saved.append(data) or True)

    result = app.process_single_match_worker(
        "123",
        include_summary_stats=False,
        require_recent_venue_form=True,
        source_match={"id": "123", "handicap": "-0.5", "goal_line": "2.5"},
    )

    assert result == (False, "123", None, "missing_recent_home_away_form")
    assert saved == []


def test_finished_batch_uses_results_source_filters_ah_and_passes_form_gate(monkeypatch, tmp_path):
    import app

    source_rows = [
        {"id": "1", "handicap": "0", "goal_line": "2.5"},
        {"id": "2", "handicap": "N/A", "goal_line": "2.5"},
        {"id": "3", "handicap": "-0.5", "goal_line": "2.75"},
    ]

    async def fake_results(**_kwargs):
        return source_rows

    calls = []

    def fake_worker(match_id, **kwargs):
        calls.append((match_id, kwargs))
        return True, match_id, "data_ah_0", "saved"

    monkeypatch.setattr(app, "scrape_main_page_finished_matches_async_direct", fake_results)
    monkeypatch.setattr(app, "load_cache_state", lambda: {"processed_ids": []})
    monkeypatch.setattr(app, "_bounded_scrape_workers", lambda *_args, **_kwargs: 1)
    monkeypatch.setattr(app, "process_single_match_worker", fake_worker)
    monkeypatch.setattr(app.sql_store, "get_match", lambda match_id: {"match_id": match_id})
    app.STOP_CACHE_EVENT.clear()
    output = tmp_path / "shard.json"

    app.process_all_finished_matches_background(
        workers=1,
        export_legacy=False,
        shard_index=0,
        shard_total=1,
        output_shard=str(output),
    )

    assert [match_id for match_id, _ in calls] == ["1", "3"]
    assert all(kwargs["require_recent_venue_form"] is True for _, kwargs in calls)
    assert [kwargs["source_match"]["id"] for _, kwargs in calls] == ["1", "3"]
    assert output.exists()


def test_finished_bot_button_dispatches_the_optimized_workflow(monkeypatch):
    import app

    class FakeResponse:
        status_code = 204
        content = b""
        text = ""

    requests_seen = []

    def fake_post(url, **kwargs):
        requests_seen.append((url, kwargs))
        return FakeResponse()

    monkeypatch.setenv("GITHUB_ACTIONS_TOKEN", "test-token")
    monkeypatch.setenv("ACTIONS_TRIGGER_KEY", "test-key")
    monkeypatch.setattr(app.requests, "post", fake_post)

    response = app.app.test_client().post(
        "/api/precacheo_trigger_bots",
        json={"kind": "finished", "workers": 4},
        headers={"X-Trigger-Key": "test-key"},
    )

    assert response.status_code == 200
    assert requests_seen[0][0].endswith("/cachear-terminados-10x.yml/dispatches")
    assert requests_seen[0][1]["json"]["inputs"]["workers_per_bot"] == "4"
    assert "test-token" not in response.get_data(as_text=True)


def test_finished_bot_falls_back_to_remote_legacy_input_names(monkeypatch):
    import app

    class FakeResponse:
        def __init__(self, status_code, text=""):
            self.status_code = status_code
            self.text = text
            self.content = b"" if status_code == 204 else text.encode()

    responses = [FakeResponse(404, "missing"), FakeResponse(404, "missing"), FakeResponse(204)]
    requests_seen = []

    def fake_post(url, **kwargs):
        requests_seen.append((url, kwargs))
        return responses.pop(0)

    monkeypatch.setenv("GITHUB_ACTIONS_TOKEN", "test-token")
    monkeypatch.delenv("ACTIONS_TRIGGER_KEY", raising=False)
    monkeypatch.setattr(app.requests, "post", fake_post)

    response = app.app.test_client().post(
        "/api/precacheo_trigger_bots",
        json={"kind": "finished", "workers": 4, "handicap": "-0.5", "ou": "2.5"},
    )

    assert response.status_code == 200
    legacy_url, legacy_request = requests_seen[-1]
    assert legacy_url.endswith("/cachear-terminados.yml/dispatches")
    assert legacy_request["json"]["inputs"] == {
        "handicap": "-0.5",
        "ou": "2.5",
        "workers": "4",
        "flush_every": "100",
    }


def test_dispatch_retries_after_github_reports_unexpected_input(monkeypatch):
    import app

    class FakeResponse:
        def __init__(self, status_code, text=""):
            self.status_code = status_code
            self.text = text
            self.content = b"" if status_code == 204 else text.encode()

    responses = [
        FakeResponse(422, 'Unexpected inputs provided: ["workers_per_bot"]'),
        FakeResponse(204),
    ]
    payloads = []

    def fake_post(_url, **kwargs):
        payloads.append(kwargs["json"])
        return responses.pop(0)

    monkeypatch.setenv("GITHUB_ACTIONS_TOKEN", "test-token")
    monkeypatch.delenv("ACTIONS_TRIGGER_KEY", raising=False)
    monkeypatch.setattr(app.requests, "post", fake_post)

    response = app.app.test_client().post(
        "/api/precacheo_trigger_bots",
        json={"kind": "finished", "workers": 4},
    )

    assert response.status_code == 200
    assert "workers_per_bot" in payloads[0]["inputs"]
    assert "workers_per_bot" not in payloads[1]["inputs"]


def test_precache_page_contains_manual_bots_and_lazy_summary_controls():
    template = (PROJECT_ROOT / "src" / "templates" / "precacheo.html").read_text(encoding="utf-8")

    assert "Bots próximos" in template
    assert "Bots terminados" in template
    assert "/api/precacheo_trigger_bots" in template
    assert "/api/precacheo_summary_stats/" in template
