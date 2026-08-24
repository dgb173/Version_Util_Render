import json

from modules import sql_store


def test_precache_export_uses_current_upcoming_rows(monkeypatch, tmp_path):
    monkeypatch.setattr(sql_store, "DATA_DIR", tmp_path)
    monkeypatch.setattr(sql_store, "ensure_bootstrap", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        sql_store,
        "_current_upcoming_match_ids",
        lambda bucket, limit=200: ["current-2", "current-1"],
    )

    requested = []

    def fake_fetch_by_ids(match_ids, **kwargs):
        requested.extend(match_ids)
        return [{"id": match_id} for match_id in match_ids]

    monkeypatch.setattr(sql_store, "fetch_matches_by_ids", fake_fetch_by_ids)
    monkeypatch.setattr(
        sql_store,
        "fetch_matches",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("stale SQL fallback used")),
    )

    output = sql_store.export_bucket_to_json("data_precacheo.json")

    assert requested == ["current-2", "current-1"]
    assert json.loads(output.read_text(encoding="utf-8")) == [
        {"id": "current-2"},
        {"id": "current-1"},
    ]
