import json

from src.modules import data_manager, explorer_archive, sql_store


def test_explorer_uses_finished_archive_when_sql_has_no_history(tmp_path, monkeypatch):
    archive = tmp_path / "explorer_recent.json"
    archive.write_text(json.dumps([
        {"match_id": "1", "score": "2:1", "_explorer_bucket": "data_ah_0.5.json"},
        {"match_id": "2", "score": "1:1", "_explorer_bucket": "data_ah_0.json"},
    ]), encoding="utf-8")
    monkeypatch.setattr(explorer_archive, "ARCHIVE_PATH", archive)
    monkeypatch.setattr(explorer_archive, "_rows", None)
    monkeypatch.setattr(sql_store, "fetch_matches", lambda **kwargs: [])
    data_manager._clear_explorer_cache()

    first = data_manager.load_explorer_matches(scan_limit=1, offset=0)
    second = data_manager.load_explorer_matches(scan_limit=1, offset=1)
    filtered = data_manager.load_explorer_matches(ah_filter=0.5, scan_limit=2)

    assert [row["match_id"] for row in first] == ["1"]
    assert [row["match_id"] for row in second] == ["2"]
    assert [row["match_id"] for row in filtered] == ["1"]
    data_manager._clear_explorer_cache()
    monkeypatch.setattr(explorer_archive, "_rows", None)
