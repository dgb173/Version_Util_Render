import json

from scripts.update_explorer_recent import update_archive


def test_finished_precache_is_added_to_historical_archive(tmp_path):
    archive = tmp_path / "archive.json"
    snapshot = tmp_path / "snapshot.json"
    precache = tmp_path / "precache.json"
    archive.write_text(json.dumps([
        {"match_id": "old", "match_date": "2026-11-13", "score": "1:0",
         "_explorer_bucket": "data_ah_0.json"},
    ]), encoding="utf-8")
    snapshot.write_text(json.dumps({"finished_matches": [
        {"id": "new", "date": "11/14/2026", "score": "2-1"},
    ]}), encoding="utf-8")
    precache.write_text(json.dumps([
        {"match_id": "new", "handicap": "0.5", "score": "?:?",
         "h2h_col3": {}, "home_name": "Home", "away_name": "Away"},
    ]), encoding="utf-8")

    assert update_archive(archive, snapshot, [precache]) == 1
    rows = json.loads(archive.read_text(encoding="utf-8"))
    assert rows[0]["match_id"] == "new"
    assert rows[0]["score"] == "2:1"
    assert rows[0]["match_date"] == "2026-11-14"
    assert rows[0]["_explorer_bucket"] == "data_ah_0.5.json"
    assert rows[1]["match_id"] == "old"
