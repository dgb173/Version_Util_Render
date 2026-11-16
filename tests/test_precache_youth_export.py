import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from modules import sql_store
from scripts import build_precache_fast_store as fast_store


YOUTH_MATCH = {
    "match_id": "3114011",
    "home_name": "Inegol Kafkasspor U19",
    "away_name": "Sakaryaspor U19",
    "league_name": "Turkey A2 League U19",
    "handicap": "0.75",
    "match_date": "11/16/2026",
    "summary_stats_status": "complete",
}


def test_precache_export_keeps_visible_youth_match(tmp_path, monkeypatch):
    monkeypatch.setattr(sql_store, "DATA_DIR", tmp_path)
    monkeypatch.setattr(sql_store, "ensure_bootstrap", lambda: None)
    monkeypatch.setattr(sql_store, "_current_upcoming_match_ids", lambda *a, **k: ["3114011"])
    monkeypatch.setattr(sql_store, "fetch_matches_by_ids", lambda *a, **k: [YOUTH_MATCH])

    output = sql_store.export_bucket_to_json("data_precacheo.json")
    assert json.loads(output.read_text(encoding="utf-8"))[0]["match_id"] == "3114011"


def test_fast_store_keeps_same_youth_match(tmp_path, monkeypatch):
    source = tmp_path / "data_precacheo.json"
    source.write_text(json.dumps([YOUTH_MATCH]), encoding="utf-8")
    fast_dir = tmp_path / "fast"
    monkeypatch.setattr(fast_store, "SOURCE_FILES", (source,))
    monkeypatch.setattr(fast_store, "FAST_DIR", fast_dir)
    monkeypatch.setattr(fast_store, "INDEX_FILE", fast_dir / "index.json")

    fast_store.main()
    headers = json.loads((fast_dir / "index.json").read_text(encoding="utf-8"))
    assert [row["match_id"] for row in headers] == ["3114011"]
