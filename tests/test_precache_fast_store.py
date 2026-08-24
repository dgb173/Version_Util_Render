import datetime as dt
import hashlib
import json

from modules import pending_results_query, precache_fast_store


def _write_fast_store(tmp_path, rows):
    fast_dir = tmp_path / ".precacheo_fast"
    fast_dir.mkdir()
    index_file = fast_dir / "index.json"
    headers = []
    for row in rows:
        match_id = str(row["match_id"])
        headers.append(
            {
                "match_id": match_id,
                "handicap": row.get("handicap"),
                "score": row.get("score"),
                "match_date": row.get("match_date"),
                "start_time": row.get("start_time"),
                "time": row.get("time"),
            }
        )
        digest = hashlib.sha256(match_id.encode("utf-8")).hexdigest()
        (fast_dir / f"{digest}.json").write_text(json.dumps(row), encoding="utf-8")
    index_file.write_text(json.dumps(headers), encoding="utf-8")
    return fast_dir, index_file


def test_upcoming_page_prefers_deploy_snapshot_over_stale_sql(monkeypatch, tmp_path):
    fast_dir, index_file = _write_fast_store(
        tmp_path,
        [
            {
                "match_id": "new-1",
                "home_name": "Local actual",
                "away_name": "Visitante actual",
                "match_date": "8/24/2026",
                "time": "18:00",
                "handicap": "0.5",
                "score": "?:?",
            }
        ],
    )
    monkeypatch.setattr(precache_fast_store, "FAST_DIR", fast_dir)
    monkeypatch.setattr(precache_fast_store, "INDEX_FILE", index_file)
    monkeypatch.setattr(precache_fast_store, "_INDEX_MTIME_NS", -1)
    monkeypatch.setattr(precache_fast_store, "_INDEX_ROWS", [])
    monkeypatch.setattr(
        pending_results_query.sql_store,
        "_connect",
        lambda: (_ for _ in ()).throw(AssertionError("No debe consultar Turso")),
    )

    result = pending_results_query.fetch_upcoming_page(
        now=dt.datetime(2026, 8, 24, 12, 0, tzinfo=dt.timezone.utc)
    )

    assert result["total"] == 1
    assert result["matches"][0]["match_id"] == "new-1"
    assert result["matches"][0]["home_name"] == "Local actual"
