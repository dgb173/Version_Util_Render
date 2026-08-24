import json
import sqlite3

from scripts import build_job_from_snapshot


def test_build_jobs_caps_snapshot_without_reading_uninitialized_queue(tmp_path):
    db_path = tmp_path / "app_data.db"
    out_path = tmp_path / "jobs.json"
    upcoming = [
        {
            "id": str(index),
            "home_team": f"Home {index}",
            "away_team": f"Away {index}",
            "handicap": "0.5",
        }
        for index in range(250)
    ]

    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE kv_store (key TEXT PRIMARY KEY, value TEXT)")
        conn.execute(
            "CREATE TABLE matches (bucket TEXT, match_id TEXT, payload_json TEXT)"
        )
        conn.execute(
            "INSERT INTO kv_store (key, value) VALUES (?, ?)",
            ("app_main_page_cache_v1", json.dumps({"upcoming_matches": upcoming})),
        )

    result = build_job_from_snapshot.build_jobs(
        db_path=db_path,
        cache_key="app_main_page_cache_v1",
        out_path=out_path,
    )

    jobs = json.loads(out_path.read_text(encoding="utf-8"))
    assert result == 0
    assert len(jobs) == 200
    assert jobs[0]["id"] == "0"
    assert jobs[-1]["id"] == "199"
