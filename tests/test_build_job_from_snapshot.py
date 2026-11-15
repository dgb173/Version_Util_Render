import json
import sqlite3

from scripts import build_job_from_snapshot


def test_build_jobs_includes_all_snapshot_matches_without_reading_uninitialized_queue(tmp_path):
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
    assert len(jobs) == 250
    assert jobs[0]["id"] == "0"
    assert jobs[-1]["id"] == "249"


def test_deferred_or_missing_sections_are_not_reused():
    sections = {
        key: {} for key in (
            "last_home_match", "last_away_match", "h2h_col3", "h2h_stadium",
            "h2h_general", "comparativas_indirectas", "recent_home_matches",
            "recent_away_matches",
        )
    }
    complete = {"history_data_version": 3, "summary_stats_status": "complete", **sections}
    assert build_job_from_snapshot._looks_like_complete_precache(complete)
    assert not build_job_from_snapshot._looks_like_complete_precache(
        {**complete, "summary_stats_status": "deferred"}
    )
    partial = dict(complete)
    partial.pop("recent_away_matches")
    assert not build_job_from_snapshot._looks_like_complete_precache(partial)
