import json

from modules import sql_store


def test_cloud_league_bucket_is_managed():
    assert "data_cloud_league.json" in sql_store.MANAGED_BUCKETS
    assert (
        sql_store.MATCH_STATE_BY_BUCKET.get(
            "data_cloud_league.json",
            sql_store.DEFAULT_MATCH_STATE,
        )
        == "historical"
    )


def test_cloud_league_is_imported_after_live_snapshots(tmp_path, monkeypatch):
    match = {"match_id": "123", "home_name": "Cloud Home"}
    (tmp_path / "data_cloud_league.json").write_text(
        json.dumps([match]),
        encoding="utf-8",
    )
    (tmp_path / "data_precacheo.json").write_text(
        json.dumps([{"match_id": "123", "home_name": "Live Home"}]),
        encoding="utf-8",
    )
    (tmp_path / "data_pending_results.json").write_text("[]", encoding="utf-8")

    db_path = tmp_path / "test.db"
    monkeypatch.setattr(sql_store, "DB_PATH", db_path)
    with sql_store._connect() as connection:
        sql_store._init_schema(connection)
        imported = sql_store._import_legacy_matches(
            connection,
            tmp_path,
            buckets=[
                "data_cloud_league.json",
                "data_precacheo.json",
                "data_pending_results.json",
            ],
        )
        row = connection.execute(
            "SELECT bucket, state, payload_json FROM matches WHERE match_id = ?",
            ("123",),
        ).fetchone()

    assert imported == 2
    assert row["bucket"] == "data_cloud_league.json"
    assert row["state"] == "historical"
    assert json.loads(row["payload_json"])["home_name"] == "Cloud Home"
