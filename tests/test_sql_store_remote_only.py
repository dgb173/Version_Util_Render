from modules import sql_store


class _FakeConnection:
    def __init__(self):
        self.row_factory = None
        self.statements = []
        self.sync_calls = 0

    def execute(self, statement):
        self.statements.append(statement)
        return self

    def sync(self):
        self.sync_calls += 1


class _FakeLibsql:
    def __init__(self):
        self.calls = []
        self.connection = _FakeConnection()

    def connect(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.connection


def test_remote_only_connects_without_creating_or_syncing_local_replica(monkeypatch):
    fake_libsql = _FakeLibsql()
    monkeypatch.setattr(sql_store, "_libsql", fake_libsql)
    monkeypatch.setattr(sql_store, "LIBSQL_URL", "libsql://explorer.example.turso.io")
    monkeypatch.setattr(sql_store, "LIBSQL_AUTH_TOKEN", "secret")
    monkeypatch.setattr(sql_store, "LIBSQL_REMOTE_ONLY", True)

    connection = sql_store._connect()

    assert connection is fake_libsql.connection
    assert fake_libsql.calls == [
        ((), {"database": "libsql://explorer.example.turso.io", "auth_token": "secret"})
    ]
    assert connection.sync_calls == 0


def test_row_value_supports_sqlite_rows_and_remote_tuple_rows():
    assert sql_store._row_value(("bucket.json",), "bucket", 0) == "bucket.json"
    assert sql_store._row_value({"bucket": "mapped.json"}, "bucket", 0) == "mapped.json"


def test_batch_upsert_writes_and_reads_explorer_payload(monkeypatch, tmp_path):
    monkeypatch.setattr(sql_store, "DB_PATH", tmp_path / "test.db")
    monkeypatch.setattr(sql_store, "BOOTSTRAP_LOCK_FILE", tmp_path / "bootstrap.lock")
    monkeypatch.setattr(sql_store, "LIBSQL_URL", "")
    monkeypatch.setattr(sql_store, "LIBSQL_REMOTE_ONLY", False)
    monkeypatch.setattr(sql_store, "SQL_BOOTSTRAP_SKIP_LEGACY", True)
    monkeypatch.setattr(sql_store, "_BOOTSTRAPPED", False)

    result = sql_store.upsert_matches([
        ({"match_id": "101", "home_name": "Home", "away_name": "Away", "handicap": 0},
         "data_ah_0.json", "historical"),
        ({"match_id": "102", "home_name": "Other", "away_name": "Team", "handicap": 0.5},
         "data_ah_0.5.json", "historical"),
    ])

    assert result == [(None, "101"), (None, "102")]
    rows = sql_store.fetch_matches(state="historical", prefer_explorer_payload=True)
    assert {row["match_id"] for row in rows} == {"101", "102"}


def test_remote_fetch_uses_http_pipeline_without_native_connection(monkeypatch):
    class _Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "results": [{
                    "type": "ok",
                    "response": {
                        "result": {
                            "rows": [[{
                                "type": "text",
                                "value": '{"match_id":"301","home_name":"Home"}',
                            }]],
                        }
                    },
                }]
            }

    calls = []

    def fake_post(url, **kwargs):
        calls.append((url, kwargs))
        return _Response()

    monkeypatch.setattr(sql_store, "LIBSQL_URL", "libsql://example.turso.io")
    monkeypatch.setattr(sql_store, "LIBSQL_AUTH_TOKEN", "secret")
    monkeypatch.setattr(sql_store, "LIBSQL_REMOTE_ONLY", True)
    monkeypatch.setattr(sql_store.requests, "post", fake_post)

    rows = sql_store.fetch_matches(state="historical", limit=25, prefer_explorer_payload=True)

    assert rows == [{"match_id": "301", "home_name": "Home"}]
    assert calls[0][0] == "https://example.turso.io/v2/pipeline"
    request = calls[0][1]["json"]["requests"][0]
    assert request["stmt"]["args"] == [
        {"type": "text", "value": "historical"},
        {"type": "integer", "value": "25"},
    ]


def test_remote_fetch_supports_bounded_pagination(monkeypatch):
    class _Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "results": [{
                    "type": "ok",
                    "response": {"result": {"rows": []}},
                }]
            }

    calls = []

    def fake_post(url, **kwargs):
        calls.append((url, kwargs))
        return _Response()

    monkeypatch.setattr(sql_store, "LIBSQL_URL", "libsql://example.turso.io")
    monkeypatch.setattr(sql_store, "LIBSQL_AUTH_TOKEN", "secret")
    monkeypatch.setattr(sql_store, "LIBSQL_REMOTE_ONLY", True)
    monkeypatch.setattr(sql_store.requests, "post", fake_post)

    assert sql_store.fetch_matches(
        state="historical",
        limit=150,
        offset=300,
        prefer_explorer_payload=True,
    ) == []

    request = calls[0][1]["json"]["requests"][0]
    assert request["stmt"]["sql"].endswith("LIMIT ? OFFSET ?")
    assert request["stmt"]["args"] == [
        {"type": "text", "value": "historical"},
        {"type": "integer", "value": "150"},
        {"type": "integer", "value": "300"},
    ]
