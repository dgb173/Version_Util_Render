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
