import json
import sqlite3

from modules import data_manager, sql_store
from modules.pattern_search import explore_matches
from scripts import sync_finished_artifacts_local as finished_sync


def test_compact_explorer_rows_keep_search_results():
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    conn.execute(
        'CREATE TABLE matches (state TEXT, bucket TEXT, match_date TEXT, '
        'updated_at TEXT, match_id TEXT, payload_json TEXT, explorer_json TEXT)'
    )
    row = {
        'match_id': 'one', 'home_name': 'Home', 'away_name': 'Away',
        'match_date': '2026-01-01 12:00', 'score': '2:1', 'handicap': 0.5,
        'main_match_odds': {'ah_linea': 0.5, 'goals_linea': 2.5},
        'last_home_match': {'score': '1:0', 'away_team': 'Old home'},
        'last_away_match': {'score': '0:1', 'home_team': 'Old away'},
        'pre_match_context': {'unused': 'x' * 1000},
        'recent_home_matches_all': [{'unused': 'x' * 1000}],
    }
    payload = json.dumps(row)
    conn.execute(
        'INSERT INTO matches VALUES (?, ?, ?, ?, ?, ?, ?)',
        ('historical', 'data_ah_0.5.json', row['match_date'], '2026-01-02', 'one', payload, payload),
    )

    full = json.loads(sql_store._fetch_matches_rows(conn, state='historical', prefer_explorer_payload=True)[0]['payload_json'])
    compact = json.loads(sql_store._fetch_matches_rows(
        conn, state='historical', prefer_explorer_payload=True, explorer_search_only=True
    )[0]['payload_json'])

    assert 'pre_match_context' not in compact
    assert 'recent_home_matches_all' not in compact
    assert explore_matches([full], {'limit': 10, 'include_stats': False}) == explore_matches(
        [compact], {'limit': 10, 'include_stats': False}
    )


def test_explorer_orders_dates_after_local_legacy_repair(monkeypatch):
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    conn.execute(
        'CREATE TABLE matches (state TEXT, bucket TEXT, match_date TEXT, '
        'updated_at TEXT, match_id TEXT, payload_json TEXT, explorer_json TEXT)'
    )
    for match_id, date in (
        ('old_slash', '9/1/2026'),
        ('recent_iso', '2026-10-04'),
        ('older_iso', '2026-09-27'),
        ('older_slash', '8/9/2026'),
    ):
        payload = json.dumps({'match_id': match_id})
        conn.execute(
            'INSERT INTO matches VALUES (?, ?, ?, ?, ?, ?, ?)',
            ('historical', 'data_ah_0.json', date, '2026-10-04', match_id, payload, payload),
        )

    monkeypatch.setattr(finished_sync.sql_store, 'ensure_bootstrap', lambda: None)
    monkeypatch.setattr(finished_sync.sql_store, '_connect', lambda: conn)
    assert finished_sync.normalize_local_historical_dates() == 2

    rows = sql_store._fetch_matches_rows(
        conn, state='historical', prefer_explorer_payload=True, explorer_search_only=True, limit=4
    )
    assert [json.loads(row['payload_json'])['match_id'] for row in rows] == [
        'recent_iso', 'older_iso', 'old_slash', 'older_slash'
    ]


def test_explorer_cache_reuses_rows_until_database_version_changes(monkeypatch):
    calls = []
    version = [(1, '2026-01-01')]
    rows = [{'score': '1:0', 'last_home_match': {'score': '1:0'},
             'last_away_match': {'score': '0:1'}}]

    monkeypatch.setattr(data_manager.sql_store, 'get_explorer_data_version', lambda: version[0])
    monkeypatch.setattr(data_manager.sql_store, 'fetch_matches',
                        lambda **kwargs: calls.append(kwargs) or rows)
    monkeypatch.setattr(data_manager, '_explorer_cache_ttl', 600)
    data_manager._clear_explorer_cache()
    try:
        assert data_manager.load_explorer_matches(compact=True) == rows
        assert data_manager.load_explorer_matches(compact=True) == rows
        assert len(calls) == 1
        assert calls[0]['explorer_search_only'] is True

        version[0] = (2, '2026-01-02')
        assert data_manager.load_explorer_matches(compact=True) == rows
        assert len(calls) == 2
    finally:
        data_manager._clear_explorer_cache()
