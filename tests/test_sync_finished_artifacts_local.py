import sqlite3

from scripts import sync_finished_artifacts_local as syncer


def _finished(score):
    return {
        'match_id': '3046050', 'home_name': 'Necaxa (W)', 'away_name': 'Cruz Azul (W)',
        'score': score, 'handicap': -2.75,
        'last_home_match': {'score': '1:0'},
        'last_away_match': {'score': '0:1'},
    }


def test_latest_artifact_wins_before_comparing_with_local_sql(monkeypatch):
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    conn.execute('CREATE TABLE matches (match_id TEXT, score TEXT, state TEXT)')
    conn.execute("INSERT INTO matches VALUES ('3046050', '0:2', 'historical')")
    monkeypatch.setattr(syncer.sql_store, 'ensure_bootstrap', lambda: None)
    monkeypatch.setattr(syncer.sql_store, '_connect', lambda: conn)

    # A final score already present in SQLite can still belong to an older,
    # incomplete analysis. A validated artifact must replace that row.
    corrected = syncer.pending_rows([_finished('0:1'), _finished('0-2')])
    assert len(corrected) == 1
    assert corrected[0][0]['score'] == '0-2'

    conn.execute("UPDATE matches SET score=NULL, state='precacheo' WHERE match_id='3046050'")
    pending = syncer.pending_rows([_finished('0:1'), _finished('0-2')])
    assert len(pending) == 1
    assert pending[0][0]['score'] == '0-2'
