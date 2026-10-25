import json
import sqlite3
from scripts.build_job_from_snapshot import build_jobs


def test_reuse_updates_the_clock_and_indexes_without_losing_statistics(tmp_path):
    db, out = tmp_path / 'db.sqlite', tmp_path / 'jobs.json'
    old = {'match_id': '1', 'home_name': 'H', 'away_name': 'A', 'history_data_version': 3,
           'last_home_match': {'match_id': '2'}, 'time': '00:30', 'match_date': '10/26/2026',
           'main_match_odds': {'ah_linea': '.5'}, 'stats_rows': [{'home': '0', 'away': '5'}]}
    source = {'id': '1', 'home_team': 'H', 'away_team': 'A', 'start_time': '2026-10-25T22:30:00Z', 'handicap': '.5'}
    c = sqlite3.connect(db)
    c.execute('CREATE TABLE kv_store(key TEXT,value TEXT)')
    c.execute('CREATE TABLE matches(bucket TEXT,match_id TEXT,payload_json TEXT,explorer_json TEXT,match_date TEXT)')
    c.execute('INSERT INTO kv_store VALUES (?,?)', ('app_main_page_cache_v1', json.dumps({'upcoming_matches': [source]})))
    c.execute('INSERT INTO matches VALUES (?,?,?,?,?)', ('data_precacheo.json','1',json.dumps(old),json.dumps(old),'2026-10-26'))
    c.commit(); c.close()
    assert build_jobs(db, 'app_main_page_cache_v1', out) == 4  # Reused: no scraping required.
    c = sqlite3.connect(db)
    stored, explorer, index = c.execute('SELECT payload_json,explorer_json,match_date FROM matches').fetchone()
    c.close()
    assert json.loads(stored)['time'] == '23:30'
    assert json.loads(stored)['stats_rows'] == old['stats_rows']
    assert json.loads(explorer)['match_date'] == '2026-10-25'
    assert index == '2026-10-25'
