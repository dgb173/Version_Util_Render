import json
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from modules.match_clock import source_wall_to_spain, record_instant, schedule_fields, sync_stored_schedule
from modules.estudio_scraper import _parse_to_spain_datetime, extract_match_datetime_spain
from bs4 import BeautifulSoup


@pytest.mark.parametrize('day,clock,expected', [
    ('2026-07-10', '20:00:00', '2026-07-10T22:00:00+02:00'),
    ('2026-10-24', '20:00:00', '2026-10-24T22:00:00+02:00'),
    ('2026-10-25', '20:00:00', '2026-10-25T21:00:00+01:00'),
    ('2026-10-25', '00:30:00', '2026-10-25T02:30:00+02:00'),
    ('2026-10-25', '01:30:00', '2026-10-25T02:30:00+01:00'),
    ('2026-03-29', '00:30:00', '2026-03-29T01:30:00+01:00'),
    ('2026-03-29', '01:30:00', '2026-03-29T03:30:00+02:00'),
])
def test_real_dst_boundaries(day, clock, expected):
    assert _parse_to_spain_datetime(day, clock).isoformat() == expected


def test_china_midnight_and_invalid_values_never_invent_a_date():
    assert source_wall_to_spain('2026-10-26', '02:00', False).isoformat() == '2026-10-25T19:00:00+01:00'
    assert source_wall_to_spain('bad', '12:00') is None
    assert source_wall_to_spain('2026-10-25', '99:00') is None
    assert extract_match_datetime_spain(BeautifulSoup('<div class="row" id="match_time">12:00</div>', 'html.parser')) is None


def test_utc_feed_and_legacy_local_start_have_different_contracts():
    assert record_instant({'time_obj': '2026-10-25T20:00:00'}).hour == 20
    assert record_instant({'start_time': '2026-10-25T20:00:00'}).hour == 19
    values = schedule_fields({'start_time': '2026-10-25T22:30:00Z'})
    assert values['match_date'] == '10/25/2026'
    assert values['time'] == '23:30'
    assert values['start_time'].endswith('+01:00')


def test_sync_updates_all_views_without_touching_statistics(tmp_path):
    with sqlite3.connect(tmp_path / 'test.db') as c:
        c.execute('CREATE TABLE matches(match_id TEXT,bucket TEXT,payload_json TEXT,explorer_json TEXT,explorer_search_json TEXT,match_date TEXT)')
        old = {'match_id': '1', 'time': '00:30', 'match_date': '10/26/2026', 'stats_rows': [{'label': 'Tiros', 'home': '0', 'away': '8'}]}
        raw = json.dumps(old)
        c.execute('INSERT INTO matches VALUES (?,?,?,?,?,?)', ('1','data_precacheo.json',raw,raw,raw,'2026-10-26'))
        source = {'start_time': '2026-10-25T22:30:00+00:00'}
        assert sync_stored_schedule(c,'1',source)
        rows = c.execute('SELECT payload_json,explorer_json,explorer_search_json,match_date FROM matches').fetchone()
        for raw in rows[:3]:
            obj = json.loads(raw)
            assert obj['time'] == '23:30'
            assert obj['stats_rows'] == old['stats_rows']
        assert rows[3] == '2026-10-25'
        assert not sync_stored_schedule(c, '1', source)


def test_main_feed_keeps_explicit_offsets_and_rejects_missing_dates():
    from modules.nowgoal_fetcher import _parse_nowgoal_date_to_spain
    assert _parse_nowgoal_date_to_spain('2026-10-25T20:00:00Z').isoformat() == '2026-10-25T21:00:00+01:00'
    assert _parse_nowgoal_date_to_spain('2026-10-25T20:00:00+02:00').isoformat() == '2026-10-25T19:00:00+01:00'
    assert _parse_nowgoal_date_to_spain('bad') is None
