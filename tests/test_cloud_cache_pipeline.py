import datetime as dt
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import cloud_cache_pipeline as cache


def row(i=1, **kw):
    return dict(match_id=str(i), home_name='Home', away_name='Away', history_data_version=3,
        last_home_match={'match_id': '81'}, last_away_match={'match_id': '82'},
        recent_home_matches=[{}], recent_away_matches=[{}], h2h_col3=None,
        h2h_stadium={}, h2h_general={}, comparativas_indirectas={},
        recent_home_matches_same_league_specific=[{}], recent_away_matches_same_league_specific=[{}],
        summary_stats_status='complete', main_match_odds={'ah_linea': '0'}, **kw)


def test_finished_profile_does_not_require_statistics_but_rejects_blank_context():
    valid = row(final_score='0:0')
    valid['summary_stats_status'] = 'deferred'
    assert cache.quality_error(valid, 'finished') is None
    assert cache.quality_error(valid, 'upcoming') == 'summary_not_downloaded'
    valid['recent_away_matches_same_league_specific'] = []
    assert cache.quality_error(valid, 'finished') == 'missing_recent_home_away_form'
    assert cache.quality_error(dict(valid, precache_placeholder=True), 'upcoming')


def test_reuse_never_accepts_legacy_placeholder_or_deferred_ficha():
    valid = row(cloud_cache_version=cache.VERSION)
    assert cache.reusable(valid, {'id': '1'}, 'upcoming')
    assert not cache.reusable(dict(valid, summary_stats_status='deferred'), {}, 'upcoming')
    assert not cache.reusable(dict(valid, precache_placeholder=True), {}, 'upcoming')


def test_windows_cap_render_without_truncating_archive(tmp_path):
    now = dt.datetime(2026, 9, 6, 10, tzinfo=dt.timezone.utc)
    for i in range(650):
        date = now + dt.timedelta(minutes=i - 249)
        cache.write_json(cache.archive_path(tmp_path, 'upcoming', str(i + 1)), row(i+1, start_time=date.isoformat()))
    counts = cache.build_windows(tmp_path, now)
    assert counts['upcoming'] == 400
    assert counts['pending'] == 200
    assert len(list((tmp_path / 'data/cache_archive/upcoming').glob('*.json'))) == 650
    future = cache.read_json(tmp_path / 'data/data_precacheo.json')
    assert [r['match_id'] for r in future][:2] == ['251', '252']
    assert cache.read_json(tmp_path / 'data/data_pending_results.json')[0]['match_id'] == '250'


def test_does_not_reuse_stats_for_another_historical_fixture():
    old = {'last_home_match': {'match_id': '11', 'stats_rows': [{'home': 5}]}}
    same = {'last_home_match': {'match_id': '11', 'stats_rows': []}}
    other = {'last_home_match': {'match_id': '12', 'stats_rows': []}}
    assert cache.preserve_stats(old, same)['last_home_match']['stats_rows']
    assert cache.preserve_stats(old, other)['last_home_match']['stats_rows'] == []


def test_merge_missing_shard_fails_before_mutating_data(tmp_path):
    prepared = tmp_path / 'prepared'
    cache.write_json(prepared / 'manifest.json', {'kind': 'upcoming', 'shards': 2})
    with pytest.raises(ValueError, match='Missing'):
        cache.merge(SimpleNamespace(root=tmp_path, prepared=prepared, results=tmp_path / 'results', kind='upcoming'))
    assert not (tmp_path / 'data').exists()


def test_archive_migration_skips_legacy_rows_without_numeric_id(tmp_path):
    cache.write_json(tmp_path / 'data/data_precacheo.json', [
        {'home_name': 'placeholder'},
        {'match_id': 'abc', 'home_name': 'invalid'},
        row(10),
    ])
    cache.migrate_archive(tmp_path)
    paths = list((tmp_path / 'data/cache_archive/upcoming').glob('*.json'))
    assert [path.name for path in paths] == ['10.json']


def test_prepare_unlimited_and_disjoint(monkeypatch, tmp_path):
    from modules import nowgoal_fetcher
    future = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1)).isoformat()
    rows = [dict(id=str(i), start_time=future, status=0) for i in range(1001)]
    monkeypatch.setattr(nowgoal_fetcher, 'fetch_main_page_matches_direct', lambda **kw: rows)
    cache.prepare(SimpleNamespace(root=tmp_path, output=tmp_path / 'prepared', kind='upcoming',
        shards=8, max_jobs=0, force_full=False, handicap='all', ou='all'))
    jobs = [r for i in range(8) for r in cache.read_json(tmp_path / f'prepared/jobs_{i}.json')]
    assert len(jobs) == len({cache.mid(r) for r in jobs}) == 1001


def test_analyze_passes_correct_profile_and_removes_future_zero_score(monkeypatch):
    from modules import estudio_scraper
    calls = []
    def fake(mid, **kw):
        calls.append(kw)
        return row(final_score='0:0')
    monkeypatch.setattr(estudio_scraper, 'analizar_partido_completo', fake)
    result, error = cache.analyze({'id':'1', 'source_verified':True}, 'upcoming', attempts=1)
    assert not error and result['final_score'] is None
    assert calls[-1]['include_summary_stats'] is True
    result, error = cache.analyze({'id':'1', 'source_verified':True, 'final_score':'2:1'}, 'finished', attempts=1)
    assert not error and result['final_score'] == '2:1'
    assert calls[-1]['include_summary_stats'] is False


def test_source_excludes_cancelled_and_live():
    from modules.nowgoal_fetcher import parse_matches_from_bf_content
    content = "B[0]=[30,'League'];" + ''.join(
        f"A[{i}]=[{100+i},30,1,2,'Home','Away','2026-09-01 10:00:00','',{status},0,0];"
        for i,status in enumerate([-1, -10, -11, -14, 1]))
    odds = {str(100+i):{'handicap':'0.5','goal_line':'2.5'} for i in range(5)}
    assert [r['id'] for r in parse_matches_from_bf_content(content, status_filter='finished', odds_by_match=odds)] == ['100']
