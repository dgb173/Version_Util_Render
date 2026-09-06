"""One snapshot, disjoint workers, validated merge, bounded Render windows.

The archive is lossless and is never capped by the UI limits. Workers only write
artifacts; the publisher alone changes repository data. No Flask/SQL bootstrap is
needed for orchestration, merging, or rendering the current windows.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import copy
import datetime as dt
import json
import math
import os
from pathlib import Path
import re
import sys
import time
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
UTC = dt.timezone.utc
SPAIN = ZoneInfo('Europe/Madrid')
VERSION = 1


def read_json(path, default=None):
    path = Path(path)
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding='utf-8'))


def write_json(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(payload, ensure_ascii=False, separators=(',', ':')), encoding='utf-8')
    tmp.replace(path)


def mid(row):
    value = str(row.get('match_id') or row.get('id') or '').strip()
    if not re.fullmatch(r'\d+', value):
        raise ValueError('Invalid match ID')
    return value


def numeric(value):
    try:
        return math.isfinite(float(str(value).replace('−', '-')))
    except (ValueError, TypeError):
        return False


def scheduled(row):
    value = row.get('start_time')
    if value:
        try:
            date = dt.datetime.fromisoformat(str(value).replace('Z', '+00:00'))
            return date.replace(tzinfo=UTC) if date.tzinfo is None else date.astimezone(UTC)
        except ValueError:
            pass
    value = str(row.get('match_date') or row.get('date') or '')
    for fmt in ('%m/%d/%Y', '%Y-%m-%d', '%d/%m/%Y'):
        try:
            date = dt.datetime.strptime(value, fmt)
            clock = re.search(r'(\d{1,2}):(\d{2})', str(row.get('time') or '00:00'))
            hour, minute = map(int, clock.groups()) if clock else (0, 0)
            return date.replace(hour=hour, minute=minute, tzinfo=SPAIN).astimezone(UTC)
        except ValueError:
            pass
    return None


def has_score(row):
    return bool(re.fullmatch(r'\d+\s*[:-]\s*\d+', str(row.get('final_score') or row.get('score') or '')))


def quality_error(row, kind):
    if not isinstance(row, dict) or row.get('error') or row.get('precache_placeholder'):
        return 'empty_or_placeholder'
    try:
        mid(row)
    except ValueError:
        return 'invalid_id'
    for field in ('home_name', 'away_name'):
        if str(row.get(field) or '').strip().lower() in ('', 'n/a', 'local', 'visitante', 'unknown'):
            return 'missing_team_identity'
    if int(row.get('history_data_version') or 0) < 3:
        return 'old_history_schema'
    # Missing H2H/indirect matches can be legitimate. Require the sections to
    # have been attempted, never fabricate an H2H to satisfy completeness.
    for key in ('last_home_match', 'last_away_match', 'h2h_col3', 'h2h_stadium',
                'h2h_general', 'comparativas_indirectas', 'recent_home_matches', 'recent_away_matches'):
        if key not in row:
            return 'missing_section:' + key
    if kind == 'finished':
        if not has_score(row):
            return 'unverified_final_score'
        if not numeric((row.get('main_match_odds') or {}).get('ah_linea')):
            return 'missing_handicap'
        for side in ('home', 'away'):
            if not row.get(f'recent_{side}_matches_same_league_specific') or not row.get(f'last_{side}_match'):
                return 'missing_recent_home_away_form'
    elif row.get('summary_stats_status') != 'complete':
        return 'summary_not_downloaded'
    return None


def preserve_stats(old, new):
    """Refresh context while retaining previously downloaded/manual statistics."""
    if not isinstance(old, dict) or not isinstance(new, dict):
        return copy.deepcopy(new)
    result = copy.deepcopy(new)
    # Only reuse statistics when the historical fixture ID has not changed.
    identity = ('match_id', 'match1_id', 'match6_id')
    same = all(not old.get(k) or not new.get(k) or str(old[k]) == str(new[k]) for k in identity)
    for key, value in old.items():
        if key == 'stats_rows' and same and value and not result.get(key):
            result[key] = copy.deepcopy(value)
        elif isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = preserve_stats(value, result[key])
    return result


def archive_path(root, kind, match_id):
    return Path(root) / 'data/cache_archive' / kind / (mid({'id': match_id}) + '.json')


def reusable(old, source, kind):
    if not old or quality_error(old, kind):
        return False
    if old.get('cloud_cache_version') != VERSION:
        return False
    for field in ('handicap', 'goal_line', 'start_time'):
        if source.get(field) is not None and str(source[field]) != str(old.get(field)):
            return False
    return True


def prepare(args):
    from modules.nowgoal_fetcher import fetch_main_page_matches_direct
    source_kind = 'upcoming' if args.kind == 'list' else args.kind
    rows = fetch_main_page_matches_direct(status_filter=source_kind, limit=None,
        require_handicap=source_kind == 'finished',
        handicap_filter=None if args.handicap == 'all' else args.handicap,
        goal_line_filter=None if args.ou == 'all' else args.ou)
    if not rows:
        raise RuntimeError('The source returned no matches. Existing data will not be replaced.')
    now = dt.datetime.now(UTC)
    unique = {}
    for row in rows:
        date = scheduled(row)
        if not date:
            continue
        if source_kind == 'upcoming' and (row.get('status') != 0 or date <= now):
            continue
        if source_kind == 'finished' and (not has_score(row) or date > now):
            continue
        row['source_verified'] = True
        unique[mid(row)] = row
    if not unique:
        raise RuntimeError('No valid dated matches in source')
    jobs = []
    if args.kind != 'list':
        for match_id, source in unique.items():
            old = read_json(archive_path(args.root, args.kind, match_id))
            if args.force_full or not reusable(old, source, args.kind):
                jobs.append(source)
        # Retry failed fixtures even when they have disappeared from today's feed.
        for source in read_json(Path(args.root) / f'data/cache_control/{args.kind}_retry.json', []):
            if mid(source) not in unique:
                jobs.append(source)
        if args.kind == 'finished' and args.handicap == 'all' and args.ou == 'all':
            # Repair the old blank rows too, without trusting their old score as
            # a verified source. Never skip solely because an ID was processed.
            queued = {mid(r) for r in jobs} | set(unique)
            import ijson
            paths = list((Path(args.root) / 'data').glob('data_ah_*.json')) + list((Path(args.root) / 'data').glob('data_minus_ah_*.json'))
            for path in paths:
                with path.open('rb') as handle:
                    for old in ijson.items(handle, 'item', use_float=True):
                        date = scheduled(old)
                        match_id = mid(old)
                        # Complete legacy histories do not need a mass reanalysis.
                        if match_id in queued or not date or date > now or (old.get('last_home_match') and old.get('last_away_match')):
                            continue
                        repaired = read_json(archive_path(args.root, 'finished', match_id))
                        if repaired and not quality_error(repaired, 'finished'):
                            continue
                        jobs.append({'id': match_id})
                        queued.add(match_id)
    if args.max_jobs:
        jobs = jobs[:args.max_jobs]  # Explicit diagnostic runs only; default is unlimited.
    output = Path(args.output)
    write_json(output / 'snapshot.json', {'kind': args.kind, 'at': now.isoformat(), 'matches': list(unique.values())})
    for index in range(args.shards):
        write_json(output / f'jobs_{index}.json', jobs[index::args.shards])
    write_json(output / 'manifest.json', {'kind': args.kind, 'shards': args.shards,
        'jobs': len(jobs), 'ids': [mid(row) for row in jobs], 'available': len(unique)})
    print(f'Prepared {len(jobs)} jobs / {len(unique)} available across {args.shards} bots', flush=True)


def analyze(source, kind, attempts=2):
    from modules.estudio_scraper import analizar_partido_completo
    error = 'analysis_failed'
    for attempt in range(attempts):
        try:
            row = analizar_partido_completo(mid(source), force_refresh=True, include_summary_stats=kind == 'upcoming')
            if not row or row.get('error'):
                raise ValueError((row or {}).get('error') or 'Empty analysis')
            row['match_id'] = mid(source)
            if source.get('source_verified'):
                for key in ('start_time', 'match_date', 'time', 'handicap', 'goal_line'):
                    if source.get(key) is not None:
                        row[key] = source[key]
                if kind == 'finished':
                    row['score'] = row['final_score'] = source['final_score']
                else:
                    # Never turn a not-yet-played fixture into a fictitious 0:0.
                    row['score'] = row['final_score'] = None
                for key, source_key in (('ah_linea', 'handicap'), ('goals_linea', 'goal_line')):
                    if numeric(source.get(source_key)):
                        row.setdefault('main_match_odds', {})[key] = source[source_key]
            error = quality_error(row, kind)
            if not error:
                row['cloud_cache_version'] = VERSION
                row['cached_at'] = dt.datetime.now(UTC).isoformat()
                row['state'] = 'historical' if kind == 'finished' else 'precacheo'
                row['cache_profile'] = 'filters' if kind == 'finished' else 'full'
                return row, None
        except Exception as exc:
            error = str(exc)
        if attempt + 1 < attempts:
            time.sleep(1 + attempt)
    return None, error


def worker(args):
    jobs = read_json(args.input)
    if not isinstance(jobs, list):
        raise ValueError('Worker input must be a list')
    result = {'kind': args.kind, 'shard': args.shard, 'rows': [], 'failures': [], 'attempted': len(jobs)}
    write_json(args.output, result)
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(analyze, source, args.kind): source for source in jobs}
        for future in concurrent.futures.as_completed(futures):
            row, error = future.result()
            if row:
                result['rows'].append(row)
            else:
                result['failures'].append({'source': futures[future], 'error': error})
            write_json(args.output, result)
            print(f"Bot {args.shard}: {len(result['rows'])} saved, {len(result['failures'])} failed / {len(jobs)}", flush=True)
    # Artifacts retain successes even on a partial failure; publisher reports it.
    return 1 if result['failures'] else 0


def migrate_archive(root):
    for name in ('data_precacheo.json', 'data_pending_results.json'):
        for row in read_json(Path(root) / 'data' / name, []):
            if not isinstance(row, dict):
                continue
            path = archive_path(root, 'upcoming', mid(row))
            if not path.exists():
                write_json(path, row)


def build_windows(root=ROOT, now=None):
    now = now or dt.datetime.now(UTC)
    migrate_archive(root)
    snapshot = read_json(Path(root) / 'data/cache_control/upcoming_snapshot.json', {})
    sources = {mid(row): row for row in snapshot.get('matches', [])}
    future, pending = [], []
    seen = set()
    for path in (Path(root) / 'data/cache_archive/upcoming').glob('*.json'):
        row = read_json(path)
        match_id = mid(row)
        source = sources.get(match_id)
        if source and not has_score(row):
            for key in ('start_time', 'match_date', 'time', 'handicap', 'goal_line'):
                if source.get(key) is not None:
                    row[key] = source[key]
        date = scheduled(row)
        if not date or has_score(row):
            continue
        seen.add(match_id)
        if date > now:
            future.append((date, match_id, row))
        elif now - date <= dt.timedelta(days=7):
            pending.append((date, match_id, row))
    for match_id, source in sources.items():
        date = scheduled(source)
        if match_id not in seen and date and date > now:
            row = dict(source, precache_placeholder=True, summary_stats_status='pending')
            future.append((date, match_id, row))
    future.sort(key=lambda x: (x[0], x[1]))
    pending.sort(key=lambda x: (x[0], x[1]), reverse=True)
    next_rows = [x[2] for x in future[:400]]
    pending_rows = [dict(x[2], state='pending_results') for x in pending[:200]]
    write_json(Path(root) / 'data/data_precacheo.json', next_rows)
    write_json(Path(root) / 'data/data_pending_results.json', pending_rows)
    old_snapshot = read_json(Path(root) / 'data.json', {})
    old_snapshot['upcoming_matches'] = sorted(
        [r for r in sources.values() if scheduled(r) and scheduled(r) > now],
        key=lambda x: (scheduled(x), mid(x)))[:400]
    old_snapshot.setdefault('finished_matches', [])
    if not sources:
        old_snapshot['upcoming_matches'] = next_rows
    old_snapshot['timestamp'] = snapshot.get('at') or now.isoformat()
    for name in ('data.json', 'data/data.json'):
        write_json(Path(root) / name, old_snapshot)
    return {'upcoming': len(next_rows), 'pending': len(pending_rows), 'archived_available': len(future) + len(pending)}


def merge(args):
    root = Path(args.root)
    prepared = Path(args.prepared)
    manifest = read_json(prepared / 'manifest.json')
    if manifest['kind'] != args.kind:
        raise ValueError('Manifest kind mismatch')
    all_rows, failures, seen = [], [], set()
    if args.kind != 'list':
        for index in range(manifest['shards']):
            matches = list(Path(args.results).rglob(f'result_{index}.json'))
            if len(matches) != 1:
                raise ValueError(f'Missing or duplicate artifact for bot {index}')
            payload = read_json(matches[0])
            expected = {mid(row) for row in read_json(prepared / f'jobs_{index}.json')}
            actual = [mid(row) for row in payload['rows']] + [mid(f['source']) for f in payload['failures']]
            if payload['kind'] != args.kind or payload['shard'] != index or set(actual) != expected or len(actual) != len(expected):
                raise ValueError(f'Incomplete or mismatched artifact for bot {index}')
            for row in payload['rows']:
                error = quality_error(row, args.kind)
                if error or mid(row) in seen:
                    raise ValueError(f'Invalid/duplicate result {mid(row)}: {error}')
                seen.add(mid(row))
                all_rows.append(row)
            failures.extend(payload['failures'])
    # Validate every shard before changing any persistent data.
    migrate_archive(root)
    changed_buckets = {}
    for row in all_rows:
        path = archive_path(root, args.kind, mid(row))
        old = read_json(path, {})
        row = preserve_stats(old, row)
        write_json(path, row)
        if args.kind == 'finished':
            from modules.data_manager import get_bucket_name
            bucket = get_bucket_name((row.get('main_match_odds') or {}).get('ah_linea'))
            if bucket not in changed_buckets:
                changed_buckets[bucket] = {mid(r): r for r in read_json(root / 'data' / bucket, [])}
            previous = changed_buckets[bucket].get(mid(row), {})
            changed_buckets[bucket][mid(row)] = preserve_stats(previous, row)
            cached_path = archive_path(root, 'upcoming', mid(row))
            if cached_path.exists():
                cached = read_json(cached_path)
                cached['final_score'] = cached['score'] = row['final_score']
                cached['state'] = 'historical'
                write_json(cached_path, cached)
    for bucket, rows in changed_buckets.items():
        write_json(root / 'data' / bucket, list(rows.values()))
    snapshot = read_json(prepared / 'snapshot.json')
    if args.kind in ('list', 'upcoming'):
        write_json(root / 'data/cache_control/upcoming_snapshot.json', snapshot)
    write_json(root / f'data/cache_control/{args.kind}_retry.json', [f['source'] for f in failures])
    counts = build_windows(root)
    status = dict(counts, kind=args.kind, saved=len(all_rows), failed=len(failures),
        available=manifest['available'], attempted=manifest['jobs'],
        status='partial' if failures else 'complete', completed_at=dt.datetime.now(UTC).isoformat(),
        run_id=os.getenv('GITHUB_RUN_ID'))
    write_json(root / f'data/cache_control/{args.kind}_status.json', status)
    print(json.dumps(status), flush=True)
    summary = os.getenv('GITHUB_STEP_SUMMARY')
    if summary:
        with open(summary, 'a', encoding='utf-8') as handle:
            handle.write(f"\n### Cache {args.kind}\n\n{len(all_rows)} guardados; {len(failures)} fallidos pendientes de reintento.\n\nRender: {counts['upcoming']} próximos / {counts['pending']} pendientes.\n")
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['prepare', 'worker', 'merge', 'window'])
    parser.add_argument('--kind', choices=['upcoming', 'finished', 'list'], default='upcoming')
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--input', type=Path)
    parser.add_argument('--output', type=Path, default=Path('prepared'))
    parser.add_argument('--prepared', type=Path, default=Path('prepared'))
    parser.add_argument('--results', type=Path, default=Path('results'))
    parser.add_argument('--shards', type=int, default=8)
    parser.add_argument('--shard', type=int, default=0)
    parser.add_argument('--workers', type=int, default=3)
    parser.add_argument('--force-full', action='store_true')
    parser.add_argument('--max-jobs', type=int, default=0)
    parser.add_argument('--handicap', default='all')
    parser.add_argument('--ou', default='all')
    args = parser.parse_args()
    if not 1 <= args.shards <= 10 or not 1 <= args.workers <= 6 or args.max_jobs < 0:
        parser.error('Invalid worker/shard/job limit')
    if args.command == 'window':
        print(build_windows(args.root))
        return 0
    return {'prepare': prepare, 'worker': worker, 'merge': merge}[args.command](args) or 0


if __name__ == '__main__':
    raise SystemExit(main())
