"""Audit or repair precache clocks against the canonical main snapshot.

No blanket +/-1 hour adjustment: only join matching fixture IDs and teams.
Default is read-only; --apply backs up changed rows before editing them.
"""
import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from modules.match_clock import schedule_fields, sync_stored_schedule


def repair(db, apply=False, json_paths=(), output=None):
    output = output or ROOT / 'artifacts/timezone_audit' / datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')
    uri = db.resolve().as_uri() + ('?mode=rw' if apply else '?mode=ro')
    with sqlite3.connect(uri, uri=True, timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        raw = conn.execute("SELECT value FROM kv_store WHERE key='app_main_page_cache_v1'").fetchone()
        if not raw:
            raise ValueError('No hay snapshot principal con el que verificar las fechas')
        snapshot = json.loads(raw[0])
        if isinstance(snapshot, str):
            snapshot = json.loads(snapshot)
        sources = {str(r.get('id') or r.get('match_id')): r for group in ('finished_matches', 'upcoming_matches')
                   for r in snapshot.get(group, []) if isinstance(r, dict) and schedule_fields(r)}
        changes, backup, clock_differences, day_changes, conflicts = [], [], 0, 0, []
        for row in conn.execute("SELECT * FROM matches WHERE state='precacheo'"):
            source = sources.get(row['match_id'])
            if not source:
                continue
            p = json.loads(row['payload_json'])
            same = all(str(p.get(s + '_name') or p.get(s + '_team') or '').strip().casefold() ==
                       str(source.get(s + '_name') or source.get(s + '_team') or '').strip().casefold()
                       for s in ('home', 'away'))
            if not same:
                conflicts.append(row['match_id'])
                continue
            expected = schedule_fields(source)
            if not any(p.get(k) != v for k, v in expected.items()):
                continue
            old = {k: p.get(k) for k in expected}
            clock_differences += str(p.get('time') or '').rstrip(':') != expected['time']
            day_changes += str(p.get('match_date') or '') != expected['match_date']
            changes.append({'match_id': row['match_id'], 'home': p.get('home_name'), 'away': p.get('away_name'),
                            'before': old, 'after': expected})
            backup.append(dict(row))
        report = {'applied': apply, 'sources': len(sources), 'records_to_sync': len(changes),
                  'clock_differences': clock_differences, 'date_label_changes': day_changes,
                  'identity_conflicts_skipped': conflicts, 'changes': changes}
        output.mkdir(parents=True, exist_ok=True)
        (output / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
        if apply:
            (output / 'backup_rows.json').write_text(json.dumps(backup, ensure_ascii=False), encoding='utf-8')
            conn.execute('BEGIN IMMEDIATE')
            for changed in changes:
                sync_stored_schedule(conn, changed['match_id'], sources[changed['match_id']])
            conn.commit()
            for path in json_paths:
                if not path.exists():
                    continue
                original_bytes = path.read_bytes()
                rows = json.loads(original_bytes.decode('utf-8'))
                if not isinstance(rows, list):
                    continue
                modified = False
                for row in rows:
                    source = sources.get(str(row.get('match_id') or row.get('id')))
                    same = source and all(str(row.get(s + '_name') or row.get(s + '_team') or '').strip().casefold() ==
                                          str(source.get(s + '_name') or source.get(s + '_team') or '').strip().casefold()
                                          for s in ('home', 'away'))
                    clock = schedule_fields(source) if same else None
                    if clock and any(row.get(k) != v for k, v in clock.items()):
                        row.update(clock); modified = True
                if modified:
                    if path.read_bytes() != original_bytes:
                        raise RuntimeError('El JSON cambió durante la reparación; repetir para conservar sus cambios')
                    (output / ('backup_' + path.name)).write_bytes(original_bytes)
                    tmp = path.with_suffix('.timezone.tmp')
                    tmp.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding='utf-8')
                    tmp.replace(path)
        print(json.dumps({k: v for k, v in report.items() if k != 'changes'}, ensure_ascii=False))
        print('Report: ' + str(output / 'report.json'))
        return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', type=Path, default=ROOT / 'data/app_data.db')
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    repair(args.db, args.apply, [ROOT / 'data/data_precacheo.json', ROOT / 'data/data_pending_results.json'])
