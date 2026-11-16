"""Disk-backed historical archive for stateless deployments."""
import gzip
import json
import os
import shutil
import sqlite3
import tempfile
import threading
import zlib
from pathlib import Path
from .explorer_movement_index import bucket as movement_bucket, index_row

DATA = Path(__file__).resolve().parents[2] / 'data'
ARCHIVE_PATH = DATA / 'explorer_recent.json'
FULL_PATH = DATA / 'explorer_full.sqlite.gz'
MOVEMENT_INDEX_PATH = DATA / 'explorer_movement_index.json.gz'
_lock = threading.Lock()
_db_path = None
_rows = None
_movement_index_available = False


def _database():
    global _db_path, _movement_index_available
    if _db_path is not None:
        return _db_path
    if not FULL_PATH.exists():
        return None
    with _lock:
        if _db_path is not None:
            return _db_path
        fd, name = tempfile.mkstemp(prefix='explorer-history-', suffix='.sqlite')
        os.close(fd)
        try:
            with gzip.open(FULL_PATH, 'rb') as source, open(name, 'wb') as dest:
                shutil.copyfileobj(source, dest)
            with sqlite3.connect(name) as conn:
                if MOVEMENT_INDEX_PATH.exists():
                    conn.execute('CREATE TABLE IF NOT EXISTS movement_index (match_id TEXT PRIMARY KEY, stadium_start REAL, stadium_end REAL, general_start REAL, general_end REAL)')
                    with gzip.open(MOVEMENT_INDEX_PATH, 'rt', encoding='utf-8') as source:
                        conn.executemany('INSERT OR REPLACE INTO movement_index VALUES (?,?,?,?,?)',
                                         (json.loads(line) for line in source))
                    conn.execute('CREATE INDEX IF NOT EXISTS movement_stadium ON movement_index(stadium_start,stadium_end)')
                    conn.execute('CREATE INDEX IF NOT EXISTS movement_general ON movement_index(general_start,general_end)')
                    _movement_index_available = True
            if ARCHIVE_PATH.exists():
                with ARCHIVE_PATH.open(encoding='utf-8') as handle:
                    recent = json.load(handle)
                with sqlite3.connect(name) as conn:
                    for row in recent:
                        if not isinstance(row, dict) or not row.get('match_id'):
                            continue
                        raw = json.dumps(row, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
                        conn.execute('INSERT OR REPLACE INTO matches VALUES (?,?,?,?)', (
                            str(row['match_id']), row.get('_explorer_bucket'),
                            row.get('match_date') or row.get('date'), zlib.compress(raw)))
                        if _movement_index_available:
                            conn.execute('INSERT OR REPLACE INTO movement_index VALUES (?,?,?,?,?)', index_row(row))
            _db_path = name
        except Exception:
            os.unlink(name)
            raise
    return _db_path


def load_page(bucket=None, limit=150, offset=0, movement_filters=None):
    global _rows
    path = _database()
    start = max(0, int(offset or 0))
    count = max(1, int(limit or 150))
    if path:
        use_index = bool(movement_filters and _movement_index_available)
        query = 'SELECT m.payload FROM matches m'
        if use_index:
            query += ' JOIN movement_index i ON i.match_id = m.match_id'
        params = []
        clauses = []
        if bucket:
            clauses.append('m.bucket = ?')
            params.append(bucket)
        if use_index:
            for section in ('stadium', 'general'):
                for part in ('start', 'end'):
                    requested = movement_filters.get(f'h2h_{section}_{part}_ah') or []
                    if not isinstance(requested, list):
                        requested = [requested]
                    conditions = []
                    for value in requested:
                        number = movement_bucket(value)
                        column = f'i.{section}_{part}'
                        operator = '>=' if number >= 2.5 else '<=' if number <= -2.5 else '='
                        conditions.append(f'{column} {operator} ?')
                        params.append(number)
                    if conditions:
                        clauses.append('(' + ' OR '.join(conditions) + ')')
        if clauses:
            query += ' WHERE ' + ' AND '.join(clauses)
        query += ' ORDER BY m.match_date DESC, m.match_id DESC LIMIT ? OFFSET ?'
        params.extend([count, start])
        with sqlite3.connect(path) as conn:
            return [json.loads(zlib.decompress(row[0])) for row in conn.execute(query, params)]
    with _lock:
        if _rows is None:
            with ARCHIVE_PATH.open(encoding='utf-8') as handle:
                _rows = json.load(handle)
    candidates = [row for row in _rows if not bucket or row.get('_explorer_bucket') == bucket]
    return candidates[start:start + count]
