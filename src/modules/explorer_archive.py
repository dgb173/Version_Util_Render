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

DATA = Path(__file__).resolve().parents[2] / 'data'
ARCHIVE_PATH = DATA / 'explorer_recent.json'
FULL_PATH = DATA / 'explorer_full.sqlite.gz'
_lock = threading.Lock()
_db_path = None
_rows = None


def _database():
    global _db_path
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
            _db_path = name
        except Exception:
            os.unlink(name)
            raise
    return _db_path


def load_page(bucket=None, limit=150, offset=0):
    global _rows
    path = _database()
    start = max(0, int(offset or 0))
    count = max(1, int(limit or 150))
    if path:
        query = 'SELECT payload FROM matches'
        params = []
        if bucket:
            query += ' WHERE bucket = ?'
            params.append(bucket)
        query += ' ORDER BY match_date DESC, match_id DESC LIMIT ? OFFSET ?'
        params.extend([count, start])
        with sqlite3.connect(path) as conn:
            return [json.loads(zlib.decompress(row[0])) for row in conn.execute(query, params)]
    with _lock:
        if _rows is None:
            with ARCHIVE_PATH.open(encoding='utf-8') as handle:
                _rows = json.load(handle)
    candidates = [row for row in _rows if not bucket or row.get('_explorer_bucket') == bucket]
    return candidates[start:start + count]
