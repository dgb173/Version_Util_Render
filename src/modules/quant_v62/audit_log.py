"""Append-only local ledger. Hashes detect edits; this is not trusted timestamping."""
import hashlib
import json
import sqlite3


def canonical(data):
    return json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",",":"), allow_nan=False)


def digest(data):
    return hashlib.sha256(canonical(data).encode()).hexdigest()


def append_record(path, record):
    payload, sha = canonical(record), digest(record)
    db = sqlite3.connect(path)
    try:
        db.executescript("""
        CREATE TABLE IF NOT EXISTS audit (
          id INTEGER PRIMARY KEY, sha256 TEXT UNIQUE NOT NULL,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP, payload TEXT NOT NULL);
        CREATE TRIGGER IF NOT EXISTS forbid_update BEFORE UPDATE ON audit
          BEGIN SELECT RAISE(ABORT, 'append only'); END;
        CREATE TRIGGER IF NOT EXISTS forbid_delete BEFORE DELETE ON audit
          BEGIN SELECT RAISE(ABORT, 'append only'); END;
        """)
        db.execute("INSERT INTO audit(sha256,payload) VALUES (?,?)",(sha,payload))
        db.commit()
    finally:
        db.close()
    return sha
