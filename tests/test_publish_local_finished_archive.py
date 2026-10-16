import gzip
import json
from pathlib import Path
import sqlite3
import subprocess
import pytest

from scripts import publish_local_finished_archive as uploader


def _git(*args, cwd):
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True)


def test_publish_keeps_existing_match_and_adds_only_new_id(tmp_path, monkeypatch):
    remote = tmp_path / "remote.git"
    remote.mkdir()
    _git("init", "--bare", "--initial-branch=main", cwd=remote)
    initial = tmp_path / "initial"
    _git("clone", str(remote), str(initial), cwd=tmp_path)
    archive = initial / uploader.ARCHIVE
    archive.mkdir(parents=True)
    (archive / "101.json").write_text('{"match_id":"101","old":true}', encoding="utf-8")
    _git("add", ".", cwd=initial)
    _git("-c", "user.name=test", "-c", "user.email=test@example.com", "commit", "-m", "initial", cwd=initial)
    _git("push", "origin", "HEAD:main", cwd=initial)

    monkeypatch.setattr(uploader, "quality_error", lambda _row, _kind: None)
    rows = [
        {"match_id": "101", "payload_json": '{"match_id":"101","old":false}'},
        {"match_id": "102", "payload_json": '{"match_id":"102","score":"2:1"}'},
    ]
    assert uploader.publish_batch(rows, str(remote)) == (1, 1)

    result = tmp_path / "result"
    _git("clone", str(remote), str(result), cwd=tmp_path)
    assert json.loads((result / uploader.ARCHIVE / "101.json").read_text(encoding="utf-8"))["old"] is True
    with gzip.open(result / uploader.ARCHIVE / "102.json.gz", "rt", encoding="utf-8") as handle:
        assert json.load(handle)["match_id"] == "102"


def test_failed_push_does_not_advance_resume_cursor(tmp_path, monkeypatch):
    db = tmp_path / "matches.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE matches (match_id TEXT, state TEXT, updated_at TEXT, score TEXT, payload_json TEXT)")
        conn.execute("INSERT INTO matches VALUES ('101','historical','2026-10-10T10:00:00','1:0','{}')")
    monkeypatch.setattr(uploader, "STATE", tmp_path / "cursor.json")
    monkeypatch.setattr(uploader.sql_store, "DB_PATH", db)
    uploader.mark()
    before = uploader.cursor()
    with sqlite3.connect(db) as conn:
        conn.execute("INSERT INTO matches VALUES ('102','historical','2026-10-11T10:00:00','2:0','{}')")
    monkeypatch.setattr(uploader, "git_remote", lambda: "unused")
    monkeypatch.setattr(uploader, "publish_batch", lambda *_args: (_ for _ in ()).throw(RuntimeError("push failed")))
    with pytest.raises(RuntimeError, match="push failed"):
        uploader.publish()
    assert uploader.cursor() == before
