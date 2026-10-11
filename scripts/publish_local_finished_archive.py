#!/usr/bin/env python
"""Publish newly cached local finished matches to GitHub without Turso.

Only the per-match archive is changed in a disposable sparse clone. The user's
working tree, legacy bucket JSONs and local SQLite database are never pushed.
"""
from __future__ import annotations

import argparse
import gzip
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))
from modules import sql_store  # noqa: E402
from scripts.cloud_cache_pipeline import quality_error  # noqa: E402

STATE = ROOT / "data" / "local_finished_upload_state.json"
ARCHIVE = Path("data/cache_archive/finished")
BATCH_SIZE = 300


def run(*args: str, cwd: Path = ROOT) -> str:
    result = subprocess.run(args, cwd=cwd, capture_output=True, text=True,
                            encoding="utf-8", errors="replace", check=False)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or f"Falló {args[0]}")
    return result.stdout.strip()


def git_remote() -> str:
    return run("git", "remote", "get-url", "origin")


def connect() -> sqlite3.Connection:
    db_path = Path(sql_store.DB_PATH)
    if not db_path.is_file():
        raise RuntimeError(f"No existe la SQLite local: {db_path}")
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def last_position(conn: sqlite3.Connection) -> tuple[str, str]:
    row = conn.execute(
        "SELECT updated_at, match_id FROM matches WHERE state='historical' "
        "ORDER BY updated_at DESC, match_id DESC LIMIT 1"
    ).fetchone()
    return (str(row[0]), str(row[1])) if row else ("", "")


def save_cursor(stamp: str, match_id: str) -> None:
    STATE.parent.mkdir(parents=True, exist_ok=True)
    temp = STATE.with_suffix(".tmp")
    temp.write_text(json.dumps({"updated_at": stamp, "match_id": match_id}), encoding="utf-8")
    os.replace(temp, STATE)


def cursor() -> tuple[str, str]:
    if not STATE.exists():
        raise RuntimeError("Primero ejecuta el paso 'mark' antes de cachear")
    state = json.loads(STATE.read_text(encoding="utf-8"))
    return str(state.get("updated_at") or ""), str(state.get("match_id") or "")


def mark() -> None:
    if STATE.exists():
        print(f"Continuando desde el último upload confirmado: {cursor()[0]}")
        return
    with connect() as conn:
        stamp, match_id = last_position(conn)
    save_cursor(stamp, match_id)
    print(f"Punto de partida guardado: {stamp or 'base vacía'}. Los partidos anteriores permanecen intactos.")


def candidates(conn: sqlite3.Connection, stamp: str, match_id: str) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT match_id, updated_at, score, payload_json FROM matches "
        "WHERE state='historical' AND "
        "(updated_at > ? OR (updated_at = ? AND match_id > ?)) "
        "ORDER BY updated_at, match_id LIMIT ?",
        (stamp, stamp, match_id, BATCH_SIZE),
    ).fetchall()


def compressed_json(row: dict) -> bytes:
    payload = json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as zipped:
        zipped.write(payload)
    return buffer.getvalue()


def publish_batch(rows: list[sqlite3.Row], remote: str) -> tuple[int, int]:
    for attempt in range(1, 4):
        with tempfile.TemporaryDirectory(prefix="finished-github-") as temp:
            checkout = Path(temp) / "repo"
            run("git", "clone", "--depth=1", "--filter=blob:none", "--sparse",
                "--single-branch", "--branch", "main", remote, str(checkout))
            run("git", "sparse-checkout", "set", str(ARCHIVE).replace("\\", "/"), cwd=checkout)
            target = checkout / ARCHIVE
            target.mkdir(parents=True, exist_ok=True)
            uploaded = skipped = 0
            for item in rows:
                match_id = str(item["match_id"])
                if not match_id.isdigit():
                    skipped += 1
                    continue
                if (target / f"{match_id}.json").exists() or (target / f"{match_id}.json.gz").exists():
                    skipped += 1
                    continue
                try:
                    match = json.loads(item["payload_json"])
                    match["match_id"] = match_id
                except (TypeError, ValueError):
                    skipped += 1
                    continue
                if quality_error(match, "finished"):
                    skipped += 1
                    continue
                (target / f"{match_id}.json.gz").write_bytes(compressed_json(match))
                uploaded += 1
            if not uploaded:
                return 0, skipped
            run("git", "add", "--", str(ARCHIVE).replace("\\", "/"), cwd=checkout)
            run("git", "config", "user.name", "local-finished-cache", cwd=checkout)
            run("git", "config", "user.email", "local-finished-cache@users.noreply.github.com", cwd=checkout)
            run("git", "commit", "-m", f"data: archive {uploaded} local finished matches", cwd=checkout)
            result = subprocess.run(["git", "push", "origin", "HEAD:main"], cwd=checkout,
                                    capture_output=True, text=True, encoding="utf-8", errors="replace")
            if result.returncode == 0:
                return uploaded, skipped
            if attempt == 3:
                raise RuntimeError(f"GitHub rechazó la subida tras 3 intentos: {result.stderr.strip()[-600:]}")
            print(f"GitHub cambió durante la subida; repitiendo sobre main actual ({attempt}/3).", flush=True)
    raise RuntimeError("No se pudo publicar el lote")


def publish() -> int:
    remote = git_remote()
    stamp, match_id = cursor()
    total_uploaded = total_skipped = 0
    while True:
        with connect() as conn:
            rows = candidates(conn, stamp, match_id)
        if not rows:
            break
        uploaded, skipped = publish_batch(rows, remote)
        stamp, match_id = str(rows[-1]["updated_at"]), str(rows[-1]["match_id"])
        save_cursor(stamp, match_id)  # Only after GitHub confirmed the push.
        total_uploaded += uploaded
        total_skipped += skipped
        print(f"Lote confirmado: {uploaded} nuevos, {skipped} existentes/incompletos; cursor {stamp}", flush=True)
    print(f"Subida terminada: {total_uploaded} nuevos en GitHub, {total_skipped} omitidos sin duplicar.")
    return total_uploaded


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("mark", "publish"))
    args = parser.parse_args()
    if args.command == "mark":
        mark()
    else:
        publish()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise SystemExit(1)
