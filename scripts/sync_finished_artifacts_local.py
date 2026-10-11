#!/usr/bin/env python
"""Import validated GitHub finished-cache artifacts into the local Explorer SQL DB."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
from modules import data_manager, sql_store  # noqa: E402


def _run(*args: str) -> str:
    completed = subprocess.run(args, cwd=ROOT, capture_output=True, text=True, encoding="utf-8")
    if completed.returncode:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip() or f"Falló {' '.join(args[:2])}")
    return completed.stdout


def github_repo() -> str:
    remote = _run("git", "remote", "get-url", "origin").strip()
    match = re.search(r"github\.com[:/]([^/\s]+/[^/\s]+?)(?:\.git)?$", remote)
    if not match:
        raise RuntimeError(f"El remoto origin no es un repositorio GitHub: {remote}")
    return match.group(1)


def recent_finished_runs(repo: str, days: int = 7) -> list[int]:
    runs_by_id = {}
    for workflow in ("cachear-terminados.yml", "cachear-terminados-10x.yml", "cachear-terminados-5x.yml"):
        try:
            raw = _run(
                "gh", "run", "list", "--repo", repo, "--workflow", workflow,
                "--limit", "30", "--json", "databaseId,status,conclusion,createdAt",
            )
        except RuntimeError as exc:
            if "workflow" in str(exc).lower() and "not found" in str(exc).lower():
                continue
            raise
        for item in json.loads(raw):
            runs_by_id[int(item["databaseId"])] = item
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)
    runs = []
    for item in runs_by_id.values():
        created = dt.datetime.fromisoformat(item["createdAt"].replace("Z", "+00:00"))
        # Turso may fail after successful analysis; the artifacts still contain
        # the validated rows and can be imported into the local SQLite store.
        if item.get("status") == "completed" and item.get("conclusion") in {"success", "failure"} and created >= cutoff:
            runs.append((created, int(item["databaseId"])))
    return [run_id for _, run_id in sorted(runs)]


def artifact_rows(repo: str, run_id: int) -> list[dict]:
    with tempfile.TemporaryDirectory(prefix="finished-cache-") as temp_dir:
        _run(
            "gh", "run", "download", str(run_id), "--repo", repo,
            "--pattern", "cache-result-*", "--dir", temp_dir,
        )
        files = sorted(Path(temp_dir).rglob("result_*.json"))
        if not files:
            raise RuntimeError(f"El run {run_id} no contiene artefactos cache-result")
        rows = []
        from scripts.cloud_cache_pipeline import quality_error
        for path in files:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if payload.get("kind") != "finished":
                raise RuntimeError(f"Artefacto inesperado: {path.name}")
            for row in payload.get("rows") or []:
                issue = quality_error(row, "finished")
                if issue:
                    raise RuntimeError(f"Artefacto {path.name} con partido incompleto: {issue}")
                rows.append(row)
        return rows


def pending_rows(rows: list[dict]) -> list[tuple[dict, str]]:
    sql_store.ensure_bootstrap()
    with sql_store._connect() as conn:
        existing = {
            str(row["match_id"]): (str(row["score"] or ""), str(row["state"] or ""))
            for row in conn.execute("SELECT match_id, score, state FROM matches").fetchall()
        }
    # Later workflow runs are authoritative for the same match ID.
    latest = {}
    for match in rows:
        if not isinstance(match, dict):
            continue
        match_id = str(match.get("match_id") or match.get("id") or "").strip()
        if match_id:
            latest[match_id] = match
    pending = {}
    for match_id, match in latest.items():
        if not match_id or not data_manager.validate_explorer_match(match):
            continue
        score = str(match.get("score") or match.get("final_score") or "").strip()
        if not score or "?" in score:
            continue
        # A matching final score does not imply a complete analysis. Replace
        # legacy rows whose previous-match and H2H columns may be empty.
        ah = match.get("handicap")
        if ah is None:
            ah = (match.get("main_match_odds") or {}).get("ah_linea")
        match["match_id"] = match_id
        pending[match_id] = (match, data_manager.get_bucket_name(ah))
    return list(pending.values())


def synced_run_ids() -> set[int]:
    sql_store.ensure_bootstrap()
    with sql_store._connect() as conn:
        present = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='local_finished_sync_runs'"
        ).fetchone()
        if not present:
            return set()
        return {int(row[0]) for row in conn.execute("SELECT run_id FROM local_finished_sync_runs")}


def normalize_local_historical_dates() -> int:
    """Repair legacy date strings once so indexed Explorer ordering stays fast."""
    sql_store.ensure_bootstrap()
    with sql_store._connect() as conn:
        rows = conn.execute(
            "SELECT match_id, match_date FROM matches WHERE state='historical' AND match_date IS NOT NULL"
        ).fetchall()
        fixes = []
        for row in rows:
            normalized = sql_store._normalize_iso_date(row["match_date"])
            if normalized and re.fullmatch(r"\d{4}-\d{2}-\d{2}", normalized) and normalized != row["match_date"]:
                fixes.append((normalized, row["match_id"]))
        if fixes:
            conn.executemany("UPDATE matches SET match_date=? WHERE match_id=?", fixes)
    return len(fixes)


def sync(dry_run: bool = False, days: int = 7) -> int:
    repo = github_repo()
    if not dry_run:
        normalized = normalize_local_historical_dates()
        if normalized:
            print(f"Fechas historicas normalizadas: {normalized}", flush=True)
    available_runs = recent_finished_runs(repo, days)
    if not available_runs:
        raise RuntimeError(f"No hay ejecuciones recientes de terminados en los últimos {days} días")
    runs = [run_id for run_id in available_runs if run_id not in synced_run_ids()]
    if not runs:
        print("Todos los runs recientes ya están incorporados en esta SQLite local.", flush=True)
        return 0
    print(f"Fuente actual: artefactos de {len(runs)} ejecuciones de GitHub ({repo}).", flush=True)
    all_rows = []
    artifact_runs = []
    for run_id in runs:
        try:
            rows = artifact_rows(repo, run_id)
        except RuntimeError as exc:
            message = str(exc).lower()
            if "no contiene artefactos" in message or ("artifact" in message and ("no " in message or "not found" in message)):
                print(f"  Run {run_id}: sin artefactos importables; se omite", flush=True)
                continue
            raise
        print(f"  Run {run_id}: {len(rows)} terminados validados", flush=True)
        all_rows.extend(rows)
        artifact_runs.append(run_id)
    if not all_rows:
        raise RuntimeError("Ningún run reciente contiene análisis completos para importar")
    changes = pending_rows(all_rows)
    print(f"Partidos nuevos o corregidos para SQLite local: {len(changes)}", flush=True)
    if dry_run:
        return len(changes)
    with sql_store._connect() as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS local_finished_sync_runs "
            "(run_id INTEGER PRIMARY KEY, synced_at TEXT NOT NULL)"
        )
        for match, bucket in changes:
            sql_store._upsert_match(conn, match, bucket=bucket, state="historical")
        stamp = dt.datetime.now(dt.timezone.utc).isoformat()
        conn.executemany(
            "INSERT OR REPLACE INTO local_finished_sync_runs(run_id, synced_at) VALUES (?, ?)",
            [(run_id, stamp) for run_id in artifact_runs],
        )
    data_manager._clear_explorer_cache()
    print(f"Incorporados {len(changes)} terminados al Explorador local.", flush=True)
    return len(changes)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()
    try:
        sync(dry_run=args.dry_run, days=max(1, min(args.days, 7)))
        return 0
    except Exception as exc:
        print(f"[ERROR] No se pudieron sincronizar los terminados: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
