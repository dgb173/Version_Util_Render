import argparse
import concurrent.futures
import datetime
import json
import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from modules import data_manager, sql_store  # noqa: E402
from modules.nowgoal_fetcher import parse_handicap_numeric  # noqa: E402
from modules.estudio_scraper import analizar_partido_completo  # noqa: E402


def _load_jobs(job_file: Path):
    with job_file.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)

    jobs = []
    if isinstance(payload, list):
        jobs = payload
    elif isinstance(payload, dict):
        if isinstance(payload.get("jobs"), list):
            jobs = payload.get("jobs")
        elif isinstance(payload.get("items"), list):
            jobs = payload.get("items")
        elif isinstance(payload.get("matches"), list):
            jobs = payload.get("matches")

    out = []
    meta_map = {}
    seen = set()
    for item in jobs:
        if isinstance(item, dict):
            mid = str(item.get("id") or item.get("match_id") or "").strip()
            meta = item
        else:
            mid = str(item).strip()
            meta = {}
        if mid and mid not in seen:
            seen.add(mid)
            out.append(mid)
            meta_map[mid] = meta
    return out, meta_map


def _has_valid_handicap(match: dict) -> bool:
    if not isinstance(match, dict):
        return False
    raw = match.get("handicap")
    if raw in (None, "", "-", "N/A", "null", "None", "?", "--", "undefined"):
        odds = match.get("main_match_odds") or {}
        raw = odds.get("ah_linea")
    if raw in (None, "", "-", "N/A", "null", "None", "?", "--", "undefined"):
        return False
    return parse_handicap_numeric(raw) is not None


def _process_match(match_id: str, job_meta: dict = None, defer_summary_stats: bool = False):
    try:
        match_data = analizar_partido_completo(
            match_id,
            include_summary_stats=not defer_summary_stats,
        )
        if not match_data or match_data.get("error"):
            return False, match_id, "scrape_error", None

        if match_data.get("handicap") in (None, "", "N/A", "-", "null", "None"):
            match_data["handicap"] = (match_data.get("main_match_odds") or {}).get("ah_linea")
        if match_data.get("goal_line") in (None, "", "N/A", "-", "null", "None"):
            match_data["goal_line"] = (match_data.get("main_match_odds") or {}).get("goals_linea")

        # Si no tiene hándicap en la página H2H, recuperarlo del job o snapshot
        if not _has_valid_handicap(match_data):
            if job_meta and isinstance(job_meta, dict):
                job_ah = job_meta.get("ah") or job_meta.get("handicap")
                if job_ah and str(job_ah).strip() not in ("N/A", "-", "None", "null", "", "?"):
                    match_data["handicap"] = str(job_ah).strip()
                    match_data.setdefault("main_match_odds", {})["ah_linea"] = str(job_ah).strip()
                job_ou = job_meta.get("ou") or job_meta.get("goal_line")
                if job_ou and str(job_ou).strip() not in ("N/A", "-", "None", "null", "", "?"):
                    match_data["goal_line"] = str(job_ou).strip()
                    match_data.setdefault("main_match_odds", {})["goals_linea"] = str(job_ou).strip()

            if not _has_valid_handicap(match_data):
                try:
                    snapshot = sql_store.get_json_state('app_main_page_cache_v1', default={}) or {}
                    for snap_m in snapshot.get('upcoming_matches', []):
                        if str(snap_m.get('id') or snap_m.get('match_id')) == str(match_id):
                            ah_snap = snap_m.get('handicap') or (snap_m.get('main_match_odds') or {}).get('ah_linea')
                            gl_snap = snap_m.get('goal_line') or (snap_m.get('main_match_odds') or {}).get('goals_linea')
                            if parse_handicap_numeric(ah_snap) is not None:
                                match_data['handicap'] = str(ah_snap)
                                match_data.setdefault('main_match_odds', {})['ah_linea'] = str(ah_snap)
                            if gl_snap and gl_snap not in ('-', 'N/A', '?'):
                                match_data['goal_line'] = str(gl_snap)
                                match_data.setdefault('main_match_odds', {})['goals_linea'] = str(gl_snap)
                            break
                except Exception:
                    pass

        # No guardar partidos sin linea de handicap asiatico
        if not _has_valid_handicap(match_data):
            return False, match_id, "skipped_no_handicap", None

        match_data["match_id"] = str(match_id)
        match_data["precacheo_date"] = datetime.datetime.now().isoformat()
        data_manager.save_precacheo_match(match_data)
        return True, match_id, "saved", match_data
    except Exception as exc:
        return False, match_id, str(exc), None


def _cleanup_precacheo_stale():
    try:
        pending_days = max(0, int(os.getenv("PRECACHEO_PENDING_MAX_AGE_DAYS", "1")))
    except Exception:
        pending_days = 1

    try:
        removed = data_manager.clean_old_precacheo_matches(
            days_threshold=1,
            pending_days_threshold=pending_days,
        )
        if removed > 0:
            print(
                f"Cleanup precacheo: removed={removed} "
                f"(pending_max_age_days={pending_days})"
            )
    except Exception as exc:
        print(f"Warning: cleanup precacheo failed: {exc}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Runner de analisis previo desde JSON para pre-cacheo."
    )
    parser.add_argument(
        "--job_file",
        required=True,
        help="Ruta al JSON con partidos ({id} o {match_id}).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
        help="Numero de workers en paralelo.",
    )
    parser.add_argument(
        "--flush_every",
        type=int,
        default=5,
        help="Compatibilidad legacy. Se usa para frecuencia de progreso por lote.",
    )
    parser.add_argument(
        "--defer-summary-stats",
        action="store_true",
        default=False,
        help="Difiere la carga de estadísticas secundarias para mayor velocidad.",
    )
    parser.add_argument(
        "--output-json",
        type=str,
        default=None,
        help="Ruta donde escribir el JSON de salida con los partidos analizados.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    job_file = Path(args.job_file)
    if not job_file.exists():
        print(f"ERROR: No existe job_file: {job_file}")
        return 2

    _cleanup_precacheo_stale()

    match_ids, job_meta_map = _load_jobs(job_file)
    total = len(match_ids)
    if total == 0:
        print("No hay partidos para procesar.")
        if args.output_json:
            out_path = Path(args.output_json)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text("[]", encoding="utf-8")
        return 0

    workers = max(1, int(args.concurrency or 1))
    flush_every = max(1, int(args.flush_every or 1))

    print(
        f"Iniciando analisis previo desde {job_file} "
        f"(matches={total}, workers={workers})"
    )

    completed = 0
    ok = 0
    failed = 0
    saved_matches = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_process_match, mid, job_meta_map.get(mid), args.defer_summary_stats): mid
            for mid in match_ids
        }

        for future in concurrent.futures.as_completed(futures):
            completed += 1
            success, mid, info, match_payload = future.result()
            if success:
                ok += 1
                if match_payload:
                    saved_matches.append(match_payload)
            else:
                failed += 1

            if completed % flush_every == 0 or completed == total:
                print(
                    f"Progreso: {completed}/{total} "
                    f"(ok={ok}, fail={failed})"
                )
                if not success:
                    print(f"  Error match {mid}: {info}")

    if args.output_json:
        out_path = Path(args.output_json)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8") as fh:
            json.dump(saved_matches, fh, ensure_ascii=False, indent=2)
        print(f"Salida exportada a {out_path} ({len(saved_matches)} partidos)")

    _cleanup_precacheo_stale()
    print(f"Finalizado. ok={ok}, fail={failed}, total={total}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
