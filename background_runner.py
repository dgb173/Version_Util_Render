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

from modules import data_manager  # noqa: E402
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
        elif isinstance(payload.get("matches"), list):
            jobs = payload.get("matches")

    out = []
    for item in jobs:
        if not isinstance(item, dict):
            continue
        raw_id = item.get("id") or item.get("match_id")
        if raw_id is None:
            continue
        match_id = str(raw_id).strip()
        if not match_id:
            continue
        out.append(match_id)
    return out


def _process_match(match_id: str):
    try:
        match_data = analizar_partido_completo(match_id)
        if not match_data or match_data.get("error"):
            return False, match_id, "scrape_error"

        match_data["match_id"] = str(match_id)
        match_data["precacheo_date"] = datetime.datetime.now().isoformat()
        data_manager.save_precacheo_match(match_data)
        return True, match_id, "saved"
    except Exception as exc:
        return False, match_id, str(exc)


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
    return parser.parse_args()


def main():
    args = parse_args()
    job_file = Path(args.job_file)
    if not job_file.exists():
        print(f"ERROR: No existe job_file: {job_file}")
        return 2

    _cleanup_precacheo_stale()

    match_ids = _load_jobs(job_file)
    total = len(match_ids)
    if total == 0:
        print("No hay partidos para procesar.")
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

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_process_match, mid): mid for mid in match_ids}

        for future in concurrent.futures.as_completed(futures):
            completed += 1
            success, mid, info = future.result()
            if success:
                ok += 1
            else:
                failed += 1

            if completed % flush_every == 0 or completed == total:
                print(
                    f"Progreso: {completed}/{total} "
                    f"(ok={ok}, fail={failed})"
                )
                if not success:
                    print(f"  Error match {mid}: {info}")

    _cleanup_precacheo_stale()
    print(f"Finalizado. ok={ok}, fail={failed}, total={total}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
