import sys
import os
import json
import time
import logging
import argparse
from contextlib import contextmanager
from pathlib import Path

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from modules import history_manager
from app import analizar_partido_completo
from modules import data_manager, sql_store

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [Worker %(worker)s] - %(message)s')

def normalize_ah(value):
    """Normalize AH value to match filter logic (0.25/0.75 -> 0.5)."""
    if not value or value == 'N/A': return None
    try:
        float_val = float(value)
        abs_val = abs(float_val)
        base = int(abs_val)
        frac = abs_val - base
        
        bucket = base
        if abs(frac - 0.0) > 0.01: # If not 0.0
            bucket = base + 0.5
            
        return -bucket if float_val < 0 else bucket
    except ValueError:
        return None

def save_match_safe(match_data):
    """Saves match data using data_manager (split buckets)."""
    # data_manager handles locking and buckets
    data_manager.save_match(match_data)


@contextmanager
def _exclusive_file_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(lock_path, 'a+', encoding='utf-8')
    try:
        if os.name == 'nt':
            import msvcrt  # type: ignore
            while True:
                try:
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                    break
                except OSError:
                    time.sleep(0.02)
        else:
            import fcntl  # type: ignore
            while True:
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except OSError:
                    time.sleep(0.02)
        yield handle
    finally:
        try:
            if os.name == 'nt':
                import msvcrt  # type: ignore
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl  # type: ignore
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        handle.close()


def _increment_shared_progress(progress_file: Path) -> int:
    lock_file = progress_file.with_suffix(progress_file.suffix + '.lock')
    with _exclusive_file_lock(lock_file):
        current = 0
        if progress_file.exists():
            try:
                current = int(progress_file.read_text(encoding='utf-8').strip() or '0')
            except Exception:
                current = 0
        current += 1
        progress_file.write_text(str(current), encoding='utf-8')
        return current


def _export_precacheo_snapshot() -> str:
    output_path = sql_store.export_bucket_to_json(data_manager.PRECACHEO_BUCKET)
    return str(output_path)


def run_scraper(worker_index, total_workers, ah_filter=None, job_file=None, progress_file=None, flush_every=5):
    logger = logging.getLogger(f"Worker-{worker_index}")
    extra = {'worker': worker_index}
    logger = logging.LoggerAdapter(logger, extra)
    
    print(f"Worker {worker_index}/{total_workers} started.")
    
    all_pending = []
    
    is_job_mode = bool(job_file and os.path.exists(job_file))

    if is_job_mode:
        print(f"Worker {worker_index}: Loading matches from job file: {job_file}")
        try:
            with open(job_file, 'r') as f:
                all_pending = json.load(f)
        except Exception as e:
            print(f"Worker {worker_index}: Error loading job file: {e}")
            return
    else:
        # Load from SQL-backed history manager
        print(f"Worker {worker_index}: Loading from SQL history")
        pending_structure = history_manager.get_pending_matches()
        for season, leagues in pending_structure.items():
            for league_id, matches in leagues.items():
                for m in matches:
                    m_obj = m if isinstance(m, dict) else {'id': m, 'ah': 'N/A'}
                    m_obj['season'] = season
                    m_obj['league_id'] = league_id
                    all_pending.append(m_obj)
        
        # Filter by AH only in legacy mode (job file should already be filtered)
        if ah_filter and ah_filter != 'all':
            filtered_pending = []
            filter_val = float(ah_filter)
            for m in all_pending:
                norm_ah = normalize_ah(m.get('ah'))
                if norm_ah is not None and norm_ah == filter_val:
                    filtered_pending.append(m)
            all_pending = filtered_pending

    if not all_pending:
        print(f"Worker {worker_index}: No matches to process. Exiting.")
        return

    # Select matches for this worker
    my_matches = [m for i, m in enumerate(all_pending) if i % total_workers == worker_index]
    
    if not my_matches:
        print(f"Worker {worker_index}: No matches assigned (Total: {len(all_pending)}). Exiting.")
        return
        
    print(f"Worker {worker_index}: Processing {len(my_matches)} matches...")
    
    processed_count = 0
    progress_path = Path(progress_file) if progress_file else None

    for match in my_matches:
        match_id = match['id']
        # print(f"Worker {worker_index}: Scraping {match_id}...") # Reduce spam
        
        try:
            # Scrape
            match_data = analizar_partido_completo(match_id, force_refresh=True)
            
            if "error" in match_data:
                print(f"Worker {worker_index}: Error scraping {match_id}: {match_data['error']}")
                history_manager.move_to_cached(match['season'], match['league_id'], match_id)
                continue
            
            # Save: in job mode this flow is "analisis previo", so persist into precacheo.
            if is_job_mode:
                match_data['match_id'] = str(match_id)
                match_data['precacheo_date'] = time.strftime('%Y-%m-%dT%H:%M:%S')
                data_manager.save_precacheo_match(match_data)
            else:
                save_match_safe(match_data)
            
            # Update History
            history_manager.move_to_cached(match['season'], match['league_id'], match_id)
            
            processed_count += 1

            if is_job_mode and progress_path:
                global_done = _increment_shared_progress(progress_path)
                if flush_every > 0 and global_done % flush_every == 0:
                    try:
                        exported = _export_precacheo_snapshot()
                        print(
                            f"Worker {worker_index}: Snapshot intermedio exportado "
                            f"(global={global_done}) -> {exported}"
                        )
                    except Exception as export_err:
                        print(f"Worker {worker_index}: Error exportando snapshot intermedio: {export_err}")

            if processed_count % 5 == 0:
                 print(f"Worker {worker_index}: Progress {processed_count}/{len(my_matches)}")
            
        except Exception as e:
            print(f"Worker {worker_index}: Critical error on {match_id}: {e}")
            try:
                history_manager.move_to_cached(match['season'], match['league_id'], match_id)
            except:
                pass

    print(f"Worker {worker_index}: Finished. Processed {processed_count} matches.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='CLI Scraper Worker')
    parser.add_argument('--index', type=int, required=True, help='Worker index (0-based)')
    parser.add_argument('--total', type=int, required=True, help='Total number of workers')
    parser.add_argument('--ah', type=str, default='all', help='AH Filter value (legacy)')
    parser.add_argument('--job_file', type=str, default=None, help='Path to JSON file with matches to scrape')
    parser.add_argument('--progress_file', type=str, default=None, help='Shared progress counter file')
    parser.add_argument('--flush_every', type=int, default=5, help='Export precacheo JSON every N global matches')
    
    args = parser.parse_args()
    
    run_scraper(args.index, args.total, args.ah, args.job_file, args.progress_file, args.flush_every)
