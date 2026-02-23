import argparse
import subprocess
import json
import os
import time
import sys
from pathlib import Path

# Configure logging/printing
def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")

def main():
    parser = argparse.ArgumentParser(description='Background Scraper Runner')
    parser.add_argument('--job_file', type=str, required=True, help='Path to JSON job file')
    parser.add_argument('--concurrency', type=int, default=1, help='Number of concurrent workers')
    parser.add_argument('--flush_every', type=int, default=5, help='Export precacheo JSON every N global matches')
    
    args = parser.parse_args()
    
    job_file = Path(args.job_file)
    if not job_file.exists():
        log(f"Error: Job file not found: {job_file}")
        sys.exit(1)
        
    try:
        with open(job_file, 'r') as f:
            matches = json.load(f)
    except Exception as e:
        log(f"Error loading job file: {e}")
        sys.exit(1)
        
    total_matches = len(matches)
    log(f"Starting background scraper management for {total_matches} matches.")
    log(f"Concurrency: {args.concurrency}")
    
    # Python command to use
    python_cmd = "py" if os.name == 'nt' else "python3"
    
    # Path to cli_scraper.py
    # Assuming background_runner.py is in root, same as cli_scraper.py
    script_path = Path(__file__).parent / 'cli_scraper.py'
    progress_file = Path(__file__).parent / 'data' / '.precache_progress.count'
    progress_lock = Path(str(progress_file) + '.lock')

    try:
        progress_file.parent.mkdir(parents=True, exist_ok=True)
        progress_file.write_text('0', encoding='utf-8')
    except Exception as e:
        log(f"Warning: could not initialize progress file: {e}")
    
    processes = []
    
    try:
        if args.concurrency > 1:
            log(f"Spawning {args.concurrency} worker processes...")
            for i in range(args.concurrency):
                # Workers share the same job file, they filter by index themselves
                cmd = [
                    python_cmd,
                    str(script_path),
                    "--index",
                    str(i),
                    "--total",
                    str(args.concurrency),
                    "--job_file",
                    str(job_file),
                    "--progress_file",
                    str(progress_file),
                    "--flush_every",
                    str(args.flush_every),
                ]
                
                # We let them inherit stdout/stderr so they print to THIS console window
                p = subprocess.Popen(cmd)
                processes.append(p)
                
            # Wait for all
            for p in processes:
                p.wait()
                
        else:
            log("Running in single process mode (Worker 0/1)...")
            cmd = [
                python_cmd,
                str(script_path),
                "--index",
                "0",
                "--total",
                "1",
                "--job_file",
                str(job_file),
                "--progress_file",
                str(progress_file),
                "--flush_every",
                str(args.flush_every),
            ]
            subprocess.run(cmd, check=True)
            
        try:
            src_path = Path(__file__).parent / 'src'
            if str(src_path) not in sys.path:
                sys.path.insert(0, str(src_path))
            from modules import sql_store  # type: ignore
            out = sql_store.export_bucket_to_json('data_precacheo.json')
            log(f"Final precacheo snapshot exported: {out}")
        except Exception as e:
            log(f"Warning: could not export final precacheo snapshot: {e}")

        log("All workers finished.")
        
    except KeyboardInterrupt:
        log("\nCaught KeyboardInterrupt. Terminating workers...")
        for p in processes:
            p.terminate()
    except Exception as e:
        log(f"Critical error: {e}")
    finally:
        # Cleanup job file
        try:
            if job_file.exists():
                os.remove(job_file)
                log("Job file cleaned up.")
        except Exception as e:
            log(f"Error cleaning up job file: {e}")
        try:
            if progress_file.exists():
                os.remove(progress_file)
            if progress_lock.exists():
                os.remove(progress_lock)
        except Exception as e:
            log(f"Error cleaning up progress files: {e}")
            
if __name__ == "__main__":
    main()
