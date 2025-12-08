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
    
    processes = []
    
    try:
        if args.concurrency > 1:
            log(f"Spawning {args.concurrency} worker processes...")
            for i in range(args.concurrency):
                # Workers share the same job file, they filter by index themselves
                cmd = [python_cmd, str(script_path), "--index", str(i), "--total", str(args.concurrency), "--job_file", str(job_file)]
                
                # We let them inherit stdout/stderr so they print to THIS console window
                p = subprocess.Popen(cmd)
                processes.append(p)
                
            # Wait for all
            for p in processes:
                p.wait()
                
        else:
            log("Running in single process mode (Worker 0/1)...")
            cmd = [python_cmd, str(script_path), "--index", "0", "--total", "1", "--job_file", str(job_file)]
            subprocess.run(cmd, check=True)
            
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
            
    log("Press Enter to close this window...")
    input()

if __name__ == "__main__":
    main()
