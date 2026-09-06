"""Replay validated cache merges against fresh main; never force push."""
import argparse
import os
from pathlib import Path
import subprocess
import tempfile
import time
from types import SimpleNamespace
from cloud_cache_pipeline import ROOT, merge


def git(*args, cwd, check=True):
    return subprocess.run(['git', *args], cwd=cwd, check=check)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--kind', choices=['upcoming', 'finished', 'list'], required=True)
    args = parser.parse_args()
    if not os.getenv('GITHUB_ACTIONS'):
        raise RuntimeError('Publisher runs only in a disposable GitHub runner')
    repo = os.environ['GITHUB_REPOSITORY']
    subprocess.run(['gh', 'auth', 'setup-git'], check=True)
    for attempt in range(3):
        with tempfile.TemporaryDirectory(prefix='cloud-cache-publish-') as directory:
            target = Path(directory) / 'repo'
            git('clone', '--depth=1', '--filter=blob:none', '--sparse', '--branch', 'main',
                f'https://github.com/{repo}.git', str(target), cwd=ROOT)
            git('sparse-checkout', 'set', 'data', cwd=target)
            merge(SimpleNamespace(root=target, kind=args.kind, prepared=ROOT / 'prepared', results=ROOT / 'results'))
            git('config', 'user.name', 'github-actions[bot]', cwd=target)
            git('config', 'user.email', '41898282+github-actions[bot]@users.noreply.github.com', cwd=target)
            git('add', '--', 'data.json', 'data/data.json', 'data/data_precacheo.json',
                'data/data_pending_results.json', 'data/cache_archive', 'data/cache_control',
                'data/data_ah_*.json', 'data/data_minus_ah_*.json', cwd=target)
            diff = git('diff', '--cached', '--quiet', cwd=target, check=False)
            if diff.returncode == 0:
                print('No changes to publish')
                return 0
            if diff.returncode != 1:
                raise RuntimeError('Could not inspect staged data')
            git('commit', '-m', f'chore: validated {args.kind} cache ({os.environ.get("GITHUB_RUN_ID")})', cwd=target)
            if git('push', 'origin', 'HEAD:main', cwd=target, check=False).returncode == 0:
                print('Cache published successfully')
                return 0
        time.sleep(2 ** attempt)
    raise RuntimeError('Publication failed after three fresh merges')


if __name__ == '__main__':
    raise SystemExit(main())
