import sys
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent

# Build the compact Pre-Cacheo index before Gunicorn marks the worker ready.
# Existing Render services do not always pick up buildCommand changes from
# render.yaml, so startup is the reliable place to guarantee the fast store.
try:
    from scripts.build_precache_fast_store import main as build_precache_fast_store

    build_precache_fast_store()
except Exception as exc:
    print(f"Warning: Pre-Cacheo fast store was not built: {exc}")

# Add src to sys.path
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

from app import app

if __name__ == "__main__":
    app.run()
