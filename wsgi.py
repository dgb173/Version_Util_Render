import sys
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent

# Add src to sys.path
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

# Pre-warm and bootstrap SQL store before Gunicorn opens ports for traffic
try:
    from modules import sql_store
    sql_store.ensure_bootstrap()
except Exception as exc:
    print(f"Warning: SQL bootstrap on startup: {exc}")

from app import app

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
