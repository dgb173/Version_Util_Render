import json
import os
import sys
import time
from pathlib import Path
import logging

from . import sql_store

# Cross-platform file locking (kept for compatibility with old imports)
if sys.platform == 'win32':
    import msvcrt
else:
    import fcntl

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the path to history.json
BASE_DIR = Path(__file__).resolve().parent.parent.parent
HISTORY_DIR = BASE_DIR / 'studied_matches'
HISTORY_FILE = HISTORY_DIR / 'history.json'


def _ensure_history_file():
    """Ensures the history file and directory exist with the correct structure."""
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    if not HISTORY_FILE.exists():
        initial_structure = {'pending': {}, 'cached': {}}
        with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(initial_structure, f, indent=2, ensure_ascii=False)
        return initial_structure
    return None


class FileLock:
    """Cross-platform context manager for file locking (legacy compatibility)."""

    def __init__(self, file_path):
        self.file_path = file_path
        self.file_handle = None

    def __enter__(self):
        # Ensure file exists before locking
        Path(self.file_path).parent.mkdir(parents=True, exist_ok=True)
        if not Path(self.file_path).exists():
            with open(self.file_path, 'w', encoding='utf-8') as f:
                f.write('{}')

        self.file_handle = open(self.file_path, 'r+', encoding='utf-8')
        while True:
            try:
                if sys.platform == 'win32':
                    msvcrt.locking(self.file_handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    fcntl.flock(self.file_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except (IOError, OSError):
                time.sleep(0.1)
        return self.file_handle

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.file_handle.seek(0)
            if sys.platform == 'win32':
                msvcrt.locking(self.file_handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(self.file_handle.fileno(), fcntl.LOCK_UN)
        except (IOError, OSError):
            pass
        if self.file_handle:
            self.file_handle.close()


def _sync_history_json_if_enabled():
    if sql_store.LEGACY_SYNC_ENABLED:
        try:
            sql_store.history_export_to_json()
        except Exception as exc:
            logger.warning('Could not sync history.json from SQL: %s', exc)


def load_history():
    """Loads history from SQL storage."""
    _ensure_history_file()
    try:
        data = sql_store.history_get_full()
        if not isinstance(data, dict):
            return {'pending': {}, 'cached': {}}
        return {
            'pending': data.get('pending', {}) if isinstance(data.get('pending', {}), dict) else {},
            'cached': data.get('cached', {}) if isinstance(data.get('cached', {}), dict) else {},
        }
    except Exception:
        return {'pending': {}, 'cached': {}}


def save_history(data):
    """Saves full history structure into SQL storage."""
    _ensure_history_file()
    if not isinstance(data, dict):
        data = {'pending': {}, 'cached': {}}
    sql_store.history_replace(data)
    _sync_history_json_if_enabled()


def add_pending_matches(season, league_id, match_data_list):
    """
    Adds match IDs (and optional AH data) to pending list.
    SQL-backed, with optional legacy JSON sync.
    """
    _ensure_history_file()

    added_count = sql_store.history_add_pending_matches(str(season), str(league_id), match_data_list)

    if added_count > 0:
        _sync_history_json_if_enabled()

    return added_count


def move_to_cached(season, league_id, match_id):
    """
    Moves a match ID from pending to cached.
    SQL-backed, with optional legacy JSON sync.
    """
    _ensure_history_file()
    sql_store.history_move_to_cached(str(season), str(league_id), str(match_id))
    _sync_history_json_if_enabled()


def get_pending_matches():
    """Returns pending matches structure."""
    return load_history().get('pending', {})
