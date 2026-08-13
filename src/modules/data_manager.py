import datetime
import logging
import os
import threading
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

from . import sql_store
from .red_cards import normalize_red_card_stats_payload

LOGGER = logging.getLogger(__name__)

# Config
DATA_DIR = Path(__file__).resolve().parent.parent.parent / 'data'
DATA_DIR.mkdir(exist_ok=True)

# Legacy-compatible bucket names
PRECACHEO_BUCKET = 'data_precacheo.json'
PENDING_RESULTS_BUCKET = 'data_pending_results.json'

# Locks
_locks = {}
_global_lock = threading.Lock()
_write_lock = threading.Lock()
_precacheo_lock = threading.Lock()

_explorer_cache_lock = threading.Lock()
_explorer_cache: Dict[Tuple[Optional[str], Optional[int]], Tuple[float, List[Dict]]] = {}
_explorer_cache_ttl = max(0, int(os.getenv('EXPLORER_CACHE_TTL_SECONDS', '30')))
_EXPLORER_CACHE_MAX_ENTRIES = 3  # Limitar entradas para evitar OOM en Render (512 MB)


def _clear_explorer_cache() -> None:
    with _explorer_cache_lock:
        _explorer_cache.clear()


def get_bucket_name(ah_val):
    """
    Determines the filename bucket for a given AH value.
    Buckets:
    - 0: 0
    - 0.5: 0.25, 0.5, 0.75
    - -0.5: -0.25, -0.5, -0.75
    - 1.5: 1.0, 1.25, 1.5, 1.75
    - -1.5: -1.0, -1.25, -1.5, -1.75
    - 2_plus: >= 2.0
    - minus_2_plus: <= -2.0
    """
    if ah_val is None:
        return 'data_unknown.json'

    if str(ah_val) == 'N/A':
        return 'data_unknown.json'

    try:
        val = float(ah_val)
    except (TypeError, ValueError):
        return 'data_unknown.json'

    if val == 0:
        return 'data_ah_0.json'

    abs_val = abs(val)
    sign = 'minus_' if val < 0 else ''

    if 0.25 <= abs_val <= 0.75:
        return f'data_{sign}ah_0.5.json'

    if 1.0 <= abs_val <= 1.75:
        return f'data_{sign}ah_1.5.json'

    if abs_val >= 2.0:
        return f'data_{sign}ah_2_plus.json'

    return 'data_others.json'


def get_file_lock(filename):
    """Legacy helper kept for compatibility with old callers."""
    with _global_lock:
        if filename not in _locks:
            _locks[filename] = threading.Lock()
        return _locks[filename]


def _is_pending_score(score: Optional[str]) -> bool:
    if score is None:
        return True
    text = str(score).strip()
    return not text or '?' in text or text in {'??', '?-?', '?:?', '? : ?', '? - ?'}


def _sync_legacy_buckets(bucket_names: Iterable[str]) -> None:
    if not sql_store.LEGACY_SYNC_ENABLED:
        return

    for bucket in sorted({b for b in bucket_names if b}):
        try:
            sql_store.export_bucket_to_json(bucket)
        except Exception as exc:
            print(f'Error syncing legacy bucket {bucket}: {exc}')


def _persist_match(match_data: Dict, bucket_name: str, state: str) -> Set[str]:
    normalize_red_card_stats_payload(match_data)
    previous_bucket, _ = sql_store.upsert_match(match_data, bucket=bucket_name, state=state)
    _clear_explorer_cache()
    changed = {bucket_name}
    if previous_bucket and previous_bucket != bucket_name:
        changed.add(previous_bucket)
    return changed


def save_match(match_data):
    """
    Saves a single match to its appropriate SQL bucket/state.
    Also syncs legacy JSON files when DATA_LEGACY_SYNC is enabled.
    """
    ah = match_data.get('handicap')
    if ah is None:
        ah = match_data.get('main_match_odds', {}).get('ah_linea')

    score = match_data.get('score')
    if score is None:
        score = match_data.get('final_score')

    # Filter: AH 3 or -3
    if ah in [3, 3.0, '3', '3.0', -3, -3.0, '-3', '-3.0']:
        print(f"Skipping match {match_data.get('match_id')} with AH {ah}")
        return False

    # Filter: valid previous handicap history required
    prev_home = match_data.get('last_home_match') or {}
    prev_away = match_data.get('last_away_match') or {}
    ah_p_home = prev_home.get('handicap_line_raw')
    ah_p_away = prev_away.get('handicap_line_raw')

    invalid_chars = [None, '', '-', 'N/A', '??']
    if ah_p_home in invalid_chars or ah_p_away in invalid_chars:
        mid = match_data.get('match_id')
        print(f"Skipping match {mid}: Missing valid AH in prev matches ({ah_p_home} / {ah_p_away})")
        return False

    # Bucket + state based on result status
    if _is_pending_score(score):
        bucket_name = PENDING_RESULTS_BUCKET
        state = 'pending_results'
    else:
        bucket_name = get_bucket_name(ah)
        state = 'historical'

    with _write_lock:
        changed_buckets = _persist_match(match_data, bucket_name, state)
        _sync_legacy_buckets(changed_buckets)

    return True


def load_all_matches():
    """Loads matches from all SQL buckets/states."""
    return sql_store.fetch_all_matches()


def load_matches_by_bucket(ah_filter):
    """
    Loads matches from the specific bucket relevant to the filter.
    If ah_filter is 'all', loads everything.
    """
    if not ah_filter or ah_filter == 'all':
        return load_all_matches()

    bucket = get_bucket_name(ah_filter)
    return sql_store.fetch_matches(bucket=bucket)


def load_explorer_matches(ah_filter=None, scan_limit=None):
    """
    Explorer should run on finalized historical data only.
    Excludes precacheo/pending states to reduce noise and latency.
    """
    if ah_filter and ah_filter != 'all':
        cache_bucket = get_bucket_name(ah_filter)
    else:
        cache_bucket = None

    cache_limit = int(scan_limit) if isinstance(scan_limit, int) and scan_limit > 0 else None
    cache_key = (cache_bucket, cache_limit)

    if _explorer_cache_ttl > 0:
        with _explorer_cache_lock:
            cached = _explorer_cache.get(cache_key)
            if cached and (time.time() - cached[0]) <= _explorer_cache_ttl:
                return cached[1]

    if ah_filter and ah_filter != 'all':
        rows = sql_store.fetch_matches(
            bucket=cache_bucket,
            state='historical',
            limit=scan_limit,
            prefer_explorer_payload=True,
        )
    else:
        rows = sql_store.fetch_matches(
            state='historical',
            limit=scan_limit,
            prefer_explorer_payload=True,
        )

    # Fallback: si no hay filas con estado 'historical', consultar sin restricción de estado
    if not rows:
        rows = sql_store.fetch_matches(
            bucket=cache_bucket if (ah_filter and ah_filter != 'all') else None,
            limit=scan_limit,
            prefer_explorer_payload=True,
        )


    if _explorer_cache_ttl > 0:
        with _explorer_cache_lock:
            # Evictar la entrada más antigua si se excede el límite
            while len(_explorer_cache) >= _EXPLORER_CACHE_MAX_ENTRIES:
                oldest_key = min(_explorer_cache, key=lambda k: _explorer_cache[k][0])
                del _explorer_cache[oldest_key]
            _explorer_cache[cache_key] = (time.time(), rows)
    return rows


# --- Pre-Cacheo Functions ---
def save_precacheo_match(match_data):
    """Saves a match to pre-cacheo storage."""
    with _precacheo_lock:
        changed_buckets = _persist_match(match_data, PRECACHEO_BUCKET, 'precacheo')
        _sync_legacy_buckets(changed_buckets)
    # Historial universal de colocación: solo añade una fila cuando AH/O-U cambia.
    # El import local evita acoplar el arranque del almacén SQL al motor de aprendizaje.
    try:
        from . import league_evolution_learning
        league_evolution_learning.record_precache_snapshot(match_data)
    except Exception:
        LOGGER.exception("No se pudo registrar el snapshot de mercado de precacheo")
    return True


def load_precacheo_matches():
    """Loads all pre-cached matches."""
    return sql_store.fetch_matches(bucket=PRECACHEO_BUCKET)


def remove_from_precacheo(match_id):
    """Removes a match from pre-cacheo after it's finalized."""
    with _precacheo_lock:
        deleted = sql_store.delete_match(str(match_id), bucket=PRECACHEO_BUCKET)
        if deleted:
            _sync_legacy_buckets({PRECACHEO_BUCKET})
    return True


def parse_match_date(date_str):
    if not date_str or date_str == 'N/A':
        return None
    raw = str(date_str).strip()
    if 'T' in raw:
        try:
            iso_dt = datetime.datetime.fromisoformat(raw.replace('Z', '+00:00'))
            return iso_dt.replace(tzinfo=None)
        except Exception:
            pass
    formats = ['%m/%d/%Y', '%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d']
    date_part = raw.split(' ')[0]
    for fmt in formats:
        try:
            return datetime.datetime.strptime(date_part, fmt)
        except Exception:
            continue
    return None


def flag_stale_prev_matches(match_data):
    """
    Compara match_date con last_home_match.date y last_away_match.date.
    Calcula el gap en días.
    Retorna un diccionario con detalles del desfase temporal y si se considera obsoleto (> 60 días).
    """
    m_date_str = match_data.get('match_date') or match_data.get('date') or match_data.get('precacheo_date')
    m_date = parse_match_date(m_date_str)
    
    if not m_date:
        return {
            'home_gap_days': None,
            'away_gap_days': None,
            'max_gap_days': None,
            'is_stale': False
        }
        
    prev_home = match_data.get('last_home_match') or {}
    prev_away = match_data.get('last_away_match') or {}
    
    home_date_str = prev_home.get('date')
    away_date_str = prev_away.get('date')
    
    home_date = parse_match_date(home_date_str) if home_date_str else None
    away_date = parse_match_date(away_date_str) if away_date_str else None
    
    home_gap = None
    if home_date:
        home_gap = abs((m_date - home_date).days)
        
    away_gap = None
    if away_date:
        away_gap = abs((m_date - away_date).days)
        
    gaps = [g for g in [home_gap, away_gap] if g is not None]
    max_gap = max(gaps) if gaps else None
    
    is_stale = (max_gap > 60) if max_gap is not None else False
    
    return {
        'home_gap_days': home_gap,
        'away_gap_days': away_gap,
        'max_gap_days': max_gap,
        'is_stale': is_stale
    }


def clean_old_precacheo_matches(days_threshold=1, pending_days_threshold=2):
    """
    Removes old pre-cacheo matches.
    1. With result: remove if older than (today - days_threshold).
    2. Without result: keep up to pending_days_threshold days.
    """
    with _precacheo_lock:
        sources = (
            (PRECACHEO_BUCKET, load_precacheo_matches()),
            (PENDING_RESULTS_BUCKET, load_pending_results_matches()),
        )
        if not any(rows for _, rows in sources):
            return 0

        try:
            days_threshold = max(0, int(days_threshold))
        except Exception:
            days_threshold = 1
        try:
            pending_days_threshold = max(0, int(pending_days_threshold))
        except Exception:
            pending_days_threshold = 2

        now = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        threshold_date = now - datetime.timedelta(days=days_threshold)
        pending_threshold_date = now - datetime.timedelta(days=pending_days_threshold)

        to_remove: Set[Tuple[str, str]] = set()
        for source_bucket, rows in sources:
            for match in rows:
                m_date_str = match.get('match_date') or match.get('date') or match.get('precacheo_date')
                m_date = parse_match_date(m_date_str)

                if m_date is None:
                    continue

                score = match.get('score') or match.get('final_score')
                has_result = bool(score) and not _is_pending_score(str(score)) and (
                    ':' in str(score) or '-' in str(score)
                )
                match_id = match.get('match_id') or match.get('id')
                if match_id in (None, ''):
                    continue

                max_date = threshold_date if has_result else pending_threshold_date
                if m_date < max_date:
                    to_remove.add((source_bucket, str(match_id)))

        removed_count = 0
        changed_buckets: Set[str] = set()
        for source_bucket, mid in to_remove:
            if mid and sql_store.delete_match(mid, bucket=source_bucket):
                removed_count += 1
                changed_buckets.add(source_bucket)

        if removed_count > 0:
            _sync_legacy_buckets(changed_buckets)
            print(f'Limpieza de Precacheo/Pendientes: {removed_count} partidos antiguos eliminados.')

        return removed_count


def get_precacheo_match(match_id):
    """Gets a single match from pre-cacheo by ID."""
    return sql_store.get_match(str(match_id), bucket=PRECACHEO_BUCKET)


def finalize_precacheo_batch(match_ids):
    """
    Finalizes a batch of matches.
    Returns: (count_success, count_failed, errors_list)
    """
    success_count = 0
    errors = []

    with _precacheo_lock:
        precacheo_data = load_precacheo_matches()
        if not precacheo_data:
            return 0, len(match_ids), ['Precacheo file not found']

        precacheo_map = {str(m.get('match_id')): m for m in precacheo_data if m.get('match_id') is not None}

        matches_to_move: List[Tuple[Dict, str]] = []
        ids_to_remove: Set[str] = set()

        for mid in match_ids:
            mid_str = str(mid)
            match = precacheo_map.get(mid_str)
            if not match:
                errors.append(f'Match {mid} not found in precacheo')
                continue

            score = match.get('score') or match.get('final_score')
            if _is_pending_score(score):
                errors.append(f'Match {mid} has no result ({score})')
                continue

            ids_to_remove.add(mid_str)

            if not validate_explorer_match(match):
                errors.append(f"Match {match.get('match_id')} skipped (missing history/invalid data)")
                continue

            ah = match.get('handicap')
            if ah is None:
                ah = match.get('main_match_odds', {}).get('ah_linea')
            target_bucket = get_bucket_name(ah)
            matches_to_move.append((match, target_bucket))

        changed_buckets: Set[str] = {PRECACHEO_BUCKET}

        for match, target_bucket in matches_to_move:
            changed = _persist_match(match, target_bucket, 'historical')
            changed_buckets.update(changed)
            success_count += 1

        for mid in ids_to_remove:
            sql_store.delete_match(mid, bucket=PRECACHEO_BUCKET)

        _sync_legacy_buckets(changed_buckets)

    return success_count, len(match_ids) - success_count, errors


def validate_explorer_match(match_data):
    """
    Validates if a match is suitable for explorer (finalized data).
    """
    score = match_data.get('score') or match_data.get('final_score')
    if _is_pending_score(score):
        return False

    lhm = match_data.get('last_home_match')
    lam = match_data.get('last_away_match')

    if not isinstance(lhm, dict) or not lhm:
        return False
    if not isinstance(lam, dict) or not lam:
        return False

    return True


def clean_bucket(filename):
    """Cleans a single bucket of invalid matches."""
    data = sql_store.fetch_matches(bucket=filename)
    if not data:
        return 0, 0

    initial_len = len(data)
    to_remove = [str(m.get('match_id')) for m in data if not validate_explorer_match(m)]

    removed_count = 0
    for mid in to_remove:
        if mid and sql_store.delete_match(mid, bucket=filename):
            removed_count += 1

    if removed_count > 0:
        _clear_explorer_cache()
        _sync_legacy_buckets({filename})

    return removed_count, initial_len


def clean_all_buckets():
    """Runs cleaning on all explorer buckets."""
    stats = {}
    total_removed = 0

    all_files = sql_store.fetch_distinct_buckets()
    for bucket_name in all_files:
        if bucket_name in [PRECACHEO_BUCKET, PENDING_RESULTS_BUCKET]:
            continue
        if not bucket_name.startswith('data_'):
            continue

        rem, tot = clean_bucket(bucket_name)
        if rem > 0:
            stats[bucket_name] = f'Removed {rem}/{tot}'
            total_removed += rem

    return total_removed, stats


def delete_match_from_bucket(match_id, ah_val):
    """
    Removes a match from its corresponding bucket.
    """
    bucket_name = get_bucket_name(ah_val)
    existing = sql_store.get_match(str(match_id), bucket=bucket_name)
    if not existing:
        return False, 'Match not found in bucket'

    deleted = sql_store.delete_match(str(match_id), bucket=bucket_name)
    if not deleted:
        return False, 'Match not found in bucket'

    _clear_explorer_cache()
    _sync_legacy_buckets({bucket_name})
    return True, 'Match deleted successfully'


def load_pending_results_matches():
    """Returns matches currently pending final score."""
    return sql_store.fetch_matches(bucket=PENDING_RESULTS_BUCKET)


def remove_pending_match(match_id):
    """Removes a match from pending-results bucket."""
    deleted = sql_store.delete_match(str(match_id), bucket=PENDING_RESULTS_BUCKET)
    if deleted:
        _sync_legacy_buckets({PENDING_RESULTS_BUCKET})
    return deleted


def rebuild_legacy_json_files():
    """Rebuilds all legacy data_*.json files from SQL storage."""
    if not sql_store.LEGACY_SYNC_ENABLED:
        return []
    return sql_store.export_all_buckets_to_json()


def import_legacy_json_to_sql(reset_first=False):
    """Manual re-import of legacy JSON files into SQL."""
    sql_store.import_legacy_json_to_db(reset_first=reset_first)
    _clear_explorer_cache()
