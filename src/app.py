# app.py - Servidor web principal (Flask)
import sys
from pathlib import Path
_src_dir = Path(__file__).resolve().parent
_root_dir = _src_dir.parent
sys.path.insert(0, str(_src_dir))
sys.path.insert(0, str(_root_dir))  # For scripts/ imports
from flask import Flask, render_template, abort, request, redirect, url_for, send_from_directory
import asyncio

from bs4 import BeautifulSoup
import datetime
from zoneinfo import ZoneInfo
import re
import math
import threading
import json
import time
import logging
import uuid
from pathlib import Path
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
import csv
import os
import gzip

# Desactivar advertencias de SSL inseguro para verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from dotenv import load_dotenv
load_dotenv()  # Carga variables desde .env (local) o usa env vars de sistema (Render)
import concurrent.futures

_json_save_lock = threading.Lock()

from modules import league_scraper
from modules import history_manager

# ¡Importante! Importa tu nuevo módulo de scraping
from modules.estudio_scraper import (
    analizar_partido_completo, 
    analizar_contexto_previo_rapido,
    get_match_progression_stats_data,
    _df_to_rows,
    format_ah_as_decimal_string_of,
    parse_ah_to_number_of,
    check_handicap_cover,
    generar_analisis_completo_mercado
)

from modules.pattern_search import find_similar_patterns, explore_matches
from modules.bookie_decoder import analyze_match_bookie_logic
from modules.scah_analyzer import analizar_partido_scah
from modules.handicap_similar_analyzer import analizar_partido_handicap_similar
from modules import winner_tracker, lexington_pattern, favorite_process_pattern, local_rerate_pattern, col3_indirect_pattern, quarter_away_pattern, last_general_context, rival_handicap_samples, housemind_ou, league_handicap_scraper, league_extraction_registry, uefa_qualifying, sofascore_context, league_market_tracker, league_evolution_learning
from flask import jsonify # Asegúrate de que jsonify está importado


app = Flask(__name__)

_league_market_jobs = {}
_league_market_jobs_lock = threading.Lock()

# --- CONFIGURACIÓN CSV ---
STUDIED_MATCHES_DIR = Path(__file__).resolve().parent.parent / 'studied_matches'
STUDIED_MATCHES_CSV = STUDIED_MATCHES_DIR / 'history.csv'
STUDIED_MATCHES_JSON = STUDIED_MATCHES_DIR / 'history.json'

def save_match_to_csv(match_data):
    """Guarda los datos básicos del partido en un CSV."""
    try:
        STUDIED_MATCHES_DIR.mkdir(parents=True, exist_ok=True)
        
        file_exists = STUDIED_MATCHES_CSV.exists()
        
        # Definir columnas
        fieldnames = [
            'timestamp', 'match_id', 'home_team', 'away_team', 
            'score', 'time', 'competition', 'ah_line', 'ou_line',
            'last_home_score', 'last_home_ah',
            'last_away_score', 'last_away_ah',
            'comp_home_rival', 'comp_home_score', 'comp_home_ah', 'comp_home_localia',
            'comp_away_rival', 'comp_away_score', 'comp_away_ah', 'comp_away_localia'
        ]
        
        # Helper to safely get nested dict values
        def get_nested(d, *keys):
            for k in keys:
                if not isinstance(d, dict): return ''
                d = d.get(k, {})
            return d if isinstance(d, str) or isinstance(d, (int, float)) else ''

        row = {
            'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'match_id': match_data.get('match_id', ''),
            'home_team': match_data.get('home_name', ''),
            'away_team': match_data.get('away_name', ''),
            'score': match_data.get('final_score', ''),
            'time': match_data.get('time', ''),
            'competition': match_data.get('league_name', ''),
            'ah_line': get_nested(match_data, 'main_match_odds', 'ah_linea'),
            'ou_line': get_nested(match_data, 'main_match_odds', 'goals_linea'),
            
            # Historial Inmediato
            'last_home_score': get_nested(match_data, 'last_home_match', 'score'),
            'last_home_ah': get_nested(match_data, 'last_home_match', 'handicap_line_raw'),
            'last_away_score': get_nested(match_data, 'last_away_match', 'score'),
            'last_away_ah': get_nested(match_data, 'last_away_match', 'handicap_line_raw'),
            
            # Comparativas Indirectas (H2H Rivales Col3)
            # Nota: Ahora están dentro de 'comparativas_indirectas' -> 'left' / 'right'
            'comp_home_rival': get_nested(match_data, 'comparativas_indirectas', 'left', 'rival_name'),
            'comp_home_score': get_nested(match_data, 'comparativas_indirectas', 'left', 'score'),
            'comp_home_ah': get_nested(match_data, 'comparativas_indirectas', 'left', 'ah_line'),
            'comp_home_localia': get_nested(match_data, 'comparativas_indirectas', 'left', 'localia'),
            
            'comp_away_rival': get_nested(match_data, 'comparativas_indirectas', 'right', 'rival_name'),
            'comp_away_score': get_nested(match_data, 'comparativas_indirectas', 'right', 'score'),
            'comp_away_ah': get_nested(match_data, 'comparativas_indirectas', 'right', 'ah_line'),
            'comp_away_localia': get_nested(match_data, 'comparativas_indirectas', 'right', 'localia'),
        }

        # Verificar duplicados
        if file_exists:
            try:
                with open(STUDIED_MATCHES_CSV, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for r in reader:
                        if r.get('match_id') == str(row['match_id']):
                            # Ya existe, no guardar
                            return
            except Exception:
                pass

        with open(STUDIED_MATCHES_CSV, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)
            
        print(f"Partido {match_data.get('match_id')} guardado en CSV.")
    except Exception as e:
        print(f"Error guardando en CSV: {e}")

from modules import data_manager
from modules import sql_store
from modules import pending_results_query
from scripts.finished_result_validation import validate_finished_result

def save_match_to_json(match_data):
    """Guarda los datos del partido usando el nuevo sistema de buckets."""
    try:
        match_data['cached_at'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        saved = data_manager.save_match(match_data)
        if saved:
            print(f"Partido {match_data.get('match_id')} guardado en bucket.")
        else:
            print(f"Partido {match_data.get('match_id')} ignorado (filtro).")
        return bool(saved)
    except Exception as e:
        print(f"Error guardando en JSON: {e}")
        return False

def save_match_to_json_thread_safe(match_data):
    # data_manager is already thread-safe per file
    save_match_to_json(match_data)


# --- Mantén tu lógica para la página principal ---
URL_NOWGOAL = "https://live20.nowgoal25.com/"

REQUEST_TIMEOUT_SECONDS = 12
_REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
    "Referer": URL_NOWGOAL,
}

_requests_session = None
_requests_session_lock = threading.Lock()
_requests_fetch_lock = threading.Lock()

_EMPTY_DATA_TEMPLATE = {"upcoming_matches": [], "finished_matches": []}
UTC_TZ = datetime.timezone.utc
SPAIN_TZ = ZoneInfo("Europe/Madrid")
MAIN_PAGE_CACHE_KEY = "app_main_page_cache_v1"
_DATA_FILE_CANDIDATES = [
    Path(__file__).resolve().parent / 'data.json',
    Path(__file__).resolve().parent.parent / 'data.json',
]
_data_file_lock = threading.Lock()
_precache_legacy_lock = threading.Lock()
_empty_snapshot_refresh_lock = threading.Lock()
_precacheo_cleanup_lock = threading.Lock()
_last_empty_snapshot_refresh_ts = 0.0
_last_precacheo_cleanup_ts = 0.0
_EMPTY_SNAPSHOT_REFRESH_COOLDOWN_SECONDS = max(
    30,
    int(os.getenv('EMPTY_SNAPSHOT_REFRESH_COOLDOWN_SECONDS', '300'))
)
_PRECACHEO_AUTO_CLEAN_INTERVAL_SECONDS = max(
    30,
    int(os.getenv('PRECACHEO_AUTO_CLEAN_INTERVAL_SECONDS', '300'))
)
_PRECACHEO_PENDING_MAX_AGE_DAYS = max(
    0,
    int(os.getenv('PRECACHEO_PENDING_MAX_AGE_DAYS', '2'))
)
_PRECACHEO_BUCKET_NAME = data_manager.PRECACHEO_BUCKET
_PRECACHEO_FILE_CANDIDATES = [
    Path(__file__).resolve().parent.parent / 'data' / _PRECACHEO_BUCKET_NAME,
    Path(__file__).resolve().parent / 'data' / _PRECACHEO_BUCKET_NAME,
    Path(__file__).resolve().parent.parent / 'data' / f'{_PRECACHEO_BUCKET_NAME}.bak',
    Path(__file__).resolve().parent / 'data' / f'{_PRECACHEO_BUCKET_NAME}.bak',
]
_precacheo_legacy_cache = None
_picks_runtime_lock = threading.Lock()
_cached_specialist_validator = None
_cached_specialist_validator_failed = False
_cached_v2_loader = None
_cached_v2_loader_loaded_at = 0.0
_cached_v2_loader_failed = False
HTML_OFFLINE_DIR = Path(__file__).resolve().parent.parent / 'html_offline'
HTML_OFFLINE_PAGE_NAME = 'offline_precacheo_simple.html'
HTML_OFFLINE_SNAPSHOT_FILE = HTML_OFFLINE_DIR / 'precacheo_snapshot.json'


def _env_flag(name, default=False):
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() not in {'', '0', 'false', 'no', 'off'}


def _env_int(name, default):
    raw = os.getenv(name, str(default))
    try:
        return int(raw)
    except Exception:
        return int(default)


def _is_app_precacheo_only():
    return _env_flag('APP_PRECACHEO_ONLY', default=False)


_PRECACHEO_ONLY_ALLOWED_EXACT_PATHS = {
    '/',
    '/favicon.ico',
    '/api/ai_prediction',
    '/precacheo-sw.js',
}
_PRECACHEO_ONLY_ALLOWED_PREFIXES = (
    '/precacheo',
    '/api/precacheo',
    '/static/',
    '/api/export_prompt',
    '/api/decode_match',
    '/api/analyze_scah',
    '/api/ai_prediction',
    '/api/matches',
    '/api/ligas_',
    '/api/finished_matches_list',
)


@app.before_request
def _enforce_precacheo_only_mode():
    """
    Render-only deployment option: expose only /precacheo UI + precacheo APIs.
    """
    if not _is_app_precacheo_only():
        return None

    path = request.path or '/'
    if path in _PRECACHEO_ONLY_ALLOWED_EXACT_PATHS:
        return None

    if any(path.startswith(prefix) for prefix in _PRECACHEO_ONLY_ALLOWED_PREFIXES):
        return None

    if path.startswith('/api/'):
        return jsonify({'error': 'Endpoint disabled in APP_PRECACHEO_ONLY mode'}), 404

    return redirect(url_for('precacheo'))


def _normalize_main_page_cache(data):
    if not isinstance(data, dict):
        return {key: [] for key in _EMPTY_DATA_TEMPLATE}

    normalized = {}
    for key in _EMPTY_DATA_TEMPLATE:
        value = data.get(key, [])
        if isinstance(value, list):
            normalized[key] = [item for item in value if isinstance(item, dict)]
        else:
            normalized[key] = []
    return normalized


def _import_legacy_main_page_cache_once():
    done_key = "app_main_page_cache_legacy_import_done"
    already_done = sql_store.get_json_state(done_key, default=False)
    if already_done:
        return

    for candidate in _DATA_FILE_CANDIDATES:
        if not candidate.exists():
            continue
        try:
            with candidate.open('r', encoding='utf-8') as fh:
                legacy_payload = json.load(fh)
            sql_store.set_json_state(MAIN_PAGE_CACHE_KEY, _normalize_main_page_cache(legacy_payload))
            print(f"Legacy main page cache imported from {candidate}")
            break
        except Exception as exc:
            print(f"Warning importing legacy main page cache ({candidate}): {exc}")
            continue

    sql_store.set_json_state(done_key, True)


def load_data_from_file():
    """Carga los datos de partidos desde SQL (cache principal de listas)."""
    with _data_file_lock:
        data = sql_store.get_json_state(MAIN_PAGE_CACHE_KEY, default=None)
        if data is None:
            _import_legacy_main_page_cache_once()
            data = sql_store.get_json_state(MAIN_PAGE_CACHE_KEY, default=_EMPTY_DATA_TEMPLATE)
        return _normalize_main_page_cache(data)


def save_data_snapshot(data):
    """Guarda snapshot de partidos (upcoming/finished) en SQL."""
    with _data_file_lock:
        sql_store.set_json_state(MAIN_PAGE_CACHE_KEY, _normalize_main_page_cache(data))


def _load_precacheo_legacy_rows(limit=None):
    global _precacheo_legacy_cache

    with _precache_legacy_lock:
        if _precacheo_legacy_cache is None:
            loaded = []
            for candidate in _PRECACHEO_FILE_CANDIDATES:
                if not candidate.exists():
                    continue
                try:
                    with candidate.open('r', encoding='utf-8') as fh:
                        payload = json.load(fh)

                    candidate_rows = []
                    if isinstance(payload, list):
                        candidate_rows = [item for item in payload if isinstance(item, dict)]
                    elif isinstance(payload, dict):
                        rows = payload.get('matches')
                        if isinstance(rows, list):
                            candidate_rows = [item for item in rows if isinstance(item, dict)]

                    if candidate_rows:
                        loaded = candidate_rows
                        print(f"Legacy precache loaded from {candidate} ({len(loaded)} rows)")
                        break
                except Exception as exc:
                    print(f"Warning loading legacy precache file ({candidate}): {exc}")
                    continue
            _precacheo_legacy_cache = loaded

        rows = _precacheo_legacy_cache or []

    if isinstance(limit, int) and limit > 0:
        return rows[:limit]
    return rows


def _snapshot_has_matches(snapshot):
    if not isinstance(snapshot, dict):
        return False
    upcoming = snapshot.get('upcoming_matches', [])
    finished = snapshot.get('finished_matches', [])
    return bool(upcoming) or bool(finished)


async def _refresh_main_page_snapshot_if_empty():
    """
    Evita pantallas vacías en deploys fresh/free.
    Si el snapshot está vacío, intenta una recarga directa desde Nowgoal y guarda en SQL.
    """
    global _last_empty_snapshot_refresh_ts

    snapshot = load_data_from_file()
    if _snapshot_has_matches(snapshot):
        return

    now_ts = time.time()
    with _empty_snapshot_refresh_lock:
        if (
            _last_empty_snapshot_refresh_ts > 0
            and (now_ts - _last_empty_snapshot_refresh_ts) < _EMPTY_SNAPSHOT_REFRESH_COOLDOWN_SECONDS
        ):
            return
        _last_empty_snapshot_refresh_ts = now_ts

    try:
        upcoming, finished = await asyncio.gather(
            scrape_main_page_matches_async_direct(limit=1500),
            scrape_main_page_finished_matches_async_direct(limit=1000),
        )
        if upcoming or finished:
            save_data_snapshot({
                'upcoming_matches': upcoming,
                'finished_matches': finished,
            })
            print(
                f"Snapshot auto-refresh completado: "
                f"{len(upcoming)} próximos / {len(finished)} finalizados."
            )
    except Exception as exc:
        print(f"Snapshot auto-refresh falló: {exc}")


def _maybe_cleanup_precacheo_stale(force=False):
    """Limpia precacheo viejo para evitar crecimiento descontrolado del cache."""
    global _last_precacheo_cleanup_ts

    now_ts = time.time()
    with _precacheo_cleanup_lock:
        if (
            not force
            and _last_precacheo_cleanup_ts > 0
            and (now_ts - _last_precacheo_cleanup_ts) < _PRECACHEO_AUTO_CLEAN_INTERVAL_SECONDS
        ):
            return 0
        _last_precacheo_cleanup_ts = now_ts

    try:
        removed = data_manager.clean_old_precacheo_matches(
            days_threshold=1,
            pending_days_threshold=_PRECACHEO_PENDING_MAX_AGE_DAYS,
        )
        if removed > 0:
            print(
                f"🧹 Precacheo cleanup: {removed} eliminados "
                f"(pendientes>{_PRECACHEO_PENDING_MAX_AGE_DAYS}d)."
            )
        return removed
    except Exception as exc:
        print(f"⚠️ Error en precacheo cleanup: {exc}")
        return 0


def _parse_time_obj(value):
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            return value.astimezone(UTC_TZ).replace(tzinfo=None)
        return value
    if isinstance(value, str):
        try:
            parsed = datetime.datetime.fromisoformat(value)
            if parsed.tzinfo is not None:
                return parsed.astimezone(UTC_TZ).replace(tzinfo=None)
            return parsed
        except ValueError:
            try:
                return datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                return None
    return None


def _parse_start_time_to_utc(value):
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=SPAIN_TZ).astimezone(UTC_TZ).replace(tzinfo=None)
        return value.astimezone(UTC_TZ).replace(tzinfo=None)
    if isinstance(value, str):
        try:
            parsed = datetime.datetime.fromisoformat(value)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=SPAIN_TZ).astimezone(UTC_TZ).replace(tzinfo=None)
        return parsed.astimezone(UTC_TZ).replace(tzinfo=None)
    return None


def _ensure_time_string(entry, parsed_time):
    if entry.get('time') or not parsed_time:
        return
    spain_time = parsed_time.replace(tzinfo=UTC_TZ).astimezone(SPAIN_TZ)
    entry['time'] = spain_time.strftime('%d/%m %H:%M')


def _build_handicap_filter_predicate(handicap_filter):
    if not handicap_filter:
        return None
    try:
        target_bucket = normalize_handicap_to_half_bucket_str(handicap_filter)
        if target_bucket is None:
            return None
        target_float = float(target_bucket)
    except Exception:
        return None

    use_range = abs(target_float) >= 2.0 and target_float != 0.0

    def predicate(raw_value):
        hv = normalize_handicap_to_half_bucket_str(raw_value or '')
        if hv is None:
            return False
        if not use_range:
            return hv == target_bucket
        hv_float = float(hv)
        if target_float > 0:
            return hv_float > 0 and hv_float >= target_float
        return hv_float < 0 and hv_float <= target_float

    return predicate


def _normalize_goal_line_option_str(value):
    try:
        parsed = _parse_handicap_to_float(value)
    except Exception:
        parsed = None
    if parsed is None:
        return None
    text = f"{parsed:.2f}"
    if '.' in text:
        text = text.rstrip('0').rstrip('.')
    return text


def _build_goal_line_filter_predicate(goal_line_filter):
    if not goal_line_filter:
        return None
    try:
        target_value = _parse_handicap_to_float(goal_line_filter)
    except Exception:
        target_value = None
    if target_value is None:
        return None
    use_range = target_value >= 4.0

    def predicate(raw_value):
        try:
            current_value = _parse_handicap_to_float(raw_value or '')
        except Exception:
            current_value = None
        if current_value is None:
            return False
        if not use_range:
            return abs(current_value - target_value) < 1e-6
        return current_value >= target_value

    return predicate


def _build_handicap_options_from_lists(match_lists):
    values = set()
    for dataset in match_lists:
        for entry in dataset or []:
            if not isinstance(entry, dict):
                continue
            normalized = normalize_handicap_to_half_bucket_str(entry.get('handicap'))
            if normalized is not None:
                values.add(normalized)
    try:
        return sorted(values, key=lambda x: float(x))
    except ValueError:
        return sorted(values)


def _build_goal_line_options_from_lists(match_lists):
    values = set()
    for dataset in match_lists:
        for entry in dataset or []:
            if not isinstance(entry, dict):
                continue
            raw_value = entry.get('goal_line') or entry.get('goal_line_alt') or entry.get('goal_line_decimal')
            normalized = _normalize_goal_line_option_str(raw_value)
            if normalized is not None:
                values.add(normalized)
    try:
        return sorted(values, key=lambda x: float(x))
    except ValueError:
        return sorted(values)


def _filter_and_slice_matches(section, limit=None, offset=0, handicap_filter=None, goal_line_filter=None, sort_desc=False, min_time=None):
    data = load_data_from_file()
    matches = data.get(section, [])
    prepared = []
    now_utc = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    for original in matches:
        entry = dict(original)
        parsed_time = _parse_time_obj(entry.get('time_obj'))
        if parsed_time is None:
            parsed_time = _parse_start_time_to_utc(entry.get('start_time'))

        # La caché puede quedarse obsoleta; en "upcoming" excluimos pasados y sin hora válida.
        if section == 'upcoming_matches':
            if parsed_time is None:
                continue
            if parsed_time < now_utc:
                continue

        entry['_sort_time'] = parsed_time
        _ensure_time_string(entry, parsed_time)

        if min_time and parsed_time and parsed_time < min_time:
            continue

        prepared.append(entry)

    handicap_predicate = _build_handicap_filter_predicate(handicap_filter)
    if handicap_predicate:
        filtered = []
        for entry in prepared:
            if handicap_predicate(entry.get('handicap', '')):
                filtered.append(entry)
        prepared = filtered

    goal_predicate = _build_goal_line_filter_predicate(goal_line_filter)
    if goal_predicate:
        filtered = []
        for entry in prepared:
            if goal_predicate(entry.get('goal_line', '')):
                filtered.append(entry)
        prepared = filtered

    with_time = [item for item in prepared if isinstance(item.get('_sort_time'), datetime.datetime)]
    without_time = [item for item in prepared if not isinstance(item.get('_sort_time'), datetime.datetime)]
    with_time.sort(key=lambda item: (item['_sort_time'], str(item.get('id', ''))), reverse=sort_desc)
    without_time.sort(key=lambda item: str(item.get('id', '')))
    prepared = with_time + without_time

    offset = max(int(offset or 0), 0)
    if offset:
        if offset >= len(prepared):
            prepared = []
        else:
            prepared = prepared[offset:]

    if limit is not None:
        try:
            limit_val = int(limit)
        except (TypeError, ValueError):
            limit_val = None
        if limit_val is not None and limit_val >= 0:
            prepared = prepared[:limit_val]

    for entry in prepared:
        # Add start_time before removing _sort_time
        parsed_time = entry.get('_sort_time')
        if isinstance(parsed_time, datetime.datetime):
            spain_time = parsed_time.replace(tzinfo=UTC_TZ).astimezone(SPAIN_TZ)
            entry['start_time'] = spain_time.isoformat()

        entry.pop('_sort_time', None)
    return prepared


def _find_match_basic_data(match_id: str):
    if not match_id:
        return None, None
    data = load_data_from_file()
    for section in ('upcoming_matches', 'finished_matches'):
        for entry in data.get(section, []):
            if str(entry.get('id')) == str(match_id):
                return entry, section
    return None, None


def _get_preview_cache_dir():
    static_root_value = app.static_folder
    if not static_root_value:
        static_root_value = Path(__file__).resolve().parent / 'static'
    static_root = Path(static_root_value).resolve()
    return static_root / 'cached_previews'


def _preview_cache_key(match_id: str) -> str:
    return f'app_preview_cache_v1:{match_id}'


def load_preview_from_cache(match_id: str):
    payload = sql_store.get_json_state(_preview_cache_key(str(match_id)), default=None)
    if isinstance(payload, dict):
        return payload

    # Legacy one-time fallback from filesystem cache
    cache_path = _get_preview_cache_dir() / f'{match_id}.json'
    if cache_path.exists():
        try:
            with cache_path.open('r', encoding='utf-8') as fh:
                cached_data = json.load(fh)
                if isinstance(cached_data, dict):
                    sql_store.set_json_state(_preview_cache_key(str(match_id)), cached_data)
                    return cached_data
        except (json.JSONDecodeError, OSError) as exc:
            print(f"Error al leer cache de analisis {cache_path}: {exc}")
    return None


def save_preview_to_cache(match_id: str, payload: dict):
    try:
        sql_store.set_json_state(_preview_cache_key(str(match_id)), payload)
    except Exception as exc:
        print(f"Error al escribir cache de analisis para {match_id}: {exc}")


def _build_nowgoal_url(path: str | None = None) -> str:
    if not path:
        return URL_NOWGOAL
    base = URL_NOWGOAL.rstrip('/')
    suffix = path.lstrip('/')
    return f"{base}/{suffix}"


def _get_shared_requests_session():
    global _requests_session
    with _requests_session_lock:
        if _requests_session is None:
            session = requests.Session()
            retries = Retry(total=3, backoff_factor=0.4, status_forcelist=[500, 502, 503, 504])
            adapter = HTTPAdapter(max_retries=retries)
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            session.headers.update(_REQUEST_HEADERS)
            _requests_session = session
        return _requests_session


def _fetch_nowgoal_html_sync(url: str) -> str | None:
    session = _get_shared_requests_session()
    try:
        with _requests_fetch_lock:
            response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS, verify=False)
        response.raise_for_status()
        return response.text
    except Exception as exc:
        print(f"Error al obtener {url} con requests: {exc}")
        return None


async def _fetch_nowgoal_html(path: str | None = None, filter_state: int | None = None, requests_first: bool = True) -> str | None:
    target_url = _build_nowgoal_url(path)
    html_content = None

    if requests_first:
        try:
            html = _fetch_nowgoal_html_sync(target_url)
            if html:
                return html
        except Exception as e:
            print(f"Error en fetch sync: {e}")
    
    return None

def _parse_number_clean(s):
    if s is None:
        return None
    txt = str(s).strip()
    txt = txt.replace('−', '-')  # unicode minus
    txt = txt.replace(',', '.')
    txt = txt.replace('+', '')
    txt = txt.replace(' ', '')
    m = re.search(r"^[+-]?\d+(?:\.\d+)?$", txt)
    if m:
        try:
            return float(m.group(0))
        except ValueError:
            return None
    return None

def _parse_number(s: str):
    if s is None:
        return None
    # Normaliza separadores y signos
    txt = str(s).strip()
    txt = txt.replace('−', '-')  # minus unicode
    txt = txt.replace(',', '.')
    txt = txt.replace(' ', '')
    # Coincide con un número decimal con signo
    m = re.search(r"^[+-]?\d+(?:\.\d+)?$", txt)
    if m:
        try:
            return float(m.group(0))
        except ValueError:
            return None
    return None

def _parse_handicap_to_float(text: str):
    if text is None:
        return None
    t = str(text).strip()
    if '/' in t:
        parts = [p for p in re.split(r"/", t) if p]
        nums = []
        for p in parts:
            v = _parse_number_clean(p)
            if v is None:
                return None
            nums.append(v)
        if not nums:
            return None
        return sum(nums) / len(nums)
    # Si viene como cadena normal (ej. "+0.25" o "-0,75")
    return _parse_number_clean(t.replace('+', ''))

def _bucket_to_half(value: float) -> float:
    if value is None:
        return None
    if value == 0:
        return 0.0
    sign = -1.0 if value < 0 else 1.0
    av = abs(value)
    base = math.floor(av + 1e-9)
    frac = av - base
    # Mapea 0.25/0.75/0.5 a .5, 0.0 queda .0
    def close(a, b):
        return abs(a - b) < 1e-6
    if close(frac, 0.0):
        bucket = float(base)
    elif close(frac, 0.5) or close(frac, 0.25) or close(frac, 0.75):
        bucket = base + 0.5
    else:
        # fallback: redondeo al múltiplo de 0.5 más cercano
        bucket = round(av * 2) / 2.0
        # si cae justo en entero, desplazar a .5 para respetar la preferencia de .25/.75 → .5
        f = bucket - math.floor(bucket)
        if close(f, 0.0) and (abs(av - (math.floor(bucket) + 0.25)) < 0.26 or abs(av - (math.floor(bucket) + 0.75)) < 0.26):
            bucket = math.floor(bucket) + 0.5
    return sign * bucket

def normalize_handicap_to_half_bucket_str(text: str):
    v = _parse_handicap_to_float(text)
    if v is None:
        return None
    b = _bucket_to_half(v)
    if b is None:
        return None
    # Formato con un decimal
    return f"{b:.1f}"

def parse_main_page_matches(html_content, limit=20, offset=0, handicap_filter=None, goal_line_filter=None):
    soup = BeautifulSoup(html_content, 'html.parser')
    # Iterate ALL rows to catch League Titles
    all_rows = soup.find_all('tr')
    upcoming_matches = []
    now_utc = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    
    current_league_name = "Unknown League"

    for row in all_rows:
        # Check for League Header
        # <tr class="Leaguestitle"> 
        # or similar. Usually contains a td with class "lName" or just text.
        if 'Leaguestitle' in row.get('class', []):
            league_a = row.find('a', class_='lName')
            if league_a:
                current_league_name = league_a.text.strip()
            else:
                # Fallback: check text content of first cell
                cells = row.find_all('td')
                if cells and len(cells) > 0:
                     current_league_name = cells[0].get_text(strip=True)
            continue
            
        match_id = row.get('id', '').replace('tr1_', '')
        if not match_id or not row.get('id', '').startswith('tr1_'):
            continue

        time_cell = row.find('td', {'name': 'timeData'})
        if not time_cell or not time_cell.has_attr('data-t'): continue
        
        try:
            match_time = datetime.datetime.strptime(time_cell['data-t'], '%Y-%m-%d %H:%M:%S')
        except (ValueError, IndexError):
            continue

        if match_time < now_utc: continue

        home_team_tag = row.find('a', {'id': f'team1_{match_id}'})
        away_team_tag = row.find('a', {'id': f'team2_{match_id}'})
        
        # Extract Rank if present (e.g. "Team [1]")
        def extract_name_rank(tag):
            if not tag: return "N/A", None
            raw = tag.text.strip()
            # Regex for [12] or (12) at end
            match = re.search(r'^(.*?)\s*[\[\(](\d+)[\]\)]$', raw)
            if match:
                return match.group(1).strip(), match.group(2)
            # Try finding a sibling span with rank
            # Parent td
            parent = tag.parent
            if parent:
                # Look for span with class 'lp' (League Position)
                lp = parent.find('span', class_='lp')
                if lp:
                    return raw, lp.get_text(strip=True)
            return raw, None

        home_team, home_rank = extract_name_rank(home_team_tag)
        away_team, away_rank = extract_name_rank(away_team_tag)

        odds_data = row.get('odds', '').split(',')
        handicap = odds_data[2] if len(odds_data) > 2 else "N/A"
        goal_line = odds_data[10] if len(odds_data) > 10 else "N/A"

        if handicap == "N/A":
            continue

        upcoming_matches.append({
            "id": match_id,
            "time_obj": match_time,
            "home_team": home_team,
            "away_team": away_team,
            "home_rank": home_rank,
            "away_rank": away_rank,
            "league": current_league_name,
            "handicap": handicap,
            "goal_line": goal_line
        })

    handicap_predicate = _build_handicap_filter_predicate(handicap_filter)
    if handicap_predicate:
        filtered = []
        for m in upcoming_matches:
            if handicap_predicate(m.get('handicap', '')):
                filtered.append(m)
        upcoming_matches = filtered

    goal_predicate = _build_goal_line_filter_predicate(goal_line_filter)
    if goal_predicate:
        filtered = []
        for m in upcoming_matches:
            if goal_predicate(m.get('goal_line', '')):
                filtered.append(m)
        upcoming_matches = filtered

    upcoming_matches.sort(key=lambda x: x['time_obj'])
    
    paginated_matches = upcoming_matches[offset:offset+limit]

    for match in paginated_matches:
        if 'time_obj' in match:
            spain_time = match['time_obj'].replace(tzinfo=UTC_TZ).astimezone(SPAIN_TZ)
            match['time'] = spain_time.strftime('%H:%M')
            match['start_time'] = spain_time.isoformat()
            del match['time_obj']
        elif '_sort_time' in match:
             spain_time = match['_sort_time'].replace(tzinfo=UTC_TZ).astimezone(SPAIN_TZ)
             match['time'] = spain_time.strftime('%H:%M')
             match['start_time'] = spain_time.isoformat()

    return paginated_matches

def parse_main_page_finished_matches(html_content, limit=20, offset=0, handicap_filter=None, goal_line_filter=None):
    soup = BeautifulSoup(html_content, 'html.parser')
    match_rows = soup.find_all('tr', id=lambda x: x and x.startswith('tr1_'))
    finished_matches = []
    for row in match_rows:
        match_id = row.get('id', '').replace('tr1_', '')
        if not match_id: continue

        state = row.get('state')
        if state is not None and state != "-1":
            continue

        cells = row.find_all('td')
        if len(cells) < 8: continue

        home_team_tag = row.find('a', {'id': f'team1_{match_id}'})
        away_team_tag = row.find('a', {'id': f'team2_{match_id}'})
        
        score_cell = cells[6]
        score_text = "N/A"
        if score_cell:
            b_tag = score_cell.find('b')
            if b_tag:
                score_text = b_tag.text.strip()
            else:
                score_text = score_cell.get_text(strip=True)

        odds_data = row.get('odds', '').split(',')
        handicap_raw = odds_data[2] if len(odds_data) > 2 else "N/A"
        goal_line = odds_data[10].strip() if len(odds_data) > 10 else "N/A"

        # Doble verificación: se mantiene el flujo normal cuando existe AH.
        # Si falta, la fila solo pasa cuando contiene un marcador final estricto;
        # el hándicap queda como N/A y nunca se inventa un valor de mercado.
        verified_result = validate_finished_result(score_text, handicap_raw)
        if not verified_result:
            continue

        time_cell = row.find('td', {'name': 'timeData'})
        match_time = datetime.datetime.now()
        if time_cell and time_cell.has_attr('data-t'):
            try:
                match_time = datetime.datetime.strptime(time_cell['data-t'], '%Y-%m-%d %H:%M:%S')
            except (ValueError, IndexError):
                continue
        
        finished_matches.append({
            "id": match_id,
            "time_obj": match_time,
            "home_team": home_team_tag.text.strip() if home_team_tag else "N/A",
            "away_team": away_team_tag.text.strip() if away_team_tag else "N/A",
            "score": verified_result["score"],
            "handicap": verified_result["handicap"],
            "goal_line": goal_line,
            "result_only": verified_result["result_only"],
        })

    handicap_predicate = _build_handicap_filter_predicate(handicap_filter)
    if handicap_predicate:
        filtered = []
        for m in finished_matches:
            if handicap_predicate(m.get('handicap', '')):
                filtered.append(m)
        finished_matches = filtered

    goal_predicate = _build_goal_line_filter_predicate(goal_line_filter)
    if goal_predicate:
        filtered = []
        for m in finished_matches:
            if goal_predicate(m.get('goal_line', '')):
                filtered.append(m)
        finished_matches = filtered

    finished_matches.sort(key=lambda x: x['time_obj'], reverse=True)
    
    paginated_matches = finished_matches[offset:offset+limit]

    for match in paginated_matches:
        # Existing logic added +2 hours for finished matches? 
        # Line 687 in original: match['time'] = (match['time_obj'] + datetime.timedelta(hours=2)).strftime('%d/%m %H:%M')
        # Let's keep that logic
        spain_time = match['time_obj'].replace(tzinfo=UTC_TZ).astimezone(SPAIN_TZ)
        match['time'] = spain_time.strftime('%d/%m %H:%M')
        match['start_time'] = spain_time.isoformat()
        del match['time_obj']

    return paginated_matches

async def get_main_page_matches_async(limit=None, offset=0, handicap_filter=None, goal_line_filter=None, min_time=None):
    await _refresh_main_page_snapshot_if_empty()
    return _filter_and_slice_matches(
        'upcoming_matches',
        limit=limit,
        offset=offset,
        handicap_filter=handicap_filter,
        goal_line_filter=goal_line_filter,
        sort_desc=False,
        min_time=min_time
    )


async def get_main_page_finished_matches_async(limit=None, offset=0, handicap_filter=None, goal_line_filter=None):
    await _refresh_main_page_snapshot_if_empty()
    return _filter_and_slice_matches(
        'finished_matches',
        limit=limit,
        offset=offset,
        handicap_filter=handicap_filter,
        goal_line_filter=goal_line_filter,
        sort_desc=True,
    )


# --- FUNCIONES DE SCRAPING DIRECTO PARA COLAB / BACKGROUND ---

async def scrape_main_page_matches_async_direct(limit=None, offset=0, handicap_filter=None, goal_line_filter=None, min_time=None):
    """
    Versión DEDICADA para scripts de fondo (Colab). Descarga la web fresca.
    NO USAR EN LA WEB (lento).
    """
    print("[DIRECT SCRAPE] Descargando pagina principal (Proximos)...")
    html = await _fetch_nowgoal_html() # path None = home
    if not html:
        print("[DIRECT SCRAPE] Error: No se pudo descargar HTML.")
        return []

    print(f"[DIRECT SCRAPE] HTML descargado ({len(html)} bytes). Parseando...")
    matches = parse_main_page_matches(
        html, 
        limit=limit, 
        offset=offset, 
        handicap_filter=handicap_filter, 
        goal_line_filter=goal_line_filter
    )
    
    if min_time:
        filtered = []
        for m in matches:
            t_str = m.get('start_time')
            if t_str and t_str != 'N/A':
                try:
                    t_obj = datetime.datetime.fromisoformat(t_str)
                    if t_obj.replace(tzinfo=None) >= min_time.replace(tzinfo=None):
                        filtered.append(m)
                except Exception as e:
                    pass
        matches = filtered

    print(f"[DIRECT SCRAPE] Encontrados {len(matches)} partidos.")
    return matches

async def scrape_main_page_finished_matches_async_direct(limit=None, offset=0, handicap_filter=None, goal_line_filter=None):
    """
    Versión DEDICADA para scripts de fondo (Colab). Descarga la web fresca para terminados.
    """
    print("[DIRECT SCRAPE] Descargando pagina principal (Finalizados)...")
    html = await _fetch_nowgoal_html()
    if not html:
        print("[DIRECT SCRAPE] Error: No se pudo descargar HTML.")
        return []

    print(f"[DIRECT SCRAPE] HTML descargado. Parseando...")
    matches = parse_main_page_finished_matches(
        html,
        limit=limit,
        offset=offset,
        handicap_filter=handicap_filter,
        goal_line_filter=goal_line_filter
    )
    print(f"[DIRECT SCRAPE] Encontrados {len(matches)} partidos terminados.")
    return matches



async def _fetch_sidebar_lists(handicap_filter=None, goal_line_filter=None):
    return await asyncio.gather(
        get_main_page_matches_async(handicap_filter=handicap_filter, goal_line_filter=goal_line_filter),
        get_main_page_finished_matches_async(handicap_filter=handicap_filter, goal_line_filter=goal_line_filter),
    )


def _render_matches_dashboard(page_mode='upcoming', page_title='Partidos'):
    handicap_filter = request.args.get('handicap')
    goal_line_filter = request.args.get('ou')
    error_msg = None
    try:
        upcoming_matches, finished_matches = asyncio.run(
            _fetch_sidebar_lists(handicap_filter, goal_line_filter)
        )
    except Exception as exc:
        print(f"ERROR al cargar datos para el dashboard: {exc}")
        upcoming_matches, finished_matches = [], []
        error_msg = f"No se pudieron cargar los partidos: {exc}"

    handicap_options = _build_handicap_options_from_lists([upcoming_matches, finished_matches])
    goal_line_options = _build_goal_line_options_from_lists([upcoming_matches, finished_matches])
    active_matches = finished_matches if page_mode == 'finished' else upcoming_matches

    return render_template(
        'index.html',
        matches=active_matches,
        upcoming_matches=upcoming_matches,
        finished_matches=finished_matches,
        handicap_filter=handicap_filter,
        goal_line_filter=goal_line_filter,
        handicap_options=handicap_options,
        goal_line_options=goal_line_options,
        page_mode=page_mode,
        page_title=page_title,
        error=error_msg,
    )


def _parse_clock_hhmm(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    match = re.search(r'(\d{1,2}):(\d{2})', text)
    if not match:
        return None
    hour = int(match.group(1))
    minute = int(match.group(2))
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return None
    return hour, minute


def _parse_precache_date_parts(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    # Handle full ISO-like strings first.
    iso_candidate = text
    if iso_candidate.endswith('Z'):
        iso_candidate = iso_candidate[:-1] + '+00:00'
    try:
        parsed_iso = datetime.datetime.fromisoformat(iso_candidate)
        return parsed_iso.year, parsed_iso.month, parsed_iso.day
    except Exception:
        pass

    # Date-only formats: yyyy-mm-dd or yyyy/mm/dd
    iso_date_match = re.match(r'^(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})$', text)
    if iso_date_match:
        year = int(iso_date_match.group(1))
        month = int(iso_date_match.group(2))
        day = int(iso_date_match.group(3))
        return year, month, day

    # Ambiguous formats: mm/dd/yyyy or dd/mm/yyyy (default to mm/dd when ambiguous).
    short_match = re.match(r'^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})$', text)
    if not short_match:
        return None

    first = int(short_match.group(1))
    second = int(short_match.group(2))
    year_raw = short_match.group(3)
    year = int(year_raw)
    if len(year_raw) == 2:
        year += 2000 if year < 70 else 1900

    if first > 12 and second <= 12:
        day = first
        month = second
    elif second > 12 and first <= 12:
        month = first
        day = second
    else:
        month = first
        day = second

    return year, month, day


def _parse_precache_match_datetime_utc(match):
    if not isinstance(match, dict):
        return None

    parsed_start = _parse_start_time_to_utc(match.get('start_time'))
    if isinstance(parsed_start, datetime.datetime):
        return parsed_start

    date_parts = _parse_precache_date_parts(match.get('match_date') or match.get('date'))
    if not date_parts:
        return None

    clock = _parse_clock_hhmm(match.get('time')) or (0, 0)
    year, month, day = date_parts
    hour, minute = clock
    try:
        dt_spain = datetime.datetime(year, month, day, hour, minute, tzinfo=SPAIN_TZ)
    except Exception:
        return None
    return dt_spain.astimezone(UTC_TZ).replace(tzinfo=None)


def _build_upcoming_matches_from_precache(limit=None, handicap_filter=None, goal_line_filter=None):
    fetch_limit = 4000
    if isinstance(limit, int) and limit > 0:
        fetch_limit = min(max(limit * 6, 1200), 12000)

    precache_rows = sql_store.fetch_matches(bucket=data_manager.PRECACHEO_BUCKET, limit=fetch_limit)
    if not precache_rows:
        precache_rows = _load_precacheo_legacy_rows(limit=fetch_limit)
    if not precache_rows:
        return []

    now_utc = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    handicap_predicate = _build_handicap_filter_predicate(handicap_filter)
    goal_predicate = _build_goal_line_filter_predicate(goal_line_filter)

    output = []
    seen_ids = set()
    for row in precache_rows:
        if not isinstance(row, dict):
            continue

        raw_id = row.get('match_id') or row.get('id')
        if raw_id in (None, ''):
            continue
        mid = str(raw_id)
        if mid in seen_ids:
            continue
        seen_ids.add(mid)

        odds = row.get('main_match_odds') if isinstance(row.get('main_match_odds'), dict) else {}
        ah_line = row.get('handicap')
        if ah_line in (None, ''):
            ah_line = odds.get('ah_linea')
        ou_line = row.get('goal_line')
        if ou_line in (None, ''):
            ou_line = odds.get('goals_linea')

        if handicap_predicate and not handicap_predicate(ah_line or ''):
            continue
        if goal_predicate and not goal_predicate(ou_line or ''):
            continue

        parsed_utc = _parse_precache_match_datetime_utc(row)
        if isinstance(parsed_utc, datetime.datetime) and parsed_utc < now_utc:
            continue

        item = {
            'id': mid,
            'match_id': mid,
            'home_team': row.get('home_team') or row.get('home_name') or '',
            'away_team': row.get('away_team') or row.get('away_name') or '',
            'league': row.get('league') or row.get('league_name') or '',
            'handicap': ah_line,
            'goal_line': ou_line,
            'time': row.get('time') or '',
            'match_date': row.get('match_date') or row.get('date') or '',
            'date': row.get('date') or row.get('match_date') or '',
            '_sort_time': parsed_utc,
        }

        if isinstance(parsed_utc, datetime.datetime):
            spain_time = parsed_utc.replace(tzinfo=UTC_TZ).astimezone(SPAIN_TZ)
            item['start_time'] = spain_time.isoformat()
            item['time'] = spain_time.strftime('%H:%M')
            if not item.get('match_date'):
                item['match_date'] = spain_time.strftime('%m/%d/%Y')
            if not item.get('date'):
                item['date'] = item['match_date']
        else:
            item['start_time'] = row.get('start_time')

        output.append(item)

    with_time = [m for m in output if isinstance(m.get('_sort_time'), datetime.datetime)]
    without_time = [m for m in output if not isinstance(m.get('_sort_time'), datetime.datetime)]
    with_time.sort(key=lambda m: (m['_sort_time'], str(m.get('id', ''))))
    without_time.sort(key=lambda m: str(m.get('id', '')))
    combined = with_time + without_time

    for m in combined:
        m.pop('_sort_time', None)

    if isinstance(limit, int) and limit > 0:
        combined = combined[:limit]
    return combined


def _get_cached_upcoming_match_ids(limit=2000):
    """
    Returns upcoming match IDs from the cached main snapshot.
    This lets precache list prioritize rows that the table is actually rendering.
    """
    try:
        snapshot = load_data_from_file()
    except Exception:
        return []

    if not isinstance(snapshot, dict):
        return []

    now_utc = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    ids = []
    seen = set()

    for entry in snapshot.get('upcoming_matches', []) or []:
        if not isinstance(entry, dict):
            continue
        raw_id = entry.get('id') or entry.get('match_id')
        if raw_id in (None, ''):
            continue

        parsed_time = _parse_time_obj(entry.get('time_obj'))
        if parsed_time is None:
            parsed_time = _parse_start_time_to_utc(entry.get('start_time'))
        if isinstance(parsed_time, datetime.datetime) and parsed_time < now_utc:
            continue

        mid = str(raw_id)
        if mid in seen:
            continue
        seen.add(mid)
        ids.append(mid)
        if isinstance(limit, int) and limit > 0 and len(ids) >= limit:
            break

    return ids


def _compact_precacheo_match_for_list(match, include_specialist_picks=False):
    """
    Trim heavyweight HTML blobs from precache rows for list APIs.
    They are not used by pre-cache table rendering and can trigger OOM in small instances.
    Inyecta automaticamente el pick de la Clave Dicotomica V4 (AH + OU).
    Salidas: FAV_CUBRE / DOG_CUBRE / NO_BET (AH) | OVER / UNDER / NO_BET (OU)
    """
    if not isinstance(match, dict):
        return {}

    compact = {}
    for key, value in match.items():
        key_l = str(key).lower()
        if key_l in {'historical_matches_html', 'market_analysis_html'}:
            continue
        if isinstance(value, str) and 'html' in key_l and len(value) > 500:
            continue
        if key_l.startswith('recent_') and key_l.endswith(('matches', 'matches_all', 'specific', 'general')) and isinstance(value, list):
            compact[key] = [
                {
                    'home': row.get('home'),
                    'away': row.get('away'),
                    'score': row.get('score') or row.get('score_raw'),
                }
                for row in value
                if isinstance(row, dict)
            ]
            continue
        compact[key] = value

    # La columna Picks tiene una sola autoridad. Se descartan motores legacy
    # persistidos para que no reaparezcan al mezclar snapshots antiguos.
    compact['specialist_picks'] = []

    # --- CLAVE DICOTOMICA V7: inyectar predicciones, lectura raw y calidad ---
    try:
        from modules.clave_dicotomica import apply_key
        pick = apply_key(match)
        compact['clave_pick_ah']    = pick.get('ah', 'NO_BET')
        compact['clave_label_ah']   = pick.get('ah_label', '')
        compact['clave_pick_ou']    = pick.get('ou', 'NO_BET')
        compact['clave_label_ou']   = pick.get('ou_label', '')
        compact['clave_engine_version'] = pick.get('engine_version', 'V7.0')
        compact['clave_raw_ah']     = pick.get('raw_ah', pick.get('ah', 'NO_BET'))
        compact['clave_raw_ou']     = pick.get('raw_ou', pick.get('ou', 'NO_BET'))
        compact['clave_tier_ah']    = pick.get('prediction_tier_ah', 'NO_BET')
        compact['clave_tier_ou']    = pick.get('prediction_tier_ou', 'NO_BET')
        compact['clave_confidence_ah'] = pick.get('confidence_ah', 'NONE')
        compact['clave_confidence_ou'] = pick.get('confidence_ou', 'NONE')
        compact['clave_quality']    = pick.get('quality', {})
        compact['clave_ah_gate_reasons'] = pick.get('ah_gate_reasons', [])
        compact['clave_ou_gate_reasons'] = pick.get('ou_gate_reasons', [])
        compact['clave_production_ah_rules'] = pick.get('production_ah_rules', [])
        compact['clave_production_ou_rules'] = pick.get('production_ou_rules', [])
        compact['clave_validated_ah_line'] = pick.get('validated_ah_line', False)
        compact['clave_validated_ah_expansion'] = pick.get('validated_ah_expansion', False)
        compact['clave_expansion_ah_rule'] = pick.get('expansion_ah_rule', None)
        compact['clave_bookie_detector'] = pick.get('bookie_detector', {})
        compact['clave_bookie_confirmation'] = pick.get('bookie_confirmation', 'NO_DATA')
        compact['clave_edge_ah']    = pick.get('edge_AH', 0)
        compact['clave_edge_ou']    = pick.get('edge_OU', 0)
        compact['clave_mr_ah']      = pick.get('mr_dog', []) + pick.get('mr_fav', [])
        compact['clave_mr_ou']      = pick.get('mr_under', []) + pick.get('mr_over', [])
        compact['clave_base_cover'] = pick.get('base_cover', '')
        compact['clave_pressure']   = pick.get('pressure', '')

        # Propiedades estructurales heredadas y banderas de aprendizaje
        compact['clave_score_draw']  = pick.get('score_DRAW', 0)
        compact['clave_draw_type']   = pick.get('draw_type', '')
        compact['clave_argumentos']  = pick.get('argumentos', [])
        compact['clave_flags']       = pick.get('flags', [])
        compact['clave_score_f']     = pick.get('score_F', 0)
        compact['clave_score_d']     = pick.get('score_D', 0)
        compact['clave_role_mode']   = pick.get('role_mode', '')
        compact['clave_is_pickem']   = pick.get('is_pickem', False)
        compact['clave_learning_hooks'] = pick.get('learning_hooks', [])
        compact['clave_stadium_rh']  = pick.get('stadium_RH', None)
        compact['clave_u10_anomalia'] = pick.get('u10_anomalia_linea_baja', False)
        compact['clave_u11_dog_persistente'] = pick.get('u11_favorito_125_dog_persistente', False)
        compact['clave_u12_bloqueo_seco'] = pick.get('u12_bloqueo_seco', False)
        compact['clave_u13_push_seco'] = pick.get('u13_push_seco', False)
        compact['clave_u14_repeticion_proceso'] = pick.get('u14_repeticion_proceso', False)
        compact['clave_u15_rebaja_protectora'] = pick.get('u15_rebaja_protectora', False)
        compact['clave_u16_fav_025_capado'] = pick.get('u16_fav_025_capado', False)
        compact['clave_u17_market_flip_validated'] = pick.get('u17_market_flip_validated', False)
        compact['clave_u18_over_counterintuitive'] = pick.get('u18_over_counterintuitive', False)
        compact['clave_u19_market_rejects_obvious_dog_x2'] = pick.get('u19_market_rejects_obvious_dog_x2', False)
        compact['clave_u20_huge_drop_protects_dog_under'] = pick.get('u20_huge_drop_protects_dog_under', False)
        compact['clave_u21_h2h_over_capped_draw_under'] = pick.get('u21_h2h_over_capped_draw_under', False)
        compact['clave_u22_pickem_dog_win_home_dnb_under'] = pick.get('u22_pickem_dog_win_home_dnb_under', False)
        compact['clave_over_counter_confirmers'] = pick.get('over_counter_confirmers', 0)
    except Exception:
        compact['clave_pick_ah']  = 'NO_BET'
        compact['clave_pick_ou']  = 'NO_BET'
        compact['clave_label_ah'] = ''
        compact['clave_label_ou'] = ''
        compact['clave_engine_version'] = 'V7.0'
        compact['clave_raw_ah'] = 'NO_BET'
        compact['clave_raw_ou'] = 'NO_BET'
        compact['clave_tier_ah'] = 'NO_BET'
        compact['clave_tier_ou'] = 'NO_BET'
        compact['clave_confidence_ah'] = 'NONE'
        compact['clave_confidence_ou'] = 'NONE'
        compact['clave_quality'] = {}
        compact['clave_ah_gate_reasons'] = []
        compact['clave_ou_gate_reasons'] = []
        compact['clave_production_ah_rules'] = []
        compact['clave_production_ou_rules'] = []
        compact['clave_validated_ah_line'] = False
        compact['clave_validated_ah_expansion'] = False
        compact['clave_expansion_ah_rule'] = None
        compact['clave_bookie_detector'] = {}
        compact['clave_bookie_confirmation'] = 'NO_DATA'
        compact['clave_edge_ah']  = 0
        compact['clave_edge_ou']  = 0
        compact['clave_mr_ah']    = []
        compact['clave_mr_ou']    = []
        compact['clave_score_draw'] = 0
        compact['clave_draw_type']  = ''
        compact['clave_argumentos'] = []
        compact['clave_flags']      = []
        compact['clave_score_f']    = 0
        compact['clave_score_d']    = 0
        compact['clave_role_mode']  = ''
        compact['clave_is_pickem']  = False
        compact['clave_learning_hooks'] = []
        compact['clave_stadium_rh'] = None
        compact['clave_u10_anomalia'] = False
        compact['clave_u11_dog_persistente'] = False
        compact['clave_u12_bloqueo_seco'] = False
        compact['clave_u13_push_seco'] = False
        compact['clave_u14_repeticion_proceso'] = False
        compact['clave_u15_rebaja_protectora'] = False
        compact['clave_u16_fav_025_capado'] = False
        compact['clave_u17_market_flip_validated'] = False
        compact['clave_u18_over_counterintuitive'] = False
        compact['clave_u19_market_rejects_obvious_dog_x2'] = False
        compact['clave_u20_huge_drop_protects_dog_under'] = False
        compact['clave_u21_h2h_over_capped_draw_under'] = False
        compact['clave_u22_pickem_dog_win_home_dnb_under'] = False
        compact['clave_over_counter_confirmers'] = 0

    return compact


@app.route('/')
def index():
    return redirect(url_for('precacheo'))


@app.route('/resultados')
def resultados():
    print("Recibida petición para Partidos Finalizados...")
    return _render_matches_dashboard('finished', 'Resultados Finalizados')


@app.route('/proximos')
def proximos():
    print("Recibida petición para /proximos")
    return _render_matches_dashboard('upcoming', 'Próximos Partidos')

@app.route('/todos_resultados')
def todos_resultados():
    """Muestra una vista dedicada con todos los partidos finalizados."""
    return render_template('finished_matches.html')


@app.route('/api/export_prompt/<match_id>')
def api_export_prompt(match_id):
    try:
        # Intentar obtener el partido directamente de la base de datos (instantaneo)
        match_data = sql_store.get_match(str(match_id))
        
        if not match_data:
            # Fallback si no esta en la DB: usar la funcion de analisis estandar
            match_data = analizar_partido_completo(str(match_id), force_refresh=False)
            if isinstance(match_data, tuple):
                match_data = match_data[0]
            
        if not match_data or (isinstance(match_data, dict) and "error" in match_data):
            return jsonify({"success": False, "error": "Datos del partido no encontrados. ¿Lo has analizado antes?"})
        
        if not isinstance(match_data, dict):
            return jsonify({"success": False, "error": "Formato de datos invalido en la cache."})

        from modules import llm_exporter
        prompt = llm_exporter.generate_llm_prompt(match_data)
        return jsonify({"success": True, "prompt": prompt})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)})

@app.route('/api/export_prompts_bulk', methods=['POST'])
def api_export_prompts_bulk():
    try:
        data = request.json or {}
        match_ids = data.get('match_ids', [])
        if not match_ids:
            return jsonify({"success": False, "error": "Falta la lista de match_ids"}), 400

        from modules import llm_exporter
        prompts = []
        for mid in match_ids:
            # Obtener el partido directamente de la base de datos
            match_data = sql_store.get_match(str(mid))
            if not match_data:
                # Fallback de análisis si no está en la base de datos
                match_data = analizar_partido_completo(str(mid), force_refresh=False)
                if isinstance(match_data, tuple):
                    match_data = match_data[0]

            if isinstance(match_data, dict) and "error" not in match_data:
                prompt = llm_exporter.generate_llm_prompt(match_data)
                prompts.append(prompt)
            else:
                prompts.append("# Partido no disponible\n- Error: No se pudieron cargar los datos del partido.")

        return jsonify({"success": True, "prompts": prompts})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/matches')
def api_matches():
    try:
        offset = int(request.args.get('offset', 0))
        limit = int(request.args.get('limit', 5))
        limit = min(limit, 1000)
        handicap_filter = request.args.get('handicap')
        ou_filter = request.args.get('ou')

        matches = asyncio.run(get_main_page_matches_async(limit, offset, handicap_filter, ou_filter))

        # Fallback: if cache is empty/stale, try a direct scrape so UI does not stay blank.
        if not matches:
            print("[api/matches] Empty cache result, trying direct scrape fallback...")
            matches = asyncio.run(
                scrape_main_page_matches_async_direct(
                    limit=limit,
                    offset=offset,
                    handicap_filter=handicap_filter,
                    goal_line_filter=ou_filter,
                    min_time=None,
                )
            ) or []
            if not matches:
                print("[api/matches] Direct scrape empty, trying precache fallback...")
                matches = _build_upcoming_matches_from_precache(
                    limit=limit,
                    handicap_filter=handicap_filter,
                    goal_line_filter=ou_filter,
                )
            if not matches:
                print("[api/matches] Precache fallback empty, trying grandes ligas fallback...")
                matches = _fetch_grandes_ligas_upcoming_matches(limit=limit)

        return jsonify({'matches': matches})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/finished_matches')
def api_finished_matches():
    try:
        offset = int(request.args.get('offset', 0))
        limit = int(request.args.get('limit', 5))
        limit = min(limit, 1000)
        handicap_filter = request.args.get('handicap')
        ou_filter = request.args.get('ou')

        matches = asyncio.run(get_main_page_finished_matches_async(limit, offset, handicap_filter, ou_filter))

        # Fallback: direct scrape if cache has no finished rows.
        if not matches:
            print("[api/finished_matches] Empty cache result, trying direct scrape fallback...")
            matches = asyncio.run(
                scrape_main_page_finished_matches_async_direct(
                    limit=limit,
                    offset=offset,
                    handicap_filter=handicap_filter,
                    goal_line_filter=ou_filter,
                )
            ) or []

        return jsonify({'matches': matches})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/all_finished_matches')
def api_all_finished_matches():
    """Devuelve todos los partidos finalizados disponibles (o un límite alto)."""
    try:
        # Reutilizamos la lógica existente pero con un límite alto
        matches = asyncio.run(get_main_page_finished_matches_async(limit=1000, offset=0))
        return jsonify({'matches': matches})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# --- GRANDES LIGAS (Top European Leagues) ---
# Configuración de ligas principales
GRANDES_LIGAS_CONFIG = {
    'Premier League': {'id': 36, 'short': 'PL', 'color': '#3d195b'},
    'EFL Championship': {'id': 34, 'short': 'CH', 'color': '#ff6600'},
    'Italy Serie A': {'id': 31, 'short': 'SA', 'color': '#008c45'},
    'German Bundesliga': {'id': 11, 'short': 'BL', 'color': '#d00027'},
    'Spain Primera Liga': {'id': 8, 'short': 'LL', 'color': '#ee1d23'},
    # Variantes de nombres que puede usar Nowgoal
    'English Premier League': {'id': 36, 'short': 'PL', 'color': '#3d195b'},
    'Serie A': {'id': 31, 'short': 'SA', 'color': '#008c45'},
    'Bundesliga': {'id': 11, 'short': 'BL', 'color': '#d00027'},
    'La Liga': {'id': 8, 'short': 'LL', 'color': '#ee1d23'},
    'LaLiga': {'id': 8, 'short': 'LL', 'color': '#ee1d23'},
}

GRANDES_LIGAS_DATA_URLS = {
    'Premier League': {
        'url': 'https://football.nowgoal26.com/jsData/matchResult/2025-2026/s36_en.js',
        'id': 36, 'short': 'PL', 'color': '#3d195b'
    },
    'La Liga': {
        'url': 'https://football.nowgoal26.com/jsData/matchResult/2025-2026/s31_en.js',
        'id': 31, 'short': 'LL', 'color': '#ee1d23'
    },
    'Serie A': {
        'url': 'https://football.nowgoal26.com/jsData/matchResult/2025-2026/s34_2948_en.js',
        'id': 34, 'short': 'SA', 'color': '#008c45'
    },
    'Ligue 1': {
        'url': 'https://football.nowgoal26.com/jsData/matchResult/2025-2026/s11_en.js',
        'id': 11, 'short': 'L1', 'color': '#091c3e'
    },
    'Bundesliga': {
        'url': 'https://football.nowgoal26.com/jsData/matchResult/2025-2026/s8_en.js',
        'id': 8, 'short': 'BL', 'color': '#d00027'
    },
}


def _is_grande_liga(league_name: str) -> bool:
    """Verifica si el nombre de liga corresponde a una de las Grandes Ligas."""
    if not league_name:
        return False
    league_lower = league_name.lower().strip()
    for config_name in GRANDES_LIGAS_CONFIG.keys():
        if config_name.lower() in league_lower or league_lower in config_name.lower():
            return True
    return False


def _get_liga_info(league_name: str) -> dict:
    """Obtiene info de la liga (short, color) o None si no es grande liga."""
    if not league_name:
        return None
    league_lower = league_name.lower().strip()
    for config_name, info in GRANDES_LIGAS_CONFIG.items():
        if config_name.lower() in league_lower or league_lower in config_name.lower():
            return info
    return None


def _fetch_grandes_ligas_upcoming_matches(limit: int | None = None, max_days: int = 7) -> list[dict]:
    """
    Fetch upcoming matches from league JS feeds used by /api/grandes_ligas.
    Returns list sorted by start_time asc.
    """
    all_matches = []
    now = datetime.datetime.now()
    max_date = now + datetime.timedelta(days=max_days)
    session = _get_shared_requests_session()

    for liga_name, liga_info in GRANDES_LIGAS_DATA_URLS.items():
        try:
            response = session.get(liga_info['url'], timeout=10)
            if response.status_code != 200:
                continue
            js_content = response.text

            teams = {}
            team_match = re.search(r'var arrTeam = \[(.*?)\];', js_content, re.DOTALL)
            if team_match:
                team_str = team_match.group(1)
                team_entries = re.findall(r'\[(\d+),\'[^\']*\',\'[^\']*\',\'([^\']+)\'', team_str)
                for tid, tname in team_entries:
                    teams[int(tid)] = tname

            round_matches = re.findall(r'jh\["R_\d+"\]\s*=\s*\[(.*?)\];', js_content, re.DOTALL)
            for round_data in round_matches:
                matches_raw = re.findall(r'\[([^\[\]]+)\]', round_data)
                for match_raw in matches_raw:
                    parts = match_raw.split(',')
                    if len(parts) < 14:
                        continue
                    try:
                        match_id = parts[0].strip()
                        date_str = parts[3].strip().strip("'")
                        home_id = int(parts[4].strip())
                        away_id = int(parts[5].strip())
                        score = parts[6].strip()
                        home_rank = parts[8].strip().strip("'")
                        away_rank = parts[9].strip().strip("'")
                        ah = parts[10].strip()
                        ou = parts[12].strip().strip("'")

                        try:
                            match_time = datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M')
                        except Exception:
                            continue

                        if match_time < now or match_time > max_date:
                            continue

                        clean_score = score.replace("'", "").strip()
                        if clean_score and clean_score != '-' and ':' not in clean_score and '-' in clean_score:
                            continue

                        home_name = teams.get(home_id, f"Team {home_id}")
                        away_name = teams.get(away_id, f"Team {away_id}")

                        # Source time is GMT+8; convert to Spain (CET/CEST approx by -7h used in existing code).
                        spain_time = match_time - datetime.timedelta(hours=7)

                        all_matches.append({
                            'id': match_id,
                            'league': liga_name,
                            'liga_short': liga_info['short'],
                            'liga_color': liga_info['color'],
                            'liga_id': liga_info['id'],
                            'home_team': home_name,
                            'away_team': away_name,
                            'home_rank': home_rank.replace("'", ""),
                            'away_rank': away_rank.replace("'", ""),
                            'handicap': ah,
                            'goal_line': ou.replace("'", ""),
                            'time': spain_time.strftime('%H:%M'),
                            'start_time': spain_time.isoformat(),
                        })
                    except Exception:
                        continue
        except Exception:
            continue

    all_matches.sort(key=lambda x: x.get('start_time', ''))
    if isinstance(limit, int) and limit > 0:
        return all_matches[:limit]
    return all_matches


@app.route('/grandes_ligas')
def grandes_ligas():
    """Página de Próximos Partidos de Grandes Ligas."""
    return render_template('grandes_ligas.html')


@app.route('/api/grandes_ligas')
def api_grandes_ligas():
    """
    Devuelve partidos de las principales ligas europeas.
    Hace scraping directo de los archivos JS de cada liga en Nowgoal.
    Solo partidos de los proximos 7 dias.
    """
    try:
        all_matches = _fetch_grandes_ligas_upcoming_matches()
        return jsonify({
            'matches': all_matches,
            'ligas': list(set(m['league'] for m in all_matches)),
            'total': len(all_matches)
        })
    except Exception as e:
        print(f"[GRANDES LIGAS] Error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/grandes_ligas_scrape', methods=['POST'])
def api_grandes_ligas_scrape():
    """
    Scrapea partidos de Grandes Ligas con workers paralelos (igual que precacheo).
    Acepta: { "match_ids": ["id1", "id2", ...], "workers": 10 }
    """
    try:
        data = request.get_json() or {}
        match_ids = data.get('match_ids', [])
        workers = data.get('workers', 10)
        
        if not match_ids:
            return jsonify({'error': 'Falta match_ids'}), 400
        
        print(f"🌍 [GRANDES LIGAS SCRAPE] Scrapeando {len(match_ids)} partidos con {workers} workers...")
        
        # Usar ThreadPoolExecutor igual que precacheo
        total = len(match_ids)
        completed = 0
        errors = 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(process_single_precache_worker, mid): mid for mid in match_ids}
            
            for future in concurrent.futures.as_completed(futures):
                mid = futures[future]
                try:
                    result = future.result()
                    if result:
                        completed += 1
                    else:
                        errors += 1
                except Exception as e:
                    print(f"   ⚠️ Error scrapeando {mid}: {e}")
                    errors += 1
        
        print(f"✅ [GRANDES LIGAS SCRAPE] Completado: {completed} éxitos, {errors} errores")
        
        return jsonify({
            'success': True,
            'scraped': completed,
            'errors': errors,
            'total': total
        })
        
    except Exception as e:
        print(f"❌ [GRANDES LIGAS SCRAPE] Error: {e}")
        return jsonify({'error': str(e)}), 500


# --- LIGAS FAVORITAS ---
# Configuración de TODAS las ligas disponibles para añadir como favoritas
TODAS_LIGAS_CONFIG = {
    # Top 5 Europeas
    'Premier League': {
        'id': 36,
        'url': 'https://football.nowgoal26.com/jsData/matchResult/2025-2026/s36_en.js',
        'short': 'PL',
        'color': '#3d195b',
        'country': 'England'
    },
    'La Liga': {
        'id': 31,
        'url': 'https://football.nowgoal26.com/jsData/matchResult/2025-2026/s31_en.js',
        'short': 'LL',
        'color': '#ee1d23',
        'country': 'Spain'
    },
    'Serie A': {
        'id': 34,
        'url': 'https://football.nowgoal26.com/jsData/matchResult/2025-2026/s34_2948_en.js',
        'short': 'SA',
        'color': '#008c45',
        'country': 'Italy'
    },
    'Bundesliga': {
        'id': 8,
        'url': 'https://football.nowgoal26.com/jsData/matchResult/2025-2026/s8_en.js',
        'short': 'BL',
        'color': '#d00027',
        'country': 'Germany'
    },
    'Ligue 1': {
        'id': 11,
        'url': 'https://football.nowgoal26.com/jsData/matchResult/2025-2026/s11_en.js',
        'short': 'L1',
        'color': '#091c3e',
        'country': 'France'
    },
    # Segundas Divisiones
    'Championship': {
        'id': 37,
        'url': 'https://football.nowgoal26.com/jsData/matchResult/2025-2026/s37_en.js',
        'short': 'CH',
        'color': '#ff6600',
        'country': 'England'
    },
    'La Liga 2': {
        'id': 32,
        'url': 'https://football.nowgoal26.com/jsData/matchResult/2025-2026/s32_en.js',
        'short': 'LL2',
        'color': '#ffcc00',
        'country': 'Spain'
    },
    'Serie B': {
        'id': 35,
        'url': 'https://football.nowgoal26.com/jsData/matchResult/2025-2026/s35_en.js',
        'short': 'SB',
        'color': '#006600',
        'country': 'Italy'
    },
    'Bundesliga 2': {
        'id': 9,
        'url': 'https://football.nowgoal26.com/jsData/matchResult/2025-2026/s9_en.js',
        'short': 'BL2',
        'color': '#aa0027',
        'country': 'Germany'
    },
    'Ligue 2': {
        'id': 12,
        'url': 'https://football.nowgoal26.com/jsData/matchResult/2025-2026/s12_en.js',
        'short': 'L2',
        'color': '#445566',
        'country': 'France'
    },
    # Otras ligas europeas
    'Eredivisie': {
        'id': 17,
        'url': 'https://football.nowgoal26.com/jsData/matchResult/2025-2026/s17_en.js',
        'short': 'ERE',
        'color': '#ff4500',
        'country': 'Netherlands'
    },
    'Primeira Liga': {
        'id': 28,
        'url': 'https://football.nowgoal26.com/jsData/matchResult/2025-2026/s28_en.js',
        'short': 'PRI',
        'color': '#006600',
        'country': 'Portugal'
    },
    'Belgian Pro League': {
        'id': 3,
        'url': 'https://football.nowgoal26.com/jsData/matchResult/2025-2026/s3_en.js',
        'short': 'BEL',
        'color': '#ff0000',
        'country': 'Belgium'
    },
    'Scottish Premiership': {
        'id': 45,
        'url': 'https://football.nowgoal26.com/jsData/matchResult/2025-2026/s45_en.js',
        'short': 'SPL',
        'color': '#003399',
        'country': 'Scotland'
    },
    'Super Lig': {
        'id': 52,
        'url': 'https://football.nowgoal26.com/jsData/matchResult/2025-2026/s52_en.js',
        'short': 'TUR',
        'color': '#e30a17',
        'country': 'Turkey'
    },
    # Sudamérica
    'Argentina Primera': {
        'id': 1,
        'url': 'https://football.nowgoal26.com/jsData/matchResult/2024/s1_en.js',
        'short': 'ARG',
        'color': '#75aadb',
        'country': 'Argentina'
    },
    'Brasileirao': {
        'id': 4,
        'url': 'https://football.nowgoal26.com/jsData/matchResult/2024/s4_en.js',
        'short': 'BRA',
        'color': '#009c3b',
        'country': 'Brazil'
    },
}

FAVORITAS_CONFIG_KEY = 'app_favoritas_config_v1'
FAVORITAS_CONFIG_LEGACY_FILE = Path(__file__).resolve().parent / 'favoritas_config.json'
_favoritas_lock = threading.Lock()


def load_favoritas_config():
    """Carga la configuración de ligas favoritas del usuario desde SQL."""
    default_cfg = {'favoritas': [], 'ocultas': [], 'favoritas_nombres': [], 'ocultas_nombres': [], 'neutras_nombres': []}
    with _favoritas_lock:
        cfg = sql_store.get_json_state(FAVORITAS_CONFIG_KEY, default=None)
        if cfg is None and FAVORITAS_CONFIG_LEGACY_FILE.exists():
            try:
                with open(FAVORITAS_CONFIG_LEGACY_FILE, 'r', encoding='utf-8') as f:
                    cfg = json.load(f)
                sql_store.set_json_state(FAVORITAS_CONFIG_KEY, cfg)
                print(f"Legacy favoritas config imported from {FAVORITAS_CONFIG_LEGACY_FILE}")
            except Exception as exc:
                print(f"Warning importing legacy favoritas config: {exc}")
                cfg = None

        if not isinstance(cfg, dict):
            cfg = dict(default_cfg)

        for key in default_cfg:
            if key not in cfg or not isinstance(cfg.get(key), list):
                cfg[key] = []
        return cfg


def save_favoritas_config(config):
    """Guarda la configuración de ligas favoritas en SQL."""
    with _favoritas_lock:
        try:
            # Asegurar que todas las claves existan antes de guardar
            for key in ['favoritas', 'ocultas', 'favoritas_nombres', 'ocultas_nombres', 'neutras_nombres']:
                if key not in config:
                    config[key] = []

            sql_store.set_json_state(FAVORITAS_CONFIG_KEY, config)
        except Exception as e:
            print(f"Error saving favoritas config: {e}")

@app.route('/ligas_favoritas')
def ligas_favoritas():
    """Página de Ligas Favoritas."""
    return render_template('ligas_favoritas.html')


@app.route('/api/ligas_disponibles')
def api_ligas_disponibles():
    """Devuelve todas las ligas disponibles para añadir como favoritas."""
    try:
        ligas = []
        for nombre, info in TODAS_LIGAS_CONFIG.items():
            ligas.append({
                'id': info['id'],
                'nombre': nombre,
                'short': info['short'],
                'color': info['color'],
                'country': info.get('country', '')
            })
        # Ordenar por país y luego por nombre
        ligas.sort(key=lambda x: (x['country'], x['nombre']))
        return jsonify({'ligas': ligas})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/ligas_favoritas/vaciar', methods=['POST'])
def api_vaciar_favoritas():
    """Limpia toda la configuración de ligas favoritas."""
    try:
        empty_config = {
            'favoritas': [],
            'ocultas': [],
            'favoritas_nombres': [],
            'ocultas_nombres': [],
            'neutras_nombres': []
        }
        save_favoritas_config(empty_config)
        return jsonify({'success': True, 'message': 'Configuración vaciada'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/ligas_favoritas_config')
def api_favoritas_config():
    """Devuelve la configuración cruda de favoritas."""
    return jsonify(load_favoritas_config())


@app.route('/api/ligas_favoritas', methods=['GET'])
def api_get_ligas_favoritas():
    """Obtiene la lista de ligas favoritas del usuario."""
    try:
        config = load_favoritas_config()
        favoritas = []
        ocultas = []
        
        for liga_id in config.get('favoritas', []):
            # Buscar info de la liga
            for nombre, info in TODAS_LIGAS_CONFIG.items():
                if info['id'] == liga_id:
                    favoritas.append({
                        'id': liga_id,
                        'nombre': nombre,
                        'short': info['short'],
                        'color': info['color'],
                        'country': info.get('country', '')
                    })
                    break
        
        for liga_id in config.get('ocultas', []):
            for nombre, info in TODAS_LIGAS_CONFIG.items():
                if info['id'] == liga_id:
                    ocultas.append({
                        'id': liga_id,
                        'nombre': nombre,
                        'short': info['short'],
                        'color': info['color'],
                        'country': info.get('country', '')
                    })
                    break
        
        return jsonify({
            'favoritas': favoritas,
            'ocultas': ocultas
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/ligas_favoritas', methods=['POST'])
def api_add_liga_favorita():
    """Añade una liga a favoritas."""
    try:
        data = request.get_json() or {}
        liga_id = data.get('liga_id')
        
        if not liga_id:
            return jsonify({'error': 'Falta liga_id'}), 400
        
        liga_id = int(liga_id)
        
        config = load_favoritas_config()
        favoritas = set(config.get('favoritas', []))
        ocultas = set(config.get('ocultas', []))
        
        # Añadir a favoritas y quitar de ocultas si estaba
        favoritas.add(liga_id)
        ocultas.discard(liga_id)
        
        config['favoritas'] = list(favoritas)
        config['ocultas'] = list(ocultas)
        save_favoritas_config(config)
        
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/ligas_favoritas/<int:liga_id>', methods=['DELETE'])
def api_ocultar_liga_favorita(liga_id):
    """Oculta una liga de favoritas (la mueve a ocultas)."""
    try:
        config = load_favoritas_config()
        favoritas = set(config.get('favoritas', []))
        ocultas = set(config.get('ocultas', []))
        
        # Mover de favoritas a ocultas
        if liga_id in favoritas:
            favoritas.remove(liga_id)
            ocultas.add(liga_id)
        
        config['favoritas'] = list(favoritas)
        config['ocultas'] = list(ocultas)
        save_favoritas_config(config)
        
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/ligas_neutras', methods=['POST'])
def api_add_liga_neutra():
    """Añade una liga a la lista de neutras (por nombre)."""
    try:
        data = request.get_json() or {}
        liga_nombre = data.get('liga_nombre')
        
        if not liga_nombre:
            return jsonify({'error': 'Falta liga_nombre'}), 400
        
        config = load_favoritas_config()
        
        if 'neutras_nombres' not in config:
            config['neutras_nombres'] = []
            
        neutras = set(n.lower() for n in config.get('neutras_nombres', []))
        neutras.add(liga_nombre.lower())
        
        config['neutras_nombres'] = list(neutras)
        save_favoritas_config(config)
        
        return jsonify({'success': True, 'liga': liga_nombre})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/ligas_neutras/<path:liga_nombre>', methods=['DELETE'])
def api_remove_liga_neutra(liga_nombre):
    """Elimina una liga de la lista de neutras."""
    try:
        liga_nombre = liga_nombre.strip()
        config = load_favoritas_config()
        
        if 'neutras_nombres' not in config:
            config['neutras_nombres'] = []
            
        neutras = set(n.lower() for n in config.get('neutras_nombres', []))
        neutras.discard(liga_nombre.lower())
        
        config['neutras_nombres'] = list(neutras)
        save_favoritas_config(config)
        
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500



@app.route('/api/ligas_ocultas/<int:liga_id>/restaurar', methods=['POST'])
def api_restaurar_liga_oculta(liga_id):
    """Restaura una liga oculta a favoritas."""
    try:
        config = load_favoritas_config()
        favoritas = set(config.get('favoritas', []))
        ocultas = set(config.get('ocultas', []))
        
        # Mover de ocultas a favoritas
        if liga_id in ocultas:
            ocultas.remove(liga_id)
            favoritas.add(liga_id)
        
        config['favoritas'] = list(favoritas)
        config['ocultas'] = list(ocultas)
        save_favoritas_config(config)
        
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# --- ENDPOINTS PARA LIGAS POR NOMBRE (Pre-Cacheo) ---
# Permite añadir/ocultar cualquier liga usando su nombre, no solo las predefinidas

@app.route('/api/ligas_favoritas_nombre', methods=['POST'])
def api_add_liga_favorita_nombre():
    """Añade una liga a favoritas usando su nombre. Si existe en TODAS_LIGAS_CONFIG, guarda por ID."""
    try:
        data = request.get_json() or {}
        liga_nombre = data.get('nombre', '').strip()
        
        if not liga_nombre:
            return jsonify({'error': 'Falta nombre de liga'}), 400
        
        config = load_favoritas_config()
        
        # Inicializar listas si no existen
        if 'favoritas' not in config:
            config['favoritas'] = []
        if 'favoritas_nombres' not in config:
            config['favoritas_nombres'] = []
        if 'ocultas' not in config:
            config['ocultas'] = []
        if 'ocultas_nombres' not in config:
            config['ocultas_nombres'] = []
        
        # Buscar si la liga existe en TODAS_LIGAS_CONFIG (por nombre)
        liga_id = None
        for nombre, info in TODAS_LIGAS_CONFIG.items():
            if nombre.lower() == liga_nombre.lower():
                liga_id = info['id']
                break
        
        if liga_id:
            # Guardar por ID (preferido)
            if liga_id not in config['favoritas']:
                config['favoritas'].append(liga_id)
            # Quitar de ocultas si estaba
            if liga_id in config['ocultas']:
                config['ocultas'].remove(liga_id)
            print(f"✅ Liga '{liga_nombre}' guardada por ID: {liga_id}")
        else:
            # Guardar por nombre (fallback para ligas no predefinidas)
            favoritas = set(n.lower() for n in config.get('favoritas_nombres', []))
            ocultas = set(n.lower() for n in config.get('ocultas_nombres', []))
            
            favoritas.add(liga_nombre.lower())
            ocultas.discard(liga_nombre.lower())
            
            config['favoritas_nombres'] = list(favoritas)
            config['ocultas_nombres'] = list(ocultas)
            print(f"⚠️ Liga '{liga_nombre}' guardada por nombre (no encontrada en config)")
        
        save_favoritas_config(config)
        
        return jsonify({'success': True, 'liga': liga_nombre, 'saved_by_id': liga_id is not None, 'id': liga_id})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/ligas_favoritas_nombre/<path:liga_nombre>', methods=['DELETE'])
def api_quitar_liga_favorita_nombre(liga_nombre):
    """Quita una liga de favoritas usando su nombre."""
    try:
        liga_nombre = liga_nombre.strip()
        
        config = load_favoritas_config()
        
        if 'favoritas_nombres' not in config:
            config['favoritas_nombres'] = []
        
        favoritas = set(n.lower() for n in config.get('favoritas_nombres', []))
        favoritas.discard(liga_nombre.lower())
        
        config['favoritas_nombres'] = list(favoritas)
        save_favoritas_config(config)
        
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/ligas_ocultas_nombre', methods=['POST'])
def api_ocultar_liga_nombre():
    """Oculta una liga usando su nombre."""
    try:
        data = request.get_json() or {}
        liga_nombre = data.get('nombre', '').strip()
        
        if not liga_nombre:
            return jsonify({'error': 'Falta nombre de liga'}), 400
        
        config = load_favoritas_config()
        
        if 'favoritas_nombres' not in config:
            config['favoritas_nombres'] = []
        if 'ocultas_nombres' not in config:
            config['ocultas_nombres'] = []
        
        favoritas = set(n.lower() for n in config.get('favoritas_nombres', []))
        ocultas = set(n.lower() for n in config.get('ocultas_nombres', []))
        
        # Mover de favoritas a ocultas
        favoritas.discard(liga_nombre.lower())
        ocultas.add(liga_nombre.lower())
        
        config['favoritas_nombres'] = list(favoritas)
        config['ocultas_nombres'] = list(ocultas)
        save_favoritas_config(config)
        
        return jsonify({'success': True, 'liga': liga_nombre})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/ligas_ocultas_nombre/<path:liga_nombre>/restaurar', methods=['POST'])
def api_restaurar_liga_oculta_nombre(liga_nombre):
    """Restaura una liga oculta usando su nombre."""
    try:
        liga_nombre = liga_nombre.strip()
        
        config = load_favoritas_config()
        
        if 'favoritas_nombres' not in config:
            config['favoritas_nombres'] = []
        if 'ocultas_nombres' not in config:
            config['ocultas_nombres'] = []
        
        favoritas = set(n.lower() for n in config.get('favoritas_nombres', []))
        ocultas = set(n.lower() for n in config.get('ocultas_nombres', []))
        
        # Mover de ocultas a favoritas
        ocultas.discard(liga_nombre.lower())
        favoritas.add(liga_nombre.lower())
        
        config['favoritas_nombres'] = list(favoritas)
        config['ocultas_nombres'] = list(ocultas)
        save_favoritas_config(config)
        
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/ligas_favoritas_completo')
def api_get_ligas_favoritas_completo():
    """Obtiene la lista completa de ligas favoritas y ocultas (por ID y por nombre)."""
    try:
        config = load_favoritas_config()
        
        # Ligas por ID (predefinidas)
        favoritas_ids = []
        ocultas_ids = []
        
        for liga_id in config.get('favoritas', []):
            for nombre, info in TODAS_LIGAS_CONFIG.items():
                if info['id'] == liga_id:
                    favoritas_ids.append({
                        'id': liga_id,
                        'nombre': nombre,
                        'short': info['short'],
                        'color': info['color']
                    })
                    break
        
        for liga_id in config.get('ocultas', []):
            for nombre, info in TODAS_LIGAS_CONFIG.items():
                if info['id'] == liga_id:
                    ocultas_ids.append({
                        'id': liga_id,
                        'nombre': nombre,
                        'short': info['short'],
                        'color': info['color']
                    })
                    break
        
        # Ligas por nombre (cualquier liga)
        favoritas_nombres = config.get('favoritas_nombres', [])
        ocultas_nombres = config.get('ocultas_nombres', [])
        neutras_nombres = config.get('neutras_nombres', [])
        
        return jsonify({
            'favoritas': favoritas_ids,
            'ocultas': ocultas_ids,
            'favoritas_nombres': favoritas_nombres,
            'ocultas_nombres': ocultas_nombres,
            'neutras_nombres': neutras_nombres
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/favoritas_matches')
def api_favoritas_matches():
    """
    Devuelve partidos de las ligas favoritas del usuario (IDs y Nombres).
    Usa la misma lógica que /api/grandes_ligas pero para la selección del usuario.
    """
    try:
        config = load_favoritas_config()
        favoritas_ids = set(config.get('favoritas', []))
        favoritas_nombres = [n.lower() for n in config.get('favoritas_nombres', [])]
        
        # Construir diccionario de ligas a scrapear
        ligas_to_scrape = {}
        
        # 1. Por ID (predefinidas)
        ids_predefinidas = set()
        for nombre, info in TODAS_LIGAS_CONFIG.items():
            if info['id'] in favoritas_ids:
                ligas_to_scrape[nombre] = info
                ids_predefinidas.add(info['id'])
        
        # 2. Por ID (Custom / Orphan)
        for fid in favoritas_ids:
            if fid not in ids_predefinidas:
                ligas_to_scrape[f"Liga ID {fid}"] = {'id': fid}
        
        # 3. Por Nombre - búsqueda flexible (coincidencia parcial)
        for fav_nombre in favoritas_nombres:
            # Buscar coincidencia parcial en TODAS_LIGAS_CONFIG
            for config_nombre, info in TODAS_LIGAS_CONFIG.items():
                if config_nombre in ligas_to_scrape:
                    continue  # Ya añadida
                config_lower = config_nombre.lower()
                # Coincidencia parcial: si alguna palabra clave está presente
                if (fav_nombre in config_lower or 
                    config_lower in fav_nombre or
                    any(word in config_lower for word in fav_nombre.split() if len(word) > 3)):
                    ligas_to_scrape[config_nombre] = info
                    print(f"✅ Liga '{fav_nombre}' matchea con '{config_nombre}'")

        if not ligas_to_scrape:
            return jsonify({
                'matches': [],
                'ligas': [],
                'total': 0,
                'message': 'No hay ligas favoritas con datos de calendario'
            })
        
        all_matches = []
        now = datetime.datetime.now()
        max_date = now + datetime.timedelta(days=7)
        
        # Cargar datos ya precacheados
        precache_data = load_favoritas_precache()
        
        session = _get_shared_requests_session()
        
        for liga_name, liga_info in ligas_to_scrape.items():
            try:
                lid = liga_info.get('id')
                url = liga_info.get('url')
                
                # Si la liga tiene URL definida en TODAS_LIGAS_CONFIG, usarla
                if url:
                    print(f"🌍 [FAVORITAS] Descargando {liga_name} (ID: {lid})...")
                    response = session.get(url, timeout=10)
                    if response.status_code != 200:
                        continue
                else:
                    # Si no tiene URL (es un ID custom), intentar el patrón genérico
                    # Si no tiene URL (es un ID custom), intentar varios patrones de temporada
                    seasons = ['2025-2026', '2024-2025', '2025', '2024', '2023-2024', '2023']
                    success = False
                    
                    # 1. Intentar encontrar el JS exacto desde la página de la liga (más fiable para IDs con sufijo como 273_462)
                    try:
                        subleague_url = f'https://football.nowgoal26.com/subleague/{lid}'
                        print(f"🌍 [FAVORITAS] Buscando JS dinámico en {subleague_url}...")
                        sub_res = session.get(subleague_url, timeout=8)
                        if sub_res.status_code == 200:
                            # Buscar patrón como /jsData/matchResult/2025-2026/s273_462_en.js
                            js_uri_match = re.search(fr'/jsData/matchResult/([^/]+)/s{lid}(_[^/.]*)?_en\.js', sub_res.text)
                            if js_uri_match:
                                season_found = js_uri_match.group(1)
                                suffix = js_uri_match.group(2) or ""
                                found_url = f'https://football.nowgoal26.com/jsData/matchResult/{season_found}/s{lid}{suffix}_en.js'
                                print(f"🎯 [FAVORITAS] Encontrado JS exacto: {found_url}")
                                response = session.get(found_url, timeout=8)
                                if response.status_code == 200 and 'var arrMatch =' in response.text:
                                    success = True
                    except Exception as e:
                        print(f"⚠️ Error buscando JS dinámico: {e}")

                    # 2. Reintentos manuales por temporada si falló lo anterior
                    if not success:
                        for season in seasons:
                            url_base = f'https://football.nowgoal26.com/jsData/matchResult/{season}/s{lid}_en.js'
                            print(f"🌍 [FAVORITAS] Probando ID Custom {lid} (Temp {season})...")
                            try:
                                response = session.get(url_base, timeout=8)
                                if response.status_code == 200 and 'var arrMatch =' in response.text:
                                    success = True
                                    break
                            except:
                                continue
                    
                    if not success:
                        print(f"❌ [FAVORITAS] No se encontró calendario para ID {lid} en ninguna temporada.")
                        continue
                        
                js_content = response.text
                teams = {}
                
                # 1. Extraer equipos (mejorado para ser más robusto con los índices)
                # Formato: [ID,'Abbr','Name','FullName',...]
                team_match = re.search(r'var arrTeam = \[(.*?)\];', js_content, re.DOTALL)
                if team_match:
                    # Extraer cada bloque [ID,...]
                    team_blocks = re.findall(r'\[(\d+),(.*?)\]', team_match.group(1))
                    for tid, tdata in team_blocks:
                        # Limpiar y buscar el primer nombre no vacío de los índices 1, 2 o 3
                        # Generalmente el nombre completo está en el índice 3 o 2
                        parts_t = tdata.split(',')
                        tname = f"Team {tid}"
                        for idx in [3, 2, 1]:
                            if len(parts_t) > idx:
                                n = parts_t[idx].strip().strip("'")
                                if n:
                                    tname = n
                                    break
                        teams[int(tid)] = tname
                
                # 2. Extraer el nombre real de la liga si es custom o falta
                if not url or liga_name.startswith("Liga ID"):
                    # Intentar var leagueName o arrLeague
                    league_match = re.search(r'var\s+leagueName\s*=\s*\'([^\']+)\';', js_content)
                    if league_match:
                        liga_name = league_match.group(1)
                    else:
                        # var arrLeague = [ID,'','','Name',...]
                        arr_league_match = re.search(r'var arrLeague = \[\d+,.*?,.*?,.*?\'([^\']+)\'', js_content)
                        if arr_league_match:
                            liga_name = arr_league_match.group(1)

                round_matches = re.findall(r'jh\["R_\d+"\]\s*=\s*\[(.*?)\];', js_content, re.DOTALL)
                for round_data in round_matches:
                    matches_raw = re.findall(r'\[([^\[\]]+)\]', round_data)
                    for match_raw in matches_raw:
                        parts = match_raw.split(',')
                        if len(parts) < 14: continue
                        
                        try:
                            match_id = parts[0].strip()
                            date_str = parts[3].strip().strip("'")
                            home_id = int(parts[4].strip())
                            away_id = int(parts[5].strip())
                            score = parts[6].strip().strip("'")
                            home_rank = parts[8].strip().strip("'")
                            away_rank = parts[9].strip().strip("'")
                            ah = parts[10].strip()
                            ou = parts[12].strip().strip("'")
                            
                            try:
                                match_time = datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M')
                            except:
                                continue
                            
                            if match_time < now or match_time > max_date:
                                continue
                            
                            clean_score = score.replace("'", "").strip()
                            if clean_score and clean_score != '-' and ':' not in clean_score and '-' in clean_score:
                                continue
                            
                            # Convertir de GMT+8 a CET (UTC+1)
                            spain_time = match_time - datetime.timedelta(hours=7)
                            date_key = spain_time.strftime('%Y-%m-%d')
                            
                            # Buscar si ya tenemos datos de análisis para este partido en el precache
                            is_precached = False
                            if date_key in precache_data.get('matches_by_date', {}):
                                exists = next((m for m in precache_data['matches_by_date'][date_key] if str(m['id']) == str(match_id)), None)
                                if exists and exists.get('precached'):
                                    is_precached = True
                            
                            all_matches.append({
                                'id': match_id,
                                'league': liga_name,
                                'liga_short': liga_info.get('short', 'L'),
                                'liga_color': liga_info.get('color', '#666'),
                                'liga_id': liga_info.get('id'),
                                'home_team': teams.get(home_id, f"Team {home_id}"),
                                'away_team': teams.get(away_id, f"Team {away_id}"),
                                'home_rank': home_rank.replace("'", ""),
                                'away_rank': away_rank.replace("'", ""),
                                'handicap': ah,
                                'goal_line': ou.replace("'", ""),
                                'time': spain_time.strftime('%H:%M'),
                                'start_time': spain_time.isoformat(),
                                'precached': is_precached
                            })
                        except:
                            continue
                
            except Exception as e:
                print(f"❌ [FAVORITAS] Error con {liga_name}: {e}")
                continue
        
        all_matches.sort(key=lambda x: x.get('start_time', ''))
        
        return jsonify({
            'matches': all_matches,
            'ligas': list(set(m['league'] for m in all_matches)),
            'total': len(all_matches)
        })
        
    except Exception as e:
        print(f"❌ [FAVORITAS] Error: {e}")
        return jsonify({'error': str(e)}), 500


# ===============================
# SISTEMA DE PRECACHEO EN SEGUNDO PLANO
# ===============================
FAVORITAS_PRECACHE_KEY = 'app_favoritas_precache_v1'
FAVORITAS_PRECACHE_LEGACY_FILE = Path(__file__).resolve().parent / 'favoritas_precache.json'
_favoritas_precache_lock = threading.Lock()
_favoritas_precache_state = {
    'running': False,
    'progress': 0,
    'total': 0,
    'current_match': '',
    'should_stop': False
}

def load_favoritas_precache():
    """Carga el estado de precacheo de favoritas desde SQL."""
    with _favoritas_precache_lock:
        data = sql_store.get_json_state(FAVORITAS_PRECACHE_KEY, default=None)
        if data is None and FAVORITAS_PRECACHE_LEGACY_FILE.exists():
            try:
                with open(FAVORITAS_PRECACHE_LEGACY_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                sql_store.set_json_state(FAVORITAS_PRECACHE_KEY, data)
                print(f"Legacy favoritas precache imported from {FAVORITAS_PRECACHE_LEGACY_FILE}")
            except Exception as exc:
                print(f"Warning importing legacy favoritas precache: {exc}")
                data = None

        if not isinstance(data, dict):
            data = {'matches_by_date': {}, 'last_update': None}
        if 'matches_by_date' not in data or not isinstance(data.get('matches_by_date'), dict):
            data['matches_by_date'] = {}
        if 'last_update' not in data:
            data['last_update'] = None
        return data

def save_favoritas_precache(data):
    """Guarda el estado de precacheo de favoritas en SQL."""
    with _favoritas_precache_lock:
        data['last_update'] = datetime.datetime.now().isoformat()
        sql_store.set_json_state(FAVORITAS_PRECACHE_KEY, data)

def _precache_worker(matches_to_scrape):
    """Worker que ejecuta el precacheo en segundo plano."""
    global _favoritas_precache_state
    
    _favoritas_precache_state['running'] = True
    _favoritas_precache_state['total'] = len(matches_to_scrape)
    _favoritas_precache_state['progress'] = 0
    _favoritas_precache_state['should_stop'] = False
    
    precache_data = load_favoritas_precache()
    
    for idx, match in enumerate(matches_to_scrape):
        if _favoritas_precache_state['should_stop']:
            print(f"⏹️ [PRECACHE] Detenido por usuario en {idx}/{len(matches_to_scrape)}")
            break
            
        match_id = match['id']
        _favoritas_precache_state['current_match'] = f"{match['home_team']} vs {match['away_team']}"
        _favoritas_precache_state['progress'] = idx + 1
        
        try:
            # Obtener fecha para organizar
            date_key = match.get('date', 'unknown')
            
            # Verificar si ya está precacheado
            if date_key in precache_data['matches_by_date']:
                existing = next((m for m in precache_data['matches_by_date'][date_key] if m['id'] == match_id), None)
                if existing and existing.get('precached'):
                    print(f"✅ [PRECACHE] {match_id} ya existe, saltando...")
                    continue
            
            # Scrapear el partido
            print(f"🔄 [PRECACHE] [{idx+1}/{len(matches_to_scrape)}] {match['home_team']} vs {match['away_team']}")
            result = analizar_partido_completo(str(match_id))
            
            if result and not result.get('error'):
                # Crear entrada con datos básicos + completos
                match_entry = {
                    'id': match_id,
                    'home_team': match['home_team'],
                    'away_team': match['away_team'],
                    'time': match.get('time', ''),
                    'liga': match.get('league', ''),
                    'liga_short': match.get('liga_short', ''),
                    'liga_color': match.get('liga_color', '#666'),
                    'home_rank': match.get('home_rank', ''),
                    'away_rank': match.get('away_rank', ''),
                    'handicap': match.get('handicap', ''),
                    'goal_line': match.get('goal_line', ''),
                    'precached': True,
                    'precached_at': datetime.datetime.now().isoformat(),
                    'data': result
                }
                
                # Añadir a la fecha correspondiente
                if date_key not in precache_data['matches_by_date']:
                    precache_data['matches_by_date'][date_key] = []
                
                # Reemplazar si existe, añadir si no
                existing_idx = next((i for i, m in enumerate(precache_data['matches_by_date'][date_key]) if m['id'] == match_id), None)
                if existing_idx is not None:
                    precache_data['matches_by_date'][date_key][existing_idx] = match_entry
                else:
                    precache_data['matches_by_date'][date_key].append(match_entry)
                
                # Guardar inmediatamente
                save_favoritas_precache(precache_data)
                print("   ✅ Guardado en SQL (favoritas precache)")
            else:
                print(f"   ❌ Error scrapeando {match_id}")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
            continue
    
    _favoritas_precache_state['running'] = False
    _favoritas_precache_state['current_match'] = ''
    print(f"🏁 [PRECACHE] Completado: {_favoritas_precache_state['progress']}/{_favoritas_precache_state['total']}")


@app.route('/api/favoritas_precache/matches')
def api_favoritas_precache_matches():
    """Devuelve los partidos precacheados organizados por fecha."""
    try:
        data = load_favoritas_precache()
        return jsonify({
            'matches_by_date': data.get('matches_by_date', {}),
            'last_update': data.get('last_update'),
            'status': {
                'running': _favoritas_precache_state['running'],
                'progress': _favoritas_precache_state['progress'],
                'total': _favoritas_precache_state['total'],
                'current_match': _favoritas_precache_state['current_match']
            }
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500





@app.route('/api/favoritas_precache/start', methods=['POST'])
def api_favoritas_precache_start():
    """Inicia el precacheo de partidos de ligas favoritas en segundo plano."""
    global _favoritas_precache_state
    
    if _favoritas_precache_state['running']:
        return jsonify({'error': 'Ya hay un precacheo en curso', 'running': True}), 400
    
    try:
        config = load_favoritas_config()
        favoritas_ids = list(config.get('favoritas', []))
        favoritas_nombres = [n.lower() for n in config.get('favoritas_nombres', [])]
        
        ligas_to_scrape = {}
        # 1. Mapear ligas predefinidas de TODAS_LIGAS_CONFIG por ID
        ids_predefinidas = []
        for nombre, info in TODAS_LIGAS_CONFIG.items():
            if info['id'] in favoritas_ids:
                ligas_to_scrape[nombre] = info
                ids_predefinidas.append(info['id'])
        
        # 2. Añadir IDs custom que no están en la config predefinida
        for fid in favoritas_ids:
            if fid not in ids_predefinidas:
                ligas_to_scrape[f"Liga {fid}"] = {'id': fid}
        
        # 3. Por Nombre
        for fav_nombre in favoritas_nombres:
            for config_nombre, info in TODAS_LIGAS_CONFIG.items():
                if config_nombre in ligas_to_scrape:
                    continue
                config_lower = config_nombre.lower()
                if (fav_nombre in config_lower or 
                    config_lower in fav_nombre or
                    any(word in config_lower for word in fav_nombre.split() if len(word) > 3)):
                    ligas_to_scrape[config_nombre] = info
        
        if not ligas_to_scrape:
            return jsonify({'error': 'No hay ligas favoritas configuradas con ID localizable.'}), 400
        
        all_matches = []
        now = datetime.datetime.now()
        max_date = now + datetime.timedelta(days=7)
        session = _get_shared_requests_session()
        
        for liga_name, liga_info in ligas_to_scrape.items():
            try:
                lid = liga_info.get('id')
                url = liga_info.get('url')
                response = None
                
                if url:
                    response = session.get(url, timeout=10)
                
                if not response or response.status_code != 200:
                    # Intentar búsqueda dinámica en subleague (id custom o url fallida)
                    try:
                        subleague_url = f'https://football.nowgoal26.com/subleague/{lid}'
                        sub_res = session.get(subleague_url, timeout=8)
                        if sub_res.status_code == 200:
                            js_uri_match = re.search(fr'/jsData/matchResult/([^/]+)/s{lid}(_[^/.]*)?_en\.js', sub_res.text)
                            if js_uri_match:
                                season_found = js_uri_match.group(1)
                                suffix = js_uri_match.group(2) or ""
                                found_url = f'https://football.nowgoal26.com/jsData/matchResult/{season_found}/s{lid}{suffix}_en.js'
                                response = session.get(found_url, timeout=8)
                    except:
                        pass
                
                if not response or response.status_code != 200:
                    # Reintentos manuales por temporada (fallback final)
                    seasons = ['2025-2026', '2024-2025', '2025', '2024']
                    for season in seasons:
                        url_base = f'https://football.nowgoal26.com/jsData/matchResult/{season}/s{lid}_en.js'
                        r = session.get(url_base, timeout=8)
                        if r.status_code == 200 and 'var arrMatch =' in r.text:
                            response = r
                            break
                
                if not response or response.status_code != 200:
                    continue
                js_content = response.text
                teams = {}
                
                # Extraer equipos (robusto)
                team_match = re.search(r'var arrTeam = \[(.*?)\];', js_content, re.DOTALL)
                if team_match:
                    team_blocks = re.findall(r'\[(\d+),(.*?)\]', team_match.group(1))
                    for tid, tdata in team_blocks:
                        parts_t = tdata.split(',')
                        tname = f"Team {tid}"
                        for idx in [3, 2, 1]:
                            if len(parts_t) > idx:
                                n = parts_t[idx].strip().strip("'")
                                if n:
                                    tname = n
                                    break
                        teams[int(tid)] = tname
                
                if liga_name.startswith("Liga "):
                    league_match = re.search(r'var\s+leagueName\s*=\s*\'([^\']+)\';', js_content)
                    if league_match:
                        liga_name = league_match.group(1)
                    else:
                        arr_league_match = re.search(r'var arrLeague = \[\d+,.*?,.*?,.*?\'([^\']+)\'', js_content)
                        if arr_league_match:
                            liga_name = arr_league_match.group(1)
                
                round_matches = re.findall(r'jh\["R_\d+"\]\s*=\s*\[(.*?)\];', js_content, re.DOTALL)
                for round_data in round_matches:
                    matches_raw = re.findall(r'\[([^\[\]]+)\]', round_data)
                    for match_raw in matches_raw:
                        parts = match_raw.split(',')
                        if len(parts) < 14: continue
                        try:
                            match_id = parts[0].strip()
                            date_str = parts[3].strip().strip("'")
                            match_time = datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M')
                            if match_time < now or match_time > max_date: continue
                            
                            score = parts[6].strip().strip("'")
                            clean_score = score.replace("'", "").strip()
                            if clean_score and clean_score != '-' and ':' not in clean_score and '-' in clean_score: continue
                            
                            spain_time = match_time - datetime.timedelta(hours=7)
                            all_matches.append({
                                'id': match_id,
                                'league': liga_name,
                                'liga_short': liga_info.get('short', 'L'),
                                'liga_color': liga_info.get('color', '#666'),
                                'home_team': teams.get(int(parts[4].strip()), f"Team {parts[4]}"),
                                'away_team': teams.get(int(parts[5].strip()), f"Team {parts[5]}"),
                                'home_rank': parts[8].strip().replace("'", ""),
                                'away_rank': parts[9].strip().replace("'", ""),
                                'handicap': parts[10].strip(),
                                'goal_line': parts[12].strip().replace("'", ""),
                                'time': spain_time.strftime('%H:%M'),
                                'date': spain_time.strftime('%Y-%m-%d'),
                            })
                        except: continue
            except: continue
        
        if not all_matches:
            return jsonify({'error': 'No se encontraron partidos próximos.'}), 400
        
        all_matches.sort(key=lambda x: (x.get('date', ''), x.get('time', '')))
        
        # Persistir estructura básica al precache si no existe
        precache_data = load_favoritas_precache()
        for match in all_matches:
            date_key = match['date']
            if date_key not in precache_data['matches_by_date']:
                precache_data['matches_by_date'][date_key] = []
            
            existing = next((m for m in precache_data['matches_by_date'][date_key] if str(m['id']) == str(match['id'])), None)
            if not existing:
                precache_data['matches_by_date'][date_key].append({
                    'id': match['id'],
                    'home_team': match['home_team'],
                    'away_team': match['away_team'],
                    'time': match['time'],
                    'liga': match['league'],
                    'liga_short': match['liga_short'],
                    'liga_color': match['liga_color'],
                    'home_rank': match['home_rank'],
                    'away_rank': match['away_rank'],
                    'handicap': match['handicap'],
                    'goal_line': match['goal_line'],
                    'precached': False
                })
        save_favoritas_precache(precache_data)
        
        thread = threading.Thread(target=_precache_worker, args=(all_matches,), daemon=True)
        thread.start()
        
        return jsonify({'message': 'Precacheo de favoritas iniciado', 'total': len(all_matches), 'running': True})
        
    except Exception as e:
        print(f"❌ Error iniciando precacheo: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/favoritas_precache/status')
def api_favoritas_precache_status():
    """Devuelve el estado actual del precacheo."""
    return jsonify({
        'running': _favoritas_precache_state['running'],
        'progress': _favoritas_precache_state['progress'],
        'total': _favoritas_precache_state['total'],
        'current_match': _favoritas_precache_state['current_match']
    })


@app.route('/api/favoritas_precache/stop', methods=['POST'])
def api_favoritas_precache_stop():
    """Detiene el precacheo en curso."""
    global _favoritas_precache_state
    _favoritas_precache_state['should_stop'] = True
    return jsonify({'message': 'Señal de parada enviada'})



PRONOSTICOS_STATE_KEY = 'app_pronosticos_state_v1'
PRONOSTICOS_LEGACY_FILE = Path(__file__).resolve().parent / 'pronosticos_state.json'
_pronosticos_lock = threading.Lock()

def load_pronosticos_state():
    """Carga el estado de pronósticos realizados desde SQL."""
    with _pronosticos_lock:
        state = sql_store.get_json_state(PRONOSTICOS_STATE_KEY, default=None)
        if state is None and PRONOSTICOS_LEGACY_FILE.exists():
            try:
                with open(PRONOSTICOS_LEGACY_FILE, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                sql_store.set_json_state(PRONOSTICOS_STATE_KEY, state)
                print(f"Legacy pronosticos state imported from {PRONOSTICOS_LEGACY_FILE}")
            except Exception as exc:
                print(f"Warning importing legacy pronosticos state: {exc}")
                state = None
    if not isinstance(state, dict):
        state = {'pronosticados': []}
    if 'pronosticados' not in state or not isinstance(state.get('pronosticados'), list):
        state['pronosticados'] = []
    return state

def save_pronosticos_state(state):
    """Guarda el estado de pronósticos en SQL."""
    with _pronosticos_lock:
        sql_store.set_json_state(PRONOSTICOS_STATE_KEY, state)


@app.route('/api/pronosticos', methods=['GET'])
def api_get_pronosticos():
    """Obtiene la lista de partidos pronosticados."""
    state = load_pronosticos_state()
    return jsonify(state)


@app.route('/api/pronosticos', methods=['POST'])
def api_save_pronostico():
    """Marca/desmarca un partido como pronosticado."""
    try:
        data = request.get_json() or {}
        match_id = data.get('match_id')
        pronosticado = data.get('pronosticado', True)
        
        if not match_id:
            return jsonify({'error': 'Falta match_id'}), 400
        
        state = load_pronosticos_state()
        pronosticados = set(state.get('pronosticados', []))
        
        if pronosticado:
            pronosticados.add(str(match_id))
        else:
            pronosticados.discard(str(match_id))
        
        state['pronosticados'] = list(pronosticados)
        save_pronosticos_state(state)
        
        return jsonify({'success': True, 'pronosticados': state['pronosticados']})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# --- CACHE STATE PERSISTENCE ---

CACHE_STATE_KEY = 'app_cache_state_v1'
CACHE_STATE_LEGACY_FILE = Path(__file__).resolve().parent / 'cache_state.json'
_cache_state_lock = threading.Lock()

def load_cache_state():
    with _cache_state_lock:
        state = sql_store.get_json_state(CACHE_STATE_KEY, default=None)
        if state is None and CACHE_STATE_LEGACY_FILE.exists():
            try:
                with open(CACHE_STATE_LEGACY_FILE, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                sql_store.set_json_state(CACHE_STATE_KEY, state)
                print(f"Legacy cache_state imported from {CACHE_STATE_LEGACY_FILE}")
            except Exception as exc:
                print(f"Warning importing legacy cache_state: {exc}")
                state = None
    if not isinstance(state, dict):
        state = {'processed_ids': []}
    if 'processed_ids' not in state or not isinstance(state.get('processed_ids'), list):
        state['processed_ids'] = []
    return state

def save_cache_state(state):
    with _cache_state_lock:
        try:
            sql_store.set_json_state(CACHE_STATE_KEY, state)
        except Exception as e:
            print(f"Error saving cache state: {e}")

def add_processed_id(match_id):
    state = load_cache_state()
    if str(match_id) not in state['processed_ids']:
        state['processed_ids'].append(str(match_id))
        save_cache_state(state)


# --- PRE-CACHE STATE PERSISTENCE ---
PRECACHE_STATE_KEY = 'app_precache_state_v1'
PRECACHE_STATE_LEGACY_FILE = Path(__file__).resolve().parent / 'precache_state.json'
_precache_state_lock = threading.Lock()

def load_precache_state():
    with _precache_state_lock:
        state = sql_store.get_json_state(PRECACHE_STATE_KEY, default=None)
        if state is None and PRECACHE_STATE_LEGACY_FILE.exists():
            try:
                with open(PRECACHE_STATE_LEGACY_FILE, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                sql_store.set_json_state(PRECACHE_STATE_KEY, state)
                print(f"Legacy precache_state imported from {PRECACHE_STATE_LEGACY_FILE}")
            except Exception as exc:
                print(f"Warning importing legacy precache_state: {exc}")
                state = None
    if not isinstance(state, dict):
        state = {'processed_ids': []}
    if 'processed_ids' not in state or not isinstance(state.get('processed_ids'), list):
        state['processed_ids'] = []
    return state

def save_precache_state(state):
    with _precache_state_lock:
        try:
            sql_store.set_json_state(PRECACHE_STATE_KEY, state)
        except Exception as e:
            print(f"Error saving precache state: {e}")

def add_precache_processed_id(match_id):
    state = load_precache_state()
    if str(match_id) not in state['processed_ids']:
        state['processed_ids'].append(str(match_id))
        save_precache_state(state)

def process_single_match_worker(match_id):
    """Worker function for single match processing."""
    try:
        # Check if already processed in this session/state
        # (Though we check before submitting, keeping it robust)
        
        # Analyze
        match_data = analizar_partido_completo(str(match_id))
        if match_data and not match_data.get('error'):
            saved = save_match_to_json(match_data)
            add_processed_id(match_id)
            bucket = sql_store.get_match_bucket(str(match_id)) if saved else None
            status = 'saved' if saved else 'filtered'
            return True, match_id, bucket, status
        else:
            return False, match_id, None, 'scrape_error'
            
    except Exception as e:
        print(f"Error processing {match_id}: {e}")
        return False, match_id, None, str(e)

def process_single_precache_worker(match_id):
    """Worker for upcoming matches (Pre-Cacheo)."""
    try:
        match_data = analizar_partido_completo(str(match_id))
        if match_data and not match_data.get('error'):
             match_data['match_id'] = str(match_id)
             match_data['precacheo_date'] = datetime.datetime.now().isoformat()
             data_manager.save_precacheo_match(match_data)
             add_precache_processed_id(match_id)
             return True, match_id
        else:
            return False, match_id
    except Exception as e:
        print(f"Error precaching {match_id}: {e}")
        return False, match_id

def process_upcoming_matches_background(handicap_filter=None, goal_line_filter=None, workers=5, order_by_recent=True):
    """
    Procesa partidos PRÓXIMOS (Pre-Cacheo) en segundo plano con optimizaciones:
    - Filtros
    - Concurrencia
    - Persistencia (Setpoint)
    - Ordenación por proximidad a hora actual (si order_by_recent=True)
    """
    filter_desc = f"AH={handicap_filter}, OU={goal_line_filter}"
    print(f"Iniciando PRE-CACHEO Background ({filter_desc})...")
    
    # Limpieza automática de partidos antiguos (configurable por env vars)
    _maybe_cleanup_precacheo_stale(force=True)

    try:
        # 1. Obtener TODOS los partidos del día (sin filtro min_time para incluir los que ya empezaron)
        # Esto permite scrapear también los que están pendientes de resultado
        matches = asyncio.run(get_main_page_matches_async(
            limit=2000, 
            offset=0, 
            handicap_filter=handicap_filter, 
            goal_line_filter=goal_line_filter,
            min_time=None  # Sin filtro de tiempo para incluir todos
        ))
        
        if not matches:
            print("⚠️ [BACKGROUND] No hay partidos en caché local. Intentando scraping directo...")
            matches = asyncio.run(scrape_main_page_matches_async_direct(
                limit=2000, 
                offset=0, 
                handicap_filter=handicap_filter, 
                goal_line_filter=goal_line_filter,
                min_time=None
            ))
        
        print(f"Se encontraron {len(matches)} partidos próximos candidatos.")
        
        # 2. Cargar estado
        state = load_precache_state()
        processed_ids = set(state.get('processed_ids', []))
        
        # 3. Filtrar los que ya están hechos y preparar para ordenar por proximidad temporal
        # Checkeo rápido contra el archivo (state) es más eficiente que cargar todo data_manager
        
        now = datetime.datetime.now()
        candidates = []  # Lista de (match_id, start_time_str, distance_from_now)
        
        for m in matches:
            mid = str(m.get('id') or m.get('match_id'))
            
            # FIX: Verificar si REALMENTE tenemos datos, no solo si el state dice que sí.
            exists_in_data = False
            is_processed = mid in processed_ids
            if is_processed:
                 # Doble check: ¿está en el archivo de precacheo?
                 cached_match = data_manager.get_precacheo_match(mid)
                 if cached_match:
                     exists_in_data = True
            
            if mid and not exists_in_data:
                # Extraer hora de inicio para ordenar
                start_time_str = m.get('start_time')
                try:
                    if start_time_str:
                        # ISO format: YYYY-MM-DDTHH:MM:SS
                        match_time = datetime.datetime.fromisoformat(start_time_str)
                        # Distancia absoluta desde ahora (para ordenar por proximidad)
                        distance = abs((match_time - now).total_seconds())
                    else:
                        distance = float('inf')  # Sin hora, al final
                except:
                    distance = float('inf')
                
                candidates.append((mid, distance))
        
        # 4. Ordenar por proximidad a la hora actual (más cercano primero)
        if order_by_recent:
            candidates.sort(key=lambda x: x[1])  # Ordenar por distancia temporal
            print(f"Ordenando partidos por proximidad temporal (más cercanos primero)...")
        
        # Extraer solo los IDs
        to_process = [c[0] for c in candidates]
                
        print(f"De los cuales {len(to_process)} son nuevos y se scrapearán.")
        
        if not to_process:
            print("Nada nuevo que scrapear en Pre-Cacheo.")
            return

        # 5. Procesar en paralelo PERO manteniendo orden de proximidad
        # Usamos chunks del tamaño de workers para procesar en batches ordenados
        max_workers = workers if workers else 5 
        total = len(to_process)
        completed = 0
        
        print(f"Iniciando Pool Pre-Cacheo con {max_workers} workers (orden: más cercanos primero)...")
        
        # Procesar en batches ordenados para garantizar que los más cercanos se hagan primero
        batch_size = max_workers * 2  # Procesar en batches un poco más grandes para eficiencia
        
        for batch_start in range(0, total, batch_size):
            if STOP_CACHE_EVENT.is_set():
                print("Señal de parada recibida (Pre-Cacheo). Deteniendo...")
                break
                
            batch = to_process[batch_start:batch_start + batch_size]
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(process_single_precache_worker, mid): mid for mid in batch}
                
                for future in concurrent.futures.as_completed(futures):
                    if STOP_CACHE_EVENT.is_set():
                        print("Señal de parada recibida (Pre-Cacheo). Cancelando tareas...")
                        executor.shutdown(wait=False, cancel_futures=True)
                        break

                    mid = futures[future]
                    try:
                        success, _ = future.result()
                        completed += 1
                        
                        if completed % 5 == 0 or completed == total:
                            print(f"Progreso Pre-Cacheo: {completed}/{total} procesados.")
                            
                    except Exception as e:
                        print(f"Excepción en worker Pre-Cacheo {mid}: {e}")
                    
        if STOP_CACHE_EVENT.is_set():
             print(f"Pre-Cacheo detenido. {completed} partidos completados.")
        else:
             print(f"Pre-Cacheo finalizado. {completed} partidos intentados.")
        
    except Exception as e:
        print(f"Error fatal en Pre-Cacheo background: {e}")

def scrape_pending_results_background():
    """
    Scrapea SOLO el resultado final de partidos ya pre-cacheados que:
    - Empezaron hace +30 minutos
    - No tienen resultado válido (score es ?:? o ??)
    """
    print("Iniciando SCRAPE de resultados pendientes...")
    
    try:
        import pytz
        
        # 1. Cargar todos los partidos pre-cacheados
        precacheo_matches = data_manager.load_precacheo_matches()
        
        if not precacheo_matches:
            print("No hay partidos en pre-cacheo.")
            return
            
        print(f"Encontrados {len(precacheo_matches)} partidos en pre-cacheo.")
        
        # 2. Hora actual en España
        spain_tz = pytz.timezone('Europe/Madrid')
        now_spain = datetime.datetime.now(spain_tz)
        thirty_minutes_ago = now_spain - datetime.timedelta(minutes=30)
        
        print(f"Hora España: {now_spain.strftime('%H:%M')}, buscando partidos que empezaron antes de {thirty_minutes_ago.strftime('%H:%M')}")
        
        # 3. Filtrar: partidos sin resultado que empezaron hace +30 minutos
        to_process = []
        today_str = now_spain.strftime('%Y-%m-%d')
        
        for m in precacheo_matches:
            # Verificar que no tenga resultado
            score = m.get('score') or m.get('final_score') or ''
            if score and score not in ['??', '?:?', '? : ?', '?-?'] and ':' in score:
                continue  # Ya tiene resultado, saltar
            
            # Verificar hora del partido
            match_time_str = m.get('match_time') or m.get('time')
            match_date_str = m.get('match_date') or today_str
            
            if not match_time_str:
                continue
                
            try:
                # Parsear hora del partido
                if ':' in match_time_str:
                    h, mi = map(int, match_time_str.split(':'))
                else:
                    continue
                    
                # match_date llega normalmente como M/D/YYYY desde Precacheo.
                parsed_date = data_manager.parse_match_date(match_date_str)
                if parsed_date is None:
                    continue
                match_dt = spain_tz.localize(parsed_date.replace(hour=h, minute=mi))
                
                # Solo si empezó hace +30 minutos
                if match_dt <= thirty_minutes_ago:
                    mid = m.get('match_id')
                    if mid:
                        to_process.append(mid)
                        print(f"  Pendiente: {m.get('home_name')} vs {m.get('away_name')} ({match_time_str})")
            except Exception as e:
                # Si no puede parsear la hora, asumir que puede ser candidato
                mid = m.get('match_id')
                if mid:
                    to_process.append(mid)
        
        print(f"Partidos a scrapear para resultado: {len(to_process)}")
        
        if not to_process:
            print("No hay partidos pendientes de resultado (todos recientes o ya tienen score).")
            return
        
        # 4. Scrapear en paralelo con 8 workers
        success_count = 0
        max_workers = 8
        total = len(to_process)
        completed = 0
        
        print(f"Iniciando scrape de resultados con {max_workers} workers...")
        
        def scrape_single_result(mid):
            """Worker para scrapear un solo partido."""
            try:
                match_data = analizar_partido_completo(str(mid), force_refresh=True)
                
                if match_data and not match_data.get('error'):
                    new_score = match_data.get('score') or match_data.get('final_score')
                    
                    if new_score and new_score not in ['??', '?:?', '? : ?']:
                        match_data['match_id'] = str(mid)
                        data_manager.save_precacheo_match(match_data)
                        return (True, mid, new_score)
                    else:
                        return (False, mid, "Sin resultado aún")
                return (False, mid, "Error scraping")
            except Exception as e:
                return (False, mid, str(e))
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(scrape_single_result, mid): mid for mid in to_process}
            
            for future in concurrent.futures.as_completed(futures):
                mid = futures[future]
                completed += 1
                try:
                    success, match_id, info = future.result()
                    if success:
                        print(f"  ✓ [{completed}/{total}] {match_id}: {info}")
                        success_count += 1
                    else:
                        print(f"  ✗ [{completed}/{total}] {match_id}: {info}")
                except Exception as e:
                    print(f"  ✗ [{completed}/{total}] {mid}: Error - {e}")
                
        print(f"Scrape de resultados completado. {success_count}/{total} obtuvieron resultado.")
        
    except Exception as e:
        print(f"Error fatal en scrape de resultados pendientes: {e}")


def process_all_finished_matches_background(
    handicap_filter=None,
    goal_line_filter=None,
    workers=5,
    flush_every=0,
    export_legacy=None,
):
    """
    Procesa partidos finalizados en segundo plano con optimizaciones:
    - Filtros
    - Concurrencia (workers configurable)
    - Persistencia (Setpoint)
    - Export incremental de JSON legacy cada N partidos (flush_every)
    """
    filter_desc = f"AH={handicap_filter}, OU={goal_line_filter}"
    if export_legacy is None:
        export_legacy = bool(sql_store.LEGACY_SYNC_ENABLED)
    else:
        export_legacy = bool(export_legacy)

    print(f"Iniciando proceso de cacheo OPTIMIZADO ({filter_desc})...")

    try:
        flush_every = int(flush_every)
    except Exception:
        flush_every = 0
    if flush_every < 0:
        flush_every = 0

    if export_legacy:
        if flush_every == 0:
            flush_every = 5
        print(f"Guardado directo en SQL + export incremental cada {flush_every} partidos.")
    else:
        print("Guardado directo en SQL (export JSON legacy desactivado).")

    def export_changed_buckets(changed_buckets):
        if not export_legacy:
            return
        if not changed_buckets:
            return
        exported = 0
        for bucket in sorted({b for b in changed_buckets if b}):
            try:
                sql_store.export_bucket_to_json(bucket)
                exported += 1
            except Exception as export_err:
                print(f"Warning: Error exportando bucket {bucket}: {export_err}")
        if exported > 0:
            print(f"Export incremental completado ({exported} bucket(s)).")

    try:
        # 1. Obtener partidos (usando filtros si existen)
        # Traemos MUCHOS para filtrar luego si es necesario, o confiamos en el endpoint
        matches = asyncio.run(get_main_page_finished_matches_async(
            limit=2000, 
            offset=0, 
            handicap_filter=handicap_filter, 
            goal_line_filter=goal_line_filter
        ))
        
        if not matches:
            print("⚠️ [BACKGROUND] No hay partidos terminados en caché local. Intentando scraping directo...")
            matches = asyncio.run(scrape_main_page_finished_matches_async_direct(
                limit=2000, 
                offset=0, 
                handicap_filter=handicap_filter, 
                goal_line_filter=goal_line_filter
            ))
        
        print(f"Se encontraron {len(matches)} partidos candidatos.")
        
        # 2. Cargar estado anterior
        state = load_cache_state()
        processed_ids = set(state.get('processed_ids', []))
        
        # 3. Filtrar los que ya están hechos
        to_process = []
        for m in matches:
            mid = str(m.get('id'))
            if mid not in processed_ids:
                to_process.append(mid)
                
        print(f"De los cuales {len(to_process)} son nuevos y se procesarán.")
        
        if not to_process:
            print("Nada nuevo que procesar.")
            return

        # 4. Procesar en paralelo con reparto fijo por worker (misma metodología que análisis previo)
        max_workers = workers
        total = len(to_process)
        state_progress = {
            'completed': 0,
            'changed_buckets': set(),
        }
        progress_lock = threading.Lock()
        
        print(f"Iniciando Pool con {max_workers} workers...")

        # Reparto determinista: worker i procesa to_process[i::max_workers]
        worker_batches = [to_process[i::max_workers] for i in range(max_workers)]
        for worker_idx, batch in enumerate(worker_batches):
            if batch:
                print(f"Worker fijo {worker_idx}: {len(batch)} partidos asignados.")

        def process_worker_batch(worker_idx, batch_ids):
            local_done = 0
            for mid in batch_ids:
                if STOP_CACHE_EVENT.is_set():
                    break

                try:
                    success, _, bucket_name, status = process_single_match_worker(mid)
                except Exception as worker_err:
                    print(f"Excepción en worker fijo {worker_idx} para {mid}: {worker_err}")
                    success, bucket_name = False, None

                local_done += 1

                with progress_lock:
                    state_progress['completed'] += 1
                    completed_now = state_progress['completed']

                    if success and bucket_name:
                        state_progress['changed_buckets'].add(bucket_name)

                    if completed_now % 5 == 0 or completed_now == total:
                        print(f"Progreso: {completed_now}/{total} procesados.")

                    if flush_every > 0 and completed_now % flush_every == 0:
                        export_changed_buckets(state_progress['changed_buckets'])
                        state_progress['changed_buckets'].clear()

            print(f"Worker fijo {worker_idx}: completados {local_done}/{len(batch_ids)}.")
            return local_done

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(process_worker_batch, worker_idx, batch): worker_idx
                for worker_idx, batch in enumerate(worker_batches)
                if batch
            }

            for future in concurrent.futures.as_completed(futures):
                worker_idx = futures[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"Excepción final en worker fijo {worker_idx}: {e}")

        # Flush final de buckets pendientes de export.
        export_changed_buckets(state_progress['changed_buckets'])
                    
        if STOP_CACHE_EVENT.is_set():
            print(f"Proceso detenido. {state_progress['completed']} partidos completados antes de parar.")
        else:
            print(f"Proceso de cacheo finalizado. {state_progress['completed']} partidos intentados.")
        
    except Exception as e:
        print(f"Error fatal en proceso de background: {e}")



# Alias para importar con nombre explícito
process_all_finished_matches_background_with_workers = process_all_finished_matches_background

# --- BACKGROUND CONTROL ---
STOP_CACHE_EVENT = threading.Event()

@app.route('/api/stop_background_cache', methods=['POST'])
def api_stop_background_cache():
    """Endpoint para detener el cacheo en background."""
    STOP_CACHE_EVENT.set()
    return jsonify({'status': 'success', 'message': 'Se ha enviado la señal de parada. El proceso se detendrá pronto.'})

@app.route('/api/cache_all_finished_background', methods=['POST'])
def api_cache_all_finished_background():
    """Endpoint para iniciar el cacheo (acepta filtros)."""
    try:
        # Debug headers
        print(f"DEBUG Headers: {request.headers}")
        # Force JSON parsing even if Content-Type is missing/wrong
        data = request.get_json(force=True, silent=True) or {}
        print(f"DEBUG Payload: {data}")
        handicap_filter = data.get('handicap')
        goal_line_filter = data.get('ou')
        workers = data.get('workers', 5)
        try:
            workers = int(workers)
            if workers <= 0:
                workers = 5
        except Exception:
            workers = 5
        flush_every = data.get('flush_every', 0)
        try:
            flush_every = int(flush_every)
            if flush_every < 0:
                flush_every = 0
        except Exception:
            flush_every = 0
        export_legacy_raw = data.get('export_legacy', sql_store.LEGACY_SYNC_ENABLED)
        if isinstance(export_legacy_raw, str):
            export_legacy = export_legacy_raw.strip().lower() in {'1', 'true', 'yes', 'on'}
        else:
            export_legacy = bool(export_legacy_raw)
        
        # Resetear señal de parada
        STOP_CACHE_EVENT.clear()

        # Iniciar hilo en segundo plano
        thread = threading.Thread(
            target=process_all_finished_matches_background,
            args=(handicap_filter, goal_line_filter, workers, flush_every, export_legacy)
        )
        thread.daemon = True 
        thread.start()
        
        return jsonify({
            'status': 'success', 
            'message': (
                f'Cacheo iniciado (Filtros: AH={handicap_filter}, OU={goal_line_filter}, '
                f'workers={workers}, flush_every={flush_every}, export_legacy={export_legacy}).'
            )
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500



def process_id_ranges_background(ranges_str):
    """Procesa rangos de IDs en segundo plano."""
    print(f"Iniciando cacheo por rangos: {ranges_str}")
    try:
        # Parsear rangos
        ids_to_process = []
        parts = [p.strip() for p in ranges_str.split(',') if p.strip()]
        for part in parts:
            if '-' in part:
                try:
                    start, end = part.split('-')
                    start, end = int(start), int(end)
                    if start > end: start, end = end, start
                    # Limitar rango para evitar locuras
                    if (end - start) > 1000:
                        print(f"Rango demasiado grande ignorado: {part}")
                        continue
                    ids_to_process.extend(range(start, end + 1))
                except ValueError:
                    print(f"Rango invalido ignorado: {part}")
            else:
                try:
                    ids_to_process.append(int(part))
                except ValueError:
                    print(f"ID invalido ignorado: {part}")
        
        # Eliminar duplicados y ordenar
        ids_to_process = sorted(list(set(ids_to_process)))
        print(f"Total IDs a procesar: {len(ids_to_process)}")
        
        count = 0
        for match_id in ids_to_process:
            print(f"Procesando ID {match_id} ({count + 1}/{len(ids_to_process)})...")
            try:
                # Verificar si ya existe en CSV para no repetir (opcional, pero recomendado)
                # Por ahora lo sobrescribimos/añadimos
                
                match_data = analizar_partido_completo(str(match_id))
                if match_data:
                    save_match_to_json(match_data)
                    count += 1
                else:
                    print(f"No se obtuvieron datos para {match_id}")
                
                time.sleep(1) # Pausa respetuosa
            except Exception as e:
                print(f"Error procesando {match_id}: {e}")
        
        print(f"Proceso de rangos finalizado. {count} partidos guardados.")

    except Exception as e:
        print(f"Error fatal en proceso de rangos: {e}")

@app.route('/api/cache_ranges_background', methods=['POST'])
def api_cache_ranges_background():
    """Endpoint para iniciar cacheo por rangos."""
    data = request.json
    ranges = data.get('ranges')
    if not ranges:
        return jsonify({'error': 'Falta el parametro ranges'}), 400
        
    thread = threading.Thread(target=process_id_ranges_background, args=(ranges,))
    thread.daemon = True
    thread.start()
    
    return jsonify({'status': 'success', 'message': 'Proceso de rangos iniciado en segundo plano.'})


@app.route('/api/precacheo_scrape_match', methods=['POST'])
def api_precacheo_scrape_match():
    """Endpoint para scrapear un partido individual desde precacheo."""
    try:
        data = request.json
        match_id = data.get('match_id')
        if not match_id:
            return jsonify({'error': 'Falta match_id'}), 400
            
        print(f"Re-scrapeando partido {match_id} por solicitud de usuario...")
        
        # Forzar refresco para que tome cambios de configuración (ej. liga neutra)
        match_data = analizar_partido_completo(str(match_id), force_refresh=True)
        
        if match_data and 'error' not in match_data:
            save_match_to_json(match_data)
            return jsonify({'status': 'success', 'message': f'Partido {match_id} actualizado.'})
        else:
            return jsonify({'error': 'No se pudieron obtener datos del partido.'}), 500
            
    except Exception as e:
        print(f"Error en precacheo_scrape_match: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/preview_basico/<string:match_id>')
def api_preview_basico(match_id):
    try:
        entry, section = _find_match_basic_data(match_id)
        if not entry:
            return jsonify({'error': 'Partido no encontrado'}), 404
        payload = {
            'id': entry.get('id'),
            'section': section,
            'home_team': entry.get('home_team'),
            'away_team': entry.get('away_team'),
            'time': entry.get('time'),
            'time_obj': entry.get('time_obj'),
            'score': entry.get('score'),
            'handicap': entry.get('handicap'),
            'goal_line': entry.get('goal_line'),
            'goal_line_alt': entry.get('goalLine'),
            'goal_line_decimal': entry.get('goal_line_decimal'),
            'competition': entry.get('competition'),
        }
        return jsonify(payload)
    except Exception as exc:
        return jsonify({'error': f'No se pudo cargar la vista previa: {exc}'}), 500




def _select_default_match_id(preloaded_upcoming, preloaded_finished):
    if preloaded_upcoming:
        return preloaded_upcoming[0].get('id')
    if preloaded_finished:
        return preloaded_finished[0].get('id')
    return None


# --- NUEVA RUTA PARA MOSTRAR EL ESTUDIO DETALLADO ---
@app.route('/estudio', defaults={'match_id': None})
@app.route('/estudio/<string:match_id>')
def mostrar_estudio(match_id):
    """
    Vista principal del estudio con barra lateral integrada.
    """
    print(f"Recibida petición para el estudio del partido ID: {match_id}")

    handicap_filter = request.args.get('handicap')
    goal_line_filter = request.args.get('ou')

    # Filter upcoming matches: apply filters and ensure they are in the future
    upcoming_matches = _filter_and_slice_matches(
        'upcoming_matches',
        limit=1000,
        handicap_filter=handicap_filter,
        goal_line_filter=goal_line_filter,
        min_time=datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    )

    # Filter finished matches: apply filters
    finished_matches = _filter_and_slice_matches(
        'finished_matches',
        limit=1000,
        handicap_filter=handicap_filter,
        goal_line_filter=goal_line_filter,
        sort_desc=True
    )

    requested_match_id = match_id or request.args.get('match_id')
    target_match_id = requested_match_id or _select_default_match_id(upcoming_matches, finished_matches)

    if not target_match_id:
        abort(404, description='No hay partidos disponibles para analizar.')

    datos_partido = analizar_partido_completo(target_match_id)

    if not datos_partido or "error" in datos_partido:
        error_message = (datos_partido or {}).get('error', 'Error desconocido')
        print(f"Error al obtener datos para {target_match_id}: {error_message}")
        abort(500, description=error_message)

    datos_partido['match_id'] = target_match_id
    print(f"Datos obtenidos para {datos_partido['home_name']} vs {datos_partido['away_name']}. Renderizando plantilla...")
    return render_template(
        'estudio.html',
        data=datos_partido,
        format_ah=format_ah_as_decimal_string_of,
        upcoming_matches=upcoming_matches,
        finished_matches=finished_matches,
        selected_match_id=target_match_id,
        current_handicap=handicap_filter,
        current_ou=goal_line_filter
    )


@app.route('/api/estudio_panel/<string:match_id>')
def api_estudio_panel(match_id):
    """
    Devuelve el panel de análisis renderizado para actualizar la vista sin recargar la página.
    """
    start_time = time.time()
    force_refresh = request.args.get('refresh', 'false').lower() == 'true'
    try:
        datos_partido = analizar_partido_completo(match_id, force_refresh=force_refresh)
        if not datos_partido or "error" in datos_partido:
            error_message = (datos_partido or {}).get('error', 'No se pudo analizar el partido.')
            return jsonify({'error': error_message}), 500

        datos_partido['match_id'] = match_id
        
        # --- GUARDAR EN JSON ---
        # Guardamos en JSON cada vez que se analiza exitosamente
        save_match_to_json(datos_partido)
        # ----------------------

        html = render_template(
            'partials/analysis_panel.html',
            data=datos_partido,
            format_ah=format_ah_as_decimal_string_of
        )
        elapsed = round(time.time() - start_time, 2)
        payload = {
            'html': html,
            'match': {
                'id': match_id,
                'home': datos_partido.get('home_name'),
                'away': datos_partido.get('away_name'),
                'score': datos_partido.get('score'),
                'time': datos_partido.get('time')
            },
            'meta': {'elapsed': elapsed}
        }
        return jsonify(payload)
    except Exception as exc:
        logging.exception("Error generando el panel dinámico para %s", match_id)
        return jsonify({'error': f'No se pudo renderizar el análisis: {exc}'}), 500


@app.route('/seguimiento-liga')
def league_market_page():
    """Cronologia de colocacion O/U para la Iceland Division 1."""
    return render_template('league_market_tracker.html')


@app.route('/api/league-market/overview')
def api_league_market_overview():
    league_id = ''.join(filter(str.isdigit, str(request.args.get('league_id') or '381'))) or '381'
    season = str(request.args.get('season') or '2025').strip()[:20]
    try:
        company_id = int(request.args.get('company_id') or 8)
        return jsonify(league_market_tracker.get_overview(league_id, season, company_id))
    except Exception as exc:
        logging.exception('Error leyendo seguimiento de liga %s/%s', league_id, season)
        return jsonify({'error': str(exc)}), 500


@app.route('/api/league-market/seasons')
def api_league_market_seasons():
    league_id = ''.join(filter(str.isdigit, str(request.args.get('league_id') or '381'))) or '381'
    return jsonify({'league_id': league_id, 'seasons': league_market_tracker.available_seasons(league_id)})


@app.route('/api/league-market/match/<match_id>')
def api_league_market_match(match_id):
    clean_id = ''.join(filter(str.isdigit, str(match_id)))
    league_id = ''.join(filter(str.isdigit, str(request.args.get('league_id') or '381'))) or '381'
    season = str(request.args.get('season') or '2025').strip()[:20]
    if not clean_id:
        return jsonify({'available': False, 'error': 'ID no valido'}), 400
    return jsonify(league_market_tracker.get_match_timeline(league_id, season, clean_id))


@app.route('/api/league-market/learning')
def api_league_market_learning():
    league_id = ''.join(filter(str.isdigit, str(request.args.get('league_id') or '381'))) or '381'
    return jsonify(league_evolution_learning.get_learning_report(league_id))


@app.route('/api/league-market/learning/train', methods=['POST'])
def api_league_market_learning_train():
    payload = request.get_json(silent=True) or {}
    league_id = ''.join(filter(str.isdigit, str(payload.get('league_id') or '381'))) or '381'
    train_seasons = [str(value).strip()[:20] for value in (payload.get('train_seasons') or ['2023', '2024'])]
    test_season = str(payload.get('test_season') or '2025').strip()[:20]
    try:
        company_id = int(payload.get('company_id') or 8)
        result = league_evolution_learning.train_league(league_id, train_seasons, test_season, company_id)
        return jsonify(result)
    except Exception as exc:
        logging.exception('Error entrenando evolución de liga %s', league_id)
        return jsonify({'error': str(exc)}), 500


@app.route('/api/precacheo/league-learning/<match_id>')
def api_precacheo_league_learning(match_id):
    clean_id = ''.join(filter(str.isdigit, str(match_id)))
    if not clean_id:
        return jsonify({'available': False, 'error': 'ID no valido'}), 400
    match = data_manager.get_precacheo_match(clean_id) or sql_store.get_match(clean_id)
    if not isinstance(match, dict):
        return jsonify({'available': False, 'error': 'Partido no encontrado'}), 404
    return jsonify(league_evolution_learning.predict_precache(match))


def _run_league_market_sync(job_id, league_id, seasons, companies):
    def progress(snapshot):
        with _league_market_jobs_lock:
            if job_id in _league_market_jobs:
                _league_market_jobs[job_id].update(snapshot)

    try:
        result = league_market_tracker.sync_league(
            league_id=league_id,
            seasons=seasons,
            companies=companies,
            workers=6,
            progress=progress,
        )
        learning = None
        if {'2023', '2024', '2025'}.issubset({str(value) for value in seasons}):
            learning = league_evolution_learning.train_league(league_id, ('2023', '2024'), '2025', 8)
        with _league_market_jobs_lock:
            _league_market_jobs[job_id].update({'state': 'complete', 'result': result, 'learning': learning})
    except Exception as exc:
        logging.exception('Error sincronizando seguimiento de liga')
        with _league_market_jobs_lock:
            _league_market_jobs[job_id].update({'state': 'failed', 'error': str(exc)})


@app.route('/api/league-market/sync', methods=['POST'])
def api_league_market_sync():
    payload = request.get_json(silent=True) or {}
    league_id = ''.join(filter(str.isdigit, str(payload.get('league_id') or '381'))) or '381'
    seasons = [str(value).strip()[:20] for value in (payload.get('seasons') or ['2023', '2024', '2025'])]
    seasons = [value for value in seasons if value][:6]
    try:
        companies = [int(value) for value in (payload.get('companies') or [8, 31, 3])][:6]
    except (TypeError, ValueError):
        return jsonify({'error': 'Lista de casas no valida'}), 400
    job_id = uuid.uuid4().hex
    with _league_market_jobs_lock:
        _league_market_jobs[job_id] = {
            'job_id': job_id, 'state': 'running', 'league_id': league_id,
            'seasons': seasons, 'completed': 0, 'total': 0, 'snapshots': 0,
        }
    thread = threading.Thread(
        target=_run_league_market_sync,
        args=(job_id, league_id, seasons, companies),
        daemon=True,
    )
    thread.start()
    return jsonify(_league_market_jobs[job_id]), 202


@app.route('/api/league-market/sync/<job_id>')
def api_league_market_sync_status(job_id):
    with _league_market_jobs_lock:
        job = dict(_league_market_jobs.get(job_id) or {})
    if not job:
        return jsonify({'error': 'Sincronizacion no encontrada'}), 404
    return jsonify(job)


@app.route('/api/sofascore/league-table', methods=['POST'])
def api_sofascore_league_table():
    """Carga la clasificación externa solo cuando la solicita el panel de estudio."""
    payload = request.get_json(silent=True) or {}
    home_name = str(payload.get('home_name') or '').strip()[:120]
    away_name = str(payload.get('away_name') or '').strip()[:120]
    league_name = str(payload.get('league_name') or '').strip()[:160]
    match_date = str(payload.get('match_date') or '').strip()[:20]
    goal_line = payload.get('goal_line')

    if not home_name or not away_name:
        return jsonify({'available': False, 'reason': 'missing_teams', 'views': {}})

    query = {
        'home_name': home_name,
        'away_name': away_name,
        'league_name': league_name,
        'match_date': match_date,
        'goal_line': goal_line,
    }
    result = sofascore_context.get_league_table_context(**query)

    # SofaScore puede fallar de forma puntual aunque el torneo sí exista. Un
    # segundo intento evita convertir ese corte breve en un falso "sin tabla".
    if not result.get('available') and result.get('reason') == 'provider_unavailable':
        time.sleep(0.2)
        result = sofascore_context.get_league_table_context(**query)

    # La fecha ayuda a escoger el evento, pero no forma parte de la tabla. Si
    # viene con un formato inesperado, dejamos que el resolvedor use el cruce.
    if (
        not result.get('available')
        and match_date
        and result.get('reason') in {'provider_unavailable', 'match_not_resolved'}
    ):
        result = sofascore_context.get_league_table_context(**{**query, 'match_date': None})
    return jsonify(result)

# --- NUEVA RUTA PARA ANALIZAR PARTIDOS FINALIZADOS ---
@app.route('/analizar_partido', methods=['GET', 'POST'])
def analizar_partido():
    """
    Ruta para analizar partidos finalizados por ID.
    """
    if request.method == 'POST':
        match_id = request.form.get('match_id')
        if match_id:
            print(f"Recibida petición para analizar partido finalizado ID: {match_id}")
            cleaned_match_id = "".join(filter(str.isdigit, match_id))
            if not cleaned_match_id:
                return render_template('analizar_partido.html', error="Por favor, introduce un ID de partido válido.")

            return redirect(url_for('mostrar_estudio', match_id=cleaned_match_id))
        else:
            return render_template('analizar_partido.html', error="Por favor, introduce un ID de partido válido.")
    
    # Si es GET, mostrar el formulario
    return render_template('analizar_partido.html')

# --- NUEVA RUTA API PARA LA VISTA PREVIA RÁPIDA ---
@app.route('/api/preview/<string:match_id>')
def api_preview(match_id):
    """
    Endpoint para la vista previa ("el ojito"). Llama al scraper COMPLETO.
    Devuelve los datos en formato JSON.
    """
    try:
        preview_data = analizar_partido_completo(match_id)
        if "error" in preview_data:
            return jsonify(preview_data), 500
        return jsonify(preview_data)
    except Exception as e:
        print(f"Error en la ruta /api/preview/{match_id}: {e}")
        return jsonify({'error': 'Ocurrió un error interno en el servidor.'}), 500


@app.route('/api/analisis/<string:match_id>')
def api_analisis(match_id):
    """
    Servicio de analisis profundo bajo demanda.
    Devuelve tanto el payload complejo como el HTML simplificado.
    """
    try:
        cached_payload = load_preview_from_cache(match_id)
        if isinstance(cached_payload, dict) and cached_payload.get('home_team'):
            print(f"Devolviendo analisis cacheado para {match_id}")
            return jsonify(cached_payload)

        start_time = time.time()
        logging.warning(f"CACHE MISS para {match_id}. Iniciando análisis profundo...")

        datos = analizar_partido_completo(match_id)
        if not datos or (isinstance(datos, dict) and datos.get('error')):
            return jsonify({'error': (datos or {}).get('error', 'No se pudieron obtener datos.')}), 500

        # --- Lógica para el payload complejo (la original) ---
        def df_to_rows(df):
            rows = []
            try:
                if df is not None and hasattr(df, 'iterrows'):
                    for idx, row in df.iterrows():
                        label = str(idx)
                        label = label.replace('Shots on Goal', 'Tiros a Puerta')                                     .replace('Shots', 'Tiros')                                     .replace('Dangerous Attacks', 'Ataques Peligrosos')                                     .replace('Attacks', 'Ataques')
                        try:
                            home_val = row['Casa']
                        except Exception:
                            home_val = ''
                        try:
                            away_val = row['Fuera']
                        except Exception:
                            away_val = ''
                        rows.append({'label': label, 'home': home_val or '', 'away': away_val or ''})
            except Exception:
                pass
            return rows

        payload = {
            'match_id': match_id,
            'home_team': datos.get('home_name', ''),
            'away_team': datos.get('away_name', ''),
            'final_score': datos.get('score'),
            'match_date': datos.get('match_date'),
            'match_time': datos.get('match_time'),
            'match_datetime': datos.get('match_datetime'),
            'recent_indirect_full': {
                'last_home': None,
                'last_away': None,
                'h2h_col3': None
            },
            'comparativas_indirectas': {
                'left': None,
                'right': None
            }
        }
        
        # --- START COVERAGE CALCULATION ---
        main_odds = datos.get("main_match_odds_data")
        home_name = datos.get("home_name")
        away_name = datos.get("away_name")
        ah_actual_num = parse_ah_to_number_of(main_odds.get('ah_linea_raw', ''))
        
        favorito_actual_name = "Ninguno (línea en 0)"
        if ah_actual_num is not None:
            if ah_actual_num > 0: favorito_actual_name = home_name
            elif ah_actual_num < 0: favorito_actual_name = away_name

        def get_cover_status_vs_current(details):
            if not details or ah_actual_num is None:
                return 'NEUTRO'
            try:
                score_str = details.get('score', '').replace(' ', '').replace(':', '-')
                if not score_str or '?' in score_str:
                    return 'NEUTRO'

                h_home = details.get('home_team')
                h_away = details.get('away_team')
                
                status, _ = check_handicap_cover(score_str, ah_actual_num, favorito_actual_name, h_home, h_away, home_name)
                return status
            except Exception:
                return 'NEUTRO'
                
        # --- Análisis mejorado de H2H Rivales ---
        def analyze_h2h_rivals(home_result, away_result):
            if not home_result or not away_result:
                return None
                
            try:
                # Obtener resultados de los partidos
                home_goals = list(map(int, home_result.get('score', '0-0').split('-')))
                away_goals = list(map(int, away_result.get('score', '0-0').split('-')))
                
                # Calcular diferencia de goles
                home_goal_diff = home_goals[0] - home_goals[1]
                away_goal_diff = away_goals[0] - away_goals[1]
                
                # Comparar resultados
                if home_goal_diff > away_goal_diff:
                    return "Contra rivales comunes, el Equipo Local ha obtenido mejores resultados"
                elif away_goal_diff > home_goal_diff:
                    return "Contra rivales comunes, el Equipo Visitante ha obtenido mejores resultados"
                else:
                    return "Los rivales han tenido resultados similares"
            except Exception:
                return None
                
        # --- Análisis de Comparativas Indirectas ---
        def analyze_indirect_comparison(result, team_name):
            if not result:
                return None
                
            try:
                # Determinar si el equipo cubrió el handicap
                status = get_cover_status_vs_current(result)
                
                if status == 'CUBIERTO':
                    return f"Contra este rival, {team_name} habría cubierto el handicap"
                elif status == 'NO CUBIERTO':
                    return f"Contra este rival, {team_name} no habría cubierto el handicap"
                else:
                    return f"Contra este rival, el resultado para {team_name} sería indeterminado"
            except Exception:
                return None
        # --- END COVERAGE CALCULATION ---

        last_home = (datos.get('last_home_match') or {})
        last_home_details = last_home.get('details') or {}
        if last_home_details:
            payload['recent_indirect_full']['last_home'] = {
                'home': last_home_details.get('home_team'),
                'away': last_home_details.get('away_team'),
                'score': (last_home_details.get('score') or '').replace(':', ' : '),
                'ah': format_ah_as_decimal_string_of(last_home_details.get('handicap_line_raw') or '-'),
                'ou': last_home_details.get('ouLine') or '-',
                'stats_rows': df_to_rows(last_home.get('stats')),
                'date': last_home_details.get('date'),
                'cover_status': get_cover_status_vs_current(last_home_details)
            }

        last_away = (datos.get('last_away_match') or {})
        last_away_details = last_away.get('details') or {}
        if last_away_details:
            payload['recent_indirect_full']['last_away'] = {
                'home': last_away_details.get('home_team'),
                'away': last_away_details.get('away_team'),
                'score': (last_away_details.get('score') or '').replace(':', ' : '),
                'ah': format_ah_as_decimal_string_of(last_away_details.get('handicap_line_raw') or '-'),
                'ou': last_away_details.get('ouLine') or '-',
                'stats_rows': df_to_rows(last_away.get('stats')),
                'date': last_away_details.get('date'),
                'cover_status': get_cover_status_vs_current(last_away_details)
            }

        h2h_col3 = (datos.get('h2h_col3') or {})
        h2h_col3_details = h2h_col3.get('details') or {}
        if h2h_col3_details and h2h_col3_details.get('status') == 'found':
            h2h_col3_details_adapted = {
                'score': f"{h2h_col3_details.get('goles_home')}:{h2h_col3_details.get('goles_away')}",
                'home_team': h2h_col3_details.get('h2h_home_team_name'),
                'away_team': h2h_col3_details.get('h2h_away_team_name')
            }
            payload['recent_indirect_full']['h2h_col3'] = {
                'home': h2h_col3_details.get('h2h_home_team_name'),
                'away': h2h_col3_details.get('h2h_away_team_name'),
                'score': f"{h2h_col3_details.get('goles_home')} : {h2h_col3_details.get('goles_away')}",
                'ah': format_ah_as_decimal_string_of(h2h_col3_details.get('handicap_line_raw') or '-'),
                'ou': h2h_col3_details.get('ou_result') or '-',
                'stats_rows': df_to_rows(h2h_col3.get('stats')),
                'date': h2h_col3_details.get('date'),
                'cover_status': get_cover_status_vs_current(h2h_col3_details_adapted),
                'analysis': analyze_h2h_rivals(last_home_details, last_away_details)
            }

        h2h_general = (datos.get('h2h_general') or {})
        h2h_general_details = h2h_general.get('details') or {}
        if h2h_general_details:
            score_text = h2h_general_details.get('res6') or ''
            cover_input = {
                'score': score_text,
                'home_team': h2h_general_details.get('h2h_gen_home'),
                'away_team': h2h_general_details.get('h2h_gen_away')
            }
            payload['recent_indirect_full']['h2h_general'] = {
                'home': h2h_general_details.get('h2h_gen_home'),
                'away': h2h_general_details.get('h2h_gen_away'),
                'score': score_text.replace(':', ' : '),
                'ah': h2h_general_details.get('ah6') or '-',
                'ou': h2h_general_details.get('ou_result6') or '-',
                'stats_rows': df_to_rows(h2h_general.get('stats')),
                'date': h2h_general_details.get('date'),
                'cover_status': get_cover_status_vs_current(cover_input) if score_text else 'NEUTRO'
            }

        comp_left = (datos.get('comp_L_vs_UV_A') or {})
        comp_left_details = comp_left.get('details') or {}
        if comp_left_details:
            payload['comparativas_indirectas']['left'] = {
                'title_home_name': datos.get('home_name'),
                'title_away_name': datos.get('away_name'),
                'home_team': comp_left_details.get('home_team'),
                'away_team': comp_left_details.get('away_team'),
                'score': (comp_left_details.get('score') or '').replace(':', ' : '),
                'ah': format_ah_as_decimal_string_of(comp_left_details.get('ah_line') or '-'),
                'ou': comp_left_details.get('ou_line') or '-',
                'localia': comp_left_details.get('localia') or '',
                'stats_rows': df_to_rows(comp_left.get('stats')),
                'cover_status': get_cover_status_vs_current(comp_left_details),
                'analysis': analyze_indirect_comparison(comp_left_details, datos.get('home_name'))
            }

        comp_right = (datos.get('comp_V_vs_UL_H') or {})
        comp_right_details = comp_right.get('details') or {}
        if comp_right_details:
            payload['comparativas_indirectas']['right'] = {
                'title_home_name': datos.get('home_name'),
                'title_away_name': datos.get('away_name'),
                'home_team': comp_right_details.get('home_team'),
                'away_team': comp_right_details.get('away_team'),
                'score': (comp_right_details.get('score') or '').replace(':', ' : '),
                'ah': format_ah_as_decimal_string_of(comp_right_details.get('ah_line') or '-'),
                'ou': comp_right_details.get('ou_line') or '-',
                'localia': comp_right_details.get('localia') or '',
                'stats_rows': df_to_rows(comp_right.get('stats')),
                'cover_status': get_cover_status_vs_current(comp_right_details),
                'analysis': analyze_indirect_comparison(comp_right_details, datos.get('away_name'))
            }

        # --- Lógica para el HTML simplificado ---
        h2h_data = datos.get("h2h_data")
        simplified_html = ""
        if all([main_odds, h2h_data, home_name, away_name]):
            simplified_html = generar_analisis_completo_mercado(main_odds, h2h_data, home_name, away_name)
        
        payload['simplified_html'] = simplified_html

        save_preview_to_cache(match_id, payload)

        end_time = time.time()
        elapsed = end_time - start_time
        logging.warning(f"[PERFORMANCE] El análisis completo para el partido {match_id} tardó {elapsed:.2f} segundos.")

        return jsonify(payload)

    except Exception as e:
        print(f"Error en la ruta /api/analisis/{match_id}: {e}")
        return jsonify({'error': 'Ocurrió un error interno en el servidor.'}), 500

@app.route('/start_analysis_background', methods=['POST'])
def start_analysis_background():
    match_id = request.json.get('match_id')
    if not match_id:
        return jsonify({'status': 'error', 'message': 'No se proporcionó match_id'}), 400

    def analysis_worker(app, match_id):
        with app.app_context():
            print(f"Iniciando análisis en segundo plano para el ID: {match_id}")
            try:
                analizar_partido_completo(match_id)
                print(f"Análisis en segundo plano finalizado para el ID: {match_id}")
            except Exception as e:
                print(f"Error en el hilo de análisis para el ID {match_id}: {e}")

    thread = threading.Thread(target=analysis_worker, args=(app, match_id))
    thread.start()

    return jsonify({'status': 'success', 'message': f'Análisis iniciado para el partido {match_id}'})

@app.route('/api/quick_view/<match_id>', methods=['GET'])
def api_quick_view(match_id):
    try:
        # Usamos analizar_partido_completo para obtener todos los datos
        # Force refresh=False para usar caché si existe
        data = analizar_partido_completo(match_id, force_refresh=False)
        
        if "error" in data:
            return jsonify({'error': data['error']}), 404
            
        # Filtramos solo lo necesario para la vista rápida
        quick_view_data = {
            "market_analysis_data": data.get("market_analysis_data"),
            "last_home_match": data.get("last_home_match"),
            "last_away_match": data.get("last_away_match"),
            "h2h_col3": data.get("h2h_col3"),
            "comparativas_indirectas": data.get("comparativas_indirectas"),
            "home_name": data.get("home_name"),
            "away_name": data.get("away_name"),
            "final_score": data.get("final_score")
        }
        
        return jsonify(quick_view_data)
    except Exception as e:
        print(f"Error en quick_view: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/pattern_search', methods=['POST'])
def api_pattern_search():
    try:
        data = request.json
        upcoming_match = data.get('upcoming_match')
        print(f"DEBUG: Pattern Search Request. Upcoming Match: {upcoming_match}")
        filter_mode = data.get('filter_mode', 'global') # global, home_strict, away_strict
        config = data.get('config', {}) # Configuración adicional (ej: filter_progression)
        
        if not upcoming_match:
            return jsonify({'error': 'Faltan datos del partido futuro'}), 400

        # Determine target AH to load only relevant bucket
        target_ah = upcoming_match.get('ah_open_home')
        
        # Load data from buckets using data_manager
        # If target_ah is None, we might need all data? 
        # Usually pattern search requires an AH. If None, it returns empty.
        # But find_similar_patterns handles None.
        # Let's load by bucket if possible.
        
        history_data = data_manager.load_matches_by_bucket(target_ah)
        
        if not history_data:
             return jsonify({'results': [], 'message': 'No hay histórico disponible.'})
        
        # Combinar config con filter_mode
        config['filter_mode'] = filter_mode
            
        results = find_similar_patterns(upcoming_match, history_data, config=config)
        
        # Limitar resultados si es necesario (top 100)
        results = results[:100]
        
        return jsonify({'results': results})
    except Exception as e:
        print(f"Error en pattern search: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/explorador')
def explorador():
    """Muestra la vista del Explorador de Datos."""
    return render_template('explorer.html')


UEFA_QUALIFYING_JOBS = {}
UEFA_QUALIFYING_JOBS_LOCK = threading.Lock()


@app.route('/explorador/fases-previas-uefa')
def explorador_fases_previas_uefa():
    """Replica completa del Explorador, limitada al catalogo UEFA qualifying."""
    return render_template(
        'explorer.html',
        explorer_scope='uefa_qualifying',
        explorer_title='Explorador · Fases previas UEFA',
        explorer_dashboard_url='/explorador/fases-previas-uefa/resumen',
    )


@app.route('/explorador/fases-previas-uefa/resumen')
def resumen_fases_previas_uefa():
    """Resumen, próximos partidos y patrones agregados UEFA."""
    return render_template(
        'uefa_qualifying.html',
        competitions=uefa_qualifying.COMPETITIONS,
        default_seasons=uefa_qualifying.DEFAULT_SEASONS,
    )


def _uefa_filter_values(raw_value):
    if isinstance(raw_value, list):
        return [str(value).strip() for value in raw_value if str(value).strip()]
    return [value.strip() for value in str(raw_value or '').split(',') if value.strip()]


@app.route('/api/uefa_qualifying/analysis')
def api_uefa_qualifying_analysis():
    try:
        result = uefa_qualifying.load_analysis(
            competition_ids=_uefa_filter_values(request.args.get('competitions')) or None,
            seasons=_uefa_filter_values(request.args.get('seasons')) or None,
            stages=_uefa_filter_values(request.args.get('stages')) or None,
            limit=min(max(int(request.args.get('limit', 5000)), 1), 20000),
        )
        result['available'] = {
            'competitions': uefa_qualifying.COMPETITIONS,
            'seasons': list(uefa_qualifying.DEFAULT_SEASONS),
            'stages': sorted({row.get('stage_name') for row in result['rows'] if row.get('stage_name')}),
        }
        return jsonify(result)
    except Exception as exc:
        app.logger.exception('Error cargando el explorador de fases previas UEFA')
        return jsonify({'error': str(exc)}), 500


@app.route('/api/uefa_qualifying/upcoming')
def api_uefa_qualifying_upcoming():
    try:
        matches = uefa_qualifying.load_precache_upcoming()
        return jsonify({'matches': matches, 'total': len(matches)})
    except Exception as exc:
        app.logger.exception('Error cargando próximos UEFA desde Pre-Cacheo')
        return jsonify({'error': str(exc)}), 500


@app.route('/api/uefa_qualifying/explorer_cards')
def api_uefa_qualifying_explorer_cards():
    try:
        result = uefa_qualifying.load_explorer_cards(
            competition_ids=_uefa_filter_values(request.args.get('competitions')) or None,
            seasons=_uefa_filter_values(request.args.get('seasons')) or None,
            stages=_uefa_filter_values(request.args.get('stages')) or None,
            page=int(request.args.get('page', 1)),
            per_page=int(request.args.get('per_page', 20)),
        )
        return jsonify(result)
    except Exception as exc:
        app.logger.exception('Error creando fichas Explorer UEFA')
        return jsonify({'error': str(exc)}), 500


@app.route('/api/match/<match_id>/progression-stats')
def api_match_progression_stats(match_id):
    """Carga bajo demanda tiros/ataques de una ficha historica y los persiste."""
    if not str(match_id).isdigit():
        return jsonify({'error': 'ID de partido no valido'}), 400
    try:
        stats_rows = _df_to_rows(get_match_progression_stats_data(str(match_id)))
        existing = sql_store.get_match(str(match_id))
        if existing and stats_rows:
            payload = {**existing, 'stats_rows': stats_rows}
            bucket = 'data_uefa_qualifying.json' if existing.get('uefa_qualifying_catalogue') else 'data.json'
            sql_store.upsert_match(payload, bucket=bucket, state='historical')
        return jsonify({'match_id': str(match_id), 'stats_rows': stats_rows})
    except Exception as exc:
        app.logger.exception('Error cargando estadisticas del partido %s', match_id)
        return jsonify({'error': str(exc)}), 500


def _run_uefa_catalog_job(job_id, competition_ids, seasons, company_id):
    with UEFA_QUALIFYING_JOBS_LOCK:
        UEFA_QUALIFYING_JOBS[job_id].update(
            status='running',
            started_at=datetime.datetime.utcnow().isoformat(),
        )
    try:
        result = uefa_qualifying.ingest_history(competition_ids, seasons, company_id)
        with UEFA_QUALIFYING_JOBS_LOCK:
            UEFA_QUALIFYING_JOBS[job_id].update(
                status='completed',
                result=result,
                completed_at=datetime.datetime.utcnow().isoformat(),
            )
    except Exception as exc:
        with UEFA_QUALIFYING_JOBS_LOCK:
            UEFA_QUALIFYING_JOBS[job_id].update(
                status='failed',
                error=str(exc),
                completed_at=datetime.datetime.utcnow().isoformat(),
            )


@app.route('/api/uefa_qualifying/scrape', methods=['POST'])
def api_uefa_qualifying_scrape():
    payload = request.get_json(silent=True) or {}
    competition_ids = _uefa_filter_values(payload.get('competitions')) or list(uefa_qualifying.COMPETITIONS)
    competition_ids = [value for value in competition_ids if value in uefa_qualifying.COMPETITIONS]
    seasons = _uefa_filter_values(payload.get('seasons')) or list(uefa_qualifying.DEFAULT_SEASONS)
    if not competition_ids or not seasons:
        return jsonify({'error': 'Selecciona al menos una competicion y una temporada'}), 400
    try:
        company_id = int(payload.get('company_id', 8))
    except (TypeError, ValueError):
        return jsonify({'error': 'Casa de cuotas no valida'}), 400

    job_id = uuid.uuid4().hex
    with UEFA_QUALIFYING_JOBS_LOCK:
        UEFA_QUALIFYING_JOBS[job_id] = {
            'job_id': job_id,
            'type': 'catalogue',
            'status': 'queued',
            'competitions': competition_ids,
            'seasons': seasons,
            'created_at': datetime.datetime.utcnow().isoformat(),
        }
    threading.Thread(
        target=_run_uefa_catalog_job,
        args=(job_id, competition_ids, seasons, company_id),
        daemon=True,
        name=f'uefa-catalog-{job_id[:8]}',
    ).start()
    return jsonify({'status': 'started', 'job_id': job_id}), 202


def _run_uefa_deep_job(job_id, rows, workers):
    with UEFA_QUALIFYING_JOBS_LOCK:
        job = UEFA_QUALIFYING_JOBS[job_id]
        job.update(status='running', started_at=datetime.datetime.utcnow().isoformat())
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    league_handicap_scraper.scrape_match_to_sql,
                    {
                        'id': row['match_id'],
                        'visible_ah': row.get('ah_line'),
                        'company_id': row.get('company_id', 8),
                    },
                    row['competition_id'],
                    True,
                ): row
                for row in rows
            }
            for future in concurrent.futures.as_completed(futures):
                row = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    result = {'id': row['match_id'], 'status': 'error', 'error': str(exc)}
                if result.get('status') == 'saved':
                    sql_store.mark_uefa_qualifying_deep_status(row['match_id'], 'enriched')
                elif result.get('status') == 'error':
                    sql_store.mark_uefa_qualifying_deep_status(row['match_id'], 'error')
                with UEFA_QUALIFYING_JOBS_LOCK:
                    job = UEFA_QUALIFYING_JOBS[job_id]
                    job['processed'] += 1
                    status = result.get('status', 'error')
                    job['counts'][status] = job['counts'].get(status, 0) + 1
                    job['last_result'] = result
        with UEFA_QUALIFYING_JOBS_LOCK:
            UEFA_QUALIFYING_JOBS[job_id].update(
                status='completed', completed_at=datetime.datetime.utcnow().isoformat()
            )
    except Exception as exc:
        with UEFA_QUALIFYING_JOBS_LOCK:
            UEFA_QUALIFYING_JOBS[job_id].update(
                status='failed', error=str(exc), completed_at=datetime.datetime.utcnow().isoformat()
            )


@app.route('/api/uefa_qualifying/enrich', methods=['POST'])
def api_uefa_qualifying_enrich():
    payload = request.get_json(silent=True) or {}
    match_ids = _uefa_filter_values(payload.get('match_ids'))
    try:
        max_matches = min(max(int(payload.get('limit', 100)), 1), 300)
        workers = min(max(int(payload.get('workers', 2)), 1), 4)
    except (TypeError, ValueError):
        return jsonify({'error': 'Limite o concurrencia no validos'}), 400
    rows = sql_store.fetch_uefa_qualifying_matches(limit=20000)
    if match_ids:
        requested = set(match_ids)
        rows = [row for row in rows if str(row.get('match_id')) in requested]
    else:
        rows = [row for row in rows if row.get('deep_status') != 'enriched']
    rows = rows[:max_matches]
    if not rows:
        return jsonify({'error': 'No hay partidos pendientes de enriquecer'}), 400
    job_id = uuid.uuid4().hex
    with UEFA_QUALIFYING_JOBS_LOCK:
        UEFA_QUALIFYING_JOBS[job_id] = {
            'job_id': job_id,
            'type': 'deep',
            'status': 'queued',
            'total': len(rows),
            'processed': 0,
            'counts': {},
            'created_at': datetime.datetime.utcnow().isoformat(),
        }
    threading.Thread(
        target=_run_uefa_deep_job,
        args=(job_id, rows, workers),
        daemon=True,
        name=f'uefa-deep-{job_id[:8]}',
    ).start()
    return jsonify({'status': 'started', 'job_id': job_id, 'total': len(rows)}), 202


@app.route('/api/uefa_qualifying/status/<job_id>')
def api_uefa_qualifying_status(job_id):
    with UEFA_QUALIFYING_JOBS_LOCK:
        job = UEFA_QUALIFYING_JOBS.get(str(job_id))
        if job is None:
            return jsonify({'error': 'Trabajo no encontrado'}), 404
        return jsonify(json.loads(json.dumps(job, ensure_ascii=False)))

@app.route('/api/explorer_search', methods=['POST'])
def api_explorer_search():
    try:
        data = request.json
        filters = data.get('filters', {})
        explorer_scope = str(data.get('scope') or filters.pop('scope', '') or '').strip().lower()
        scope_filters = data.get('scope_filters') or {}
        print(f"DEBUG: Explorer Search Request. Filters: {filters}")

        # Allow deep analysis while keeping a hard safety ceiling.
        try:
            req_limit = int(filters.get('limit', 10000))
        except (TypeError, ValueError):
            req_limit = 10000
        filters['limit'] = max(1, min(req_limit, 20000))

        analyze_all = bool(filters.get('analyze_all', False))
        # Keep full stat rows available in explorer unless caller explicitly disables them.
        if 'include_stats' not in filters:
            filters['include_stats'] = True

        # Explorer should read finalized historical rows from SQL only.
        raw_ah_filter = filters.get('handicap')
        ah_filter = raw_ah_filter
        if isinstance(raw_ah_filter, list):
            ah_filter = raw_ah_filter[0] if len(raw_ah_filter) == 1 else None
        if ah_filter in (None, ''):
            ah_filter = filters.get('exact_handicap')
        scan_limit = None
        if not analyze_all and not ah_filter:
            has_strict_filters = any(
                filters.get(k)
                for k in (
                    'team',
                    'result',
                    'prev_home_wdl',
                    'prev_away_wdl',
                    'prev_home_real_wdl',
                    'prev_away_real_wdl',
                    'prev_home_ah',
                    'prev_away_ah',
                    'h2h_stadium_mov',
                    'h2h_stadium_res',
                    'h2h_general_mov',
                    'h2h_general_res',
                    'h2h_col3_ah',
                    'ind_local_ah',
                    'ind_visitante_ah',
                    'exact_handicap',
                    'favorite_side',
                    'favorite_result',
                    'cover_result',
                    'ou_limit',
                )
            ) or bool(filters.get('exclude_empty')) or bool(filters.get('only_with_history'))

            # Keep first response fast: scan a recent window instead of full table.
            # For stricter searches we scan a bit wider.
            if has_strict_filters:
                scan_limit = min(max(filters['limit'] * 3, 3000), 5000)
            else:
                scan_limit = min(max(filters['limit'] * 2, 2000), 3500)

        if explorer_scope == 'uefa_qualifying':
            uefa_rows = sql_store.fetch_uefa_qualifying_matches(
                competition_ids=_uefa_filter_values(scope_filters.get('competitions')) or None,
                seasons=_uefa_filter_values(scope_filters.get('seasons')) or None,
                stages=_uefa_filter_values(scope_filters.get('stages')) or None,
                limit=20000,
            )
            uefa_ids = [str(row.get('match_id')) for row in uefa_rows if row.get('match_id')]
            history_data = sql_store.fetch_matches_by_ids(
                uefa_ids,
                state='historical',
                limit=20000,
                prefer_explorer_payload=True,
            )
        else:
            history_data = data_manager.load_explorer_matches(ah_filter, scan_limit=scan_limit)
            
        if not history_data:
             return jsonify({'results': [], 'message': 'No hay histórico disponible.'})
            
        results = explore_matches(history_data, filters=filters)

        # A full Explorer response can exceed 50 MB. Browsers advertise gzip by
        # default; compressing here avoids truncated JSON/connection resets while
        # preserving the complete result set and every client-side filter.
        response_payload = json.dumps(
            {'results': results},
            ensure_ascii=False,
            separators=(',', ':'),
        ).encode('utf-8')
        if 'gzip' in request.headers.get('Accept-Encoding', '').lower():
            response = app.response_class(
                gzip.compress(response_payload, compresslevel=5),
                mimetype='application/json',
            )
            response.headers['Content-Encoding'] = 'gzip'
            response.headers['Vary'] = 'Accept-Encoding'
            return response
        return app.response_class(response_payload, mimetype='application/json')
    except Exception as e:
        print(f"Error en explorer search: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/delete_match', methods=['POST'])
def api_delete_match():
    try:
        data = request.json
        match_id = data.get('match_id')
        ah = data.get('ah')
        
        if not match_id:
            return jsonify({'error': 'Falta match_id'}), 400
            
        success, message = data_manager.delete_match_from_bucket(match_id, ah)
        
        if success:
            return jsonify({'status': 'success', 'message': message})
        else:
            return jsonify({'error': message}), 404
    except Exception as e:
        print(f"Error en delete match: {e}")
        return jsonify({'error': str(e)}), 500

# --- PRE-CACHEO ROUTES ---
def _get_precacheo_view_config():
    lite_mode = _env_flag('RENDER_LITE_MODE', default=False)

    default_items = 120 if lite_mode else 300
    items_per_page = _env_int('PRECACHEO_UI_ITEMS_PER_PAGE', default_items)
    items_per_page = max(30, min(items_per_page, 500))

    default_client_batch = 60 if lite_mode else 180
    picks_client_max = _env_int('PRECACHEO_PICKS_CLIENT_MAX', default_client_batch)
    picks_client_max = max(10, min(picks_client_max, 400))

    enable_qwen = _env_flag('PRECACHEO_ENABLE_QWEN', default=not lite_mode)
    enable_heavy_actions = _env_flag('PRECACHEO_ENABLE_HEAVY_ACTIONS', default=not lite_mode)
    actions_mode = str(os.getenv('PRECACHEO_ACTIONS_MODE', '')).strip().lower()
    if actions_mode not in {'full', 'minimal', 'ai_stats_only'}:
        actions_mode = 'full' if enable_heavy_actions else 'minimal'

    auto_scrape_on_gap = _env_flag('PRECACHEO_AUTO_SCRAPE_ON_GAP', default=lite_mode)
    default_auto_scrape_batch = 12 if lite_mode else 24
    auto_scrape_batch_max = _env_int('PRECACHEO_AUTO_SCRAPE_BATCH_MAX', default_auto_scrape_batch)
    auto_scrape_batch_max = max(1, min(auto_scrape_batch_max, 60))

    default_auto_scrape_concurrency = 1 if lite_mode else 2
    auto_scrape_concurrency = _env_int('PRECACHEO_AUTO_SCRAPE_CONCURRENCY', default_auto_scrape_concurrency)
    auto_scrape_concurrency = max(1, min(auto_scrape_concurrency, 4))

    return {
        'lite_mode': lite_mode,
        'items_per_page': items_per_page,
        'picks_client_max': picks_client_max,
        'enable_qwen': enable_qwen,
        'enable_heavy_actions': enable_heavy_actions,
        'actions_mode': actions_mode,
        'auto_scrape_on_gap': auto_scrape_on_gap,
        'auto_scrape_batch_max': auto_scrape_batch_max,
        'auto_scrape_concurrency': auto_scrape_concurrency,
    }


@app.route('/precacheo')
def precacheo():
    """Muestra la vista de Pre-Cacheo para partidos próximos."""
    cfg = _get_precacheo_view_config()
    return render_template(
        'precacheo.html',
        app_precacheo_only=_is_app_precacheo_only(),
        precacheo_lite_mode=cfg['lite_mode'],
        precacheo_items_per_page=cfg['items_per_page'],
        precacheo_picks_client_max=cfg['picks_client_max'],
        precacheo_enable_qwen=cfg['enable_qwen'],
        precacheo_enable_heavy_actions=cfg['enable_heavy_actions'],
        precacheo_actions_mode=cfg['actions_mode'],
        precacheo_auto_scrape_on_gap=cfg['auto_scrape_on_gap'],
        precacheo_auto_scrape_batch_max=cfg['auto_scrape_batch_max'],
        precacheo_auto_scrape_concurrency=cfg['auto_scrape_concurrency'],
    )


@app.route('/precacheo-sw.js')
def precacheo_sw():
    """Service worker dedicado al apartado de pre-cacheo."""
    response = send_from_directory(app.static_folder, 'js/precacheo_sw.js')
    response.headers['Content-Type'] = 'application/javascript'
    response.headers['Cache-Control'] = 'no-cache'
    return response


@app.route('/html_offline')
def html_offline_page():
    """Sirve el HTML offline simple desde la carpeta html_offline."""
    return send_from_directory(HTML_OFFLINE_DIR, HTML_OFFLINE_PAGE_NAME)


@app.route('/html_offline/<path:filename>')
def html_offline_assets(filename):
    """Sirve archivos auxiliares de la carpeta html_offline."""
    return send_from_directory(HTML_OFFLINE_DIR, filename)


@app.route('/api/html_offline/save', methods=['POST'])
def api_html_offline_save():
    """Guarda snapshot de pre-cacheo en html_offline/precacheo_snapshot.json."""
    try:
        payload = request.get_json(silent=True) or {}
        matches = payload.get('matches')

        if not isinstance(matches, list):
            fetch_limit = _env_int('HTML_OFFLINE_SNAPSHOT_LIMIT', 2000)
            fetch_limit = max(200, min(fetch_limit, 5000))
            matches = sql_store.fetch_matches(bucket=data_manager.PRECACHEO_BUCKET, limit=fetch_limit)
            if not matches:
                matches = _load_precacheo_legacy_rows(limit=fetch_limit)

        if not isinstance(matches, list) or len(matches) == 0:
            return jsonify({'error': 'No hay partidos para guardar en html_offline'}), 400

        payload_to_save = {
            'saved_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'source': 'api_html_offline_save',
            'count': len(matches),
            'matches': matches,
        }

        HTML_OFFLINE_DIR.mkdir(parents=True, exist_ok=True)
        with HTML_OFFLINE_SNAPSHOT_FILE.open('w', encoding='utf-8') as fh:
            json.dump(payload_to_save, fh, ensure_ascii=False)

        return jsonify({
            'status': 'ok',
            'count': len(matches),
            'file': str(HTML_OFFLINE_SNAPSHOT_FILE),
            'saved_at': payload_to_save['saved_at'],
        })
    except Exception as exc:
        return jsonify({'error': f'No se pudo guardar snapshot html_offline: {exc}'}), 500


@app.route('/api/html_offline/load')
def api_html_offline_load():
    """Carga snapshot guardado en la carpeta html_offline."""
    try:
        if not HTML_OFFLINE_SNAPSHOT_FILE.exists():
            return jsonify({'error': 'No existe snapshot en html_offline'}), 404

        with HTML_OFFLINE_SNAPSHOT_FILE.open('r', encoding='utf-8') as fh:
            payload = json.load(fh)

        matches = []
        if isinstance(payload, list):
            matches = payload
            payload = {'matches': matches}
        elif isinstance(payload, dict):
            matches = payload.get('matches', [])

        if not isinstance(matches, list):
            matches = []

        return jsonify({
            'status': 'ok',
            'saved_at': payload.get('saved_at'),
            'count': len(matches),
            'matches': matches,
        })
    except Exception as exc:
        return jsonify({'error': f'No se pudo cargar snapshot html_offline: {exc}'}), 500


@app.route('/api/precacheo_list')
def api_precacheo_list():
    """Lista partidos pre-cacheados SIN evaluar picks (carga rápida).
    Los picks se solicitan bajo demanda via /api/precacheo_picks_batch.
    """
    try:
        _maybe_cleanup_precacheo_stale(force=False)

        try:
            default_limit = int(os.getenv('PRECACHEO_LIST_DEFAULT_LIMIT', '700'))
        except Exception:
            default_limit = 700
        default_limit = max(100, default_limit)

        try:
            max_limit = int(os.getenv('PRECACHEO_LIST_MAX_LIMIT', '2000'))
        except Exception:
            max_limit = 2000
        max_limit = max(default_limit, max_limit)

        limit = None
        limit_arg = request.args.get('limit')
        if limit_arg not in (None, ''):
            try:
                limit_val = int(limit_arg)
                if limit_val > 0:
                    limit = limit_val
            except Exception:
                limit = None

        if limit is None:
            try:
                env_limit_raw = os.getenv('PRECACHEO_LIST_LIMIT', '')
                env_limit = int(env_limit_raw) if env_limit_raw else 0
            except Exception:
                env_limit = 0
            if env_limit > 0:
                limit = env_limit

        if limit is None:
            limit = default_limit
        limit = max(1, min(int(limit), max_limit))

        if limit is not None:
            matches = []

            # Prioritize rows that are currently visible in /api/matches so table joins work under limits.
            preferred_ids = _get_cached_upcoming_match_ids(limit=max(limit * 3, 1200))
            if preferred_ids:
                matches = sql_store.fetch_matches_by_ids(
                    preferred_ids,
                    bucket=data_manager.PRECACHEO_BUCKET,
                    limit=limit,
                )

            # Fill remaining slots with latest rows to keep behavior stable for other consumers.
            if len(matches) < limit:
                seen_ids = {
                    str(m.get('match_id') or m.get('id'))
                    for m in matches
                    if isinstance(m, dict) and (m.get('match_id') or m.get('id')) not in (None, '')
                }
                needed = limit - len(matches)
                refill_limit = max(limit, needed * 3)
                refill_rows = sql_store.fetch_matches(
                    bucket=data_manager.PRECACHEO_BUCKET,
                    limit=refill_limit,
                )
                for row in refill_rows:
                    if not isinstance(row, dict):
                        continue
                    raw_id = row.get('match_id') or row.get('id')
                    if raw_id in (None, ''):
                        continue
                    mid = str(raw_id)
                    if mid in seen_ids:
                        continue
                    seen_ids.add(mid)
                    matches.append(row)
                    if len(matches) >= limit:
                        break
        else:
            matches = data_manager.load_precacheo_matches()

        if not matches:
            matches = _load_precacheo_legacy_rows(limit=limit)

        # Fallback: if precache storage is empty, expose upcoming list as lightweight rows.
        # This prevents the Pre-Cacheo table from rendering completely empty after cold boots.
        if not matches:
            fallback_limit = limit if isinstance(limit, int) and limit > 0 else 1000
            fallback_upcoming = asyncio.run(
                get_main_page_matches_async(limit=fallback_limit, offset=0)
            ) or []
            if not fallback_upcoming:
                print("[api/precacheo_list] Empty precache + cache, trying direct scrape fallback...")
                fallback_upcoming = asyncio.run(
                    scrape_main_page_matches_async_direct(limit=fallback_limit, offset=0)
                ) or []
            if not fallback_upcoming:
                print("[api/precacheo_list] Direct scrape empty, trying grandes ligas fallback...")
                fallback_upcoming = _fetch_grandes_ligas_upcoming_matches(limit=fallback_limit) or []

            pseudo_rows = []
            for m in fallback_upcoming:
                mid = m.get('id') or m.get('match_id')
                if mid is None:
                    continue
                mid = str(mid)
                ah_line = m.get('handicap')
                ou_line = m.get('goal_line')
                pseudo_rows.append({
                    'match_id': mid,
                    'id': mid,
                    'home_name': m.get('home_team') or m.get('home_name') or '',
                    'away_name': m.get('away_team') or m.get('away_name') or '',
                    'league_name': m.get('league') or m.get('league_name') or '',
                    'match_date': m.get('match_date') or m.get('date') or m.get('start_time') or '',
                    'date': m.get('date') or m.get('match_date') or '',
                    'time': m.get('time') or '',
                    'start_time': m.get('start_time'),
                    'handicap': ah_line,
                    'main_match_odds': {
                        'ah_linea': ah_line,
                        'goals_linea': ou_line,
                    },
                    'specialist_picks': [],
                    'precache_placeholder': True,
                })
            matches = pseudo_rows

        # Asegurar que cada partido tenga la key (vacía por ahora)
        for m in matches:
            if 'specialist_picks' not in m:
                m['specialist_picks'] = []

        # El modo "pendientes" de /precacheo usa esta misma respuesta. Mezclamos
        # tambien el bucket de resultados pendientes para que conserve el analisis
        # completo y pueda renderizar el pick recomendado nuevo en ambas vistas.
        try:
            pending_limit = limit if isinstance(limit, int) and limit > 0 else None
            pending_rows = sql_store.fetch_matches(
                bucket=data_manager.PENDING_RESULTS_BUCKET,
                limit=pending_limit,
            )
        except Exception as pending_exc:
            print(f"[api/precacheo_list] No se pudieron cargar pendientes: {pending_exc}")
            pending_rows = []

        if pending_rows:
            seen_ids = {
                str(m.get('match_id') or m.get('id'))
                for m in matches
                if isinstance(m, dict) and (m.get('match_id') or m.get('id')) not in (None, '')
            }
            for pending_row in pending_rows:
                if not isinstance(pending_row, dict):
                    continue
                raw_id = pending_row.get('match_id') or pending_row.get('id')
                if raw_id in (None, ''):
                    continue
                mid = str(raw_id)
                if mid in seen_ids:
                    continue
                pending_row.setdefault('state', 'pending_results')
                pending_row.setdefault('bucket', data_manager.PENDING_RESULTS_BUCKET)
                pending_row.setdefault('specialist_picks', [])
                matches.append(pending_row)
                seen_ids.add(mid)

        compact_arg = str(request.args.get('compact', '1')).strip().lower()
        use_compact = compact_arg not in {'0', 'false', 'no'}
        if use_compact:
            matches = [_compact_precacheo_match_for_list(m) for m in matches if isinstance(m, dict)]

        return jsonify({'matches': matches})
    except Exception as e:
        print(f"Error loading precacheo: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/finished_matches_list')
def api_finished_matches_list():
    """Lista partidos terminados para la pestaña de Picks Pasados."""
    try:
        # Obtener partidos terminados de nowgoal
        from modules.nowgoal_fetcher import fetch_main_page
        html_content = fetch_main_page()
        
        if not html_content:
            return jsonify({'matches': [], 'error': 'No se pudo obtener datos'})
        
        # Parsear partidos terminados (máximo 500 para paginación de 100)
        finished = parse_main_page_finished_matches(html_content, limit=500, offset=0)
        
        # Formatear para el frontend
        matches = []
        for m in finished:
            matches.append({
                'match_id': m.get('id'),
                'home': m.get('home_team', 'Local'),
                'away': m.get('away_team', 'Visitante'),
                'score': m.get('score', '-'),
                'handicap': float(m.get('handicap', 0)) if m.get('handicap') not in [None, 'N/A', ''] else 0,
                'time': m.get('time', ''),
                'date': m.get('start_time', ''),
                'league': m.get('league', '')
            })
        
        return jsonify({'matches': matches})
    except Exception as e:
        print(f"Error loading finished matches: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e), 'matches': []}), 500


def _get_specialist_validator_cached():
    global _cached_specialist_validator, _cached_specialist_validator_failed
    if _cached_specialist_validator is not None or _cached_specialist_validator_failed:
        return _cached_specialist_validator

    with _picks_runtime_lock:
        if _cached_specialist_validator is not None or _cached_specialist_validator_failed:
            return _cached_specialist_validator
        try:
            from modules.specialist_validator import validator as sv
            sv.load_rules()
            _cached_specialist_validator = sv
        except Exception as exc:
            _cached_specialist_validator_failed = True
            print(f"[picks_batch] Specialist validator init error: {exc}")

    return _cached_specialist_validator


def _get_v2_loader_cached():
    global _cached_v2_loader, _cached_v2_loader_loaded_at, _cached_v2_loader_failed

    if not _env_flag('PRECACHEO_PICKS_ENABLE_V2', default=True):
        return None

    reload_seconds = _env_int('PRECACHEO_PICKS_V2_RELOAD_SECONDS', 1200)
    now_ts = time.time()

    with _picks_runtime_lock:
        should_reload = _cached_v2_loader is None
        if _cached_v2_loader is not None and reload_seconds > 0:
            should_reload = (now_ts - _cached_v2_loader_loaded_at) >= reload_seconds
        if _cached_v2_loader_failed and _cached_v2_loader is None:
            # Retry after cooldown
            should_reload = (now_ts - _cached_v2_loader_loaded_at) >= max(60, reload_seconds if reload_seconds > 0 else 300)

        if should_reload:
            try:
                from scripts.pattern_miner_v2.precacheo_loader import reload_loader, loader
                reload_loader()
                _cached_v2_loader = loader
                _cached_v2_loader_loaded_at = now_ts
                _cached_v2_loader_failed = False
            except Exception as exc:
                _cached_v2_loader = None
                _cached_v2_loader_loaded_at = now_ts
                _cached_v2_loader_failed = True
                print(f"[picks_batch] V2 loader init error: {exc}")

    return _cached_v2_loader


@app.route('/api/precacheo_picks_batch', methods=['POST'])
def api_precacheo_picks_batch():
    """Evalua exclusivamente la Clave Dicotomica Universal para un lote."""
    try:
        data = request.json
        match_ids = data.get('match_ids', [])
        
        if not match_ids:
            return jsonify({'picks': {}})

        # Normalizar IDs y aplicar límite de seguridad
        normalized_ids = []
        seen_ids = set()
        for mid in match_ids:
            if mid is None:
                continue
            mid_str = str(mid)
            if mid_str and mid_str not in seen_ids:
                seen_ids.add(mid_str)
                normalized_ids.append(mid_str)

        try:
            max_ids_raw = os.getenv('PRECACHEO_PICKS_BATCH_MAX', '')
            max_ids = int(max_ids_raw) if max_ids_raw else 0
        except Exception:
            max_ids = 0
        if max_ids <= 0:
            max_ids = 80 if _env_flag('RENDER_LITE_MODE', default=False) else 200
        if max_ids > 0 and len(normalized_ids) > max_ids:
            normalized_ids = normalized_ids[:max_ids]

        matches_by_id = {}
        for mid_str in normalized_ids:
            match = data_manager.get_precacheo_match(mid_str)
            if match:
                matches_by_id[mid_str] = match
        
        results = {}
        
        from modules.clave_universal_picks import build_universal_picks

        # Evaluar solo los partidos solicitados con una unica autoridad.
        for mid in normalized_ids:
            m = matches_by_id.get(mid)
            if not m:
                continue
            
            picks = build_universal_picks(m)
            results[str(mid)] = picks
        
        return jsonify({'picks': results})
    except Exception as e:
        print(f"Error in picks_batch: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/precacheo_scrape', methods=['POST'])
def api_precacheo_scrape():
    """Scrapea un partido y lo guarda en precacheo."""
    try:
        data = request.json
        match_id = data.get('match_id')
        
        if not match_id:
            return jsonify({'error': 'Falta match_id'}), 400
        
        # Es una accion manual: siempre consulta de nuevo la fuente.
        match_data = analizar_partido_completo(str(match_id), force_refresh=True)
        
        if not match_data or match_data.get('error'):
            return jsonify({'error': match_data.get('error', 'No se pudo scrapear')}), 500
        
        match_data['match_id'] = str(match_id)
        match_data['precacheo_date'] = datetime.datetime.now().isoformat()

        # Save to precacheo
        data_manager.save_precacheo_match(match_data)

        response_match = _compact_precacheo_match_for_list(match_data)
        response_id = str(response_match.get('id') or response_match.get('match_id') or match_id)
        response_match['id'] = response_id
        response_match['match_id'] = response_id
        if not response_match.get('home_team'):
            response_match['home_team'] = match_data.get('home_team') or match_data.get('home_name') or ''
        if not response_match.get('away_team'):
            response_match['away_team'] = match_data.get('away_team') or match_data.get('away_name') or ''
        if not response_match.get('league'):
            response_match['league'] = match_data.get('league') or match_data.get('league_name') or ''
        if 'specialist_picks' not in response_match:
            response_match['specialist_picks'] = []

        return jsonify({'status': 'success', 'match': response_match})
    except Exception as e:
        print(f"Error scraping for precacheo: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/precacheo_context_fast', methods=['POST'])
def api_precacheo_context_fast():
    """Actualiza solo casa/fuera + ultimo H2H y reutiliza el resultado durante 8 horas."""
    try:
        payload = request.json or {}
        match_id = "".join(filter(str.isdigit, str(payload.get('match_id') or '')))
        if not match_id:
            return jsonify({'error': 'Falta match_id'}), 400

        existing = data_manager.get_precacheo_match(match_id) or {}
        cached_context = existing.get('pre_match_context') if isinstance(existing, dict) else None
        generated_epoch = float((cached_context or {}).get('generated_at_epoch') or 0)
        cache_age = time.time() - generated_epoch if generated_epoch else None
        if cached_context and cache_age is not None and 0 <= cache_age < 8 * 3600 and not payload.get('force'):
            return jsonify({
                'status': 'success',
                'context': cached_context,
                'cached': True,
                'elapsed_seconds': 0,
            })

        main_odds = existing.get('main_match_odds') or {}
        context = analizar_contexto_previo_rapido(
            match_id,
            current_ah=main_odds.get('ah_linea') or existing.get('handicap'),
            current_goal_line=main_odds.get('goals_linea') or existing.get('goal_line'),
        )
        if not context or context.get('error'):
            return jsonify({'error': (context or {}).get('error', 'No se pudo generar el contexto')}), 500

        current = context.get('current') or {}
        merged = dict(existing)
        merged.update({
            'match_id': match_id,
            'home_name': merged.get('home_name') or current.get('home_name'),
            'away_name': merged.get('away_name') or current.get('away_name'),
            'league_name': merged.get('league_name') or current.get('league_name'),
            'pre_match_context': context,
            'context_scraped_at': context.get('generated_at'),
        })
        data_manager.save_precacheo_match(merged)
        return jsonify({
            'status': 'success',
            'context': context,
            'cached': False,
            'elapsed_seconds': context.get('elapsed_seconds'),
        })
    except Exception as exc:
        logging.exception('Error en /api/precacheo_context_fast')
        return jsonify({'error': str(exc)}), 500



@app.route('/api/precacheo_scrape_background', methods=['POST'])
def api_precacheo_scrape_background():
    """Endpoint para iniciar el scrapeo de pre-cacheo en background (con filtros)."""
    try:
        data = request.json or {}
        handicap_filter = data.get('handicap')
        goal_line_filter = data.get('ou')
        workers = data.get('workers', 5)
        
        # Iniciar hilo
        thread = threading.Thread(
            target=process_upcoming_matches_background,
            args=(handicap_filter, goal_line_filter, workers)
        )
        thread.daemon = True 
        thread.start()
        
        return jsonify({
            'status': 'success', 
            'message': f'Pre-Cacheo iniciado (Filtros: AH={handicap_filter}, OU={goal_line_filter}, Workers={workers}).'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/scrape_pending_results', methods=['POST'])
def api_scrape_pending_results():
    """Endpoint para scrapear resultados de partidos pendientes (+30 min sin score)."""
    try:
        # Iniciar hilo
        thread = threading.Thread(target=scrape_pending_results_background)
        thread.daemon = True 
        thread.start()
        
        return jsonify({
            'status': 'success', 
            'message': 'Buscando resultados de partidos pendientes (partidos +30 min sin score)...'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/precacheo_pending_list')
def api_precacheo_pending_list():
    """Devuelve resultados pendientes paginados y filtrados desde SQL."""
    try:
        _maybe_cleanup_precacheo_stale(force=False)

        try:
            page = max(1, int(request.args.get('page', '1')))
        except (TypeError, ValueError):
            page = 1
        try:
            per_page = max(1, min(int(request.args.get('per_page', '100')), 100))
        except (TypeError, ValueError):
            per_page = 100

        handicap_filters = []
        for raw_value in request.args.getlist('handicap'):
            for value in str(raw_value).split(','):
                value = value.strip()
                if value and value not in handicap_filters:
                    handicap_filters.append(value)

        result = pending_results_query.fetch_pending_page(
            page=page,
            per_page=per_page,
            handicap_buckets=handicap_filters,
            min_age_minutes=30,
            max_age_hours=48,
        )
        # pending_results_query ya lee explorer_json (payload compacto). No se
        # recalcula la Clave para 100 filas durante una simple navegación.
        result['matches'] = [
            row for row in result.get('matches', []) if isinstance(row, dict)
        ]
        body = json.dumps(result, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
        accepts_gzip = 'gzip' in str(request.headers.get('Accept-Encoding') or '').lower()
        if accepts_gzip:
            response = app.response_class(
                gzip.compress(body, compresslevel=3),
                content_type='application/json; charset=utf-8',
            )
            response.headers['Content-Encoding'] = 'gzip'
            response.headers['Vary'] = 'Accept-Encoding'
        else:
            response = app.response_class(body, content_type='application/json; charset=utf-8')
        response.headers['Cache-Control'] = 'no-store'
        return response
    except Exception as exc:
        logging.exception("Error loading paginated pending results")
        return jsonify({'error': str(exc), 'matches': []}), 500


@app.route('/api/precacheo_upcoming_list')
def api_precacheo_upcoming_list():
    """Devuelve próximos partidos paginados de 100 en 100 desde SQL."""
    try:
        _maybe_cleanup_precacheo_stale(force=False)
        try:
            page = max(1, int(request.args.get('page', '1')))
        except (TypeError, ValueError):
            page = 1
        try:
            per_page = max(1, min(int(request.args.get('per_page', '100')), 100))
        except (TypeError, ValueError):
            per_page = 100
        handicap_filters = []
        for raw_value in request.args.getlist('handicap'):
            for value in str(raw_value).split(','):
                value = value.strip()
                if value and value not in handicap_filters:
                    handicap_filters.append(value)
        result = pending_results_query.fetch_upcoming_page(
            page=page,
            per_page=per_page,
            handicap_buckets=handicap_filters,
        )
        body = json.dumps(result, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
        accepts_gzip = 'gzip' in str(request.headers.get('Accept-Encoding') or '').lower()
        if accepts_gzip:
            response = app.response_class(
                gzip.compress(body, compresslevel=3),
                content_type='application/json; charset=utf-8',
            )
            response.headers['Content-Encoding'] = 'gzip'
            response.headers['Vary'] = 'Accept-Encoding'
        else:
            response = app.response_class(body, content_type='application/json; charset=utf-8')
        response.headers['Cache-Control'] = 'no-store'
        return response
    except Exception as exc:
        logging.exception("Error loading paginated upcoming matches")
        return jsonify({'error': str(exc), 'matches': []}), 500


@app.route('/api/housemind_ou/status')
def api_housemind_ou_status():
    """Expose the frozen model audit and its current safety gate."""
    return jsonify(housemind_ou.model_status())


@app.route('/api/housemind_ou/<string:match_id>')
def api_housemind_ou_match(match_id):
    """Return an auditable O/U decision, including explicit NO_BET reasons."""
    match = data_manager.get_precacheo_match(match_id) or sql_store.get_match(str(match_id))
    if not match:
        return jsonify({'error': 'Partido no encontrado'}), 404

    prediction = housemind_ou.predict_probability(match)
    feature_vector = prediction.pop('feature_vector', {}) or {}
    prediction['quality'] = feature_vector.get('quality') or {}
    prediction['match'] = {
        'match_id': str(match_id),
        'home': match.get('home_name') or match.get('home_team') or '',
        'away': match.get('away_name') or match.get('away_team') or '',
        'league': match.get('league_name') or match.get('league') or '',
        **(feature_vector.get('meta') or {}),
    }
    return jsonify(prediction)


@app.route('/api/precacheo_last_general', methods=['POST'])
def api_precacheo_last_general():
    """Scrapea bajo demanda el contexto de último partido general de ambos equipos."""
    try:
        payload = request.get_json(silent=True) or {}
        match_id = payload.get('match_id')
        force_refresh = bool(payload.get('force_refresh'))
        if not match_id:
            return jsonify({'status': 'error', 'error': 'Falta match_id'}), 400

        context, cached = last_general_context.get_or_create(str(match_id), force_refresh=force_refresh)
        if not context or context.get('error'):
            return jsonify({
                'status': 'error',
                'error': (context or {}).get('error', 'No se pudo generar Último General')
            }), 500

        return jsonify({'status': 'success', 'cached': cached, 'context': context})
    except Exception as exc:
        logging.exception("Error en /api/precacheo_last_general")
        return jsonify({'status': 'error', 'error': str(exc)}), 500


@app.route('/api/precacheo_last_general_batch', methods=['POST'])
def api_precacheo_last_general_batch():
    """Procesa Último General para una lista elegida por el usuario."""
    try:
        payload = request.get_json(silent=True) or {}
        raw_ids = payload.get('match_ids') or []
        force_refresh = bool(payload.get('force_refresh'))
        try:
            max_items = int(payload.get('max_items') or 0) or None
        except Exception:
            max_items = None

        if not raw_ids:
            raw_ids = [
                str(m.get('match_id') or m.get('id'))
                for m in data_manager.load_precacheo_matches()
                if m.get('match_id') or m.get('id')
            ]

        result = last_general_context.process_match_ids(raw_ids, force_refresh=force_refresh, max_items=max_items)
        return jsonify({'status': 'success', **result})
    except Exception as exc:
        logging.exception("Error en /api/precacheo_last_general_batch")
        return jsonify({'status': 'error', 'error': str(exc)}), 500


@app.route('/api/precacheo_rival_handicap_samples', methods=['POST'])
def api_precacheo_rival_handicap_samples():
    """Scraping manual de muestras AH y comparacion Col3 ampliada."""
    try:
        payload = request.get_json(silent=True) or {}
        match_id = payload.get('match_id')
        if not match_id:
            return jsonify({'status': 'error', 'error': 'Falta match_id'}), 400

        context = rival_handicap_samples.analyze(str(match_id))
        if not context or context.get('error'):
            return jsonify({
                'status': 'error',
                'error': (context or {}).get('error', 'No se pudo generar la comparativa AH')
            }), 500
        return jsonify({'status': 'success', 'cached': False, 'context': context})
    except Exception as exc:
        logging.exception("Error en /api/precacheo_rival_handicap_samples")
        return jsonify({'status': 'error', 'error': str(exc)}), 500


@app.route('/api/precacheo_finalize/<match_id>', methods=['POST'])
def api_precacheo_finalize(match_id):
    """Re-scrapea un partido finalizado y lo mueve al bucket oficial."""
    try:
        # Re-scrape to get final result
        match_data = analizar_partido_completo(str(match_id), force_refresh=True)
        
        if not match_data or match_data.get('error'):
            return jsonify({'error': match_data.get('error', 'No se pudo re-scrapear')}), 500
        
        match_data['match_id'] = str(match_id)
        
        # Check if match is actually finished
        score = match_data.get('score') or match_data.get('final_score')
        if not score or score in ['??', '?-?', '? - ?']:
            return jsonify({'error': 'El partido aún no ha terminado'}), 400
        
        # Save to official bucket
        data_manager.save_match(match_data)
        
        # Remove from precacheo
        data_manager.remove_from_precacheo(match_id)
        
        return jsonify({'status': 'success', 'message': f'Partido {match_id} finalizado y movido al bucket oficial.'})
    except Exception as e:
        print(f"Error finalizing precacheo match: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/precacheo_finalize_batch', methods=['POST'])
def api_precacheo_finalize_batch():
    """Finaliza un lote de partidos (optimizado)."""
    try:
        data = request.json
        match_ids = data.get('match_ids', [])
        
        if not match_ids:
            return jsonify({'error': 'No match_ids provided'}), 400

        success, failed, errors = data_manager.finalize_precacheo_batch(match_ids)
        
        return jsonify({
            'status': 'success',
            'processed': len(match_ids),
            'success_count': success,
            'failed_count': failed,
            'errors': errors[:5] # Limit errors returned
        })
    except Exception as e:
        print(f"Error executing batch finalize: {e}")
        return jsonify({'error': str(e)}), 500
@app.route('/api/decode_match', methods=['POST'])
def api_decode_match():
    """
    Endpoint para el análisis de 'Mente del Bookie'.
    Analiza un partido basado en el lenguaje de hándicaps y lógica forense.
    """
    try:
        data = request.json
        match_id = data.get('match_id')
        
        if not match_id:
            return jsonify({'error': 'Falta match_id'}), 400
            
        # Obtener datos completos del partido (usando caché si es posible)
        match_data = analizar_partido_completo(str(match_id), force_refresh=False)
        
        if not match_data or 'error' in match_data:
            return jsonify({'error': 'No se pudieron obtener datos completos para el análisis.'}), 500
            
        # Ejecutar análisis del diccionario
        report = analyze_match_bookie_logic(match_data)
        
        return jsonify({
            'status': 'success',
            'match_id': match_id,
            'report': report
        })
    except Exception as e:
        print(f"Error en decode match: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/scraper')
def scraper_view():
    pending_matches = history_manager.get_pending_matches()
    return render_template('scraper.html', pending_matches=pending_matches)


@app.route('/extraer-liga')
def league_extractor_view():
    """Pantalla dedicada a importar y analizar una liga completa por jornadas."""
    return render_template('league_extractor.html')


LEAGUE_AH_JOBS = {}
LEAGUE_AH_JOBS_LOCK = threading.Lock()


def _update_league_ah_job(job_id, **changes):
    with LEAGUE_AH_JOBS_LOCK:
        job = LEAGUE_AH_JOBS.get(job_id)
        if job is not None:
            job.update(changes)


def _run_league_ah_job(job_id, extraction_id, matches, league_id, workers, force):
    _update_league_ah_job(job_id, status='running', started_at=datetime.datetime.utcnow().isoformat())
    league_extraction_registry.update_extraction_status(extraction_id, 'running')
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    league_handicap_scraper.scrape_match_to_sql,
                    match,
                    league_id,
                    force,
                ): match
                for match in matches
            }
            for future in concurrent.futures.as_completed(futures):
                source_match = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    result = {
                        'id': source_match.get('id'),
                        'status': 'error',
                        'error': str(exc),
                    }
                result['round'] = str(source_match.get('round') or '')
                league_extraction_registry.update_match(
                    extraction_id,
                    str(source_match.get('id') or ''),
                    result,
                )

                with LEAGUE_AH_JOBS_LOCK:
                    job = LEAGUE_AH_JOBS.get(job_id)
                    if job is None:
                        return
                    job['processed'] += 1
                    job['results'].append(result)
                    result_status = result.get('status', 'error')
                    job['counts'][result_status] = job['counts'].get(result_status, 0) + 1
                    job['updated_at'] = datetime.datetime.utcnow().isoformat()

        _update_league_ah_job(
            job_id,
            status='completed',
            completed_at=datetime.datetime.utcnow().isoformat(),
        )
        league_extraction_registry.update_extraction_status(extraction_id, 'completed')
    except Exception as exc:
        _update_league_ah_job(
            job_id,
            status='failed',
            error=str(exc),
            completed_at=datetime.datetime.utcnow().isoformat(),
        )
        league_extraction_registry.update_extraction_status(extraction_id, 'failed')


@app.route('/api/league_handicap/preview', methods=['POST'])
def api_league_handicap_preview():
    try:
        payload = request.get_json(silent=True) or {}
        league_reference = str(payload.get('league_reference') or '').strip()
        if not league_reference:
            return jsonify({'error': 'Introduce la URL o el ID de la liga'}), 400

        raw_target_ah = payload.get('ah')
        try:
            target_ah = None if raw_target_ah in (None, '') else float(raw_target_ah)
            company_id = int(payload.get('company_id', 8))
        except (TypeError, ValueError):
            return jsonify({'error': 'El handicap opcional y la casa deben ser valores numericos'}), 400

        match_status = str(payload.get('match_status') or 'all').strip().lower()
        if match_status not in {'all', 'finished', 'upcoming'}:
            return jsonify({'error': 'Filtro de estado no valido'}), 400

        preview = league_handicap_scraper.preview_league_handicap(
            league_reference=league_reference,
            target_ah=target_ah,
            season=str(payload.get('season') or '').strip(),
            company_id=company_id,
            match_status=match_status,
        )
        return jsonify({'status': 'success', **preview})
    except (ValueError, RuntimeError) as exc:
        return jsonify({'error': str(exc)}), 400
    except requests.RequestException as exc:
        return jsonify({'error': f'NowGoal no respondio correctamente: {exc}'}), 502
    except Exception as exc:
        app.logger.exception('Error previsualizando liga por handicap')
        return jsonify({'error': str(exc)}), 500


@app.route('/api/league_handicap/start', methods=['POST'])
def api_league_handicap_start():
    try:
        payload = request.get_json(silent=True) or {}
        league_id = ''.join(filter(str.isdigit, str(payload.get('league_id') or '')))
        if not league_id:
            return jsonify({'error': 'ID de liga no valido'}), 400

        raw_target_ah = payload.get('target_ah')
        try:
            company_id = int(payload.get('company_id', 8))
            target_ah = None if raw_target_ah in (None, '') else float(raw_target_ah)
            workers = max(1, min(10, int(payload.get('workers', 4))))
        except (TypeError, ValueError):
            return jsonify({'error': 'Parametros numericos no validos'}), 400

        matches = league_handicap_scraper.sanitize_selected_matches(
            payload.get('matches') or [],
            company_id,
        )
        if target_ah is not None:
            matches = [
                m for m in matches
                if m.get('visible_ah') is not None and abs(m['visible_ah'] - target_ah) < 1e-9
            ]
        if not matches:
            return jsonify({'error': 'No hay partidos validos seleccionados'}), 400
        if len(matches) > 500:
            return jsonify({'error': 'El limite por trabajo es de 500 partidos'}), 400

        extraction = league_extraction_registry.create_extraction(
            league_id=league_id,
            league_name=str(payload.get('league_name') or f'Liga {league_id}').strip(),
            season=str(payload.get('season') or '').strip(),
            company_id=company_id,
            target_ah=target_ah,
            matches=matches,
            label=str(payload.get('label') or '').strip(),
        )
        extraction_id = extraction['extraction_id']
        job_id = uuid.uuid4().hex
        job = {
            'job_id': job_id,
            'extraction_id': extraction_id,
            'status': 'queued',
            'league_id': league_id,
            'target_ah': target_ah,
            'company_id': company_id,
            'total': len(matches),
            'processed': 0,
            'counts': {},
            'results': [],
            'created_at': datetime.datetime.utcnow().isoformat(),
            'updated_at': datetime.datetime.utcnow().isoformat(),
        }
        with LEAGUE_AH_JOBS_LOCK:
            completed_jobs = [
                key for key, value in LEAGUE_AH_JOBS.items()
                if value.get('status') in {'completed', 'failed'}
            ]
            while len(LEAGUE_AH_JOBS) >= 50 and completed_jobs:
                LEAGUE_AH_JOBS.pop(completed_jobs.pop(0), None)
            LEAGUE_AH_JOBS[job_id] = job

        thread = threading.Thread(
            target=_run_league_ah_job,
            args=(job_id, extraction_id, matches, league_id, workers, bool(payload.get('force'))),
            daemon=True,
            name=f'league-ah-{job_id[:8]}',
        )
        thread.start()
        return jsonify({
            'status': 'started',
            'job_id': job_id,
            'extraction_id': extraction_id,
            'total': len(matches),
        }), 202
    except Exception as exc:
        app.logger.exception('Error iniciando scrapeo por handicap')
        return jsonify({'error': str(exc)}), 500


@app.route('/api/league_handicap/status/<job_id>')
def api_league_handicap_status(job_id):
    with LEAGUE_AH_JOBS_LOCK:
        job = LEAGUE_AH_JOBS.get(str(job_id))
        if job is None:
            return jsonify({'error': 'Trabajo no encontrado'}), 404
        snapshot = json.loads(json.dumps(job, ensure_ascii=False))
    return jsonify(snapshot)


@app.route('/api/league-extractions')
def api_league_extractions():
    return jsonify({'extractions': league_extraction_registry.list_extractions()})


@app.route('/api/league-extractions/register', methods=['POST'])
def api_register_league_extraction():
    payload = request.get_json(silent=True) or {}
    league_id = ''.join(filter(str.isdigit, str(payload.get('league_id') or '')))
    if not league_id:
        return jsonify({'error': 'ID de liga no valido'}), 400
    try:
        company_id = int(payload.get('company_id', 8))
        raw_target_ah = payload.get('target_ah')
        target_ah = None if raw_target_ah in (None, '') else float(raw_target_ah)
    except (TypeError, ValueError):
        return jsonify({'error': 'Parametros numericos no validos'}), 400
    matches = payload.get('matches') or []
    if not isinstance(matches, list) or not matches:
        return jsonify({'error': 'No hay calendario para registrar'}), 400
    extraction = league_extraction_registry.register_existing_league(
        league_id=league_id,
        league_name=str(payload.get('league_name') or f'Liga {league_id}').strip(),
        season=str(payload.get('season') or '').strip(),
        company_id=company_id,
        target_ah=target_ah,
        matches=matches,
        label=str(payload.get('label') or '').strip(),
    )
    return jsonify({'status': 'registered', 'extraction': extraction})


def _league_extraction_match_payload(extraction, source_match):
    match_id = str(source_match.get('id') or '')
    stored = sql_store.get_match(match_id) if match_id else None
    if isinstance(stored, dict):
        compact = {
            key: stored.get(key)
            for key in (
                'match_id',
                'home_name',
                'home_team',
                'away_name',
                'away_team',
                'final_score',
                'score',
                'match_date',
                'date',
                'handicap',
                'goal_line',
                'main_match_odds',
                'last_home_match',
                'last_away_match',
                'h2h_stadium',
                'h2h_general',
                'h2h_col3',
                'comparativas_indirectas',
                'market_analysis_data',
            )
            if stored.get(key) is not None
        }
    else:
        compact = None
    return {
        'source': source_match,
        'data': compact,
    }


def _league_round_groups(matches):
    """Agrupa el registro por fase y jornada sin mezclar rondas homónimas."""
    groups = {}
    sub_order = {}
    stage_translations = {
        'league': '',
        'final qual.': 'Clasificación final',
        'semifinals': 'Semifinales',
        'semifinal': 'Semifinal',
        'final': 'Final',
    }
    for source_index, match in enumerate(matches):
        sub_id = str(match.get('sub_id') or '0')
        round_value = str(match.get('round') or 'Sin jornada')
        round_key = f'{sub_id}:{round_value}'
        sub_order.setdefault(sub_id, source_index)
        group = groups.setdefault(round_key, {
            'key': round_key,
            'round': round_value,
            'sub_id': sub_id,
            'sub_name': str(match.get('sub_name') or '').strip(),
            'matches': [],
        })
        group['matches'].append(match)

    def round_sort_value(value):
        return (0, int(value)) if str(value).isdigit() else (1, str(value).casefold())

    ordered = sorted(
        groups.values(),
        key=lambda group: (
            sub_order.get(group['sub_id'], 10**9),
            round_sort_value(group['round']),
        ),
    )
    for group in ordered:
        stage = group['sub_name']
        translated = stage_translations.get(stage.casefold(), stage)
        base = f"Jornada {group['round']}"
        group['label'] = f'{translated} · {base}' if translated else base
        group['count'] = len(group['matches'])
        group['available'] = sum(
            1 for match in group['matches']
            if str(match.get('status') or '') in {'saved', 'exists'}
        )
    return ordered


@app.route('/api/league-extractions/<extraction_id>')
def api_league_extraction_detail(extraction_id):
    extraction = league_extraction_registry.get_extraction(extraction_id)
    if extraction is None:
        return jsonify({'error': 'Extraccion no encontrada'}), 404
    registered = extraction.pop('matches', [])
    round_groups = _league_round_groups(registered)
    requested_round = str(request.args.get('round') or '').strip()
    active_group = next(
        (group for group in round_groups if group['key'] == requested_round),
        round_groups[0] if round_groups else None,
    )
    selected_matches = active_group['matches'] if active_group else []
    public_rounds = [
        {key: group[key] for key in ('key', 'label', 'round', 'sub_id', 'sub_name', 'count', 'available')}
        for group in round_groups
    ]
    return jsonify({
        'extraction': extraction,
        'total': len(registered),
        'round_total': len(selected_matches),
        'current_round': active_group['key'] if active_group else None,
        'rounds': public_rounds,
        'matches': [
            _league_extraction_match_payload(extraction, match)
            for match in selected_matches
        ],
    })


@app.route('/api/league-extractions/<extraction_id>/match/<match_id>')
def api_league_extraction_match(extraction_id, match_id):
    extraction = league_extraction_registry.get_extraction(extraction_id)
    if extraction is None:
        return jsonify({'error': 'Extraccion no encontrada'}), 404
    clean_id = ''.join(filter(str.isdigit, str(match_id)))
    source_match = next(
        (match for match in extraction.get('matches', []) if str(match.get('id')) == clean_id),
        None,
    )
    if source_match is None:
        return jsonify({'error': 'El partido no pertenece a esta extraccion'}), 404
    return jsonify(_league_extraction_match_payload(extraction, source_match))


@app.route('/api/scrape_league', methods=['POST'])
def api_scrape_league():
    try:
        data = request.json
        season = data.get('season')
        league_ids_raw = data.get('league_ids')
        ah_filter = data.get('ah_filter') # New filter

        if not season or not league_ids_raw:
            return jsonify({'error': 'Faltan datos (season, league_ids)'}), 400

        league_ids = [lid.strip() for lid in league_ids_raw.split(',') if lid.strip()]
        
        results = []
        total_matches = 0
        
        for lid in league_ids:
            # Pass ah_filter to the scraper
            scrape_result = league_scraper.extract_ids_by_params(season, lid, ah_filter=ah_filter)
            
            if "error" in scrape_result:
                results.append(f"Liga {lid}: Error - {scrape_result['error']}")
            else:
                matches = scrape_result['match_data']
                count = len(matches)
                total_matches += count
                
                # Add to pending matches
                history_manager.add_pending_matches(season, lid, matches)
                results.append(f"Liga {lid}: {count} partidos encontrados.")

        return jsonify({
            'message': f"Proceso completado. Total partidos encontrados: {total_matches}",
            'details': results
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Global list to track active scraper processes
ACTIVE_SCRAPERS = []
ACTIVE_SCRAPERS_LOCK = threading.Lock()

@app.route('/api/stop_scraping', methods=['POST'])
def api_stop_scraping():
    """Stops all active scraper subprocesses."""
    count = 0
    with ACTIVE_SCRAPERS_LOCK:
        for p in ACTIVE_SCRAPERS:
            try:
                p.terminate() # Try graceful termination first
                count += 1
            except Exception as e:
                print(f"Error stopping process: {e}")
        ACTIVE_SCRAPERS.clear()
        
    return jsonify({"message": f"Se han detenido {count} procesos de scraping."})

@app.route('/api/cache_matches', methods=['POST'])
def api_cache_matches():
    # ... existing code ...
    pass # Placeholder, do not replace entire function if not full content

# --- PENDING MATCHES ENDPOINTS ---

@app.route('/api/pending_matches')
def api_pending_matches():
    """Devuelve la lista de partidos con resultado pendiente (??)."""
    try:
        matches = data_manager.load_pending_results_matches()
        return jsonify({'matches': matches})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/reanalyze_pending', methods=['POST'])
def api_reanalyze_pending():
    """Re-analiza un partido pendiente para ver si ya tiene resultado."""
    try:
        data = request.json
        match_id = data.get('match_id')
        if not match_id:
             return jsonify({'error': 'Falta match_id'}), 400
             
        # Re-analyze
        match_data = analizar_partido_completo(str(match_id), force_refresh=True)
        if not match_data or match_data.get('error'):
             return jsonify({'error': 'Falló el análisis'}), 500
             
        # Check new score
        score = match_data.get('score') or match_data.get('final_score')
        result_found = score and score != '??' and score != '?-?'
        
        # Save (this handles moving to correct bucket if score found, or updating pending if not)
        data_manager.save_match(match_data)
        
        # Defensive cleanup in case the match still exists in pending bucket.
        if result_found:
            data_manager.remove_pending_match(match_id)
        
        return jsonify({
            'status': 'success', 
            'match': match_data,
            'result_found': bool(result_found)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# =============================================
# AI PREDICTION ENDPOINT (Groq API)
# =============================================
GROQ_API_KEY = os.environ.get('GROQ_API_KEY', '')

def _ai_prediction_has_core_context(match_data):
    """Comprueba si tenemos contexto suficiente para generar una predicción útil."""
    if not isinstance(match_data, dict) or not match_data:
        return False

    last_home = match_data.get('last_home_match') or {}
    last_away = match_data.get('last_away_match') or {}
    home_rank = (match_data.get('home_standings') or {}).get('ranking') or match_data.get('home_rank')
    away_rank = (match_data.get('away_standings') or {}).get('ranking') or match_data.get('away_rank')
    handicap = (
        match_data.get('handicap')
        or match_data.get('asian_handicap_raw')
        or match_data.get('asian_handicap')
        or (match_data.get('main_match_odds') or {}).get('ah_linea')
        or (match_data.get('main_match_odds') or {}).get('ah_linea_raw')
    )

    return bool(
        last_home.get('score')
        and last_away.get('score')
        and home_rank
        and away_rank
        and handicap
    )


def _load_ai_prediction_match_data(match_id):
    """
    Carga el partido desde precacheo.
    Si faltan datos clave, intenta scrapearlo al vuelo para no generar texto pobre o inventado.
    """
    cached_match = data_manager.get_precacheo_match(str(match_id)) or {}
    if _ai_prediction_has_core_context(cached_match):
        return cached_match

    try:
        refreshed_match = analizar_partido_completo(str(match_id))
    except Exception as exc:
        print(f"AI Prediction refresh error for {match_id}: {exc}")
        return cached_match if isinstance(cached_match, dict) else {}

    if refreshed_match and not refreshed_match.get('error'):
        refreshed_match['match_id'] = str(match_id)
        try:
            data_manager.save_precacheo_match(refreshed_match)
        except Exception as exc:
            print(f"AI Prediction cache save warning for {match_id}: {exc}")
        return refreshed_match

    return cached_match if isinstance(cached_match, dict) else {}


def _resolve_ai_prediction_team_name(match_data, frontend_name, home_side=True):
    if frontend_name:
        return frontend_name

    if not isinstance(match_data, dict):
        return 'Home Team' if home_side else 'Away Team'

    if home_side:
        return match_data.get('home_name') or match_data.get('home_team') or 'Home Team'

    return match_data.get('away_name') or match_data.get('away_team') or 'Away Team'


def _resolve_ai_prediction_handicap(match_data):
    if not isinstance(match_data, dict):
        return 'N/A'

    main_match_odds = match_data.get('main_match_odds') or {}
    candidates = [
        match_data.get('handicap'),
        match_data.get('asian_handicap_raw'),
        match_data.get('asian_handicap'),
        main_match_odds.get('ah_linea'),
        main_match_odds.get('ah_linea_raw'),
    ]

    for value in candidates:
        if value not in (None, '', 'N/A', '-', '?', '??'):
            return str(value)
    return 'N/A'


def _format_ai_prediction_previous_match(team_name, prev_match):
    if not isinstance(prev_match, dict) or not prev_match.get('score'):
        return 'No data'

    prev_home_team = (prev_match.get('home_team') or '').strip()
    prev_away_team = (prev_match.get('away_team') or '').strip()
    normalized_team = (team_name or '').strip().lower()

    if normalized_team and prev_home_team.lower() == normalized_team:
        venue_label = 'home'
        opponent = prev_away_team or 'unknown opponent'
    elif normalized_team and prev_away_team.lower() == normalized_team:
        venue_label = 'away'
        opponent = prev_home_team or 'unknown opponent'
    else:
        venue_label = 'previous'
        opponent = prev_away_team or prev_home_team or 'unknown opponent'

    ah_value = prev_match.get('handicap_line_raw') or prev_match.get('ah') or 'N/A'
    match_date = prev_match.get('date') or 'unknown date'
    score = prev_match.get('score') or 'N/A'

    return (
        f"{team_name}'s last {venue_label} game was {score} against "
        f"{opponent} on {match_date} (AH: {ah_value})"
    )


def _extract_ai_prediction_dangerous_attacks(prev_match):
    if not isinstance(prev_match, dict):
        return 'No data'

    stats_rows = prev_match.get('stats_rows') or []
    for stat in stats_rows:
        label = str(stat.get('label', '')).lower()
        if 'peligros' in label or 'dangerous' in label:
            return f"{stat.get('home', 'N/A')} vs {stat.get('away', 'N/A')}"

    return 'No data'


@app.route('/api/ai_prediction', methods=['POST'])
def api_ai_prediction():
    """Generate AI match prediction using Groq API."""
    try:
        data = request.get_json() or {}
        match_id = data.get('match_id')
        winner = data.get('winner', 'home')  # 'home' or 'away'
        
        # Accept team names from frontend (in case precacheo data doesn't have them)
        frontend_home_team = data.get('home_team')
        frontend_away_team = data.get('away_team')
        
        if not match_id:
            return jsonify({'error': 'match_id is required'}), 400
        
        if not GROQ_API_KEY:
            return jsonify({'error': 'GROQ_API_KEY no configurada'}), 500
        
        # Get match data from precacheo, refreshing it if the stored context is incomplete.
        match_data = _load_ai_prediction_match_data(str(match_id))
        
        # Use frontend team names if available, fallback to precacheo data
        home_team = _resolve_ai_prediction_team_name(match_data, frontend_home_team, home_side=True)
        away_team = _resolve_ai_prediction_team_name(match_data, frontend_away_team, home_side=False)
        handicap = _resolve_ai_prediction_handicap(match_data)
        
        # Get prev match info if available
        prev_home = match_data.get('last_home_match', {})
        prev_away = match_data.get('last_away_match', {})
        
        prev_home_info = _format_ai_prediction_previous_match(home_team, prev_home)
        prev_away_info = _format_ai_prediction_previous_match(away_team, prev_away)
        prev_home_stats = _extract_ai_prediction_dangerous_attacks(prev_home)
        prev_away_stats = _extract_ai_prediction_dangerous_attacks(prev_away)
        
        # Determine winner team name
        winner_team = home_team if winner == 'home' else away_team
        
        # Get team rankings
        home_rank = (match_data.get('home_standings') or {}).get('ranking') or match_data.get('home_rank') or 'N/A'
        away_rank = (match_data.get('away_standings') or {}).get('ranking') or match_data.get('away_rank') or 'N/A'
        
        # Build prompt with specific winner and persona
        import random
        
        # Random starter phrases to avoid repetitive openings
        starters = [
            "El ángulo que más me gusta aquí:",
            "La lectura rápida de este partido:",
            "Lo que me hace entrar aquí:",
            "La clave de este cruce está en esto:",
            "Si me tengo que quedar con un lado, es este:",
            "La sensación más clara que me deja este partido:",
            "Hay un detalle muy fuerte en este encuentro:",
            "La mejor forma de leer este choque es esta:",
        ]
        random_starter = random.choice(starters)
        
        prompt = f"""{random_starter}

Escribe un pronóstico futbolístico en ESPAÑOL, con tono humano y natural, como si se lo mandarás a un colega que apuesta.

Partido: {home_team} [Rank {home_rank}] vs {away_team} [Rank {away_rank}]
Hándicap asiático actual: {handicap}

Último resultado relevante del local: {prev_home_info}
Último resultado relevante del visitante: {prev_away_info}
Ataques peligrosos del local en ese partido: {prev_home_stats}
Ataques peligrosos del visitante en ese partido: {prev_away_stats}

SELECCIÓN OBLIGATORIA: gana {winner_team}

REGLAS OBLIGATORIAS:
1. Menciona SIEMPRE los dos últimos resultados y el rival de cada uno si está disponible.
2. Menciona el ranking de ambos equipos y el hándicap asiático actual.
3. Usa un único párrafo de entre 80 y 150 palabras.
4. No empieces con "{winner_team}" ni con una frase genérica repetitiva.
5. Tono humano, directo y natural. Seguro, pero no robótico.
6. No hagas listas ni viñetas.
7. Usa SOLO los datos de arriba. No inventes rivales, marcadores, rankings ni estadísticas.
8. Si algún dato sale como "No data", dilo de forma natural en vez de inventarlo.

Escribe el pronóstico ahora:"""

        # Call Groq API
        from groq import Groq
        
        client = Groq(api_key=GROQ_API_KEY)
        
        chat_completion = client.chat.completions.create(
            messages=[
                {"role": "user", "content": prompt}
            ],
            model="llama-3.3-70b-versatile",  # Fast and capable model
            temperature=0.9, # Higher temperature for more natural/unpredictable text
            max_tokens=350
        )
        
        prediction_text = chat_completion.choices[0].message.content
        
        return jsonify({
            'status': 'success',
            'match_id': match_id,
            'home_team': home_team,
            'away_team': away_team,
            'handicap': handicap,
            'prediction': prediction_text
        })
        
    except ImportError:
        return jsonify({'error': 'groq package not installed. Run: pip install groq'}), 500
    except Exception as e:
        print(f"AI Prediction error: {e}")
        return jsonify({'error': str(e)}), 500

# ==========================================
#  PATTERN LAB - VALIDATION ENDPOINT
# ==========================================
@app.route('/api/validate_pattern', methods=['POST'])
def api_validate_pattern():
    """
    Valida un patrón desde la PERSPECTIVA DEL FAVORITO.
    Recibe:
    - prev_fav_wdl: 'W', 'D', 'L' (Resultado del favorito en su partido previo)
    - prev_fav_ah: Handicap que tuvo el favorito en su partido previo (ej: -0.5, +1.5)
    - prev_fav_margin: Margen del favorito (goles a favor - goles en contra)
    - current_ah: Handicap actual del partido (para determinar quién es favorito)
    - h2h_winner: 'Rival_Fav', 'Rival_Underdog', 'Draw' (Optional)
    """
    try:
        payload = request.json
        
        # Criterios de búsqueda (NORMALIZADOS AL FAVORITO)
        target_fav_wdl = payload.get('prev_fav_wdl')  # W/D/L del favorito
        target_fav_ah_bucket = payload.get('prev_fav_ah_bucket')  # Bucket del AH previo del fav (ej: 0.5, 1, 1.5)
        target_fav_margin_range = payload.get('prev_fav_margin')  # Rango de margen (ej: ">2", "1-2", "0")
        target_h2h_winner = payload.get('h2h_winner')
        current_ah = float(payload.get('ah_line', 0))

        if target_fav_ah_bucket in ('', None):
            target_fav_ah_bucket = None
        elif isinstance(target_fav_ah_bucket, str):
            try:
                target_fav_ah_bucket = float(target_fav_ah_bucket)
            except ValueError:
                pass
        
        # Cargar históricos desde SQL (sin dependencia de testing_inputs/testing_results.json)
        inputs = sql_store.fetch_matches(state='historical')
        if not inputs:
            return jsonify({'status': 'error', 'message': 'Historical data not found in SQL'}), 404
            
        matched_matches = []
        stats = {'total': 0, 'fav_wins': 0, 'underdog_wins': 0, 'voids': 0}
        
        # Helpers
        def parse_score_l(s):
            if not s: return None
            try:
                p = s.replace('-', ':').split(':')
                return int(p[0]), int(p[1])
            except: return None

        def get_wdl_and_margin(s_str, team, h_team, a_team):
            """Retorna (WDL, margin_for_team)"""
            s = parse_score_l(s_str)
            if not s: return 'N/A', 0
            
            if team == h_team:
                margin = s[0] - s[1]
                if margin > 0: return 'W', margin
                elif margin < 0: return 'L', margin
                else: return 'D', 0
            elif team == a_team:
                margin = s[1] - s[0]
                if margin > 0: return 'W', margin
                elif margin < 0: return 'L', margin
                else: return 'D', 0
            return 'N/A', 0
        
        def bucket_ah(ah_val):
            """Agrupa AH en buckets: 0, 0.5, 1, 1.5, 2+"""
            if ah_val in (None, '', '-', 'N/A', '??', '?'):
                return None
            try:
                ah_num = float(ah_val)
            except (TypeError, ValueError):
                return None

            av = abs(ah_num)
            sign = -1 if ah_num < 0 else 1
            
            if av < 0.01: return 0.0
            elif 0.24 <= av <= 0.76: return 0.5 * sign
            elif 0.99 <= av <= 1.01: return 1.0 * sign
            elif 1.24 <= av <= 1.76: return 1.5 * sign
            elif av >= 1.99: return 2.0 * sign
            else: return round(av * 2) / 2 * sign  # Fallback
        
        def margin_matches(margin, target_range):
            """Verifica si el margen cae en el rango objetivo"""
            if not target_range: return True
            if target_range == ">2": return margin > 2
            elif target_range == "2": return margin == 2
            elif target_range == "1": return margin == 1
            elif target_range == "0": return margin == 0
            elif target_range == "<0": return margin < 0
            return True

        def normalize_name_l(n): return n.lower().strip() if n else ""
        
        print(f"[FAVORITE PERSPECTIVE] Validating: AH={current_ah}, FavWDL={target_fav_wdl}, FavAH={target_fav_ah_bucket}")

        for match in inputs:
            # 1. Determinar quién es favorito en el partido histórico
            odds = match.get('main_match_odds') or {}
            hist_ah_raw = odds.get('ah_linea')
            if hist_ah_raw in (None, '', 'N/A'):
                hist_ah_raw = match.get('handicap')

            try:
                hist_ah = float(hist_ah_raw)
            except (TypeError, ValueError):
                continue
            
            # AH > 0 -> Home favorito, AH < 0 -> Away favorito, AH == 0 -> Neutro
            if hist_ah > 0.01:
                hist_fav = 'home'
                hist_underdog = 'away'
            elif hist_ah < -0.01:
                hist_fav = 'away'
                hist_underdog = 'home'
            else:
                # Handicap 0: buscar por otros criterios (saltamos por ahora)
                continue
            
            # 2. Extraer datos del favorito histórico
            cur_h = match.get('home_name')
            cur_a = match.get('away_name')
            
            if hist_fav == 'home':
                fav_name = cur_h
                fav_prev = match.get('last_home_match', {})
            else:
                fav_name = cur_a
                fav_prev = match.get('last_away_match', {})
            
            # WDL y margen del favorito en SU partido previo
            fav_wdl, fav_margin = get_wdl_and_margin(
                fav_prev.get('score'),
                fav_name,
                fav_prev.get('home_team'),
                fav_prev.get('away_team')
            )
            
            # AH que tuvo el favorito en su partido previo
            fav_prev_ah_raw = fav_prev.get('handicap_line_raw')
            fav_prev_ah_bucket = bucket_ah(fav_prev_ah_raw)
            
            # 3. Filtrar por criterios del favorito
            if target_fav_wdl and fav_wdl != target_fav_wdl: continue
            if target_fav_ah_bucket is not None and fav_prev_ah_bucket != target_fav_ah_bucket: continue
            if target_fav_margin_range and not margin_matches(fav_margin, target_fav_margin_range): continue
            
            # 4. Resultado del partido histórico (desde perspectiva del favorito)
            match_id = str(match.get('match_id') or '')
            if not match_id:
                continue

            res_str = match.get('score') or match.get('final_score')
            if not res_str: continue
            
            final_s = parse_score_l(res_str)
            if not final_s: continue
            
            # Match encontrado
            matched_matches.append({
                'match_id': match_id,
                'home': cur_h,
                'away': cur_a,
                'favorite': fav_name,
                'score': res_str,
                'ah': hist_ah,
                'date': match.get('date') or match.get('match_date') or 'N/A'
            })
            
            stats['total'] += 1
            
            # Calcular resultado desde perspectiva del FAVORITO
            if hist_fav == 'home':
                if final_s[0] > final_s[1]: stats['fav_wins'] += 1
                elif final_s[1] > final_s[0]: stats['underdog_wins'] += 1
                else: stats['voids'] += 1
            else:  # hist_fav == 'away'
                if final_s[1] > final_s[0]: stats['fav_wins'] += 1
                elif final_s[0] > final_s[1]: stats['underdog_wins'] += 1
                else: stats['voids'] += 1
                 
        # Calcular porcentajes
        eff = stats['total'] - stats['voids']
        fav_pct = (stats['fav_wins'] / eff * 100) if eff > 0 else 0
        underdog_pct = (stats['underdog_wins'] / eff * 100) if eff > 0 else 0
        
        return jsonify({
            'status': 'success',
            'matches': matched_matches[:50],
            'stats': {
                'total': stats['total'],
                'effective': eff,
                'fav_win_pct': fav_pct,
                'underdog_win_pct': underdog_pct
            }
        })

    except Exception as e:
        print(f"Error in validate_pattern: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500

# ==========================================
#  PATTERN EXPLORER - HANDICAP-NORMALIZED SEARCH
# ==========================================
@app.route('/api/precacheo_pattern_search', methods=['POST'])
def api_precacheo_pattern_search():
    """
    Busca patrones similares con lógica NORMALIZADA POR HANDICAP.
    
    REGLA CLAVE: Solo muestra partidos previos donde el equipo tuvo el MISMO ROL:
    - Si hoy es favorito (AH negativo) → Buscar partidos donde fue favorito
    - Si hoy es underdog (AH positivo) → Buscar partidos donde fue underdog
    
    Calcula MEJORA en H2H Col3 comparando AH histórico vs actual.
    """
    try:
        data = request.json
        match_id = data.get('match_id')
        
        if not match_id:
            return jsonify({'error': 'match_id is required'}), 400
        
        # Aceptar datos del partido desde el frontend
        target_match = data.get('match_data')

        # Fuente SQL principal para partidos próximos
        snapshot = load_data_from_file()
        matches = snapshot.get('upcoming_matches', []) if isinstance(snapshot, dict) else []

        # Fallback: precacheo SQL si no hay snapshot reciente
        if not matches:
            matches = data_manager.load_precacheo_matches()

        # Si no se envían datos completos, buscar en las listas SQL
        if not target_match:
            target_match = next((m for m in matches if str(m.get('id')) == str(match_id) or str(m.get('match_id')) == str(match_id)), None)
            
        if not target_match:
            return jsonify({'error': 'Match not found'}), 404
        
        # Info del partido actual - soportar ambas estructuras de datos
        current_home = target_match.get('home_name') or target_match.get('home_team')
        current_away = target_match.get('away_name') or target_match.get('away_team')
        
        # AH puede estar en diferentes ubicaciones
        current_ah = 0
        if target_match.get('main_match_odds'):
            current_ah = float(target_match['main_match_odds'].get('ah_linea', 0))
        elif target_match.get('ah_line'):
            current_ah = float(target_match['ah_line'])
        elif target_match.get('handicap'):
            current_ah = float(target_match['handicap'])
        
        # Determinar roles
        is_home_favorite = current_ah > 0.01
        is_away_favorite = current_ah < -0.01
        
        # HELPERS
        def get_ah_bucket(ah):
            """Convierte AH a bucket normalizado"""
            if ah is None: return 0
            av = abs(float(ah))
            sign = -1 if float(ah) < 0 else 1
            
            if av < 0.01: return 0
            elif 0.24 <= av <= 0.76: return 0.5 * sign
            elif 0.99 <= av <= 1.01: return 1.0 * sign
            elif 1.24 <= av <= 1.76: return 1.5 * sign
            elif av >= 1.99: return 2.0 * sign
            return round(av * 2) / 2 * sign
        
        def filter_by_role(team_name, matches_list, should_be_favorite, current_bucket):
            """
            Filtra partidos donde el equipo tuvo el MISMO ROL.
            should_be_favorite: True si buscamos partidos donde fue favorito.
            current_bucket: Bucket del AH actual para buscar rango similar.
            """
            filtered = []
            for m in matches_list:
                # Determinar AH del equipo en ese partido
                h_name = m.get('home_team') or m.get('home_name')
                a_name = m.get('away_team') or m.get('away_name')
                
                if not h_name or not a_name:
                    continue
                
                team_ah = None
                if team_name.lower() == h_name.lower():
                    # Equipo jugó de local, su AH es el que está en el partido
                    odds = m.get('odds', {}) or m.get('main_match_odds', {})
                    raw_ah = odds.get('ah_linea') or odds.get('handicap_line')
                    if raw_ah: team_ah = float(raw_ah)
                elif team_name.lower() == a_name.lower():
                    # Equipo jugó de visitante, su AH es el inverso
                    odds = m.get('odds', {}) or m.get('main_match_odds', {})
                    raw_ah = odds.get('ah_linea') or odds.get('handicap_line')
                    if raw_ah: team_ah = -float(raw_ah)
                
                if team_ah is None:
                    continue
                
                # Verificar si tuvo el mismo rol
                was_favorite = team_ah < -0.01
                
                if was_favorite != should_be_favorite:
                    continue
                
                # Verificar que el bucket esté en rango similar (±0.5)
                team_bucket = get_ah_bucket(team_ah)
                if abs(abs(team_bucket) - abs(current_bucket)) > 0.5:
                    continue
                
                filtered.append(m)
            
            return filtered
        
        # Buscar partidos con MISMO ROL
        home_bucket = get_ah_bucket(current_ah if is_home_favorite else -current_ah)
        away_bucket = get_ah_bucket(-current_ah if is_away_favorite else current_ah)
        
        prev_home_matches = filter_by_role(current_home, matches, is_home_favorite, home_bucket)
        prev_away_matches = filter_by_role(current_away, matches, is_away_favorite, away_bucket)
        
        # Tomar el más reciente
        prev_home = prev_home_matches[0] if prev_home_matches else None
        prev_away = prev_away_matches[0] if prev_away_matches else None
        
        # Preparar respuesta
        match_info = {
            'home': current_home,
            'away': current_away,
            'ah_actual': current_ah,
            'favorito': current_home if is_home_favorite else (current_away if is_away_favorite else 'Neutro')
        }
        
        # ===== BÚSQUEDA DE PATRONES SIMILARES =====
        # Buscar todos los partidos con AH similar al actual
        similar_matches = []
        target_bucket = get_ah_bucket(current_ah)
        
        for m in matches:
            if m.get('match_id') == match_id:
                continue  # Skip current match
            
            m_ah = m.get('main_match_odds', {}).get('ah_linea')
            if not m_ah:
                continue
            
            m_bucket = get_ah_bucket(float(m_ah))
            
            # Solo incluir partidos con AH en mismo bucket (±0.5)
            if abs(abs(m_bucket) - abs(target_bucket)) > 0.5:
                continue
            
            similar_matches.append(m)
        
        # ===== FORMATEAR RESULTADOS CON TODOS LOS DATOS =====
        def format_match_data(match):
            """Formatea un partido con TODOS los datos necesarios"""
            m_home = match.get('home_name') or match.get('home_team')
            m_away = match.get('away_name') or match.get('away_team')
            m_odds = match.get('main_match_odds', {})
            m_ah = m_odds.get('ah_linea', 0)
            m_score = match.get('score') or match.get('final_score')
            m_date = match.get('date', '')
            
            # Calcular cobertura
            covered = 'N/A'
            if m_score:
                try:
                    parts = m_score.replace('-', ':').split(':')
                    if len(parts) == 2:
                        h_goals = int(parts[0])
                        a_goals = int(parts[1])
                        diff = h_goals - a_goals + float(m_ah)
                        if diff > 0.01:
                            covered = 'COVER'
                        elif diff < -0.01:
                            covered = 'NO_COVER'
                        else:
                            covered = 'PUSH'
                except:
                    pass
            
            # Prev Home - con filtro por ROL
            prev_h_data = match.get('last_home_match') or {}
            # Asegurar que es un dict
            if not isinstance(prev_h_data, dict):
                prev_h_data = {}
            
            m_is_home_fav = float(m_ah) > 0.01
            
            # Prev Away - con filtro por ROL  
            prev_a_data = match.get('last_away_match') or {}
            # Asegurar que es un dict
            if not isinstance(prev_a_data, dict):
                prev_a_data = {}
            
            m_is_away_fav = float(m_ah) < -0.01
            
            # H2H Col3 con MEJORA
            h2h_col3 = match.get('h2h_col3') or {}
            if not isinstance(h2h_col3, dict):
                h2h_col3 = {}
            
            h2h_mejora = 'N/A'
            
            if h2h_col3.get('status') == 'found':
                h2h_ah_raw = h2h_col3.get('ah')
                h2h_score = h2h_col3.get('score', '')
                h2h_covered = False
                
                # Calcular si cubrió en H2H
                if h2h_ah_raw and h2h_score:
                    try:
                        parts = h2h_score.replace('-', ':').split(':')
                        if len(parts) == 2:
                            h_g = int(parts[0])
                            a_g = int(parts[1])
                            diff = h_g - a_g + float(h2h_ah_raw)
                            h2h_covered = diff > 0.01
                    except:
                        pass
                
                # Calcular MEJORA
                if h2h_ah_raw:
                    h2h_bucket = get_ah_bucket(float(h2h_ah_raw))
                    current_bucket = get_ah_bucket(float(m_ah))
                    
                    if abs(h2h_bucket) > abs(current_bucket) and h2h_covered:
                        h2h_mejora = 'MEJORA'
                    elif abs(h2h_bucket) == abs(current_bucket) and h2h_covered:
                        h2h_mejora = 'IGUALA'
                    elif abs(h2h_bucket) == abs(current_bucket) and not h2h_covered:
                        h2h_mejora = 'IGUALA'
                    else:
                        h2h_mejora = 'EMPEORA'
                
                h2h_col3['cover_status'] = h2h_mejora
            
            # Indirectas
            ind_local = match.get('ind_local') or {}
            if not isinstance(ind_local, dict):
                ind_local = {}
                
            ind_visitante = match.get('ind_visitante') or {}
            if not isinstance(ind_visitante, dict):
                ind_visitante = {}
            
            
            return {
                'match_id': match.get('match_id'),
                'date': m_date,
                'home': m_home,
                'away': m_away,
                'ah': m_ah,
                'score': m_score,
                'covered': covered,
                'prev_home': {
                    'score': prev_h_data.get('score'),
                    'ah': prev_h_data.get('handicap_line_raw') or prev_h_data.get('ah'),
                    'home_team': prev_h_data.get('home_team'),
                    'away_team': prev_h_data.get('away_team'),
                    'date': prev_h_data.get('date'),
                    'stats': prev_h_data.get('stats_rows', [])
                },
                'prev_away': {
                    'score': prev_a_data.get('score'),
                    'ah': prev_a_data.get('handicap_line_raw') or prev_a_data.get('ah'),
                    'home_team': prev_a_data.get('home_team'),
                    'away_team': prev_a_data.get('away_team'),
                    'date': prev_a_data.get('date'),
                    'stats': prev_a_data.get('stats_rows', [])
                },
                'h2h_stadium': match.get('h2h_stadium', {}),
                'h2h_general': match.get('h2h_general', {}),
                'h2h_col3': h2h_col3,
                'ind_local': ind_local,
                'ind_visitante': ind_visitante
            }
        
        results = [format_match_data(m) for m in similar_matches[:100]]  # Limitar a 100
        
        # Formatear prev_home y prev_away del partido actual
        formatted_prev_home = None
        formatted_prev_away = None
        
        if prev_home:
            prev_h_data = prev_home.get('last_home_match') if prev_home.get('home_name') == current_home else prev_home.get('last_away_match')
            if not prev_h_data or not isinstance(prev_h_data, dict):
                prev_h_data = {}
            
            formatted_prev_home = {
                'score': prev_h_data.get('score'),
                'ah': prev_h_data.get('handicap_line_raw') or prev_h_data.get('ah'),
                'home_team': prev_h_data.get('home_team'),
                'away_team': prev_h_data.get('away_team'),
                'date': prev_h_data.get('date')
            }
        
        if prev_away:
            prev_a_data = prev_away.get('last_away_match') if prev_away.get('away_name') == current_away else prev_away.get('last_home_match')
            if not prev_a_data or not isinstance(prev_a_data, dict):
                prev_a_data = {}
            
            formatted_prev_away = {
                'score': prev_a_data.get('score'),
                'ah': prev_a_data.get('handicap_line_raw') or prev_a_data.get('ah'),
                'home_team': prev_a_data.get('home_team'),
                'away_team': prev_a_data.get('away_team'),
                'date': prev_a_data.get('date')
            }
        
        return jsonify({
            'status': 'success',
            'match_info': match_info,
            'results': results,
            'prev_home': formatted_prev_home,
            'prev_away': formatted_prev_away
        })
    
    except Exception as e:
        print(f"Error in precacheo_pattern_search: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/analyze_scah', methods=['POST'])
def api_analyze_scah():
    """
    Endpoint para analizar un partido con el modelo SCAH v10.0
    """
    try:
        data = request.json
        match_id = data.get('match_id')
        if not match_id:
            return jsonify({'error': 'Falta match_id'}), 400

        # Obtener datos completos del partido (usando caché si es posible)
        match_data = analizar_partido_completo(str(match_id), force_refresh=False)
        
        if not match_data or 'error' in match_data:
            return jsonify({'error': 'No se pudieron obtener datos completos para el análisis SCAH.'}), 500

        resultado = analizar_partido_scah(match_data)
        
        return jsonify({
            'status': 'success',
            'match_id': match_id,
            'report': resultado
        })
    except Exception as e:
        print(f"Error en analyze_scah: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/ganadores')
def route_ganadores():
    return render_template('ganadores.html')


@app.route('/api/underdog_stats')
def api_underdog_stats():
    try:
        stats = winner_tracker.get_underdog_bust_stats()
        return jsonify(stats)
    except Exception as e:
        print(f"Error en api_underdog_stats: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/underdog_matches')
def api_underdog_matches():
    try:
        pattern = request.args.get('pattern')
        show_stale = request.args.get('show_stale', 'false').lower() == 'true'
        
        all_matches = winner_tracker.scan_all_historical_buckets()
        
        filtered = []
        for m in all_matches:
            if m['is_stale'] and not show_stale:
                continue
                
            if pattern:
                if pattern not in m['label']:
                    continue
            filtered.append(m)
            
        return jsonify(filtered)
    except Exception as e:
        print(f"Error en api_underdog_matches: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/underdog_patterns')
def api_underdog_patterns():
    try:
        min_sample = int(request.args.get('min_sample', 10))
        min_bust_rate = float(request.args.get('min_rate', 0.6))
        
        stats = winner_tracker.get_underdog_bust_stats()
        patterns = stats.get('patterns', [])
        
        filtered = [
            p for p in patterns 
            if p['total'] >= min_sample and p['bust_rate'] >= min_bust_rate
        ]
        
        return jsonify(filtered)
    except Exception as e:
        print(f"Error en api_underdog_patterns: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/freshness_report')
def api_freshness_report():
    try:
        all_matches = winner_tracker.scan_all_historical_buckets()
        report = {
            'FRESH_OK': sum(1 for m in all_matches if m['fresh_code'] == 'FRESH_OK'),
            'FRESH_WARN': sum(1 for m in all_matches if m['fresh_code'] == 'FRESH_WARN'),
            'FRESH_STALE': sum(1 for m in all_matches if m['fresh_code'] == 'FRESH_STALE'),
            'FRESH_MISSING': sum(1 for m in all_matches if m['fresh_code'] == 'FRESH_MISSING'),
        }
        return jsonify(report)
    except Exception as e:
        print(f"Error en api_freshness_report: {e}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    # use_reloader=False para evitar que werkzeug detecte cambios en site-packages (plotly, etc.)
    # Threaded para mejor respuesta en desarrollo
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True, use_reloader=False)
