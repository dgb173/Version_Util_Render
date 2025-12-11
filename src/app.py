# app.py - Servidor web principal (Flask)
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from flask import Flask, render_template, abort, request, redirect, url_for
import asyncio

from bs4 import BeautifulSoup
import datetime
import re
import math
import threading
import json
import time
import logging
from pathlib import Path
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import csv
import os
import concurrent.futures

_json_save_lock = threading.Lock()

from modules import league_scraper
from modules import history_manager

# ¡Importante! Importa tu nuevo módulo de scraping
from modules.estudio_scraper import (
    analizar_partido_completo, 
    format_ah_as_decimal_string_of,
    parse_ah_to_number_of,
    check_handicap_cover,
    generar_analisis_completo_mercado
)

from modules.pattern_search import find_similar_patterns, explore_matches
from flask import jsonify # Asegúrate de que jsonify está importado

app = Flask(__name__)

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

def save_match_to_json(match_data):
    """Guarda los datos del partido usando el nuevo sistema de buckets."""
    try:
        match_data['cached_at'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        saved = data_manager.save_match(match_data)
        if saved:
            print(f"Partido {match_data.get('match_id')} guardado en bucket.")
        else:
            print(f"Partido {match_data.get('match_id')} ignorado (filtro).")
    except Exception as e:
        print(f"Error guardando en JSON: {e}")

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
_DATA_FILE_CANDIDATES = [
    Path(__file__).resolve().parent / 'data.json',
    Path(__file__).resolve().parent.parent / 'data.json',
]
for _candidate in _DATA_FILE_CANDIDATES:
    if _candidate.exists():
        DATA_FILE = _candidate
        break
else:
    DATA_FILE = _DATA_FILE_CANDIDATES[0]

_data_file_lock = threading.Lock()


def load_data_from_file():
    """Carga los datos desde el archivo JSON, similar a la app ligera."""
    with _data_file_lock:
        if not DATA_FILE.exists():
            return {key: [] for key in _EMPTY_DATA_TEMPLATE}
        try:
            with DATA_FILE.open('r', encoding='utf-8') as fh:
                data = json.load(fh)
        except (json.JSONDecodeError, OSError) as exc:
            print(f"Error al leer {DATA_FILE}: {exc}")
            return {key: [] for key in _EMPTY_DATA_TEMPLATE}
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


def _parse_time_obj(value):
    if isinstance(value, datetime.datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.datetime.fromisoformat(value)
        except ValueError:
            try:
                return datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                return None
    return None


def _ensure_time_string(entry, parsed_time):
    if entry.get('time') or not parsed_time:
        return
    entry['time'] = parsed_time.strftime('%d/%m %H:%M')


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
    for original in matches:
        entry = dict(original)
        parsed_time = _parse_time_obj(entry.get('time_obj'))
        entry['_sort_time'] = parsed_time or datetime.datetime.min
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

    prepared.sort(key=lambda item: (item['_sort_time'], item.get('id', '')), reverse=sort_desc)

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
        if '_sort_time' in entry:
             spain_time = entry['_sort_time'] + datetime.timedelta(hours=1) # Matching the +1 logic from parsing
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


def load_preview_from_cache(match_id: str):
    cache_dir = _get_preview_cache_dir()
    cache_path = cache_dir / f'{match_id}.json'
    if cache_path.exists():
        try:
            with cache_path.open('r', encoding='utf-8') as fh:
                cached_data = json.load(fh)
                if isinstance(cached_data, dict):
                    return cached_data
        except (json.JSONDecodeError, OSError) as exc:
            print(f"Error al leer cache de analisis {cache_path}: {exc}")
    return None


def save_preview_to_cache(match_id: str, payload: dict):
    cache_dir = _get_preview_cache_dir()
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_path = cache_dir / f'{match_id}.json'
        with cache_path.open('w', encoding='utf-8') as fh:
            json.dump(payload, fh, ensure_ascii=False)
    except OSError as exc:
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
            response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
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
    match_rows = soup.find_all('tr', id=lambda x: x and x.startswith('tr1_'))
    upcoming_matches = []
    now_utc = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)

    for row in match_rows:
        match_id = row.get('id', '').replace('tr1_', '')
        if not match_id: continue

        time_cell = row.find('td', {'name': 'timeData'})
        if not time_cell or not time_cell.has_attr('data-t'): continue
        
        try:
            match_time = datetime.datetime.strptime(time_cell['data-t'], '%Y-%m-%d %H:%M:%S')
        except (ValueError, IndexError):
            continue

        if match_time < now_utc: continue

        home_team_tag = row.find('a', {'id': f'team1_{match_id}'})
        away_team_tag = row.find('a', {'id': f'team2_{match_id}'})
        odds_data = row.get('odds', '').split(',')
        handicap = odds_data[2] if len(odds_data) > 2 else "N/A"
        goal_line = odds_data[10] if len(odds_data) > 10 else "N/A"

        if handicap == "N/A":
            continue


        upcoming_matches.append({
            "id": match_id,
            "time_obj": match_time,
            "home_team": home_team_tag.text.strip() if home_team_tag else "N/A",
            "away_team": away_team_tag.text.strip() if away_team_tag else "N/A",
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
        # Spanish time: UTC+1 in winter, adjust as needed
        # We need to preserve the full datetime for frontend filtering
        # _filter_and_slice_matches ensures '_sort_time' is available but we popped it? No, wait.
        # Here we are processing 'paginated_matches' which are just dicts.
        
        # We already have 'match_time' available as 'time_obj' or we can reconstruct it?
        # In this function 'parse_main_page_matches', we created 'time_obj'.
        # We should iterate passing 'time_obj' to string BEFORE deleting it if we want custom format,
        # OR just keep 'time_obj' until the end.
        
        # Let's add 'start_time' ISO string
        if 'time_obj' in match:
            # We want Spain time for display, but maybe ISO for filtering?
            # Let's keep consistent with existing logic: existing logic adds 1 hour.
            spain_time = match['time_obj'] + datetime.timedelta(hours=1)
            match['time'] = spain_time.strftime('%H:%M')
            match['start_time'] = spain_time.isoformat() # Full ISO for filtering
            
            # If date is not today, we might want to include date in 'time' field? 
            # The user asked for "Fecha" column to show date if not today.
            # Frontend will handle display, we just provide data.
            
            del match['time_obj']
        elif '_sort_time' in match:
             # Fallback if coming from _filter_and_slice_matches logic where time_obj might be absent
             spain_time = match['_sort_time'] + datetime.timedelta(hours=1)
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

        if not re.match(r'^\d+\s*-\s*\d+$', score_text):
            continue

        odds_data = row.get('odds', '').split(',')
        handicap = odds_data[2] if len(odds_data) > 2 else "N/A"
        goal_line = odds_data[10] if len(odds_data) > 10 else "N/A"

        if handicap == "N/A":
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
            "score": score_text,
            "handicap": handicap,
            "goal_line": goal_line
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
        spain_time = match['time_obj'] + datetime.timedelta(hours=2)
        match['time'] = spain_time.strftime('%d/%m %H:%M')
        match['start_time'] = spain_time.isoformat()
        del match['time_obj']

    return paginated_matches

async def get_main_page_matches_async(limit=None, offset=0, handicap_filter=None, goal_line_filter=None, min_time=None):
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
    return _filter_and_slice_matches(
        'finished_matches',
        limit=limit,
        offset=offset,
        handicap_filter=handicap_filter,
        goal_line_filter=goal_line_filter,
        sort_desc=True,
    )


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


@app.route('/api/matches')
def api_matches():
    try:
        offset = int(request.args.get('offset', 0))
        limit = int(request.args.get('limit', 5))
        limit = min(limit, 1000)
        matches = asyncio.run(get_main_page_matches_async(limit, offset, request.args.get('handicap'), request.args.get('ou')))
        return jsonify({'matches': matches})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/finished_matches')
def api_finished_matches():
    try:
        offset = int(request.args.get('offset', 0))
        limit = int(request.args.get('limit', 5))
        limit = min(limit, 1000)
        matches = asyncio.run(get_main_page_finished_matches_async(limit, offset, request.args.get('handicap'), request.args.get('ou')))
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

# --- CACHE STATE PERSISTENCE ---
CACHE_STATE_FILE = Path(__file__).resolve().parent / 'cache_state.json'
_cache_state_lock = threading.Lock()

def load_cache_state():
    with _cache_state_lock:
        if CACHE_STATE_FILE.exists():
            try:
                with open(CACHE_STATE_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
    return {'processed_ids': []}

def save_cache_state(state):
    with _cache_state_lock:
        try:
            with open(CACHE_STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(state, f, ensure_ascii=False)
        except Exception as e:
            print(f"Error saving cache state: {e}")

def add_processed_id(match_id):
    state = load_cache_state()
    if str(match_id) not in state['processed_ids']:
        state['processed_ids'].append(str(match_id))
        save_cache_state(state)


# --- PRE-CACHE STATE PERSISTENCE ---
PRECACHE_STATE_FILE = Path(__file__).resolve().parent / 'precache_state.json'
_precache_state_lock = threading.Lock()

def load_precache_state():
    with _precache_state_lock:
        if PRECACHE_STATE_FILE.exists():
            try:
                with open(PRECACHE_STATE_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
    return {'processed_ids': []}

def save_precache_state(state):
    with _precache_state_lock:
        try:
            with open(PRECACHE_STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(state, f, ensure_ascii=False)
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
            save_match_to_json(match_data)
            add_processed_id(match_id)
            return True, match_id
        else:
            return False, match_id
            
    except Exception as e:
        print(f"Error processing {match_id}: {e}")
        return False, match_id

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

def process_upcoming_matches_background(handicap_filter=None, goal_line_filter=None, workers=5):
    """
    Procesa partidos PRÓXIMOS (Pre-Cacheo) en segundo plano con optimizaciones:
    - Filtros
    - Concurrencia
    - Persistencia (Setpoint)
    """
    filter_desc = f"AH={handicap_filter}, OU={goal_line_filter}"
    print(f"Iniciando PRE-CACHEO Background ({filter_desc})...")
    
    try:
        # 1. Obtener partidos próximos (limit alto)
        # Filtrar para que solo sean partidos que empiezan de AHORA en adelante
        # (o quizás un margen de 15 mins atrás por si acaba de empezar y quieres apurar)
        # Por petición de usuario: "no los que ya han finalizado".
        # Usamos now() como referencia.
        matches = asyncio.run(get_main_page_matches_async(
            limit=2000, 
            offset=0, 
            handicap_filter=handicap_filter, 
            goal_line_filter=goal_line_filter,
            min_time=datetime.datetime.now()
        ))
        
        print(f"Se encontraron {len(matches)} partidos próximos candidatos.")
        
        # 2. Cargar estado
        state = load_precache_state()
        processed_ids = set(state.get('processed_ids', []))
        
        # 3. Filtrar los que ya están hechos (ya sea en estado o en data_manager)
        # Checkeo rápido contra el archivo (state) es más eficiente que cargar todo data_manager
        # Pero para ser robustos, si data_manager ya lo tiene, también saltar.
        # Por simplicidad y velocidad, confiamos en state + check interno de worker si fuese critico.
        
        to_process = []
        for m in matches:
            mid = str(m.get('id') or m.get('match_id'))
            
            # FIX: Verificar si REALMENTE tenemos datos, no solo si el state dice que sí.
            # Esto corrige el problema donde state=processed pero data=missing (por error de paths).
            exists_in_data = False
            # Opcional: Podríamos confiar solo en data_manager y obviar processed_ids para precacheo
            # Pero para ser eficientes, si no está en processed_ids, seguro es nuevo.
            # Si ESTÁ en processed_ids, verificamos si existe de verdad.
            
            is_processed = mid in processed_ids
            if is_processed:
                 # Doble check: ¿está en el archivo de precacheo?
                 cached_match = data_manager.get_precacheo_match(mid)
                 if cached_match:
                     exists_in_data = True
            
            if mid and not exists_in_data:
                to_process.append(mid)
                
        print(f"De los cuales {len(to_process)} son nuevos y se scrapearán.")
        
        if not to_process:
            print("Nada nuevo que scrapear en Pre-Cacheo.")
            return

        # 4. Procesar en paralelo
        max_workers = workers if workers else 5 
        total = len(to_process)
        completed = 0
        
        print(f"Iniciando Pool Pre-Cacheo con {max_workers} workers...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_single_precache_worker, mid): mid for mid in to_process}
            
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
    - Empezaron hace +2 horas
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
        two_hours_ago = now_spain - datetime.timedelta(hours=2)
        
        print(f"Hora España: {now_spain.strftime('%H:%M')}, buscando partidos que empezaron antes de {two_hours_ago.strftime('%H:%M')}")
        
        # 3. Filtrar: partidos sin resultado que empezaron hace +2 horas
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
                    
                # Crear datetime del partido en zona España
                match_dt = spain_tz.localize(datetime.datetime.strptime(
                    f"{match_date_str} {match_time_str}", 
                    "%Y-%m-%d %H:%M"
                ))
                
                # Solo si empezó hace +2 horas
                if match_dt <= two_hours_ago:
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


def process_all_finished_matches_background(handicap_filter=None, goal_line_filter=None):
    """
    Procesa partidos finalizados en segundo plano con optimizaciones:
    - Filtros
    - Concurrencia
    - Persistencia (Setpoint)
    """
    filter_desc = f"AH={handicap_filter}, OU={goal_line_filter}"
    print(f"Iniciando proceso de cacheo OPTIMIZADO ({filter_desc})...")
    
    try:
        # 1. Obtener partidos (usando filtros si existen)
        # Traemos MUCHOS para filtrar luego si es necesario, o confiamos en el endpoint
        matches = asyncio.run(get_main_page_finished_matches_async(
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

        # 4. Procesar en paralelo
        max_workers = 5 # Ajustable
        total = len(to_process)
        completed = 0
        
        print(f"Iniciando Pool con {max_workers} workers...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_single_match_worker, mid): mid for mid in to_process}
            
            for future in concurrent.futures.as_completed(futures):
                # STOP CHECK
                if STOP_CACHE_EVENT.is_set():
                    print("Señal de parada recibida. Cancelando tareas restantes...")
                    executor.shutdown(wait=False, cancel_futures=True)
                    break

                mid = futures[future]
                try:
                    success, _ = future.result()
                    completed += 1
                    if success:
                        pass # Log extra if needed
                    
                    # Log de progreso simple
                    if completed % 5 == 0 or completed == total:
                        print(f"Progreso: {completed}/{total} procesados.")
                        
                except Exception as e:
                    print(f"Excepción en worker {mid}: {e}")
                    
        if STOP_CACHE_EVENT.is_set():
            print(f"Proceso detenido. {completed} partidos completados antes de parar.")
        else:
            print(f"Proceso de cacheo finalizado. {completed} partidos intentados.")
        
    except Exception as e:
        print(f"Error fatal en proceso de background: {e}")

        return jsonify({'error': str(e)}), 500


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
        
        # Resetear señal de parada
        STOP_CACHE_EVENT.clear()

        # Iniciar hilo en segundo plano
        thread = threading.Thread(
            target=process_all_finished_matches_background,
            args=(handicap_filter, goal_line_filter)
        )
        thread.daemon = True 
        thread.start()
        
        return jsonify({
            'status': 'success', 
            'message': f'Cacheo iniciado (Filtros: AH={handicap_filter}, OU={goal_line_filter}).'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500



    matches = search_history_by_handicap(handicap)
    return jsonify({'matches': matches})

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
            
        results = find_similar_patterns(upcoming_match, history_data, config={'filter_mode': filter_mode})
        
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

@app.route('/api/explorer_search', methods=['POST'])
def api_explorer_search():
    try:
        data = request.json
        filters = data.get('filters', {})
        print(f"DEBUG: Explorer Search Request. Filters: {filters}")
        
        # Load data using data_manager
        # If handicap filter is present, load only that bucket
        ah_filter = filters.get('handicap')
        
        history_data = data_manager.load_matches_by_bucket(ah_filter)
            
        if not history_data:
             return jsonify({'results': [], 'message': 'No hay histórico disponible.'})
            
        results = explore_matches(history_data, filters=filters)
        
        return jsonify({'results': results})
    except Exception as e:
        print(f"Error en explorer search: {e}")
        return jsonify({'error': str(e)}), 500

# --- PRE-CACHEO ROUTES ---
@app.route('/precacheo')
def precacheo():
    """Muestra la vista de Pre-Cacheo para partidos próximos."""
    return render_template('precacheo.html')

@app.route('/api/precacheo_list')
def api_precacheo_list():
    """Lista todos los partidos pre-cacheados."""
    try:
        matches = data_manager.load_precacheo_matches()
        return jsonify({'matches': matches})
    except Exception as e:
        print(f"Error loading precacheo: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/precacheo_scrape', methods=['POST'])
def api_precacheo_scrape():
    """Scrapea un partido y lo guarda en precacheo."""
    try:
        data = request.json
        match_id = data.get('match_id')
        
        if not match_id:
            return jsonify({'error': 'Falta match_id'}), 400
        
        # Scrape the match
        match_data = analizar_partido_completo(str(match_id))
        
        if not match_data or match_data.get('error'):
            return jsonify({'error': match_data.get('error', 'No se pudo scrapear')}), 500
        
        match_data['match_id'] = str(match_id)
        match_data['precacheo_date'] = datetime.datetime.now().isoformat()
        
        # Save to precacheo
        data_manager.save_precacheo_match(match_data)
        
        return jsonify({'status': 'success', 'match': {
            'match_id': match_id,
            'home_name': match_data.get('home_name'),
            'away_name': match_data.get('away_name'),
            'handicap': match_data.get('main_match_odds', {}).get('ah_linea'),
            'score': match_data.get('score')
        }})
    except Exception as e:
        print(f"Error scraping for precacheo: {e}")
        return jsonify({'error': str(e)}), 500

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
    """Endpoint para scrapear solo los resultados de partidos pendientes (+2h sin score)."""
    try:
        # Iniciar hilo
        thread = threading.Thread(target=scrape_pending_results_background)
        thread.daemon = True 
        thread.start()
        
        return jsonify({
            'status': 'success', 
            'message': 'Buscando resultados de partidos pendientes (partidos +2h sin score)...'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/precacheo_pattern_search', methods=['POST'])
def api_precacheo_pattern_search():
    """
    Busca patrones similares en el histórico para un partido de Pre-Cacheo.
    Criterios: AH actual = mismo bucket, AH del partido previo del favorito = mismo bucket
    """
    try:
        data = request.json
        match_id = data.get('match_id')
        
        if not match_id:
            return jsonify({'error': 'Falta match_id'}), 400
        
        # 1. Cargar datos del partido desde precacheo
        precacheo_match = data_manager.get_precacheo_match(str(match_id))
        
        if not precacheo_match:
            return jsonify({'error': 'Partido no encontrado en precacheo'}), 404
        
        # 2. Extraer AH actual
        main_odds = precacheo_match.get('main_match_odds', {})
        ah_actual_raw = main_odds.get('ah_linea') or precacheo_match.get('handicap')
        
        if not ah_actual_raw:
            return jsonify({'error': 'No hay AH disponible'}), 400
        
        try:
            ah_actual = float(ah_actual_raw)
        except:
            return jsonify({'error': f'AH inválido: {ah_actual_raw}'}), 400
        
        # 3. Determinar favorito (AH > 0: Local, AH < 0: Visitante)
        is_home_favorite = ah_actual > 0
        
        # 4. Extraer AH del partido previo del favorito
        if is_home_favorite:
            prev_match = precacheo_match.get('last_home_match', {})
            fav_name = precacheo_match.get('home_name', 'Local')
        else:
            prev_match = precacheo_match.get('last_away_match', {})
            fav_name = precacheo_match.get('away_name', 'Visitante')
        
        prev_ah_raw = prev_match.get('handicap_line_raw') if prev_match else None
        prev_ah = None
        if prev_ah_raw:
            try:
                prev_ah = float(prev_ah_raw)
            except:
                prev_ah = None
        
        # 5. Detectar si el favorito cubrió en su partido previo
        # IMPORTANTE: Cover = si el favorito cubrió el handicap de SU partido previo
        prev_fav_covered = None  # None = no data, True = cubrió, False = no cubrió
        if prev_match:
            prev_score = prev_match.get('score') or prev_match.get('final_score')
            prev_ah_for_calc = prev_match.get('handicap_line_raw')
            # IMPORTANTE: El favorito jugó de LOCAL si is_home_favorite=True (last_home_match)
            # o de VISITANTE si is_home_favorite=False (last_away_match)
            prev_was_home = is_home_favorite
            
            # Usar la función asian_result para calcular correctamente
            if prev_score and prev_ah_for_calc:
                try:
                    from modules.pattern_search import asian_result
                    
                    # Parsear score
                    score_clean = prev_score.replace(' ', '').replace('-', ':')
                    parts = score_clean.split(':')
                    if len(parts) == 2:
                        hg, ag = int(parts[0]), int(parts[1])
                        ah_val = float(prev_ah_for_calc)
                        
                        # El favorito actual jugó en ese partido
                        if prev_was_home:
                            # Favorito jugó de LOCAL, AH que tenía era el line_raw
                            # Si AH era negativo, era el favorito, si positivo, era underdog
                            res = asian_result(hg, ag, ah_val)
                        else:
                            # Favorito jugó de VISITANTE
                            # Invertir goles y signo del AH
                            res = asian_result(ag, hg, -ah_val)
                        
                        cat = res.get('category', 'UNKNOWN')
                        if cat == 'COVER':
                            prev_fav_covered = True
                        elif cat == 'NO_COVER':
                            prev_fav_covered = False
                        # PUSH = None (no se cuenta)
                except Exception as e:
                    print(f"Error calculando prev_fav_covered: {e}")
                    pass
        
        # 5b. Calcular WDL del partido previo del favorito (WIN/DRAW/LOSS desde perspectiva del favorito)
        prev_fav_wdl = None  # 'WIN', 'DRAW', 'LOSS'
        if prev_match:
            prev_score = prev_match.get('score') or prev_match.get('final_score')
            # IMPORTANTE: Determinar si el favorito jugó de LOCAL o VISITANTE en su partido previo
            # - Si is_home_favorite = True, usamos last_home_match, donde el favorito jugó de LOCAL
            # - Si is_home_favorite = False, usamos last_away_match, donde el favorito jugó de VISITANTE
            prev_was_home = is_home_favorite  # Crucial: inverso de lo que teníamos antes
            
            if prev_score:
                try:
                    score_clean = prev_score.replace(' ', '').replace('-', ':')
                    parts = score_clean.split(':')
                    if len(parts) == 2:
                        hg, ag = int(parts[0]), int(parts[1])
                        
                        # Determinar resultado desde perspectiva del favorito
                        if prev_was_home:
                            # Favorito jugó de LOCAL
                            if hg > ag:
                                prev_fav_wdl = 'WIN'
                            elif hg < ag:
                                prev_fav_wdl = 'LOSS'
                            else:
                                prev_fav_wdl = 'DRAW'
                        else:
                            # Favorito jugó de VISITANTE
                            if ag > hg:
                                prev_fav_wdl = 'WIN'
                            elif ag < hg:
                                prev_fav_wdl = 'LOSS'
                            else:
                                prev_fav_wdl = 'DRAW'
                except Exception as e:
                    print(f"Error calculando prev_fav_wdl: {e}")
                    pass
        
        # 6. Cargar datos históricos
        from modules.pattern_search import explore_matches
        history_data = data_manager.load_matches_by_bucket(ah_actual)
        
        if not history_data:
            return jsonify({'results': [], 'message': 'No hay datos históricos'})
        
        # 7. Buscar con filtros
        filters = {'handicap': ah_actual, 'limit': 100}  # Más resultados para poder filtrar
        if prev_ah is not None:
            if is_home_favorite:
                filters['prev_home_ah'] = prev_ah
            else:
                filters['prev_away_ah'] = prev_ah
        
        all_results = explore_matches(history_data, filters=filters)
        
        # 7. Formatear con TODOS los datos (máximo 30)
        formatted_results = []
        for item in all_results[:30]:
            c = item.get('candidate', {})
            ev = item.get('evaluation', {})
            cover_status = ev.get('home') if is_home_favorite else ev.get('away')
            
            # Prev Home data - ahora con movement
            prev_home = item.get('prev_home', {}) or {}
            prev_home_data = None
            if prev_home.get('score'):
                prev_home_data = {
                    'ah': prev_home.get('ah'),
                    'score': prev_home.get('score'),
                    'wdl': prev_home.get('wdl'),
                    'rival': prev_home.get('rival'),
                    'movement': prev_home.get('movement')  # ej: "0.25 -> 0.5"
                }
            
            # Prev Away data - ahora con movement
            prev_away = item.get('prev_away', {}) or {}
            prev_away_data = None
            if prev_away.get('score'):
                prev_away_data = {
                    'ah': prev_away.get('ah'),
                    'score': prev_away.get('score'),
                    'wdl': prev_away.get('wdl'),
                    'rival': prev_away.get('rival'),
                    'movement': prev_away.get('movement')
                }
            
            # H2H Estadio data - tiene movement y score
            h2h_stadium = item.get('h2h_stadium', {}) or {}
            h2h_stadium_data = None
            if h2h_stadium.get('score') or h2h_stadium.get('movement'):
                h2h_stadium_data = {
                    'movement': h2h_stadium.get('movement'),  # ej: "0.5 -> 0.75"
                    'score': h2h_stadium.get('score'),
                    'wdl': h2h_stadium.get('wdl')
                }
            
            # H2H General data - tiene movement y score
            h2h_general = item.get('h2h_general', {}) or {}
            h2h_general_data = None
            if h2h_general.get('score') or h2h_general.get('movement'):
                h2h_general_data = {
                    'movement': h2h_general.get('movement'),
                    'score': h2h_general.get('score'),
                    'wdl': h2h_general.get('wdl')
                }
            
            # H2H Col3 data - tiene home_team/away_team
            h2h_col3 = item.get('h2h_col3', {}) or {}
            h2h_col3_data = None
            if h2h_col3.get('score'):
                h2h_col3_data = {
                    'score': h2h_col3.get('score'),
                    'ah': h2h_col3.get('ah'),
                    'home_team': h2h_col3.get('home_team'),
                    'away_team': h2h_col3.get('away_team')
                }
            
            # Ind. Local e Ind. Visitante
            ind_local = item.get('ind_local') or {}
            ind_visitante = item.get('ind_visitante') or {}
            
            formatted_results.append({
                'match_id': item.get('match_id') or c.get('match_id'),
                'date': c.get('date'),
                'home': c.get('home'),
                'away': c.get('away'),
                'score': c.get('score'),
                'ah': c.get('ah_real'),
                'ou': c.get('ou_line'),
                'covered': cover_status,
                'prev_home': prev_home_data,
                'prev_away': prev_away_data,
                'h2h_stadium': h2h_stadium_data,
                'h2h_general': h2h_general_data,
                'h2h_col3': h2h_col3_data,
                'ind_local': ind_local if isinstance(ind_local, dict) and (ind_local.get('score') or ind_local.get('ah')) else None,
                'ind_visitante': ind_visitante if isinstance(ind_visitante, dict) and (ind_visitante.get('score') or ind_visitante.get('ah')) else None
            })
        
        return jsonify({
            'status': 'success',
            'match_info': {
                'ah_actual': ah_actual, 
                'favorito': fav_name, 
                'prev_ah_favorito': prev_ah, 
                'is_home_fav': is_home_favorite,
                'prev_fav_covered': prev_fav_covered,  # True/False/None para auto-filtrar
                'prev_fav_wdl': prev_fav_wdl  # 'WIN'/'DRAW'/'LOSS'/None para filtrar por tipo de resultado
            },
            'results': formatted_results,
            'total_found': len(all_results)
        })
        
    except Exception as e:
        print(f"Error en pattern search: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


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
@app.route('/scraper')
def scraper_view():
    pending_matches = history_manager.get_pending_matches()
    return render_template('scraper.html', pending_matches=pending_matches)

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
        # Load directly from the specific bucket file if possible, or search all
        # To be safe and consistent with data_manager, we should use a method there.
        # For now, let's load manualy or generic.
        # Assuming data_manager has logic or we implement reading 'data_pending_results.json'
        
        pending_file =  Path(__file__).resolve().parent / 'data' / 'data_pending_results.json'
        matches = []
        if pending_file.exists():
            try:
                with open(pending_file, 'r', encoding='utf-8') as f:
                    matches = json.load(f)
            except:
                matches = []
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
        
        # If result found, we should remove from pending file explicitly if save_match doesn't do it automatically
        # data_manager.save_match ADDS to buckets. It might not remove from pending if it goes to another bucket.
        # We need to manually remove from pending if it moved to a numbered bucket.
        
        if result_found:
            # Load pending, remove this id, save
            pending_file = Path(__file__).resolve().parent / 'data' / 'data_pending_results.json'
            if pending_file.exists():
                try:
                    with open(pending_file, 'r', encoding='utf-8') as f:
                        pm = json.load(f)
                    
                    new_pm = [m for m in pm if str(m.get('match_id')) != str(match_id)]
                    
                    if len(new_pm) != len(pm):
                         with open(pending_file, 'w', encoding='utf-8') as f:
                            json.dump(new_pm, f, indent=2, ensure_ascii=False)
                except:
                    pass
        
        return jsonify({
            'status': 'success', 
            'match': match_data,
            'result_found': bool(result_found)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    data = request.json
    matches_to_cache = data.get('matches', []) # List of {season, league_id, match_id}
    concurrency = int(data.get('concurrency', 1))
    
    if not matches_to_cache:
        return jsonify({'error': 'No matches provided'}), 400

    import subprocess
    import tempfile
    
    # Create temp job file that will persist until the background process deletes it
    # We use delete=False so it stays on disk
    try:
        # Generate a unique filename in a 'jobs' directory or tmp
        jobs_dir = Path(__file__).resolve().parent.parent / 'jobs'
        jobs_dir.mkdir(exist_ok=True)
        job_file_path = jobs_dir / f"job_{int(time.time())}_{len(matches_to_cache)}.json"
        
        with open(job_file_path, 'w') as f:
            json.dump(matches_to_cache, f)
            
        print(f"Created job file: {job_file_path}")
        
        # Prepare command to launch background_runner.py
        # We need to find where background_runner.py is (root dim)
        runner_script = Path(__file__).resolve().parent.parent / 'background_runner.py'
        
        if not runner_script.exists():
             return jsonify({'error': f'Runner script not found at {runner_script}'}), 500

        # Command: py background_runner.py --job_file <path> --concurrency <N>
        python_cmd = "py" # As per user rules
        
        cmd = [python_cmd, str(runner_script), "--job_file", str(job_file_path), "--concurrency", str(concurrency)]
        
        # Launch in new console
        creation_flags = subprocess.CREATE_NEW_CONSOLE if os.name == 'nt' else 0
        
        # We use Popen to let it run detached
        subprocess.Popen(cmd, creationflags=creation_flags, close_fds=True)
        
        return jsonify({
            'status': 'started', 
            'message': f'Proceso iniciado en nueva ventana. {len(matches_to_cache)} partidos en cola.'
        })

    except Exception as e:
        print(f"Error launching background process: {e}")
        return jsonify({'error': str(e)}), 500

# =============================================
# AI PREDICTION ENDPOINT (Gemini API)
# =============================================
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY', '')

@app.route('/api/ai_prediction', methods=['POST'])
def api_ai_prediction():
    """Generate AI match prediction using Gemini API."""
    try:
        data = request.get_json()
        match_id = data.get('match_id')
        winner = data.get('winner', 'home')  # 'home' or 'away'
        
        # Accept team names from frontend (in case precacheo data doesn't have them)
        frontend_home_team = data.get('home_team')
        frontend_away_team = data.get('away_team')
        
        if not match_id:
            return jsonify({'error': 'match_id is required'}), 400
        
        # Get match data from precacheo (it's a list of matches)
        precacheo_list = data_manager.load_precacheo_matches()
        match_data = {}
        for m in precacheo_list:
            if str(m.get('match_id')) == str(match_id):
                match_data = m
                break
        
        # Use frontend team names if available, fallback to precacheo data
        home_team = frontend_home_team or match_data.get('home_team', 'Home Team')
        away_team = frontend_away_team or match_data.get('away_team', 'Away Team')
        handicap = match_data.get('asian_handicap_raw') or match_data.get('asian_handicap') or 'N/A'
        
        # Get prev match info if available
        prev_home = match_data.get('last_home_match', {})
        prev_away = match_data.get('last_away_match', {})
        
        prev_home_info = ""
        if prev_home and prev_home.get('score'):
            prev_home_info = f"{home_team}'s last home game: {prev_home.get('score', 'N/A')} (AH: {prev_home.get('handicap_line_raw', 'N/A')})"
        
        prev_away_info = ""
        if prev_away and prev_away.get('score'):
            prev_away_info = f"{away_team}'s last away game: {prev_away.get('score', 'N/A')} (AH: {prev_away.get('handicap_line_raw', 'N/A')})"
        
        # Determine winner team name
        winner_team = home_team if winner == 'home' else away_team
        loser_team = away_team if winner == 'home' else home_team
        
        # Build prompt with specific winner
        prompt = f"""You are a football betting analyst. The user has selected that {winner_team} will WIN this match.

Match: {home_team} vs {away_team}
Asian Handicap: {handicap}
{prev_home_info}
{prev_away_info}

Write a confident betting prediction (50-70 words) in English explaining why {winner_team} will beat {loser_team} and cover the handicap. Mention both team names. Be specific about factors like form, home/away advantage, and handicap coverage. Do NOT hedge - the user chose {winner_team} to win."""

        # Call Gemini API
        import google.generativeai as genai
        
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-2.0-flash')
        
        response = model.generate_content(prompt)
        prediction_text = response.text
        
        return jsonify({
            'status': 'success',
            'match_id': match_id,
            'home_team': home_team,
            'away_team': away_team,
            'handicap': handicap,
            'prediction': prediction_text
        })
        
    except ImportError:
        return jsonify({'error': 'google-generativeai package not installed. Run: pip install google-generativeai'}), 500
    except Exception as e:
        print(f"AI Prediction error: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)


