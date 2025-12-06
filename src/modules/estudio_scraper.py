# src/modules/estudio_scraper.py

import time
import copy
import requests
import re
import json
import math
import threading
from contextlib import contextmanager
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
# Selenium imports removed
SELENIUM_AVAILABLE = False


# --- CONFIGURACIÓN GLOBAL ---
BASE_URL_OF = "https://live2.nowgoal26.com"
SELENIUM_TIMEOUT_SECONDS_OF = 10
PLACEHOLDER_NODATA = "*(No disponible)*"
REQUEST_TIMEOUT_SECONDS = 10
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
    "Referer": BASE_URL_OF,
}
SOUP_CACHE_TTL_SECONDS = 45
STATS_CACHE_TTL_SECONDS = 300
ANALYSIS_CACHE_TTL_SECONDS = 120

_requests_session = None
_requests_session_lock = threading.Lock()
_soup_cache = {}
_soup_cache_lock = threading.Lock()
_stats_cache = {}
_stats_cache_lock = threading.Lock()
_analysis_cache = {}
_analysis_cache_lock = threading.Lock()
_STATS_NOT_FOUND = object()


def _read_cache(cache_dict, key, ttl_seconds, lock):
    with lock:
        entry = cache_dict.get(key)
        if not entry:
            return None
        ts, value = entry
        if (time.time() - ts) > ttl_seconds:
            cache_dict.pop(key, None)
            return None
        return value

def _write_cache(cache_dict, key, value, lock):
    with lock:
        cache_dict[key] = (time.time(), value)


def _get_cached_analysis(match_id: str):
    cached = _read_cache(_analysis_cache, match_id, ANALYSIS_CACHE_TTL_SECONDS, _analysis_cache_lock)
    if cached is None:
        return None
    return copy.deepcopy(cached)


def _set_cached_analysis(match_id: str, payload: dict):
    _write_cache(_analysis_cache, match_id, copy.deepcopy(payload), _analysis_cache_lock)

# --- FUNCIONES HELPER PARA PARSEO Y FORMATEO ---
def parse_ah_to_number_of(ah_line_str: str):
    if not isinstance(ah_line_str, str): return None
    s = ah_line_str.strip().replace(' ', '')
    if not s or s in ['-', '?']: return None
    original_starts_with_minus = ah_line_str.strip().startswith('-')
    try:
        if '/' in s:
            parts = s.split('/')
            if len(parts) != 2: return None
            p1_str, p2_str = parts[0], parts[1]
            val1 = float(p1_str)
            val2 = float(p2_str)
            if val1 < 0 and not p2_str.startswith('-') and val2 > 0:
                 val2 = -abs(val2)
            elif original_starts_with_minus and val1 == 0.0 and \
                 (p1_str == "0" or p1_str == "-0") and \
                 not p2_str.startswith('-') and val2 > 0:
                val2 = -abs(val2)
            return (val1 + val2) / 2.0
        else:
            return float(s)
    except (ValueError, IndexError):
        return None

def format_ah_as_decimal_string_of(ah_line_str: str, for_sheets=False, absolute=False):
    if not isinstance(ah_line_str, str) or not ah_line_str.strip() or ah_line_str.strip() in ['-', '?']:
        return ah_line_str.strip() if isinstance(ah_line_str, str) and ah_line_str.strip() in ['-','?'] else '-'
    numeric_value = parse_ah_to_number_of(ah_line_str)
    if numeric_value is None:
        return ah_line_str.strip() if ah_line_str.strip() in ['-','?'] else '-'
    if numeric_value == 0.0: return "0"
    sign = -1 if numeric_value < 0 else 1
    abs_num = abs(numeric_value)
    mod_val = abs_num % 1
    if mod_val == 0.0: abs_rounded = abs_num
    elif mod_val == 0.25: abs_rounded = math.floor(abs_num) + 0.25
    elif mod_val == 0.5: abs_rounded = abs_num
    elif mod_val == 0.75: abs_rounded = math.floor(abs_num) + 0.75
    else:
        if mod_val < 0.25: abs_rounded = math.floor(abs_num)
        elif mod_val < 0.75: abs_rounded = math.floor(abs_num) + 0.5
        else: abs_rounded = math.ceil(abs_num)
    final_value_signed = sign * abs_rounded
    
    if absolute:
        final_value_signed = abs(final_value_signed)

    if final_value_signed == 0.0: output_str = "0"
    elif abs(final_value_signed - round(final_value_signed, 0)) < 1e-9 : output_str = str(int(round(final_value_signed, 0)))
    elif abs(final_value_signed - (math.floor(final_value_signed) + 0.5)) < 1e-9: output_str = f"{final_value_signed:.1f}"
    elif abs(final_value_signed - (math.floor(final_value_signed) + 0.25)) < 1e-9 or \
         abs(final_value_signed - (math.floor(final_value_signed) + 0.75)) < 1e-9: output_str = f"{final_value_signed:.2f}".replace(".25", ".25").replace(".75", ".75")
    else: output_str = f"{final_value_signed:.2f}"
    if for_sheets:
        return "'" + output_str.replace('.', ',') if output_str not in ['-','?'] else output_str
    return output_str

def _df_to_rows(df):
    rows = []
    if df is None or df.empty:
        return rows
    for idx, row in df.iterrows():
        label = str(idx).replace('Shots on Goal', 'Tiros a Puerta').replace('Shots', 'Tiros').replace('Dangerous Attacks', 'Ataques Peligrosos').replace('Attacks', 'Ataques')
        rows.append({
            'label': label,
            'home': row.get('Casa', ''),
            'away': row.get('Fuera', '')
        })
    return rows

# --- SISTEMA DE ANÁLISIS DE MERCADO ---
def check_handicap_cover(resultado_raw: str, ah_line_num: float, favorite_team_name: str, home_team_in_h2h: str, away_team_in_h2h: str, main_home_team_name: str):
    try:
        goles_h, goles_a = map(int, resultado_raw.split('-'))
        if ah_line_num == 0.0:
            if main_home_team_name.lower() == home_team_in_h2h.lower():
                if goles_h > goles_a: return ("CUBIERTO", True)
                elif goles_a > goles_h: return ("NO CUBIERTO", False)
                else: return ("PUSH", None)
            else:
                if goles_a > goles_h: return ("CUBIERTO", True)
                elif goles_h > goles_a: return ("NO CUBIERTO", False)
                else: return ("PUSH", None)
        
        if favorite_team_name.lower() == home_team_in_h2h.lower():
            favorite_margin = goles_h - goles_a
        elif favorite_team_name.lower() == away_team_in_h2h.lower():
            favorite_margin = goles_a - goles_h
        else:
            return ("indeterminado", None)
        
        if favorite_margin - abs(ah_line_num) > 0.05:
            return ("CUBIERTO", True)
        elif favorite_margin - abs(ah_line_num) < -0.05:
            return ("NO CUBIERTO", False)
        else:
            return ("PUSH", None)
    except (ValueError, TypeError, AttributeError):
        return ("indeterminado", None)

def check_goal_line_cover(resultado_raw: str, goal_line_num: float):
    try:
        goles_h, goles_a = map(int, resultado_raw.split('-'))
        total_goles = goles_h + goles_a
        if total_goles > goal_line_num:
            return ("SUPERADA (Over)", True)
        elif total_goles < goal_line_num:
            return (f"NO SUPERADA (UNDER)", False)
        else:
            return ("PUSH (Igual)", None)
    except (ValueError, TypeError):
        return ("indeterminado", None)

def _analizar_precedente_handicap(precedente_data, ah_actual_num, favorito_actual_name, main_home_team_name):
    res_raw = precedente_data.get('res_raw')
    ah_raw = precedente_data.get('ah_raw')
    home_team_precedente = precedente_data.get('home')
    away_team_precedente = precedente_data.get('away')

    if not all([res_raw, res_raw != '?-?', ah_raw, ah_raw != '-']):
        return {"html": "<li><span class='ah-value'>Hándicap:</span> No hay datos suficientes en este precedente.</li>", "movement": "N/A", "result": "N/A", "evaluation": "N/A", "is_covered": None}

    ah_historico_num = parse_ah_to_number_of(ah_raw)
    comparativa_texto = ""

    if ah_historico_num is not None and ah_actual_num is not None:
        formatted_ah_historico = format_ah_as_decimal_string_of(ah_raw)
        formatted_ah_actual = format_ah_as_decimal_string_of(str(ah_actual_num))
        line_movement_str = f"{formatted_ah_historico} → {formatted_ah_actual}"
        
        favorito_historico_name = None
        if ah_historico_num > 0:
            favorito_historico_name = home_team_precedente
        elif ah_historico_num < 0:
            favorito_historico_name = away_team_precedente
        
        if favorito_actual_name.lower() == (favorito_historico_name or "").lower():
            if abs(ah_actual_num) > abs(ah_historico_num):
                comparativa_texto = f"El mercado considera a este equipo <strong>más favorito</strong> que en el precedente (movimiento: <strong style='color: green; font-size:1.2em;'>{line_movement_str}</strong>). "
            elif abs(ah_actual_num) < abs(ah_historico_num):
                comparativa_texto = f"El mercado considera a este equipo <strong>menos favorito</strong> que en el precedente (movimiento: <strong style='color: orange; font-size:1.2em;'>{line_movement_str}</strong>). "
            else:
                comparativa_texto = f"El mercado mantiene una línea de <strong>magnitud idéntica</strong> a la del precedente (<strong>{formatted_ah_historico}</strong>). "
        else:
            if favorito_historico_name and favorito_actual_name != "Ninguno (línea en 0)":
                comparativa_texto = f"Ha habido un <strong>cambio total de favoritismo</strong>. En el precedente el favorito era '{favorito_historico_name}' (movimiento: <strong style='color: red; font-size:1.2em;'>{line_movement_str}</strong>). "
            elif not favorito_historico_name:
                comparativa_texto = f"El mercado establece un favorito claro, considerándolo <strong>mucho más favorito</strong> que en el precedente (movimiento: <strong style='color: green; font-size:1.2em;'>{line_movement_str}</strong>). "
            else:
                comparativa_texto = f"El mercado <strong>ha eliminado al favorito</strong> ('{favorito_historico_name}') que existía en el precedente (movimiento: <strong style='color: orange; font-size:1.2em;'>{line_movement_str}</strong>). "
    else:
        comparativa_texto = f"No se pudo realizar una comparación detallada (línea histórica: <strong>{format_ah_as_decimal_string_of(ah_raw)}</strong>). "

    resultado_cover, cubierto = check_handicap_cover(res_raw, ah_actual_num, favorito_actual_name, home_team_precedente, away_team_precedente, main_home_team_name)
    
    if cubierto is True:
        cover_html = f"<span style='color: green; font-weight: bold;'>CUBIERTO ✅</span>"
    elif cubierto is False:
        cover_html = f"<span style='color: red; font-weight: bold;'>NO CUBIERTO ❌</span>"
    else:
        cover_html = f"<span style='color: #6c757d; font-weight: bold;'>{resultado_cover.upper()} 🤔</span>"

    return {
        "html": f"<li><span class='ah-value'>Hándicap:</span> {comparativa_texto}Con el resultado ({res_raw.replace('-' , ':')}), la línea actual se habría considerado {cover_html}.</li>",
        "movement": line_movement_str if 'line_movement_str' in locals() else "N/A",
        "result": res_raw.replace('-', ':'),
        "evaluation": resultado_cover,
        "is_covered": cubierto
    }

def _analizar_precedente_goles(precedente_data, goles_actual_num):
    res_raw = precedente_data.get('res_raw')
    if not res_raw or res_raw == '?-?':
        return "<li><span class='score-value'>Goles:</span> No hay datos suficientes en este precedente.</li>"
    try:
        total_goles = sum(map(int, res_raw.split('-')))
        resultado_cover, _ = check_goal_line_cover(res_raw, goles_actual_num)
        if 'SUPERADA' in resultado_cover:
            cover_html = f"<span style='color: green; font-weight: bold;'>{resultado_cover}</span>"
        elif 'NO SUPERADA' in resultado_cover:
            cover_html = f"<span style='color: red; font-weight: bold;'>{resultado_cover}</span>"
        else:
            cover_html = f"<span style='color: #6c757d; font-weight: bold;'>{resultado_cover}</span>"
        
        return f"<li><span class='score-value'>Goles:</span> El partido tuvo <strong>{total_goles} goles</strong>, por lo que la línea actual habría resultado {cover_html}.</li>"
    except (ValueError, TypeError):
        return "<li><span class='score-value'>Goles:</span> No se pudo procesar el resultado del precedente.</li>"

def generar_analisis_completo_mercado(main_odds, h2h_data, home_name, away_name):
    ah_actual_str = format_ah_as_decimal_string_of(main_odds.get('ah_linea_raw', '-'))
    ah_actual_num = parse_ah_to_number_of(ah_actual_str)
    goles_actual_num = parse_ah_to_number_of(main_odds.get('goals_linea_raw', '-'))

    if ah_actual_num is None or goles_actual_num is None: return "", {}

    favorito_name, favorito_html = "Ninguno (línea en 0)", "Ninguno (línea en 0)"
    if ah_actual_num < 0:
        favorito_name, favorito_html = away_name, f"<span class='away-color'>{away_name}</span>"
    elif ah_actual_num > 0:
        favorito_name, favorito_html = home_name, f"<span class='home-color'>{home_name}</span>"
    
    titulo_html = f"<p style='margin-bottom: 12px;'><strong>📊 Análisis de Mercado vs. Histórico H2H</strong><br><span style='font-style: italic; font-size: 0.9em;'>Líneas actuales: AH {ah_actual_str} / Goles {goles_actual_num} | Favorito: {favorito_html}</span></p>"

    precedente_estadio = {
        'res_raw': h2h_data.get('res1_raw'), 'ah_raw': h2h_data.get('ah1'),
        'home': home_name, 'away': away_name, 'match_id': h2h_data.get('match1_id')
    }
    sintesis_ah_estadio_data = _analizar_precedente_handicap(precedente_estadio, ah_actual_num, favorito_name, home_name)
    sintesis_ah_estadio = sintesis_ah_estadio_data["html"]
    sintesis_goles_estadio = _analizar_precedente_goles(precedente_estadio, goles_actual_num)
    
    analisis_estadio_html = (
        f"<div style='margin-bottom: 10px;'>"
        f"  <strong style='font-size: 1.05em;'>🏟️ Análisis del Precedente en Este Estadio</strong>"
        f"  <ul style='margin: 5px 0 0 20px; padding-left: 0;'>{sintesis_ah_estadio}{sintesis_goles_estadio}</ul>"
        f"</div>"
    )

    precedente_general_id = h2h_data.get('match6_id')
    
    if precedente_estadio['match_id'] and precedente_general_id and precedente_estadio['match_id'] == precedente_general_id:
        analisis_general_html = (
            "<div style='margin-top: 10px;'>"
            "  <strong>✈️ Análisis del H2H General Más Reciente</strong>"
            "  <p style='margin: 5px 0 0 20px; font-style: italic; font-size: 0.9em;'>"
            "    El precedente es el mismo partido analizado arriba."
            "  </p>"
            "</div>"
        )
    else:
        precedente_general = {
            'res_raw': h2h_data.get('res6_raw'),
            'ah_raw': h2h_data.get('ah6'),
            'home': h2h_data.get('h2h_gen_home'),
            'away': h2h_data.get('h2h_gen_away'),
            'match_id': precedente_general_id
        }
        sintesis_ah_general_data = _analizar_precedente_handicap(precedente_general, ah_actual_num, favorito_name, home_name)
        sintesis_ah_general = sintesis_ah_general_data["html"]
        sintesis_goles_general = _analizar_precedente_goles(precedente_general, goles_actual_num)
        
        analisis_general_html = (
            f"<div>"
            f"  <strong style='font-size: 1.05em;'>✈️ Análisis del H2H General Más Reciente</strong>"
            f"  <ul style='margin: 5px 0 0 20px; padding-left: 0;'>{sintesis_ah_general}{sintesis_goles_general}</ul>"
            f"</div>"
        )
        
    html_output = f"""
    <div style="border-left: 4px solid #1E90FF; padding: 12px 15px; margin-top: 15px; background-color: #f0f2f6; border-radius: 5px; font-size: 0.95em;">
        {titulo_html}
        {analisis_estadio_html}
        {analisis_general_html}
    </div>
    """
    
    structured_data = {
        "stadium": {
            "movement": sintesis_ah_estadio_data.get("movement"),
            "result": sintesis_ah_estadio_data.get("result"),
            "evaluation": sintesis_ah_estadio_data.get("evaluation"),
            "is_covered": sintesis_ah_estadio_data.get("is_covered")
        },
        "general": {
            "movement": sintesis_ah_general_data.get("movement") if 'sintesis_ah_general_data' in locals() else (sintesis_ah_estadio_data.get("movement") if precedente_estadio['match_id'] == precedente_general_id else "N/A"),
            "result": sintesis_ah_general_data.get("result") if 'sintesis_ah_general_data' in locals() else (sintesis_ah_estadio_data.get("result") if precedente_estadio['match_id'] == precedente_general_id else "N/A"),
            "evaluation": sintesis_ah_general_data.get("evaluation") if 'sintesis_ah_general_data' in locals() else (sintesis_ah_estadio_data.get("evaluation") if precedente_estadio['match_id'] == precedente_general_id else "N/A"),
            "is_covered": sintesis_ah_general_data.get("is_covered") if 'sintesis_ah_general_data' in locals() else (sintesis_ah_estadio_data.get("is_covered") if precedente_estadio['match_id'] == precedente_general_id else None)
        }
    }
    
    return html_output, structured_data

def _build_historical_matches_list_html(home_matches, away_matches, home_team_name, away_team_name):
    if not home_matches and not away_matches:
        return ""

    html = "<div class='historical-matches-container'><div class='row'>"

    def build_table(matches, title, team_name, is_home_context):
        if not matches: return ""
        
        table_html = f"""
        <div class="col-lg-6">
            <div class="card mb-3">
                <div class="card-header bg-light">
                    <h6 class="mb-0"><strong>{title}</strong> <small class="text-muted">({team_name})</small></h6>
                </div>
                <div class="table-responsive">
                    <table class="table table-sm table-hover mb-0" style="font-size: 0.85rem;">
                        <thead class="table-light">
                            <tr>
                                <th>Liga</th>
                                <th>Fecha</th>
                                <th class="text-end">Local</th>
                                <th class="text-center">Res</th>
                                <th>Visitante</th>
                                <th class="text-center">AH</th>
                                <th class="text-center">O/U</th>
                            </tr>
                        </thead>
                        <tbody>
        """
        
        for m in matches:
            date = m.get('date', '-')
            league = m.get('league_id_hist', '-')
            home = m.get('home', '-')
            away = m.get('away', '-')
            score = m.get('score', '-')
            ah = m.get('ahLine', '-')
            ou = m.get('ouLine', '-')
            
            # Highlight logic
            home_class = "fw-bold text-primary" if team_name.lower() in home.lower() else ""
            away_class = "fw-bold text-primary" if team_name.lower() in away.lower() else ""
            
            # Score coloring (simple win/loss logic if possible, otherwise just bold)
            score_style = "font-weight:bold;"
            
            table_html += f"""
                        <tr>
                            <td>{league}</td>
                            <td>{date}</td>
                            <td class="text-end {home_class}">{home}</td>
                            <td class="text-center" style="{score_style}">{score}</td>
                            <td class="{away_class}">{away}</td>
                            <td class="text-center"><span class="badge bg-light text-dark border">{ah}</span></td>
                            <td class="text-center"><span class="badge bg-light text-dark border">{ou}</span></td>
                        </tr>
            """
        
        table_html += """
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
        """
        return table_html

    if home_matches:
        html += build_table(home_matches, "Partidos en Casa", home_team_name, True)
    
    if away_matches:
        html += build_table(away_matches, "Partidos Fuera", away_team_name, False)

    html += "</div></div>"
    return html

def _calculate_stats_for_matches(matches, team_name):
    stats = {'W': 0, 'D': 0, 'L': 0, 'O': 0, 'U': 0, 'Push': 0, 'HasOU': False}
    for m in matches:
        score_raw = m.get('score_raw', '')
        if not score_raw or '-' not in score_raw: continue
        try:
            parts = score_raw.split('-')
            h_s = int(parts[0])
            a_s = int(parts[1])
        except:
            continue
            
        is_home_team = team_name.lower() in m.get('home', '').lower()
        
        # W/D/L
        if is_home_team:
            if h_s > a_s: stats['W'] += 1
            elif h_s == a_s: stats['D'] += 1
            else: stats['L'] += 1
        else:
            if a_s > h_s: stats['W'] += 1
            elif a_s == h_s: stats['D'] += 1
            else: stats['L'] += 1
            
        # O/U - Requires ouLine to be present and numeric
        ou_line_str = m.get('ouLine', 'N/A')
        if ou_line_str and ou_line_str not in ['-', 'N/A', '?']:
            try:
                line = float(ou_line_str)
                total = h_s + a_s
                stats['HasOU'] = True
                if total > line: stats['O'] += 1
                elif total < line: stats['U'] += 1
                else: stats['Push'] += 1
            except:
                pass
                
    return stats

def _build_historical_matches_list_html(home_matches, away_matches, home_team_name, away_team_name):
    if not home_matches and not away_matches:
        return ""

    html = "<div class='historical-matches-container'><div class='row'>"

    def build_table(matches, title, team_name, is_home_context):
        if not matches: return ""
        
        stats = _calculate_stats_for_matches(matches, team_name)
        
        table_html = f"""
        <div class="col-lg-6">
            <div class="card mb-3">
                <div class="card-header bg-light">
                    <h6 class="mb-0"><strong>{title}</strong> <small class="text-muted">({team_name})</small></h6>
                </div>
                <div class="table-responsive">
                    <table class="table table-sm table-hover mb-0" style="font-size: 0.85rem;">
                        <thead class="table-light">
                            <tr>
                                <th>Liga</th>
                                <th>Fecha</th>
                                <th class="text-end">Local</th>
                                <th class="text-center">Res</th>
                                <th>Visitante</th>
                                <th class="text-center">AH</th>

                            </tr>
                        </thead>
                        <tbody>
        """
        
        for m in matches:
            date = m.get('date', '-')
            league = m.get('league_id_hist', '-')
            home = m.get('home', '-')
            away = m.get('away', '-')
            score = m.get('score', '-')
            ah = m.get('ahLine', '-')
            ou = m.get('ouLine', '-')
            
            # Highlight logic
            home_class = "fw-bold text-primary" if team_name.lower() in home.lower() else ""
            away_class = "fw-bold text-primary" if team_name.lower() in away.lower() else ""
            
            # Score coloring
            score_style = "font-weight:bold;"
            
            table_html += f"""
                        <tr>
                            <td>{league}</td>
                            <td>{date}</td>
                            <td class="text-end {home_class}">{home}</td>
                            <td class="text-center" style="{score_style}">{score}</td>
                            <td class="{away_class}">{away}</td>
                            <td class="text-center"><span class="badge bg-light text-dark border">{ah}</span></td>

                        </tr>
            """
        
        table_html += """
                        </tbody>
                    </table>
                </div>
                <div class="card-footer bg-white">
                    <div class="d-flex justify-content-around text-center" style="font-size: 0.9rem;">
        """
        
        # Add Stats
        table_html += f"""
                        <div>
                            <span class="text-success fw-bold">V: {stats['W']}</span> | 
                            <span class="text-muted fw-bold">E: {stats['D']}</span> | 
                            <span class="text-danger fw-bold">D: {stats['L']}</span>
                        </div>
        """
        

            
        table_html += """
                    </div>
                </div>
            </div>
        </div>
        """
        return table_html

    if home_matches:
        html += build_table(home_matches, "Partidos en Casa", home_team_name, True)
    
    if away_matches:
        html += build_table(away_matches, "Partidos Fuera", away_team_name, False)

    html += "</div></div>"
    return html

# --- FUNCIONES DE EXTRACCIÓN DE DATOS ---
def extract_vs_odds(soup):
    """
    Extrae y parsea la variable Vs_hOdds del script para obtener las cuotas históricas.
    Retorna un diccionario: { match_id: ah_line_str }
    Prioriza Bet365 (ID 8) > Crown (ID 3).
    """
    odds_map = {}
    if not soup: return odds_map
    
    script_content = None
    for script in soup.find_all('script'):
        if script.string and 'var Vs_hOdds' in script.string:
            script_content = script.string
            break
            
    if not script_content: return odds_map
    
    try:
        # Extraer el array Vs_hOdds = [[...]];
        match = re.search(r'var Vs_hOdds\s*=\s*(\[\[.*?\]\]);', script_content, re.DOTALL)
        if match:
            raw_data = match.group(1)
            # Limpiar para JSON
            raw_data = raw_data.replace("'", '"')
            # Manejar posibles trailing commas o ,,
            while ',,' in raw_data:
                raw_data = raw_data.replace(',,', ',null,')
            
            data = json.loads(raw_data)
            
            # Procesar datos
            # Formato: [MatchID, BookieID, H, AH, A, ...]
            # Índices: 0=ID, 1=Bookie, 3=AH Inicial
            
            # Agrupar por match_id
            temp_map = {}
            for row in data:
                if len(row) < 4: continue
                mid = str(row[0])
                bookie = row[1]
                ah = row[3]
                
                if mid not in temp_map:
                    temp_map[mid] = {}
                temp_map[mid][bookie] = ah
            
            # Seleccionar mejor bookie
            for mid, bookies in temp_map.items():
                if 8 in bookies: # Bet365
                    odds_map[mid] = str(bookies[8])
                elif 3 in bookies: # Crown
                    odds_map[mid] = str(bookies[3])
                elif bookies: # Cualquiera
                    odds_map[mid] = str(next(iter(bookies.values())))
                    
    except Exception as e:
        print(f"Error parsing Vs_hOdds: {e}")
        
    return odds_map

def get_match_details_from_row_of(row_element, score_class_selector='score', source_table_type='h2h', odds_map=None):
    try:
        cells = row_element.find_all('td')
        home_idx, score_idx, away_idx, ah_idx = 2, 3, 4, 11
        if len(cells) <= ah_idx: return None
        date_span = cells[1].find('span', attrs={'name': 'timeData'})
        # Priorizar data-t si existe (formato YYYY-MM-DD HH:MM:SS)
        if date_span and date_span.get('data-t'):
             date_txt = date_span.get('data-t', '').split(' ')[0]
        elif cells[1].get('data-t'):
             date_txt = cells[1].get('data-t', '').split(' ')[0]
        else:
             date_txt = date_span.get_text(strip=True) if date_span else ''
        
        def get_cell_txt(idx):
            a = cells[idx].find('a')
            return a.get_text(strip=True) if a else cells[idx].get_text(strip=True)
        
        def get_red_card(idx):
            # Buscar span con clase 'rcard' o 'red-card'
            rc = cells[idx].find('span', class_=lambda c: c and ('rcard' in c or 'red-card' in c))
            return rc.get_text(strip=True) if rc else None

        home, away = get_cell_txt(home_idx), get_cell_txt(away_idx)
        home_red, away_red = get_red_card(home_idx), get_red_card(away_idx)

        if not home or not away: return None
        score_cell = cells[score_idx]
        score_span = score_cell.find('span', class_=lambda c: isinstance(c, str) and score_class_selector in c)
        score_raw_text = (score_span.get_text(strip=True) if score_span else score_cell.get_text(strip=True)) or ''
        m = re.search(r'(\d+)\s*-\s*(\d+)', score_raw_text)
        score_raw, score_fmt = (f"{m.group(1)}-{m.group(2)}", f"{m.group(1)}:{m.group(2)}") if m else ('?-?', '?:?')
        ah_cell = cells[ah_idx]
        ah_line_raw = (ah_cell.get('data-o') or ah_cell.text).strip()
        
        # Fallback usando odds_map si está disponible y el dato está vacío
        if (not ah_line_raw or ah_line_raw == '-') and odds_map:
            match_index = row_element.get('index')
            if match_index and match_index in odds_map:
                ah_line_raw = odds_map[match_index]

        ah_line_fmt = format_ah_as_decimal_string_of(ah_line_raw) if ah_line_raw not in ['', '-'] else '-'
        
        # Intentar extraer Goal Line (O/U)
        # Basado en analisis.txt, la columna O/U parece estar después de AH Away
        # Indices típicos: Home(2), Score(3), Away(4), ... AH(11) ...
        # En analisis.txt:
        # td[10] -> AH Home Odds
        # td[11] -> AH Line
        # td[12] -> AH Away Odds
        # td[13] -> AH Result (W/L)
        # td[14] -> OU Result (U/O) ?? No, wait.
        
        # Vamos a intentar extraer de la celda siguiente a AH si existe
        ou_line_raw = 'N/A'
        if len(cells) > 12:
             # A veces la linea de gol esta en otra columna o data attribute
             # Por ahora, si no la encontramos explícitamente, dejaremos N/A o intentaremos buscar en data-o
             # En analisis.txt, la celda 12 (indice 12) tiene data-o="0.90" (Away Odds?)
             pass

        return {
            'date': date_txt, 'home': home, 'away': away, 'score': score_fmt,
            'score_raw': score_raw, 'ahLine': ah_line_fmt, 'ahLine_raw': ah_line_raw or '-',
            'ouLine': ou_line_raw, # Placeholder por ahora
            'matchIndex': row_element.get('index'), 'vs': row_element.get('vs'),
            'league_id_hist': row_element.get('title') or row_element.get('name'), # Usar title como nombre de liga si existe
            'home_red': home_red, 'away_red': away_red
        }
    except Exception:
        return None

def get_requests_session_of():
    global _requests_session
    with _requests_session_lock:
        if _requests_session is None:
            session = requests.Session()
            retries = Retry(total=3, backoff_factor=0.4, status_forcelist=[500, 502, 503, 504])
            adapter = HTTPAdapter(max_retries=retries, pool_connections=32, pool_maxsize=32)
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            session.headers.update(REQUEST_HEADERS)
            _requests_session = session
        return _requests_session

def get_match_progression_stats_data(match_id: str) -> pd.DataFrame | None:
    if not match_id or not str(match_id).isdigit():
        return None
    match_id = str(match_id)
    cached_value = _read_cache(_stats_cache, match_id, STATS_CACHE_TTL_SECONDS, _stats_cache_lock)
    if cached_value is not None:
        if cached_value is _STATS_NOT_FOUND:
            return None
        return cached_value.copy(deep=True)

    url = f"{BASE_URL_OF}/match/live-{match_id}"
    try:
        session = get_requests_session_of()
        response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        stat_titles = {"Shots": "-", "Shots on Goal": "-", "Attacks": "-", "Dangerous Attacks": "-"}
        team_tech_div = soup.find('div', id='teamTechDiv_detail')
        if team_tech_div and (stat_list := team_tech_div.find('ul', class_='stat')):
            for li in stat_list.find_all('li'):
                if (title_span := li.find('span', class_='stat-title')) and (stat_title := title_span.get_text(strip=True)) in stat_titles:
                    values = [v.get_text(strip=True) for v in li.find_all('span', class_='stat-c')]
                    if len(values) == 2:
                        stat_titles[stat_title] = {"Home": values[0], "Away": values[1]}
        table_rows = [{"Estadistica_EN": name, "Casa": vals.get('Home', '-'), "Fuera": vals.get('Away', '-')}
                      for name, vals in stat_titles.items() if isinstance(vals, dict)]
        df = pd.DataFrame(table_rows)
        df = df.set_index("Estadistica_EN") if not df.empty else df
        cache_value = df.copy(deep=True) if df is not None else _STATS_NOT_FOUND
        _write_cache(_stats_cache, match_id, cache_value, _stats_cache_lock)
        return df
    except requests.RequestException:
        _write_cache(_stats_cache, match_id, _STATS_NOT_FOUND, _stats_cache_lock)
        return None

def get_rival_a_for_original_h2h_of(soup, league_id=None):
    if not soup or not (table := soup.find("table", id="table_v1")): return None, None, None
    for row in table.find_all("tr", id=re.compile(r"tr1_\d+")):
        if league_id and row.get("name") != str(league_id):
            continue
        if row.get("vs") == "1" and (key_id := row.get("index")):
            onclicks = row.find_all("a", onclick=True)
            if len(onclicks) > 1 and (rival_tag := onclicks[1]) and (rival_id_match := re.search(r"team\((\d+)\)", rival_tag.get("onclick", ""))):
                return key_id, rival_id_match.group(1), rival_tag.text.strip()
    return None, None, None

def get_rival_b_for_original_h2h_of(soup, league_id=None):
    if not soup or not (table := soup.find("table", id="table_v2")): return None, None, None
    for row in table.find_all("tr", id=re.compile(r"tr2_\d+")):
        if league_id and row.get("name") != str(league_id):
            continue
        if row.get("vs") == "1" and (key_id := row.get("index")):
            onclicks = row.find_all("a", onclick=True)
            if len(onclicks) > 0 and (rival_tag := onclicks[0]) and (rival_id_match := re.search(r"team\((\d+)\)", rival_tag.get("onclick", ""))):
                return key_id, rival_id_match.group(1), rival_tag.text.strip()
    return None, None, None

def get_h2h_details_for_original_logic_of(key_match_id, rival_a_id, rival_b_id, rival_a_name="Rival A", rival_b_name="Rival B"):
    if not all([key_match_id, rival_a_id, rival_b_id]):
        return {"status": "error", "resultado": "N/A (Datos incompletos para H2H)"}
    
    url = f"{BASE_URL_OF}/match/h2h-{key_match_id}"
    try:
        session = get_requests_session_of()
        response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "lxml")
        
        # Extraer odds del script Vs_hOdds
        odds_map = extract_vs_odds(soup)
        
    except Exception as e:
        return {"status": "error", "resultado": f"N/A (Error Requests en H2H Col3: {type(e).__name__})"}

    if not (table := soup.find("table", id="table_v2")):
        return {"status": "error", "resultado": "N/A (Tabla H2H Col3 no encontrada)"}
    
    for row in table.find_all("tr", id=re.compile(r"tr2_\d+")):
        links = row.find_all("a", onclick=True)
        if len(links) < 2: continue
        
        # Extract IDs from onclick="...team(123)..."
        h_id_m = re.search(r"team\((\d+)\)", links[0].get("onclick", ""))
        a_id_m = re.search(r"team\((\d+)\)", links[1].get("onclick", ""))
        
        if not (h_id_m and a_id_m): continue
        h_id, a_id = h_id_m.group(1), a_id_m.group(1)
        
        if {h_id, a_id} == {str(rival_a_id), str(rival_b_id)}:
            if not (score_span := row.find("span", class_="fscore_2")) or "-" not in score_span.text: continue
            score = score_span.text.strip().split("(")[0].strip()
            try:
                g_h, g_a = score.split("-", 1)
            except ValueError:
                continue
                
            tds = row.find_all("td")
            handicap_raw = "N/A"
            if len(tds) > 11:
                cell = tds[11]
                handicap_raw = (cell.get("data-o") or cell.text).strip()
            
            # Fallback con Vs_hOdds
            if (not handicap_raw or handicap_raw == '-' or handicap_raw == 'N/A'):
                match_idx = row.get('index')
                if match_idx and match_idx in odds_map:
                    handicap_raw = odds_map[match_idx]
            
            # Extract Date
            date_txt = "N/A"
            if len(tds) > 1:
                date_span = tds[1].find('span', attrs={'name': 'timeData'})
                if date_span and date_span.get('data-t'):
                    date_txt = date_span.get('data-t', '').split(' ')[0]
                elif tds[1].get('data-t'):
                    date_txt = tds[1].get('data-t', '').split(' ')[0]
                else:
                    date_txt = date_span.get_text(strip=True) if date_span else ''

            # Extract Red Cards
            def get_red_card(cell):
                rc = cell.find('span', class_=lambda c: c and ('rcard' in c or 'red-card' in c))
                return rc.get_text(strip=True) if rc else None
            
            # Assuming home team is in cell 2 (index 2) and away in cell 4 (index 4) based on typical layout
            # But we need to be careful about which link corresponds to which team.
            # The links list has [home_link, away_link].
            # Let's try to find the parent td for each link to check for red cards.
            home_red = get_red_card(links[0].find_parent('td'))
            away_red = get_red_card(links[1].find_parent('td'))

            return {
                "status": "found", "goles_home": g_h.strip(), "goles_away": g_a.strip(),
                "handicap": handicap_raw or "N/A", "match_id": row.get('index'),
                "h2h_home_team_name": links[0].text.strip(), "h2h_away_team_name": links[1].text.strip(),
                "date": date_txt,
                "home_red": home_red, "away_red": away_red
            }
    return {"status": "not_found", "resultado": f"H2H directo no encontrado para {rival_a_name} vs {rival_b_name}."}

def get_team_league_info_from_script_of(soup):
    script_tag = soup.find("script", string=re.compile(r"var _matchInfo = "))
    if not (script_tag and script_tag.string): return (None,) * 3 + ("N/A",) * 3
    content = script_tag.string
    def find_val(pattern):
        match = re.search(pattern, content)
        return match.group(1).replace("\'", "'") if match else None
    home_id = find_val(r"hId:\s*parseInt\('(\d+)'\)")
    away_id = find_val(r"gId:\s*parseInt\('(\d+)'\)")
    league_id = find_val(r"sclassId:\s*parseInt\('(\d+)'\)")
    home_name = find_val(r"hName:\s*'([^']*)'") or "N/A"
    away_name = find_val(r"gName:\s*'([^']*)'") or "N/A"
    league_name = find_val(r"lName:\s*'([^']*)'") or "N/A"
    return home_id, away_id, league_id, home_name, away_name, league_name

def _parse_date_ddmmyyyy(d: str) -> tuple:
    # Intentar formato DD-MM-YYYY
    m = re.search(r'(\d{2})-(\d{2})-(\d{4})', d or '')
    if m: return (int(m.group(3)), int(m.group(2)), int(m.group(1)))
    
    # Intentar formato YYYY-MM-DD
    m2 = re.search(r'(\d{4})-(\d{2})-(\d{2})', d or '')
    if m2: return (int(m2.group(1)), int(m2.group(2)), int(m2.group(3)))
    
    return (1900, 1, 1)

def extract_recent_matches(soup, table_id, team_name, league_id, is_home_game, odds_map=None, limit=5):
    """
    Extrae una lista de los últimos partidos del equipo en esa condición (Local/Visitante).
    Retorna una lista de diccionarios con detalles del partido.
    """
    if not soup or not (table := soup.find("table", id=table_id)): return []
    matches = []
    score_selector = 'fscore_1' if is_home_game else 'fscore_2'
    
    # Iterar sobre las filas de la tabla
    for row in table.find_all("tr", id=re.compile(rf"tr{table_id[-1]}_\d+")):
        if not (details := get_match_details_from_row_of(row, score_class_selector=score_selector, source_table_type='hist', odds_map=odds_map)):
            continue
            
        # Filtrar por liga si es necesario (aunque el usuario pidió "todos", a veces es mejor filtrar)
        # El usuario dijo "todos", así que quizás no filtramos por liga aquí, o lo hacemos opcional.
        # Pero mantengamos la lógica de "Home vs Home" y "Away vs Away" estricta.
        
        is_team_home = team_name.lower() in details.get('home', '').lower()
        is_team_away = team_name.lower() in details.get('away', '').lower()
        
        # Condición: El equipo analizado debe jugar en la condición especificada (Local o Visitante)
        if (is_home_game and is_team_home) or (not is_home_game and is_team_away):
            matches.append(details)
            
    # Ordenar por fecha descendente
    matches.sort(key=lambda x: _parse_date_ddmmyyyy(x.get('date', '')), reverse=True)
    
    return matches[:limit]

def extract_last_match_in_league_of(soup, table_id, team_name, league_id, is_home_game, odds_map=None):
    # Reutilizamos la nueva función pero limitamos a 1 y filtramos por liga si se pide
    matches = extract_recent_matches(soup, table_id, team_name, league_id, is_home_game, odds_map, limit=20)
    
    if league_id:
        matches = [m for m in matches if m.get("league_id_hist") == str(league_id)]
        
    if not matches: return None
    
    last_match = matches[0]
    return {
        "date": last_match.get('date', 'N/A'), "home_team": last_match.get('home'),
        "away_team": last_match.get('away'), "score": last_match.get('score_raw', 'N/A').replace('-', ':'),
        "handicap_line_raw": last_match.get('ahLine_raw', 'N/A'), "match_id": last_match.get('matchIndex')
    }

def fetch_odds_from_bf_data(match_id):
    """
    Fallback para obtener líneas de hándicap y goles desde bf_en-idn.js
    cuando no están disponibles en el HTML principal.
    """
    url = f"{BASE_URL_OF}/gf/data/bf_en-idn.js"
    try:
        session = get_requests_session_of()
        response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        content = response.text
        
        # Buscar la entrada correspondiente al match_id
        # Formato esperado: A[123]=[2696131,...]
        # Buscamos el ID del partido en el contenido
        match_pattern = re.compile(r"A\[\d+\]=\[(.*?)\];")
        
        for match in match_pattern.finditer(content):
            row_str = match.group(1)
            if str(match_id) not in row_str:
                continue
            
            # Encontrado, ahora parseamos con cuidado
            # Reemplazar comillas simples por dobles para JSON
            row_str_clean = row_str.replace("'", '"')
            
            # Reemplazar valores vacíos ,, por ,null,
            while ',,' in row_str_clean:
                row_str_clean = row_str_clean.replace(',,', ',null,')
            
            # Manejar comas al inicio o final dentro de los corchetes (aunque aquí ya tenemos el contenido)
            if row_str_clean.endswith(','):
                row_str_clean += 'null'
            if row_str_clean.startswith(','):
                row_str_clean = 'null' + row_str_clean
                
            try:
                # Envolver en corchetes para parsear como lista
                data = json.loads(f"[{row_str_clean}]")
                
                # Verificar que sea el ID correcto (índice 0)
                if str(data[0]) == str(match_id):
                    # Extraer datos
                    # Índice 21: Hándicap (ej: 0.5, -1, etc.)
                    # Índice 25: Línea de goles (ej: 2.5, 3, etc.)
                    
                    ah_line = data[21] if len(data) > 21 and data[21] is not None else None
                    goals_line = data[25] if len(data) > 25 else None
                    
                    return {
                        "ah_linea_raw": str(ah_line) if ah_line is not None else "N/A",
                        "goals_linea_raw": str(goals_line) if goals_line is not None else "N/A"
                    }
            except json.JSONDecodeError:
                continue
        
        return None
    except Exception as e:
        print(f"Error fetching bf_data: {e}")
        return None

def fetch_odds_from_ajax(match_id):
    """
    Fallback para obtener cuotas desde la API AJAX (especialmente para partidos finalizados).
    Intenta obtener datos de Bet365 (ID 8 o 281) o Sbobet (ID 31).
    """
    url = f"{BASE_URL_OF}/Ajax/SoccerAjax/?type=1&id={match_id}"
    try:
        session = get_requests_session_of()
        response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        if response.status_code != 200:
            return None
            
        data_json = response.json()
        if data_json.get("ErrCode") != 0 or not data_json.get("Data"):
            return None
            
        raw_data = data_json["Data"]
        # Formato: ID*Odds1;Odds2;...^ID*Odds1;...
        companies = raw_data.split('^')
        
        target_odds = None
        
        # Prioridad de IDs: 8 (Bet365), 281 (Bet365), 31 (Sbobet), o el que tenga "*" si no hay ID
        priority_ids = ["8", "281", "31", ""] 
        
        for pid in priority_ids:
            for company_data in companies:
                if "*" not in company_data: continue
                
                comp_id, odds_str = company_data.split('*', 1)
                # Limpiar ID (puede ser "1;" o "8;36" -> tomamos el primero)
                comp_id_clean = comp_id.split(';')[0]
                
                # print(f"Checking company ID: '{comp_id}' (Clean: '{comp_id_clean}') against priority '{pid}'")
                
                # Si pid es "", buscamos el que no tenga ID (ej: "*...") -> comp_id será ""
                if comp_id_clean == pid:
                    # Parsear odds
                    parts = odds_str.split(';')
                    # Buscamos la parte que tenga suficientes datos (al menos 14 campos para AH y OU)
                    # Estructura típica: 1x2(3), AH_Init(3), ?, AH_Live(3), ?, OU_Init(3), ...
                    # Indices aproximados:
                    # 3: AH Home, 4: AH Line, 5: AH Away
                    # 11: OU Over, 12: OU Line, 13: OU Under
                    
                    for part in parts:
                        vals = part.split(',')
                        if len(vals) >= 14:
                            # Verificar que tenga datos válidos (no vacíos)
                            if vals[4] and vals[12]:
                                target_odds = {
                                    "ah_linea_raw": vals[4],
                                    "goals_linea_raw": vals[12]
                                }
                                # print(f"Found odds for ID '{pid}': {target_odds}")
                                # print(f"Found odds for ID '{pid}': {target_odds}")
                                break
                    if target_odds: break
            if target_odds: break
            
        return target_odds

    except Exception as e:
        print(f"Error fetching AJAX odds: {e}")
        return None

def extract_bet365_initial_odds_of(soup, match_id=None):
    odds_info = {
        "ah_home_cuota": "N/A", "ah_linea_raw": "N/A", "ah_away_cuota": "N/A",
        "goals_over_cuota": "N/A", "goals_linea_raw": "N/A", "goals_under_cuota": "N/A"
    }
    
    if soup:
        bet365_row = None
        
        # 1. Estrategia más robusta: Buscar la celda que contiene "Bet365"
        bet365_cell = soup.find("td", string=lambda text: text and "Bet365" in text)
        if not bet365_cell:
            bet365_cell = soup.find("b", string=lambda text: text and "Bet365" in text)
            if bet365_cell:
                bet365_cell = bet365_cell.find_parent("td")
        
        if bet365_cell:
            bet365_row = bet365_cell.find_parent("tr")
        
        # 2. Si falla, intentar selectores específicos conocidos (Legacy)
        if not bet365_row:
            bet365_row = soup.select_one("tr#tr_o_1_8[name='earlyOdds'], tr#tr_o_1_31[name='earlyOdds']")

        # 3. Si encontramos la fila, extraemos los datos
        if bet365_row:
            tds = bet365_row.find_all("td")
            if len(tds) >= 11:
                odds_info["ah_home_cuota"] = tds[2].get("data-o", tds[2].text).strip()
                odds_info["ah_linea_raw"] = tds[3].get("data-o", tds[3].text).strip()
                odds_info["ah_away_cuota"] = tds[4].get("data-o", tds[4].text).strip()
                odds_info["goals_over_cuota"] = tds[8].get("data-o", tds[8].text).strip()
                odds_info["goals_linea_raw"] = tds[9].get("data-o", tds[9].text).strip()
                odds_info["goals_under_cuota"] = tds[10].get("data-o", tds[10].text).strip()

    # Fallback 1: AJAX (para partidos finalizados donde HTML está vacío)
    if (odds_info["ah_linea_raw"] in ["N/A", "-", ""] or odds_info["goals_linea_raw"] in ["N/A", "-", ""]) and match_id:
        ajax_odds = fetch_odds_from_ajax(match_id)
        if ajax_odds:
            if odds_info["ah_linea_raw"] in ["N/A", "-", ""]:
                odds_info["ah_linea_raw"] = ajax_odds.get("ah_linea_raw", "N/A")
            if odds_info["goals_linea_raw"] in ["N/A", "-", ""]:
                odds_info["goals_linea_raw"] = ajax_odds.get("goals_linea_raw", "N/A")

    # Fallback 2: BF Data (para partidos en vivo/futuros si AJAX falla)
    if (odds_info["ah_linea_raw"] in ["N/A", "-", ""] or odds_info["goals_linea_raw"] in ["N/A", "-", ""]) and match_id:
        fallback_data = fetch_odds_from_bf_data(match_id)
        if fallback_data:
            if odds_info["ah_linea_raw"] in ["N/A", "-", ""]:
                odds_info["ah_linea_raw"] = fallback_data.get("ah_linea_raw", "N/A")
            if odds_info["goals_linea_raw"] in ["N/A", "-", ""]:
                odds_info["goals_linea_raw"] = fallback_data.get("goals_linea_raw", "N/A")
                
    return odds_info

def extract_standings_data_from_h2h_page_of(soup, team_name):
    data = {
        "name": team_name, "ranking": "N/A", "total_pj": "N/A", "total_v": "N/A",
        "total_e": "N/A", "total_d": "N/A", "total_gf": "N/A", "total_gc": "N/A",
        "specific_pj": "N/A", "specific_v": "N/A", "specific_e": "N/A",
        "specific_d": "N/A", "specific_gf": "N/A", "specific_gc": "N/A",
        "specific_type": "N/A"
    }
    if not soup or not team_name: return data
    standings_section = soup.find("div", id="porletP4")
    if not standings_section: return data
    team_table_soup = None
    is_home_table = False
    home_div = standings_section.find("div", class_="home-div")
    if home_div and team_name.lower() in home_div.get_text(strip=True).lower():
        team_table_soup = home_div.find("table", class_="team-table-home")
        is_home_table = True
        data["specific_type"] = "Est. como Local (en Liga)"
    else:
        guest_div = standings_section.find("div", class_="guest-div")
        if guest_div and team_name.lower() in guest_div.get_text(strip=True).lower():
            team_table_soup = guest_div.find("table", class_="team-table-guest")
            is_home_table = False
            data["specific_type"] = "Est. como Visitante (en Liga)"
    if not team_table_soup: return data
    header_link = team_table_soup.find("a")
    if header_link:
        full_text = header_link.get_text(separator=" ", strip=True)
        rank_match = re.search(r'\[.*?-(\d+)\]', full_text)
        if rank_match: data["ranking"] = rank_match.group(1)
    all_rows = team_table_soup.find_all("tr", align="center")
    is_ft_section = False
    for row in all_rows:
        header_cell = row.find("th")
        if header_cell:
            header_text = header_cell.get_text(strip=True)
            if "FT" in header_text: is_ft_section = True
            elif "HT" in header_text: is_ft_section = False
            continue
        if is_ft_section and len(cells := row.find_all("td")) >= 7:
            row_type_element = cells[0].find("span") or cells[0]
            row_type = row_type_element.get_text(strip=True)
            stats = [cell.get_text(strip=True) for cell in cells[1:7]]
            pj, v, e, d, gf, gc = stats
            if row_type == "Total":
                data.update({"total_pj": pj, "total_v": v, "total_e": e, "total_d": d, "total_gf": gf, "total_gc": gc})
            specific_row_needed = "Home" if is_home_table else "Away"
            if row_type == specific_row_needed:
                data.update({"specific_pj": pj, "specific_v": v, "specific_e": e, "specific_d": d, "specific_gf": gf, "specific_gc": gc})
    return data

def extract_over_under_stats_from_div_of(soup, team_type: str):
    default_stats = {"over_pct": 0, "under_pct": 0, "push_pct": 0, "total": 0}
    if not soup: return default_stats
    table_id = "table_v1" if team_type == 'home' else "table_v2"
    table = soup.find("table", id=table_id)
    if not table: return default_stats
    y_bar = table.find("ul", class_="y-bar")
    if not y_bar: return default_stats
    ou_group = None
    for group in y_bar.find_all("li", class_="group"):
        if "Over/Under Odds" in group.get_text():
            ou_group = group
            break
    if not ou_group: return default_stats
    try:
        total_text = ou_group.find("div", class_="tit").find("span").get_text(strip=True)
        total_match = re.search(r'\((\d+)\s*games\)', total_text)
        total = int(total_match.group(1)) if total_match else 0
        values = ou_group.find_all("span", class_="value")
        if len(values) == 3:
            over_pct_text = values[0].get_text(strip=True).replace('%', '')
            push_pct_text = values[1].get_text(strip=True).replace('%', '')
            under_pct_text = values[2].get_text(strip=True).replace('%', '')
            return {"over_pct": float(over_pct_text), "under_pct": float(under_pct_text), "push_pct": float(push_pct_text), "total": total}
    except (ValueError, TypeError, AttributeError):
        return default_stats
    return default_stats

def extract_final_score_of(soup):
    try:
        scores = soup.select('#mScore .end .score')
        if len(scores) == 2 and scores[0].text.strip().isdigit() and scores[1].text.strip().isdigit():
            hs, aws = scores[0].text.strip(), scores[1].text.strip()
            return f"{hs}:{aws}", f"{hs}-{aws}"
    except Exception: pass
    return '?:?', '?-?'

def extract_match_time_of(soup):
    """Extrae la hora del partido del HTML."""
    if not soup: return "N/A"
    try:
        # Buscar en el script matchInfo primero
        # Usamos una búsqueda más amplia para el script
        scripts = soup.find_all("script")
        for script in scripts:
            if script.string and "var _matchInfo" in script.string:
                match = re.search(r"mTime:\s*'([^']*)'", script.string)
                if match:
                    # Formato suele ser YYYY-MM-DD HH:MM:SS
                    full_time = match.group(1)
                    if ' ' in full_time:
                        return full_time.split(' ')[1][:5] # HH:MM
                    return full_time
                break
        
        # Fallback al HTML
        time_div = soup.find("div", class_="row", id="match_time")
        if time_div:
            return time_div.get_text(strip=True)
            
        # Fallback a un span con data-t (común en listas pero a veces en header)
        time_span = soup.find("span", attrs={"name": "timeData"})
        if time_span and time_span.get("data-t"):
             full_time = time_span.get("data-t")
             if ' ' in full_time:
                return full_time.split(' ')[1][:5]
             return full_time

    except Exception:
        pass
    return "N/A"


def extract_h2h_data_of(soup, home_name, away_name, league_id=None, odds_map=None):
    results = {'ah1': '-', 'res1': '?:?', 'res1_raw': '?-?', 'match1_id': None, 'ah6': '-', 'res6': '?:?', 'res6_raw': '?-?', 'match6_id': None, 'h2h_gen_home': "Local (H2H Gen)", 'h2h_gen_away': "Visitante (H2H Gen)"}
    if not soup or not home_name or not away_name or not (h2h_table := soup.find("table", id="table_v3")): return results
    all_matches = []
    for r in h2h_table.find_all("tr", id=re.compile(r"tr3_\d+")):
        if (d := get_match_details_from_row_of(r, score_class_selector='fscore_3', source_table_type='h2h', odds_map=odds_map)):
            if not league_id or (d.get('league_id_hist') and d.get('league_id_hist') == str(league_id)):
                all_matches.append(d)
    if not all_matches: return results
    all_matches.sort(key=lambda x: _parse_date_ddmmyyyy(x.get('date', '')), reverse=True)
    most_recent = all_matches[0]
    results.update({
        'ah6': most_recent.get('ahLine', '-'), 'res6': most_recent.get('score', '?:?'), 'res6_raw': most_recent.get('score_raw', '?-?'),
        'match6_id': most_recent.get('matchIndex'), 'h2h_gen_home': most_recent.get('home'), 'h2h_gen_away': most_recent.get('away'),
        'date': most_recent.get('date', 'N/A'), 'home_red': most_recent.get('home_red'), 'away_red': most_recent.get('away_red')
    })
    for d in all_matches:
        if d['home'].lower() == home_name.lower() and d['away'].lower() == away_name.lower():
            results.update({
                'ah1': d.get('ahLine', '-'), 'res1': d.get('score', '?:?'), 'res1_raw': d.get('score_raw', '?-?'),
                'match1_id': d.get('matchIndex'), 'date': d.get('date', 'N/A'), 'home_red': d.get('home_red'), 'away_red': d.get('away_red')
            })
            break
    return results

def extract_comparative_match_of(soup, table_id, main_team, opponent, league_id, is_home_table, odds_map=None):
    if not opponent or opponent == "N/A" or not main_team or not (table := soup.find("table", id=table_id)): return None
    score_selector = 'fscore_1' if is_home_table else 'fscore_2'
    for row in table.find_all("tr", id=re.compile(rf"tr{table_id[-1]}_\d+")):
        if not (details := get_match_details_from_row_of(row, score_class_selector=score_selector, source_table_type='hist', odds_map=odds_map)): continue
        if league_id and details.get('league_id_hist') and details.get('league_id_hist') != str(league_id): continue
        h, a = details.get('home','').lower(), details.get('away','').lower()
        main, opp = main_team.lower(), opponent.lower()
        if (main == h and opp == a) or (main == a and opp == h):
            return {
                "score": details.get('score', '?:?'), "ah_line": details.get('ahLine', '-'), "localia": 'H' if main == h else 'A',
                "home_team": details.get('home'), "away_team": details.get('away'), "match_id": details.get('matchIndex'),
                "date": details.get('date', 'N/A'), "home_red": details.get('home_red'), "away_red": details.get('away_red')
            }
    return None


def _load_main_match_soup(main_match_id: str):
    main_page_url = f"{BASE_URL_OF}/match/h2h-{main_match_id}"
    session = get_requests_session_of()
    response = session.get(main_page_url, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    return BeautifulSoup(response.text, "lxml")

from pathlib import Path
from modules.backtesting import BettingSimulator

def load_cached_finished_matches():
    """Carga los partidos finalizados desde data.json."""
    # Intentar localizar data.json en directorios padres
    candidates = [
        Path(__file__).resolve().parent.parent.parent / 'data.json', # src/modules/../.. -> root
        Path("C:/Users/Usuario/Desktop/V_buena/data.json") # Absolute fallback
    ]
    
    data_file = None
    for c in candidates:
        if c.exists():
            data_file = c
            break
            
    if not data_file:
        return []

    try:
        with open(data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get('finished_matches', [])
    except Exception as e:
        print(f"Error loading data.json: {e}")
        return []

def analizar_partido_completo(match_id: str, force_refresh: bool = False):
    main_match_id = "".join(filter(str.isdigit, str(match_id)))
    if not main_match_id:
        return {"error": "ID de partido inválido."}

    if not force_refresh:
        cached_payload = _get_cached_analysis(main_match_id)
        if cached_payload:
            return cached_payload

    start_time = time.time()
    try:
        soup_completo = _load_main_match_soup(main_match_id)
        home_id, away_id, league_id, home_name, away_name, league_name = get_team_league_info_from_script_of(soup_completo)
        home_standings = extract_standings_data_from_h2h_page_of(soup_completo, home_name)
        away_standings = extract_standings_data_from_h2h_page_of(soup_completo, away_name)
        home_ou_stats = extract_over_under_stats_from_div_of(soup_completo, 'home')
        away_ou_stats = extract_over_under_stats_from_div_of(soup_completo, 'away')
        key_match_id_rival_a, rival_a_id, rival_a_name = get_rival_a_for_original_h2h_of(soup_completo, league_id)
        _, rival_b_id, rival_b_name = get_rival_b_for_original_h2h_of(soup_completo, league_id)
        
        # Extraer mapa de cuotas históricas
        odds_map = extract_vs_odds(soup_completo)
        
        last_home_match = extract_last_match_in_league_of(soup_completo, "table_v1", home_name, league_id, True, odds_map)
        last_away_match = extract_last_match_in_league_of(soup_completo, "table_v2", away_name, league_id, False, odds_map)
        
        # Extraer listas de partidos recientes (Home vs Home, Away vs Away)
        recent_home_matches = extract_recent_matches(soup_completo, "table_v1", home_name, None, True, odds_map, limit=10)
        recent_away_matches = extract_recent_matches(soup_completo, "table_v2", away_name, None, False, odds_map, limit=10)
        
        h2h_data = extract_h2h_data_of(soup_completo, home_name, away_name, None, odds_map)
        comp_L_vs_UV_A = extract_comparative_match_of(soup_completo, "table_v1", home_name, (last_away_match or {}).get('home_team'), league_id, True, odds_map)
        comp_V_vs_UL_H = extract_comparative_match_of(soup_completo, "table_v2", away_name, (last_home_match or {}).get('away_team'), league_id, False, odds_map)
        main_match_odds_data = extract_bet365_initial_odds_of(soup_completo, main_match_id)
        final_score, _ = extract_final_score_of(soup_completo)
        match_time = extract_match_time_of(soup_completo)
        details_h2h_col3 = get_h2h_details_for_original_logic_of(
            key_match_id_rival_a, rival_a_id, rival_b_id, rival_a_name, rival_b_name
        )
        # --- Determinar Rivales Intencionados (para CSV aunque no haya match) ---
        rival_name_for_home_to_find = "N/A"
        if last_away_match:
            # El rival del Home Team para la comparativa es el equipo contra el que jugó el Away Team recientemente
            lat_home = last_away_match.get('home_team', '')
            lat_away = last_away_match.get('away_team', '')
            # Asumimos que away_name jugó ahí. Si away_name es home, rival es away.
            if away_name.lower() in lat_home.lower(): rival_name_for_home_to_find = lat_away
            else: rival_name_for_home_to_find = lat_home

        rival_name_for_away_to_find = "N/A"
        if last_home_match:
            lhm_home = last_home_match.get('home_team', '')
            lhm_away = last_home_match.get('away_team', '')
            if home_name.lower() in lhm_home.lower(): rival_name_for_away_to_find = lhm_away
            else: rival_name_for_away_to_find = lhm_home
        # ---------------------------------------------------------------------

        # --- Determinar Rivales para Comparativas Indirectas (si existen) ---
        if comp_L_vs_UV_A:
            # Home Team vs Rival. Find Rival.
            h_team = comp_L_vs_UV_A.get('home_team', '')
            a_team = comp_L_vs_UV_A.get('away_team', '')
            # Simple heuristic: The one that is NOT the home_name is the rival
            # Normalize for comparison
            hn_norm = home_name.lower().strip()
            if h_team.lower().strip() == hn_norm:
                comp_L_vs_UV_A['rival_name'] = a_team
            elif a_team.lower().strip() == hn_norm:
                comp_L_vs_UV_A['rival_name'] = h_team
            else:
                # Fallback: try partial match
                if hn_norm in h_team.lower(): comp_L_vs_UV_A['rival_name'] = a_team
                elif hn_norm in a_team.lower(): comp_L_vs_UV_A['rival_name'] = h_team
                else: comp_L_vs_UV_A['rival_name'] = "Rival Desconocido"

        if comp_V_vs_UL_H:
            # Away Team vs Rival.
            h_team = comp_V_vs_UL_H.get('home_team', '')
            a_team = comp_V_vs_UL_H.get('away_team', '')
            an_norm = away_name.lower().strip()
            if h_team.lower().strip() == an_norm:
                comp_V_vs_UL_H['rival_name'] = a_team
            elif a_team.lower().strip() == an_norm:
                comp_V_vs_UL_H['rival_name'] = h_team
            else:
                if an_norm in h_team.lower(): comp_V_vs_UL_H['rival_name'] = a_team
                elif an_norm in a_team.lower(): comp_V_vs_UL_H['rival_name'] = h_team
                else: comp_V_vs_UL_H['rival_name'] = "Rival Desconocido"
        # -----------------------------------------------------


        # --- GLOBAL BACKTESTING LOGIC ---
        simulator = BettingSimulator()
        
        # Parse current lines
        ah_actual_str = format_ah_as_decimal_string_of(main_match_odds_data.get('ah_linea_raw', '-'))
        ah_actual_num = parse_ah_to_number_of(ah_actual_str)
        
        goles_actual_str = format_ah_as_decimal_string_of(main_match_odds_data.get('goals_linea_raw', '-'))
        goles_actual_num = parse_ah_to_number_of(goles_actual_str)
        
        backtest_global = {"validez": False, "mensaje": "No hay línea AH/OU actual para simular."}

        if ah_actual_num is not None and goles_actual_num is not None:
            # 1. Cargar clones globales
            all_finished = load_cached_finished_matches()
            global_clones = []
            
            # Normalizar AH y OU actual para comparación
            target_ah_str = ah_actual_str
            target_ou_str = goles_actual_str
            
            for m in all_finished:
                # Normalizar handicap y goal_line del partido cacheado
                m_ah_raw = m.get('handicap')
                m_ou_raw = m.get('goal_line')
                
                if not m_ah_raw or not m_ou_raw: continue
                
                # Usamos la misma función de formateo para asegurar consistencia
                m_ah_str = format_ah_as_decimal_string_of(m_ah_raw)
                m_ou_str = format_ah_as_decimal_string_of(m_ou_raw)
                
                # CRITERIO DE PATRÓN ESTRICTO: AH + O/U deben coincidir
                if m_ah_str == target_ah_str and m_ou_str == target_ou_str:
                    # Es un clon!
                    clone_data = {
                        'score_raw': m.get('score'),
                        'match_id': m.get('id')
                    }
                    global_clones.append(clone_data)
            
            # 2. Simular
            if global_clones:
                backtest_global = simulator.simular_escenario_actual(
                    global_clones, ah_actual_num, goles_actual_num
                )
            else:
                backtest_global = {"validez": False, "mensaje": f"No se encontraron clones con Patrón AH {target_ah_str} + O/U {target_ou_str}."}
        # -------------------------

    except Exception as exc:
        return {"error": f"Error durante el análisis: {exc}"}

    market_analysis_html, market_analysis_data = generar_analisis_completo_mercado(main_match_odds_data, h2h_data, home_name, away_name)
    historical_matches_html = _build_historical_matches_list_html(recent_home_matches, recent_away_matches, home_name, away_name)

    def get_stats_rows(match_id_value):
        if not match_id_value:
            return []
        df = get_match_progression_stats_data(str(match_id_value))
        return _df_to_rows(df)

    last_home_match_stats = get_stats_rows((last_home_match or {}).get('match_id'))
    last_away_match_stats = get_stats_rows((last_away_match or {}).get('match_id'))
    h2h_col3_stats = get_stats_rows((details_h2h_col3 or {}).get('match_id'))
    comp_L_vs_UV_A_stats = get_stats_rows((comp_L_vs_UV_A or {}).get('match_id'))
    comp_V_vs_UL_H_stats = get_stats_rows((comp_V_vs_UL_H or {}).get('match_id'))
    h2h_stadium_stats = get_stats_rows(h2h_data.get('match1_id'))
    h2h_general_stats = get_stats_rows(h2h_data.get('match6_id'))

    results = {
        "match_id": main_match_id,
        "home_name": home_name,
        "away_name": away_name,
        "league_name": league_name,
        "final_score": final_score,
        "time": match_time,
        "home_standings": home_standings,
        "away_standings": away_standings,
        "home_ou_stats": home_ou_stats,
        "away_ou_stats": away_ou_stats,
        "main_match_odds": {
            "ah_linea": format_ah_as_decimal_string_of(main_match_odds_data.get('ah_linea_raw', '?')),
            "goals_linea": format_ah_as_decimal_string_of(main_match_odds_data.get('goals_linea_raw', '?'))
        },
        "market_analysis_html": market_analysis_html,
        "market_analysis_data": market_analysis_data,
        "historical_matches_html": historical_matches_html,
        "last_home_match": {**last_home_match, "stats_rows": last_home_match_stats} if last_home_match else None,
        "last_away_match": {**last_away_match, "stats_rows": last_away_match_stats} if last_away_match else None,
        "h2h_col3": {
            **details_h2h_col3,
            "stats_rows": h2h_col3_stats
        } if details_h2h_col3 else None,
        
        "comparativas_indirectas": {
            "left": {
                **(comp_L_vs_UV_A if comp_L_vs_UV_A else {}),
                "stats_rows": comp_L_vs_UV_A_stats if comp_L_vs_UV_A else None,
                "title_home_name": home_name,
                "title_away_name": away_name,
                "rival_name": comp_L_vs_UV_A.get('rival_name') if comp_L_vs_UV_A else rival_name_for_home_to_find
            },
            "right": {
                **(comp_V_vs_UL_H if comp_V_vs_UL_H else {}),
                "stats_rows": comp_V_vs_UL_H_stats if comp_V_vs_UL_H else None,
                "title_home_name": home_name,
                "title_away_name": away_name,
                "rival_name": comp_V_vs_UL_H.get('rival_name') if comp_V_vs_UL_H else rival_name_for_away_to_find
            }
        },

        "h2h_stadium": {**h2h_data, "stats_rows": h2h_stadium_stats},
        "h2h_general": {**h2h_data, "stats_rows": h2h_general_stats},
        "backtest_global": backtest_global,
        "execution_time_seconds": round(time.time() - start_time, 2),
    }

    _set_cached_analysis(main_match_id, results)
    return copy.deepcopy(results)
