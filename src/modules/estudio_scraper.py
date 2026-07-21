# src/modules/estudio_scraper.py

import time
import copy
import os
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import re
import json
import math
import threading
from contextlib import contextmanager
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from pathlib import Path
from . import sql_store
from .red_cards import extract_red_card_count_from_cell, normalize_red_card_stats_payload
# Selenium imports removed
SELENIUM_AVAILABLE = False

# Playwright support for dynamic content
try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False


# --- CONFIGURACIÓN GLOBAL ---
BASE_URL_OF = os.getenv("NOWGOAL_BASE_URL", "https://www.nowgoal26.com").rstrip("/")
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

    
    # Formateo mejorado en dos filas
    score_fmt = res_raw.replace('-', ':')
    
    # Fila 1: Resultado y Equipos
    row1_html = (
        f"<div style='margin-bottom: 2px;'>"
        f"  <span style='color: #2563eb; font-weight: bold;'>{home_team_precedente}</span> "
        f"  <span style='font-weight: bold; padding: 0 4px;'>{score_fmt}</span> "
        f"  <span style='color: #f97316; font-weight: bold;'>{away_team_precedente}</span>"
        f"</div>"
    )

    # Fila 2: Análisis de cuota
    row2_html = (
        f"<div>"
        f"  <span class='ah-value'>Hándicap:</span> {comparativa_texto} "
        f"  La línea actual habría sido {cover_html}."
        f"</div>"
    )

    return {
        "html": f"<li style='margin-bottom: 8px;'>{row1_html}{row2_html}</li>",
        "movement": line_movement_str if 'line_movement_str' in locals() else "N/A",
        "result": score_fmt,
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

def identificar_boss_bot_h2h_col3(h2h_col3_data):
    """
    Identifica qué equipo es BOSS (fuerte) y cuál es BOT (débil) según el resultado del H2H Col3.
    
    Args:
        h2h_col3_data: Diccionario con datos del H2H Col3 (goles_home, goles_away, h2h_home_team_name, h2h_away_team_name)
    
    Returns:
        dict: {"boss_name": str, "bot_name": str, "tipo": "clara"|"empate"}
    """
    if not h2h_col3_data or h2h_col3_data.get('status') != 'found':
        return {"boss_name": None, "bot_name": None, "tipo": "sin_datos"}
    
    try:
        goles_h = int(h2h_col3_data.get('goles_home', 0))
        goles_a = int(h2h_col3_data.get('goles_away', 0))
        home_name = h2h_col3_data.get('h2h_home_team_name', '')
        away_name = h2h_col3_data.get('h2h_away_team_name', '')
        
        if goles_h > goles_a:
            return {"boss_name": home_name, "bot_name": away_name, "tipo": "clara"}
        elif goles_a > goles_h:
            return {"boss_name": away_name, "bot_name": home_name, "tipo": "clara"}
        else:
            return {"boss_name": None, "bot_name": None, "tipo": "empate"}
    except (ValueError, TypeError):
        return {"boss_name": None, "bot_name": None, "tipo": "error"}

def check_cobertura_ah(partido_data, equipo_analizado_name, main_home_name):
    """
    Verifica si un equipo CUBRIÓ o FALLÓ el handicap en un partido específico.
    
    Args:
        partido_data: Diccionario con datos del partido (score, handicap_line_raw, home_team, away_team)
        equipo_analizado_name: Nombre del equipo que estamos analizando
        main_home_name: Nombre del equipo local del partido principal (para determinar favorito)
    
    Returns:
        bool|None: True si CUBRIÓ, False si FALLÓ, None si PUSH o sin datos
    """
    if not partido_data:
        return None
    
    score = partido_data.get('score', '')
    ah_raw = partido_data.get('handicap_line_raw', '')
    home_team = partido_data.get('home_team', '')
    away_team = partido_data.get('away_team', '')
    
    if not score or not ah_raw or ah_raw in ['N/A', '-', '']:
        return None
    
    # Parsear el score (formato puede ser "1:2" o "1-2")
    score_clean = score.replace(':', '-')
    
    # Parsear el AH
    ah_num = parse_ah_to_number_of(ah_raw)
    if ah_num is None:
        return None
    
    # Determinar quién es favorito según el AH
    # AH positivo = Local favorito, AH negativo = Visitante favorito
    if ah_num > 0:
        favorito_name = home_team
    elif ah_num < 0:
        favorito_name = away_team
    else:
        favorito_name = home_team  # Si AH=0, consideramos empate, pero usamos local como referencia
    
    # Usar la función existente check_handicap_cover
    result_text, is_covered = check_handicap_cover(
        resultado_raw=score_clean,
        ah_line_num=abs(ah_num),
        favorite_team_name=favorito_name,
        home_team_in_h2h=home_team,
        away_team_in_h2h=away_team,
        main_home_team_name=main_home_name
    )
    
    return is_covered

def analizar_triangulacion(h2h_col3_data, prev_home_data, prev_away_data, main_home_name, main_away_name, current_ah):
    """
    Analiza la triangulación completa según la matriz de 13 casos de ANTIGRAVITY V3.0.
    
    Args:
        h2h_col3_data: Datos del H2H Col3
        prev_home_data: Datos del partido previo del local
        prev_away_data: Datos del partido previo del visitante
        main_home_name: Nombre del equipo local actual
        main_away_name: Nombre del equipo visitante actual
        current_ah: Handicap actual del partido
    
    Returns:
        dict: Diagnóstico completo con caso, señal, prioridad, recomendación
    """
    # Si no hay datos de H2H Col3, retornar N/A
    if not h2h_col3_data or h2h_col3_data.get('status') != 'found':
        return {
            "caso": None,
            "diagnostico": "Sin datos H2H Col3",
            "senal": "-",
            "prioridad": "N/A",
            "recomendacion_ah": "-",
            "explicacion": "No hay datos suficientes para análisis de triangulación",
            "etiqueta_inversa": False
        }
    
    # Identificar BOSS y BOT
    boss_bot = identificar_boss_bot_h2h_col3(h2h_col3_data)
    
    # Si fue empate, aplicar lógica de Tabla 3 (contexto INVERSA)
    if boss_bot['tipo'] == 'empate':
        return _analizar_caso_inversa(h2h_col3_data, prev_home_data, prev_away_data, main_home_name, main_away_name)
    
    # Determinar quién es favorito y underdog en el partido actual
    ah_num = parse_ah_to_number_of(current_ah) if current_ah else 0
    if ah_num is None:
        ah_num = 0
    
    if ah_num > 0:
        favorito_actual = main_home_name
        underdog_actual = main_away_name
    elif ah_num < 0:
        favorito_actual = main_away_name
        underdog_actual = main_home_name
    else:
        favorito_actual = main_home_name  # Empate técnico, usamos local
        underdog_actual = main_away_name
    
    # Determinar quién jugó contra quién en los partidos previos
    # Necesitamos saber si el favorito jugó contra BOSS o BOT, y lo mismo para el underdog
    
    # Para el favorito: verificar si jugó contra BOSS o BOT
    fav_prev_data = prev_home_data if favorito_actual == main_home_name else prev_away_data
    dog_prev_data = prev_away_data if underdog_actual == main_away_name else prev_home_data
    
    if not fav_prev_data or not dog_prev_data:
        return {
            "caso": None,
            "diagnostico": "Datos incompletos",
            "senal": "-",
            "prioridad": "N/A",
            "recomendacion_ah": "-",
            "explicacion": "No hay datos de partidos previos",
            "etiqueta_inversa": False
        }
    
    # Identificar contra quién jugó cada uno
    fav_rival = fav_prev_data.get('away_team') if favorito_actual == main_home_name else fav_prev_data.get('home_team')
    dog_rival = dog_prev_data.get('away_team') if underdog_actual == main_away_name else dog_prev_data.get('home_team')
    
    # Verificar si jugaron contra el mismo rival (Cruce Simétrico) o diferentes (Cruce Asimétrico)
    mismo_rival = fav_rival and dog_rival and fav_rival.lower() == dog_rival.lower()
    
    # Verificar cobertura de cada uno
    fav_cubrio = check_cobertura_ah(fav_prev_data, favorito_actual, main_home_name)
    dog_cubrio = check_cobertura_ah(dog_prev_data, underdog_actual, main_home_name)
    
    if mismo_rival:
        # TABLA 2: Cruce Simétrico
        return _analizar_caso_simetrico(fav_cubrio, dog_cubrio, fav_rival, boss_bot, current_ah, favorito_actual)
    else:
        # TABLA 1: Cruce Asimétrico
        return _analizar_caso_asimetrico(fav_cubrio, dog_cubrio, fav_rival, dog_rival, boss_bot, current_ah, favorito_actual)

def _analizar_caso_asimetrico(fav_cubrio, dog_cubrio, fav_rival, dog_rival, boss_bot, current_ah, favorito_actual):
    """Analiza casos de la Tabla 1: Cruce Asimétrico"""
    boss = boss_bot.get('boss_name', '').lower()
    bot = boss_bot.get('bot_name', '').lower()
    
    fav_rival_lower = fav_rival.lower() if fav_rival else ''
    dog_rival_lower = dog_rival.lower() if dog_rival else ''
    
    fav_jugo_vs_boss = boss in fav_rival_lower
    dog_jugo_vs_boss = boss in dog_rival_lower
    fav_jugo_vs_bot = bot in fav_rival_lower
    dog_jugo_vs_bot = bot in dog_rival_lower
    
    # Caso #1: LA APLANADORA - Fav vs BOSS CUBRIÓ + Dog vs BOT FALLÓ
    if fav_jugo_vs_boss and dog_jugo_vs_bot and fav_cubrio == True and dog_cubrio == False:
        return {
            "caso": 1,
            "diagnostico": "LA APLANADORA",
            "senal": "🚀",
            "prioridad": "MAXIMA",
            "recomendacion_ah": f"Favorito -1.5",
            "explicacion": "El Favorito resistió al Jefe; el Dog no pudo con el Empleado. Diferencia abismal.",
            "etiqueta_inversa": False
        }
    
    # Caso #2: VALOR OCULTO - Fav vs BOSS FALLÓ (poco) + Dog vs BOT FALLÓ
    if fav_jugo_vs_boss and dog_jugo_vs_bot and fav_cubrio == False and dog_cubrio == False:
        return {
            "caso": 2,
            "diagnostico": "VALOR OCULTO",
            "senal": "🛡️",
            "prioridad": "MEDIA",
            "recomendacion_ah": f"Favorito -0.5",
            "explicacion": "El Fav perdió con la élite. El Dog es mediocre. El mercado castiga al Fav injustamente.",
            "etiqueta_inversa": False
        }
    
    # Caso #3: LA TRAMPA MORTAL - Fav vs BOT FALLÓ + Dog vs BOSS CUBRIÓ
    if fav_jugo_vs_bot and dog_jugo_vs_boss and fav_cubrio == False and dog_cubrio == True:
        return {
            "caso": 3,
            "diagnostico": "LA TRAMPA MORTAL",
            "senal": "💣",
            "prioridad": "MAXIMA",
            "recomendacion_ah": f"Lay Fav / Gana Dog",
            "explicacion": "El Fav falló la prueba fácil. El Dog pasó la difícil. El Fav es mentira.",
            "etiqueta_inversa": False
        }
    
    # Caso #4: LÓGICA ESTÁNDAR - Fav vs BOT CUBRIÓ + Dog vs BOSS FALLÓ
    if fav_jugo_vs_bot and dog_jugo_vs_boss and fav_cubrio == True and dog_cubrio == False:
        return {
            "caso": 4,
            "diagnostico": "LÓGICA ESTÁNDAR",
            "senal": "⚖️",
            "prioridad": "BAJA",
            "recomendacion_ah": f"Pass / Fav -0.25",
            "explicacion": "El Fav cumplió, el Dog falló. No hay mucho valor, cuota justa.",
            "etiqueta_inversa": False
        }
    
    # Caso #5: CHOQUE DE TRENES - Ambos CUBRIERON contra rivales de diferente nivel
    if fav_cubrio == True and dog_cubrio == True:
        return {
            "caso": 5,
            "diagnostico": "CHOQUE DE TRENES",
            "senal": "✅",
            "prioridad": "MEDIA",
            "recomendacion_ah": f"Favorito Win",
            "explicacion": "Ambos vienen en buena dinámica relativa. El Fav es mejor por calidad de rival.",
            "etiqueta_inversa": False
        }
    
    # Caso #6: CHOQUE DE COJOS - Ambos FALLARON
    if fav_cubrio == False and dog_cubrio == False:
        return {
            "caso": 6,
            "diagnostico": "CHOQUE DE COJOS",
            "senal": "🗑️",
            "prioridad": "SKIP",
            "recomendacion_ah": "SKIP (NO BET)",
            "explicacion": "Ambos lo hicieron mal. Partido de azar puro.",
            "etiqueta_inversa": False
        }
    
    # Caso por defecto
    return {
        "caso": None,
        "diagnostico": "Sin clasificar",
        "senal": "-",
        "prioridad": "N/A",
        "recomendacion_ah": "-",
        "explicacion": "Combinación no catalogada en matriz asimétrica",
        "etiqueta_inversa": False
    }

def _analizar_caso_simetrico(fav_cubrio, dog_cubrio, rival_comun, boss_bot, current_ah, favorito_actual):
    """Analiza casos de la Tabla 2: Cruce Simétrico"""
    boss = boss_bot.get('boss_name', '').lower()
    rival_lower = rival_comun.lower() if rival_comun else ''
    
    jugaron_vs_boss = boss in rival_lower
    
    # Caso #7: JERARQUÍA CLARA - Ambos vs BOSS, Fav CUBRIÓ, Dog FALLÓ
    if jugaron_vs_boss and fav_cubrio == True and dog_cubrio == False:
        return {
            "caso": 7,
            "diagnostico": "JERARQUÍA CLARA",
            "senal": "✅",
            "prioridad": "ALTA",
            "recomendacion_ah": f"Favorito -0.75",
            "explicacion": "Ante la máxima exigencia, solo el Fav respondió.",
            "etiqueta_inversa": False
        }
    
    # Caso #8: FALSO FAVORITO - Ambos vs BOSS, Fav FALLÓ, Dog CUBRIÓ
    if jugaron_vs_boss and fav_cubrio == False and dog_cubrio == True:
        return {
            "caso": 8,
            "diagnostico": "FALSO FAVORITO",
            "senal": "💎",
            "prioridad": "MAXIMA",
            "recomendacion_ah": f"Gana Dog (+AH)",
            "explicacion": "El Dog jugó mejor contra el equipo fuerte.",
            "etiqueta_inversa": False
        }
    
    # Caso #9: CUMPLIMIENTO - Ambos vs BOT, Fav CUBRIÓ, Dog FALLÓ
    if not jugaron_vs_boss and fav_cubrio == True and dog_cubrio == False:
        return {
            "caso": 9,
            "diagnostico": "CUMPLIMIENTO",
            "senal": "🚀",
            "prioridad": "ALTA",
            "recomendacion_ah": f"Favorito -1.0",
            "explicacion": "El Fav gana a los malos, el Dog sufre con los malos.",
            "etiqueta_inversa": False
        }
    
    # Caso #10: INCOMPETENCIA - Ambos vs BOT, Fav FALLÓ, Dog CUBRIÓ
    if not jugaron_vs_boss and fav_cubrio == False and dog_cubrio == True:
        return {
            "caso": 10,
            "diagnostico": "INCOMPETENCIA",
            "senal": "💣",
            "prioridad": "MAXIMA",
            "recomendacion_ah": f"Lay Fav",
            "explicacion": "El Fav no gana ni a los malos. El Dog sí.",
            "etiqueta_inversa": False
        }
    
    # Caso por defecto
    return {
        "caso": None,
        "diagnostico": "Sin clasificar",
        "senal": "-",
        "prioridad": "N/A",
        "recomendacion_ah": "-",
        "explicacion": "Combinación no catalogada en matriz simétrica",
        "etiqueta_inversa": False
    }

def _analizar_caso_inversa(h2h_col3_data, prev_home_data, prev_away_data, main_home_name, main_away_name):
    """Analiza casos de la Tabla 3: Contexto INVERSA (cuando H2H Col3 fue empate)"""
    # Verificar si ambos empataron
    try:
        ph_score = prev_home_data.get('score', '').replace(':', '-')
        pa_score = prev_away_data.get('score', '').replace(':', '-')
        
        ph_goles = ph_score.split('-') if '-' in ph_score else []
        pa_goles = pa_score.split('-') if '-' in pa_score else []
        
        ambos_empataron = False
        if len(ph_goles) == 2 and len(pa_goles) == 2:
            ph_h, ph_a = int(ph_goles[0]), int(ph_goles[1])
            pa_h, pa_a = int(pa_goles[0]), int(pa_goles[1])
            ambos_empataron = (ph_h == ph_a) and (pa_h == pa_a)
        
        # Caso #11: BLOQUEO DEFENSIVO - Ambos empataron
        if ambos_empataron:
            return {
                "caso": 11,
                "diagnostico": "BLOQUEO DEFENSIVO",
                "senal": "🧊",
                "prioridad": "MEDIA",
                "recomendacion_ah": "UNDER 2.5",
                "explicacion": "Nadie tiene ventaja real. El mercado espera pocos goles.",
                "etiqueta_inversa": True
            }
    except (ValueError, TypeError, AttributeError):
        pass
    
    # Para casos #12 y #13 necesitaría más contexto sobre quién ganó/perdió y si jugaron local/fuera
    # Por ahora retornamos un caso genérico de INVERSA
    return {
        "caso": None,
        "diagnostico": "INVERSA (Empate H2H)",
        "senal": "⚖️",
        "prioridad": "BAJA",
        "recomendacion_ah": "-",
        "explicacion": "El H2H Col3 fue empate. Análisis de patrón inverso requiere más datos.",
        "etiqueta_inversa": True
    }

def generar_analisis_completo_mercado(main_odds, h2h_data, home_name, away_name, prev_home_data=None, prev_away_data=None):
    ah_actual_str = format_ah_as_decimal_string_of(main_odds.get('ah_linea_raw', '-'))
    ah_actual_num = parse_ah_to_number_of(ah_actual_str)
    goles_actual_num = parse_ah_to_number_of(main_odds.get('goals_linea_raw', '-'))

    favorito_name, favorito_html = "Ninguno (línea en 0)", "Ninguno (línea en 0)"
    if ah_actual_num is not None:
        if ah_actual_num < 0:
            # AH Negativo significa que el VISITANTE es favorito (según lógica Nowgoal/Feedback)
            favorito_name, favorito_html = away_name, f"<span class='away-color'>{away_name}</span>"
        elif ah_actual_num > 0:
            # AH Positivo significa que el LOCAL es favorito
            favorito_name, favorito_html = home_name, f"<span class='home-color'>{home_name}</span>"
    
    titulo_html = f"<p style='margin-bottom: 12px;'><strong>📊 Análisis de Mercado vs. Histórico H2H</strong><br><span style='font-style: italic; font-size: 0.9em;'>Líneas actuales: AH {ah_actual_str} / Goles {main_odds.get('goals_linea_raw', '-')} | Favorito: {favorito_html}</span></p>"

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
    
    # Extraer datos del H2H Col3
    h2h_col3_data = h2h_data.get('col3_data', {})
    
    # Analizar triangulación si hay datos disponibles
    triangulacion_data = analizar_triangulacion(
        h2h_col3_data=h2h_col3_data,
        prev_home_data=prev_home_data,
        prev_away_data=prev_away_data,
        main_home_name=home_name,
        main_away_name=away_name,
        current_ah=main_odds.get('ah_linea_raw', '')
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
            "is_covered": sintesis_ah_estadio_data.get("is_covered"),
            "date": h2h_data.get('date1')
        },
        "general": {
            "movement": sintesis_ah_general_data.get("movement") if 'sintesis_ah_general_data' in locals() else (sintesis_ah_estadio_data.get("movement") if precedente_estadio['match_id'] == precedente_general_id else "N/A"),
            "result": sintesis_ah_general_data.get("result") if 'sintesis_ah_general_data' in locals() else (sintesis_ah_estadio_data.get("result") if precedente_estadio['match_id'] == precedente_general_id else "N/A"),
            "evaluation": sintesis_ah_general_data.get("evaluation") if 'sintesis_ah_general_data' in locals() else (sintesis_ah_estadio_data.get("evaluation") if precedente_estadio['match_id'] == precedente_general_id else "N/A"),
            "is_covered": sintesis_ah_general_data.get("is_covered") if 'sintesis_ah_general_data' in locals() else (sintesis_ah_estadio_data.get("is_covered") if precedente_estadio['match_id'] == precedente_general_id else None),
            "date": h2h_data.get('date6') if 'sintesis_ah_general_data' in locals() else (h2h_data.get('date1') if precedente_estadio['match_id'] == precedente_general_id else "N/A")
        },
        "triangulacion": triangulacion_data
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
        
        home, away = get_cell_txt(home_idx), get_cell_txt(away_idx)
        home_red = extract_red_card_count_from_cell(cells[home_idx])
        away_red = extract_red_card_count_from_cell(cells[away_idx])
        links = row_element.find_all("a", onclick=True)
        team_ids = []
        for link in links[:2]:
            id_match = re.search(r"team\((\d+)\)", link.get("onclick", ""))
            team_ids.append(id_match.group(1) if id_match else None)
        home_id = team_ids[0] if len(team_ids) > 0 else None
        away_id = team_ids[1] if len(team_ids) > 1 else None

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
            'home_id': home_id, 'away_id': away_id,
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
        response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS, verify=False)
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
    if not soup or not (table := soup.find("table", id="table_v1")): return None, None, None, False
    
    rival_diff_league = None
    
    for row in table.find_all("tr", id=re.compile(r"tr1_\d+")):
        if row.get("vs") == "1" and (key_id := row.get("index")):
            onclicks = row.find_all("a", onclick=True)
            if len(onclicks) > 1 and (rival_tag := onclicks[1]) and (rival_id_match := re.search(r"team\((\d+)\)", rival_tag.get("onclick", ""))):
                res = (key_id, rival_id_match.group(1), rival_tag.text.strip())
                
                # Si coincide la liga o no se especificó liga, es el ideal
                if not league_id or row.get("name") == str(league_id):
                    return res[0], res[1], res[2], False
                else:
                    # Guardamos el primero de otra liga como fallback
                    if not rival_diff_league:
                        rival_diff_league = res
                        
    if rival_diff_league:
        return rival_diff_league[0], rival_diff_league[1], rival_diff_league[2], True
    return None, None, None, False

def get_rival_b_for_original_h2h_of(soup, league_id=None):
    if not soup or not (table := soup.find("table", id="table_v2")): return None, None, None, False
    
    rival_diff_league = None
    
    for row in table.find_all("tr", id=re.compile(r"tr2_\d+")):
        if row.get("vs") == "1" and (key_id := row.get("index")):
            onclicks = row.find_all("a", onclick=True)
            if len(onclicks) > 0 and (rival_tag := onclicks[0]) and (rival_id_match := re.search(r"team\((\d+)\)", rival_tag.get("onclick", ""))):
                res = (key_id, rival_id_match.group(1), rival_tag.text.strip())
                
                if not league_id or row.get("name") == str(league_id):
                    return res[0], res[1], res[2], False
                else:
                    if not rival_diff_league:
                        rival_diff_league = res
                        
    if rival_diff_league:
        return rival_diff_league[0], rival_diff_league[1], rival_diff_league[2], True
    return None, None, None, False

def get_h2h_details_for_original_logic_of(key_match_id, rival_a_id, rival_b_id, rival_a_name="Rival A", rival_b_name="Rival B"):
    if not all([key_match_id, rival_a_id, rival_b_id]):
        return {"status": "error", "resultado": "N/A (Datos incompletos para H2H)"}
    
    url = f"{BASE_URL_OF}/match/h2h-{key_match_id}"
    try:
        session = get_requests_session_of()
        response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS, verify=False)
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

            # Assuming home team is in cell 2 (index 2) and away in cell 4 (index 4) based on typical layout
            # But we need to be careful about which link corresponds to which team.
            # The links list has [home_link, away_link].
            # Let's try to find the parent td for each link to check for red cards.
            home_red = extract_red_card_count_from_cell(links[0].find_parent('td'))
            away_red = extract_red_card_count_from_cell(links[1].find_parent('td'))

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

def extract_recent_matches(soup, table_id, team_name, league_id, is_home_game, odds_map=None, limit=5, is_neutral_venue=False):
    """
    Extrae una lista de los últimos partidos del equipo en esa condición (Local/Visitante).
    Retorna una lista de diccionarios con detalles del partido.
    Si is_neutral_venue=True, ignora la condición is_home_game y trae partidos donde el equipo sea Local O Visitante.
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
        should_include = False
        if is_neutral_venue:
            # Si es neutro, solo verificamos que el equipo esté involucrado (local o visitante)
            should_include = is_team_home or is_team_away
        else:
            # Comportamiento estándar: Home vs Home, Away vs Away
            should_include = (is_home_game and is_team_home) or (not is_home_game and is_team_away)
        
        if should_include:
            matches.append(details)
            
    # Ordenar por fecha descendente
    matches.sort(key=lambda x: _parse_date_ddmmyyyy(x.get('date', '')), reverse=True)
    
    return matches[:limit]

def extract_last_match_in_league_of(soup, table_id, team_name, league_id, is_home_game, odds_map=None, is_neutral_venue=False):
    # Reutilizamos la nueva función pero limitamos a 20 para buscar la liga
    matches = extract_recent_matches(soup, table_id, team_name, league_id, is_home_game, odds_map, limit=20, is_neutral_venue=is_neutral_venue)
    
    is_diff_league = False
    filtered_matches = []
    if league_id:
        filtered_matches = [m for m in matches if m.get("league_id_hist") == str(league_id)]
        
    if not filtered_matches:
        # Fallback: si no hay en esta liga, tomamos el más reciente de cualquier liga (de los 20 extraídos)
        if matches:
            last_match = matches[0]
            is_diff_league = True
        else:
            return None
    else:
        last_match = filtered_matches[0]
    
    return {
        "date": last_match.get('date', 'N/A'), "home_team": last_match.get('home'),
        "away_team": last_match.get('away'), "score": last_match.get('score_raw', 'N/A').replace('-', ':'),
        "handicap_line_raw": last_match.get('ahLine_raw', 'N/A'), "match_id": last_match.get('matchIndex'),
        "is_different_league": is_diff_league
    }

def fetch_odds_from_bf_data(match_id):
    """
    Fallback para obtener líneas de hándicap y goles desde bf_en-idn.js
    cuando no están disponibles en el HTML principal.
    """
    url = f"{BASE_URL_OF}/gf/data/bf_en-idn.js"
    try:
        session = get_requests_session_of()
        response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS, verify=False)
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
        response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS, verify=False)
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
                            # Priorizar índice 8 (Main AH) sobre índice 4 (Secondary AH)
                            ah_val = vals[8] if vals[8] else vals[4]
                            
                            if ah_val and vals[12]:
                                target_odds = {
                                    "ah_linea_raw": ah_val,
                                    "goals_linea_raw": vals[12]
                                }
                                # print(f"Found odds for ID '{pid}': {target_odds}")
                                break
                    if target_odds: break
            if target_odds: break
            
        return target_odds

    except Exception as e:
        print(f"Error fetching AJAX odds: {e}")
        return None


def fetch_odds_with_playwright(match_id: str):
    """
    Fallback usando Playwright para cargar cuotas que requieren JavaScript.
    Obtiene las cuotas iniciales de Bet365 desde el liveCompareDiv.
    """
    if not PLAYWRIGHT_AVAILABLE:
        return None
    
    odds_info = None
    url = f"{BASE_URL_OF}/match/h2h-{match_id}"
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080},
            )
            page = context.new_page()
            
            # Eliminar señales de webdriver
            page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            page.goto(url, wait_until="networkidle", timeout=30000)
            page.wait_for_timeout(5000)  # Esperar carga de JS dinámico
            
            # Buscar fila Bet365 Initial
            bet365_row = page.query_selector("tr#tr_o_1_8[name='earlyOdds']")
            if not bet365_row:
                bet365_row = page.query_selector("tr#tr_o_1_31[name='earlyOdds']")  # Sbobet fallback
            
            if bet365_row:
                tds = bet365_row.query_selector_all("td")
                if len(tds) >= 11:
                    odds_info = {
                        "ah_linea_raw": tds[3].get_attribute("data-o") or tds[3].inner_text(),
                        "goals_linea_raw": tds[9].get_attribute("data-o") or tds[9].inner_text(),
                    }
            
            browser.close()
            
    except Exception as e:
        print(f"Playwright fetch_odds error: {e}")
    
    return odds_info


def extract_bet365_initial_odds_of(soup, match_id=None):
    odds_info = {
        "ah_home_cuota": "N/A", "ah_linea_raw": "N/A", "ah_away_cuota": "N/A",
        "goals_over_cuota": "N/A", "goals_linea_raw": "N/A", "goals_under_cuota": "N/A"
    }
    
    if soup:
        bet365_row = None
        
        # 1. PRIORIDAD: Buscar fila "Initial" (earlyOdds) de Bet365 con selectores CSS específicos
        # Bet365 tiene ID 8, Sbobet tiene ID 31
        bet365_row = soup.select_one("tr#tr_o_1_8[name='earlyOdds']")
        if not bet365_row:
            bet365_row = soup.select_one("tr#tr_o_1_31[name='earlyOdds']")  # Fallback a Sbobet
        
        # 2. Si falla, buscar por texto "Bet365" dentro de <b> o <td>
        if not bet365_row:
            bet365_b = soup.find("b", string=lambda text: text and "Bet365" in text)
            if bet365_b:
                # Encontrar el padre <tr> y buscar la fila con earlyOdds (la fila "Initial")
                parent_tr = bet365_b.find_parent("tr")
                if parent_tr and parent_tr.get("name") == "earlyOdds":
                    bet365_row = parent_tr
            
            # Si todavía no hay, buscar cualquier fila earlyOdds que contenga Bet365
            if not bet365_row:
                for row in soup.find_all("tr", attrs={"name": "earlyOdds"}):
                    if row.find("b", string=lambda t: t and "Bet365" in t):
                        bet365_row = row
                        break

        # 3. Si encontramos la fila, extraemos los datos
        if bet365_row:
            tds = bet365_row.find_all("td")
            # La estructura es: [Bet365, Initial, AH_Home, AH_Line, AH_Away, 1X2_H, 1X2_D, 1X2_A, OU_Over, OU_Line, OU_Under, Trends]
            # Índices: 0=Bet365, 1=Initial, 2=AH_Home, 3=AH_Line, 4=AH_Away, 5-7=1X2, 8=OU_Over, 9=OU_Line, 10=OU_Under
            if len(tds) >= 11:
                odds_info["ah_home_cuota"] = tds[2].get("data-o", tds[2].get_text(strip=True)) or "N/A"
                odds_info["ah_linea_raw"] = tds[3].get("data-o", tds[3].get_text(strip=True)) or "N/A"
                odds_info["ah_away_cuota"] = tds[4].get("data-o", tds[4].get_text(strip=True)) or "N/A"
                odds_info["goals_over_cuota"] = tds[8].get("data-o", tds[8].get_text(strip=True)) or "N/A"
                odds_info["goals_linea_raw"] = tds[9].get("data-o", tds[9].get_text(strip=True)) or "N/A"
                odds_info["goals_under_cuota"] = tds[10].get("data-o", tds[10].get_text(strip=True)) or "N/A"
        
        # 3b. FALLBACK 18Bet: Si el AH de Bet365/Sbobet es "-" o vacío, intentar con 18Bet (ID 42)
        if odds_info["ah_linea_raw"] in ["N/A", "-", "", None]:
            bet18_row = soup.select_one("tr#tr_o_1_42[name='earlyOdds']")
            if bet18_row:
                tds_18 = bet18_row.find_all("td")
                if len(tds_18) >= 11:
                    ah_18 = tds_18[3].get("data-o", tds_18[3].get_text(strip=True)) or ""
                    # Solo usamos 18Bet si tiene un valor válido de AH
                    if ah_18 and ah_18 not in ["-", "N/A", ""]:
                        odds_info["ah_home_cuota"] = tds_18[2].get("data-o", tds_18[2].get_text(strip=True)) or "N/A"
                        odds_info["ah_linea_raw"] = ah_18
                        odds_info["ah_away_cuota"] = tds_18[4].get("data-o", tds_18[4].get_text(strip=True)) or "N/A"
                        # También actualizamos O/U si está vacío
                        if odds_info["goals_linea_raw"] in ["N/A", "-", "", None]:
                            odds_info["goals_over_cuota"] = tds_18[8].get("data-o", tds_18[8].get_text(strip=True)) or "N/A"
                            odds_info["goals_linea_raw"] = tds_18[9].get("data-o", tds_18[9].get_text(strip=True)) or "N/A"
                            odds_info["goals_under_cuota"] = tds_18[10].get("data-o", tds_18[10].get_text(strip=True)) or "N/A"

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
    
    # Fallback 3: Playwright (para cuotas que requieren JavaScript dinámico)
    if PLAYWRIGHT_AVAILABLE and (odds_info["ah_linea_raw"] in ["N/A", "-", ""] or odds_info["goals_linea_raw"] in ["N/A", "-", ""]) and match_id:
        pw_odds = fetch_odds_with_playwright(match_id)
        if pw_odds:
            if odds_info["ah_linea_raw"] in ["N/A", "-", ""]:
                odds_info["ah_linea_raw"] = pw_odds.get("ah_linea_raw", "N/A")
            if odds_info["goals_linea_raw"] in ["N/A", "-", ""]:
                odds_info["goals_linea_raw"] = pw_odds.get("goals_linea_raw", "N/A")
                
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
        rank_match = re.search(r'\[.*?(\d+)\]', full_text)
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

def _parse_to_spain_datetime(date_str, time_str, is_utc=True):
    from datetime import datetime, timedelta
    
    # Normalizar AM/PM
    am_pm = ""
    time_str = time_str.strip()
    if time_str.upper().endswith("PM"):
        am_pm = "PM"
        time_str = time_str[:-2].strip()
    elif time_str.upper().endswith("AM"):
        am_pm = "AM"
        time_str = time_str[:-2].strip()
        
    time_str = time_str.rstrip(":")
    
    # Parsear hora y minuto
    h, m = 12, 0
    try:
        parts = time_str.split(":")
        if len(parts) >= 2:
            h = int(parts[0])
            m = int(parts[1])
    except Exception:
        pass
        
    if am_pm == "PM" and h < 12:
        h += 12
    elif am_pm == "AM" and h == 12:
        h = 0
        
    # Parsear fecha
    year, month, day = 2026, 6, 16
    try:
        if "-" in date_str:
            parts = date_str.split("-")
            if len(parts) == 3:
                if len(parts[0]) == 4:
                    year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
                else:
                    day, month, year = int(parts[0]), int(parts[1]), int(parts[2])
        elif "/" in date_str:
            parts = date_str.split("/")
            if len(parts) == 3:
                p1, p2, p3 = int(parts[0]), int(parts[1]), int(parts[2])
                if p3 >= 100:
                    if p1 > 12:
                        day, month, year = p1, p2, p3
                    elif p2 > 12:
                        month, day, year = p1, p2, p3
                    else:
                        month, day, year = p1, p2, p3
                else:
                    year, month, day = p1, p2, p3
    except Exception:
        pass
        
    try:
        dt = datetime(year, month, day, h, m)
    except Exception:
        dt = datetime(2026, 6, 16, 12, 0)
        
    # Convertir a España
    if is_utc:
        # En verano (abril a octubre) España es UTC+2, en invierno (noviembre a marzo) es UTC+1
        if 4 <= dt.month <= 10:
            dt_spain = dt + timedelta(hours=2)
        else:
            dt_spain = dt + timedelta(hours=1)
    else:
        # De Pekín (GMT+8) a España: restamos 7 horas
        dt_spain = dt - timedelta(hours=7)
        
    return dt_spain

def extract_match_datetime_spain(soup):
    """Extrae el datetime del partido convertido a la hora de España."""
    if not soup:
        return None
        
    try:
        # 1. Intentar con el span timeData (suele estar en UTC)
        time_span = soup.find("span", attrs={"name": "timeData"})
        if time_span and time_span.get("data-t"):
            full_time = time_span.get("data-t").strip()
            if ' ' in full_time:
                date_part, time_part = full_time.split(' ', 1)
                return _parse_to_spain_datetime(date_part, time_part, is_utc=True)
                
        # 2. Intentar con el script matchInfo (suele estar en GMT+8 o similar)
        scripts = soup.find_all("script")
        for script in scripts:
            if script.string and "var _matchInfo" in script.string:
                match = re.search(r"(?:matchTime|mTime):\s*'([^']*)'", script.string)
                if match:
                    full_time = match.group(1).strip()
                    if ' ' in full_time:
                        date_part, time_part = full_time.split(' ', 1)
                        return _parse_to_spain_datetime(date_part, time_part, is_utc=False)
                break
                
        # 3. Fallback al HTML div#match_time
        time_div = soup.find("div", class_="row", id="match_time")
        if time_div:
            text = time_div.get_text(strip=True)
            if ' ' in text:
                date_part, time_part = text.split(' ', 1)
                return _parse_to_spain_datetime(date_part, time_part, is_utc=True)
            else:
                from datetime import datetime
                today_str = datetime.now().strftime("%m/%d/%Y")
                return _parse_to_spain_datetime(today_str, text, is_utc=True)

    except Exception:
        pass
    return None

def extract_match_time_of(soup):
    """Extrae la hora del partido convertida a España (HH:MM)."""
    dt = extract_match_datetime_spain(soup)
    if dt:
        return dt.strftime("%H:%M")
    return "N/A"

def extract_match_date_of(soup):
    """Extrae la fecha del partido convertida a España (M/D/Y)."""
    dt = extract_match_datetime_spain(soup)
    if dt:
        return f"{dt.month}/{dt.day}/{dt.year}"
    return "N/A"


def _normalize_team_name(name):
    if not name: return ""
    # Normalización básica: lowercase, quitar espacios y sufijos comunes
    n = name.lower().strip()
    # Eliminar puntuación común y sufijos (ordenados por longitud para evitar solapamientos incorrectos)
    suffixes = [" u19", " u21", " u23", " fc", " cf", " ssc", " as", " csd", " cd", " sc", " ac", " afc", " yth"]
    for s in suffixes:
        if n.endswith(s):
            n = n[:-len(s)].strip()
    # Reemplazar algunos caracteres especiales
    n = n.replace("-", " ").replace(".", "")
    return n.strip()

def extract_h2h_data_of(soup, home_name, away_name, league_id=None, odds_map=None):
    results = {
        'ah1': '-', 'res1': '?:?', 'res1_raw': '?-?', 'match1_id': None, 'date1': 'N/A',
        'ah6': '-', 'res6': '?:?', 'res6_raw': '?-?', 'match6_id': None, 'date6': 'N/A',
        'h2h_gen_home': "Local (H2H Gen)", 'h2h_gen_away': "Visitante (H2H Gen)"
    }
    if not soup or not home_name or not away_name: return results
    
    all_matches = []
    
    # Lista de IDs de tablas donde buscar (Priorizando table_v3 que es la de H2H directo)
    table_ids = ["table_v3", "table_v1", "table_v2"]
    hn_norm = _normalize_team_name(home_name)
    an_norm = _normalize_team_name(away_name)
    
    seen_match_indices = set()

    for t_id in table_ids:
        h2h_table = soup.find("table", id=t_id)
        if not h2h_table: continue
        
        row_id_pattern = re.compile(rf"tr{t_id[-1]}_(\d+)")
        for r in h2h_table.find_all("tr", id=row_id_pattern):
            d = get_match_details_from_row_of(r, score_class_selector=f'fscore_{t_id[-1]}', source_table_type='h2h', odds_map=odds_map)
            if not d: continue
            
            m_idx = d.get('matchIndex')
            if m_idx in seen_match_indices: continue
            
            # Verificar si es un partido entre estos dos equipos (Home vs Away o Away vs Home)
            h_curr = _normalize_team_name(d.get('home', ''))
            a_curr = _normalize_team_name(d.get('away', ''))
            
            is_match = (h_curr == hn_norm and a_curr == an_norm) or (h_curr == an_norm and a_curr == hn_norm)
            
            if is_match:
                if not league_id or (d.get('league_id_hist') and d.get('league_id_hist') == str(league_id)):
                    all_matches.append(d)
                    seen_match_indices.add(m_idx)
    
    if not all_matches: return results
    
    # Ordenar por fecha descendente
    all_matches.sort(key=lambda x: _parse_date_ddmmyyyy(x.get('date', '')), reverse=True)
    
    # H2H General es simplemente el más reciente
    most_recent = all_matches[0]
    results.update({
        'ah6': most_recent.get('ahLine', '-'), 'res6': most_recent.get('score', '?:?'), 'res6_raw': most_recent.get('score_raw', '?-?'),
        'match6_id': most_recent.get('matchIndex'), 'date6': most_recent.get('date', 'N/A'),
        'h2h_gen_home': most_recent.get('home'), 'h2h_gen_away': most_recent.get('away'),
        'home_red_gen': most_recent.get('home_red'), 'away_red_gen': most_recent.get('away_red')
    })
    
    # H2H Estadio es el más reciente donde Local es Local y Visitante es Visitante
    for d in all_matches:
        if _normalize_team_name(d['home']) == hn_norm and _normalize_team_name(d['away']) == an_norm:
            results.update({
                'ah1': d.get('ahLine', '-'), 'res1': d.get('score', '?:?'), 'res1_raw': d.get('score_raw', '?-?'),
                'match1_id': d.get('matchIndex'), 'date1': d.get('date', 'N/A'),
                'home_red_stadium': d.get('home_red'), 'away_red_stadium': d.get('away_red')
            })
            break
            
    return results

def extract_comparative_match_of(soup, table_id, main_team, opponent, league_id, is_home_table, odds_map=None):
    if not opponent or opponent == "N/A" or not main_team or not (table := soup.find("table", id=table_id)): return None
    score_selector = 'fscore_1' if is_home_table else 'fscore_2'
    
    match_same_league = None
    match_diff_league = None
    
    for row in table.find_all("tr", id=re.compile(rf"tr{table_id[-1]}_\d+")):
        if not (details := get_match_details_from_row_of(row, score_class_selector=score_selector, source_table_type='hist', odds_map=odds_map)): continue
        
        h, a = details.get('home','').lower(), details.get('away','').lower()
        main, opp = main_team.lower(), opponent.lower()
        
        if (main == h and opp == a) or (main == a and opp == h):
            res = {
                "score": details.get('score', '?:?'), "ah_line": details.get('ahLine', '-'), "localia": 'H' if main == h else 'A',
                "home_team": details.get('home'), "away_team": details.get('away'), "match_id": details.get('matchIndex'),
                "date": details.get('date', 'N/A'), "home_red": details.get('home_red'), "away_red": details.get('away_red'),
                "is_different_league": False
            }
            
            # Si es la misma liga, retornamos de inmediato
            if not league_id or details.get('league_id_hist') == str(league_id):
                return res
            else:
                # Si es diferente liga, lo guardamos por si no encontramos nada mejor
                if not match_diff_league:
                    res["is_different_league"] = True
                    match_diff_league = res
                    
    return match_diff_league


def _load_main_match_soup(main_match_id: str):
    main_page_url = f"{BASE_URL_OF}/match/h2h-{main_match_id}"
    session = get_requests_session_of()
    response = session.get(main_page_url, timeout=REQUEST_TIMEOUT_SECONDS, verify=False)
    response.raise_for_status()
    return BeautifulSoup(response.text, "lxml")

from pathlib import Path

def load_cached_finished_matches():
    """Carga los partidos finalizados desde data.json."""
    # Intentar localizar data.json en directorios padres
    candidates = [
        Path(__file__).resolve().parent.parent.parent / 'data.json', # src/modules/../.. -> root
        Path("data.json").resolve() # Fallback to current working directory
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

def analizar_partido_completo(match_id: str, force_refresh: bool = False, check_odds_early: bool = False):
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
        
        # --- EARLY ODDS CHECK (Optimizacion) ---
        if check_odds_early:
            early_odds = extract_bet365_initial_odds_of(soup_completo, main_match_id)
            ah_check = early_odds.get('ah_linea_raw', 'N/A')
            if ah_check in ['N/A', '-', '?', '', None]:
                # Intento de Fallback rápido (si está implementado en extract_bet365... ya lo hace, pero revisemos extras)
                # extract_bet365_initial_odds_of ya tiene fallbacks internos.
                return {"error": "No valid AH odds found", "skipped": True}
        
        # --- LIGA NEUTRA LOGIC ---
        is_neutral_venue = False
        try:
            data_cfg = sql_store.get_json_state('app_favoritas_config_v1', default={}) or {}
            neutras_cfg = [n.lower() for n in data_cfg.get('neutras_nombres', [])]
            if league_name and league_name.lower() in neutras_cfg:
                is_neutral_venue = True
        except Exception as e:
            print(f"Warning checking neutral league: {e}")
            
        home_standings = extract_standings_data_from_h2h_page_of(soup_completo, home_name)
        away_standings = extract_standings_data_from_h2h_page_of(soup_completo, away_name)
        home_ou_stats = extract_over_under_stats_from_div_of(soup_completo, 'home')
        away_ou_stats = extract_over_under_stats_from_div_of(soup_completo, 'away')
        key_match_id_rival_a, rival_a_id, rival_a_name, is_diff_league_a = get_rival_a_for_original_h2h_of(soup_completo, league_id)
        _, rival_b_id, rival_b_name, is_diff_league_b = get_rival_b_for_original_h2h_of(soup_completo, league_id)
        
        # Extraer mapa de cuotas históricas
        odds_map = extract_vs_odds(soup_completo)
        
        last_home_match = extract_last_match_in_league_of(soup_completo, "table_v1", home_name, league_id, True, odds_map, is_neutral_venue=is_neutral_venue)
        last_away_match = extract_last_match_in_league_of(soup_completo, "table_v2", away_name, league_id, False, odds_map, is_neutral_venue=is_neutral_venue)
        
        # Extraer listas de partidos recientes (Home vs Home, Away vs Away)
        # Tambien aplicamos is_neutral_venue a las listas completas para consistencia
        recent_home_matches = extract_recent_matches(soup_completo, "table_v1", home_name, None, True, odds_map, limit=10, is_neutral_venue=is_neutral_venue)
        recent_away_matches = extract_recent_matches(soup_completo, "table_v2", away_name, None, False, odds_map, limit=10, is_neutral_venue=is_neutral_venue)
        recent_away_matches_all = extract_recent_matches(soup_completo, "table_v2", away_name, None, False, odds_map, limit=15, is_neutral_venue=True)
        
        h2h_data = extract_h2h_data_of(soup_completo, home_name, away_name, None, odds_map)
        comp_L_vs_UV_A = extract_comparative_match_of(soup_completo, "table_v1", home_name, (last_away_match or {}).get('home_team'), league_id, True, odds_map)
        comp_V_vs_UL_H = extract_comparative_match_of(soup_completo, "table_v2", away_name, (last_home_match or {}).get('away_team'), league_id, False, odds_map)
        main_match_odds_data = extract_bet365_initial_odds_of(soup_completo, main_match_id)
        final_score, _ = extract_final_score_of(soup_completo)
        match_time = extract_match_time_of(soup_completo)
        match_date = extract_match_date_of(soup_completo)
        details_h2h_col3 = get_h2h_details_for_original_logic_of(
            key_match_id_rival_a, rival_a_id, rival_b_id, rival_a_name, rival_b_name
        )
        if details_h2h_col3 and details_h2h_col3.get("status") == "found":
            details_h2h_col3["is_different_league"] = is_diff_league_a or is_diff_league_b
        
        # Agregar datos del H2H Col3 al diccionario h2h_data
        h2h_data['col3_data'] = details_h2h_col3 if details_h2h_col3 else {}
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

    except Exception as exc:
        return {"error": f"Error durante el análisis: {exc}"}

    market_analysis_html, market_analysis_data = generar_analisis_completo_mercado(
        main_match_odds_data, 
        h2h_data, 
        home_name, 
        away_name,
        prev_home_data=last_home_match,
        prev_away_data=last_away_match
    )
    historical_matches_html = _build_historical_matches_list_html(recent_home_matches, recent_away_matches, home_name, away_name)

    def get_stats_rows(match_id_value):
        if not match_id_value:
            return []
        df = get_match_progression_stats_data(str(match_id_value))
        return _df_to_rows(df)

    main_match_stats = get_stats_rows(main_match_id)
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
        "match_date": match_date,
        "home_standings": home_standings,
        "away_standings": away_standings,
        "home_ou_stats": home_ou_stats,
        "away_ou_stats": away_ou_stats,
        "main_match_odds": {
            "ah_linea": format_ah_as_decimal_string_of(main_match_odds_data.get('ah_linea_raw', '?')),
            "goals_linea": format_ah_as_decimal_string_of(main_match_odds_data.get('goals_linea_raw', '?'))
        },
        # Estadisticas del propio partido. El Explorador historico usa esta
        # coleccion al reutilizar el encuentro como Prev/H2H/indirecta.
        "stats_rows": main_match_stats,
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
        "recent_home_matches": recent_home_matches,
        "recent_away_matches": recent_away_matches,
        "recent_away_matches_all": recent_away_matches_all,
        "execution_time_seconds": round(time.time() - start_time, 2),
    }

    normalize_red_card_stats_payload(results)
    _set_cached_analysis(main_match_id, results)
    return copy.deepcopy(results)
