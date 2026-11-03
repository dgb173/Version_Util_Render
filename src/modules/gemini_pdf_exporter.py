# -*- coding: utf-8 -*-
"""
Módulo para extraer estadísticas de los últimos 5 partidos de ambos equipos
y generar un Dossier en PDF y texto estructurado listo para análisis en Gemini / LLMs.
"""

import io
import json
import logging
from typing import Dict, List, Any, Optional

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None

from .last_general_context import _get_stats_rows
from .llm_exporter import evaluate_handicap_cover, parse_handicap

LOGGER = logging.getLogger(__name__)


def _clean_str(val: Any) -> str:
    if val is None or val == "" or str(val).strip() in ("None", "null", "undefined"):
        return "-"
    return str(val).strip()


def _parse_num(val: Any) -> float:
    if val is None:
        return 0.0
    try:
        cleaned = str(val).replace("%", "").strip()
        return float(cleaned)
    except (ValueError, TypeError):
        return 0.0


def extract_last_5_matches_with_stats(team_name: str, matches_list: List[Dict], is_home_subject: bool) -> List[Dict]:
    """
    Toma hasta los 5 partidos más recientes de un equipo y extrae sus datos
    y estadísticas detalladas completas (tiros, puerta, ataques, peligrosos, rojas, etc.).
    """
    if not isinstance(matches_list, list):
        return []

    norm_subject = team_name.lower().strip()
    extracted = []

    for m in matches_list[:5]:
        if not isinstance(m, dict):
            continue

        raw_home = m.get('home') or m.get('home_team') or ''
        raw_away = m.get('away') or m.get('away_team') or ''
        norm_h = raw_home.lower().strip()
        norm_a = raw_away.lower().strip()

        # Determinar si el equipo analizado jugó de local o visitante en ese partido
        if norm_h and norm_subject in norm_h:
            is_subject_home = True
            rival = raw_away
        elif norm_a and norm_subject in norm_a:
            is_subject_home = False
            rival = raw_home
        else:
            is_subject_home = is_home_subject
            rival = raw_away if is_home_subject else raw_home

        mid = m.get('matchIndex') or m.get('match_id') or m.get('id')
        score = _clean_str(m.get('score') or m.get('score_raw'))
        ah_line = _clean_str(m.get('ahLine') or m.get('ahLine_raw') or m.get('handicap') or m.get('handicap_line_raw'))
        ou_line = _clean_str(m.get('ouLine') or m.get('goal_line') or m.get('goals_linea'))
        date = _clean_str(m.get('date') or m.get('match_date'))

        # Calcular si cubrió hándicap
        cover_status = evaluate_handicap_cover(score, ah_line, is_subject_home) if ah_line != '-' and score != '-' else ''

        # Obtener estadísticas detalladas
        stats_rows = m.get('stats_rows') or []
        if not stats_rows and mid:
            try:
                stats_rows = _get_stats_rows(str(mid)) or []
            except Exception as e:
                LOGGER.debug("No se pudieron cargar stats para match %s: %s", mid, e)
                stats_rows = []

        # Parsear métricas clave orientadas al equipo analizado (A Favor vs En Contra)
        shots_fav, shots_con = 0, 0
        sot_fav, sot_con = 0, 0
        att_fav, att_con = 0, 0
        dang_fav, dang_con = 0, 0
        corners_fav, corners_con = 0, 0
        poss_fav, poss_con = "50%", "50%"
        red_fav, red_con = 0, 0

        for s in stats_rows:
            label = str(s.get('name') or s.get('label') or '').lower().strip()
            h_val = _parse_num(s.get('home'))
            a_val = _parse_num(s.get('away'))

            sub_val = h_val if is_subject_home else a_val
            opp_val = a_val if is_subject_home else h_val

            if 'puerta' in label or 'target' in label:
                sot_fav, sot_con = int(sub_val), int(opp_val)
            elif 'tiro' in label or 'shot' in label:
                shots_fav, shots_con = int(sub_val), int(opp_val)
            elif 'peligros' in label or 'dangerous' in label:
                dang_fav, dang_con = int(sub_val), int(opp_val)
            elif 'ataque' in label or 'attack' in label:
                att_fav, att_con = int(sub_val), int(opp_val)
            elif 'córner' in label or 'corner' in label:
                corners_fav, corners_con = int(sub_val), int(opp_val)
            elif 'posesi' in label or 'possession' in label:
                poss_fav = str(s.get('home') if is_subject_home else s.get('away'))
                poss_con = str(s.get('away') if is_subject_home else s.get('home'))
            elif 'roja' in label or 'red' in label:
                red_fav, red_con = int(sub_val), int(opp_val)

        # Fallback para rojas desde las propiedades directas del partido si existen
        if red_fav == 0 and red_con == 0:
            rh = int(m.get('home_red') or 0)
            ra = int(m.get('away_red') or 0)
            red_fav = rh if is_subject_home else ra
            red_con = ra if is_subject_home else rh

        extracted.append({
            'match_id': mid,
            'date': date,
            'rival': rival,
            'location': 'CASA' if is_subject_home else 'FUERA',
            'score': score,
            'ah_line': ah_line,
            'cover_status': cover_status,
            'ou_line': ou_line,
            'shots_fav': shots_fav,
            'shots_con': shots_con,
            'sot_fav': sot_fav,
            'sot_con': sot_con,
            'att_fav': att_fav,
            'att_con': att_con,
            'dang_fav': dang_fav,
            'dang_con': dang_con,
            'corners_fav': corners_fav,
            'corners_con': corners_con,
            'poss_fav': poss_fav,
            'poss_con': poss_con,
            'red_fav': red_fav,
            'red_con': red_con,
            'has_full_stats': bool(shots_fav or dang_fav or att_fav or sot_fav)
        })

    return extracted


def compute_team_5_match_summary(matches_list: List[Dict]) -> Dict[str, Any]:
    """Calcula promedios, ratios de cobertura y tendencias de los 5 partidos."""
    if not matches_list:
        return {
            'count': 0, 'wdl': '0-0-0', 'goals': '0:0', 'cover_rate': '0%',
            'avg_sot': 0.0, 'avg_dang': 0.0, 'cover_summary': 'Sin datos'
        }

    wins, draws, losses = 0, 0, 0
    goals_for, goals_against = 0, 0
    covered_cnt, not_covered_cnt, push_cnt = 0, 0, 0
    tot_sot_fav, tot_sot_con = 0, 0
    tot_dang_fav, tot_dang_con = 0, 0
    valid_stats_cnt = 0

    for m in matches_list:
        score = m.get('score', '')
        if score and ':' in score:
            try:
                parts = score.split(':')
                gh = int(parts[0])
                ga = int(parts[1])
                is_home = (m.get('location') == 'CASA')
                gf = gh if is_home else ga
                ga_opp = ga if is_home else gh
                goals_for += gf
                goals_against += ga_opp
                if gf > ga_opp: wins += 1
                elif gf == ga_opp: draws += 1
                else: losses += 1
            except Exception:
                pass

        cov = m.get('cover_status', '')
        if cov == 'CUBRIÓ': covered_cnt += 1
        elif cov == 'NO CUBRIÓ': not_covered_cnt += 1
        elif cov == 'PUSH (Igualó)': push_cnt += 1

        if m.get('has_full_stats'):
            valid_stats_cnt += 1
            tot_sot_fav += m.get('sot_fav', 0)
            tot_sot_con += m.get('sot_con', 0)
            tot_dang_fav += m.get('dang_fav', 0)
            tot_dang_con += m.get('dang_con', 0)

    n = len(matches_list)
    cov_total = covered_cnt + not_covered_cnt + push_cnt
    cov_pct = f"{round((covered_cnt / cov_total) * 100)}%" if cov_total > 0 else "N/A"
    denom = max(valid_stats_cnt, 1)

    return {
        'count': n,
        'wdl': f"{wins}V - {draws}E - {losses}D",
        'goals': f"{goals_for}:{goals_against} ({'+' if goals_for >= goals_against else ''}{goals_for - goals_against})",
        'cover_summary': f"{covered_cnt} Cubrió · {not_covered_cnt} No Cubrió · {push_cnt} Push ({cov_pct})",
        'avg_sot_fav': round(tot_sot_fav / denom, 1),
        'avg_sot_con': round(tot_sot_con / denom, 1),
        'avg_dang_fav': round(tot_dang_fav / denom, 1),
        'avg_dang_con': round(tot_dang_con / denom, 1)
    }


def build_match_gemini_dossier_data(match_data: Dict, match_index: int, total_matches: int, mode: str = 'precacheo') -> Dict[str, Any]:
    """Compila toda la información estructurada de un partido y sus últimos 5 partidos."""
    home_name = _clean_str(match_data.get('home_name') or match_data.get('home') or match_data.get('home_team'))
    away_name = _clean_str(match_data.get('away_name') or match_data.get('away') or match_data.get('away_team'))
    league = _clean_str(match_data.get('league_name') or match_data.get('league') or (match_data.get('candidate') or {}).get('league'))
    date = _clean_str(match_data.get('match_date') or match_data.get('date') or (match_data.get('candidate') or {}).get('date'))
    ah_line = _clean_str(match_data.get('handicap') or (match_data.get('main_match_odds') or {}).get('ah_linea') or (match_data.get('candidate') or {}).get('ah_real'))
    goal_line = _clean_str(match_data.get('goal_line') or (match_data.get('main_match_odds') or {}).get('goals_linea') or (match_data.get('candidate') or {}).get('ou_line'))
    mid = str(match_data.get('match_id') or (match_data.get('candidate') or {}).get('match_id') or '')

    # Determinar favorito actual
    ah_num = parse_handicap(ah_line)
    fav_text = "Pick'em (Línea 0)"
    if ah_num is not None:
        if ah_num > 0: fav_text = f"Local: {home_name} (AH {ah_line})"
        elif ah_num < 0: fav_text = f"Visitante: {away_name} (AH {ah_line})"

    # Obtener listas de últimos partidos
    pre_ctx = match_data.get('pre_match_context') or {}
    cur_ctx = pre_ctx.get('current') or pre_ctx

    raw_home_matches = (
        match_data.get('recent_home_matches_all') or
        cur_ctx.get('home_matches_all') or
        match_data.get('recent_home_matches') or
        cur_ctx.get('home_matches') or []
    )
    raw_away_matches = (
        match_data.get('recent_away_matches_all') or
        cur_ctx.get('away_matches_all') or
        match_data.get('recent_away_matches') or
        cur_ctx.get('away_matches') or []
    )

    home_5 = extract_last_5_matches_with_stats(home_name, raw_home_matches, is_home_subject=True)
    away_5 = extract_last_5_matches_with_stats(away_name, raw_away_matches, is_home_subject=False)

    home_summary = compute_team_5_match_summary(home_5)
    away_summary = compute_team_5_match_summary(away_5)

    # Precedentes H2H
    h2h_stadium = match_data.get('h2h_stadium') or {}
    h2h_general = match_data.get('h2h_general') or {}
    stadium_mkt = (match_data.get('market_analysis_data') or {}).get('stadium') or {}
    general_mkt = (match_data.get('market_analysis_data') or {}).get('general') or {}

    h2h_st_res = _clean_str(h2h_stadium.get('res1_raw') or h2h_stadium.get('res1') or stadium_mkt.get('result') or h2h_stadium.get('score'))
    h2h_st_ah = _clean_str(h2h_stadium.get('ah1') or stadium_mkt.get('movement') or h2h_stadium.get('movement'))
    h2h_st_date = _clean_str(h2h_stadium.get('date1') or stadium_mkt.get('date'))
    h2h_st_eval = _clean_str(stadium_mkt.get('evaluation'))

    h2h_gen_res = _clean_str(h2h_general.get('res6_raw') or h2h_general.get('res6') or general_mkt.get('result') or h2h_general.get('score'))
    h2h_gen_ah = _clean_str(h2h_general.get('ah6') or general_mkt.get('movement') or h2h_general.get('movement'))
    h2h_gen_date = _clean_str(h2h_general.get('date6') or general_mkt.get('date'))
    h2h_gen_eval = _clean_str(general_mkt.get('evaluation'))

    # Si es partido finalizado (Explorador)
    final_score = _clean_str(match_data.get('final_score') or match_data.get('score') or (match_data.get('candidate') or {}).get('score'))
    cover_result = _clean_str(match_data.get('cover_result') or (match_data.get('candidate') or {}).get('cover_result'))

    return {
        'index': match_index,
        'total': total_matches,
        'mode': mode,
        'match_id': mid,
        'home_name': home_name,
        'away_name': away_name,
        'league': league,
        'date': date,
        'ah_line': ah_line,
        'goal_line': goal_line,
        'favorite': fav_text,
        'final_score': final_score,
        'cover_result': cover_result,
        'home_5': home_5,
        'away_5': away_5,
        'home_summary': home_summary,
        'away_summary': away_summary,
        'h2h_stadium': {
            'date': h2h_st_date, 'score': h2h_st_res, 'ah': h2h_st_ah, 'eval': h2h_st_eval
        },
        'h2h_general': {
            'date': h2h_gen_date, 'score': h2h_gen_res, 'ah': h2h_gen_ah, 'eval': h2h_gen_eval
        }
    }


def render_match_html_page(dossier: Dict[str, Any]) -> str:
    """Renderiza el HTML estético de 1 página de partido optimizado para insert_htmlbox en PyMuPDF."""
    is_expl = dossier['mode'] == 'explorer'
    idx = dossier['index']
    tot = dossier['total']

    # Estilos de badges para cobertura
    def badge_cov(cov: str) -> str:
        if not cov: return "-"
        if "CUBRIÓ" in cov:
            return '<span style="background-color:#dcfce7; color:#15803d; font-weight:bold; padding:1px 4px; border-radius:3px;">CUBRIÓ</span>'
        if "NO CUBRIÓ" in cov:
            return '<span style="background-color:#fee2e2; color:#991b1b; font-weight:bold; padding:1px 4px; border-radius:3px;">NO CUBRIÓ</span>'
        if "PUSH" in cov:
            return '<span style="background-color:#f1f5f9; color:#475569; font-weight:bold; padding:1px 4px; border-radius:3px;">PUSH</span>'
        return cov

    # Render filas de 5 partidos
    def render_rows(matches: List[Dict], team_label: str) -> str:
        if not matches:
            return '<tr><td colspan="7" style="text-align:center; color:#94a3b8; padding:6px;">Sin historial reciente disponible</td></tr>'
        html_rows = []
        for m in matches:
            red_badge = f' <span style="background-color:#ef4444; color:#fff; font-size:7pt; padding:1px 3px; border-radius:2px; font-weight:bold;">🟥 {m["red_fav"]}</span>' if m.get('red_fav', 0) > 0 else ''
            opp_red = f' <span style="background-color:#ef4444; color:#fff; font-size:7pt; padding:1px 3px; border-radius:2px; font-weight:bold;">🟥 {m["red_con"]}</span>' if m.get('red_con', 0) > 0 else ''
            cov = badge_cov(m.get('cover_status', ''))
            html_rows.append(f"""
            <tr style="border-bottom: 1px solid #e2e8f0;">
                <td style="padding: 3px 4px; font-size: 7.5pt; color: #64748b;">{m['date']}</td>
                <td style="padding: 3px 4px; font-size: 7.5pt;">vs <b>{m['rival']}</b> <span style="font-size:6.5pt; color:#64748b;">({m['location']})</span>{opp_red}</td>
                <td style="padding: 3px 4px; text-align: center; font-size: 8pt; font-weight: bold;">{m['score']}{red_badge}</td>
                <td style="padding: 3px 4px; text-align: center; font-size: 7.5pt;">{m['ah_line']}</td>
                <td style="padding: 3px 4px; text-align: center; font-size: 7.5pt;">{cov}</td>
                <td style="padding: 3px 4px; text-align: center; font-size: 7.5pt; font-weight:bold;">{m['sot_fav']}:{m['sot_con']} <span style="font-weight:normal; font-size:6.5pt; color:#64748b;">({m['shots_fav']}:{m['shots_con']})</span></td>
                <td style="padding: 3px 4px; text-align: center; font-size: 7.5pt; font-weight:bold; color:#047857;">{m['dang_fav']}:{m['dang_con']}</td>
            </tr>
            """)
        return "".join(html_rows)

    hs = dossier['home_summary']
    asum = dossier['away_summary']
    h2h_st = dossier['h2h_stadium']
    h2h_gen = dossier['h2h_general']

    result_header_box = ""
    if is_expl:
        result_header_box = f"""
        <div style="background-color: #f0fdf4; border: 1.5px solid #22c55e; border-radius: 4px; padding: 4px 8px; margin-bottom: 6px; display: flex; justify-content: space-between;">
            <span style="font-size: 8.5pt; font-weight: bold; color: #15803d;">RESULTADO FINAL (FT): {dossier['final_score']}</span>
            <span style="font-size: 8.5pt; font-weight: bold; color: #15803d;">COBERTURA AH: {badge_cov(dossier['cover_result'])}</span>
        </div>
        """

    prompt_instruction = (
        "Analiza el partido comparando las estadísticas previas de los 5 partidos (disparidad entre ataques peligrosos/tiros a puerta vs marcadores reales) para explicar por qué se dio este resultado y qué señales previas avisaban del desenlace."
        if is_expl else
        "Analiza el volumen ofensivo real (ataques peligrosos y tiros a puerta) de los últimos 5 partidos vs marcadores reales para detectar superioridad oculta, fatiga o valor frente a la línea de Hándicap actual."
    )

    html = f"""
    <div style="font-family: Arial, Helvetica, sans-serif; color: #1e293b; line-height: 1.25; margin: 0; padding: 0;">
        <!-- Cabecera de Partido -->
        <div style="border-bottom: 2px solid #059669; padding-bottom: 4px; margin-bottom: 6px;">
            <table style="width: 100%; border-collapse: collapse;">
                <tr>
                    <td style="vertical-align: middle;">
                        <span style="background-color: #059669; color: #ffffff; font-size: 7.5pt; font-weight: bold; padding: 2px 6px; border-radius: 3px;">
                            PARTIDO {idx} DE {tot} · {'EXPLORADOR HISTÓRICO' if is_expl else 'PRE-CACHEO'}
                        </span>
                        <span style="font-size: 8pt; color: #64748b; margin-left: 6px;">ID: {dossier['match_id']} | {dossier['date']} | {dossier['league']}</span>
                    </td>
                    <td style="text-align: right; vertical-align: middle;">
                        <span style="font-size: 8.5pt; font-weight: bold; color: #047857;">AH: {dossier['ah_line']} | O/U: {dossier['goal_line']}</span>
                    </td>
                </tr>
            </table>
            <h1 style="font-size: 13pt; margin: 4px 0 2px 0; color: #0f172a;">
                <span style="color: #0284c7;">{dossier['home_name']}</span> <span style="font-size: 10pt; color: #94a3b8; font-weight: normal;">vs</span> <span style="color: #dc2626;">{dossier['away_name']}</span>
            </h1>
            <div style="font-size: 7.5pt; color: #475569;">
                <b>Favorito Mercado:</b> {dossier['favorite']}
            </div>
        </div>

        {result_header_box}

        <!-- Bloque Local: 5 Últimos Partidos -->
        <div style="margin-bottom: 6px;">
            <table style="width: 100%; border-collapse: collapse; margin-bottom: 3px;">
                <tr>
                    <td style="font-size: 8.5pt; font-weight: bold; color: #0284c7;">🏠 {dossier['home_name']} (ÚLTIMOS 5 PARTIDOS)</td>
                    <td style="font-size: 7pt; text-align: right; color: #475569;">
                        <b>Balance:</b> {hs['wdl']} | <b>Goles:</b> {hs['goals']} | <b>AH:</b> {hs['cover_summary']} | <b>Medias:</b> {hs['avg_sot_fav']}:{hs['avg_sot_con']} Tiros Pta · {hs['avg_dang_fav']}:{hs['avg_dang_con']} Atq.Peligrosos
                    </td>
                </tr>
            </table>
            <table style="width: 100%; border-collapse: collapse; font-size: 7.5pt;">
                <thead>
                    <tr style="background-color: #f1f5f9; color: #334155; border-bottom: 1.5px solid #cbd5e1;">
                        <th style="padding: 2px 4px; text-align: left; width: 62px;">Fecha</th>
                        <th style="padding: 2px 4px; text-align: left;">Rival (Sede)</th>
                        <th style="padding: 2px 4px; text-align: center; width: 48px;">Score</th>
                        <th style="padding: 2px 4px; text-align: center; width: 44px;">Línea AH</th>
                        <th style="padding: 2px 4px; text-align: center; width: 64px;">Cobertura</th>
                        <th style="padding: 2px 4px; text-align: center; width: 85px;">Tiros Pta (Tot)</th>
                        <th style="padding: 2px 4px; text-align: center; width: 80px;">Atq. Peligrosos</th>
                    </tr>
                </thead>
                <tbody>
                    {render_rows(dossier['home_5'], dossier['home_name'])}
                </tbody>
            </table>
        </div>

        <!-- Bloque Visitante: 5 Últimos Partidos -->
        <div style="margin-bottom: 6px;">
            <table style="width: 100%; border-collapse: collapse; margin-bottom: 3px;">
                <tr>
                    <td style="font-size: 8.5pt; font-weight: bold; color: #dc2626;">✈️ {dossier['away_name']} (ÚLTIMOS 5 PARTIDOS)</td>
                    <td style="font-size: 7pt; text-align: right; color: #475569;">
                        <b>Balance:</b> {asum['wdl']} | <b>Goles:</b> {asum['goals']} | <b>AH:</b> {asum['cover_summary']} | <b>Medias:</b> {asum['avg_sot_fav']}:{asum['avg_sot_con']} Tiros Pta · {asum['avg_dang_fav']}:{asum['avg_dang_con']} Atq.Peligrosos
                    </td>
                </tr>
            </table>
            <table style="width: 100%; border-collapse: collapse; font-size: 7.5pt;">
                <thead>
                    <tr style="background-color: #f1f5f9; color: #334155; border-bottom: 1.5px solid #cbd5e1;">
                        <th style="padding: 2px 4px; text-align: left; width: 62px;">Fecha</th>
                        <th style="padding: 2px 4px; text-align: left;">Rival (Sede)</th>
                        <th style="padding: 2px 4px; text-align: center; width: 48px;">Score</th>
                        <th style="padding: 2px 4px; text-align: center; width: 44px;">Línea AH</th>
                        <th style="padding: 2px 4px; text-align: center; width: 64px;">Cobertura</th>
                        <th style="padding: 2px 4px; text-align: center; width: 85px;">Tiros Pta (Tot)</th>
                        <th style="padding: 2px 4px; text-align: center; width: 80px;">Atq. Peligrosos</th>
                    </tr>
                </thead>
                <tbody>
                    {render_rows(dossier['away_5'], dossier['away_name'])}
                </tbody>
            </table>
        </div>

        <!-- Precedentes H2H y Guía para Gemini -->
        <div style="background-color: #f8fafc; border: 1px solid #e2e8f0; border-radius: 4px; padding: 4px 8px; margin-top: 4px;">
            <table style="width: 100%; border-collapse: collapse;">
                <tr>
                    <td style="width: 50%; vertical-align: top; font-size: 7.5pt; padding-right: 8px; border-right: 1px dashed #cbd5e1;">
                        <b>HISTORIAL H2H DIRECTO:</b><br>
                        • <b>Estadio:</b> {h2h_st['date']} | Marcador: <b>{h2h_st['score']}</b> | AH: {h2h_st['ah']} ({h2h_st['eval']})<br>
                        • <b>General:</b> {h2h_gen['date']} | Marcador: <b>{h2h_gen['score']}</b> | AH: {h2h_gen['ah']} ({h2h_gen['eval']})
                    </td>
                    <td style="width: 50%; vertical-align: top; font-size: 7pt; padding-left: 8px; color: #334155;">
                        <b style="color: #047857;">INSTRUCCIÓN GEMINI:</b><br>
                        {prompt_instruction}
                    </td>
                </tr>
            </table>
        </div>
    </div>
    """
    return html


def generate_gemini_pdf_dossier(matches_data_list: List[Dict], mode: str = 'precacheo') -> bytes:
    """
    Genera un archivo PDF binario multi-página con el dossier completo de los partidos seleccionados
    usando PyMuPDF (fitz) y su motor de renderizado HTML vectorizado.
    """
    if not fitz:
        raise RuntimeError("PyMuPDF (fitz) no está disponible en el entorno.")

    doc = fitz.open()
    total = len(matches_data_list)
    margin = 28
    width, height = 595, 842  # A4 vertical
    rect = fitz.Rect(margin, margin, width - margin, height - margin)

    for i, match in enumerate(matches_data_list, start=1):
        page = doc.new_page(width=width, height=height)
        dossier = build_match_gemini_dossier_data(match, i, total, mode=mode)
        html_content = render_match_html_page(dossier)
        page.insert_htmlbox(rect, html_content)

    return doc.tobytes()


def generate_gemini_text_prompt(matches_data_list: List[Dict], mode: str = 'precacheo') -> str:
    """Genera una versión en texto / markdown estructurado para copiar directo al portapapeles."""
    lines = []
    total = len(matches_data_list)
    title = "DOSSIER DE PARTIDOS PARA ANÁLISIS EN GEMINI (ÚLTIMOS 5 PARTIDOS + STATS)"
    lines.append(f"# {title}")
    lines.append(f"- **Total Partidos Seleccionados**: {total}")
    lines.append(f"- **Modo**: {'Explorador Histórico (Finalizados)' if mode == 'explorer' else 'Pre-Cacheo (Próximos Partidos)'}\n")
    lines.append("=" * 80)

    for i, match in enumerate(matches_data_list, start=1):
        d = build_match_gemini_dossier_data(match, i, total, mode=mode)
        lines.append(f"\n## PARTIDO {i}/{total}: {d['home_name']} vs {d['away_name']}")
        lines.append(f"- **Competición**: {d['league']} | **Fecha**: {d['date']}")
        lines.append(f"- **Línea Hándicap Asiático**: {d['ah_line']} | **Línea Goles**: {d['goal_line']}")
        lines.append(f"- **Favorito**: {d['favorite']}")
        if mode == 'explorer':
            lines.append(f"- **RESULTADO FINAL**: {d['final_score']} (Cobertura AH: {d['cover_result']})")

        hs = d['home_summary']
        lines.append(f"\n### 🏠 {d['home_name']} - Balance 5 Partidos: {hs['wdl']} | Goles: {hs['goals']} | Cobertura AH: {hs['cover_summary']}")
        lines.append(f"  * Promedios 5 partidos: {hs['avg_sot_fav']}:{hs['avg_sot_con']} Tiros Puerta | {hs['avg_dang_fav']}:{hs['avg_dang_con']} Ataques Peligrosos")
        for m in d['home_5']:
            lines.append(f"  - [{m['date']}] vs {m['rival']} ({m['location']}): {m['score']} (AH {m['ah_line']}: {m['cover_status']}) | Tiros: {m['shots_fav']}:{m['shots_con']} (Puerta: {m['sot_fav']}:{m['sot_con']}) | Atq.Peligrosos: {m['dang_fav']}:{m['dang_con']} | Rojas: {m['red_fav']}:{m['red_con']}")

        asum = d['away_summary']
        lines.append(f"\n### ✈️ {d['away_name']} - Balance 5 Partidos: {asum['wdl']} | Goles: {asum['goals']} | Cobertura AH: {asum['cover_summary']}")
        lines.append(f"  * Promedios 5 partidos: {asum['avg_sot_fav']}:{asum['avg_sot_con']} Tiros Puerta | {asum['avg_dang_fav']}:{asum['avg_dang_con']} Ataques Peligrosos")
        for m in d['away_5']:
            lines.append(f"  - [{m['date']}] vs {m['rival']} ({m['location']}): {m['score']} (AH {m['ah_line']}: {m['cover_status']}) | Tiros: {m['shots_fav']}:{m['shots_con']} (Puerta: {m['sot_fav']}:{m['sot_con']}) | Atq.Peligrosos: {m['dang_fav']}:{m['dang_con']} | Rojas: {m['red_fav']}:{m['red_con']}")

        lines.append(f"\n### 🏟️ H2H Directo:")
        lines.append(f"- Estadio: {d['h2h_stadium']['date']} | Marcador: {d['h2h_stadium']['score']} | AH: {d['h2h_stadium']['ah']} ({d['h2h_stadium']['eval']})")
        lines.append(f"- General: {d['h2h_general']['date']} | Marcador: {d['h2h_general']['score']} | AH: {d['h2h_general']['ah']} ({d['h2h_general']['eval']})")
        lines.append("-" * 60)

    return "\n".join(lines)
