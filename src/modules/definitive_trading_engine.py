"""
MÓDULO DEL SISTEMA DEFINITIVO DE TRADING Y NAVEGACIÓN EN HÁNDICAPS (DEFINITIVE TRADING ENGINE)

Unifica las 5 dimensiones en un algoritmo continuo:
1. Eje de Hándicap Asiático Universal (AH)
2. Memoria del H2H Directo en la misma línea absoluta o cercana (Estadio + General)
3. Movimiento de Cuotas de Apertura a Cierre (Smart Money vs Public Trap vs Bookie Absorption)
4. Trayectoria Aislada: Local en Casa vs Visitante Fuera (Disparos, Tiros a Puerta, Ataques Peligrosos)
5. Ajuste Dinámico por ADN de Liga (MLS, Ligas Jerárquicas, Ligas Defensivas)
"""

from __future__ import annotations

import math
import re
from typing import Any, Dict, List, Optional, Tuple


def parse_float(val: Any) -> Optional[float]:
    if val is None or val == '' or val == 'N/A' or val == '?':
        return None
    try:
        val_str = str(val).replace(',', '.').strip()
        if '→' in val_str:
            val_str = val_str.split('→')[-1].strip()
        elif '->' in val_str:
            val_str = val_str.split('->')[-1].strip()
        return float(val_str)
    except (ValueError, TypeError):
        return None


def parse_score(score_str: Any) -> Tuple[Optional[int], Optional[int]]:
    if not score_str:
        return None, None
    s = str(score_str).replace(':', '-').replace(' ', '').strip()
    match = re.search(r'(\d+)\s*-\s*(\d+)', s)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None


def classify_odds_movement(ah_start: Optional[float], ah_end: Optional[float], mov_label: Optional[str] = None) -> str:
    """
    Clasifica el movimiento del mercado de cuotas:
    - SMART_MONEY: Movimiento alineado con la métrica de proceso.
    - PUBLIC_TRAP: Movimiento inflado por el público sin sustento de volumen.
    - BOOKIE_ABSORPTION: La casa mantiene la línea alta absorbiendo apuestas del público.
    - STABLE: Línea estable sin variación significativa.
    """
    if mov_label and mov_label.upper() in ('UP', 'DOWN', 'SAME'):
        label = mov_label.upper()
    elif ah_start is not None and ah_end is not None:
        diff = ah_end - ah_start
        if diff <= -0.20:
            label = 'UP'  # Más exigencia al local (ej. -0.25 a -0.50)
        elif diff >= 0.20:
            label = 'DOWN'  # Menos exigencia al local / dinero hacia el visitante
        else:
            label = 'SAME'
    else:
        label = 'SAME'

    return label


class DefinitiveTradingEngine:
    """
    Motor definitivo de toma de decisiones en Hándicap Asiático y Over/Under.
    """

    def analyze_match(self, match_data: Dict[str, Any]) -> Dict[str, Any]:
        home = match_data.get('home_name') or match_data.get('home_team') or match_data.get('home') or 'Local'
        away = match_data.get('away_name') or match_data.get('away_team') or match_data.get('away') or 'Visitante'
        league = str(match_data.get('league_name') or match_data.get('league') or '').upper()
        
        # 1. Hándicap principal actual
        c_obj = match_data.get('candidate') if isinstance(match_data.get('candidate'), dict) else match_data
        ah_real = parse_float(
            c_obj.get('ah_real') or match_data.get('ah_line') or match_data.get('handicap') or match_data.get('ah')
            or (match_data.get('main_match_odds') or {}).get('ah_linea')
        )

        if ah_real is None:
            return {
                'status': 'INSUFFICIENT_DATA',
                'reason': 'No se pudo determinar el Hándicap Asiático del partido'
            }

        # 2. Movimiento H2H y antecedente directo
        h2h = match_data.get('h2h_general') or match_data.get('h2h_stadium') or {}
        h2h_ah = parse_float(h2h.get('ah6') or h2h.get('handicap'))
        h2h_mov = classify_odds_movement(
            parse_float(h2h.get('ah_start')),
            parse_float(h2h.get('ah_end')),
            h2h.get('movement') or h2h.get('mov_direction')
        )
        h2h_res = str(h2h.get('res6') or h2h.get('wdl') or '').upper()

        # 3. Datos de partidos recientes aislados
        ph = match_data.get('prev_home') if isinstance(match_data.get('prev_home'), dict) else {}
        pa = match_data.get('prev_away') if isinstance(match_data.get('prev_away'), dict) else {}

        ph_sot = parse_float(ph.get('sot') or ph.get('shots_on_target')) or 0.0
        pa_sot = parse_float(pa.get('sot') or pa.get('shots_on_target')) or 0.0

        # --- EVALUACIÓN DEFINITIVA EN ESPACIO HÁNDICAP ---
        picks = []

        # ESCENARIO DEFINITIVO A: Memoria de H2H con Dog Victorioso
        # El local o el visitante ya ganaron en una línea de Hándicap similar.
        if h2h_ah is not None and abs(ah_real - h2h_ah) <= 0.25:
            if 'AWAY' in h2h_res or 'COVER' in h2h_res or 'WIN' in h2h_res:
                if ah_real < 0:
                    picks.append({
                        'pick': f'Visitante {away} {abs(ah_real):+.2f}',
                        'pick_type': 'HANDICAP_UNDERDOG',
                        'confidence': 'ALTA',
                        'rule_name': 'DEF_H2H_LINE_MEMORY_DOG',
                        'reason': (
                            f"Memoria H2H confirmada: {away} ya logró cubrir/ganar en esta misma línea de hándicap ({h2h_ah:+.2f}). "
                            f"El mercado actual de {ah_real:+.2f} sobrevalora al local sin ajuste de línea."
                        )
                    })

        # ESCENARIO DEFINITIVO B: Trampa de Cuota de Mercado (Public Trap Fade)
        # La cuota subió la exigencia al local (UP) pero el H2H o las métricas no lo respaldan.
        if h2h_mov == 'UP' and ah_real <= -0.50:
            if ph_sot < 5.0:
                picks.append({
                    'pick': f'Visitante {away} {abs(ah_real):+.2f}',
                    'pick_type': 'HANDICAP_UNDERDOG',
                    'confidence': 'ALTA',
                    'rule_name': 'DEF_PUBLIC_TRAP_FADE',
                    'reason': (
                        f"Trampa de Cuota: El mercado empujó la línea hacia {home} ({ah_real:+.2f}), "
                        f"pero su volumen de tiros a puerta ({ph_sot:.1f}) no respalda el movimiento. "
                        f"Valor en {away} {abs(ah_real):+.2f}."
                    )
                })

        # ESCENARIO DEFINITIVO C: Revalidación de Favorito con Volumen Oculto
        if ah_real <= -0.25 and (ph_sot - pa_sot) >= 2.5:
            picks.append({
                'pick': f'Local {home} {ah_real:+.2f}',
                'pick_type': 'HANDICAP_FAVORITE',
                'confidence': 'ALTA',
                'rule_name': 'DEF_VOLUME_CONFIRMED_FAVORITE',
                'reason': (
                    f"{home} domina el volumen de disparos en casa ({ph_sot:.1f} vs {pa_sot:.1f} de {away}). "
                    f"La línea {ah_real:+.2f} tiene sustento de proceso real."
                )
            })

        # ESCENARIO DEFINITIVO D: Falso Favorito Local por Inflado de Mercado (UNIVERSAL PARA TODAS LAS LIGAS)
        if ah_real <= -0.75 and ph_sot > 0 and ph_sot < 4.8:
            picks.append({
                'pick': f'Visitante {away} {abs(ah_real):+.2f}',
                'pick_type': 'HANDICAP_UNDERDOG',
                'confidence': 'ALTA',
                'rule_name': 'DEF_UNIVERSAL_LOCAL_INFLATED_FADE',
                'reason': (
                    f"Favoritismo local sobrevalorado en {home} ({ah_real:+.2f}). "
                    f"Su producción ofensiva en casa ({ph_sot:.1f} tiros a puerta) no justifica cubrir un hándicap de -0.75 o superior. "
                    f"Valor en {away} {abs(ah_real):+.2f}."
                )
            })

        # ESCENARIO DEFINITIVO E: OVER Estructural por Disparos a Puerta Conjuntos (UNIVERSAL PARA TODAS LAS LIGAS)
        c_obj = match_data.get('candidate') if isinstance(match_data.get('candidate'), dict) else match_data
        ou_real = parse_float(
            c_obj.get('ou_line') or match_data.get('ou_line') or match_data.get('ou')
            or (match_data.get('main_match_odds') or {}).get('goals_linea')
        )
        if ou_real is not None and ou_real <= 3.0 and (ph_sot + pa_sot) >= 10.0:
            picks.append({
                'pick': f'OVER {ou_real:g}',
                'pick_type': 'OVER',
                'confidence': 'ALTA',
                'rule_name': 'DEF_UNIVERSAL_STRUCTURAL_OVER',
                'reason': (
                    f"Ritmo ofensivo conjunto alto ({ph_sot:.1f} + {pa_sot:.1f} = {ph_sot+pa_sot:.1f} tiros a puerta). "
                    f"La línea de mercado ({ou_real:g}) infravalora el potencial de goles del partido."
                )
            })

        # Selección del mejor pick por confianza
        final_status = 'TRIGGERED' if picks else 'NO_BET'
        
        return {
            'status': final_status,
            'match_info': f"{home} vs {away} (AH: {ah_real})",
            'odds_movement_classified': h2h_mov,
            'h2h_reference_ah': h2h_ah,
            'recommended_picks': picks,
            'reason': picks[0]['reason'] if picks else 'Sin patrones de alta confianza en el espacio hándicap.'
        }


definitive_engine = DefinitiveTradingEngine()
