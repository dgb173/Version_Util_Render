# Modulo: clave_dicotomica.py
# Motor de la Clave Dicotomica V7 - Sistema de Argumentacion Universal
# Devuelve picks reales: FAV_CUBRE / DOG_CUBRE / NO_BET (AH) y OVER / UNDER / NO_BET (OU)
# Incluye: U1-U8 + P9 | score_DRAW como dimension de neutralizacion
# Filosofia: entrar en la mente de la casa de apuestas y su sistema de colocacion de handicaps
from datetime import datetime
import re


ENGINE_VERSION = 'V7.0'

# Promocion basada en cinco bloques cronologicos, no en el porcentaje escrito
# originalmente en el nombre de la micro-regla.
AH_PRODUCTION_RULE_PREFIXES = (
    'MR-F4 ',
    'MR-F9 ',
    'MR-D15 ',
    'MR-D16 ',
)

AH_BLOCKING_FLAGS = {
    'F_PREV_GOLEADA_FALSA_CAOTICA',
    'DOG_AP_PRESSURE_ONLY',
    'EXTREME_LOWER_TO_H025',
}

# Puerta aprendida exclusivamente en el 80% cronologico anterior. Criterio:
# n >= 50, acierto decidido >= 53% y settlement medio > 0.03. El 20% final
# queda fuera de la seleccion y se usa solo para auditar generalizacion.
AH_VALIDATED_EXACT_LINES = (0.0, 0.25, 1.25, 1.50, 1.75)

AH_LINE_GATE_REASON = 'linea AH sin validacion cronologica suficiente'

# Expansion congelada con protocolo 60/20/20. La regla nacio en discovery,
# supero validation y solo despues se audito en el test final intacto.
AH_EXPANSION_RULE_ID = 'EXP_AH_01_HOME_FAV_M05_BOOKIE_NEUTRAL'

def parse_score(s):
    if not s or ':' not in str(s) or '?' in str(s): return None, None
    try:
        parts = str(s).split(':')
        return int(parts[0]), int(parts[1])
    except: return None, None

def parse_ah(v):
    if v is None or v == '' or v == 'N/A' or v == '?' or v == '-': return None
    try:
        if '/' in str(v):
            parts = str(v).split('/')
            return (float(parts[0]) + float(parts[1])) / 2
        return float(v)
    except: return None

def get_stats(rows):
    r = {}
    if not rows: return r
    for row in rows:
        lbl = (row.get('label') or '').strip()
        try:
            r[lbl] = {'h': float(str(row.get('home','0')).replace('%','')),
                      'a': float(str(row.get('away','0')).replace('%',''))}
        except: pass
    return r

def _get_draw_rate(standings, context='general'):
    """Extrae la tasa de empates del objeto standings.
    context: 'general', 'home' o 'away'."""
    try:
        if context == 'home':
            d = int(standings.get('home_draws', 0) or 0)
            total = int(standings.get('home_played', 0) or 0)
        elif context == 'away':
            d = int(standings.get('away_draws', 0) or 0)
            total = int(standings.get('away_played', 0) or 0)
        else:
            d = int(standings.get('draws', 0) or 0)
            total = int(standings.get('played', 0) or 0)
        return d / total if total > 0 else 0.0
    except:
        return 0.0

def _year_from_date(value):
    """Devuelve el año de una fecha flexible sin romper el motor si viene vacia."""
    txt = str(value or '').strip()
    if not txt or txt in {'-', 'N/A', '?'}:
        return None
    for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d'):
        try:
            return datetime.strptime(txt[:10], fmt).year
        except Exception:
            pass
    try:
        for token in txt.replace('/', '-').split('-'):
            if len(token) == 4 and token.isdigit():
                return int(token)
    except Exception:
        pass
    return None


def _parse_date(value):
    txt = str(value or '').strip().replace('Z', '+00:00')
    if not txt:
        return None
    try:
        return datetime.fromisoformat(txt).date()
    except Exception:
        pass
    token = txt.split(' ', 1)[0]
    for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d'):
        try:
            return datetime.strptime(token, fmt).date()
        except Exception:
            pass
    return None


def _same_team(a, b):
    left = ''.join(ch for ch in str(a or '').lower() if ch.isalnum())
    right = ''.join(ch for ch in str(b or '').lower() if ch.isalnum())
    return bool(left and right and (left == right or left in right or right in left))


def _first_ah(*values):
    for value in values:
        parsed = parse_ah(value)
        if parsed is not None:
            return parsed
    return None


def _movement_start(value):
    numbers = re.findall(r'[+\-]?\d+(?:[\.,]\d+)?', str(value or ''))
    return parse_ah(numbers[0].replace(',', '.')) if numbers else None


def _col3_payload(match):
    raw = match.get('h2h_col3') or {}
    if not isinstance(raw, dict):
        return {}, {}
    nested = raw.get('col3_data')
    return raw, nested if isinstance(nested, dict) else raw


def _no_bet_result(reason, *, fav='', dog='', h=0.0, ou_raw=0.0):
    """Return the complete V7 contract when prediction inputs are unusable."""
    return {
        'engine_version': ENGINE_VERSION,
        'ah': 'NO_BET',
        'ah_label': 'NO BET AH',
        'ou': 'NO_BET',
        'ou_label': 'NO BET OU',
        'raw_ah': 'NO_BET',
        'raw_ah_label': 'NO BET AH',
        'raw_ou': 'NO_BET',
        'raw_ou_label': 'NO BET OU',
        'prediction_tier_ah': 'NO_BET',
        'prediction_tier_ou': 'NO_BET',
        'core_ah': 'NO_BET',
        'core_prediction_tier_ah': 'NO_BET',
        'core_ah_gate_reasons': [reason],
        'confidence_ah': 'NONE',
        'confidence_ou': 'NONE',
        'ah_gate_reasons': [reason],
        'ou_gate_reasons': [reason],
        'production_ah_rules': [],
        'production_ou_rules': [],
        'validated_ah_line': False,
        'validated_ah_expansion': False,
        'expansion_ah_rule': None,
        'bookie_detector': {},
        'bookie_confirmation': 'NO_DATA',
        'blocking_flags': [],
        'argumentos': [reason],
        'notes': [reason],
        'flags': ['NO_DATA'],
        'edge_AH': 0,
        'edge_OU': 0,
        'score_F': 0,
        'score_D': 0,
        'score_DRAW': 0,
        'score_OVER': 0,
        'score_UNDER': 0,
        'draw_risk': 0,
        'draw_type': '',
        'mr_dog': [],
        'mr_fav': [],
        'mr_over': [],
        'mr_under': [],
        'base_cover': '',
        'base_stats': '',
        'pressure': '',
        'ah_fam': '',
        'ou_fam': '',
        'fav': fav,
        'dog': dog,
        'role_mode': '',
        'is_pickem': False,
        'h': h,
        'ou_raw': ou_raw,
        'RH': 0,
        'TH': 0,
        'stadium_RH': None,
        'learning_hooks': [],
        'quality': {
            'eligible': False,
            'score': 0,
            'evidence_blocks': 0,
            'stats_blocks': 0,
            'nonpast_contexts': [],
            'stale_contexts': [],
            'warnings': [reason],
        },
    }

def apply_key(m):
    """
    Clave Dicotomica V7 - Sistema de Argumentacion Universal
    =========================================================
    Siempre desde la vision del FAVORITO (F) cuando existe favorito de mercado.
    AH=0 -> PICKEM / DNB: no hay favorito real sin cuota adicional.
    En AH=0 se usa el local solo como referencia geometrica para poder medir
    residuales; el output se etiqueta como DNB/pickem, no como favorito/dog.

    Salidas AH: FAV_CUBRE | DOG_CUBRE | NO_BET
    Salidas OU: OVER | UNDER | NO_BET

    Reglas universales:
      U1 - FILTRO_OU_INFLADA
      U2 - CALIDAD_RELATIVA_INVERTIDA
      U3 - RESISTENCIA_LOCAL_HISTORICA
      U4 - TABLA_IGUALADA_FORMA_DECIDE
      U5 - HANDICAP_REPETIDO_VOLUMEN_OCULTO
      U6 - EMPATE_NEUTRALIZACION_JERARQUIA
      U7 - NO_CONFIRMACION_MERCADO
      U8 - INDIRECTA_ESPEJO_EMPATE
      P9 - MEJORA_REAL
    """
    score_F = 0.0
    score_D = 0.0
    score_DRAW = 0.0
    score_OVER = 0.0
    score_UNDER = 0.0
    draw_risk = 0.0
    mr_active_fav = []
    mr_active_dog = []
    mr_active_over = []
    mr_active_under = []
    argumentos = []
    flags = set()
    quality_warnings = []

    odds = m.get('main_match_odds', {})
    ah_raw = parse_ah(odds.get('ah_linea'))
    ou_raw = parse_ah(odds.get('goals_linea'))

    if ah_raw is None or ou_raw is None:
        return _no_bet_result('Sin odds suficientes')

    h = abs(ah_raw)
    is_pickem = h < 0.01
    fav_is_home = True if is_pickem else ah_raw > 0
    home_name = m.get('home_name', '')
    away_name = m.get('away_name', '')
    fav_name = home_name if fav_is_home else away_name
    dog_name = away_name if fav_is_home else home_name
    role_mode = 'PICKEM_DNB' if is_pickem else 'MARKET_FAVORITE'
    if is_pickem:
        flags.add('PICKEM_DNB')
        score_DRAW += 0.4
        draw_risk += 0.25
        argumentos.append(
            '[PICKEM] AH=0 no crea favorito visitante. Se lee como DNB/pickem; '
            'el local es solo referencia de calculo y el empate queda priorizado.'
        )

    # H2H general: la base puede traer campos directos o market_analysis_data.
    h2h = m.get('h2h_general') or {}
    market_data = m.get('market_analysis_data') or {}
    market_general = market_data.get('general') or {} if isinstance(market_data, dict) else {}
    h2h_score_str = (
        h2h.get('res6') or market_general.get('result') or market_general.get('score')
        or h2h.get('res1') or h2h.get('score') or ''
    )
    h2h_home_name = h2h.get('h2h_gen_home') or h2h.get('home_team') or ''
    h2h_away_name = h2h.get('h2h_gen_away') or h2h.get('away_team') or ''
    h2h_ah_value = _first_ah(
        h2h.get('ah6'), h2h.get('ah1'), h2h.get('ah_line'),
        _movement_start(market_general.get('movement')),
    )
    h2h_ah = h2h_ah_value if h2h_ah_value is not None else 0.0
    h2h_date_raw = market_general.get('date') or h2h.get('date6') or h2h.get('date1') or h2h.get('date')
    match_date_obj = _parse_date(m.get('match_date') or m.get('date') or m.get('start_time'))
    h2h_date_obj = _parse_date(h2h_date_raw)
    hg_h2h, ag_h2h = parse_score(h2h_score_str)
    if hg_h2h is None or (match_date_obj and h2h_date_obj and h2h_date_obj >= match_date_obj):
        reason = 'H2H no anterior al partido' if hg_h2h is not None else 'Sin H2H valido'
        return _no_bet_result(reason, fav=fav_name, dog=dog_name, h=h, ou_raw=ou_raw)

    fav_in_h2h_is_home = _same_team(fav_name, h2h_home_name) if h2h_home_name else fav_is_home
    goles_F_h2h = hg_h2h if fav_in_h2h_is_home else ag_h2h
    goles_D_h2h = ag_h2h if fav_in_h2h_is_home else hg_h2h
    RH = (goles_F_h2h - goles_D_h2h) - h
    TH = hg_h2h + ag_h2h

    delta = h - abs(h2h_ah)
    historical_favorite = h2h_home_name if h2h_ah > 0 else h2h_away_name if h2h_ah < 0 else ''
    if historical_favorite:
        same_favorite = _same_team(fav_name, historical_favorite)
        role_reversed = not same_favorite
    elif abs(h2h_ah) > 0.01:
        # Sin nombres, la orientacion ya usada para calcular RH permite comparar
        # el rol del favorito actual con el signo de la linea historica.
        same_favorite = fav_in_h2h_is_home == (h2h_ah > 0)
        role_reversed = not same_favorite
    else:
        same_favorite = False
        role_reversed = False
    if is_pickem and historical_favorite:
        pressure = 'PRESSURE_FAV_REMOVED'
    elif not is_pickem and not same_favorite:
        pressure = 'PRESSURE_NEW_FAV'
        flags.add('PRESSURE_NEW_FAV')
    elif delta >= 0.75:
        pressure = 'PRESSURE_RAISE_AGGRESSIVE'
    elif delta >= 0.25:
        pressure = 'PRESSURE_RAISE'
    elif delta <= -0.25:
        pressure = 'PRESSURE_LOWER'
    else:
        pressure = 'PRESSURE_SAME'

    if h == 0:       ah_fam = 'H0'
    elif h == 0.25:  ah_fam = 'H025'
    elif h <= 0.75:  ah_fam = 'H05_075'
    elif h <= 1.25:  ah_fam = 'H1_125'
    elif h <= 1.75:  ah_fam = 'H15_175'
    else:            ah_fam = 'H2_PLUS'

    if ou_raw <= 2.25:   ou_fam = 'OU_LOW'
    elif ou_raw <= 2.75: ou_fam = 'OU_MID'
    elif ou_raw <= 3.5:  ou_fam = 'OU_HIGH'
    else:                ou_fam = 'OU_EXTREME'

    # NODO 2: BASE COVER
    if RH >= 0.25:
        base_cover = 'COVER'; score_F += 2.0
        argumentos.append(f'[RAIZ] H2H COVER (RH={RH:+.2f}): F habria cubierto AH{ah_raw:+.2f} en el H2H. Base favorable al favorito.')
    elif RH <= -0.25:
        base_cover = 'FAIL'; score_D += 2.0
        argumentos.append(f'[RAIZ] H2H FAIL (RH={RH:+.2f}): F NO habria cubierto. Base favorable al dog.')
    else:
        base_cover = 'PUSH'; score_F += 0.5; score_D += 0.5
        argumentos.append(f'[RAIZ] H2H PUSH (RH={RH:+.2f}): zona bisagra, el contexto decide.')

    # PRESION
    if pressure == 'PRESSURE_RAISE_AGGRESSIVE':
        score_F += 1.0
        argumentos.append(f'[PRESION] Casa SUBE fuerte (DELTA={delta:+.2f}). El mercado cree en el favorito mas que en el H2H.')
    elif pressure == 'PRESSURE_RAISE':
        score_F += 0.5
        argumentos.append(f'[PRESION] Casa sube levemente (DELTA={delta:+.2f}).')
    elif pressure == 'PRESSURE_LOWER':
        score_F += 0.3
        argumentos.append(f'[PRESION] Casa BAJA la exigencia (DELTA={delta:+.2f}). Dudas sobre el favorito.')
        if role_reversed:
            flags.add('ROL_INVERTIDO')
            argumentos.append('[ROL INVERTIDO] El equipo que era DOG en H2H ahora es el FAVORITO. Cambio de jerarquia — requiere validacion extra.')

    # STATS H2H
    h2h_stats = get_stats(h2h.get('stats_rows', []))
    side_F = 'h' if fav_in_h2h_is_home else 'a'
    side_D = 'a' if fav_in_h2h_is_home else 'h'
    sap_h2h_F = h2h_stats.get('Tiros a Puerta', {}).get(side_F, 0)
    sap_h2h_D = h2h_stats.get('Tiros a Puerta', {}).get(side_D, 0)
    ap_h2h_F  = h2h_stats.get('Ataques Peligrosos', {}).get(side_F, 0)
    ap_h2h_D  = h2h_stats.get('Ataques Peligrosos', {}).get(side_D, 0)
    atk_h2h_F = h2h_stats.get('Ataques', {}).get(side_F, 0)
    atk_h2h_D = h2h_stats.get('Ataques', {}).get(side_D, 0)
    tiros_F   = h2h_stats.get('Tiros', {}).get(side_F, 0)
    tiros_D   = h2h_stats.get('Tiros', {}).get(side_D, 0)
    dom_count = sum([sap_h2h_F > sap_h2h_D, ap_h2h_F > ap_h2h_D, tiros_F > tiros_D])
    if dom_count >= 2:   base_stats = 'STATS_STRONG_FOR'
    elif dom_count == 1: base_stats = 'STATS_LEAN_FOR'
    else:                base_stats = 'STATS_NEUTRAL_OR_AGAINST'

    # VOLUMEN SIN CONVERSION
    vsf_flag = (base_stats == 'STATS_STRONG_FOR' and base_cover in ['PUSH', 'FAIL'])
    if vsf_flag:
        flags.add('VOLUMEN_SIN_CONVERSION')
        score_UNDER += 0.5
        argumentos.append('[VSC] F domina stats del H2H pero NO gano. Patron: eficacia baja, proceso sin conversion -> indica partido cerrado en goles.')

    # H025-8 / universal: el volumen no puede mandar si resultado+handicap no pagaron.
    # En lineas bajas esto suele crear dog/empate o, si el OU es alto, bloqueo del under automatico.
    if h <= 0.25 and base_stats == 'STATS_STRONG_FOR' and base_cover in ['PUSH', 'FAIL']:
        flags.add('RESULTADO_BLOQUEA_VOLUMEN')
        score_F -= 0.45
        score_D += 0.55
        score_DRAW += 0.35
        if ou_raw >= 3.25:
            score_OVER += 0.35
        else:
            score_UNDER += 0.25
        argumentos.append(
            '[H025-8 RESULTADO_BLOQUEA_VOLUMEN] El favorito tenia volumen, pero resultado+handicap no lo pagaron. '
            'En AH bajo las stats no pueden decidir solas; se abre rama dog/empate y se separa el O/U.'
        )

    # PREVIA F
    lhm = (m.get('last_home_match') or {}) if fav_is_home else (m.get('last_away_match') or {})
    lam_D = (m.get('last_away_match') or {}) if fav_is_home else (m.get('last_home_match') or {})
    fav_prev_score = lhm.get('score', '')
    fav_prev_ah_raw = parse_ah(lhm.get('handicap_line_raw')) or 0.0
    fg, fa = parse_score(fav_prev_score)
    goles_F_prev = fg if fav_is_home else fa
    goles_rival_prev = fa if fav_is_home else fg
    margin_F_prev = (goles_F_prev - goles_rival_prev) if goles_F_prev is not None else None
    RF = (margin_F_prev - h) if margin_F_prev is not None else None
    total_F_prev = (goles_F_prev + goles_rival_prev) if goles_F_prev is not None else None
    fav_prev_stats = get_stats(lhm.get('stats_rows', []))
    sot_fav = fav_prev_stats.get('Tiros a Puerta', {}).get('h' if fav_is_home else 'a', 0)
    fav_cover_as_fav = False
    fav_recent = 'UNKNOWN'

    if RF is not None:
        if RF >= 0.25:
            fav_recent = 'COVER'; score_F += 1.0
            if (fav_is_home and fav_prev_ah_raw > 0) or (not fav_is_home and fav_prev_ah_raw < 0):
                fav_cover_as_fav = True; score_F += 0.5
                argumentos.append(f'[PREVIA F] F cubrio como FAVORITO (RF={RF:+.2f}). Forma reciente avala el rol de hoy.')
            else:
                score_D += 0.3
                argumentos.append(f'[PREVIA F] F cubrio como DOG (RF={RF:+.2f}). No valida ser favorito hoy — posible trampa narrativa.')
        elif RF <= -0.25:
            fav_recent = 'FAIL'; score_D += 0.5
            argumentos.append(f'[PREVIA F] F FALLO (RF={RF:+.2f}). Forma reciente negativa del favorito.')
        else:
            fav_recent = 'PUSH'
            argumentos.append(f'[PREVIA F] F en PUSH (RF={RF:+.2f}).')

    if goles_rival_prev is not None and goles_rival_prev >= 4:
        flags.add('F_GOLEADO_PREV')
        argumentos.append(f'[GOLEADA F] F recibio {goles_rival_prev} goles en su previa. Puede haber inflado el OU artificialmente.')

    # PREVIA D
    dog_prev_score = lam_D.get('score', '')
    dg, da_g = parse_score(dog_prev_score)
    goles_D_prev = da_g if fav_is_home else dg
    goles_rival_D = dg if fav_is_home else da_g
    total_D_prev = (goles_D_prev + goles_rival_D) if goles_D_prev is not None else None
    dog_prev_ah_raw = parse_ah(lam_D.get('handicap_line_raw')) or 0.0
    dog_prev_stats = get_stats(lam_D.get('stats_rows', []))
    sot_dog = dog_prev_stats.get('Tiros a Puerta', {}).get('a' if fav_is_home else 'h', 0)
    goleada_D = (goles_rival_D is not None and goles_rival_D >= 3)
    if goleada_D:
        flags.add('GOLEADA_D_ENCAJADA')
        argumentos.append(f'[GOLEADA D] Dog recibio {goles_rival_D} goles en previa. Puede estar descontado en precio — no inflar automaticamente al favorito.')
    dog_recent_goals = total_D_prev
    dog_goals_2_minus = (dog_recent_goals is not None and dog_recent_goals <= 2)
    if dog_goals_2_minus:
        flags.add('DOG_CERRADO')
        argumentos.append('[DOG CERRADO] El dog viene de partido con <=2 goles totales. Perfil goleador bajo.')
    dog_RF = None
    if goles_D_prev is not None:
        dog_RF = (goles_D_prev - goles_rival_D) + h
    if dog_RF is not None and dog_RF >= 0:
        argumentos.append(f'[PREVIA D] Dog cubrio en su ultima previa (dog_RF={dog_RF:+.2f}). Viene en forma.')
    elif dog_RF is not None:
        argumentos.append(f'[PREVIA D] Dog no cubrio en su ultima previa (dog_RF={dog_RF:+.2f}).')

    dog_prev_margin = (goles_D_prev - goles_rival_D) if goles_D_prev is not None else None
    dog_prev_team_pressure = None
    if dog_prev_ah_raw is not None:
        # La linea esta expresada desde el local de ese partido previo.
        # Si el dog actual venia como visitante, su presion es la inversa.
        dog_prev_team_pressure = (-dog_prev_ah_raw) if fav_is_home else dog_prev_ah_raw
    dog_won_as_fav_or_pickem = (
        dog_prev_margin is not None and dog_prev_margin > 0 and
        dog_prev_team_pressure is not None and dog_prev_team_pressure >= -0.01
    )
    if h <= 0.5 and dog_won_as_fav_or_pickem:
        flags.add('DOG_GANA_COMO_FAV_O_PICKEM')
        score_D += 0.75
        argumentos.append(
            f'[DOG FAV/PICKEM] El no favorito actual gano su previa con rol de favorito/pickem '
            f'(presion={dog_prev_team_pressure:+.2f}, margen={dog_prev_margin:+.1f}). '
            f'En AH bajo esto es una variable dominante de vida competitiva.'
        )

    # CLASIFICACIÓN DE PARTIDO PREVIO (Nivel 3)
    def classify_prev_match(goals_for, goals_against, sot_for, sot_against, ap_for, ap_against):
        if goals_for is None or goals_against is None:
            return "NORMAL"
        sot_edge = (sot_for - sot_against) if (sot_for is not None and sot_against is not None) else 0
        ap_edge = (ap_for - ap_against) if (ap_for is not None and ap_against is not None) else 0
        if goals_for >= 4 and sot_edge > 0 and ap_edge > 0:
            return "GOLEADA_LIMPIA"
        if goals_for >= 4 and (sot_edge < 0 or ap_edge < 0):
            return "GOLEADA_FALSA_CAOTICA"
        if goals_for == 0 and (sot_for == 0 or sot_for is None):
            return "ATAQUE_ESTERIL"
        if goals_against >= 3 and sot_against is not None and sot_against >= 6:
            return "DEFENSA_ROTA"
        return "NORMAL"

    # Previa F
    side_F_prev = 'h' if fav_is_home else 'a'
    side_rival_F_prev = 'a' if fav_is_home else 'h'
    sot_rival_F = fav_prev_stats.get('Tiros a Puerta', {}).get(side_rival_F_prev, 0)
    ap_fav_prev = fav_prev_stats.get('Ataques Peligrosos', {}).get(side_F_prev, 0)
    ap_rival_F_prev = fav_prev_stats.get('Ataques Peligrosos', {}).get(side_rival_F_prev, 0)

    fav_prev_classification = classify_prev_match(
        goals_for=goles_F_prev,
        goals_against=goles_rival_prev,
        sot_for=sot_fav,
        sot_against=sot_rival_F,
        ap_for=ap_fav_prev,
        ap_against=ap_rival_F_prev
    )
    flags.add(f"F_PREV_{fav_prev_classification}")
    argumentos.append(f"[PREVIA F CLASIFICACION] Clasificacion: {fav_prev_classification}")

    # Previa D
    side_D_prev = 'a' if fav_is_home else 'h'
    side_rival_D_prev = 'h' if fav_is_home else 'a'
    sot_dog_prev = dog_prev_stats.get('Tiros a Puerta', {}).get(side_D_prev, 0)
    sot_rival_D_prev = dog_prev_stats.get('Tiros a Puerta', {}).get(side_rival_D_prev, 0)
    ap_dog_prev = dog_prev_stats.get('Ataques Peligrosos', {}).get(side_D_prev, 0)
    ap_rival_D_prev = dog_prev_stats.get('Ataques Peligrosos', {}).get(side_rival_D_prev, 0)

    dog_prev_classification = classify_prev_match(
        goals_for=goles_D_prev,
        goals_against=goles_rival_D,
        sot_for=sot_dog_prev,
        sot_against=sot_rival_D_prev,
        ap_for=ap_dog_prev,
        ap_against=ap_rival_D_prev
    )
    flags.add(f"D_PREV_{dog_prev_classification}")
    argumentos.append(f"[PREVIA D CLASIFICACION] Clasificacion: {dog_prev_classification}")
    comp = m.get('comparativas_indirectas', {})
    left  = comp.get('left', {})
    right = comp.get('right', {})
    ind_fav = left if fav_is_home else right
    ind_dog = right if fav_is_home else left
    lg, la = parse_score(ind_fav.get('score', ''))
    rg, ra = parse_score(ind_dog.get('score', ''))
    margin_F_ind = ind_fav_goals = ind_fav_ah_raw = None
    margin_D_ind = ind_dog_goals = ind_dog_ah_raw = None

    if lg is not None:
        fav_loc = ind_fav.get('localia', 'H')
        margin_F_ind = (lg - la) if fav_loc == 'H' else (la - lg)
        ind_fav_goals = lg + la
        ind_fav_ah_value = _first_ah(
            ind_fav.get('ah_line'), ind_fav.get('handicap_line_raw'),
            ind_fav.get('handicap'), ind_fav.get('ah'),
        )
        ind_fav_ah_raw = ind_fav_ah_value if ind_fav_ah_value is not None else 0.0
    if rg is not None:
        dog_loc = ind_dog.get('localia', 'A')
        margin_D_ind = (ra - rg) if dog_loc == 'A' else (rg - ra)
        ind_dog_goals = rg + ra
        ind_dog_ah_value = _first_ah(
            ind_dog.get('ah_line'), ind_dog.get('handicap_line_raw'),
            ind_dog.get('handicap'), ind_dog.get('ah'),
        )
        ind_dog_ah_raw = ind_dog_ah_value if ind_dog_ah_value is not None else 0.0

    diff_F = None
    if margin_F_ind is not None and margin_D_ind is not None:
        diff_F = margin_F_ind - margin_D_ind
        if diff_F >= 2.0:
            score_F += 1.0
            argumentos.append(f'[INDIRECTA] F mas solido que D vs rival comun (diff={diff_F:+.1f}). Indirectas avalan al favorito.')
        elif diff_F <= -1.5:
            score_D += 1.0
            argumentos.append(f'[INDIRECTA] D mas solido que F vs rival comun (diff={diff_F:+.1f}). Indirectas avalan al dog.')

        if h <= 0.25 and diff_F <= -2.0 and dog_RF is not None and dog_RF >= 0:
            flags.add('INDIRECTA_DOG_SUPERA_FAV_AH_BAJO')
            score_D += 0.75
            score_DRAW += 0.25
            argumentos.append(
                f'[H025 INDIRECTA DOG] En AH bajo, el dog mejora claramente la comparativa indirecta '
                f'(diff={diff_F:+.1f}) y ademas viene de cubrir. Esta rama puede decantar el sistema hacia dog/empate.'
            )

    # TABLA
    rank_h = m.get('home_standings', {}).get('ranking')
    rank_a = m.get('away_standings', {}).get('ranking')
    wins_h = m.get('home_standings', {}).get('wins', -1)
    wins_a = m.get('away_standings', {}).get('wins', -1)
    tabla_igualada = False
    rank_fav = rank_dog = wins_fav = wins_dog = None
    try:
        rank_fav = int(rank_h) if fav_is_home else int(rank_a)
        rank_dog = int(rank_a) if fav_is_home else int(rank_h)
        wins_fav = int(wins_h) if fav_is_home else int(wins_a)
        wins_dog = int(wins_a) if fav_is_home else int(wins_h)
        if abs(rank_fav - rank_dog) <= 1 and abs(wins_fav - wins_dog) <= 1:
            tabla_igualada = True; flags.add('TABLA_IGUALADA')
        if rank_fav < rank_dog:
            score_F += 0.3
            argumentos.append(f'[TABLA] F mejor en tabla ({rank_fav}º vs {rank_dog}º). Pequena ventaja estructural.')
        else:
            score_D += 0.3; flags.add('TABLE_FAV_WORSE')
            argumentos.append(f'[TABLA] D igual o mejor en tabla ({rank_fav}º vs {rank_dog}º). El mercado sobrevalora al favorito.')
    except: pass

    # ======================================================
    # REGLAS UNIVERSALES
    # ======================================================

    # U1: FILTRO_OU_INFLADA
    u1_active = False
    if (total_F_prev is not None and
        total_F_prev >= ou_raw + 1.5 and
        TH <= ou_raw - 0.75 and
        (total_D_prev is None or total_D_prev <= ou_raw)):
        u1_active = True; flags.add('OU_INFLADA')
        score_UNDER += 1.0
        argumentos.append(
            f'[U1 OU_INFLADA] Previa de F tuvo {total_F_prev} goles (outlier vs linea {ou_raw}). '
            f'H2H real solo tuvo {TH} goles. OU fijado por el outlier, no por el patron real del cruce. '
            f'Correccion: ignorar F_prev en OU, sumar UNDER +1.0.')

    # U2: CALIDAD_RELATIVA_INVERTIDA
    u2_active = False
    if (margin_F_ind is not None and margin_D_ind is not None and
        margin_D_ind >= 1 and margin_F_ind <= -1 and
        ind_dog_ah_raw is not None and ind_fav_ah_raw is not None and
        abs(ind_dog_ah_raw) >= abs(ind_fav_ah_raw) + 0.5):
        u2_active = True; flags.add('CALIDAD_RELATIVA_INVERTIDA')
        score_D += 1.5
        argumentos.append(
            f'[U2 CALIDAD_INVERTIDA] D gano vs rival comun (margin={margin_D_ind:+.1f}) siendo underdog ({ind_dog_ah_raw:+.2f}). '
            f'F perdio vs rival comun (margin={margin_F_ind:+.1f}) siendo favorito ({ind_fav_ah_raw:+.2f}). '
            f'La jerarquia real del partido esta INVERTIDA respecto a la cuota.')

    # STADIUM_OLD + H2H ESTADIO (memoria de patron vs prueba definitiva)
    # El nombre canonico de la base es h2h_stadium; se mantiene el alias legacy.
    h2h_est = m.get('h2h_stadium') or m.get('h2h_estadio') or {}
    market_stadium = market_data.get('stadium') or {} if isinstance(market_data, dict) else {}
    h2h_est_score_str = (
        market_stadium.get('result') or market_stadium.get('score')
        or h2h_est.get('res1') or h2h_est.get('res6') or h2h_est.get('score') or ''
    )
    h2h_est_date = (
        market_stadium.get('date') or h2h_est.get('date1')
        or h2h_est.get('date6') or h2h_est.get('date') or ''
    )
    h2h_est_ah = _first_ah(
        h2h_est.get('ah1'), h2h_est.get('ah6'), h2h_est.get('ah_line'),
        _movement_start(market_stadium.get('movement')),
    )
    same_h2h_as_general = False
    is_stadium_old = False
    stadium_total = None
    stadium_RH = None
    if h2h_est_date and len(h2h_est_date) >= 4:
        try:
            h2h_year = _year_from_date(h2h_est_date)
            match_year = _year_from_date(m.get('match_date') or m.get('date', '')) or 2026
            if h2h_year is not None and (match_year - h2h_year) >= 3:
                is_stadium_old = True
                flags.add('STADIUM_OLD')
                argumentos.append(
                    f'[STADIUM_OLD] El H2H estadio es antiguo ({h2h_est_date}). '
                    f'Se degrada su peso a confirmacion secundaria si contradice al H2H general o previas.'
                )
        except Exception:
            pass

    hg_est_base, ag_est_base = parse_score(h2h_est_score_str)
    if hg_est_base is not None:
        stadium_total = hg_est_base + ag_est_base
        goles_F_est = hg_est_base if fav_is_home else ag_est_base
        goles_D_est = ag_est_base if fav_is_home else hg_est_base
        stadium_RH = (goles_F_est - goles_D_est) - h
        same_match_id = bool(
            h2h_est.get('match1_id') and h2h.get('match6_id')
            and str(h2h_est.get('match1_id')) == str(h2h.get('match6_id'))
        )
        same_h2h_as_general = same_match_id or (
            h2h_est_score_str == h2h_score_str and str(h2h_est_date) == str(h2h_date_raw)
        )
        if not same_h2h_as_general:
            stadium_weight = 0.35 if is_stadium_old else 1.05
            if stadium_RH >= 0.25:
                score_F += stadium_weight
                argumentos.append(
                    f'[H2H ESTADIO] F cubre en este estadio (RH_est={stadium_RH:+.2f}). '
                    f'Peso={stadium_weight:.2f} por {"memoria antigua" if is_stadium_old else "precedente util"}.'
                )
            elif stadium_RH <= -0.25:
                score_D += stadium_weight
                argumentos.append(
                    f'[H2H ESTADIO] F no cubre en este estadio (RH_est={stadium_RH:+.2f}). '
                    f'Peso={stadium_weight:.2f} por {"memoria antigua" if is_stadium_old else "precedente util"}.'
                )
            else:
                score_DRAW += 0.35 if is_stadium_old else 0.65
                argumentos.append(
                    f'[H2H ESTADIO] Residual de estadio en zona push (RH_est={stadium_RH:+.2f}). '
                    'Refuerza empate/linea fina.'
                )

    # U3: RESISTENCIA_LOCAL_HISTORICA
    u3_active = False
    hg2, ag2 = parse_score(h2h_est_score_str) if h2h_est else (None, None)
    h2h_draw = (hg_h2h == ag_h2h)
    h2h2_draw = (hg2 is not None and hg2 == ag2)
    both_push = h2h_draw and (h2h2_draw or hg2 is None)
    if both_push and not fav_is_home:
        u3_active = True; flags.add('RESISTENCIA_LOCAL')
        score_D += 0.5; draw_risk += 0.3
        argumentos.append(
            '[U3 RESISTENCIA_LOCAL] Los H2H previos terminaron en empate con F como visitante favorito. '
            'El local tiene capacidad estructural de resistir al visitante incluso siendo el favorito del mercado.')

        # Ajuste por STADIUM_OLD si contradice el H2H general reciente que no es empate
        if is_stadium_old and not h2h_draw:
            score_D -= 0.25
            argumentos.append('[STADIUM_OLD] Se reduce el peso de U3 local porque el estadio es de hace >=3 temporadas y no hay confirmacion reciente.')

        th2 = (hg2 + ag2) if hg2 is not None else TH
        if TH <= 2 and th2 <= 2:
            score_UNDER += 0.5; flags.add('DOBLE_PUSH_UNDER')
            argumentos.append('[U3+UNDER] Ambos H2H tuvieron <=2 goles. Patron DOBLE_PUSH_UNDER: partido estructuralmente cerrado.')

    # U4: TABLA_IGUALADA_FORMA_DECIDE
    u4_active = False
    if tabla_igualada:
        u4_active = True
        flags.add('U4_TABLA_IGUALADA')
        argumentos.append(
            f'[U4 TABLA_IGUALADA] Ambos equipos tienen la misma o similar posicion en tabla. '
            f'No hay jerarquía estructural — la FORMA RECIENTE decide con peso doble.'
        )
        if fav_recent == 'FAIL' and dog_RF is not None and dog_RF >= 0:
            score_D += 0.5
            argumentos.append('[U4] F fallo en su previa Y D cubrio. Con tabla igualada esto es determinante — Dog favorecido.')
        elif fav_recent == 'COVER' and fav_cover_as_fav and (dog_RF is None or dog_RF < 0):
            score_F += 0.5
            argumentos.append('[U4] F cubrio como favorito Y D no cubrio. Con tabla igualada esto es determinante — Fav favorecido.')

    # U5: HANDICAP_REPETIDO_VOLUMEN_OCULTO
    u5_active = False
    if h2h_ah == ah_raw and base_cover == 'FAIL':
        tiros_dom_dog = (tiros_D > tiros_F)
        ap_dom_dog = (ap_h2h_D > ap_h2h_F)
        ind_favorece_dog = (margin_D_ind is not None and margin_F_ind is not None and margin_D_ind > margin_F_ind)

        if (tiros_dom_dog or ap_dom_dog) and ind_favorece_dog:
            u5_active = True
            flags.add('HANDICAP_REPETIDO_VOLUMEN_OCULTO')
            score_D += 2.0
            score_UNDER += 0.5
            argumentos.append(
                f'[U5 VOLUMEN_OCULTO] Se repite el handicap ({ah_raw}) del H2H Estadio. '
                f'En ese partido F gano en el marcador (0:1/1:2) pero el Dog {dog_name} domino en volumen '
                f'(Tiros: {tiros_D} vs {tiros_F}, AP: {ap_h2h_D} vs {ap_h2h_F}). '
                f'Las comparativas indirectas confirman superioridad del Dog hoy. La linea esta inflada por el marcador historico ficticio. score_D += 2.0'
            )

    # P9: MEJORA_REAL
    p9_active = False
    try:
        if (not fav_is_home and delta <= -0.75 and rank_fav is not None and
            rank_fav <= 3 and RH <= -2.0):
            p9_active = True; flags.add('MEJORA_REAL')
            score_D -= 1.0; score_OVER += 1.0
            argumentos.append(
                f'[P9 MEJORA_REAL] Visitante top3 (pos={rank_fav}) con linea bajada {delta:+.2f} pese a H2H negativo (RH={RH:+.2f}). '
                f'El mercado tiene info sobre mejora real del equipo esta temporada. '
                f'NO apostar contra el mercado en AH. Explotar OU (dos equipos de calidad = goles).')
    except: pass

    # ---- U6: EMPATE POR NEUTRALIZACION DE JERARQUIA ----
    # Cuando la casa ve al favorito mejor pero NO se atreve a darle -0.25
    # Eso no es apoyo al favorito. Es zona de empate.
    u6_active = False
    ranking_gap = abs(rank_fav - rank_dog) if rank_fav is not None and rank_dog is not None else 0
    home_st = m.get('home_standings', {})
    away_st = m.get('away_standings', {})
    draw_rate_home = _get_draw_rate(home_st, 'home')
    draw_rate_away = _get_draw_rate(away_st, 'away')
    draw_density = (draw_rate_home >= 0.35 or draw_rate_away >= 0.35)
    h2h_margin_low = (abs(goles_F_h2h - goles_D_h2h) <= 1)
    fav_contextually_better = (rank_fav is not None and rank_dog is not None and rank_fav < rank_dog and ranking_gap >= 4)
    market_no_upgrade = (abs(h) <= 0.25 and delta <= 0)

    if (abs(h) <= 0.25 and
        (ranking_gap >= 4 or fav_recent == 'COVER') and
        draw_density and
        h2h_margin_low):
        u6_active = True; flags.add('EMPATE_NEUTRALIZACION')
        score_DRAW += 1.5; score_D += 0.5; score_F -= 0.5
        argumentos.append(
            f'[U6 EMPATE_NEUTRALIZACION] La casa ve superioridad del favorito (gap={ranking_gap} posiciones, previa={fav_recent}) '
            f'pero NO le da -0.25. Eso grita: "no tengo fuerza para ponerle como ganador". '
            f'Draw rate local={draw_rate_home:.0%}, visitante={draw_rate_away:.0%}. '
            f'H2H cerrado (margin={abs(goles_F_h2h - goles_D_h2h)}). El empate es el centro geometrico del partido.')

    # ---- U7: NO CONFIRMACION DEL MERCADO TRAS BUENA PREVIA ----
    # Si F viene de cubrir y esta mejor en tabla, pero la linea NO sube, el mercado FRENA la lectura
    u7_active = False
    fav_table_better = (rank_fav is not None and rank_dog is not None and rank_fav < rank_dog)
    if (fav_recent == 'COVER' and
        fav_table_better and
        (h <= abs(h2h_ah) or h == 0) and
        (dog_RF is not None and dog_RF >= 0 or draw_density)):
        u7_active = True; flags.add('NO_CONFIRMACION_MERCADO')
        score_F -= 0.8; score_DRAW += 1.0; score_D += 0.7
        argumentos.append(
            '[U7 NO_CONFIRMACION] F viene de cubrir y esta mejor en tabla, pero la linea NO mejora. '
            'El mercado NO compra victoria clara del favorito. Si viene bien y la linea no sube, '
            'el mercado esta frenando la lectura. Empate o Dog mas probable que lo que dice el score bruto.')

    # ---- U8: INDIRECTA ESPEJO DE EMPATE ----
    # Ambos equipos llegan al rival comun con residuales similares
    u8_active = False
    if margin_F_ind is not None and margin_D_ind is not None:
        indirect_gap = abs(margin_F_ind - margin_D_ind)
        if indirect_gap <= 0.5 and abs(h) <= 0.25:
            u8_active = True; flags.add('INDIRECTA_ESPEJO_EMPATE')
            score_DRAW += 1.0
            argumentos.append(
                f'[U8 ESPEJO_EMPATE] Ambos equipos tuvieron rendimiento similar vs rival comun '
                f'(F={margin_F_ind:+.1f}, D={margin_D_ind:+.1f}, gap={indirect_gap:.1f}). '
                f'Con AH={ah_raw} el mercado no separa. Las indirectas confirman la simetria. '
                f'El empate es el centro del mapa.')

    # U19: FAVORITO MANTENIDO TRAS DERROTA H2H CON PROCESO IGUALADO
    # Patron Miami AC - Brevard SC: el publico ve X2 por H2H perdido + rival comun,
    # pero la casa mantiene la misma linea del favorito. Si el H2H fue de proceso
    # igualado, la derrota se trata como varianza y la linea repetida es una senal.
    u19_active = False
    h2h_margin_fav = goles_F_h2h - goles_D_h2h
    h2h_process_close_count = sum([
        abs((sap_h2h_F or 0) - (sap_h2h_D or 0)) <= 1,
        abs((ap_h2h_F or 0) - (ap_h2h_D or 0)) <= 6,
        abs((atk_h2h_F or 0) - (atk_h2h_D or 0)) <= 8,
        abs((tiros_F or 0) - (tiros_D or 0)) <= 3,
    ])
    h2h_process_equal = h2h_process_close_count >= 3
    same_favorite_same_line = (
        not is_pickem and
        not role_reversed and
        h2h_ah != 0 and
        abs(abs(h2h_ah) - h) <= 0.01
    )
    public_x2_story = (
        base_cover == 'FAIL' and
        h2h_margin_fav <= -1 and
        (
            (margin_F_ind is not None and margin_D_ind is not None and margin_F_ind <= 0 and margin_D_ind >= 2)
            or (dog_RF is not None and dog_RF >= 2 and fav_recent != 'COVER')
        )
    )
    direct_goal_memory_open = (TH >= 4 or (ou_raw >= 3.0 and TH >= ou_raw + 0.75))

    if same_favorite_same_line and public_x2_story and h2h_process_equal and h <= 0.75:
        u19_active = True
        flags.add('MARKET_REJECTS_OBVIOUS_DOG_X2')
        score_F += 3.4
        score_D -= 1.0
        score_DRAW -= 0.2
        mr_active_fav.append('U19 FAV_MANTENIDO_TRAS_H2H_FAIL_IGUALADO')
        argumentos.append(
            f'[U19 MARKET_REJECTS_X2] El favorito perdio el H2H ({h2h_score_str}) y el dog tiene relato publico '
            f'(rival comun / previa), pero la casa mantiene la misma linea h={h:.2f}. '
            f'El H2H fue de proceso igualado (close={h2h_process_close_count}/4), no dominio real del dog. '
            f'La linea repetida rechaza el X2 obvio. score_F += 3.4, score_D -= 1.0.'
        )
        if direct_goal_memory_open and ou_raw >= 3.0:
            score_OVER += 2.0
            score_UNDER = max(0.0, score_UNDER - 0.5)
            mr_active_over.append('U19 OVER_POR_REVANCHA_ABIERTA')
            flags.add('U19_OVER_REVANCHA_ABIERTA')
            argumentos.append(
                f'[U19 OVER] H2H directo abierto ({TH} goles) y OU={ou_raw}. '
                f'La revancha del favorito no invalida goles; proyecta favorito/local + over.'
            )

    # U20: REBAJA BRUTAL DESDE H1_PLUS/H2_PLUS PROTEGE DOG Y ENFRIA OVER
    # Patron Helsinki B (W) - PK Keski Uusimaa (W): H2H 4-1 con AH 3,
    # hoy AH 1.25. El marcador viejo cubriria la linea actual, pero la casa
    # reduce mas de 1 gol y medio la exigencia. Eso no vende repeticion de 4-1:
    # protege al visitante y suele inflar el OU por memoria de goleada.
    u20_active = False
    huge_line_drop = (
        not is_pickem and
        pressure == 'PRESSURE_LOWER' and
        abs(h2h_ah) >= 2.25 and
        (abs(h2h_ah) - h) >= 1.0 and
        1.0 <= h <= 1.5
    )
    old_blowout_now_cheaper = (
        base_cover == 'COVER' and
        (goles_F_h2h - goles_D_h2h) >= 3 and
        TH >= 4
    )
    dog_has_current_counterweight = (
        (margin_D_ind is not None and margin_D_ind >= 1.5)
        or dog_prev_classification == 'ATAQUE_ESTERIL'
        or (goles_rival_D is not None and goles_rival_D >= 3)
    )
    col3_u20_raw, col3_u20 = _col3_payload(m)
    col3_total_for_u20 = None
    try:
        if isinstance(col3_u20, dict):
            gh = col3_u20.get('goles_home')
            ga = col3_u20.get('goles_away')
            if gh is not None and ga is not None:
                col3_total_for_u20 = int(gh) + int(ga)
            else:
                ch, ca = parse_score(col3_u20.get('score') or col3_u20.get('result') or '')
                if ch is not None:
                    col3_total_for_u20 = ch + ca
    except Exception:
        col3_total_for_u20 = None
    mirror_enfria_total = col3_total_for_u20 is not None and col3_total_for_u20 <= 2

    if huge_line_drop and old_blowout_now_cheaper and dog_has_current_counterweight:
        u20_active = True
        flags.add('HUGE_DROP_PROTECTS_DOG')
        score_F -= 1.6
        score_D += 2.6
        mr_active_dog.append('U20 REBAJA_BRUTAL_PROTEGE_DOG')
        argumentos.append(
            f'[U20 REBAJA_BRUTAL] El H2H fue {h2h_score_str} con AH historico {h2h_ah:+.2f}, '
            f'pero hoy baja a h={h:.2f} (delta={delta:+.2f}). Aunque el 4-1 cubriria la linea actual, '
            f'la casa no repite la exigencia: protege al dog y rechaza la goleada vieja como expectativa central. '
            f'score_F -= 1.6, score_D += 2.6.'
        )
        if ou_raw >= 3.75:
            flags.add('OU_INFLADO_POR_GOLEADA_ANTIGUA')
            score_OVER = max(0.0, score_OVER - 2.2)
            score_UNDER += 2.4
            if mirror_enfria_total:
                score_UNDER += 0.6
                flags.add('COL3_ENFRIA_OU_EXTREMO')
            mr_active_under.append('U20 UNDER_OU_INFLADO_POR_REBAJA_AH')
            argumentos.append(
                f'[U20 UNDER] OU={ou_raw} esta apoyado por memoria de 4-1, pero la rebaja AH brutal '
                f'indica menos margen y menos persecucion de goleada. '
                f'Col3_total={col3_total_for_u20}. Bloquear OVER y favorecer UNDER.'
            )

    # H025-7: rebaja extrema hacia 0.25 no es apoyo libre al favorito.
    # Si la casa baja desde una exigencia alta al minimo, hay proteccion/igualdad salvo validacion clara.
    extreme_lower_to_025 = (h == 0.25 and abs(h2h_ah) >= 1.0 and pressure == 'PRESSURE_LOWER')
    if extreme_lower_to_025:
        flags.add('EXTREME_LOWER_TO_H025')
        fav_validated_after_drop = (
            base_cover == 'COVER' and
            (fav_recent == 'COVER' or base_stats == 'STATS_STRONG_FOR') and
            not ('RESULTADO_BLOQUEA_VOLUMEN' in flags)
        )
        if fav_validated_after_drop:
            score_F += 0.35
            argumentos.append(
                '[H025-7 REBAJA PROTECTORA] La linea cae desde handicap alto a 0.25, '
                'pero hay cobertura/forma suficiente. Se mantiene al favorito, con stake prudente.'
            )
        else:
            score_F -= 0.85
            score_D += 0.55
            score_DRAW += 0.75
            argumentos.append(
                '[H025-7 REBAJA EXTREMA] La linea cae desde handicap alto a 0.25 sin validacion clara. '
                'No es premio al favorito: es aviso de igualdad, dog o empate.'
            )

    # ---- DESAMBIGUACIÓN UNIVERSAL DE HANDICAP (RAMAS 1-4) ----
    latest_h2h_fav_sot_edge = sap_h2h_F - sap_h2h_D
    latest_h2h_fav_ap_edge = ap_h2h_F - ap_h2h_D
    latest_h2h_fav_shots_edge = tiros_F - tiros_D

    volume_revenge = (
        latest_h2h_fav_sot_edge >= 3 or
        latest_h2h_fav_ap_edge >= 20 or
        latest_h2h_fav_shots_edge >= 5
    )

    dog_side_rival = 'h' if fav_is_home else 'a'
    dog_prev_conceded_goals = goles_rival_D if goles_rival_D is not None else 0
    dog_prev_conceded_sot = dog_prev_stats.get('Tiros a Puerta', {}).get(dog_side_rival, 0)
    dog_prev_conceded_shots = dog_prev_stats.get('Tiros', {}).get(dog_side_rival, 0)

    dog_broken = (
        dog_prev_conceded_goals >= 3 or
        dog_prev_conceded_sot >= 6 or
        dog_prev_conceded_shots >= 16
    )

    open_total = (ou_raw >= 3.0)

    h2h_gen_total = TH
    h2h_gen_raw = m.get('h2h_general', {})
    if h2h_gen_raw:
        hg_gen, ag_gen = parse_score(h2h_gen_raw.get('score'))
        if hg_gen is not None:
            h2h_gen_total = hg_gen + ag_gen

    draw_lock = (
        ou_raw <= 2.25 or
        (draw_rate_home >= 0.35 and draw_rate_away >= 0.30) or
        (TH <= 1 and h2h_gen_total <= 2)
    )

    table_gap_fav = (rank_dog - rank_fav) if (rank_fav is not None and rank_dog is not None) else 0
    dog_bottom_zone = (rank_dog is not None and rank_dog >= 9)
    structural_gap = (table_gap_fav >= 5 or dog_bottom_zone)

    indirect_neutral = (diff_F is not None and abs(diff_F) <= 0.25)

    market_insists_against_h2h = (
        h >= 0.5 and (base_cover == 'FAIL' or base_cover == 'PUSH' or (goles_D_h2h is not None and goles_F_h2h is not None and goles_D_h2h > goles_F_h2h))
    )

    if market_insists_against_h2h:
        flags.add('MARKET_INSISTS_AGAINST_H2H')
        argumentos.append('[DESAMBIGUACION] El mercado exige ganar al favorito (h>=0.5) contra un H2H de memoria negativo.')

        # RAMA 1: Revancha validada por volumen
        if volume_revenge and dog_broken and open_total:
            flags.add('RAMA_REVANCHA_VALIDADA')
            score_F += 2.0
            score_OVER += 1.0
            score_D -= 1.0
            argumentos.append(
                f'[RAMA 1 - REVANCHA] Gano el Dog el marcador antiguo, pero ya no controla el emparejamiento. '
                f'El favorito viene con volumen real (SOT edge={latest_h2h_fav_sot_edge:+.1f}, AP edge={latest_h2h_fav_ap_edge:+.1f}). '
                f'El dog llega roto atras (goles concedidos={dog_prev_conceded_goals}, Tiros={dog_prev_conceded_shots}). '
                f'Confirmamos favoritismo F. score_F += 2.0, score_D -= 1.0, score_OVER += 1.0.'
            )

        # RAMA 2: Desgaste estructural
        elif structural_gap and not draw_lock:
            flags.add('RAMA_DESGASTE_ESTRUCTURAL')
            score_F += 1.5
            score_D -= 0.7
            draw_risk -= 0.3
            argumentos.append(
                f'[RAMA 2 - DESGASTE] El H2H es incomodo, pero el Dog esta muy deteriorado por tabla/forma (gap={table_gap_fav} pos, dog_bottom={dog_bottom_zone}). '
                f'El mercado no respeta la memoria antigua porque el nivel actual ya no acompana. score_F += 1.5, score_D -= 0.7.'
            )

        # RAMA 3: Favorito bloqueado hacia empate
        elif draw_lock and not volume_revenge:
            flags.add('RAMA_FAVORITO_BLOQUEADO_EMPATE')
            score_DRAW += 2.0
            score_D += 0.8
            score_F -= 1.0
            argumentos.append(
                f'[RAMA 3 - BLOQUEO] F tiene mejor tabla/nombre, pero el partido esta comprimido por OU o empates. '
                f'El mercado le exige ganar, pero la geometria apunta a empate. score_DRAW += 2.0, score_F -= 1.0.'
            )

        # RAMA 4: Trampa de continuidad falsa
        elif not volume_revenge and not dog_broken and indirect_neutral:
            flags.add('RAMA_CONTINUIDAD_FALSA')
            score_D += 2.0
            score_F -= 1.0
            score_DRAW += 0.7
            argumentos.append(
                f'[RAMA 4 - TRAMPA_CONTINUIDAD] El mercado vende continuidad del favorito, pero el Dog no llega roto, '
                f'el volumen H2H no valida al favorito y las indirectas no separan. score_D += 2.0, score_F -= 1.0.'
            )
        else:
            argumentos.append('[DESAMBIGUACION] Ninguna rama del patron de desambiguacion se cumplio al 100% — sin ajustes extraordinarios.')

    # ---- U10: ANOMALÍA DE LÍNEA BAJA (FRENADO DEL MERCADO / REPETICION TRAS GOLEADA) ----
    u10_active = False
    h2h_ah_abs = abs(h2h_ah) if h2h_ah is not None else 0.0

    # Comprobar si el handicap se mantiene conservador/repetido a pesar del optimismo extremo
    handicap_repetido_o_bajo = (h <= 1.25 and (h == h2h_ah_abs or h2h_ah_abs == 0))
    fav_goleo_prev = (margin_F_prev is not None and margin_F_prev >= 3)
    dog_tiene_gol = (goles_D_prev is not None and goles_D_prev >= 2)

    if (diff_F is not None and diff_F >= 3.0 and
        fav_recent == 'COVER' and
        handicap_repetido_o_bajo and
        fav_goleo_prev and
        dog_tiene_gol and
        rank_fav is not None and rank_dog is not None and rank_fav <= 4 and rank_dog >= 8):
        u10_active = True
        flags.add('ANOMALIA_LINEA_BAJA_REPETIDA')
        score_F -= 4.0
        score_D += 3.5
        score_DRAW += 0.5
        argumentos.append(
            f'[U10 ANOMALIA_LINEA_BAJA] El handicap se repite o es conservador (h={h}) a pesar de la goleada previa de F '
            f'(margin={margin_F_prev:+.1f}) y las indirectas (diff_F={diff_F:+.2f}). El Dog {dog_name} tiene pegada (goles_prev={goles_D_prev}). '
            f'La casa NO compra la goleada y el valor real se desplaza al Dog. Ajuste agresivo: score_F -= 4.0, score_D += 3.5.'
        )

    # ---- U11: FAVORITO_1.25_CON_DOG_GOLEADOR_PERSISTENTE ----
    u11_active = False

    # 1. Extraer goles del H2H Estadio
    hg_est, ag_est = parse_score(h2h_est_score_str) if h2h_est else (None, None)
    goles_F_h2h_stadium = hg_est if fav_is_home else ag_est
    goles_D_h2h_stadium = ag_est if fav_is_home else hg_est

    # Goles H2H General
    goles_F_h2h_general = goles_F_h2h
    goles_D_h2h_general = goles_D_h2h

    # Calculo de persistencia y ataque activo del Dog
    dog_goals_h2h_stadium_val = goles_D_h2h_stadium if goles_D_h2h_stadium is not None else 0
    dog_goals_h2h_general_val = goles_D_h2h_general if goles_D_h2h_general is not None else 0

    dog_scoring_persistence = (dog_goals_h2h_stadium_val >= 2 and dog_goals_h2h_general_val >= 2)
    dog_current_attack_alive = (
        (goles_D_prev is not None and goles_D_prev >= 2)
        or (sot_dog is not None and sot_dog >= 5)
    )

    # Margenes de H2H
    h2h_stadium_fav_margin = (goles_F_h2h_stadium - goles_D_h2h_stadium) if (goles_F_h2h_stadium is not None and goles_D_h2h_stadium is not None) else 0
    h2h_general_fav_margin = (goles_F_h2h_general - goles_D_h2h_general) if (goles_F_h2h_general is not None and goles_D_h2h_general is not None) else 0

    # Dominios de AP en previa de F
    ap_fav_prev = 0
    ap_rival_F_prev = 0
    if lhm:
        fav_prev_stats = get_stats(lhm.get('stats_rows', []))
        side_F_prev = 'h' if fav_is_home else 'a'
        side_rival_prev = 'a' if fav_is_home else 'h'
        ap_fav_prev = fav_prev_stats.get('Ataques Peligrosos', {}).get(side_F_prev, 0)
        ap_rival_F_prev = fav_prev_stats.get('Ataques Peligrosos', {}).get(side_rival_prev, 0)

    fav_prev_ap_edge = ap_fav_prev - ap_rival_F_prev
    fav_big_win_but_not_territorial = (
        margin_F_prev is not None and margin_F_prev >= 3
        and fav_prev_ap_edge <= 0
    )

    # Activacion del patron
    if h == 1.25:
        if h2h_stadium_fav_margin == 2 and h2h_general_fav_margin == 2:
            if dog_scoring_persistence:
                if dog_current_attack_alive:
                    if ou_raw >= 3.25:
                        u11_active = True
                        flags.add('FAVORITO_125_DOG_PERSISTENTE')
                        score_D += 2.0
                        score_OVER += 1.2
                        score_DRAW += 0.7
                        score_F -= 1.2

                        # Si ademas se cumple que la previa del favorito fue una big win sin AP dominante
                        if fav_big_win_but_not_territorial:
                            score_F -= 0.8
                            score_D += 0.5
                            flags.add('FAV_BIG_WIN_AP_NEGATIVO')

                        argumentos.append(
                            f'[U11 FAVORITO_1.25_DOG_PERSISTENTE] OLS-KuPS pattern: h=1.25, '
                            f'H2H de estadio y general ganados por margen exacto de 2, pero con Dog marcando >=2 goles. '
                            f'Dog activo (prev_goals={goles_D_prev}) y OU alto ({ou_raw}). '
                            f'El margen es fragil. score_D += 2.0, score_OVER += 1.2, score_F -= 1.2.'
                        )

    # ---- U12: BLOQUEO SECO POR VOLUMEN ESTÉRIL (Dziugas-Zalgiris Pattern) ----
    u12_active = False

    # 1. Variables de volumen de ataque esteril del Favorito
    fav_ap_sterile = (sot_fav == 0 and ap_fav_prev >= 60) if (sot_fav is not None) else False

    # 2. Variables de volumen superficial del Dog
    # Extraer AP y Tiros de la previa de D
    ap_dog_prev = 0
    tiros_dog_prev = 0
    tiros_rival_dog_prev = 0
    if lam_D:
        dog_prev_stats = get_stats(lam_D.get('stats_rows', []))
        side_D_prev = 'a' if fav_is_home else 'h'
        ap_dog_prev = dog_prev_stats.get('Ataques Peligrosos', {}).get(side_D_prev, 0)
        tiros_dog_prev = dog_prev_stats.get('Tiros', {}).get(side_D_prev, 0)
        tiros_rival_dog_prev = dog_prev_stats.get('Tiros', {}).get('h' if fav_is_home else 'a', 0)

    dog_prev_shots_edge = tiros_dog_prev - tiros_rival_dog_prev
    dog_surface_volume = (dog_prev_shots_edge > 0 and ap_dog_prev <= 20)

    sterile_attack_bilateral = (
        (sot_fav is not None and sot_fav <= 1)
        and (
            (ap_dog_prev is not None and ap_dog_prev <= 20)
            or (sot_dog is not None and sot_dog <= 2)
            or dog_surface_volume
        )
    )

    # 3. H2H Marcador Inflado por Eficacia
    fav_h2h_margin = (goles_F_h2h - goles_D_h2h) if (goles_F_h2h is not None and goles_D_h2h is not None) else 0
    inflated_h2h_score = (
        fav_h2h_margin >= 2
        and sap_h2h_F is not None and sap_h2h_F <= 4
        and (ap_h2h_D is not None and ap_h2h_F is not None and (ap_h2h_D - ap_h2h_F) >= 15)
    )

    # 4. Espejo 0-0 de compresion maxima (Ancla de Goles Cero)
    col3_raw, col3 = _col3_payload(m)
    col3_score_str = ''
    if isinstance(col3, dict):
        gh = col3.get('goles_home')
        ga = col3.get('goles_away')
        if gh is not None and ga is not None:
            col3_score_str = f"{gh}:{ga}"
        else:
            col3_score_str = col3.get('score', '')

    mirror_zero_zero = (col3_score_str == '0:0')

    zero_zero_anchor = (
        mirror_zero_zero
        or h2h_score_str == '0:0'
        or (ind_fav.get('score', '') == '0:0')
        or (ind_dog.get('score', '') == '0:0')
    )

    # Activacion en lineas de handicap cortas (0, 0.25)
    if h <= 0.25:
        if sterile_attack_bilateral and inflated_h2h_score and zero_zero_anchor:
            u12_active = True
            flags.add('BLOQUEO_SECO_0_0')
            score_DRAW += 2.0
            score_UNDER += 2.0
            score_F -= 1.0
            score_D += 1.0   # Le sumamos a D para empujar a DOG_CUBRE (+0.25)
            score_OVER -= 1.5
            score_UNDER += 0.5

            # Alertas en argumentos
            argumentos.append(
                f'[U12 BLOQUEO_SECO_0_0] Dziugas-Zalgiris Pattern: Linea corta (h={h}), '
                f'ataque esteril bilateral (F_sot={sot_fav}, D_ap={ap_dog_prev}), H2H previo inflado y espejo/indirectas con 0-0. '
                f'Proyeccion de empate seco 0-0. score_DRAW += 2.0, score_UNDER += 2.5, score_D += 1.0.'
            )

    # ---- U13: FAVORITO -1 PUSH SECO (SE do Gama vs Mixto EC Pattern) ----
    u13_active = False

    # 1. dog_volume_but_total_compressed
    dog_volume_but_total_compressed = (
        sot_dog is not None and sot_dog >= 6
        and ou_raw <= 2.25
    )
    if dog_volume_but_total_compressed:
        flags.add('DOG_VOL_COMPRESSED')
        score_OVER -= 0.8
        score_UNDER += 0.8

    # 2. dog_ap_is_pressure_not_finish
    dog_h2h_ap_edge = (ap_h2h_D - ap_h2h_F) if (ap_h2h_D is not None and ap_h2h_F is not None) else 0
    dog_h2h_sot_edge = (sap_h2h_D - sap_h2h_F) if (sap_h2h_D is not None and sap_h2h_F is not None) else 0

    if dog_h2h_ap_edge >= 25 and dog_h2h_sot_edge <= 0:
        flags.add('DOG_AP_PRESSURE_ONLY')
        score_UNDER += 0.4

    # 3. market_rejects_recent_overs
    is_women_league = ('(w)' in home_name.lower() or '(w)' in away_name.lower())
    both_prev_scored_heavy = (goles_F_prev is not None and goles_F_prev >= 3 and goles_D_prev is not None and goles_D_prev >= 3)

    market_rejects_recent_overs = (
        total_F_prev is not None and total_F_prev >= 4
        and total_D_prev is not None and total_D_prev >= 4
        and ou_raw <= 2.25
        and not (is_women_league or both_prev_scored_heavy)
    )
    if market_rejects_recent_overs:
        flags.add('MKT_REJECTS_OVERS')
        score_UNDER += 1.2

    # 4. H2H Reciente Victoria del Favorito por 1 (margen 1)
    fav_h2h_margin = (goles_F_h2h - goles_D_h2h) if (goles_F_h2h is not None and goles_D_h2h is not None) else 0
    h2h_recent_fav_win_by_1 = (fav_h2h_margin == 1)

    # Activacion de U13 en handicap exacto de 1
    if h == 1.0:
        if h2h_recent_fav_win_by_1:
            if ou_raw <= 2.25:
                u13_active = True
                flags.add('FAVORITO_1_PUSH_SECO')

                # Ajustes de score
                score_UNDER += 1.4
                score_F -= 1.0     # Evitamos que se vaya a FAV_CUBRE
                score_D -= 0.5     # Evitamos que se vaya a DOG_CUBRE completo

                # Registramos argumentos
                argumentos.append(
                    f'[U13 FAVORITO_-1_PUSH_SECO] Gama-Mixto Pattern: Linea de handicap 1 (h={h}), '
                    f'H2H previo ganado por fav por la minima (margen={fav_h2h_margin}) y OU comprimido ({ou_raw}). '
                    f'El mercado ancla el partido en victoria minima (PUSH). score_UNDER += 1.4, score_F -= 1.0, score_D -= 0.5.'
                )

    # ---- U14: REPETICIÓN DE LÍNEA AJUSTADA POR PROCESO (Jelgava Pattern) ----
    u14_active = False
    h2h_end_line = abs(h2h_ah) if h2h_ah is not None else 0.0
    line_repeated = abs(h - h2h_end_line) <= 0.25

    fav_h2h_ap_edge = (ap_h2h_F - ap_h2h_D) if (ap_h2h_F is not None and ap_h2h_D is not None) else 0
    fav_h2h_shots_edge = (tiros_F - tiros_D) if (tiros_F is not None and tiros_D is not None) else 0
    fav_h2h_sot_edge = (sap_h2h_F - sap_h2h_D) if (sap_h2h_F is not None and sap_h2h_D is not None) else 0

    fav_process_h2h_positive = (
        fav_h2h_ap_edge >= 20
        or fav_h2h_shots_edge >= 5
        or fav_h2h_sot_edge >= 2
    )
    fav_recent_correction = (
        fav_recent == 'COVER'
        and sot_fav is not None
        and sot_fav >= 5
    )

    if line_repeated and fav_process_h2h_positive and fav_recent_correction:
        u14_active = True
        flags.add('REPETICION_LINEA_PROCESO')
        score_F += 1.8
        score_D -= 0.8
        argumentos.append(
            f'[U14 REPETICION_PROCESO] Jelgava Pattern: La casa repite la linea (h={h}) vs H2H previo. '
            f'El favorito tuvo volumen positivo en H2H previo (AP_edge={fav_h2h_ap_edge:+.1f}) y su previa corrige el remate '
            f'(sot={sot_fav}). Se mantiene la confianza en el Favorito. score_F += 1.8, score_D -= 0.8.'
        )

    # ---- U15: REBAJA PROTECTORA DEL FAVORITO (Riga Pattern) ----
    u15_active = False
    fav_prev_ah = fav_prev_ah_raw if fav_prev_ah_raw is not None else 0.0
    fav_failed_hard_line = (fav_recent == 'FAIL' and abs(fav_prev_ah) >= 1.25)
    current_line_protected = (h <= 0.75)
    h2h_validates_fav = (base_cover == 'COVER' and fav_h2h_sot_edge >= 3)

    if fav_failed_hard_line and current_line_protected and h2h_validates_fav:
        u15_active = True
        flags.add('REBAJA_PROTECTORA_FAV')
        score_F += 1.7
        argumentos.append(
            f'[U15 REBAJA_PROTECTORA] Riga Pattern: F fallo linea dura en previa (ah_prev={fav_prev_ah:+.2f}), '
            f'pero el mercado rebaja y protege la linea actual (h={h}). El H2H lo valida (base_cover={base_cover}, SOT_edge={fav_h2h_sot_edge:+.1f}). '
            f'Se repite favorito en linea comoda. score_F += 1.7.'
        )

    # ---- U16: FAVORITO 0.25 CAPADO POR GOLEADA FALSA (Minnesota vs Tacoma Pattern) ----
    u16_active = False

    # 1. line_repeated_025
    line_repeated_025 = (h == 0.25 and h2h_end_line == 0.25)

    # 2. h2h_fav_process
    h2h_fav_process = (
        fav_h2h_sot_edge >= 3
        or fav_h2h_shots_edge >= 5
        or fav_h2h_ap_edge >= 15
    )

    # 3. h2h_not_won
    h2h_not_won = (
        h2h_score_str in ['draw', '1:1']
        or fav_h2h_margin <= 0
    )

    # 4. fav_big_win_false
    fav_big_win_false = (fav_prev_classification == 'GOLEADA_FALSA_CAOTICA')

    # 5. dog_alive
    dog_alive = (
        goles_rival_D is not None and goles_rival_D <= 1
        and sot_rival_D_prev is not None and sot_rival_D_prev <= 4
    )

    # 6. indirect_process_flip
    fav_ind_margin_raw = margin_F_ind if margin_F_ind is not None else 0
    dog_ind_margin_raw = margin_D_ind if margin_D_ind is not None else 0

    fav_prev_sot_edge = (sot_fav - sot_rival_F) if (sot_fav is not None and sot_rival_F is not None) else 0
    dog_prev_sot_edge = (sot_dog_prev - sot_rival_D_prev) if (sot_dog_prev is not None and sot_rival_D_prev is not None) else 0

    # Mapear stats indirectas si existen
    sot_ind_fav_F = 0
    sot_ind_fav_rival = 0
    if ind_fav:
        ind_fav_stats = get_stats(ind_fav.get('stats_rows', []))
        loc_fav_ind = ind_fav.get('localia', 'H')
        side_F_ind = 'h' if loc_fav_ind == 'H' else 'a'
        side_rival_ind = 'a' if loc_fav_ind == 'H' else 'h'
        sot_ind_fav_F = ind_fav_stats.get('Tiros a Puerta', {}).get(side_F_ind, 0)
        sot_ind_fav_rival = ind_fav_stats.get('Tiros a Puerta', {}).get(side_rival_ind, 0)

    sot_ind_dog_D = 0
    sot_ind_dog_rival = 0
    if ind_dog:
        ind_dog_stats = get_stats(ind_dog.get('stats_rows', []))
        loc_dog_ind = ind_dog.get('localia', 'A')
        side_D_ind = 'h' if loc_dog_ind == 'H' else 'a'
        side_rival_ind = 'a' if loc_dog_ind == 'H' else 'h'
        sot_ind_dog_D = ind_dog_stats.get('Tiros a Puerta', {}).get(side_D_ind, 0)
        sot_ind_dog_rival = ind_dog_stats.get('Tiros a Puerta', {}).get(side_rival_ind, 0)

    fav_ind_sot_edge = sot_ind_fav_F - sot_ind_fav_rival
    dog_ind_sot_edge = sot_ind_dog_D - sot_ind_dog_rival

    indirect_process_flip = (
        (fav_ind_margin_raw > dog_ind_margin_raw and fav_ind_sot_edge < dog_ind_sot_edge)
        or (margin_F_prev is not None and dog_RF is not None and margin_F_prev > dog_RF and fav_prev_sot_edge < dog_prev_sot_edge)
    )

    # 7. open_total
    open_total = (ou_raw >= 3.0)

    if (
        line_repeated_025
        and h2h_fav_process
        and h2h_not_won
        and fav_big_win_false
        and dog_alive
        and indirect_process_flip
        and open_total
    ):
        u16_active = True
        flags.add('FAV_025_CAPADO_X2_OVER')
        score_D += 1.5      # Soporte a X2
        score_DRAW += 1.2
        score_OVER += 1.5
        score_F -= 0.8

        argumentos.append(
            f'[U16 FAV_025_CAPADO] Minnesota-Tacoma Pattern: Linea de handicap 0.25 (h={h}) repetida, '
            f'H2H previo empatado pero con volumen positivo de F. F viene de previa con goleada falsa/caotica '
            f'(sot_edge={fav_prev_sot_edge:+.1f}), el Dog viene vivo y las indirectas muestran flip de proceso. '
            f'El favorito esta capado por su defensa. Proyeccion de X2 y Over. score_D += 1.5, score_DRAW += 1.2, score_OVER += 1.5, score_F -= 0.8.'
        )

    # ---- U17: INVERSIÓN DE MERCADO VALIDADA POR PREVIA (Kadhimiya SC vs Al Hussein Pattern) ----
    u17_active = False

    # 1. Inversion de mercado: El H2H general o de estadio fue negativo/empate para F (base_cover in ['FAIL', 'PUSH']),
    # pero hoy el mercado lo sitúa como favorito corto (h <= 0.5 y ah_raw de F es el favorito).
    market_flip = (
        not is_pickem
        and
        base_cover in ['FAIL', 'PUSH']
        and h <= 0.5
    )

    # 2. Previa del favorito validada: limpia y solida
    fav_prev_validated = (
        fav_prev_classification in ['GOLEADA_LIMPIA', 'NORMAL']
        and fav_recent == 'COVER'
    )

    # 3. Dog no llega en racha de goleada limpia
    dog_not_heavy = (dog_prev_classification != 'GOLEADA_LIMPIA')

    if market_flip and fav_prev_validated and dog_not_heavy:
        u17_active = True
        flags.add('MARKET_FLIP_VALIDATED')
        score_F += 1.8
        score_D -= 0.8
        argumentos.append(
            f'[U17 MARKET_FLIP] Kadhimiya SC vs Al Hussein Pattern: El mercado invierte favorito contra H2H historico '
            f'negativo (base_cover={base_cover}), pero la forma reciente de F lo valida (prev_classification={fav_prev_classification}, cover={fav_recent}). '
            f'Se sigue la direccion del mercado. score_F += 1.8, score_D -= 0.8.'
        )

    # ---- MICRO-REGLAS AH ----
    if base_stats == 'STATS_LEAN_FOR' and margin_F_prev is not None and margin_F_prev >= 3:
        score_D += 2.5; mr_active_dog.append('MR-D1 TRAMPA NARRATIVA 80%')
        argumentos.append('[MR-D1] TRAMPA: F aparenta dominar en H2H pero su previa fue goleada amplía como dog.')
    h2_extreme_ou_inflation = (
        ah_fam == 'H2_PLUS' and base_stats == 'STATS_STRONG_FOR' and ou_raw >= 4.0
    )
    if h2_extreme_ou_inflation:
        score_D += 2.5
        mr_active_dog.append('MR-D2 H2+OU4 INFLACION')

    # MR-D3 era equivalente a MR-D2 y duplicaba el score. Se conserva una sola senal.

    # MR-D4: RAISE_AGGRESSIVE + DOG_RECENT_GOALS_2_MINUS -> DOG 76.5%
    if pressure == 'PRESSURE_RAISE_AGGRESSIVE' and dog_goals_2_minus:
        score_D += 2.0
        mr_active_dog.append('MR-D4 RAISE+DOG_CERRADO 76.5%')

    # MR-D5: AH_025 + STATS_STRONG_FOR + IND_DOG_STRONG_FOR -> DOG 75%
    ind_dog_strong = (margin_D_ind is not None and margin_D_ind >= 2)
    if ah_fam == 'H025' and base_stats == 'STATS_STRONG_FOR' and ind_dog_strong:
        score_D += 2.0
        mr_active_dog.append('MR-D5 AH025+STATS+IND_D 75%')

    # MR-D7: DOG_RECENT_STATS_NEUTRAL + TABLE_FAV_WORSE -> DOG 73.3%
    dog_stats_dom = dog_prev_stats.get('Ataques Peligrosos', {})
    dog_neutral = (sot_dog <= 3)
    if dog_neutral and 'TABLE_FAV_WORSE' in flags:
        score_D += 1.8
        mr_active_dog.append('MR-D7 DOG_NEUTRAL+TABLE 73.3%')

    # MR-D8: AH_1_125 + NEW_FAV + IND_FAV_COVER_FAIL -> DOG 73.3%
    ind_fav_fail = (margin_F_ind is not None and margin_F_ind < 0)
    if ah_fam == 'H1_125' and pressure == 'PRESSURE_NEW_FAV' and ind_fav_fail:
        score_D += 1.8
        mr_active_dog.append('MR-D8 NEW_FAV+IND_FAIL 73.3%')

    # MR-D14: AH_025 + FAV_RECENT_STATS_LEAN_FOR -> DOG 69.4%
    fav_stats_lean = (base_stats in ['STATS_LEAN_FOR'])
    if ah_fam == 'H025' and fav_stats_lean:
        score_D += 1.3
        mr_active_dog.append('MR-D14 AH025+LEAN 69.4%')

    # MR-D15: TOTAL_OVER_LINE + OU_4_PLUS -> DOG 69.2%
    h2h_over = (TH - ou_raw) >= 0.25
    if h2h_over and ou_raw >= 4.0:
        score_D += 1.3
        mr_active_dog.append('MR-D15 H2H_OVER_OU4')

    # MR-D16: BASE_COVER + OU_4_PLUS -> DOG 69.2%
    if base_cover == 'COVER' and ou_raw >= 4.0:
        score_D += 1.3
        mr_active_dog.append('MR-D16 BASE_COVER_OU4')

    # MR-F1: AH_LOW + TOTAL_OVER_LINE + IND_FAV_COVER_PUSH -> FAV 75%
    ind_fav_push = (margin_F_ind is not None and abs(margin_F_ind) <= 1)
    if h <= 0.5 and h2h_over and ind_fav_push:
        score_F += 2.0
        mr_active_fav.append('MR-F1 LOW+OVER+IND_PUSH 75%')

    # MR-F2: AH_LOW + STATS_NEUTRAL + FAV_RECENT_STATS_STRONG_AGAINST -> FAV 73.1%
    fav_stats_strong_against = (sot_fav >= 4 and RF is not None and RF < 0)
    if h <= 0.5 and base_stats in ['STATS_NEUTRAL_OR_AGAINST'] and fav_stats_strong_against:
        score_F += 1.8
        mr_active_fav.append('MR-F2 LOW+NEUTRAL+STRONG_AGAINST 73.1%')

    # MR-F3: H05_075 + TOTAL_UNDER_LINE + IND_FAV_STATS_NEUTRAL -> FAV 70.4%
    h2h_under = (TH - ou_raw) <= -0.25
    ind_fav_neutral = (margin_F_ind is not None and abs(margin_F_ind) <= 1)
    if ah_fam == 'H05_075' and h2h_under and ind_fav_neutral:
        score_F += 1.5
        mr_active_fav.append('MR-F3 H05+UNDER+IND_NEUTRAL 70.4%')

    # MR-F4: AH_15 + IND_FAV_VALIDATES -> FAV 68.8%
    if ah_fam == 'H15_175' and diff_F is not None and diff_F >= 1.5:
        score_F += 1.5
        mr_active_fav.append('MR-F4 H15_INDIRECTA_VALIDA')

    # MR-F9: H05_075 + SAME + DOG_RECENT_DRAW -> FAV 65.6%
    dog_draw = (goles_D_prev is not None and goles_D_prev == goles_rival_D)
    if ah_fam == 'H05_075' and pressure == 'PRESSURE_SAME' and dog_draw:
        score_F += 1.0
        mr_active_fav.append('MR-F9 H05_LINEA_IGUAL_DOG_EMPATA')

    # ---- MICRO-REGLAS OU ----
    # MR-OU1: STATS_LEAN_FOR + IND_FAV_STATS_NEUTRAL + OU_MID -> UNDER 85.7%
    if base_stats == 'STATS_LEAN_FOR' and ind_fav_neutral and ou_fam == 'OU_MID':
        score_UNDER += 3.0
        mr_active_under.append('MR-OU1 UNDER 85.7%')

    # MR-OU2: AH_025 + H2H_UNDER + IND_DOG_MARGIN_POS1 -> UNDER 84.6%
    ind_dog_pos1 = (margin_D_ind is not None and margin_D_ind >= 1)
    if ah_fam == 'H025' and h2h_under and ind_dog_pos1:
        score_UNDER += 2.8
        mr_active_under.append('MR-OU2 H025_H2H_UNDER_IND_DOG_POS')

    # MR-OU3: AH_LOW + NEW_FAV + DOG_RECENT_LEAN_AGAINST -> UNDER 82.1%
    if h <= 0.5 and pressure == 'PRESSURE_NEW_FAV' and sot_dog <= 2:
        score_UNDER += 2.5
        mr_active_under.append('MR-OU3 LOW+NEWFAV+DOG_UNDER 82.1%')

    # MR-OU5: FAIL + IND_DOG_STATS + OU_HIGH -> UNDER 75%
    if base_cover == 'FAIL' and ind_dog_strong and ou_fam in ['OU_HIGH', 'OU_EXTREME']:
        score_UNDER += 2.0
        mr_active_under.append('MR-OU5 FAIL+IND_D+OUHIGH 75%')

    # MR-OU7: NEW_FAV + STATS_STRONG_FOR + OU_LOW -> UNDER 72.7%
    if pressure == 'PRESSURE_NEW_FAV' and base_stats == 'STATS_STRONG_FOR' and ou_fam == 'OU_LOW':
        score_UNDER += 1.8
        mr_active_under.append('MR-OU7 NEWFAV+STATS+OULOW 72.7%')

    # MR-OV1: AH_025 + stats contra F + F golo 4+ en previa -> OVER 70%
    total_F_prev = (goles_F_prev + goles_rival_prev) if goles_F_prev is not None else None
    if ah_fam == 'H025' and base_stats == 'STATS_NEUTRAL_OR_AGAINST' and total_F_prev is not None and total_F_prev >= 4:
        score_OVER += 1.8
        mr_active_over.append('MR-OV1 H025+GOALS4+ 70%')

    # OU acumulacion base
    if h2h_over: score_OVER += 2.0
    elif h2h_under: score_UNDER += 2.0
    else:
        score_OVER += 0.5
        score_UNDER += 0.5

    if total_F_prev is not None:
        if total_F_prev - ou_raw >= 0.25: score_OVER += 1.0
        elif total_F_prev - ou_raw <= -0.25: score_UNDER += 1.0

    if total_D_prev is not None:
        if total_D_prev - ou_raw >= 0.25: score_OVER += 0.5
        elif total_D_prev - ou_raw <= -0.25: score_UNDER += 0.5

    # H025-9 / universal: OVER contraintuitivo con OU alto.
    # Si la linea actual sigue alta pese a H2H de 3 goles o cercanos, el UNDER no es automatico.
    u18_over_counterintuitive = False
    over_counter_confirmers = 0
    direct_totals = [t for t in [TH, stadium_total] if t is not None]
    direct_near_high_line = [
        t for t in direct_totals
        if ou_raw >= 3.25 and t < ou_raw and (ou_raw - t) <= 0.75
    ]
    fav_prev_shots = fav_prev_stats.get('Tiros', {}).get(side_F_prev, 0)
    dog_prev_shots = dog_prev_stats.get('Tiros', {}).get(side_D_prev, 0)
    fav_recent_hidden_volume = (
        (fav_prev_shots is not None and fav_prev_shots >= 14) or
        (sot_fav is not None and sot_fav >= 6) or
        (ap_fav_prev is not None and ap_fav_prev >= 55)
    )
    dog_recent_alive_for_goals = (
        (dog_RF is not None and dog_RF >= 0) or
        dog_won_as_fav_or_pickem or
        (sot_dog_prev is not None and sot_dog_prev >= 4) or
        (dog_prev_shots is not None and dog_prev_shots >= 12)
    )
    fragile_result_map = (
        (goles_rival_prev is not None and goles_rival_prev >= 2) or
        (goles_rival_D is not None and goles_rival_D >= 1) or
        base_cover in ['PUSH', 'FAIL']
    )
    indirect_goal_support = (
        (ind_fav_goals is not None and ind_fav_goals >= 4) or
        (ind_dog_goals is not None and ind_dog_goals >= 2)
    )
    over_counter_confirmers = sum([
        len(direct_near_high_line) >= 1,
        fav_recent_hidden_volume,
        dog_recent_alive_for_goals,
        fragile_result_map,
        indirect_goal_support,
        h <= 0.25,
    ])
    if ou_raw >= 3.25 and len(direct_near_high_line) >= 1 and over_counter_confirmers >= 3:
        u18_over_counterintuitive = True
        flags.add('OU_HIGH_COUNTERINTUITIVE')
        score_UNDER = max(0.0, score_UNDER - 1.8)
        score_OVER += 1.4 + (0.35 * min(over_counter_confirmers, 5))
        mr_active_over.append('H025-9 OU_ALTO_CONTRAINTUITIVO')
        argumentos.append(
            f'[H025-9 OU_HIGH_COUNTER] La linea OU={ou_raw} sigue alta aunque los H2H directos quedan cerca '
            f'por debajo ({direct_near_high_line}). Confirmadores={over_counter_confirmers}: volumen oculto={fav_recent_hidden_volume}, '
            f'dog vivo={dog_recent_alive_for_goals}, fragilidad={fragile_result_map}, indirectas gol={indirect_goal_support}. '
            f'Se bloquea el UNDER automatico y se abre rama OVER.'
        )

    # U21: H2H over antiguo capado por OU + empate congelado.
    # Patron CA Lugano Reserves - Defensores de Cambaceres Reserves:
    # el H2H fue 3-3, pero la casa deja OU 2.75 y pide AH 1 al favorito.
    # Si las dos previas recientes son empates/push, el 3-3 no se persigue:
    # queda como memoria inflada. La lectura pasa a dog + under.
    u21_active = False
    h2h_high_draw = (
        goles_F_h2h == goles_D_h2h and
        TH >= 5
    )
    ou_capped_from_high_draw = (
        ou_raw <= 2.75 and
        (TH - ou_raw) >= 2.25
    )
    market_demands_margin_after_draw = (
        h >= 0.75 and
        base_cover == 'FAIL' and
        pressure in {'PRESSURE_RAISE', 'PRESSURE_RAISE_AGGRESSIVE', 'PRESSURE_SAME'}
    )
    both_recent_no_win = (
        margin_F_prev is not None and
        dog_prev_margin is not None and
        margin_F_prev == 0 and
        dog_prev_margin == 0
    )
    h2h_process_split = (
        (sap_h2h_D >= sap_h2h_F and ap_h2h_F >= ap_h2h_D) or
        base_stats == 'STATS_LEAN_FOR'
    )

    if h2h_high_draw and ou_capped_from_high_draw and market_demands_margin_after_draw and both_recent_no_win:
        u21_active = True
        flags.add('H2H_OVER_ANTIGUO_OU_CAPADO')
        flags.add('DOBLE_PUSH_RECIENTE')
        flags.add('U21_EMPATE_CONGELADO_UNDER')
        score_F -= 0.6
        score_D += 1.2
        score_DRAW += 1.3
        draw_risk += 0.6
        score_OVER = max(0.0, score_OVER - 2.0)
        score_UNDER += 2.6
        mr_active_dog.append('U21 DOG_POR_EMPATE_CONGELADO')
        mr_active_under.append('U21 UNDER_H2H_OVER_CAPADO')
        argumentos.append(
            f'[U21 H2H_OVER_CAPADO] El H2H fue empate abierto ({h2h_score_str}, TH={TH}), '
            f'pero hoy el OU queda capado en {ou_raw} y el AH exige h={h:.2f} a un favorito que no cubrio ese cruce '
            f'(RH={RH:+.2f}). Las dos previas recientes no separan ganador '
            f'(F_margin={margin_F_prev:+.1f}, D_margin={dog_prev_margin:+.1f}). '
            f'El 3-3 funciona como memoria de over, no como proyeccion. '
            f'score_D += 1.2, score_UNDER += 2.6, score_DRAW += 1.3.'
        )
        if h2h_process_split:
            score_F -= 0.3
            score_D += 0.3
            argumentos.append(
                '[U21 PROCESO_PARTIDO] El H2H no fue dominio limpio del favorito '
                '(proceso repartido entre tiros/SOT y ataques peligrosos). Se refuerza dog AH.'
            )

    # U22: AH0 no neutral cuando el antiguo dog ya gano el H2H general.
    # Patron Virginia United SC (W) - Logan Lightning (W): el local actual gano
    # fuera el H2H reciente siendo dog/presion menor, ahora vuelve a casa con AH=0.
    # El OU alto viene de goleadas sufridas y H2H estadio viejo, pero el cruce
    # reciente fue 0-1 y ambos llegan con ataque cero ante rivales fuertes.
    u22_active = False

    def _same_team_name(a, b):
        aa = ''.join(ch for ch in str(a or '').lower() if ch.isalnum())
        bb = ''.join(ch for ch in str(b or '').lower() if ch.isalnum())
        return bool(aa and bb and (aa in bb or bb in aa))

    dog_prev_opponent = None
    try:
        dog_prev_opponent = lam_D.get('home_team') if fav_is_home else lam_D.get('away_team')
    except Exception:
        dog_prev_opponent = None

    fav_vs_dog_prev_common_gap = None
    if margin_F_ind is not None and dog_prev_margin is not None:
        if _same_team_name(ind_fav.get('rival_name'), dog_prev_opponent):
            fav_vs_dog_prev_common_gap = margin_F_ind - dog_prev_margin

    h2h_latest_direct_low_win = (
        is_pickem and
        base_cover == 'COVER' and
        role_reversed and
        TH <= 1 and
        (goles_F_h2h - goles_D_h2h) >= 1
    )
    both_recent_attack_zero = (
        goles_F_prev == 0 and
        goles_D_prev == 0
    )
    ou_inflated_by_external_blowouts = (
        ou_raw >= 3.5 and
        both_recent_attack_zero and
        (
            (goles_rival_prev is not None and goles_rival_prev >= 3) or
            (goles_rival_D is not None and goles_rival_D >= 5) or
            (stadium_total is not None and stadium_total >= 6)
        )
    )
    common_strong_rival_validates_fav = (
        fav_vs_dog_prev_common_gap is not None and
        fav_vs_dog_prev_common_gap >= 3
    )

    if h2h_latest_direct_low_win and (common_strong_rival_validates_fav or dog_prev_classification == 'DEFENSA_ROTA'):
        u22_active = True
        flags.add('PICKEM_DOG_WIN_TO_HOME_DNB')
        score_F += 2.4
        score_D -= 0.8
        score_DRAW = max(0.0, score_DRAW - 0.2)
        mr_active_fav.append('U22 PICKEM_DOG_WIN_TO_HOME_DNB')
        argumentos.append(
            f'[U22 PICKEM_DNB_VALIDADO] AH=0 no es neutral aqui: {fav_name} gano el H2H reciente '
            f'como dog/rol inferior ({h2h_score_str}, RH={RH:+.2f}) y ahora vuelve a casa en DNB. '
            f'El rival comun fuerte valida al local (gap={fav_vs_dog_prev_common_gap}). '
            f'score_F += 2.4, score_D -= 0.8.'
        )

        if ou_inflated_by_external_blowouts:
            flags.add('OU_ALTO_INFLADO_POR_GOLEADAS_AJENAS')
            flags.add('ATAQUE_CERO_BILATERAL')
            score_OVER = max(0.0, score_OVER - 1.4)
            score_UNDER += 2.8
            mr_active_under.append('U22 UNDER_AH0_OU_INFLADO')
            argumentos.append(
                f'[U22 UNDER] OU={ou_raw} esta inflado por goleadas ajenas/externas '
                f'(F_prev={fav_prev_score}, D_prev={dog_prev_score}, stadium_total={stadium_total}), '
                f'pero el H2H reciente fue {TH} gol y ambos ataques recientes quedaron en cero. '
                f'score_OVER -= 1.4, score_UNDER += 2.8.'
            )

    # OU_CAPADO
    bloqueo_over = False
    if ou_raw <= 2.25:
        score_UNDER += 1.0
        draw_risk += 0.5
        if score_OVER < 3.0:
            bloqueo_over = True

    # Draw risk AH
    if h <= 0.25: draw_risk += 0.5
    if TH <= 1: draw_risk += 0.5

    # ======================================================
    # SCORE_DRAW: CAPEAR AL FAVORITO CUANDO EL EMPATE DOMINA
    # ======================================================
    # Logica: AH=0 no es "partido sin info". Es "partido con info BLOQUEADA".
    # Cuando score_DRAW >= 2.0 y la linea es <= 0.25, el favorito no puede tener
    # un edge enorme salvo que haya una indirecta brutal o calidad clarisima.
    if score_DRAW >= 2.0 and abs(h) <= 0.25:
        cap = score_DRAW + 0.5
        if score_F > cap:
            diff_cap = score_F - cap
            score_F = cap
            argumentos.append(
                f'[DRAW CAP] score_DRAW={score_DRAW:.1f} capea score_F de {score_F + diff_cap:.1f} a {cap:.1f}. '
                f'El empate es demasiado probable para que el favorito tenga edge real.')
        # El draw tambien alimenta el draw_risk
        draw_risk += score_DRAW * 0.3
        argumentos.append(
            f'[DRAW RISK+] score_DRAW={score_DRAW:.1f} incrementa draw_risk en {score_DRAW * 0.3:.2f}. '
            f'El mercado bloqueado + empates convergen.')

    # Edges
    edge_AH = score_F - score_D - draw_risk
    edge_OU = score_OVER - score_UNDER

    # Determinar umbrales
    has_mr_fav = len(mr_active_fav) > 0
    has_mr_dog = len(mr_active_dog) > 0
    conflict_mr = has_mr_fav and has_mr_dog
    has_any_mr_ah = has_mr_fav or has_mr_dog

    if p9_active:
        thresh_ah = 99
    elif conflict_mr:
        thresh_ah = 3.00
    elif has_any_mr_ah:
        thresh_ah = 1.15
    else:
        thresh_ah = 2.10

    has_mr_ou = len(mr_active_over) > 0 or len(mr_active_under) > 0
    thresh_ou = 1.15 if has_mr_ou else 2.10
    if u18_over_counterintuitive:
        thresh_ou = min(thresh_ou, 0.75 if over_counter_confirmers >= 4 else 1.00)
    if u20_active and 'OU_INFLADO_POR_GOLEADA_ANTIGUA' in flags:
        thresh_ou = min(thresh_ou, 0.45 if 'COL3_ENFRIA_OU_EXTREMO' in flags else 0.75)
    if u21_active:
        thresh_ou = min(thresh_ou, 0.75)
    if u22_active and 'OU_ALTO_INFLADO_POR_GOLEADAS_AJENAS' in flags:
        thresh_ou = min(thresh_ou, 0.75)

    # DECISION AH
    if p9_active:
        ah_pick = 'NO_BET'
        ah_label = f'NO BET AH (MEJORA_REAL: el mercado conoce la mejora de {fav_name})'
    elif edge_AH >= thresh_ah:
        ah_pick = 'FAV_CUBRE'
        ah_label = f'{fav_name} AH 0 / DNB' if is_pickem else f'{fav_name} cubre AH -{h:.2f}'
    elif edge_AH <= -thresh_ah:
        ah_pick = 'DOG_CUBRE'
        ah_label = f'{dog_name} AH 0 / DNB' if is_pickem else f'{dog_name} cubre +{h:.2f} ({fav_name} NO supera la linea)'
    else:
        ah_pick = 'NO_BET'
        ah_label = 'NO BET AH (PICKEM/DNB)' if is_pickem else 'NO BET AH'

    # DECISION OU
    if bloqueo_over and score_OVER < 3.0:
        if edge_OU <= -thresh_ou:
            ou_pick = 'UNDER'
            ou_label = f'UNDER {ou_raw}'
        else:
            ou_pick = 'NO_BET'
            ou_label = 'NO BET OU (OU capado)'
    elif edge_OU >= thresh_ou:
        ou_pick = 'OVER'
        ou_label = f'OVER {ou_raw}'
    elif edge_OU <= -thresh_ou:
        ou_pick = 'UNDER'
        ou_label = f'UNDER {ou_raw}'
    else:
        ou_pick = 'NO_BET'
        ou_label = 'NO BET OU'

    if u18_over_counterintuitive and ou_pick == 'UNDER':
        ou_pick = 'NO_BET'
        ou_label = 'NO BET OU (UNDER bloqueado por OU alto contraintuitivo)'
        argumentos.append(
            '[OU BLOCK] El modelo queria UNDER, pero H025-9 bloquea el under automatico: '
            'linea alta sostenida + H2H cercano + confirmadores de gol.'
        )

    # ======================================================
    # V7: CAPA DE PREDICCION Y ABSTENCION
    # ======================================================
    # El mapa completo se conserva en raw_*. Solo se publica una apuesta cuando
    # el patron supero bloques cronologicos o el edge estructural es fuerte.
    raw_ah_pick = ah_pick
    raw_ah_label = ah_label
    raw_ou_pick = ou_pick
    raw_ou_label = ou_label

    evidence_blocks = 1  # H2H general, validado en la puerta de entrada.
    if stadium_RH is not None and not same_h2h_as_general:
        evidence_blocks += 1
    if RF is not None:
        evidence_blocks += 1
    if dog_RF is not None:
        evidence_blocks += 1
    if margin_F_ind is not None:
        evidence_blocks += 1
    if margin_D_ind is not None:
        evidence_blocks += 1
    col3_h, col3_a = parse_score(col3_score_str)
    if col3_h is not None:
        evidence_blocks += 1

    stats_blocks = sum([
        bool(h2h_stats),
        bool(fav_prev_stats),
        bool(dog_prev_stats),
        bool(ind_fav.get('stats_rows')),
        bool(ind_dog.get('stats_rows')),
    ])

    context_dates = {
        'stadium': h2h_est_date,
        'prev_fav': lhm.get('date') if isinstance(lhm, dict) else None,
        'prev_dog': lam_D.get('date') if isinstance(lam_D, dict) else None,
        'ind_fav': ind_fav.get('date') if isinstance(ind_fav, dict) else None,
        'ind_dog': ind_dog.get('date') if isinstance(ind_dog, dict) else None,
        'col3': col3.get('date') if isinstance(col3, dict) else None,
    }
    nonpast_contexts = []
    stale_contexts = []
    if match_date_obj:
        for source_name, source_date_raw in context_dates.items():
            source_date_obj = _parse_date(source_date_raw)
            if source_date_obj is None:
                continue
            age_days = (match_date_obj - source_date_obj).days
            if age_days <= 0:
                nonpast_contexts.append(source_name)
            elif age_days > 1095:
                stale_contexts.append(source_name)
    if nonpast_contexts:
        quality_warnings.append('NONPAST:' + ','.join(sorted(nonpast_contexts)))
        flags.add('NONPAST_CONTEXT_BLOCK')
    if stale_contexts:
        quality_warnings.append('STALE:' + ','.join(sorted(stale_contexts)))
        flags.add('STALE_CONTEXT')
    if evidence_blocks < 3:
        quality_warnings.append('LOW_EVIDENCE')
        flags.add('LOW_EVIDENCE')

    quality_eligible = evidence_blocks >= 3 and not nonpast_contexts
    quality_score = max(0, min(100, evidence_blocks * 14 + stats_blocks * 5 - len(stale_contexts) * 4))

    production_fav_rules = [
        rule for rule in mr_active_fav
        if any(rule.startswith(prefix) for prefix in AH_PRODUCTION_RULE_PREFIXES)
    ]
    production_dog_rules = [
        rule for rule in mr_active_dog
        if any(rule.startswith(prefix) for prefix in AH_PRODUCTION_RULE_PREFIXES)
    ]
    production_ah_rules = production_fav_rules + production_dog_rules
    production_rule_supports_ah = (
        raw_ah_pick == 'FAV_CUBRE' and bool(production_fav_rules)
    ) or (
        raw_ah_pick == 'DOG_CUBRE' and bool(production_dog_rules)
    )
    production_rule_conflict = bool(production_fav_rules and production_dog_rules)
    blocking_flags = sorted(AH_BLOCKING_FLAGS.intersection(flags))
    validated_ah_line = any(abs(ah_raw - line) < 0.01 for line in AH_VALIDATED_EXACT_LINES)

    ah_gate_reasons = []
    ah_tier = 'NO_BET'
    ah_confidence = 'NONE'
    if raw_ah_pick in {'FAV_CUBRE', 'DOG_CUBRE'}:
        if not quality_eligible:
            ah_gate_reasons.append('calidad insuficiente')
        if blocking_flags:
            ah_gate_reasons.append('bloqueos historicos: ' + ', '.join(blocking_flags))
        if production_rule_conflict:
            ah_gate_reasons.append('micro-reglas de produccion enfrentadas')
        if not validated_ah_line:
            ah_gate_reasons.append(AH_LINE_GATE_REASON)
        strong_structural_edge = abs(edge_AH) >= 3.5
        if not (strong_structural_edge or production_rule_supports_ah):
            ah_gate_reasons.append('edge menor de 3.5 sin micro-regla promovida')

        if not ah_gate_reasons:
            ah_tier = 'PRODUCTION'
            ah_confidence = 'HIGH' if abs(edge_AH) >= 4.5 and production_rule_supports_ah else 'MEDIUM'
        else:
            ah_pick = 'NO_BET'
            ah_label = 'NO BET AH (V7: ' + '; '.join(ah_gate_reasons) + ')'
            ah_tier = 'OBSERVATION'
            argumentos.append(
                f'[V7 AH GUARD] Se conserva lectura raw={raw_ah_pick}, pero no se publica: '
                + '; '.join(ah_gate_reasons) + '.'
            )

    # La unica rama OU que conserva direccion en los cinco bloques es MR-OU2
    # cuando no hay cambio de favorito. El resto queda visible como observacion.
    mr_ou2_active = any(rule.startswith('MR-OU2 ') for rule in mr_active_under)
    ou_rule_production = raw_ou_pick == 'UNDER' and mr_ou2_active and pressure != 'PRESSURE_NEW_FAV'
    ou_gate_reasons = []
    ou_tier = 'NO_BET'
    ou_confidence = 'NONE'
    if raw_ou_pick in {'OVER', 'UNDER'}:
        if not quality_eligible:
            ou_gate_reasons.append('calidad insuficiente')
        if not ou_rule_production:
            ou_gate_reasons.append('sin regla OU promovida estable')
        if len(mr_active_over) > 0 and len(mr_active_under) > 0:
            ou_gate_reasons.append('conflicto OVER/UNDER')
        if not ou_gate_reasons:
            ou_tier = 'PRODUCTION'
            ou_confidence = 'MEDIUM'
        else:
            ou_pick = 'NO_BET'
            ou_label = 'NO BET OU (V7: ' + '; '.join(ou_gate_reasons) + ')'
            ou_tier = 'OBSERVATION'
            argumentos.append(
                f'[V7 OU GUARD] Se conserva lectura raw={raw_ou_pick}, pero no se publica: '
                + '; '.join(ou_gate_reasons) + '.'
            )

    if ah_tier == 'PRODUCTION':
        argumentos.append(
            f'[V7 AH PRODUCTION] raw={raw_ah_pick}, edge={edge_AH:+.2f}, '
            f'reglas={production_ah_rules or ["EDGE_3.5_PLUS"]}, evidencia={evidence_blocks}.'
        )
    if ou_tier == 'PRODUCTION':
        argumentos.append(
            f'[V7 OU PRODUCTION] UNDER por MR-OU2 sin cambio de favorito, '
            f'edge={edge_OU:+.2f}, evidencia={evidence_blocks}.'
        )

    # Foto del nucleo antes de aplicar expansiones. Permite reauditar la regla
    # contra el mismo universo que la genero, incluso despues de publicarla.
    core_ah_pick = ah_pick
    core_ah_tier = ah_tier
    core_ah_gate_reasons = list(ah_gate_reasons)

    # Segunda capa: lectura explicable de colocacion de cuotas y rombo Col3.
    # No crea apuestas por si sola; confirma o avisa de conflicto sobre la
    # direccion que ya supero las puertas cronologicas de la Clave.
    try:
        from .bookie_pattern_detector import detect_bookie_patterns
        bookie_detector = detect_bookie_patterns(m, raw_ah_pick)
    except Exception as exc:
        bookie_detector = {
            'available': False,
            'confirmation': 'ERROR',
            'signals': [],
            'col3_direction': None,
            'col3_agrees': None,
            'error': type(exc).__name__,
        }
    bookie_confirmation = bookie_detector.get('confirmation', 'NO_DATA')

    expansion_ah_rule = None
    validated_ah_expansion = False
    if (
        core_ah_tier == 'OBSERVATION'
        and raw_ah_pick == 'FAV_CUBRE'
        and abs(ah_raw - 0.50) < 0.01
        and core_ah_gate_reasons == [AH_LINE_GATE_REASON]
        and bookie_confirmation == 'NEUTRAL'
    ):
        expansion_ah_rule = AH_EXPANSION_RULE_ID
        validated_ah_expansion = True
        ah_pick = raw_ah_pick
        ah_label = raw_ah_label
        ah_tier = 'PRODUCTION_EXPANSION'
        ah_confidence = 'MEDIUM'
        ah_gate_reasons = []
        production_ah_rules = list(production_ah_rules) + [AH_EXPANSION_RULE_ID]
        argumentos.append(
            '[V7 AH EXPANSION] Favorito local -0.50 con lectura Clave FAV_CUBRE '
            'y posicionamiento de casa NEUTRAL. Regla congelada 60/20/20: '
            '61.67% discovery (n=60), 83.33% validation (n=12), '
            '64.71% test final intacto (n=17).'
        )

    if ah_tier == 'PRODUCTION' and bookie_confirmation == 'STRONG_CONFIRM':
        ah_confidence = 'HIGH'
    elif (
        ah_tier == 'PRODUCTION'
        and bookie_detector.get('col3_agrees') is False
    ):
        ah_confidence = 'LOW'
    if bookie_confirmation in {'STRONG_CONFIRM', 'CONFIRM', 'CONFLICT'}:
        argumentos.append(
            '[BOOKIE POSITIONING] '
            f"{bookie_confirmation}; Col3={bookie_detector.get('col3_direction') or 'N/A'} "
            f"({'coincide' if bookie_detector.get('col3_agrees') else 'conflicto' if bookie_detector.get('col3_agrees') is False else 'sin rama'}); "
            f"alineadas={bookie_detector.get('aligned_signals') or []}; "
            f"conflictos={bookie_detector.get('conflicting_signals') or []}."
        )

    # Tipo de empate esperado (para display, no afecta al pick)
    draw_type = ''
    if score_DRAW >= 1.5:
        if ou_raw <= 2.25:   draw_type = '0:0 / 1:1'
        elif ou_raw <= 2.75: draw_type = '1:1 / 2:2'
        else:                draw_type = '2:2 / 3:3'

    # Argumento final de veredicto
    vrd = (f'[VEREDICTO] edge_AH={edge_AH:+.2f} (thresh={thresh_ah:.2f}) -> {ah_pick} | '
           f'edge_OU={edge_OU:+.2f} (thresh={thresh_ou:.2f}) -> {ou_pick}')
    if draw_type:
        vrd += f' | draw_type={draw_type} (score_DRAW={score_DRAW:.1f})'
    argumentos.append(vrd)

    return {
        'engine_version': ENGINE_VERSION,
        'ah': ah_pick,
        'ah_label': ah_label,
        'ou': ou_pick,
        'ou_label': ou_label,
        'raw_ah': raw_ah_pick,
        'raw_ah_label': raw_ah_label,
        'raw_ou': raw_ou_pick,
        'raw_ou_label': raw_ou_label,
        'prediction_tier_ah': ah_tier,
        'prediction_tier_ou': ou_tier,
        'core_ah': core_ah_pick,
        'core_prediction_tier_ah': core_ah_tier,
        'core_ah_gate_reasons': core_ah_gate_reasons,
        'confidence_ah': ah_confidence,
        'confidence_ou': ou_confidence,
        'ah_gate_reasons': ah_gate_reasons,
        'ou_gate_reasons': ou_gate_reasons,
        'production_ah_rules': production_ah_rules,
        'production_ou_rules': ['MR-OU2_STABLE_NO_NEW_FAV'] if ou_rule_production else [],
        'validated_ah_line': validated_ah_line,
        'validated_ah_expansion': validated_ah_expansion,
        'expansion_ah_rule': expansion_ah_rule,
        'bookie_detector': bookie_detector,
        'bookie_confirmation': bookie_confirmation,
        'blocking_flags': blocking_flags,
        'quality': {
            'eligible': quality_eligible,
            'score': quality_score,
            'evidence_blocks': evidence_blocks,
            'stats_blocks': stats_blocks,
            'nonpast_contexts': nonpast_contexts,
            'stale_contexts': stale_contexts,
            'warnings': quality_warnings,
        },
        'edge_AH': round(edge_AH, 2),
        'edge_OU': round(edge_OU, 2),
        'score_F': round(score_F, 2),
        'score_D': round(score_D, 2),
        'score_DRAW': round(score_DRAW, 2),
        'score_OVER': round(score_OVER, 2),
        'score_UNDER': round(score_UNDER, 2),
        'draw_risk': round(draw_risk, 2),
        'draw_type': draw_type,
        'mr_dog': mr_active_dog,
        'mr_fav': mr_active_fav,
        'mr_over': mr_active_over,
        'mr_under': mr_active_under,
        'argumentos': argumentos,
        'flags': sorted(flags),
        'notes': argumentos,
        'base_cover': base_cover,
        'base_stats': base_stats,
        'pressure': pressure,
        'ah_fam': ah_fam,
        'ou_fam': ou_fam,
        'fav': fav_name,
        'dog': dog_name,
        'role_mode': role_mode,
        'is_pickem': is_pickem,
        'h': h,
        'ou_raw': ou_raw,
        'RH': round(RH, 2),
        'TH': TH,
        'stadium_RH': round(stadium_RH, 2) if stadium_RH is not None else None,
        'learning_hooks': sorted([
            flag for flag in flags
            if flag in {
                'PICKEM_DNB',
                'RESULTADO_BLOQUEA_VOLUMEN',
                'DOG_GANA_COMO_FAV_O_PICKEM',
                'INDIRECTA_DOG_SUPERA_FAV_AH_BAJO',
                'STADIUM_OLD',
                'EXTREME_LOWER_TO_H025',
                'OU_HIGH_COUNTERINTUITIVE',
                'MARKET_INSISTS_AGAINST_H2H',
                'RAMA_CONTINUIDAD_FALSA',
                'RAMA_FAVORITO_BLOQUEADO_EMPATE',
                'MARKET_REJECTS_OBVIOUS_DOG_X2',
                'U19_OVER_REVANCHA_ABIERTA',
                'HUGE_DROP_PROTECTS_DOG',
                'OU_INFLADO_POR_GOLEADA_ANTIGUA',
                'COL3_ENFRIA_OU_EXTREMO',
                'H2H_OVER_ANTIGUO_OU_CAPADO',
                'DOBLE_PUSH_RECIENTE',
                'U21_EMPATE_CONGELADO_UNDER',
                'PICKEM_DOG_WIN_TO_HOME_DNB',
                'OU_ALTO_INFLADO_POR_GOLEADAS_AJENAS',
                'ATAQUE_CERO_BILATERAL',
            }
        ]),
        'u1_ou_inflada': u1_active,
        'u2_calidad_invertida': u2_active,
        'u3_resistencia_local': u3_active,
        'u4_tabla_igualada': u4_active,
        'u5_volumen_oculto': u5_active,
        'u6_empate_neutralizacion': u6_active,
        'u7_no_confirmacion': u7_active,
        'u8_espejo_empate': u8_active,
        'p9_mejora_real': p9_active,
        'u10_anomalia_linea_baja': u10_active,
        'u11_favorito_125_dog_persistente': u11_active,
        'u12_bloqueo_seco': u12_active,
        'u13_push_seco': u13_active,
        'u14_repeticion_proceso': u14_active,
        'u15_rebaja_protectora': u15_active,
        'u16_fav_025_capado': u16_active,
        'u17_market_flip_validated': u17_active,
        'u18_over_counterintuitive': u18_over_counterintuitive,
        'over_counter_confirmers': over_counter_confirmers,
        'u19_market_rejects_obvious_dog_x2': u19_active,
        'u20_huge_drop_protects_dog_under': u20_active,
        'u21_h2h_over_capped_draw_under': u21_active,
        'u22_pickem_dog_win_home_dnb_under': u22_active,
    }

# Fin del modulo clave_dicotomica.py - V7 predictiva con abstencion calibrada
