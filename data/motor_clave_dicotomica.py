import json
import sys
import os
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / 'src'))
from modules.clave_dicotomica import apply_key as apply_key_v6

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

def apply_key(m):
    """
    Aplica la clave dicotomica V4 al partido y devuelve picks reales:
      AH: 'FAV_CUBRE' | 'DOG_CUBRE' | 'NO_BET'
      OU: 'OVER' | 'UNDER' | 'NO_BET'
    """
    score_F = 0.0
    score_D = 0.0
    score_OVER = 0.0
    score_UNDER = 0.0
    draw_risk = 0.0
    mr_active_fav = []
    mr_active_dog = []
    mr_active_over = []
    mr_active_under = []
    notes = []

    odds = m.get('main_match_odds', {})
    ah_raw = parse_ah(odds.get('ah_linea'))
    ou_raw = parse_ah(odds.get('goals_linea'))

    if ah_raw is None or ou_raw is None:
        return {'ah': 'NO_BET', 'ou': 'NO_BET', 'reason': 'Sin odds'}

    h = abs(ah_raw)
    is_pickem = h < 0.01
    # AH=0 es PICKEM/DNB. Usamos el local solo como referencia de calculo,
    # nunca como favorito real ni como visitante favorito por defecto.
    fav_is_home = True if is_pickem else ah_raw > 0
    
    home_name = m.get('home_name','')
    away_name = m.get('away_name','')
    fav_name = home_name if fav_is_home else away_name
    dog_name = away_name if fav_is_home else home_name
    if is_pickem:
        notes.append('PICKEM_DNB: AH=0 no crea favorito visitante; local usado solo como referencia.')
        draw_risk += 0.25

    # H2H
    h2h = m.get('h2h_general', {})
    h2h_score_str = h2h.get('res6','')
    h2h_home_name = h2h.get('h2h_gen_home','')
    h2h_ah = parse_ah(h2h.get('ah6')) or 0.0

    hg_h2h, ag_h2h = parse_score(h2h_score_str)
    if hg_h2h is None:
        return {'ah': 'NO_BET', 'ou': 'NO_BET', 'reason': 'Sin H2H valido'}

    # Orientar H2H desde F
    fav_in_h2h_is_home = (fav_name.lower() in h2h_home_name.lower() or
                          h2h_home_name.lower() in fav_name.lower()) if h2h_home_name else fav_is_home
    goles_F_h2h = hg_h2h if fav_in_h2h_is_home else ag_h2h
    goles_D_h2h = ag_h2h if fav_in_h2h_is_home else hg_h2h
    margin_F_h2h = goles_F_h2h - goles_D_h2h
    RH = margin_F_h2h - h
    TH = hg_h2h + ag_h2h

    # Movimiento de presion
    delta = h - abs(h2h_ah)
    if h2h_ah == 0 or h2h_ah is None:
        pressure = 'PRESSURE_SAME'
    elif delta >= 0.75:
        pressure = 'PRESSURE_RAISE_AGGRESSIVE'
    elif delta >= 0.25:
        pressure = 'PRESSURE_RAISE'
    elif delta <= -0.25:
        pressure = 'PRESSURE_LOWER'
    else:
        pressure = 'PRESSURE_SAME'

    # Familia AH
    if h == 0:       ah_fam = 'H0'
    elif h == 0.25:  ah_fam = 'H025'
    elif h <= 0.75:  ah_fam = 'H05_075'
    elif h <= 1.25:  ah_fam = 'H1_125'
    elif h <= 1.75:  ah_fam = 'H15_175'
    else:            ah_fam = 'H2_PLUS'

    # Familia OU
    if ou_raw <= 2.25:  ou_fam = 'OU_LOW'
    elif ou_raw <= 2.75: ou_fam = 'OU_MID'
    elif ou_raw <= 3.5:  ou_fam = 'OU_HIGH'
    else:                ou_fam = 'OU_EXTREME'

    # NODO 2: BASE_COVER
    if RH >= 0.25:
        base_cover = 'COVER'
        score_F += 2.0
        notes.append(f'H2H COVER (RH={RH:.2f})')
    elif RH <= -0.25:
        base_cover = 'FAIL'
        score_D += 2.0
        notes.append(f'H2H FAIL (RH={RH:.2f})')
    else:
        base_cover = 'PUSH'
        score_F += 0.5
        score_D += 0.5
        notes.append(f'H2H PUSH (RH={RH:.2f})')

    # Presion
    if pressure == 'PRESSURE_RAISE_AGGRESSIVE':
        score_F += 1.0
        notes.append('RAISE_AGGRESSIVE: la casa sube fuerte la exigencia')
    elif pressure == 'PRESSURE_RAISE':
        score_F += 0.5
    elif pressure == 'PRESSURE_LOWER':
        score_F += 0.3
    
    # Stats H2H
    h2h_stats = get_stats(h2h.get('stats_rows', []))
    sap_h2h_F = h2h_stats.get('Tiros a Puerta', {}).get('h' if fav_in_h2h_is_home else 'a', 0)
    sap_h2h_D = h2h_stats.get('Tiros a Puerta', {}).get('a' if fav_in_h2h_is_home else 'h', 0)
    ap_h2h_F  = h2h_stats.get('Ataques Peligrosos', {}).get('h' if fav_in_h2h_is_home else 'a', 0)
    ap_h2h_D  = h2h_stats.get('Ataques Peligrosos', {}).get('a' if fav_in_h2h_is_home else 'h', 0)
    tiros_F   = h2h_stats.get('Tiros', {}).get('h' if fav_in_h2h_is_home else 'a', 0)
    tiros_D   = h2h_stats.get('Tiros', {}).get('a' if fav_in_h2h_is_home else 'h', 0)
    dom_count = sum([sap_h2h_F > sap_h2h_D, ap_h2h_F > ap_h2h_D, tiros_F > tiros_D])
    if dom_count >= 2:
        base_stats = 'STATS_STRONG_FOR'
    elif dom_count == 1:
        base_stats = 'STATS_LEAN_FOR'
    else:
        base_stats = 'STATS_NEUTRAL_OR_AGAINST'

    # Previa F
    lhm = (m.get('last_home_match') or {}) if fav_is_home else (m.get('last_away_match') or {})
    lam_D = (m.get('last_away_match') or {}) if fav_is_home else (m.get('last_home_match') or {})

    fav_prev_score = lhm.get('score', '')
    fav_prev_ah = parse_ah(lhm.get('handicap_line_raw')) or 0.0
    fg, fa = parse_score(fav_prev_score)
    goles_F_prev = fg if fav_is_home else fa
    goles_rival_prev = fa if fav_is_home else fg
    margin_F_prev = (goles_F_prev - goles_rival_prev) if goles_F_prev is not None else None
    RF = (margin_F_prev - h) if margin_F_prev is not None else None

    fav_prev_stats = get_stats(lhm.get('stats_rows', []))
    sot_fav = fav_prev_stats.get('Tiros a Puerta', {}).get('h' if fav_is_home else 'a', 0)
    ap_fav  = fav_prev_stats.get('Ataques Peligrosos', {}).get('h' if fav_is_home else 'a', 0)
    
    fav_cover_as_fav = False
    if RF is not None:
        if RF >= 0.25:
            fav_recent = 'COVER'
            score_F += 1.0
            # Check if F was also FAV in that match
            if (fav_is_home and fav_prev_ah > 0) or (not fav_is_home and fav_prev_ah < 0):
                fav_cover_as_fav = True
                score_F += 0.5
            else:
                # Cover as dog - not valid signal for being fav today
                score_D += 0.3
            notes.append(f'F_PREV COVER (RF={RF:.2f})')
        elif RF <= -0.25:
            fav_recent = 'FAIL'
            score_D += 0.5
            notes.append(f'F_PREV FAIL (RF={RF:.2f})')
            if margin_F_prev is not None and margin_F_prev >= 3:
                notes.append('FAV_RECENT_MARGIN_GE_POS3 -> TRAMPA')
        else:
            fav_recent = 'PUSH'
            notes.append(f'F_PREV PUSH (RF={RF:.2f})')
    else:
        fav_recent = 'UNKNOWN'

    # Previa D
    dog_prev_score = lam_D.get('score', '')
    dg, da_g = parse_score(dog_prev_score)
    goles_D_prev = da_g if fav_is_home else dg
    goles_rival_D = dg if fav_is_home else da_g
    total_D_prev = (goles_D_prev + goles_rival_D) if goles_D_prev is not None else None
    dog_prev_stats = get_stats(lam_D.get('stats_rows', []))
    sot_dog = dog_prev_stats.get('Tiros a Puerta', {}).get('a' if fav_is_home else 'h', 0)

    # Goleada encajada
    goleada_D = (goles_rival_D is not None and goles_rival_D >= 3)
    if goleada_D:
        notes.append('GOLEADA_D_ENCAJADA (puede estar descontada)')
    
    dog_recent_goals = (goles_D_prev + goles_rival_D) if goles_D_prev is not None else None
    dog_goals_2_minus = (dog_recent_goals is not None and dog_recent_goals <= 2)

    # Indirectas
    comp = m.get('comparativas_indirectas', {})
    left  = comp.get('left', {})
    right = comp.get('right', {})
    ind_fav  = left if fav_is_home else right
    ind_dog  = right if fav_is_home else left

    lg, la = parse_score(ind_fav.get('score', ''))
    rg, ra = parse_score(ind_dog.get('score', ''))
    
    if lg is not None:
        fav_loc = ind_fav.get('localia', 'H')
        margin_F_ind = (lg - la) if fav_loc == 'H' else (la - lg)
        ind_fav_goals = lg + la
    else:
        margin_F_ind = None
        ind_fav_goals = None

    if rg is not None:
        dog_loc = ind_dog.get('localia', 'A')
        margin_D_ind = (ra - rg) if dog_loc == 'A' else (rg - ra)
        ind_dog_goals = rg + ra
    else:
        margin_D_ind = None
        ind_dog_goals = None

    diff_F = None
    if margin_F_ind is not None and margin_D_ind is not None:
        diff_F = margin_F_ind - margin_D_ind
        if diff_F >= 2.0:
            score_F += 1.0
            notes.append(f'IND_FAV_VALIDATES (diff={diff_F:.1f})')
        elif diff_F <= -1.5:
            score_D += 1.0
            notes.append(f'IND_DOG_VALIDATES (diff={diff_F:.1f})')

    # Tabla
    rank_h = m.get('home_standings', {}).get('ranking')
    rank_a = m.get('away_standings', {}).get('ranking')
    try:
        rank_fav = int(rank_h) if fav_is_home else int(rank_a)
        rank_dog = int(rank_a) if fav_is_home else int(rank_h)
        if rank_fav < rank_dog:
            score_F += 0.3
        else:
            score_D += 0.3
            notes.append('TABLE_FAV_WORSE')
    except: pass

    # ---- MICRO-REGLAS AH ENTRENADAS ----
    # MR-D1: STATS_LEAN_FOR + FAV_RECENT_MARGIN_GE_POS3 -> DOG 80%
    if base_stats == 'STATS_LEAN_FOR' and margin_F_prev is not None and margin_F_prev >= 3:
        score_D += 2.5
        mr_active_dog.append('MR-D1 TRAMPA NARRATIVA 80%')

    # MR-D2: AH_2_PLUS + STATS_STRONG_FOR + OU_EXTREME -> DOG 78.6%
    if ah_fam == 'H2_PLUS' and base_stats == 'STATS_STRONG_FOR' and ou_fam == 'OU_EXTREME':
        score_D += 2.5
        mr_active_dog.append('MR-D2 INFLACION H2+ 78.6%')

    # MR-D3: AH_2_PLUS + STATS_STRONG_FOR + OU_4_PLUS -> DOG 78.6%
    if ah_fam == 'H2_PLUS' and base_stats == 'STATS_STRONG_FOR' and ou_raw >= 4.0:
        score_D += 2.5
        mr_active_dog.append('MR-D3 INFLACION H2+OU4 78.6%')

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
    if dog_neutral and 'TABLE_FAV_WORSE' in notes:
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
        mr_active_dog.append('MR-D15 H2H_OVER+OU4 69.2%')

    # MR-D16: BASE_COVER + OU_4_PLUS -> DOG 69.2%
    if base_cover == 'COVER' and ou_raw >= 4.0:
        score_D += 1.3
        mr_active_dog.append('MR-D16 COVER+OU4 69.2%')

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
        mr_active_fav.append('MR-F4 H15+IND_VALIDA 68.8%')

    # MR-F9: H05_075 + SAME + DOG_RECENT_DRAW -> FAV 65.6%
    dog_draw = (goles_D_prev is not None and goles_D_prev == goles_rival_D)
    if ah_fam == 'H05_075' and pressure == 'PRESSURE_SAME' and dog_draw:
        score_F += 1.0
        mr_active_fav.append('MR-F9 H05+SAME+DOG_DRAW 65.6%')

    # ---- MICRO-REGLAS OU ----
    # MR-OU1: STATS_LEAN_FOR + IND_FAV_STATS_NEUTRAL + OU_MID -> UNDER 85.7%
    if base_stats == 'STATS_LEAN_FOR' and ind_fav_neutral and ou_fam == 'OU_MID':
        score_UNDER += 3.0
        mr_active_under.append('MR-OU1 UNDER 85.7%')

    # MR-OU2: AH_025 + H2H_UNDER + IND_DOG_MARGIN_POS1 -> UNDER 84.6%
    ind_dog_pos1 = (margin_D_ind is not None and margin_D_ind >= 1)
    if ah_fam == 'H025' and h2h_under and ind_dog_pos1:
        score_UNDER += 2.8
        mr_active_under.append('MR-OU2 AH025+UNDER+IND_D_POS1 84.6%')

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

    # Edges
    edge_AH = score_F - score_D - draw_risk
    edge_OU = score_OVER - score_UNDER

    # Determinar umbrales
    has_mr_fav = len(mr_active_fav) > 0
    has_mr_dog = len(mr_active_dog) > 0
    conflict_mr = has_mr_fav and has_mr_dog
    has_any_mr_ah = has_mr_fav or has_mr_dog

    if conflict_mr:
        thresh_ah = 3.00
    elif has_any_mr_ah:
        thresh_ah = 1.15
    else:
        thresh_ah = 2.10

    has_mr_ou = len(mr_active_over) > 0 or len(mr_active_under) > 0
    thresh_ou = 1.15 if has_mr_ou else 2.10

    # DECISION AH
    if edge_AH >= thresh_ah:
        ah_pick = 'FAV_CUBRE'
        ah_label = f'{fav_name} cubre AH {ah_raw:+.2f}'
    elif edge_AH <= -thresh_ah:
        ah_pick = 'DOG_CUBRE'
        ah_label = f'{dog_name} cubre +{h:.2f} (favorito NO supera la linea)'
    else:
        ah_pick = 'NO_BET'
        ah_label = 'NO BET AH'

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

    return {
        'ah': ah_pick,
        'ah_label': ah_label,
        'ou': ou_pick,
        'ou_label': ou_label,
        'edge_AH': round(edge_AH, 2),
        'edge_OU': round(edge_OU, 2),
        'score_F': round(score_F, 2),
        'score_D': round(score_D, 2),
        'score_OVER': round(score_OVER, 2),
        'score_UNDER': round(score_UNDER, 2),
        'draw_risk': round(draw_risk, 2),
        'mr_dog': mr_active_dog,
        'mr_fav': mr_active_fav,
        'mr_over': mr_active_over,
        'mr_under': mr_active_under,
        'notes': notes,
        'base_cover': base_cover,
        'base_stats': base_stats,
        'pressure': pressure,
        'ah_fam': ah_fam,
        'ou_fam': ou_fam,
        'fav': fav_name,
        'dog': dog_name,
        'h': h,
        'ou': ou_raw,
        'RH': round(RH, 2),
        'TH': TH,
    }

# Cargar precacheo
data = json.load(open('data_precacheo.json', encoding='utf-8'))
matches = data if isinstance(data, list) else list(data.values())

results = []
for m in matches:
    try:
        result = apply_key_v6(m)
        result['match_id'] = m.get('match_id', '')
        result['home'] = m.get('home_name', '')
        result['away'] = m.get('away_name', '')
        result['league'] = m.get('league_name', '')
        result['date'] = m.get('match_date', '')
        result['ah_raw'] = m.get('main_match_odds', {}).get('ah_linea', '')
        result['ou_raw'] = m.get('main_match_odds', {}).get('goals_linea', '')
        results.append(result)
    except Exception as e:
        results.append({'ah': 'NO_BET', 'ou': 'NO_BET', 'ah_label': 'ERROR', 'ou_label': 'ERROR',
                        'home': m.get('home_name',''), 'away': m.get('away_name',''),
                        'date': m.get('match_date',''), 'league': m.get('league_name',''),
                        'match_id': m.get('match_id',''), 'ah_raw': '', 'ou_raw': '',
                        'edge_AH': 0, 'edge_OU': 0, 'notes': [str(e)], 'mr_dog': [], 'mr_fav': [],
                        'mr_over': [], 'mr_under': [], 'error': str(e)})

# Estadisticas
total = len(results)
bet_ah = [r for r in results if r['ah'] != 'NO_BET']
bet_ou = [r for r in results if r['ou'] != 'NO_BET']
fav_picks = [r for r in bet_ah if r['ah'] == 'FAV_CUBRE']
dog_picks = [r for r in bet_ah if r['ah'] == 'DOG_CUBRE']
over_picks = [r for r in bet_ou if r['ou'] == 'OVER']
under_picks = [r for r in bet_ou if r['ou'] == 'UNDER']

print(f'=== RESULTADOS CLAVE DICOTOMICA V6/V7 ===')
print(f'Total partidos en precacheo: {total}')
print(f'AH picks totales: {len(bet_ah)} ({len(bet_ah)/total*100:.1f}%)')
print(f'  FAV_CUBRE: {len(fav_picks)} | DOG_CUBRE: {len(dog_picks)}')
print(f'OU picks totales: {len(bet_ou)} ({len(bet_ou)/total*100:.1f}%)')
print(f'  OVER: {len(over_picks)} | UNDER: {len(under_picks)}')
print()

# Mostrar todos los picks
print('=== PICKS CONCRETOS ===')
for r in results:
    if r['ah'] != 'NO_BET' or r['ou'] != 'NO_BET':
        mr_all = r['mr_dog'] + r['mr_fav']
        mr_ou_all = r['mr_under'] + r['mr_over']
        print(f"{r['date']} | {r['home']} vs {r['away']}")
        print(f"  AH {r.get('ah_raw','?')} | OU {r.get('ou_raw','?')}")
        print(f"  PICK AH:  {r['ah_label']}")
        print(f"  PICK OU:  {r['ou_label']}")
        print(f"  edge_AH={r['edge_AH']:+.2f} | edge_OU={r['edge_OU']:+.2f}")
        if mr_all: print(f"  MR: {' | '.join(mr_all)}")
        if mr_ou_all: print(f"  MR_OU: {' | '.join(mr_ou_all)}")
        print()

# Guardar JSON de resultados
with open('picks_clave_dicotomica.json', 'w', encoding='utf-8') as f:
    json.dump(results, f, ensure_ascii=False, indent=2)
print('Guardado: picks_clave_dicotomica.json')
