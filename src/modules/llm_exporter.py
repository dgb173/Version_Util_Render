def format_stats_block(stats_rows, team_a_name, team_b_name):
    if not stats_rows:
        return "  * (No hay estadísticas detalladas)\n"
    
    text = ""
    for row in stats_rows:
        label = row.get('label', '')
        val_a = row.get('home', '0')
        val_b = row.get('away', '0')
        text += f"  * {label}: {val_a} ({team_a_name}) vs {val_b} ({team_b_name})\n"
    return text

def calculate_wdl(score_str, ah_line, is_home):
    if not score_str or ah_line == 'N/A': return "Desconocido"
    try:
        parts = score_str.replace('-', ':').split(':')
        hg = int(parts[0])
        ag = int(parts[1])
        ah = float(ah_line)
        
        # En la DB: positivo = Local favorito. Para calcular, invertimos el AH a perspectiva local.
        ah_local = -ah 
        
        diff = hg - ag if is_home else ag - hg
        abs_ah = abs(ah_local)
        
        if diff > abs_ah: return "CUBRIÓ"
        elif diff < abs_ah: return "NO CUBRIÓ"
        else: return "PUSH (Igualó)"
    except:
        return "Desconocido"

def parse_handicap(val):
    if val is None or val == "" or val == "N/A" or val == "-":
        return None
    try:
        val_str = str(val).replace(",", ".").strip()
        if "→" in val_str:
            val_str = val_str.split("→")[-1].strip()
        elif "->" in val_str:
            val_str = val_str.split("->")[-1].strip()
        return float(val_str)
    except ValueError:
        return None

def evaluate_handicap_cover(score_str, ah_line_raw, is_home_subject):
    if not score_str or score_str in ("-", "?:?", "?-?"):
        return ""
    ah_num = parse_handicap(ah_line_raw)
    if ah_num is None:
        return ""
    try:
        parts = score_str.replace('-', ':').split(':')
        goles_h = int(parts[0])
        goles_a = int(parts[1])
        
        if ah_num > 0:
            fav_is_local = True
            abs_ah = ah_num
        elif ah_num < 0:
            fav_is_local = False
            abs_ah = abs(ah_num)
        else:
            if goles_h > goles_a:
                return "CUBRIÓ" if is_home_subject else "NO CUBRIÓ"
            elif goles_a > goles_h:
                return "NO CUBRIÓ" if is_home_subject else "CUBRIÓ"
            else:
                return "PUSH (Igualó)"

        diff_local = goles_h - goles_a
        
        if fav_is_local:
            fav_margin = diff_local - abs_ah
        else:
            fav_margin = -diff_local - abs_ah
            
        if fav_margin > 0.01:
            fav_covered = True
            fav_push = False
        elif fav_margin < -0.01:
            fav_covered = False
            fav_push = False
        else:
            fav_covered = False
            fav_push = True
            
        subject_is_fav = (fav_is_local and is_home_subject) or (not fav_is_local and not is_home_subject)
        
        if fav_push:
            return "PUSH (Igualó)"
        
        if subject_is_fav:
            return "CUBRIÓ" if fav_covered else "NO CUBRIÓ"
        else:
            return "CUBRIÓ" if not fav_covered else "NO CUBRIÓ"
    except Exception:
        return ""

def check_handicap_cover_pure(score_str, ah_line_num, favorite_team_name, home_team_in_h2h, away_team_in_h2h, main_home_team_name):
    if not score_str or score_str in ("-", "?:?", "?-?"):
        return "Desconocido"
    try:
        parts = score_str.replace(':', '-').split('-')
        goles_h = int(parts[0])
        goles_a = int(parts[1])
        
        if ah_line_num is None:
            return "Desconocido"
            
        if ah_line_num == 0.0:
            if main_home_team_name.lower() == home_team_in_h2h.lower():
                if goles_h > goles_a: return "CUBRIÓ"
                elif goles_a > goles_h: return "NO CUBRIÓ"
                else: return "PUSH (Igualó)"
            else:
                if goles_a > goles_h: return "CUBRIÓ"
                elif goles_h > goles_a: return "NO CUBRIÓ"
                else: return "PUSH (Igualó)"
        
        if favorite_team_name.lower() == home_team_in_h2h.lower():
            favorite_margin = goles_h - goles_a
        elif favorite_team_name.lower() == away_team_in_h2h.lower():
            favorite_margin = goles_a - goles_h
        else:
            return "Desconocido"
            
        diff = favorite_margin - abs(ah_line_num)
        if diff > 0.01:
            return "CUBRIÓ"
        elif diff < -0.01:
            return "NO CUBRIÓ"
        else:
            return "PUSH (Igualó)"
    except Exception:
        return "Desconocido"

def generate_notebooklm_match_format(payload):
    if not payload:
        return "Error: No se pudieron cargar los datos del partido."

    def clean_value(val):
        if val is None or val == "" or val == "N/A" or val == "undefined":
            return "-"
        return str(val)

    def format_stats(stats_list, home_team="", away_team=""):
        if not stats_list or not isinstance(stats_list, list):
            return "    - Sin estadísticas detalladas."
        
        lines = []
        for row in stats_list:
            label = row.get("label") or row.get("name") or ""
            home_val = clean_value(row.get("home"))
            away_val = clean_value(row.get("away"))
            if label:
                lines.append(f"    - {label}: {home_val} ({home_team}) vs {away_val} ({away_team})")
                
        return "\n".join(lines) if lines else "    - Sin estadísticas detalladas."

    home = clean_value(payload.get("home_name") or payload.get("home_team"))
    away = clean_value(payload.get("away_name") or payload.get("away_team"))
    league = clean_value(payload.get("league_name") or payload.get("league"))
    date = clean_value(payload.get("match_date") or payload.get("date"))
    handicap = clean_value(payload.get("handicap") or (payload.get("main_match_odds") or {}).get("ah_linea"))
    goal_line = clean_value(payload.get("goal_line") or (payload.get("main_match_odds") or {}).get("goals_linea"))
    score = clean_value(payload.get("score") or payload.get("final_score"))
    
    md = []
    md.append(f"# Partido: {home} vs {away}")
    md.append(f"- **Liga**: {league}")
    md.append(f"- **Fecha**: {date}")
    md.append(f"- **Hándicap Inicial**: {handicap}")
    md.append(f"- **Línea de Goles**: {goal_line}")
    md.append(f"- **Resultado FT**: {score}")
    
    # Standings
    home_std = payload.get("home_standings") or {}
    away_std = payload.get("away_standings") or {}
    if home_std or away_std:
        md.append("\n## Clasificación / Posición")
        md.append(f"- **{home}**: Puesto {clean_value(home_std.get('ranking'))} (Stats general: {clean_value(home_std.get('total_v'))}-{clean_value(home_std.get('total_e'))}-{clean_value(home_std.get('total_d'))})")
        md.append(f"- **{away}**: Puesto {clean_value(away_std.get('ranking'))} (Stats general: {clean_value(away_std.get('total_v'))}-{clean_value(away_std.get('total_e'))}-{clean_value(away_std.get('total_d'))})")

    # Prev Home
    lhm = payload.get("last_home_match")
    if lhm and isinstance(lhm, dict):
        md.append(f"\n## Partido Previo de {home} (Local)")
        lhm_home = clean_value(lhm.get("home_team"))
        lhm_away = clean_value(lhm.get("away_team"))
        md.append(f"- **Encuentro**: {lhm_home} vs {lhm_away}")
        md.append(f"- **Fecha**: {clean_value(lhm.get('date'))}")
        md.append(f"- **Resultado**: {clean_value(lhm.get('score'))}")
        
        # Añadir si cubrió
        lhm_hc = clean_value(lhm.get('handicap_line_raw'))
        cover_status = evaluate_handicap_cover(lhm.get("score"), lhm.get("handicap_line_raw"), True)
        if cover_status:
            lhm_hc = f"{lhm_hc} ({cover_status})"
        md.append(f"- **Línea de Hándicap**: {lhm_hc}")
        
        md.append("- **Estadísticas**:")
        md.append(format_stats(lhm.get("stats_rows"), lhm_home, lhm_away))

    # Prev Away
    lam = payload.get("last_away_match")
    if lam and isinstance(lam, dict):
        md.append(f"\n## Partido Previo de {away} (Visitante)")
        lam_home = clean_value(lam.get("home_team"))
        lam_away = clean_value(lam.get("away_team"))
        md.append(f"- **Encuentro**: {lam_home} vs {lam_away}")
        md.append(f"- **Fecha**: {clean_value(lam.get('date'))}")
        md.append(f"- **Resultado**: {clean_value(lam.get('score'))}")
        
        # Añadir si cubrió
        lam_hc = clean_value(lam.get('handicap_line_raw'))
        cover_status = evaluate_handicap_cover(lam.get("score"), lam.get("handicap_line_raw"), False)
        if cover_status:
            lam_hc = f"{lam_hc} ({cover_status})"
        md.append(f"- **Línea de Hándicap**: {lam_hc}")
        
        md.append("- **Estadísticas**:")
        md.append(format_stats(lam.get("stats_rows"), lam_home, lam_away))

    # H2H Estadio
    h2h_estadio = payload.get("h2h_stadium") or {}
    m_estadio = (payload.get("market_analysis_data") or {}).get("stadium") or {}
    
    has_stadium = h2h_estadio.get("match1_id") is not None or h2h_estadio.get("res1") not in (None, "?:?", "?-?") or m_estadio.get("result") not in (None, "", "N/A")
    
    if has_stadium:
        md.append(f"\n## Enfrentamiento Directo Estadio (H2H Estadio)")
        he_home = home
        he_away = away
        he_date = h2h_estadio.get("date1") or m_estadio.get("date") or "N/A"
        he_score = h2h_estadio.get("res1") or m_estadio.get("result") or m_estadio.get("score") or "?:?"
        
        # Priorizar el movimiento de cuota completo calculado
        he_movement = m_estadio.get("movement")
        if not he_movement or str(he_movement).strip() in ("N/A", "-"):
            he_movement = h2h_estadio.get("ah1") or "-"
            
        # Calcular cobertura H2H
        he_cover = m_estadio.get("evaluation")
        if not he_cover or str(he_cover).strip() in ("N/A", "-"):
            current_ah_raw = payload.get("handicap") or (payload.get("main_match_odds") or {}).get("ah_linea")
            current_ah = parse_handicap(current_ah_raw)
            if current_ah is not None:
                fav_team = home if current_ah > 0 else (away if current_ah < 0 else home)
                he_cover = check_handicap_cover_pure(he_score, current_ah, fav_team, home, away, home)
            else:
                he_cover = ""
                
        # Normalizar el texto de cobertura
        he_cover_str = ""
        if he_cover:
            he_cover_upper = str(he_cover).upper()
            if "NO CUBIERTO" in he_cover_upper or "FALLÓ" in he_cover_upper or "FALLO" in he_cover_upper or "NO CUBRIÓ" in he_cover_upper or "NO CUBRIO" in he_cover_upper:
                he_cover_str = "NO CUBRIÓ"
            elif "CUBIERTO" in he_cover_upper or "CUBRIÓ" in he_cover_upper or "CUBRIO" in he_cover_upper:
                he_cover_str = "CUBRIÓ"
            elif "PUSH" in he_cover_upper or "IGUALÓ" in he_cover_upper or "IGUALO" in he_cover_upper:
                he_cover_str = "PUSH (Igualó)"
                
        he_movement_display = clean_value(he_movement)
        if he_cover_str:
            he_movement_display = f"{he_movement_display} ({he_cover_str})"
        
        md.append(f"- **Encuentro**: {he_home} vs {he_away}")
        md.append(f"- **Fecha**: {clean_value(he_date)}")
        md.append(f"- **Resultado**: {clean_value(he_score)}")
        md.append(f"- **Movimiento**: {he_movement_display}")
        md.append("- **Estadísticas**:")
        md.append(format_stats(h2h_estadio.get("stats_rows"), he_home, he_away))

    # H2H General
    h2h_general = payload.get("h2h_general") or {}
    m_general = (payload.get("market_analysis_data") or {}).get("general") or {}
    
    has_general = h2h_general.get("match6_id") is not None or h2h_general.get("res6") not in (None, "?:?", "?-?") or m_general.get("result") not in (None, "", "N/A")
    
    if has_general:
        md.append(f"\n## Enfrentamiento Directo General (H2H General)")
        hg_home = h2h_general.get("h2h_gen_home") or m_general.get("home_team") or home
        hg_away = h2h_general.get("h2h_gen_away") or m_general.get("away_team") or away
        hg_date = h2h_general.get("date6") or m_general.get("date") or "N/A"
        hg_score = h2h_general.get("res6") or m_general.get("result") or m_general.get("score") or "?:?"
        
        # Priorizar el movimiento de cuota completo calculado
        hg_movement = m_general.get("movement")
        if not hg_movement or str(hg_movement).strip() in ("N/A", "-"):
            hg_movement = h2h_general.get("ah6") or "-"
            
        # Calcular cobertura H2H
        hg_cover = m_general.get("evaluation")
        if not hg_cover or str(hg_cover).strip() in ("N/A", "-"):
            current_ah_raw = payload.get("handicap") or (payload.get("main_match_odds") or {}).get("ah_linea")
            current_ah = parse_handicap(current_ah_raw)
            if current_ah is not None:
                fav_team = home if current_ah > 0 else (away if current_ah < 0 else home)
                hg_cover = check_handicap_cover_pure(hg_score, current_ah, fav_team, hg_home, hg_away, home)
            else:
                hg_cover = ""
                
        # Normalizar el texto de cobertura
        hg_cover_str = ""
        if hg_cover:
            hg_cover_upper = str(hg_cover).upper()
            if "NO CUBIERTO" in hg_cover_upper or "FALLÓ" in hg_cover_upper or "FALLO" in hg_cover_upper or "NO CUBRIÓ" in hg_cover_upper or "NO CUBRIO" in hg_cover_upper:
                hg_cover_str = "NO CUBRIÓ"
            elif "CUBIERTO" in hg_cover_upper or "CUBRIÓ" in hg_cover_upper or "CUBRIO" in hg_cover_upper:
                hg_cover_str = "CUBRIÓ"
            elif "PUSH" in hg_cover_upper or "IGUALÓ" in hg_cover_upper or "IGUALO" in hg_cover_upper:
                hg_cover_str = "PUSH (Igualó)"
                
        hg_movement_display = clean_value(hg_movement)
        if hg_cover_str:
            hg_movement_display = f"{hg_movement_display} ({hg_cover_str})"
        
        md.append(f"- **Encuentro**: {hg_home} vs {hg_away}")
        md.append(f"- **Fecha**: {clean_value(hg_date)}")
        md.append(f"- **Resultado**: {clean_value(hg_score)}")
        md.append(f"- **Movimiento**: {hg_movement_display}")
        md.append("- **Estadísticas**:")
        md.append(format_stats(h2h_general.get("stats_rows"), hg_home, hg_away))

    # H2H Col3 (Espejo)
    col3 = payload.get("h2h_col3")
    if col3 and isinstance(col3, dict) and col3.get("status") == "found":
        md.append(f"\n## Enfrentamiento Col3 Espejo (H2H)")
        c3_home = clean_value(col3.get("h2h_home_team_name") or col3.get("home_team"))
        c3_away = clean_value(col3.get("h2h_away_team_name") or col3.get("away_team"))
        score_c3 = f"{col3.get('goles_home')}:{col3.get('goles_away')}" if col3.get("goles_home") is not None else col3.get("score", "-")
        
        # Calcular si cubrió
        c3_ah = clean_value(col3.get('handicap') or col3.get('ah_line'))
        cover_status = evaluate_handicap_cover(score_c3, col3.get('handicap') or col3.get('ah_line'), True)
        if cover_status:
            c3_ah = f"{c3_ah} ({cover_status})"
            
        md.append(f"- **Encuentro**: {c3_home} vs {c3_away}")
        md.append(f"- **Fecha**: {clean_value(col3.get('date'))}")
        md.append(f"- **Resultado**: {score_c3}")
        md.append(f"- **Hándicap Espejo**: {c3_ah}")
        md.append("- **Estadísticas**:")
        md.append(format_stats(col3.get("stats_rows"), c3_home, c3_away))

    # Comparativas Indirectas
    ind = payload.get("comparativas_indirectas") or {}
    ind_l = ind.get("left")
    ind_r = ind.get("right")
    
    has_l = ind_l and ind_l.get("home_team") is not None
    has_r = ind_r and ind_r.get("home_team") is not None
    
    if has_l or has_r:
        md.append("\n## Comparativas Indirectas")
        if has_l:
            md.append(f"### Indirecta Local ({home} vs Rival)")
            il_home = clean_value(ind_l.get("home_team"))
            il_away = clean_value(ind_l.get("away_team"))
            
            # Calcular si cubrió
            il_is_home = ind_l.get("localia") in ("H", "L", "Local", "Home")
            il_ah = clean_value(ind_l.get('ah_line') or ind_l.get('ah'))
            cover_status = evaluate_handicap_cover(ind_l.get('score'), ind_l.get('ah_line') or ind_l.get('ah'), il_is_home)
            if cover_status:
                il_ah = f"{il_ah} ({cover_status})"
                
            md.append(f"  - **Encuentro**: {il_home} vs {il_away}")
            md.append(f"  - **Fecha**: {clean_value(ind_l.get('date'))}")
            md.append(f"  - **Resultado**: {clean_value(ind_l.get('score'))}")
            md.append(f"  - **Hándicap**: {il_ah}")
            md.append("  - **Estadísticas**:")
            md.append(format_stats(ind_l.get("stats_rows"), il_home, il_away))
            
        if has_r:
            md.append(f"### Indirecta Visitante ({away} vs Rival)")
            ir_home = clean_value(ind_r.get("home_team"))
            ir_away = clean_value(ind_r.get("away_team"))
            
            # Calcular si cubrió
            ir_is_home = ind_r.get("localia") in ("H", "L", "Local", "Home")
            ir_ah = clean_value(ind_r.get('ah_line') or ind_r.get('ah'))
            cover_status = evaluate_handicap_cover(ind_r.get('score'), ind_r.get('ah_line') or ind_r.get('ah'), ir_is_home)
            if cover_status:
                ir_ah = f"{ir_ah} ({cover_status})"
                
            md.append(f"  - **Encuentro**: {ir_home} vs {ir_away}")
            md.append(f"  - **Fecha**: {clean_value(ind_r.get('date'))}")
            md.append(f"  - **Resultado**: {clean_value(ind_r.get('score'))}")
            md.append(f"  - **Hándicap**: {ir_ah}")
            md.append("  - **Estadísticas**:")
            md.append(format_stats(ind_r.get("stats_rows"), ir_home, ir_away))

    return "\n".join(md)


def _plain_clipboard_text(text):
    return text.replace("*", "")


def _analysis_instructions(match):
    goal_line = (
        match.get("goal_line")
        or (match.get("main_match_odds") or {}).get("goals_linea")
        or "N/A"
    )
    mls_block = ""
    try:
        from . import mls_system
        mls_res = mls_system.analyze_mls_match(match)
        if mls_res.get('is_mls') and mls_res.get('recommended_picks'):
            picks_str = "\n".join([f"  * [{p.get('rule_name')}] {p.get('pick')} ({p.get('confidence')} confianza) - {p.get('reason')}" for p in mls_res['recommended_picks']])
            mls_block = f"\n\n========================================================================\nSISTEMA MLS (HÁNDICAP + OVER/UNDER DEDICADO)\n========================================================================\n{picks_str}\n"
    except Exception as e:
        mls_block = ""

    return f"""{mls_block}

---

ANÁLISIS SOLICITADO

Actúa como analista de datos deportivos. Usa exclusivamente los datos anteriores y no inventes valores. Si falta un dato, escribe N/A.

Genera primero esta tabla Markdown compacta:

| Equipo | Pos | Registro | Pts | AH cubierto | Tendencia O/U | Tiros | Tiros a puerta | Ataques | Ataques peligrosos | Eficiencia ofensiva | Eficiencia defensiva | Alerta |
|---|---:|---|---:|---|---|---:|---:|---:|---:|---:|---:|---|

REGLAS DE CÁLCULO

1. Puntos: victorias × 3 + empates.
2. AH cubierto: calcula CUBRIÓ / (CUBRIÓ + NO CUBRIÓ). Excluye PUSH, desconocidos y partidos sin hándicap. Marca 🟢 si es >=60%, 🔴 si es <40% y 🟡 en el resto.
3. Over/Under: suma los goles de cada partido. Usa su línea O/U cuando esté indicada; si no aparece, compara con la línea actual {goal_line}. Excluye pushes del porcentaje. Marca 🟢 OVER si Over >=60%, 🔴 UNDER si Over <=40% y 🟡 EQUILIBRADO en el resto.
4. Promedios: calcula tiros, tiros a puerta, ataques y ataques peligrosos usando solo partidos que tengan esa estadística. Respeta siempre qué equipo es local o visitante en cada encuentro.
5. Eficiencia ofensiva: goles marcados / tiros a puerta propios. Eficiencia defensiva: goles recibidos / tiros a puerta del rival. Si el denominador es 0 o no existe, usa N/A.
6. Alerta: más de 60% derrotas = ⚠️ MAL MOMENTO; más de 60% victorias = 🏆 BUEN MOMENTO; más de 2 goles recibidos de media = ⚠️ DEFENSA FRÁGIL; menos de 1 gol marcado de media = 🔴 POBRE ATAQUE.

DEBAJO DE LA TABLA

- Resume los goles medios de ambos equipos.
- Compara sus porcentajes de cobertura AH.
- Resume los H2H disponibles.
- Avisa si ambos superan 55% Over o 55% Under.
- Sugiere obligatoriamente la mejor apuesta AH y la mejor apuesta O/U basándote en la tendencia de los datos, con una justificación breve. Queda PROHIBIDO responder NO BET.


No muestres IDs, buckets, nombres de tablas ni referencias internas del sistema.
"""

def generate_llm_prompt(match):
    """
    Generates a structured prompt designed to be copy-pasted into an LLM
    like NotebookLM or ChatGPT to get predictions.
    Uses the same structured sections, returned as plain text for clipboard use.
    """
    data_text = generate_notebooklm_match_format(match)
    return _plain_clipboard_text(data_text + _analysis_instructions(match))
