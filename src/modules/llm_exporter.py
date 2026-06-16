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
        
        # is_favorite: 
        # Si is_home y ah_local < 0 -> local es favorito
        # Si no is_home y ah_local > 0 -> visitante es favorito (ah_local es negativo para local, positivo para visitante)
        
        if diff > abs_ah: return "CUBRIÓ"
        elif diff < abs_ah: return "NO CUBRIÓ"
        else: return "PUSH (Igualó)"
    except:
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
    score = clean_value(payload.get("score") or payload.get("final_score"))
    
    md = []
    md.append(f"# Partido: {home} vs {away}")
    md.append(f"- **ID**: {payload.get('match_id') or payload.get('id')}")
    md.append(f"- **Liga**: {league}")
    md.append(f"- **Fecha**: {date}")
    md.append(f"- **Hándicap Inicial**: {handicap}")
    md.append(f"- **Resultado FT**: {score}")
    md.append(f"- **Estado**: {clean_value(payload.get('state') or 'precacheo')}")
    md.append(f"- **Bucket**: {clean_value(payload.get('bucket') or 'data_precacheo.json')}")
    
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
        md.append(f"- **Línea de Hándicap**: {clean_value(lhm.get('handicap_line_raw'))}")
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
        md.append(f"- **Línea de Hándicap**: {clean_value(lam.get('handicap_line_raw'))}")
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
        he_movement = h2h_estadio.get("ah1") or m_estadio.get("movement") or "-"
        
        md.append(f"- **Encuentro**: {he_home} vs {he_away}")
        md.append(f"- **Fecha**: {clean_value(he_date)}")
        md.append(f"- **Resultado**: {clean_value(he_score)}")
        md.append(f"- **Movimiento**: {clean_value(he_movement)}")
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
        hg_movement = h2h_general.get("ah6") or m_general.get("movement") or "-"
        
        md.append(f"- **Encuentro**: {hg_home} vs {hg_away}")
        md.append(f"- **Fecha**: {clean_value(hg_date)}")
        md.append(f"- **Resultado**: {clean_value(hg_score)}")
        md.append(f"- **Movimiento**: {clean_value(hg_movement)}")
        md.append("- **Estadísticas**:")
        md.append(format_stats(h2h_general.get("stats_rows"), hg_home, hg_away))

    # H2H Col3 (Espejo)
    col3 = payload.get("h2h_col3")
    if col3 and isinstance(col3, dict) and col3.get("status") == "found":
        md.append(f"\n## Enfrentamiento Col3 Espejo (H2H)")
        c3_home = clean_value(col3.get("h2h_home_team_name") or col3.get("home_team"))
        c3_away = clean_value(col3.get("h2h_away_team_name") or col3.get("away_team"))
        score_c3 = f"{col3.get('goles_home')}:{col3.get('goles_away')}" if col3.get("goles_home") is not None else col3.get("score", "-")
        md.append(f"- **Encuentro**: {c3_home} vs {c3_away}")
        md.append(f"- **Fecha**: {clean_value(col3.get('date'))}")
        md.append(f"- **Resultado**: {score_c3}")
        md.append(f"- **Hándicap Espejo**: {clean_value(col3.get('handicap') or col3.get('ah_line'))}")
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
            md.append(f"  - **Encuentro**: {il_home} vs {il_away}")
            md.append(f"  - **Fecha**: {clean_value(ind_l.get('date'))}")
            md.append(f"  - **Resultado**: {clean_value(ind_l.get('score'))}")
            md.append(f"  - **Hándicap**: {clean_value(ind_l.get('ah_line') or ind_l.get('ah'))}")
            md.append("  - **Estadísticas**:")
            md.append(format_stats(ind_l.get("stats_rows"), il_home, il_away))
            
        if has_r:
            md.append(f"### Indirecta Visitante ({away} vs Rival)")
            ir_home = clean_value(ind_r.get("home_team"))
            ir_away = clean_value(ind_r.get("away_team"))
            md.append(f"  - **Encuentro**: {ir_home} vs {ir_away}")
            md.append(f"  - **Fecha**: {clean_value(ind_r.get('date'))}")
            md.append(f"  - **Resultado**: {clean_value(ind_r.get('score'))}")
            md.append(f"  - **Hándicap**: {clean_value(ind_r.get('ah_line') or ind_r.get('ah'))}")
            md.append("  - **Estadísticas**:")
            md.append(format_stats(ind_r.get("stats_rows"), ir_home, ir_away))

    return "\n".join(md)

def generate_llm_prompt(match):
    """
    Generates a structured prompt designed to be copy-pasted into an LLM
    like NotebookLM or ChatGPT to get predictions.
    Uses the exact same clean Markdown structure used for NotebookLM exports.
    """
    return generate_notebooklm_match_format(match)