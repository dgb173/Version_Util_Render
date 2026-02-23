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
        
        # Cargar datos
        data_dir = Path(__file__).resolve().parent.parent / 'data'
        cp_path = data_dir / 'cp_1.json'
        
        with open(cp_path, 'r', encoding='utf-8') as f:
            matches = json.load(f)
        
        # Encontrar partido solicitado
        target_match = next((m for m in matches if m.get('match_id') == match_id), None)
        if not target_match:
            return jsonify({'error': 'Match not found'}), 404
        
        # Info del partido actual
        current_home = target_match.get('home_name')
        current_away = target_match.get('away_name')
        current_ah = float(target_match.get('main_match_odds', {}).get('ah_linea', 0))
        
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
                
                if was_favorite !=should_be_favorite:
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
        
        results = []
        # TODO: Aquí iría la lógica para buscar partidos similares en la base de datos
        # Por ahora retorno estructura vacía
        
        return jsonify({
            'status': 'success',
            'match_info': match_info,
            'results': results,
            'prev_home': prev_home,
            'prev_away': prev_away
        })
    
    except Exception as e:
        print(f"Error in precacheo_pattern_search: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

