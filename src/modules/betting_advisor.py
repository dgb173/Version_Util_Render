# src/modules/betting_advisor.py
"""
Sistema de recomendación de apuestas basado en:
- Comparación de handicap (quién es favorito)
- Score compuesto de estadísticas (Tiros, TaP, Ataques, Ataques Peligrosos)
- 7 fuentes: Prev Home, Prev Away, H2H Estadio, H2H General, H2H Col3, Ind Local, Ind Visitante
"""

class BettingAdvisor:
    
    # Las 4 estadísticas que analizamos
    STATS_KEYS = ['Tiros', 'Tiros a Puerta', 'Ataques', 'Ataques Peligrosos']
    
    def __init__(self):
        pass
    
    def _parse_stats_rows(self, stats_rows: list) -> dict:
        """
        Convierte stats_rows a un diccionario {label: {home: val, away: val}}
        """
        result = {}
        if not stats_rows:
            return result
        for row in stats_rows:
            label = row.get('label', '').strip()
            try:
                home_val = float(row.get('home', 0))
                away_val = float(row.get('away', 0))
                result[label] = {'home': home_val, 'away': away_val}
            except (ValueError, TypeError):
                continue
        return result
    
    def _compare_stats(self, stats_dict: dict, home_perspective: bool = True) -> dict:
        """
        Compara las 4 estadísticas y devuelve puntos para cada equipo.
        
        Args:
            stats_dict: Diccionario con stats parseadas
            home_perspective: True si 'home' en stats corresponde al LOCAL del partido principal,
                            False si está invertido (ej: partido away del visitante)
        
        Returns:
            {'local_points': X, 'visit_points': Y, 'details': [...]}
        """
        local_pts = 0
        visit_pts = 0
        details = []
        
        for stat_key in self.STATS_KEYS:
            if stat_key not in stats_dict:
                continue
            
            home_val = stats_dict[stat_key]['home']
            away_val = stats_dict[stat_key]['away']
            
            if home_perspective:
                local_val, visit_val = home_val, away_val
            else:
                local_val, visit_val = away_val, home_val
            
            if local_val > visit_val:
                local_pts += 1
                details.append(f"{stat_key}: L+")
            elif visit_val > local_val:
                visit_pts += 1
                details.append(f"{stat_key}: V+")
            # Empate = 0 puntos
        
        return {
            'local_points': local_pts,
            'visit_points': visit_pts,
            'details': details
        }
    
    def _analyze_source(self, source_data: dict, source_name: str, 
                        main_home_name: str, main_away_name: str) -> dict:
        """
        Analiza una fuente de datos y devuelve los puntos.
        Determina automáticamente la perspectiva correcta.
        """
        if not source_data:
            return {'local_points': 0, 'visit_points': 0, 'source': source_name, 'valid': False}
        
        stats_rows = source_data.get('stats_rows', [])
        if not stats_rows:
            return {'local_points': 0, 'visit_points': 0, 'source': source_name, 'valid': False}
        
        stats_dict = self._parse_stats_rows(stats_rows)
        if not stats_dict:
            return {'local_points': 0, 'visit_points': 0, 'source': source_name, 'valid': False}
        
        # Determinar perspectiva según el tipo de fuente
        home_perspective = True
        
        if source_name == 'prev_home':
            # El equipo LOCAL del partido principal jugó de local aquí
            home_perspective = True
        elif source_name == 'prev_away':
            # El equipo VISITANTE del partido principal jugó de visitante aquí
            # Por lo tanto, 'away' en stats = nuestro visitante
            home_perspective = False
        elif source_name in ['h2h_stadium', 'h2h_general']:
            # H2H: el 'home' del H2H puede no ser el mismo que el partido principal
            h2h_home = source_data.get('h2h_gen_home', source_data.get('home_team', ''))
            # Si el home del H2H es el local del partido principal
            home_perspective = (main_home_name.lower() in h2h_home.lower() or 
                               h2h_home.lower() in main_home_name.lower())
        elif source_name == 'h2h_col3':
            # H2H Col3: verificar quién está en posición home
            h2h_home = source_data.get('h2h_home_team_name', '')
            home_perspective = (main_home_name.lower() in h2h_home.lower() or 
                               h2h_home.lower() in main_home_name.lower())
        elif source_name == 'ind_local':
            # Comparativa indirecta del LOCAL
            localia = source_data.get('localia', 'H')
            home_perspective = (localia == 'H')
        elif source_name == 'ind_visitante':
            # Comparativa indirecta del VISITANTE  
            localia = source_data.get('localia', 'A')
            home_perspective = (localia == 'A')  # Invertido porque queremos perspectiva del visitante
        
        result = self._compare_stats(stats_dict, home_perspective)
        result['source'] = source_name
        result['valid'] = True
        
        return result
    
    def get_recommendation(self, match_data: dict) -> dict:
        """
        Analiza un partido y devuelve la recomendación.
        
        Returns:
            {
                'pick': 'BET_LOCAL' | 'BET_VISITA' | 'NO_BET' | 'CONFLICTO',
                'score_local': int,
                'score_visit': int,
                'score_display': '22-6',
                'favorito_ha': 'LOCAL' | 'VISITA' | 'NEUTRO',
                'confidence': 'high' | 'medium' | 'low',
                'sources_analyzed': int,
                'reason': str
            }
        """
        main_home_name = match_data.get('home_name', '')
        main_away_name = match_data.get('away_name', '')
        
        # Determinar favorito por handicap
        ah_linea = match_data.get('main_match_odds', {}).get('ah_linea', '0')
        try:
            ah_value = float(ah_linea)
        except (ValueError, TypeError):
            ah_value = 0
        
        if ah_value > 0:
            favorito_ha = 'LOCAL'
        elif ah_value < 0:
            favorito_ha = 'VISITA'
        else:
            favorito_ha = 'NEUTRO'
        
        # Analizar cada fuente
        sources_results = []
        
        # 1. Prev Home (último partido del local jugando de local)
        prev_home = match_data.get('last_home_match', {})
        sources_results.append(self._analyze_source(prev_home, 'prev_home', main_home_name, main_away_name))
        
        # 2. Prev Away (último partido del visitante jugando de visitante)
        prev_away = match_data.get('last_away_match', {})
        sources_results.append(self._analyze_source(prev_away, 'prev_away', main_home_name, main_away_name))
        
        # 3. H2H Estadio
        h2h_stadium = match_data.get('h2h_stadium', {})
        sources_results.append(self._analyze_source(h2h_stadium, 'h2h_stadium', main_home_name, main_away_name))
        
        # 4. H2H General
        h2h_general = match_data.get('h2h_general', {})
        sources_results.append(self._analyze_source(h2h_general, 'h2h_general', main_home_name, main_away_name))
        
        # 5. H2H Col3
        h2h_col3 = match_data.get('h2h_col3', {})
        sources_results.append(self._analyze_source(h2h_col3, 'h2h_col3', main_home_name, main_away_name))
        
        # 6 & 7. Comparativas Indirectas
        comparativas = match_data.get('comparativas_indirectas', {})
        ind_local = comparativas.get('left', {})
        ind_visitante = comparativas.get('right', {})
        sources_results.append(self._analyze_source(ind_local, 'ind_local', main_home_name, main_away_name))
        sources_results.append(self._analyze_source(ind_visitante, 'ind_visitante', main_home_name, main_away_name))
        
        # Sumar puntos totales
        total_local = sum(r['local_points'] for r in sources_results)
        total_visit = sum(r['visit_points'] for r in sources_results)
        valid_sources = sum(1 for r in sources_results if r.get('valid', False))
        
        # Determinar pick
        diff = total_local - total_visit
        total_points = total_local + total_visit
        
        # Calcular confianza basada en diferencia y fuentes válidas
        if valid_sources < 2:
            confidence = 'low'
            pick = 'NO_BET'
            reason = 'Pocas fuentes con datos'
        elif total_points == 0:
            confidence = 'low'
            pick = 'NO_BET'
            reason = 'Sin estadísticas disponibles'
        else:
            pct_local = (total_local / total_points) * 100 if total_points > 0 else 50
            
            if pct_local >= 65:  # LOCAL domina claramente
                confidence = 'high'
                stats_winner = 'LOCAL'
            elif pct_local >= 55:  # LOCAL algo mejor
                confidence = 'medium'
                stats_winner = 'LOCAL'
            elif pct_local <= 35:  # VISITA domina claramente
                confidence = 'high'
                stats_winner = 'VISITA'
            elif pct_local <= 45:  # VISITA algo mejor
                confidence = 'medium'
                stats_winner = 'VISITA'
            else:  # 45-55% = muy parejo
                confidence = 'low'
                stats_winner = 'NEUTRO'
                pick = 'NO_BET'
                reason = f'Stats equilibradas ({total_local}-{total_visit})'
            
            if stats_winner != 'NEUTRO':
                # Comparar con favorito por handicap
                if favorito_ha == stats_winner:
                    pick = f'BET_{stats_winner}'
                    reason = f'HA+Stats alineados ({total_local}-{total_visit})'
                elif favorito_ha == 'NEUTRO':
                    pick = f'BET_{stats_winner}'
                    reason = f'Stats dominan sin favorito HA ({total_local}-{total_visit})'
                else:
                    pick = 'CONFLICTO'
                    reason = f'HA dice {favorito_ha}, Stats dicen {stats_winner}'
        
        return {
            'pick': pick,
            'score_local': total_local,
            'score_visit': total_visit,
            'score_display': f'{total_local}-{total_visit}',
            'favorito_ha': favorito_ha,
            'confidence': confidence,
            'sources_analyzed': valid_sources,
            'reason': reason
        }


# Singleton para uso global
_advisor_instance = None

def get_advisor() -> BettingAdvisor:
    global _advisor_instance
    if _advisor_instance is None:
        _advisor_instance = BettingAdvisor()
    return _advisor_instance
