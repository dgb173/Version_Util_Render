# src/modules/backtesting.py

class BettingSimulator:
    def __init__(self):
        # Estados posibles
        self.WIN = "GANADO"
        self.LOSS = "PERDIDO"
        self.PUSH = "NULO" # O medio nulo

    def evaluar_linea(self, goles_favor, goles_contra, linea):
        """
        Evalúa si se supera una línea (AH o O/U) dado un marcador.
        linea: Puede ser AH (ej. -0.75) o O/U (ej. 2.5)
        """
        # La lógica matemática exacta para AH y O/U
        diferencia = goles_favor - goles_contra
        resultado_apuesta = diferencia + linea
        
        # Lógica simplificada para demostración (Tú ya tienes la compleja en tu sistema)
        # Nota: En un sistema real, esto debería manejar medios ganados/perdidos
        if resultado_apuesta > 0.25: return self.WIN
        if resultado_apuesta < -0.25: return self.LOSS
        return self.PUSH 

    def evaluar_over_under(self, goles_totales, linea_ou):
        if goles_totales > linea_ou: return "OVER"
        if goles_totales < linea_ou: return "UNDER"
        return "PUSH"

    def simular_escenario_actual(self, clones_historicos, target_ah, target_ou):
        """
        clones_historicos: Lista de partidos pasados que coinciden en patrón.
        target_ah: El hándicap del partido DE HOY (-0.75).
        target_ou: La línea de gol del partido DE HOY (2.0).
        """
        stats = {
            "total_muestras": len(clones_historicos),
            "ah_wins": 0,
            "ou_overs": 0,
            "ou_unders": 0,
            "match_ids": []
        }

        # print(f"🔄 Simulando líneas actuales (AH: {target_ah} | O/U: {target_ou}) en {len(clones_historicos)} partidos clones...")

        for partido in clones_historicos:
            # Extraemos el resultado real que ocurrió en el pasado
            # Adaptamos para usar las claves que vienen de estudio_scraper
            # score_raw suele ser "3-1"
            score_raw = partido.get('score_raw', '')
            if not score_raw or '-' not in score_raw:
                continue
                
            try:
                goles_local, goles_visit = map(int, score_raw.split('-'))
            except ValueError:
                continue

            total_goles = goles_local + goles_visit
            
            # Guardamos ID
            if 'matchIndex' in partido:
                stats['match_ids'].append(partido['matchIndex'])
            elif 'match_id' in partido:
                stats['match_ids'].append(partido['match_id'])
            
            # 1. SIMULACIÓN HÁNDICAP ACTUAL
            # IMPORTANTE: La lógica de evaluar_linea asume que 'linea' se suma a la diferencia.
            # Si AH es -0.75 para el local, y gana 2-0 (diff +2), 2 + (-0.75) = 1.25 > 0 -> WIN.
            # Si AH es +0.5 para el local, y empata 1-1 (diff 0), 0 + 0.5 = 0.5 > 0 -> WIN.
            # Esta lógica es correcta para perspectiva LOCAL.
            
            # Si el partido histórico es desde la perspectiva del equipo analizado (que puede ser local o visitante en ese partido),
            # necesitamos saber si 'goles_local' se refiere a nuestro equipo o no.
            # En estudio_scraper, 'recent_home_matches' son partidos donde el equipo analizado JUGÓ DE LOCAL.
            # 'recent_away_matches' son partidos donde el equipo analizado JUGÓ DE VISITANTE.
            
            # Asumiremos que 'target_ah' siempre es relativo al equipo LOCAL del partido ACTUAL.
            # Si estamos analizando al equipo LOCAL actual con sus partidos de LOCAL históricos:
            # Usamos goles_local - goles_visit.
            
            # Si estamos analizando al equipo VISITANTE actual con sus partidos de VISITANTE históricos:
            # El AH del visitante es usualmente el inverso del local.
            # Pero 'target_ah' suele ser la línea principal del partido.
            # Si target_ah es -0.5 (Local favorito), entonces para el Visitante es +0.5.
            
            # Para simplificar, simular_escenario_actual debería recibir el AH relativo al equipo que estamos probando.
            # O bien, asumimos que target_ah es siempre desde la perspectiva del LOCAL del partido ACTUAL.
            
            # Vamos a asumir que target_ah es la línea del LOCAL.
            # Y vamos a evaluar si esa línea se hubiera cubierto en el partido histórico.
            
            # CASO 1: Estamos backtesteando al LOCAL actual con sus partidos de LOCAL.
            # Usamos goles_local (histórico) y goles_visit (histórico).
            resultado_ah = self.evaluar_linea(goles_local, goles_visit, target_ah)
            if resultado_ah == self.WIN:
                stats['ah_wins'] += 1
            
            # 2. SIMULACIÓN OVER/UNDER ACTUAL
            resultado_ou = self.evaluar_over_under(total_goles, target_ou)
            if resultado_ou == "OVER":
                stats['ou_overs'] += 1
            elif resultado_ou == "UNDER":
                stats['ou_unders'] += 1

        # Calcular porcentajes
        if stats['total_muestras'] > 0:
            prob_ah = (stats['ah_wins'] / stats['total_muestras']) * 100
            prob_over = (stats['ou_overs'] / stats['total_muestras']) * 100
            prob_under = (stats['ou_unders'] / stats['total_muestras']) * 100
            
            return {
                "validez": True,
                "stats": stats,
                "prob_ah": prob_ah,
                "prob_over": prob_over,
                "prob_under": prob_under,
                "mensaje": f"📊 ANÁLISIS DE LÍNEAS ACTUALES:\n"
                           f" - La línea AH {target_ah} se hubiera cubierto en el {prob_ah:.1f}% de casos similares.\n"
                           f" - La línea O/U {target_ou} hubiera sido OVER el {prob_over:.1f}% y UNDER el {prob_under:.1f}%."
            }
        else:
            return {"validez": False, "mensaje": "No hay suficientes clones para simular."}
