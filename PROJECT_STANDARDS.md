# PROYECTO: Analizador de Apuestas - Estándares de Lógica

Este documento centraliza las reglas de negocio y lógica de cálculo para estandarizar el comportamiento del sistema y de la IA.

## 1. Lógica de Handicaps y Favoritismo
Para todos los cálculos del sistema (H2H Col3, Specialist Validator, ML Predictor):

*   **Handicap POSITIVO (> 0):** El favorito es el **LOCAL**.
*   **Handicap CERO o NEGATIVO (<= 0):** El favorito es el **VISITANTE**.
    *   *Regla Crítica:* Si el AH es exactamente **0**, el sistema debe tratar al **Visitante** como el favorito para los cálculos de cobertura y comparativas.

## 2. Definición de Resultados (Cover/WDL)
*   **VERDE (text-success / ✓):** El equipo favorito cubrió la línea (Ganó el handicap).
*   **ROJO (text-danger / ✗):** El equipo favorito falló (Perdió el handicap).
*   **NARANJA (text-push / -):** El resultado fue nulo (Push / Empate exacto con el handicap).

## 3. Comparativa H2H Col3 (Mirror Comparison)
Se utiliza para evaluar si el favorito de hoy está rindiendo mejor o peor que un equipo "espejo" en condiciones similares.
*   **Favorito:** Determinado por la regla del punto 1.
*   **Equipo Espejo:** El rival que enfrentó el oponente del favorito en su partido previo.
*   **Rendimiento:**
    *   **MEJORA:** El favorito obtuvo un mejor resultado (W > D > L) que el espejo.
    *   **IGUALA:** Ambos obtuvieron el mismo resultado.
    *   **EMPEORA:** El favorito obtuvo un peor resultado que el espejo.

## 4. Tipos de Comparación
*   **DIRECTA:** Las localías coinciden. El favorito de hoy jugó en la misma condición que se analiza en el histórico.
*   **INVERSA:** Las localías están cambiadas. El favorito de hoy jugó en la condición contraria a la que se analiza en el histórico.

## 5. Filtros de Calidad de Datos
*   **Juveniles:** Por defecto, se excluyen partidos de categorías U19, U21, etc., a menos que sean ligas específicas permitidas (ej. Italia Primavera, Inglaterra PL2).
*   **Ligas Neutras:** Si una liga es marcada como neutra, el handicap no se invierte en el scraping y se trata con una lógica de campo neutral.
