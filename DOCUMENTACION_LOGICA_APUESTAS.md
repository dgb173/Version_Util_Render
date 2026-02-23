# DOCUMENTACIÓN TÉCNICA: LÓGICA DE APUESTAS Y PATRONES (SISTEMA DE HÉRCULES)

Este documento detalla la lógica avanzada, los criterios de evaluación y las métricas implementadas en el sistema de predicción `mega_trainer.py` y `precacheo.html`, según las especificaciones del usuario.

## 1. Filosofía de "Infalibilidad" y Validación Temporal

El sistema evita el sobreajuste (overfitting) mediante una estricta separación temporal de los datos.

*   **Método "Máquina del Tiempo":**
    *   **Entrenamiento (80%):** La IA busca patrones solo en los partidos más antiguos. Encuentra reglas que funcionaron en el pasado (ej. "Local gana si viene de ganar fuera...").
    *   **Validación (20%):** Una vez encontrado un patrón, se pone a prueba ("Audit") contra los partidos más recientes, que la IA **nunca ha visto**.
    *   **Criterio de Aceptación:** Solo se aceptan patrones que mantienen un alto acierto (>60-70%) en la fase de validación. Si un patrón era bueno antes pero falla ahora, se descarta.

## 2. Lógica de Handicap Contextual (La "Expectativa")

El sistema no solo mira si un equipo ganó o perdió, sino si **cubrió la expectativa** (Handicap) que tenía la casa de apuestas en ese momento.

### Variables Clave:
*   **`il_covered_actual` (Indirect Left Covered):** Indica si el Local, en su partido indirecto previo, superó su handicap real.
    *   *Ejemplo:* Local tenía handicap +0.5 (No favorito). Empató 1-1. Resultado ajustado: 1.5 vs 1. **CUBRIÓ**.
*   **`ir_covered_actual` (Indirect Right Covered - Visita):** Indica si el Visitante, en su partido indirecto (jugando como Away contra un Rival), superó su handicap.
    *   *Lógica:* Si el Rival (Home) tenía handicap -0.5 y el partido quedó 1-1, el Rival FALLÓ (-0.5 vs 1). Por tanto, el Visitante (Away) CUBRIÓ (+0.5).
*   **`h2h_covered`:** Indica si en el último enfrentamiento directo (H2H), el equipo Local de hoy cubrió la línea de handicap que tenía *aquel día*.
*   **Estado de Favorito Previo (`ph_was_fav`, `pa_was_fav`):** Detecta si el equipo era favorito o "underdog" en su partido anterior, independientemente de si jugaba en casa o fuera.

## 3. Comparativa de Rendimiento Cruzado ("Performance Diff")

Se ha implementado una variable combinada de alto valor predictivo:
*   **`ind_perf_home_good`:** Se activa SOLO si:
    1.  El Local **CUBRIÓ** su handicap en su indirecta.
    2.  El Visitante **FALLÓ** su handicap en su indirecta (contra el mismo rival común).
    Esta métrica aísla la "forma real" de los equipos más allá del resultado simple.

## 4. Estadísticas de Dominio (Stats)

El sistema evalúa la calidad del juego mediante:
*   **Dominio Directo:** Comparación de *Promedios* (ej. `ph_mean_da > pa_mean_da`).
*   **Umbrales Absolutos:** Detección de picos de rendimiento.
    *   `ph_high_da`: Local tuvo > 50 Ataques Peligrosos.
    *   `pa_low_sot`: Visitante tuvo <= 2 Tiros a Puerta.
    *   *Uso:* La IA combina esto para encontrar escenarios de "Asedio Total".

## 5. Over/Under Contextual

Para los goles, se usan criterios similares:
*   **Tendencia de Mercado:** ¿La línea de gol subió o bajó antes del partido?
*   **Historia Reciente:** ¿Ambos equipos vienen de hacer "Over" en sus indirectas (`ind_ambas_over`)?

## 6. Métrica de ROI (Retorno de Inversión)

La visualización en el frontend (`precacheo.html`) ha sido ajustada para mostrar un ROI realista basado en cuotas promedio de **1.90**.
*   **Fórmula Anterior:** `Probabilidad - 50` (Errónea, muy pesimista).
*   **Fórmula Actual:** `(Probabilidad * 1.90) - 100`.
    *   *Ejemplo:* Un 66% de acierto real implica un ROI de aprox. +25% a cuota 1.90.

## 7. Interfaz Visual ("Red Pulse")

Para facilitar la identificación de las "Joyas" (Patrones Infalibles):
*   Se aplica un efecto de **borde rojo palpitante** (`animation: pulseRed`) **SOLO** sobre la etiqueta del Pick (Local/Visitante), evitando ensuciar toda la fila.
*   Esto asegura que el usuario sepa *exactamente* cuál es la apuesta recomendada de inmediato.
