# Caso de Ejemplo: CS Cerrito vs Plaza Colonia (2026-06-28)

Este partido es un ejemplo canonico de como la repeticion de linea en el handicap y el marcador historico bruto ocultan una clara superioridad del local en volumen de juego.

## Datos del Partido
- **Encuentro**: CS Cerrito vs Plaza Colonia
- **Handicap Inicial (h)**: 0
- **Linea de Goles (OU)**: 2.0
- **Resultado FT**: 1:0 (Gano el Local)

---

## Triggers del Skill Activados

### 1. Repeticion del Handicap en H2H Estadio (misma_linea_h2h_local)
- En el ultimo enfrentamiento directo en este estadio (2024-07-20), el handicap fue 0 (DNB).
- El marcador bruto fue 0:1 a favor del visitante (Plaza Colonia).
- Hoy la linea vuelve a abrir en 0. El apostador promedio asume continuidad del favorito.

### 2. Volumen Oculto del Local en el H2H Directo (empate_con_superioridad_oculta)
Si abrimos el microscopio de estadisticas de aquel 0:1 previo en casa de Cerrito:
- **Tiros**: 6 (CS Cerrito) vs 2 (Plaza Colonia)
- **Tiros a Puerta**: 0 (CS Cerrito) vs 1 (Plaza Colonia)
- **Ataques**: 109 (CS Cerrito) vs 106 (Plaza Colonia)
- **Ataques Peligrosos**: 67 (CS Cerrito) vs 54 (Plaza Colonia)

*Lectura*: Cerrito domino todo el volumen de juego. Plaza Colonia marco en su unico tiro a puerta y gano de forma fortuita. La superioridad real de juego era del local, oculta bajo el marcador bruto 0-1.

### 3. Comparativa Indirecta Invertida (calidad_relativa_invertida)
- Cerrito (DOG) gano de visita a Paysandu FC 0:1 (teniendo handicap de dog +0.25).
- Plaza Colonia (FAV) perdio en casa contra CA Atenas 1:2 (siendo favorito -0.5).
- **diff_F** = margin_F_ind - margin_D_ind = -1 - 1 = -2.0 (Superioridad del local Cerrito contra los rivales comunes).

---

## Veredicto Metodologico

1. **Lectura**: El mercado mantiene el handicap 0 basandose en que Plaza Colonia gano el H2H previo en este estadio y viene de ganar su previa. Sin embargo, Cerrito domino el volumen de juego en ese H2H y demuestra un rendimiento relativo superior frente a rivales indirectos comunes. El valor esta totalmente del lado del local.
2. **Pick Base**: Local 0 (CS Cerrito AH 0) -> Gano 1:0 (Acierto completo)
3. **Pick Goles**: UNDER 2.0 (Historial directo de neutralizacion extrema: 1:0 y 0:1) -> Acierto completo
4. **Bloque Mandante**: volumen oculto e indirecta invertida.
