---
name: handicap-repetido-local-oculto
description: Leer partidos de handicap corto cuando el handicap del ultimo partido del visitante se repite, pero el local ya gano esa misma linea como no favorito y ademas dejo superioridad oculta de volumen en un empate o marcador corto. Usar para entrenar el skill de apuestas con feedback humano, desarrollar reglas nuevas desde casos reales y decidir entre local 0, local -0.25 o no bet.
---

# Handicap Repetido Local Oculto

## Idea central

No leer la repeticion del handicap del visitante como una señal automatica pro-visitante.

La repeticion de linea solo vale si sobrevive a tres filtros:

1. memoria de matchup real
2. contexto del rival reciente
3. volumen oculto del local

## Cuándo activar este skill

Activalo si se cumplen al menos dos de estas condiciones:

- el `AH actual` coincide exacta o casi exactamente con el `handicap_line_raw` del ultimo partido fuera del visitante
- el visitante viene de ganar o cubrir en esa linea repetida
- el local ya le habia ganado al visitante con la misma linea absoluta, pero siendo no favorito o jugando fuera
- el local viene de un `draw` o marcador corto contra el rival del visitante, pero con superioridad fuerte en tiros
- el ultimo partido del local fue una derrota ante un rival muy superior por linea (`-1.25`, `-1.5` o mas dura)

## Workflow

1. Corregir orientacion.
   - Si el `H2H general` tiene localias invertidas respecto al partido actual, reexpresa marcador y stats desde el equipo actual.
   - No valores un `0:1` como derrota si el equipo actual era visitante y gano.

2. Detectar repeticion del handicap visitante.
   - Compara `AH actual` con `last_away_match.handicap_line_raw`.
   - Si la diferencia es `<= 0.00`, marca `repeticion exacta`.
   - Si la diferencia es `<= 0.25`, marca `repeticion de bucket`.
   - La repeticion sola no decide pick.

3. Buscar memoria de matchup en la misma linea.
   - Revisa si el local actual ya gano al visitante con la misma linea absoluta.
   - Da mas valor si aquella victoria fue:
     - con el local actual jugando fuera
     - o con el local actual siendo no favorito
   - Esta memoria pesa mas que la simple repeticion del ultimo handicap del visitante.

4. Releer los empates del local.
   - Si el local empato contra el rival del visitante, no lo trates como bloque neutro por defecto.
   - Reetiqueta ese empate como `superioridad oculta` si hay al menos una de estas condiciones:
     - diferencia de tiros `>= 8`
     - diferencia de tiros a puerta `>= 5`
     - diferencia de ataques peligrosos `>= 35`
   - Si se activan dos o mas, el empate vale como bloque pro-local.

5. Descontar derrotas recientes contra rivales de otra jerarquia.
   - Si el local perdio su ultimo partido ante un rival que salia con `-1.25`, `-1.5` o mas fuerte, no lo castigues igual que una mala derrota en linea corta.
   - Ese bloque solo invalida al local si ademas fue superado de forma clara en volumen y no existe memoria positiva del matchup actual.

6. Sintesis final.
   - Si hay `repeticion exacta visitante` + `victoria previa del local en misma linea como no favorito` + `superioridad oculta local`, la lectura base no es visitante.
   - En `AH 0.25`, la traduccion operativa por defecto es:
     - `local 0` si el partido sigue cerrado
     - `local -0.25` si H2H y volumen empujan en la misma direccion
   - Si solo existe la repeticion de linea del visitante, pero no la memoria del local ni la superioridad oculta, no fuerces pick local.

## Prioridades

- Prioridad 1: H2H del mismo matchup en la misma linea absoluta
- Prioridad 2: empate del local con dominio fuerte de volumen
- Prioridad 3: repeticion del handicap del visitante
- Prioridad 4: derrota reciente del local ante rival de linea mucho mas dura

## Output contract

Entrega siempre:

1. `trigger`:
   - `repeticion_visitante`
   - `misma_linea_h2h_local`
   - `empate_con_superioridad_oculta`
   - `derrota_vs_rival_muy_superior`
2. `lectura`:
   - una frase corta explicando por que la repeticion visitante no basta o si esta vez si basta
3. `pick_base`:
   - `Local 0`
   - `Local -0.25`
   - `No favorito +0.25`
   - `NO BET`
4. `pick_agresivo` opcional
5. `bloque_mandante`:
   - `H2H misma linea`
   - `volumen oculto`
   - `repeticion visitante`
   - `castigo al local`

## Guardrails

- Nunca usar la repeticion del handicap del visitante como argumento unico.
- Si el local ya resolvio ese mismo matchup en la misma linea absoluta y como no favorito, ese dato no se minimiza.
- Un `draw` con aplastamiento de tiros no es neutro.
- No vender una derrota del local ante rival `-1.5` como si fuera una mala señal equivalente a fallar un `0` o `0.25`.

## Caso canonico

Usa como referencia:

- `references/yirga_addis_2026-03-26.md`
