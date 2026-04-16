# Caso Canonico: Yirga Chefe Bunaa (W) vs Addis Ababa Ketema (W)

## Partido

- Fecha: `2026-03-26`
- AH actual: `Yirga -0.25`
- OU actual: `2.0`
- Resultado real: `4:2`

## Señales que activan el skill

### 1. Repeticion exacta del handicap del visitante

- Ultimo partido fuera del visitante:
  - `Arba Minch (W) 0:2 Addis Ababa Ketema (W)`
  - `AH raw = 0.25`
- Partido actual:
  - `AH raw = 0.25`

Lectura:

- la linea del visitante se repite de forma exacta
- esto obliga a mirar si el mercado esta reciclando un contexto que ya fue resuelto mejor por el local actual

### 2. El local ya habia ganado ese matchup en la misma linea, pero siendo no favorito

- H2H general:
  - `Addis Ababa Ketema (W) 0:1 Yirga Chefe Bunaa (W)`
  - `AH raw historico = 0.25`

Lectura:

- el local actual ya gano a este mismo rival con la misma linea absoluta
- ademas lo hizo fuera y como no favorito
- esta memoria de matchup pesa mas que la simple repeticion del ultimo handicap del visitante

### 3. Empate del local con superioridad oculta frente al rival del visitante

- Indirecta local:
  - `Yirga 1:1 Arba Minch`
  - `AH = 0`
- Volumen:
  - tiros `14 vs 1`
  - tiros a puerta `8 vs 1`
  - ataques `80 vs 70`
  - ataques peligrosos `116 vs 62`

Lectura:

- el `1:1` no fue neutral
- fue un empate con superioridad fuerte del local
- ese bloque debe pasar de neutral a pro-local

### 4. La derrota reciente del local no era contra una linea comparable

- Prev home:
  - `Yirga 1:4 Mechal`
  - rival salia `-1.5`

Lectura:

- no era una linea de `0` o `0.25`
- el local venia de medirse a un rival muy superior por mercado
- esa derrota no puede pesar mas que:
  - la victoria H2H en misma linea contra Addis
  - el empate con volumen brutal frente a Arba Minch

## Traduccion operativa

- `pick_base`: `Yirga 0`
- `pick_agresivo`: `Yirga -0.25`
- `bloque_mandante`: `H2H misma linea + empate con superioridad oculta`

## Regla sintetica

Si el handicap del visitante se repite, pero el local ya gano al mismo rival en esa misma linea absoluta como no favorito y ademas llega de un empate con dominio fuerte de tiros frente al rival del visitante, no se compra automaticamente al visitante.

En linea `0.25`, la salida correcta es sesgo local, no sesgo visitante.
