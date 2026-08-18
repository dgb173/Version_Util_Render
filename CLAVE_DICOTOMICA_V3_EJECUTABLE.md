# ============================================================
# CLAVE DICOTOMICA MAESTRA — VERSION DEFINITIVA ENTRENADA
# Sistema de apuestas AH + O/U | Raiz = ultimo partido real
# Todo desde la vision del FAVORITO actual
# Entrenada con 5.084 partidos reales | 80 reglas validadas
# ============================================================

## CONVENCIONES DEL SISTEMA

```
F  = FAVORITO actual  (AH > 0 -> LOCAL | AH < 0 -> VISITANTE | AH = 0 -> PICKEM / sin favorito claro)
D  = DOG actual
h  = abs(AH_actual)

Residual H2H:   RH  = (goles_F_en_H2H - goles_D_en_H2H) - h
Residual F_prev: RF = (goles_F_en_previa - goles_rival_en_previa) - h
Total H2H:      TH  = goles_F_en_H2H + goles_D_en_H2H

Movimiento:     DELTA = h_actual - abs(AH_en_H2H)
  DELTA >= +0.75  -> PRESSURE_RAISE_AGGRESSIVE
  DELTA >= +0.25  -> PRESSURE_RAISE
  -0.24 a +0.24   -> PRESSURE_SAME
  DELTA <= -0.25  -> PRESSURE_LOWER
  F distinto      -> PRESSURE_NEW_FAV

Familias AH (h):
  H0       = 0        (AH = 0, PICKEM / mercado DNB separado)
  H025     = 0.25
  H05_075  = 0.5 / 0.75
  H1_125   = 1 / 1.25
  H15_175  = 1.5 / 1.75
  H2_PLUS  = 2+

Familias OU:
  OU_LOW    = <= 2.25
  OU_MID    = 2.5 / 2.75
  OU_HIGH   = 3.0 / 3.25 / 3.5
  OU_EXTREME = >= 4.0
```

---

# NODO 0 — JERARQUIA UNIVERSAL DE LECTURA

Esta jerarquia no sustituye el arbol: lo ordena. La clave no debe sumar
variables como si todas pesaran igual. Primero se busca la variable madre; solo
despues se permite que las variables inferiores confirmen, bloqueen o desempaten.

```
JERARQUIA OBLIGATORIA:

1. ULTIMO H2H GENERAL ENTRE ELLOS
   - resultado
   - handicap historico
   - favorito historico
   - movimiento hasta el handicap actual
   - cobertura del favorito actual contra la linea actual

2. ULTIMO H2H EN ESTE ESTADIO, si existe
   - mismo calculo, pero en la localia actual
   - sirve para confirmar, corregir o bloquear al H2H general

3. RESULTADO + HANDICAP DE LAS PREVIAS POR LOCALIA REAL
   - local actual: ultima previa como local
   - visitante actual: ultima previa como visitante
   - cubrir como dog NO valida automaticamente cubrir como favorito

4. COMPARATIVAS INDIRECTAS Y H2H COL3
   - margen contra rival comun
   - cobertura contra la linea de ese partido
   - empate/push como senal de techo en AH bajo

5. POSICIONES / TABLA
   - solo contradiccion o validacion secundaria
   - nunca por encima del H2H + handicap

6. ESTADISTICAS DE PROCESO
   - tiros, tiros a puerta, ataques y ataques peligrosos
   - confirman o alertan, pero no mandan por encima de resultado + handicap
```

## Regla de variable dominante

Una sola variable puede decantar el sistema, pero solo si pertenece a un nivel
alto de la jerarquia o si explica una contradiccion numerica fuerte.

```
VARIABLE DOMINANTE VALIDA:
  - H2H general con residual extremo contra la linea actual.
  - H2H estadio que repite exactamente la linea actual y ya mostro el fallo.
  - Cambio de favorito entre H2H previo y partido actual con AH bajo.
  - Rebaja/subida de handicap que cambia el problema matematico.
  - OU capado que convierte un favorito minimo en riesgo de empate.

VARIABLE NO DOMINANTE:
  - una estadistica aislada
  - una tabla mejor sin handicap que la respalde
  - una goleada previa sin saber si cubrio como favorito o como dog
  - una indirecta antigua que contradice H2H general y estadio
```

## Resultado + handicap por encima de estadisticas

El sistema debe leer primero lo que exigia la linea y si el equipo lo cumplio.
Las estadisticas solo entran despues.

```
Ejemplo:
  Equipo gana 1-0 con AH 0 -> CUBRE.
  Aunque haya tirado menos, el resultado + handicap dice que cumplio.

Ejemplo contrario:
  Equipo domina tiros pero empata con AH 0.25 -> NO CUBRE.
  El proceso no borra que no supero la barrera matematica.
```

## Formula universal replicable para cualquier handicap

El analisis no se basa en buscar una narrativa, sino en resolver una secuencia
de barreras numericas. Esta formula se aplica igual a `0.25`, `0.75`, `1.25`,
`2.25` o cualquier otra familia.

```
PASO 1. DEFINIR LA OBLIGACION ACTUAL
  h = abs(AH_actual)
  F = equipo obligado por la linea actual
  D = equipo protegido por la linea actual

  Pregunta:
    Que necesita hacer F para cubrir hoy?

PASO 2. COMPARAR EL ULTIMO H2H GENERAL CONTRA ESA OBLIGACION
  RH_general = margen_F_en_H2H_general - h

  COVER -> el H2H general valida la obligacion actual.
  PUSH  -> el H2H general dejo techo/empate de linea.
  FAIL  -> el H2H general castiga la obligacion actual.

PASO 3. LEER EL MOVIMIENTO DE HANDICAP
  No basta saber quien gano.
  Hay que saber si la casa:
    - mantiene la misma exigencia,
    - baja la exigencia,
    - sube la exigencia,
    - cambia el favorito.

PASO 4. CONTRASTAR CON H2H ESTADIO
  Si confirma al H2H general -> aumenta confianza.
  Si contradice -> no se suma; se abre bifurcacion.
  Si es antiguo -> memoria secundaria salvo repeticion exacta de patron.

PASO 5. PREVIAS POR LOCALIA REAL
  F_prev y D_prev no se leen como forma general.
  Se leen como:
    - resultado
    - handicap
    - cobertura
    - condicion de favorito/dog
    - proceso ofensivo/defensivo

PASO 6. INDIRECTAS Y COL3
  No mandan por encima del H2H salvo que expliquen una contradiccion.
  Sirven para responder:
    - el rival comun confirma el margen?
    - hay empate/push repetido?
    - hay colapso defensivo?
    - hay over oculto?

PASO 7. DECISION
  Si una variable de nivel alto domina y no tiene bloqueo fuerte -> pick.
  Si hay dos variables altas en conflicto -> NO BET salvo edge extremo.
  Si solo hay variables bajas -> NO BET.
```

## Como una sola variable puede decantar el sistema

Una variable aislada puede mandar, pero debe explicar la linea. Si no explica la
linea, solo es ruido.

```
VARIABLE DOMINANTE UNIVERSAL:

1. Residual extremo
   H2H general/estadio muestra que la linea actual es demasiado alta o baja.

2. Repeticion exacta
   Misma localia + misma familia AH + mismo tipo de resultado/cobertura.

3. Cambio de favorito
   El mercado gira contra el ganador/cubridor anterior.

4. Rebaja o subida que cambia el problema
   Un -1.25 que pasa a -0.25 no exige lo mismo.
   Un -0.25 que pasa a -1.25 tampoco.

5. Resultado peor/mejor que proceso
   Un equipo no cubre, pero produce volumen ofensivo extremo.
   Esto no debe cambiar AH solo, pero puede cambiar O/U.

6. Linea de goles contraintuitiva
   La casa mantiene/sube OU aunque los marcadores anteriores no lo superaron.
```

## Separacion universal AH/O-U

El mismo arbol puede dar una direccion para handicap y otra para goles.

```
DOG AH + OVER:
  favorito no cubre la obligacion,
  pero el partido se rompe por volumen, varianza o defensas abiertas.

FAV AH + UNDER:
  favorito cubre por control minimo,
  pero no hay ritmo para superar la linea de goles.

DOG AH + UNDER:
  favorito no rompe barrera y el mapa trae empate/push/OU bajo.

FAV AH + OVER:
  favorito cubre y la ruta de cobertura exige goles.
```

Regla practica:

```
Nunca derivar automaticamente el O/U desde el AH.
El AH responde: quien supera la barrera?
El O/U responde: cuantos goles exige el guion?
```

## Cambio de favorito por mercado en AH bajo

Especialmente en H025/H05, si el ultimo H2H general favorecia al rival pero la
linea actual cambia el favorito hacia el otro equipo, no se debe activar DOG de
forma automatica. Primero se etiqueta el caso.

```
Si H2H_GENERAL = FAIL para el favorito actual
y el favorito historico era el rival
y el mercado actual convierte al equipo actual en favorito:

  -> NEW_FAV_BY_MARKET_CONTRADICTION

Despues:
  - Si H2H_ESTADIO repite la misma linea y el favorito actual ya fallo:
      DOG / NO BET AH.

  - Si H2H_ESTADIO valida al favorito actual o el mercado invierte un favoritismo
    anterior del rival:
      el favorito actual queda vivo.

  - Si no hay estadio y OU <= 2.25:
      NO BET AH salvo confirmacion muy fuerte por previas + indirectas.

  - Si el caso combina AH 0.25 + OU bajo + Col3/indirectas de empate:
      DOG por proteccion o NO BET AH; UNDER preferente.
```

## Repeticion de fallo en estadio

Este bloqueo pesa mas que una previa reciente bonita.

```
Si H2H_ESTADIO tuvo:
  misma localia actual
  misma familia de AH actual
  favorito actual no cubrio

Entonces:
  -> STADIUM_REPEAT_FAIL
  -> bloquear FAVORITO salvo que el H2H general y las indirectas lo contradigan
     de forma limpia.
```

## AH bajo + OU bajo = riesgo estructural de empate

En AH 0.25/0.5, el empate destruye o dana al favorito. Si ademas el OU esta
capado, el sistema debe exigir mas confirmacion para comprar favorito.

```
Si h <= 0.25 y OU <= 2.25:
  draw_risk += 1.0

Si ademas:
  H2H_GENERAL = FAIL/PUSH
  o H2H_ESTADIO = PUSH/FAIL
  o Col3 = DRAW/PUSH
  o indirecta principal = DRAW/PUSH

Entonces:
  -> NO BET AH o DOG proteccion
  -> UNDER / NO BET OU
```

## Leyes especiales para AH 0.25

El AH 0.25 no se debe leer como "favorito fuerte". Es una linea de frontera:
el mercado solo pide ganar, pero el empate ya castiga. Por eso el movimiento
previo entre ellos manda mas que la tabla o una estadistica aislada.

```
H025-1. INVERSION DE FAVORITO

Si el ultimo H2H general lo gano/cubrio el rival,
pero la linea actual convierte al perdedor o no dominante en favorito 0.25:

  -> MARKET_FLIP_025
  -> NO convertir automaticamente el H2H en DOG.
  -> La pregunta correcta pasa a ser:
       "por que el mercado no mantiene al ganador anterior como favorito?"

Si ademas el H2H estadio no contradice o el favorito actual trae previa local
cubierta:
  -> FAV queda vivo.

Si ademas el H2H estadio repite fallo del favorito actual:
  -> DOG / NO BET.
```

```
H025-2. DOBLE FALLO H2H

Si H2H_GENERAL = FAIL para el favorito actual
y H2H_ESTADIO = FAIL/PUSH para el favorito actual
y no hay inversion de mercado claramente explicable:

  -> DOG cubre AH o NO BET AH.
```

```
H025-3. REBAJA DE BARRERA

Si el favorito actual ya existia como favorito en H2H previo
pero la casa baja la exigencia de 0.5/0.75 a 0.25:

  - Si habia perdido o empatado, la rebaja no es automaticamente negativa.
  - Con previa local cubierta o indirecta fuerte, puede ser compra de favorito.
  - Sin confirmacion, se bloquea por riesgo de empate.
```

```
H025-4. EMPATE REPETIDO

Si aparecen dos o mas senales de empate:
  - H2H general draw/push
  - H2H estadio draw/push
  - Col3 draw/push
  - indirecta draw/push
  - OU <= 2.25

Entonces:
  -> no comprar favorito 0.25.
  -> DOG por proteccion o NO BET AH.
  -> UNDER preferente si el total historico no obliga al over.
```

```
H025-5. ESTADIO MISMA LINEA

Si en el mismo estadio el favorito actual cubrio con la misma familia H025
y el H2H general no lo invalida de forma fuerte:

  -> FAV puede ser variable dominante.

Si en el mismo estadio el favorito actual fallo con la misma familia H025:

  -> STADIUM_REPEAT_FAIL.
  -> bloquear favorito salvo inversion de mercado + previa/indirecta fuertes.
```

```
H025-6. ESTADIO ANTIGUO

El H2H estadio sigue en la jerarquia, pero si es muy antiguo no puede mandar
solo. Se etiqueta como STADIUM_OLD.

Si H2H_ESTADIO tiene mas de 3 temporadas:
  -> usar como memoria de patron, no como prueba definitiva.

Puede ser dominante solo si:
  - coincide con H2H general reciente,
  - o coincide con previas/indirectas actuales,
  - o repite exactamente misma familia de handicap y mismo tipo de resultado.

Si contradice H2H general reciente y variables actuales:
  -> baja a confirmacion secundaria.
```

```
H025-7. REBAJA EXTREMA DESDE H1_PLUS

Si un H2H estadio antiguo cubrio una linea alta (1.0, 1.25 o mas)
pero hoy la casa baja a 0.25:

  -> no leerlo como "favorito gratis".
  -> leerlo como cambio de problema matematico.

Si el H2H general es DRAW/PUSH
o Col3 trae empate/push
o el dog viene cubriendo:
  -> bloquear favorito o exigir confirmacion nueva.

Si la previa local y la indirecta local tambien cubren:
  -> favorito puede quedar vivo, pero no por el 5-0 antiguo aislado.
```

```
H025-8. ANTIGUO FAVORITO CON VOLUMEN PERO SIN PREMIO

Si en el H2H general el rival tuvo mas tiros/volumen pero no gano o no cubrio,
y el mercado actual gira el favoritismo hacia el otro equipo:

  -> no arrastrar esas estadisticas como senal dog.
  -> el resultado + handicap dicen que el antiguo favorito fallo su obligacion.

Este caso permite FAV actual si:
  - hay MARKET_FLIP_025,
  - el favorito actual cubrio su previa local,
  - y no hay STADIUM_REPEAT_FAIL reciente.
```

```
H025-9. DOG + OVER CONTRAINTUITIVO CON OU ALTO

En AH 0.25 puede acertar el DOG y fallar el UNDER si la linea de goles esta
alta pese a que los H2H directos no la superaron. Esa linea alta no siempre es
inflacion: puede ser aviso de ruptura.

Activar OU_HIGH_COUNTERINTUITIVE si:
  - OU actual >= 3.5
  - H2H general/estadio quedan a 0.5 goles de la linea, no muy lejos
    (ejemplo: 2-1 contra OU 3.5)
  - la casa no baja la linea de goles aunque los H2H fueran UNDER

Confirmadores de OVER:
  - favorito/local viene de no cubrir, pero con volumen ofensivo muy alto
    (resultado peor que proceso: tiros/SOT claramente superiores)
  - dog/visitante viene de ganar o cubrir fuera con buen proceso
  - indirecta local muestra colapso defensivo o partido roto
  - indirecta visitante empata/cubre contra el mismo rival que gano al local
  - liga de reservas/filial/juvenil con varianza alta

Decision:
  Si OU_HIGH_COUNTERINTUITIVE + 2 confirmadores:
    -> bloquear UNDER.

  Si OU_HIGH_COUNTERINTUITIVE + 3 o mas confirmadores:
    -> OVER o NO BET OU si hay conflicto fuerte.

  Si los H2H son 0-0, 1-0 o 1-1 y no hay volumen ofensivo actual:
    -> mantener UNDER.
```

---

# PARTE A — HANDICAP ASIATICO (AH)

## Foco principal: ¿CUBRE o NO CUBRE el favorito?

---

## NODO 1 — PUERTA DE ENTRADA

```
¿Hay H2H con score real (no "?:?" ni "N/A") y linea actual disponible?
  NO -> NO BET TOTAL
  SI -> Calcular RH = (goles_F_h2h - goles_D_h2h) - h
        (Si F era visitante en el H2H, usar sus goles como visitante)
        -> NODO 2
```

---

## NODO 2 — RAIZ: ¿EL FAVORITO CUBRE EN EL ULTIMO H2H?

```
¿RH >= +0.25?
  SI  -> BASE_COVER = COVER    -> NODO 3 (RAMA FAVORITO)
  NO  ->
    ¿RH entre -0.24 y +0.24?
      SI -> BASE_COVER = PUSH  -> NODO 4 (ZONA BISAGRA)
      NO -> BASE_COVER = FAIL  -> NODO 5 (RAMA DOG)
```

---

## NODO 3 — COVER: RAMA PRINCIPAL DEL FAVORITO

*El H2H avala al favorito. Ahora verificar si la casa mantiene o sube la linea.*

```
¿DELTA (movimiento de presion) es >= +0.25?

  SI (la casa SUBE la exigencia) ->
    ¿DELTA >= +0.75? (PRESSURE_RAISE_AGGRESSIVE)
      SI ->
        ¿h >= 2.0?
          SI -> ZONA TRAMPA: [COVER + RAISE_AGGRESSIVE + H2_PLUS]
                  -> Ver NODO 10 (micro-reglas). Puede ser DOG.
          NO -> FAVORITO moderado. Confirmar con NODO 6 (previa F).
      NO (PRESSURE_RAISE normal) -> FAVORITO. Confirmar con NODO 6.

  NO (la casa BAJA o MANTIENE la exigencia) ->
    ¿Es PRESSURE_NEW_FAV (F es distinto al del H2H)?
      SI -> Favorito nuevo. Solo comprar con previa y indirecta alineadas.
            Ir a NODO 6 con umbral alto.
      NO (SAME o LOWER) -> FAVORITO con confianza normal. Ir a NODO 6.
```

**Reglas entrenadas que operan en COVER:**

| Condicion | Dir | Val | Soporte | Gap |
|---|---|---|---|---|
| COVER + OU_4_PLUS | DOG | 69.2% | 13 | 4.9 |
| COVER + DOG_RECENT_STATS_LEAN_AGAINST + OU_LOW | DOG | 68.8% | 16 | 3.9 |
| COVER + DOG_RECENT_MARGIN_M_POS1 + OU_MID | DOG | 66.7% | 18 | 0.5 |
| COVER + RAISE_AGGRESSIVE + STATS_STRONG_FOR + OVER | DOG | 66.7% | 12 | 0.8 |
| SAME + IND_FAV_COVER_COVER + OU_HIGH | FAV | 66.7% | 15 | 3.9 |
| SAME + IND_FAV_VALIDATES_AH + OU_MID | FAV | 63.2% | 19 | 0.1 |
| H05_075 + SAME + DOG_RECENT_DRAW | FAV | 65.6% | 32 | 3.1 |

> ⚠️ Si COVER + OU_4_PLUS: el sistema dice DOG 69.2% de las veces.
> Razon: el H2H pago la linea pero el OU extremo indica que la linea fue inflada.

---

## NODO 4 — PUSH: ZONA BISAGRA (RH = 0)

*El H2H quedo justo en la linea. Ni aval ni veto. Depende del contexto.*

```
¿h = 0 o 0.25?
  SI -> Riesgo de empate alto. draw_risk += 1.
        ¿OU_actual <= 2.25?
          SI -> UNDER / NO BET AH (partido que se perfila 0-0, 1-0, 1-1)
          NO -> NO BET AH. Evaluar solo OU.

  NO (h >= 0.5) ->
    ¿h >= 2.0? (PUSH en H2_PLUS)
      SI -> Push es TECHO, no fuerza. El favorito no demostro dominio.
            -> DOG o NO BET AH.
      NO (h en H05 a H15) ->
        ¿PRESSURE es RAISE o RAISE_AGGRESSIVE?
          SI -> La casa sube pese al push. Zona de trampa.
                Ver NODO 10.
          NO -> NO BET AH. Evaluar solo previa y OU.
```

**Reglas entrenadas que operan en PUSH:**

| Condicion | Dir | Val | Soporte | Gap |
|---|---|---|---|---|
| PUSH + IND_FAV_GOALS_2_MINUS + OU_LOW | DOG | 71.4% | 14 | 6.6 |
| PUSH + IND_FAV_SHORT_CURRENT_AH + OU_LOW | DOG | 69.2% | 13 | 2.2 |
| PUSH + IND_DOG_FRESH + OU_LOW | DOG | 66.7% | 21 | 3.0 |
| PUSH + IND_DOG_COVER_COVER | DOG | 66.7% | 21 | 3.9 |
| H05_075 + COVER_FAIL + IND_DOG_MARGIN_POS2 | DOG | 67.9% | 28 | 2.5 |

---

## NODO 5 — FAIL: RAMA PRINCIPAL DEL DOG

*El H2H castiga al favorito. El dog es el candidato base.*

```
¿Los stats del H2H favorecen al F? (F domina 2 de 3: SaP, AP, Tiros)
  SI -> BASE_STATS = STATS_LEAN_FOR o STATS_STRONG_FOR
        (VOLUMEN_PERDONA_RESULTADO)
    ¿h <= 0.75?
      SI -> Volumen puede rescatar favorito. Ir NODO 6 para confirmar.
      NO (h >= 1.0) ->
        ¿h >= 2.0?
          SI -> Volumen NO basta. BASE_STATS_STRONG_FOR + H2_PLUS = DOG.
                Aplicar MR-D1 y MR-D2 del NODO 10.
          NO -> DOG confirmado. Ver NODO 6 para cuantificar.

  NO -> BASE_STATS = STATS_NEUTRAL o STATS_AGAINST
        El marcador Y el proceso castigan al favorito.
        -> DOG con alta confianza. Ver NODO 10.
```

**Reglas entrenadas que operan en FAIL:**

| Condicion | Dir | Val | Soporte | Gap |
|---|---|---|---|---|
| STATS_LEAN_FOR + FAV_RECENT_MARGIN_GE_POS3 | DOG | 80.0% | 15 | 17.5 |
| AH_2+ + STATS_STRONG_FOR + OU_4+ | DOG | 78.6% | 14 | 29.0 |
| AH_2+ + STATS_STRONG_FOR + OU_EXTREME | DOG | 78.6% | 14 | 29.0 |
| RAISE_AGGRESSIVE + DOG_RECENT_GOALS_2_MINUS | DOG | 76.5% | 17 | 26.9 |
| AH_025 + STATS_STRONG_FOR + IND_DOG_STRONG_FOR | DOG | 75.0% | 28 | 25.4 |
| RAISE_AGGRESSIVE + IND_DOG_GOALS_2_MINUS | DOG | 75.0% | 16 | 25.4 |
| DOG_RECENT_STATS_NEUTRAL + TABLE_FAV_WORSE | DOG | 73.3% | 15 | 23.7 |
| AH_1_125 + NEW_FAV + IND_FAV_COVER_FAIL | DOG | 73.3% | 15 | 23.7 |
| STATS_LEAN_FOR + IND_DOG_MARGIN_NEG1 | DOG | 68.2% | 22 | 5.7 |

---

## NODO 6 — PREVIA DEL FAVORITO (F_last_match)

*Desde la localia real de F (local->last_home | visitante->last_away)*

```
RF = (goles_F_prev - goles_rival_prev) - h

¿RF >= +0.25?
  SI -> FAV_RECENT = COVER
    ¿F era FAVORITO en esa previa (AH_prev lo ponia como F)?
      SI -> FAV_RECENT_COVER_AS_FAV -> +peso favorito hoy
      NO -> FAV_RECENT_COVER_AS_DOG -> cobertura como dog, NO valida ser F hoy

  NO (RF entre -0.24 y +0.24) -> FAV_RECENT = PUSH (neutro)

  NO (RF <= -0.25) -> FAV_RECENT = FAIL
    ¿Cuanto fue el margen? (goles_F_prev - goles_rival_prev)
      >= +3 -> FAV_RECENT_MARGIN_GE_POS3 (F gano bien pese a calcular negativo)
      Empate o derrota por 1 -> FAV_RECENT_MARGIN_NEG1
      >= -3  -> FAV_RECENT_COLLAPSE
```

**Reglas entrenadas que operan en PREVIA F:**

| Condicion | Dir | Val | Soporte | Gap |
|---|---|---|---|---|
| STATS_LEAN_FOR + FAV_RECENT_MARGIN_GE_POS3 | **DOG 80%** | 15 | 17.5 | ← TRAMPA |
| AH_025 + FAV_RECENT_STATS_LEAN_FOR | DOG | 69.4% | 36 | 5.8 |
| AH_LOW + STATS_NEUTRAL + FAV_RECENT_STATS_STRONG_AGAINST | FAV | 73.1% | 26 | 10.6 |
| TOTAL_UNDER_LINE + FAV_RECENT_MARGIN_NEG1 + OU_HIGH | (OU) | ver Parte B | - | - |

> ⚠️ TRAMPA: Si F vino de ganar por +3 pero el sistema lo lee como DOG es porque
> ese margen grande viene de un partido en condicion distinta (dog o rival debil),
> lo que infla la percepcion pero no valida la linea actual.

---

## NODO 7 — PREVIA DEL DOG (D_last_match)

*Desde la localia real de D*

```
¿Cuantos goles marco el rival de D en su ultimo partido?
  >= 3 -> DOG_RECENT_COLLAPSE_3_PLUS o GOLEADA_ENCAJADA
         (La derrota puede estar DESCONTADA. No inflar al F.)

¿Cuantos goles marco D en total en su ultimo partido?
  <= 2 -> DOG_RECENT_GOALS_2_MINUS (partido cerrado del dog)
  >= 4 -> DOG_RECENT_GOALS_4_PLUS (dog con participacion en goles alta)

¿El dog cubrio en su previa?
  SI -> DOG_RECENT_COVER_COVER (dog llega cubriendo)
  NO -> DOG_RECENT_COVER_FAIL
```

**Reglas entrenadas que operan en PREVIA D:**

| Condicion | Dir | Val | Soporte | Gap |
|---|---|---|---|---|
| RAISE_AGGRESSIVE + DOG_RECENT_GOALS_2_MINUS | DOG | 76.5% | 17 | 26.9 |
| AH_025 + DOG_RECENT_COLLAPSE_3_PLUS | DOG | 66.7% | 33 | 4.4 |
| AH_025 + DOG_RECENT_MARGIN_LE_NEG3 | DOG | 66.7% | 33 | 4.4 |
| STATS_LEAN_FOR + IND_DOG_GOALS_4_PLUS + OU_MID | DOG | 69.6% | 23 | 7.3 |

---

## NODO 8 — INDIRECTAS (rival comun)

```
diff_F = margen_de_F_vs_rival - margen_de_D_vs_rival
  (positivo = F fue mejor que D con el mismo rival)

¿diff_F >= +2.0?
  SI -> IND_FAV_VALIDATES_AH -> +peso favorito
  NO ->
    ¿diff_F <= -1.5?
      SI -> IND_DOG_VALIDATES -> +peso dog

¿El favorito actual cubro su indirecta?
  SI -> IND_FAV_COVER_COVER  -> refuerza favorito
  NO -> IND_FAV_COVER_FAIL   -> activa alerta

¿Los goles del favorito en la indirecta son <= 2?
  SI -> IND_FAV_GOALS_2_MINUS -> el favorito no marco (proceso debil)
```

**Reglas entrenadas que operan en INDIRECTAS:**

| Condicion | Dir | Val | Soporte | Gap |
|---|---|---|---|---|
| SAME + IND_FAV_COVER_COVER + OU_HIGH | FAV | 66.7% | 15 | 3.9 |
| AH_025 + STATS_STRONG_FOR + IND_DOG_STRONG_FOR | DOG | 75.0% | 28 | 25.4 |
| AH_1_125 + NEW_FAV + IND_FAV_COVER_FAIL | DOG | 73.3% | 15 | 23.7 |
| RAISE + IND_FAV_VALIDATES_AH + OU_LOW | DOG | 71.4% | 14 | 7.4 |
| STATS_STRONG_FOR + IND_DOG_COVER_COVER + OU_MID | DOG | 68.4% | 19 | 1.4 |
| AH_025 + FAIL + IND_DOG_MARGIN_POS2 | DOG | 67.9% | 28 | 2.5 |
| AH_15 + IND_FAV_VALIDATES_AH | FAV | 68.8% | 16 | 6.6 |
| H05_075 + TOTAL_UNDER + IND_FAV_STATS_NEUTRAL | FAV | 70.4% | 27 | 5.5 |

---

## NODO 9 — TABLA Y CONTEXTO

```
¿Ranking del F es mejor (numero menor) que el de D?
  SI -> TABLE_FAV_BETTER (tabla acompana la linea) -> +0.3 confianza F
  NO -> TABLE_FAV_WORSE
    ¿El H2H o las indirectas explican la contradiccion?
      NO -> DOG mas probable. Activar alerta.
```

**Regla entrenada:**
| Condicion | Dir | Val | Soporte |
|---|---|---|---|
| DOG_RECENT_STATS_NEUTRAL + TABLE_FAV_WORSE | DOG | 73.3% | 15 |

---

## NODO 10 — MICRO-REGLAS ENTRENADAS (NODOS DE ACTIVACION)

*Todas las reglas con validacion >= 62% y soporte >= 12. Se activan si se cumplen sus condiciones exactas.*

### MICRO-REGLAS -> DOG (apostar al no favorito)

```
MR-D1 [80% | 15 casos | GAP 17.5]
  STATS_LEAN_FOR + FAV_RECENT_MARGIN_GE_POS3
  "El favorito gano bien su previa pero no domina en el H2H"
  -> Trampa de narrativa. score_D += 2.5

MR-D2 [78.6% | 14 casos | GAP 29.0]
  AH_2_PLUS + STATS_STRONG_FOR + OU_4_PLUS
  "Favorito muy alto + proceso a su favor + goles extremos"
  -> Inflacion de linea. score_D += 2.5

MR-D3 [78.6% | 14 casos | GAP 29.0]
  AH_2_PLUS + STATS_STRONG_FOR + OU_EXTREME
  "Equivalente a MR-D2 por familia OU"
  -> score_D += 2.5

MR-D4 [76.5% | 17 casos | GAP 26.9]
  RAISE_AGGRESSIVE + DOG_RECENT_GOALS_2_MINUS
  "La casa sube agresivo y el dog viene de partido cerrado"
  -> score_D += 2.0

MR-D5 [75.0% | 28 casos | GAP 25.4]
  AH_025 + STATS_STRONG_FOR + IND_DOG_STATS_STRONG_FOR
  "Linea muy baja + F domina proceso + D tambien domina su indirecta"
  -> score_D += 2.0

MR-D6 [75.0% | 16 casos | GAP 25.4]
  RAISE_AGGRESSIVE + IND_DOG_GOALS_2_MINUS
  "Subida agresiva de linea + dog no marco en su indirecta (proceso defensivo)"
  -> score_D += 2.0

MR-D7 [73.3% | 15 casos | GAP 23.7]
  DOG_RECENT_STATS_NEUTRAL + TABLE_FAV_WORSE
  "Dog llego con proceso neutro pero la tabla dice que es mejor"
  -> score_D += 1.8

MR-D8 [73.3% | 15 casos | GAP 23.7]
  AH_1_125 + NEW_FAV + IND_FAV_COVER_FAIL
  "Nuevo favorito en linea bisagra y su indirecta fallo"
  -> score_D += 1.8

MR-D9 [73.3% | 15 casos | GAP 23.7]
  STATS_STRONG_FOR + OU_EXTREME
  "F domina proceso en el H2H pero el OU es extremo (partido roto)"
  -> score_D += 1.5

MR-D10 [71.4% | 14 casos | GAP 6.6]
  PUSH + IND_FAV_GOALS_2_MINUS + OU_LOW
  "H2H en push + F no marco en indirecta + OU bajo"
  -> score_D += 1.5 (regla estable, gap bajo)

MR-D11 [71.4% | 14 casos | GAP 7.4]
  RAISE + IND_FAV_VALIDATES_AH + OU_LOW
  "La casa sube la linea y el OU dice partido cerrado"
  -> Linea sube pero el ritmo de goles no acompana. score_D += 1.5

MR-D12 [70.4% => invertida]
  [Ver FAV Nodo 3 regla contraria]

MR-D13 [69.6% | 23 casos | GAP 7.3]
  STATS_STRONG_FOR + IND_DOG_GOALS_4_PLUS + OU_MID
  "F domina proceso + D marco muchos en su indirecta + OU medio"
  -> score_D += 1.3

MR-D14 [69.4% | 36 casos | GAP 5.8]
  AH_025 + FAV_RECENT_STATS_LEAN_FOR
  "Linea minima + stats de la previa de F leves a su favor"
  -> score_D += 1.3

MR-D15 [69.2% | 13 casos | GAP 2.0]
  TOTAL_OVER_LINE + OU_4_PLUS
  "El H2H fue OVER y el OU actual es extremo"
  -> La casa no baja el OU: posible sobreexpectativa. score_D += 1.3

MR-D16 [69.2% | 13 casos | GAP 4.9]
  BASE_COVER + OU_4_PLUS
  "H2H COVER + OU extremo = trampa de linea alta con OU extremo"
  -> score_D += 1.3

MR-D17 [69.2% | 13 casos | GAP 6.4]
  NEW_FAV + STATS_LEAN_FOR + TOTAL_UNDER_LINE
  "Nuevo favorito + leve dominio + H2H fue under"
  -> score_D += 1.3

MR-D18 [68.8% | 16 casos | GAP 3.9]
  COVER + DOG_RECENT_STATS_LEAN_AGAINST + OU_LOW
  "H2H COVER pero dog llega mejor de lo que parece y OU es bajo"
  -> score_D += 1.2

MR-D19 [67.9% | 28 casos | GAP 2.5]
  AH_LOW + FAIL + IND_DOG_MARGIN_POS2
  "Linea baja + H2H FAIL + dog marco bien en su indirecta (+2)"
  -> score_D += 1.2

MR-D20 [66.7% | 33 casos | GAP 4.4]
  AH_025 + DOG_RECENT_COLLAPSE_3_PLUS
  "Linea minima + D llego con goleada encajada (puede estar descontada)"
  -> La goleada ya esta en el precio. score_D += 1.0

MR-D21 [66.7% | 12 casos | GAP 0.5]
  RAISE_AGGRESSIVE + STATS_STRONG_FOR + TOTAL_OVER_LINE
  "Subida agresiva + F domina + H2H fue OVER"
  -> Patron de trampa acumulada. score_D += 1.0 (GAP bajo = regla estable)

MR-D22 [66.7% | 12 casos | GAP 4.3]
  H2H_STADIUM_COVER_PUSH
  "El H2H en ese estadio quedo en PUSH (no COVER, no FAIL)"
  -> score_D += 1.0

MR-D23 [65.7% | 35 casos | GAP 3.4]
  AH_025 + COVER + IND_FAV_MISSING
  "Linea baja + H2H COVER + sin indirecta del favorito"
  -> Datos incompletos en partido que parece facil. score_D += 0.8

MR-D24 [65.0% | 20 casos | GAP 2.7]
  TOTAL_UNDER_LINE + FAV_RECENT_MARGIN_GE_POS3 + OU_MID
  "H2H fue under + F gano bien su previa + OU medio"
  -> score_D += 1.0
```

### MICRO-REGLAS -> FAVORITO (apostar al favorito)

```
MR-F1 [75.0% | 20 casos | GAP 24.6]
  AH_LOW + TOTAL_OVER_LINE + IND_FAV_COVER_PUSH
  "Linea baja + H2H fue OVER + indirecta del F en push (ni bien ni mal)"
  -> El F no necesita demostrar demolicion. score_F += 2.0

MR-F2 [73.1% | 26 casos | GAP 10.6]
  AH_LOW + STATS_NEUTRAL + FAV_RECENT_STATS_STRONG_AGAINST
  "Linea baja + H2H neutral + el rival de F en su previa fue muy fuerte"
  -> F aguanto presion alta. Hoy tiene rival mas facil. score_F += 1.8

MR-F3 [70.4% | 27 casos | GAP 5.5]
  H05_075 + TOTAL_UNDER_LINE + IND_FAV_STATS_NEUTRAL
  "Favorito ganar + H2H fue under + indirecta de F es neutra"
  -> Patron de control limpio. score_F += 1.5

MR-F4 [68.8% | 16 casos | GAP 6.6]
  AH_15 + IND_FAV_VALIDATES_AH
  "Favorito con separacion real (1.5) y su indirecta valida ese margen"
  -> score_F += 1.5

MR-F5 [67.9% | 28 casos | GAP 5.7]
  AH_075 + IND_DOG_MARGIN_DRAW
  "El dog empato en su indirecta (no llega dominante)"
  -> score_F += 1.3

MR-F6 [66.7% | 15 casos | GAP 3.9]
  SAME + IND_FAV_COVER_COVER + OU_HIGH
  "La casa mantiene la linea + F cubrió su indirecta + OU alto"
  -> Patron de continuidad real. score_F += 1.2

MR-F7 [66.7% | 12 casos | GAP 1.3]
  AH_15_175 + STATS_STRONG_FOR + IND_FAV_MARGIN_GE_POS3
  "Favorito con separacion alta + domina proceso + indirecta +3"
  -> score_F += 1.2 (GAP bajo: regla muy estable)

MR-F8 [66.7% | 12 casos | GAP 3.5]
  RAISE + IND_DOG_COVER_PUSH
  "La casa sube la linea y el dog solo hizo PUSH en su indirecta"
  -> score_F += 1.0

MR-F9 [65.6% | 32 casos | GAP 3.1]
  H05_075 + SAME + DOG_RECENT_DRAW
  "Favorito con ganar obligado + casa no cambia + dog llega con empate"
  -> score_F += 1.0

MR-F10 [64.7% | 34 casos | GAP 2.6]
  AH_075 + IND_FAV_MARGIN_DRAW
  "El favorito de 0.75 empato en su indirecta (llega sin demolicion pero competente)"
  -> score_F += 0.8

MR-F11 [64.0% | 25 casos | GAP 0.1]
  LOWER + IND_FAV_SHORT_CURRENT_AH + OU_LOW
  "La casa baja la linea + F tenia linea parecida en su indirecta + OU bajo"
  -> score_F += 1.2 (GAP casi 0: regla muy estable)

MR-F12 [63.2% | 19 casos | GAP 0.1]
  SAME + IND_FAV_VALIDATES_AH + OU_MID
  "Casa mantiene + indirecta de F valida el margen + OU medio"
  -> score_F += 0.8 (muy estable, poca potencia pero sin ruido)
```

---

## NODO 11 — CALCULO DEL EDGE AH Y DECISION

```
draw_risk = 0
  Si h <= 0.25:            draw_risk += 0.5
  Si TH <= 1 (H2H cerrado): draw_risk += 0.5
  Si OU <= 2.25:           draw_risk += 0.5
  Si LOWER + OU <= 2.25:   draw_risk += 0.3

edge_AH = score_F - score_D - draw_risk

DECISION:
  Si >= 1 MR activa alineada Y sin conflicto:
    >= +1.15 -> FAVORITO cubre AH
    <= -1.15 -> NO FAVORITO: dog cubre, favorito NO supera

  Si 0 MR activas:
    >= +2.10 -> FAVORITO cubre AH
    <= -2.10 -> NO FAVORITO: dog cubre

  Si MRs en conflicto (una pro-F, otra pro-D):
    >= +3.00 -> FAVORITO cubre AH
    <= -3.00 -> NO FAVORITO: dog cubre

  ELSE: NO BET AH
```

---

# PARTE B — OVER / UNDER (goles)

## Foco: ¿supera o no supera la linea de goles? Siempre desde el ultimo partido.

---

## NODO 12 — RAIZ OU: TOTAL DEL H2H vs LINEA ACTUAL

```
TH_residual = TH - OU_actual

¿TH_residual >= +0.25?
  SI -> H2H_OU = OVER (el H2H habria sido OVER hoy)

¿TH_residual entre -0.24 y +0.24?
  -> H2H_OU = PUSH (el H2H quedo justo en la linea)

¿TH_residual <= -0.25?
  -> H2H_OU = UNDER (el H2H habria sido UNDER hoy)
```

---

## NODO 13 — PREVIA F vs LINEA OU

```
total_F_prev = goles_F_prev + goles_rival_F_prev

¿total_F_prev - OU_actual >= +0.25?
  -> F_PREV_OU = OVER

¿total_F_prev - OU_actual <= -0.25?
  -> F_PREV_OU = UNDER

ELSE: F_PREV_OU = PUSH
```

---

## NODO 14 — PREVIA D vs LINEA OU

```
total_D_prev = goles_D_prev + goles_rival_D_prev

¿total_D_prev - OU_actual >= +0.25?
  -> D_PREV_OU = OVER

¿total_D_prev - OU_actual <= -0.25?
  -> D_PREV_OU = UNDER

ELSE: D_PREV_OU = PUSH
```

---

## NODO 15 — MICRO-REGLAS OU (entrenadas desde el sistema)

*Integradas desde el reaudit_62 y el sistema binario H2H*

### MICRO-REGLAS -> UNDER (la linea NO se supera)

```
MR-OU1 [85.7% | 14 casos] ← MAS FUERTE DEL SISTEMA
  STATS_LEAN_FOR + IND_FAV_STATS_STRONG_FOR + OU_MID
  "F leve en H2H + su indirecta fue muy controlada + OU medio"
  -> score_UNDER += 3.0

MR-OU2 [84.6% | 13 casos]
  AH_025 + H2H_TOTAL_UNDER_LINE + IND_DOG_MARGIN_POS1
  "Linea baja + H2H fue under + dog gano por 1 en su indirecta"
  -> score_UNDER += 2.8

MR-OU3 [82.1% | 28 casos]
  AH_LOW + NEW_FAV + DOG_RECENT_STATS_LEAN_AGAINST
  "Nuevo favorito en linea baja + dog llego siendo dominado en su previa"
  -> score_UNDER += 2.5

MR-OU4 [76.7% | 30 casos]
  FAIL + FAV_RECENT_MARGIN_NEG1 + OU_HIGH
  "H2H FAIL + F llego perdiendo por 1 en su previa + OU alto"
  -> score_UNDER += 2.0

MR-OU5 [75.0% | 28 casos]
  FAIL + IND_DOG_STATS_STRONG_FOR + OU_HIGH
  "H2H FAIL + D domino su indirecta + OU alto"
  -> score_UNDER += 2.0

MR-OU6 [74.2% | 31 casos]
  NEW_FAV + DOG_RECENT_STATS_LEAN_AGAINST
  "Nuevo favorito + dog llego siendo dominado"
  -> score_UNDER += 2.0

MR-OU7 [72.7% | 33 casos]
  NEW_FAV + STATS_STRONG_FOR + OU_LOW
  "Nuevo favorito + F domina proceso + OU ya es bajo"
  -> score_UNDER += 1.8

MR-OU8 [72.4% | 29 casos]
  STATS_STRONG_FOR + H2H_TOTAL_UNDER_LINE + OU_HIGH
  "F domina stats + H2H fue under + el OU es alto"
  -> La casa puso OU alto sin que el H2H ni las stats lo respalden. score_UNDER += 1.8

MR-OU9 [71.4% | 15 casos]
  IND_DOG_MARGIN_POS2 + OU_LOW
  "D gano por +2 en su indirecta + OU ya es bajo"
  -> score_UNDER += 1.5

MR-OU10 [70.6% | 17 casos]
  LOWER + TOTAL_OVER_LINE + OU_HIGH
  "La casa baja la linea de handicap + H2H fue over + OU alto"
  -> La casa baja presion pero mantiene OU: inconsistencia. score_UNDER += 1.5
```

### MICRO-REGLAS -> OVER (la linea SE SUPERA)

```
MR-OV1 [70.0% | 20 casos]
  AH_025 + STATS_STRONG_AGAINST + FAV_RECENT_GOALS_4_PLUS
  "Linea baja + el rival domino las stats H2H + F marco 4+ en su previa"
  -> Partido abierto, ambos marcan. score_OVER += 1.8

MR-OV2 [65.4% | 26 casos]
  STATS_NEUTRAL + H2H_GENERAL_FAIL + OU_MID
  "Stats H2H neutras + el H2H fue FAIL + OU medio"
  -> score_OVER += 1.2
```

---

## NODO 16 — BLOQUEO OU_CAPADO Y EDGE OU

```
Si OU_actual <= 2.25 (OU_CAPADO):
  score_UNDER += 1.0
  Si score_OVER < 3.0: BLOQUEO_OVER = true

edge_OU = score_OVER - score_UNDER

DECISION:
  Si BLOQUEO_OVER y score_OVER < 3.0: -> UNDER o NO BET OU

  Si >= 1 MR_OU activa alineada:
    >= +1.15 -> OVER (supera la linea)
    <= -1.15 -> UNDER (no supera la linea)

  Sin MR activa:
    >= +2.10 -> OVER
    <= -2.10 -> UNDER

  ELSE: NO BET OU
```

---

# PARTE C — CUADRANTE FINAL: LAS 8 SALIDAS POSIBLES

```
Q1: FAVORITO + OVER
    "El F cubre Y el partido es goleador"
    Accion: apostar F AH + apostar OVER
    Guion: dominio con produccion ofensiva

Q2: FAVORITO + UNDER
    "El F cubre Y el partido es cerrado"
    Accion: apostar F AH + LAY OVER (o apostar UNDER)
    Guion: F controla sin abrir el marcador

Q3: NO FAVORITO + OVER
    "El D cubre Y el partido produce goles"
    Accion: apostar D AH + apostar OVER
    Guion: F inflado, el partido se abre aunque el D no gane

Q4: NO FAVORITO + UNDER
    "El D cubre Y el partido es corto"
    Accion: apostar D AH + LAY OVER
    Guion: F sobrevalorado, partido de resistencia, pocos goles

Q5: FAVORITO SOLO
    AH al favorito sin pick en goles

Q6: NO FAVORITO SOLO
    AH al dog sin pick en goles
    "El favorito NO supera la linea de goles, solo el dog resiste"

Q7: OVER SOLO
    Solo pick de goles (el marcador sera alto)

Q8: NO BET TOTAL
    Ninguna señal es suficientemente clara
```

---

# PARTE D — MOVIMIENTOS DE CUOTAS: PATRON UNIVERSAL

*El foco del sistema es leer LO QUE EXIGE LA CASA hoy vs lo que dijo el historico.*

```
PATRON 1: LINEA SUBE + H2H FAIL = TRAMPA
  La casa sube la exigencia aunque el H2H castiga al F.
  Esto casi siempre es una trampa de mercado -> DOG.
  Reglas: MR-D4, MR-D6, MR-D21

PATRON 2: LINEA SUBE + H2H COVER + OU EXTREMO = INFLACION
  La casa sube mas la linea sobre un H2H que ya validaba al F.
  El OU extremo dice que la casa anticipa partido abierto.
  Esto es inflacion de linea, no mayor autoridad -> DOG.
  Reglas: MR-D2, MR-D3, MR-D16

PATRON 3: LINEA BAJA + IND_FAV_OK + OU_LOW = CONTROL
  La casa baja la linea y el OU es bajo.
  Guion de control limpio del F. El sistema no persigue goles.
  -> FAVORITO + UNDER.
  Reglas: MR-F11, MR-F3

PATRON 4: NUEVO FAVORITO + IND_FAV_FAIL = TRAMPA DE ROL
  La casa invierte la jerarquia pero la indirecta del F falla.
  El mercado esta asignando un rol que los datos no validan.
  -> DOG. Reglas: MR-D8, MR-D17

PATRON 5: MISMO FAVORITO + MISMO NIVEL + IND_FAV_OK = CONTINUIDAD
  La casa no cambia. La indirecta valida. El F llega bien.
  -> FAVORITO. Reglas: MR-F6, MR-F12

PATRON 6: LINEA ALTA (H2_PLUS) + OU EXTREMO = PARTIDO ROTO
  Cuando la casa pone H2+ y OU4+ a la vez, el mercado proyecta
  un partido abierto con el favorito muy superior.
  PERO: el H2H raramente valida ambas cosas -> DOG posible.
  Reglas: MR-D2, MR-D3

PATRON 7: NUEVO FAVORITO + DOG DOMINADO EN PREVIA = UNDER ESTRUCTURAL
  La casa crea nuevo favorito y el dog llego siendo aplastado.
  El partido se perfila corto con el F controlando.
  -> UNDER. Reglas: MR-OU3, MR-OU6, MR-OU7

PATRON 8: COVER + OU_4_PLUS = FAVORITO PERO OVER TRAMPA
  El H2H valida al F PERO el OU extremo muestra que la linea
  fue inflada para compensar el OU alto.
  -> FAVORITO puede cubrir pero el OU es inflado. Separar el pick.
  Reglas: MR-D16 (AH) + MR-OU1 (OU)

PATRON 9: FAVORITO MANTENIDO TRAS H2H FAIL DE PROCESO IGUALADO
  El F perdio el H2H y el rival tiene relato publico de X2,
  pero la casa mantiene el mismo handicap al F.
  Si el H2H fue de proceso igualado, la derrota fue varianza,
  no superioridad estructural del dog.
  -> FAVORITO + OVER si el H2H fue abierto.
  Regla: U19 MARKET_REJECTS_OBVIOUS_DOG_X2

PATRON 10: REBAJA BRUTAL DESDE H1_PLUS/H2_PLUS = DOG + UNDER
  El H2H antiguo fue una goleada del favorito con AH enorme
  (ej. 3.00) y el marcador bruto superaria tambien la linea actual.
  Pero la casa baja mucho el handicap actual (ej. 3.00 -> 1.25).
  Esa rebaja no confirma repeticion de goleada: protege al dog.
  Si ademas el OU queda extremo (3.75/4.0) por memoria de ese 4-1
  y el Col3/indirectas no sostienen ruptura, el over queda inflado.
  -> DOG AH + UNDER/NO BET OVER.
  Regla: U20 HUGE_DROP_PROTECTS_DOG + OU_INFLADO_POR_GOLEADA_ANTIGUA

PATRON 11: H2H OVER ANTIGUO + OU CAPADO + DOBLE PUSH RECIENTE
  El H2H fue empate abierto (ej. 3-3), pero con la linea actual
  el favorito NO habria cubierto. La casa pide AH alto (0.75/1.00)
  y, aun asi, deja el OU en 2.75 o menos.
  Si las dos previas recientes no separan ganador (empate/push),
  el H2H de 6 goles no se persigue como over: queda capado.
  -> DOG AH + UNDER.
  Regla: U21 H2H_OVER_ANTIGUO_OU_CAPADO + DOBLE_PUSH_RECIENTE

PATRON 12: AH0 TRAS DOG QUE GANO H2H RECIENTE = DNB LOCAL + UNDER
  AH=0 no siempre es neutral. Si el local actual gano el ultimo H2H
  general siendo dog/rol inferior y ahora vuelve a casa en pick'em,
  el mercado ya no lo esta castigando: lo valida como DNB.
  Si el OU es alto por goleadas sufridas externas, pero el H2H reciente
  fue 0-1/1-0 y ambos llegan con ataque cero ante rivales fuertes,
  se bloquea el over por memoria de goleadas.
  -> LOCAL DNB + UNDER.
  Regla: U22 PICKEM_DOG_WIN_TO_HOME_DNB + OU_ALTO_INFLADO_POR_GOLEADAS_AJENAS
```

---

# PARTE E — TABLA DE CONFLICTOS Y PRIORIDADES

| Conflicto | Regla de prioridad |
|---|---|
| H2H FAIL vs VOLUMEN F domina | Si h >= 1.25: FAIL > volumen. Si h <= 0.75: volumen puede rescatar |
| COVER + OU_4_PLUS vs FAVORITO | OU extremo prevalece. Separar AH y OU. |
| RAISE_AGGRESSIVE vs H2H COVER | Si h >= 2.0: zona trampa. Evaluar solo con MR confirmada. |
| OU_CAPADO vs H2H OVER | OU_CAPADO bloquea OVER salvo score_OVER >= 3.0 |
| NEW_FAV + IND_FAV_FAIL vs COVER | COVER es del H2H, FAV del futuro. IND_FAIL prevalece si h >= 1.0 |
| GOLEADA_D_ENCAJADA vs AH alto | Goleada descontada. No inflar F sin que F valide al mismo rival. |
| MR_F vs MR_D simultaneas | Activar umbral conflicto 3.00 |
| TABLE_FAV_WORSE vs STATS_STRONG_FOR | Si DOG_RECENT_NEUTRAL: DOG 73.3%. La tabla pesa mas que el proceso. |

---

# CHECKLIST FINAL (10 pasos antes de emitir el pick)

```
[1] ¿Calcule RH contra la linea ACTUAL (no contra el marcador bruto)?
[2] ¿Si F era visitante en el H2H, use sus goles como visitante?
[3] ¿Verifique que h2h_stadium y h2h_general no son el mismo partido?
[4] ¿Determine si F cubrio su previa como FAVORITO o como DOG?
[5] ¿Verifique GOLEADA_D_ENCAJADA y si puede estar descontada?
[6] ¿Comprobe INFLACION_RIVAL_COMUN (D fue goleado, F no separo al mismo rival)?
[7] ¿Aplique el BLOQUEO_OVER si OU <= 2.25 y score_OVER < 3.0?
[8] ¿Pase por TODAS las micro-reglas de los NODOS 10 y 15?
[9] ¿Hay CONFLICTO de micro-reglas? Si si, el umbral es 3.00.
[10] ¿El pick es consistente: si el AH dice DOG, el OU puede decir OVER (Q3)?
     "NO FAVORITO: el favorito NO supera la exigencia de la linea"
     "NO SUPERA: la linea de goles no se alcanza"
```
