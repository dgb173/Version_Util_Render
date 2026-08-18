# Revision de la clave dicotomica de Gemini

Fecha de revision: 2026-06-30

## Veredicto corto

La idea base de la clave es buena: partir del ultimo H2H, medir el residual contra el AH actual, separar AH de O/U y bloquear con `NO BET` cuando el arbol no cierra.

Pero no la usaria aun como sistema definitivo. La parte documental dice "80 reglas validadas", pero con filtros mas conservadores el sistema no mantiene reglas AH robustas. La prioridad no es anadir mas reglas, sino convertir la clave en un ciclo reproducible de entrenamiento, validacion, auditoria de fallos y promocion de reglas.

## Lo que esta bien

- La raiz matematica es correcta: `residual = margen_favorito_en_H2H - abs(AH_actual)`.
- La separacion entre AH y O/U esta bien planteada. Un partido puede favorecer al dog en handicap y seguir siendo over.
- El documento ya recoge fallos reales utiles: residual mal normalizado, doble conteo, confundir cubrir como dog con validar favorito, comprar over en O/U bajo y omitir over correlacionado con favorito visitante.
- El sistema tiene una buena idea defensiva: si no hay micro-regla fuerte o el edge no supera umbral, salida `NO BET`.

## Problemas importantes

### 1. "80 reglas validadas" esta inflado

En `CLAVE_DICOTOMICA_V3_EJECUTABLE.md` se declara:

```text
Entrenada con 5.084 partidos reales | 80 reglas validadas
```

Pero las salidas existentes muestran otra cosa:

- `data/sistema_binario_real_v2.json`: 0 reglas AH y 2 reglas O/U.
- `data/sistema_binario_real_v2_65.json`: 8 reglas AH y 10 reglas O/U.
- `data/sistema_binario_real_v2_diag62.json`: 80 reglas AH y 64 O/U, pero con filtro laxo: validacion >= 62%, soporte bajo y gaps altos.

Con una pasada media que ejecute:

```powershell
python scripts\explorador_automejora\train_binary_market_system_v2.py --project-root . --output-json scratch\revision_clave_dicotomica\medium_market_rules.json --output-html scratch\revision_clave_dicotomica\medium_market_rules.html --output-md scratch\revision_clave_dicotomica\medium_market_rules.md --validation-ratio 0.25 --min-quality 5 --min-train-support 45 --min-validation-support 16 --min-hit 65 --min-lift 3 --max-gap 12 --max-combo 2 --max-rules 50
```

resultado:

```text
usable=5080
train=3810
validation=1270
side_rules=0
goal_rules=3
```

Esto significa que AH todavia no esta suficientemente estable con criterios medios.

### 2. AH = 0 esta mal tratado

El documento fuerza:

```text
AH = 0 -> FAVORITO = visitante
```

Eso es peligroso. En entrenamiento, `AH=0` es `PICKEM` y se rechaza como caso sin favorito claro. La solucion correcta:

```text
AH = 0 -> NO BET AH, o mercado separado tipo DNB/pick'em.
```

No debe mezclarse con favorito/no favorito normal.

### 3. El motor no implementa la clave completa

`data/motor_clave_dicotomica.py` implementa solo un subconjunto de las reglas del Markdown. Ademas:

- El documento habla de V3, el motor dice V4.
- El motor se ejecuta directamente al importarse porque no tiene guardia `if __name__ == "__main__"`.
- El motor carga `data_precacheo.json` desde el directorio actual, lo que puede fallar si se ejecuta desde la raiz del proyecto.
- Algunas reglas documentadas no coinciden con el codigo.

Ejemplo importante:

```text
Documento MR-OU1:
STATS_LEAN_FOR + IND_FAV_STATS_STRONG_FOR + OU_MID

Motor:
base_stats == STATS_LEAN_FOR and ind_fav_neutral and OU_MID
```

Eso no es la misma regla.

### 4. Hay reglas muertas por presion `NEW_FAV`

El motor calcula la presion solo con:

```text
delta = h_actual - abs(h2h_ah)
```

Pero no compara si el favorito historico era el mismo que el favorito actual. Por eso `PRESSURE_NEW_FAV` practicamente no se genera en el motor, aunque hay reglas que dependen de esa etiqueta:

- `MR-D8`
- `MR-OU3`
- `MR-OU7`

Estas reglas pueden quedar muertas aunque aparezcan en la clave.

### 5. Hay duplicados y doble conteo

Ejemplo:

```text
MR-D2: AH_2_PLUS + STATS_STRONG_FOR + OU_4_PLUS
MR-D3: AH_2_PLUS + STATS_STRONG_FOR + OU_EXTREME
```

Como `OU_EXTREME` es `>= 4.0`, estas reglas se pisan casi por completo. Si ambas suman score, inflan artificialmente al dog.

## Reglas que si sobreviven en pasada media

Con parametros medios, solo sobreviven tres reglas O/U, todas hacia UNDER:

```text
UNDER | val 15/20 (75.0%) | train 33/50 (66.0%)
IND_DOG_MARGIN_M_POS2 + OU_FAMILY=OU_LOW

UNDER | val 23/31 (74.19%) | train 46/70 (65.71%)
BASE_PRESSURE=PRESSURE_NEW_FAV + DOG_RECENT_STATS_LEAN_AGAINST

UNDER | val 24/34 (70.59%) | train 37/54 (68.52%)
IND_DOG_MARGIN_M_POS2 + TABLE=TABLE_UNKNOWN
```

Estas pueden quedar como candidatas, no como dogma. Necesitan validacion futura.

## Como entrenarla rapido y bien

### Paso 1. Congelar un protocolo

No cambiar reglas mirando un partido suelto. Cada regla debe tener:

- `train_wins/train_bets`
- `validation_wins/validation_bets`
- `hit_rate_validation`
- `lift_vs_base`
- `gap_train_validation`
- ejemplos de aciertos
- ejemplos de fallos

### Paso 2. Usar tres niveles de reglas

```text
PRODUCCION:
validacion >= 68%
soporte_validacion >= 25
soporte_train >= 60
gap <= 10
lift >= 5

OBSERVACION:
validacion >= 65%
soporte_validacion >= 16
soporte_train >= 45
gap <= 12
lift >= 3

LABORATORIO:
validacion >= 62%
soporte_validacion >= 12
soporte_train >= 35
gap <= 18
lift >= 0
```

Solo `PRODUCCION` puede mover un pick real. `OBSERVACION` solo baja/sube confianza. `LABORATORIO` no debe apostar: sirve para investigar.

### Paso 3. Separar mercados

Entrenar cuatro salidas por separado:

```text
AH_FAVORITO_CUBRE
AH_DOG_CUBRE
OU_OVER
OU_UNDER
```

No mezclar una regla AH con una conclusion O/U salvo que haya una regla correlacionada validada.

### Paso 4. Tratar `NO BET` como clase real

Ahora el sistema mide sobre favorito/dog/over/under, pero debe medir tambien:

```text
Cuando el sistema dice NO BET, cuantos de esos partidos eran ruido?
```

Una buena clave no es la que da mas picks, sino la que evita los mapas sin cierre.

### Paso 5. Validacion temporal, no aleatoria

El split actual es temporal por orden de fecha. Eso esta bien. Mantenerlo:

```text
train = partidos antiguos
validation = partidos recientes
```

Luego anadir `walk-forward`:

```text
entrenar hasta semana N
validar semana N+1
guardar aciertos/fallos
avanzar una semana
```

Esto evita que una regla parezca buena por casualidad historica.

### Paso 6. Corregir el motor antes de meter mas reglas

Orden recomendado:

1. Convertir `AH=0` en `NO BET AH`.
2. Detectar bien `PRESSURE_NEW_FAV` comparando favorito historico contra favorito actual.
3. Meter guardia `if __name__ == "__main__"` en `data/motor_clave_dicotomica.py`.
4. Cargar reglas desde JSON entrenado, no copiarlas a mano en el motor.
5. Deduplicar reglas equivalentes antes de sumar score.
6. Bloquear reglas con gap alto o soporte bajo.

## Decision practica

Uso recomendado ahora:

```text
AH:
usar como mapa de lectura, no como motor automatico.

O/U:
solo considerar UNDER si activa una de las 3 reglas supervivientes o una regla nueva validada.

Picks:
exigir micro-regla de produccion + no conflicto + edge suficiente.

Sin eso:
NO BET.
```

