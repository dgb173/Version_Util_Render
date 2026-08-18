# Mapa dicotomico completo AH/O-U

Sistema para leer partidos desde el ultimo enfrentamiento directo como raiz del
problema matematico.

La salida no es "apostar siempre". La salida correcta puede ser:

- `FAVORITO AH`
- `NO FAVORITO AH`
- `OVER`
- `UNDER`
- `NO BET`

La forma de no fallar mas no es forzar mas picks, sino bloquear todo caso donde
el arbol no cierre. El sistema debe preferir perder una oportunidad antes que
convertir ruido en pronostico.

---

## 0. Principio matematico

La variable madre es el handicap actual.

No se pregunta primero:

- quien es mejor
- quien gano el ultimo partido
- quien va arriba en la tabla

Se pregunta:

> Que exige hoy la casa y que dijo el ultimo H2H sobre esa exigencia?

La raiz siempre es:

```text
ULTIMO H2H + AH del H2H + resultado del H2H + cobertura del AH actual
```

Despues se cruzan:

- movimiento de handicap
- familia de AH
- O/U actual
- total del H2H contra O/U actual
- previa local
- previa visitante
- indirecta local contra rival comun
- indirecta visitante contra rival comun
- H2H Col3
- estadisticas de proceso como confirmacion, no como motor
- tabla/clasificacion solo como contradiccion o validacion secundaria

---

## 1. Variables normalizadas

### 1A. Equipos

```text
H = local actual
A = visitante actual
F = favorito actual segun AH
D = no favorito actual
```

Si `AH actual > 0`:

```text
F = local
D = visitante
```

Si `AH actual < 0`:

```text
F = visitante
D = local
```

Si `AH actual = 0`:

```text
zona pick'em / DNB
```

### 1B. Handicap actual

```text
h = abs(AH actual)
```

Familias:

```text
H0      = 0
H025    = 0.25
H05_075 = 0.5 / 0.75
H1_125  = 1 / 1.25
H15_175 = 1.5 / 1.75
H2PLUS  = 2 / 2.25 / 2.5 / 3+
```

Interpretacion:

```text
0       = empate devuelve
0.25    = empate medio castigo / media proteccion
0.5     = favorito necesita ganar
0.75    = un gol medio gana / medio push
1       = un gol push
1.25    = un gol medio pierde
1.5     = necesita dos
1.75    = dos medio gana
2       = dos push
2.25+   = necesita goleada real
```

### 1C. H2H desde el favorito actual

Para cada H2H:

```text
margin_F = goles_F - goles_D
residual_actual = margin_F - h
cover_actual =
  COVER si residual_actual >= 0.25
  PUSH  si -0.25 < residual_actual < 0.25
  FAIL  si residual_actual <= -0.25
```

Esto es la raiz.

Ejemplo:

```text
H2H: favorito gana 3:2
AH actual: -0.25  -> residual = 1 - 0.25 = +0.75 => COVER
AH actual: -2.25  -> residual = 1 - 2.25 = -1.25 => FAIL
```

Mismo marcador, lectura opuesta. Esta es la idea central.

### 1D. Movimiento de presion

Desde el favorito actual:

```text
pressure_then = exigencia del favorito actual en el H2H anterior
pressure_now  = abs(AH actual)
delta = pressure_now - pressure_then
```

Etiquetas:

```text
SAME_FAV_SAME_PRESSURE
SAME_FAV_RAISE_PRESSURE
SAME_FAV_RAISE_AGGRESSIVE
SAME_FAV_LOWER_PRESSURE
SAME_FAV_LOWER_AGGRESSIVE
NEW_FAVORITE
NEW_FAVORITE_HIGH
FAVORITE_REMOVED
NO_FAVORITE_PRESSURE
UNKNOWN
```

Regla:

```text
Movimiento sin residual no vale.
Residual sin movimiento no basta.
Movimiento + residual + familia AH = lectura.
```

---

## 1E. Previas por localia real como variables binarias

Esta parte es obligatoria en la matriz.

No se mira "ultimo partido general". Se mira:

```text
LOCAL actual  -> su ultimo partido como local
VISITANTE actual -> su ultimo partido como visitante
```

Despues se reorienta desde el favorito actual:

```text
Si F = local:
  F_prev = ultimo partido local del local actual
  D_prev = ultimo partido visitante del visitante actual

Si F = visitante:
  F_prev = ultimo partido visitante del visitante actual
  D_prev = ultimo partido local del local actual
```

Para cada previa se calculan variables matematicas:

```text
prev_line_team = handicap previo visto desde ese equipo
prev_margin_team = goles_equipo - goles_rival
prev_residual = prev_margin_team - abs(prev_line_team)
```

Variable binaria:

```text
PREV_COVER =
  1 si prev_residual >= 0.25
  0 si prev_residual <= -0.25
  PUSH si esta en zona de igualdad
  UNKNOWN si no hay AH previo
```

Tambien se guarda la familia del AH previo:

```text
PREV_AH_0
PREV_AH_0_25
PREV_AH_0_5_0_75
PREV_AH_1_1_25
PREV_AH_1_5_1_75
PREV_AH_2_PLUS
```

Y se compara contra el AH actual:

```text
PREV_AH_LT_CURRENT  = antes exigian menos que hoy
PREV_AH_EQ_CURRENT  = exigencia parecida
PREV_AH_GT_CURRENT  = antes exigian mas que hoy
PREV_AH_UNKNOWN     = no hay linea previa
```

Esta comparacion es clave:

```text
F_prev CUBRE con AH previo parecido o superior al actual
=> valida favorito.

F_prev CUBRE con AH previo mucho menor que el actual
=> no valida automaticamente; solo dice que llego bien.

F_prev NO CUBRE y hoy le suben exigencia
=> alerta contra favorito.

D_prev CUBRE y hoy recibe handicap
=> alerta a favor del no favorito.

D_prev NO CUBRE por goleada y hoy recibe AH muy alto
=> puede estar descontado; no sumar automaticamente al favorito.
```

Ejemplo:

```text
Union actual -2.25.
Union previo local: 2:2 con AH desconocido/menor -> no valida -2.25.
Lansing previo visitante: pierde 6:0 con AH +1.5 -> no cubre.
Pero Union tampoco separo a Oakland.
Conclusion: la derrota de Lansing puede estar ya descontada; no comprar -2.25.
```

### 1F. Previas por localia real como variables O/U

La misma previa tambien se convierte en matriz de goles.

Para cada `F_prev` y `D_prev`:

```text
prev_total_goals = goles_equipo + goles_rival
prev_ou_line = linea O/U de ese partido previo, si existe
prev_ou_residual = prev_total_goals - prev_ou_line
```

Variable binaria si existe O/U previo:

```text
PREV_OU_OVER  = prev_ou_residual >= 0.25
PREV_OU_UNDER = prev_ou_residual <= -0.25
PREV_OU_PUSH  = zona neutra
PREV_OU_UNKNOWN = no hay linea O/U previa
```

Comparacion contra O/U actual:

```text
PREV_OU_LT_CURRENT = antes exigian menos goles que hoy
PREV_OU_EQ_CURRENT = exigencia similar
PREV_OU_GT_CURRENT = antes exigian mas goles que hoy
PREV_OU_UNKNOWN    = no hay O/U previo
```

Si no existe O/U previo, no se inventa. Se usa una variable secundaria:

```text
prev_total_goals_vs_ou_actual =
  OVER_CURRENT_LINE
  UNDER_CURRENT_LINE
  PUSH_CURRENT_LINE
```

Lectura:

```text
F_prev OVER con O/U previo igual o mayor que hoy
=> valida OVER.

F_prev OVER con O/U previo mucho menor que hoy
=> no valida automaticamente; hoy la casa exige mas goles.

D_prev UNDER + O/U actual bajo
=> refuerza UNDER.

D_prev OVER + AH alto
=> puede apoyar varianza, pero no necesariamente favorito.

Previas OVER pero O/U actual no sube
=> la casa no persigue memoria goleadora: cuidado con OVER.
```

---

## 2. Puerta de calidad

Antes de pronosticar, filtrar.

### 2A. Datos minimos para AH

Si falta `AH actual`:

```text
NO BET AH
```

Si falta `resultado final del H2H raiz`:

```text
NO BET AH salvo Col3 + indirectas muy alineadas
```

Si falta `linea historica del H2H`:

```text
se puede usar cobertura contra linea actual,
pero movimiento baja de peso
```

Si H2H viejo y sin H2H general reciente:

```text
NO BET o confianza baja
```

### 2B. Datos minimos para O/U

Si falta `O/U actual`:

```text
NO BET O/U
```

Si no hay ningun total util:

```text
NO BET O/U
```

Si solo hay marcador bruto sin tiros/proceso:

```text
O/U baja confianza
```

### 2C. Datos que contaminan

Bloquear o bajar confianza si:

```text
- equipos sin historico suficiente
- ligas muy mezcladas
- H2H con plantillas muy antiguas
- datos sin estadisticas y sin indirectas
- marcador corregido manualmente no confirmado
- AH absurdo sin comparativas
- O/U extremo sin muestra similar
```

---

## 3. Arbol AH raiz: cobertura del H2H contra la linea actual

### 3A. H2H cubre la linea actual para el favorito

```text
IF H2H.cover_actual = COVER
THEN memoria inicial = FAVORITO
```

Pasar a movimiento.

#### 3A.1 Movimiento sube presion

```text
IF COVER + RAISE_PRESSURE
```

Pregunta:

```text
la nueva familia AH cambia el pago real?
```

Si `0.25 -> 0.5`:

```text
exige ganar, pero sigue cerca
FAVORITO si previas/indirectas no contradicen
```

Si `0.75 -> 1`:

```text
un gol pasa de medio ganar a push
FAVORITO con cuidado
```

Si `1 -> 1.5`:

```text
un gol ya no vale
exigir previa/indirecta fuerte
```

Si `1.5 -> 2+`:

```text
solo FAVORITO si H2H cubrio tambien margen amplio
y la indirecta valida separacion
```

#### 3A.2 Movimiento baja presion

```text
IF COVER + LOWER_PRESSURE
```

Lectura:

```text
la casa mantiene jerarquia pero abarata entrada
```

Doble posibilidad:

```text
- FAVORITO si baja de 1/1.25 a 0.25/0.5 y el H2H fue dominante
- UNDER si baja presion y O/U bajo/capado
```

#### 3A.3 Favorito nuevo

```text
IF COVER + NEW_FAVORITE
```

Lectura:

```text
el mercado corrige jerarquia historica
```

Validar con:

```text
- previa favorito cubre
- indirecta favorito no falla
- no favorito no llega vivo
```

Si falta validacion:

```text
NO BET o NO FAVORITO protegido
```

### 3B. H2H queda en push contra la linea actual

```text
IF H2H.cover_actual = PUSH
THEN zona frontera
```

No comprar favorito por defecto.

Preguntas:

```text
1. AH actual es 0 / 0.25?
2. O/U actual es bajo?
3. H2H fue empate o victoria minima?
```

Si SI:

```text
riesgo de empate alto
preferir NO BET AH / UNDER
```

Si AH es 1 / 1.25:

```text
favorito puede ganar sin cubrir
mirar no favorito +handicap
```

Si AH es 2+:

```text
push no es fuerza: es techo
NO FAVORITO si indirectas no validan goleada
```

### 3C. H2H falla la linea actual para el favorito

```text
IF H2H.cover_actual = FAIL
THEN memoria inicial = NO FAVORITO
```

Solo rescatar favorito si se cumplen varias condiciones.

#### 3C.1 Falla pero domina volumen

```text
IF FAIL + stats favorecen favorito
```

No decidir aun.

Depende de familia AH:

```text
AH 0/0.25/0.5:
  volumen puede rescatar favorito

AH 0.75/1:
  volumen rescata solo si previa/indirecta acompana

AH 1.25/1.75:
  volumen ya no basta

AH 2+:
  volumen casi nunca rescata
  activar posible inflacion
```

#### 3C.2 Falla y no domina volumen

```text
IF FAIL + stats neutras/contra favorito
THEN NO FAVORITO
```

Solo bloquear si:

```text
- no favorito tiene derrota reciente extrema
- favorito viene de cubrir con autoridad
- mercado baja mucho la exigencia
```

#### 3C.3 Falla por margen pero casa baja presion

```text
IF FAIL + LOWER_PRESSURE
```

Lectura:

```text
la casa no abandona favorito pero reconoce techo
```

Salida probable:

```text
AH: NO BET o favorito en linea menor
O/U: UNDER si OU bajo/medio
```

#### 3C.4 Falla pero casa sube presion

```text
IF FAIL + RAISE_PRESSURE
```

Esta es zona contraintuitiva.

Preguntas:

```text
1. Es subida pequena dentro de la misma familia?
2. El H2H tuvo volumen muy claro?
3. La previa del favorito cubrio?
4. La indirecta del favorito valida margen?
```

Si todas SI:

```text
FAVORITO contraintuitivo
```

Si alguna NO:

```text
NO FAVORITO / NO BET
```

Si subida agresiva `>= +1.0`:

```text
NO FAVORITO salvo demolicion validada por indirectas
```

---

## 4. Arbol de movimiento AH

### 4A. Baja de presion manteniendo favorito

Ejemplos:

```text
1.25 -> 0.25
1 -> 0.25
0.75 -> 0.25
```

Lectura:

```text
la casa mantiene lado pero reduce castigo
```

Dicotomia:

```text
IF H2H fallo pero hubo volumen
AND baja a 0.25
THEN favorito posible, pero riesgo empate
```

```text
IF baja presion + OU bajo
THEN UNDER / NO BET AH
```

```text
IF baja presion + previa favorito cubre + indirecta valida
THEN FAVORITO
```

### 4B. Sube presion manteniendo favorito

Lectura:

```text
la casa exige mas margen
```

Dicotomia:

```text
IF H2H cubrio linea actual
AND previa/indirecta valida
THEN FAVORITO
```

```text
IF H2H no cubre linea actual
AND subida cambia familia AH
THEN NO FAVORITO
```

```text
IF subida agresiva + O/U alto
THEN NO FAVORITO + posible OVER
```

### 4C. Favorito nuevo

Lectura:

```text
la casa invierte jerarquia
```

Dicotomia:

```text
IF favorito nuevo + H2H cubre actual + previa cubre
THEN FAVORITO
```

```text
IF favorito nuevo + H2H falla actual + AH >= 1.5
THEN NO FAVORITO
```

```text
IF favorito nuevo + OU bajo
THEN UNDER / riesgo empate
```

### 4D. Favorito retirado

Lectura:

```text
la casa abandona antigua jerarquia
```

Dicotomia:

```text
IF antiguo favorito no cubrio H2H
AND ahora pierde estatus
THEN NO FAVORITO del antiguo / nuevo lado protegido
```

```text
IF estatus retirado + OU bajo
THEN UNDER
```

---

## 5. Mapa por familias AH

### 5A. AH 0

```text
mercado = DNB / pick'em
```

Preguntas:

```text
1. H2H fue empate?
2. O/U <= 2.25?
3. Col3 empate?
```

Si SI:

```text
NO BET AH / UNDER
```

Si un lado domina proceso y la casa no le da handicap:

```text
posible trampa: NO BET salvo indirectas muy claras
```

### 5B. AH 0.25

Zona mas sensible.

```text
empate medio castigo al favorito
empate medio beneficio al no favorito
```

Dicotomia:

```text
IF H2H favorito cubre + O/U medio/alto + indirecta favorece
THEN FAVORITO
```

```text
IF H2H falla + OU bajo
THEN NO FAVORITO / UNDER
```

```text
IF favorito domina volumen pero no resultado
AND casa mantiene 0.25
THEN no forzar: mirar previas
```

Regla entrenada:

```text
AH_0_25 + BASE_STATS_STRONG_FOR + IND_DOG_STATS_STRONG_FOR
=> NO FAVORITO / UNDER tiende a tener valor
```

### 5C. AH 0.5 / 0.75

```text
favorito necesita ganar
```

Dicotomia:

```text
IF H2H cubre + previa favorito cubre
THEN FAVORITO
```

```text
IF H2H fue empate/perdida + OU bajo
THEN NO FAVORITO
```

```text
IF 0.75 y H2H gano por 1
THEN cubre parcialmente / mirar cuota y O/U
```

### 5D. AH 1 / 1.25

Zona bisagra.

```text
un gol ya no cobra limpio
```

Dicotomia:

```text
IF H2H margen >= 2
THEN favorito posible
```

```text
IF H2H margen = 1
THEN favorito gana partido pero no problema matematico
```

```text
IF O/U bajo
THEN NO FAVORITO / UNDER
```

### 5E. AH 1.5 / 1.75

```text
favorito necesita separacion real
```

Dicotomia:

```text
IF H2H cubre + indirecta cubre + previa cubre
THEN FAVORITO
```

```text
IF una sola pata falla
THEN bajar confianza
```

```text
IF H2H no cubre pero volumen favorece
THEN volumen no basta
```

### 5F. AH 2+

Zona de inflacion.

```text
el favorito debe golear, no solo ser mejor
```

Dicotomia:

```text
IF H2H margen >= h
AND previa favorito margen >= h
AND indirecta favorito margen >= h
THEN FAVORITO
```

```text
IF H2H gana corto
THEN NO FAVORITO
```

```text
IF O/U >= 4
THEN puede haber OVER pero no necesariamente favorito
```

Regla entrenada:

```text
AH_2_PLUS + BASE_STATS_STRONG_FOR + OU_4_PLUS
=> NO FAVORITO
```

---

## 6. Arbol O/U raiz

La linea de goles no se decide por gusto ofensivo. Se decide por:

```text
total_H2H contra OU actual
movimiento AH
familia AH
previas
indirectas
Col3
```

### 6A. Total del H2H contra O/U actual

```text
total_h2h = goles_h2h_local + goles_h2h_visitante
residual_ou = total_h2h - OU_actual
```

Etiquetas:

```text
H2H_OVER_LINE   si residual_ou >= 0.25
H2H_PUSH_LINE   si cerca de 0
H2H_UNDER_LINE  si residual_ou <= -0.25
```

### 6B. Si H2H fue OVER contra linea actual

Preguntas:

```text
1. El O/U actual acompana o sube?
2. Hubo volumen ofensivo real?
3. Ambas previas tienen 3+ goles?
4. Indirectas tienen 3+ goles?
5. AH sugiere partido abierto o dominio con techo?
```

Si 1-4 SI y 5 abierto:

```text
OVER
```

Si H2H over pero O/U actual no sube:

```text
posible memoria no perseguida
NO BET / UNDER
```

### 6C. Si H2H fue UNDER contra linea actual

Preguntas:

```text
1. O/U <= 2.25?
2. AH 0/0.25?
3. H2H empate o victoria minima?
4. Col3 frio?
```

Si SI:

```text
UNDER
```

Si O/U alto pese a H2H under:

```text
la casa espera cambio de ritmo
mirar previas/indirectas
```

### 6D. O/U bajo

```text
OU <= 2.25
```

Dicotomia:

```text
IF AH 0/0.25 + H2H empate
THEN UNDER
```

```text
IF favorito nuevo + OU bajo
THEN UNDER antes que favorito
```

```text
IF previas goleadoras pero OU bajo
THEN la casa no persigue goles: UNDER/NO BET
```

### 6E. O/U medio

```text
OU 2.5 / 2.75
```

Dicotomia:

```text
IF H2H over + indirectas over
THEN OVER
```

```text
IF H2H under + Col3 under
THEN UNDER
```

```text
IF señales mixtas
THEN NO BET
```

### 6F. O/U alto

```text
OU 3 / 3.25 / 3.5
```

Dicotomia:

```text
IF AH bajo + O/U alto
THEN partido de intercambio: OVER posible
```

```text
IF AH alto + O/U alto
THEN decidir:
  - si favorito valida margen -> favorito + OVER
  - si favorito no valida margen -> no favorito + OVER
```

### 6G. O/U extremo

```text
OU >= 4
```

Dicotomia:

```text
IF AH 2+ + favorito no cubre H2H actual
THEN NO FAVORITO + OVER posible
```

```text
IF AH 2+ + favorito cubre todas las patas
THEN FAVORITO + OVER
```

```text
IF O/U extremo solo viene por un 6:0 reciente del no favorito
THEN inflacion: NO FAVORITO, no over automatico
```

---

## 7. Cruce AH + O/U

### 7A. Favorito + Over

Solo si:

```text
H2H cubre linea actual
AND movimiento sube o mantiene presion
AND O/U acompana
AND previa favorito cubre
AND indirecta favorito valida margen
AND no hay inflacion por rival comun
```

### 7B. Favorito + Under

Guion:

```text
favorito controla pero no rompe
```

Condiciones:

```text
AH bajo/medio
H2H favorece favorito
O/U bajo/medio
Col3 frio
no favorito no produce
```

### 7C. No favorito + Over

Guion:

```text
partido abierto, pero linea AH inflada
```

Condiciones:

```text
AH alto
O/U alto
H2H no cubre AH actual
favorito gana corto o no separa
no favorito viene castigado visualmente
indirecta no valida goleada del favorito
```

Ejemplo:

```text
Union -2.25 vs Lansing
O/U 4.5
H2H 3:2 no cubre -2.25
Union 2:2 vs Oakland
Oakland 6:0 Lansing
=> Lansing +2.25 + OVER
```

### 7D. No favorito + Under

Guion:

```text
favorito sobrevalorado y partido corto
```

Condiciones:

```text
AH 0/0.25/0.5
O/U bajo
H2H empate/victoria minima
Col3 frio
indirecta del dog resiste
```

---

## 8. Indirectas: rival comun

### 8A. Indirecta valida favorito

```text
fav vs rival comun: gana/cubre/produce
dog vs mismo rival: falla/no produce
```

Entonces:

```text
sube favorito
```

Pero si AH >= 2:

```text
exigir margen comparable a la linea
```

### 8B. Indirecta valida no favorito

```text
dog vs rival comun: cubre o compite
fav vs rival comun: no separa
```

Entonces:

```text
sube NO FAVORITO
```

### 8C. Inflacion por rival comun

Patron:

```text
dog viene de goleada encajada
fav no pudo separar al mismo rival
AH actual sube mucho
```

Salida:

```text
NO FAVORITO
```

Si O/U alto:

```text
NO FAVORITO + OVER posible
```

---

## 9. H2H Col3

Col3 no manda. Confirma o bloquea.

### 9A. Col3 confirma favorito

```text
Col3 cubre mismo sentido
AND H2H raiz cubre
THEN sube confianza
```

### 9B. Col3 contradice favorito

```text
Col3 no cubre
AND H2H raiz falla
THEN NO FAVORITO
```

### 9C. Col3 empate

```text
Col3 empate + OU bajo
THEN UNDER / riesgo empate
```

---

## 10. Estadisticas de proceso

Stats no son motor. Son confirmacion.

Orden:

```text
1. residual AH
2. movimiento AH
3. familia AH
4. O/U
5. indirectas/previas
6. stats
```

Stats que validan:

```text
tiros a puerta
tiros totales
ataques peligrosos
ataques
```

Dicotomia:

```text
IF stats favorecen favorito
AND AH bajo
THEN pueden rescatar favorito
```

```text
IF stats favorecen favorito
AND AH alto
THEN no rescatan por si solas
```

```text
IF stats favorecen dog
AND H2H falla favorito
THEN NO FAVORITO fuerte
```

---

## 11. Tabla/clasificacion

La tabla nunca decide sola.

### 11A. Linea acompana tabla

```text
favorito mejor clasificado
```

Solo valida si H2H/indirectas no contradicen.

### 11B. Linea contradice tabla

```text
favorito peor clasificado
```

Pregunta:

```text
H2H explica la contradiccion?
```

Si SI:

```text
posible favorito contraintuitivo
```

Si NO:

```text
NO FAVORITO / NO BET
```

---

## 12. Micro-reglas entrenadas

Las micro-reglas no sustituyen al arbol. Solo ajustan.

### 12A. Reglas AH operativas

```text
AH_0_25 + BASE_STATS_STRONG_FOR + IND_DOG_STATS_STRONG_FOR
=> NO FAVORITO
```

```text
DOG_RECENT_STATS_NEUTRAL + TABLE_FAV_WORSE
=> NO FAVORITO
```

```text
BASE_STATS_STRONG_FOR + IND_DOG_COVER + OU_MID
=> NO FAVORITO
```

```text
AH_LOW + BASE_COVER_FAIL + IND_DOG_MARGIN_POS2
=> NO FAVORITO
```

```text
AH_2_PLUS + BASE_STATS_STRONG_FOR + OU_4_PLUS
=> NO FAVORITO
```

### 12B. Reglas O/U operativas

```text
AH_LOW + NEW_FAVORITE + DOG_RECENT_STATS_LEAN_AGAINST
=> UNDER
```

```text
IND_DOG_MARGIN_POS2 + OU_LOW
=> UNDER
```

```text
BASE_PRESSURE_NEW_FAV + DOG_RECENT_STATS_LEAN_AGAINST
=> UNDER
```

```text
BASE_STATS_NEUTRAL + IND_DOG_STATS_STRONG_AGAINST + OU_MID
=> OVER
```

### 12C. Conflicto de micro-reglas

Si una regla favorece favorito y otra no favorito:

```text
NO BET salvo diferencia extrema
```

Si una regla favorece OVER y otra UNDER:

```text
NO BET O/U
```

---

## 13. Umbrales finales

### 13A. AH

```text
score_F = suma argumentos favorito
score_D = suma argumentos no favorito
draw_risk = riesgo empate / push / OU bajo
edge_AH = score_F - score_D - draw_risk
```

Salida:

```text
IF edge_AH >= umbral_favorito
THEN FAVORITO AH

IF edge_AH <= -umbral_dog
THEN NO FAVORITO AH

ELSE NO BET AH
```

Umbrales:

```text
con micro-regla alineada: 1.15 / 1.25
sin micro-regla: 2.10
con conflicto: 3.00
```

### 13B. O/U

```text
score_OVER = suma argumentos over
score_UNDER = suma argumentos under
edge_OU = score_OVER - score_UNDER
```

Salida:

```text
IF edge_OU >= 1.15
THEN OVER

IF edge_OU <= -1.15
THEN UNDER

ELSE NO BET O/U
```

Si hay conflicto entrenado:

```text
subir umbral o NO BET
```

---

## 14. Mapa final resumido

```text
1. Tomar ultimo H2H.
2. Orientarlo desde favorito actual.
3. Calcular residual contra AH actual.
4. Calcular cobertura: COVER / PUSH / FAIL.
5. Calcular movimiento: baja / sube / nuevo favorito / retirado.
6. Clasificar familia AH.
7. Calcular total H2H contra O/U actual.
8. Leer previas.
9. Leer indirectas.
10. Leer Col3.
11. Aplicar stats como confirmacion.
12. Aplicar micro-reglas entrenadas.
13. Resolver conflictos.
14. Emitir AH, O/U o NO BET.
```

---

## 15. Fuentes externas usadas para el enfoque

- Los mercados de linea se mueven por informacion, volumen y participantes
  fuertes; el movimiento debe leerse con contexto, no seguirse ciegamente.
  Fuente: Sportstrade, guia de line movement.
- En Asian Handicap, las lineas de cuarto dividen el stake y cambiar de
  `-0.75` a `-1` no equivale a cambiar de `-1` a `-1.5`; cada salto cambia
  el problema matematico de cobertura. Fuente: OddsIndex, guia de Asian
  Handicap.
- La literatura de mercados deportivos muestra que las cuotas suelen ser
  bastante informativas pero no perfectas; tambien puede haber sobrerreaccion
  en movimientos de linea. Fuente: Simon, Management Science, 2024.
- Para goles, el enfoque de Poisson/Dixon-Coles recuerda que el futbol tiene
  dependencia especial en marcadores bajos, por eso empates, `0-0`, `1-1`,
  `1-0` y `0-1` deben tratarse como rama propia en O/U bajo. Fuente:
  Dixon-Coles, "Modelling Association Football Scores and Inefficiencies in
  the Football Betting Market".

---

## 16. Bloqueos aprendidos por fallos recientes

### 16A. Cubrir como no favorito no valida ser favorito hoy

Si un equipo venia de cubrir recibiendo handicap:

```text
prev_pressure < 0
PREV_COVER = 1
```

y hoy pasa a favorito:

```text
current_pressure >= 0.5
```

entonces esa cobertura previa no valida el favorito.

Lectura:

```text
cubrir como dog con empate o derrota corta no es lo mismo que cubrir como
favorito obligado a ganar.
```

Salida:

```text
bajar favorito / NO BET AH
```

### 16B. AH 0.5/0.75 con O/U bajo

Si:

```text
AH actual = 0.5 / 0.75
O/U actual <= 2.25
```

el favorito necesita ganar en un mapa de pocos goles.

Salida:

```text
NO BET AH salvo micro-regla entrenada fuerte.
```

### 16C. O/U bajo no persigue memoria goleadora

Si:

```text
O/U actual <= 2.25
```

y los overs vienen solo de:

```text
H2H viejo
previas de 3 goles
sin O/U propio previo
sin dos señales recientes de 4+ goles
```

entonces:

```text
no comprar OVER.
```

Salida:

```text
NO BET O/U o UNDER si el resto del mapa enfria.
```

### 16D. H2H duplicado estadio/general

Si `H2H estadio` y `H2H general` son el mismo partido:

```text
mismo resultado
misma fecha
```

se cuenta una sola vez.

No se permite que el mismo `4:2` sume como dos H2H distintos.

### 16E. Ruptura de favorito visitante

Patron aprendido por los casos:

```text
Missouri Reign (W) 0-5 Lou Fusz Athletic (W)
Midlakes United 0-4 Snohomish United
```

Condiciones:

```text
favorito actual = visitante
AH actual entre -1 y -1.5
O/U actual entre 2.5 y 3.5
favorito visitante cubrio su ultima salida
favorito visitante gano por margen >= 2
```

Validacion extra por rival comun:

```text
favorito visitante vence al rival comun
dog actual llega con resultado mejor que proceso
dog actual fue dominado en tiros/ataques peligrosos
```

O por ritmo:

```text
previa del favorito tiene 5+ goles
o indirectas/Col3 tienen 4+ goles repetidos
```

Salida:

```text
FAVORITO VISITANTE AH
OVER
```

Lectura:

```text
No es over por memoria visual. Es over por ruptura:
el favorito visitante no solo puede cubrir -1/-1.25,
sino que el dog muestra fragilidad de proceso o el mapa trae ritmo alto.
```
