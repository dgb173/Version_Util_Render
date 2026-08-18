# Clave dicotomica binaria H2H-first para AH y Over/Under

Objetivo: leer como se posiciona la casa de apuestas a partir del ultimo H2H,
sin empezar por el marcador bruto ni por la clasificacion. La pregunta madre no
es "quien gano", sino:

> Que exige hoy la casa y que dijo el ultimo H2H sobre esa exigencia?

Esta clave sirve para llegar a una lectura binaria:

- AH: `FAVORITO`, `NO_FAVORITO`, `NO BET`
- Goles: `UNDER`, `OVER`, `NO BET`

La regla central es antiintuitiva: un H2H perdido o empatado por el favorito no
siempre va contra el favorito. Si el volumen fue claramente suyo y la casa
mantiene o sube su estatus, la casa puede estar corrigiendo el marcador, no
repitiendolo.

---

## 0. Puerta de entrada

### 0A. Hay H2H directo util?

Si NO:

- No abrir pronostico fuerte.
- Usar `NO BET` salvo que Col3 + indirectas + previas coincidan de forma muy
  clara.

Si SI:

- Pasar a 1.
- Orientar siempre el H2H desde el favorito actual, no desde el local historico.

---

## 1. Lectura AH desde el H2H

### 1A. El favorito actual cubria la linea de hoy en el H2H?

Si SI:

- Memoria inicial pro favorito.
- Pasar a 2A.

Si NO:

- Veto inicial contra favorito.
- Pasar a 1B.

### 1B. Aunque no cubrio, el favorito actual domino el volumen del H2H?

Usar como volumen:

- tiros a puerta
- tiros totales
- ataques peligrosos
- ataques

Si SI:

- Etiqueta: `VOLUMEN_PERDONA_RESULTADO`.
- Lectura antiintuitiva: la casa puede estar corrigiendo un marcador enganoso.
- Pasar a 2B.

Si NO:

- El marcador y el proceso castigan al favorito.
- Lectura base: `NO_FAVORITO`.
- Pasar a 4 para ver si el O/U confirma control o ruptura.

---

## 2. Movimiento de presion de la casa

### 2A. Si el H2H ya validaba al favorito

Pregunta: la casa mantiene/sube/baja la exigencia?

Si `RAISE_PRESSURE_KEEP_FAVORITE`:

- La casa compra continuidad y mas autoridad.
- AH: `FAVORITO`, salvo que OU bajo + indirectas negativas bloqueen.

Si `SAME_PRESSURE_KEEP_FAVORITE`:

- La casa repite memoria.
- AH: `FAVORITO` moderado.

Si `LOWER_PRESSURE_KEEP_FAVORITE`:

- La casa abarata la entrada pero mantiene jerarquia.
- AH: `FAVORITO`, pero bajar confianza si el H2H fue viejo o el OU esta capado.

### 2B. Si el H2H NO validaba al favorito, pero el volumen si

Pregunta: la casa crea o mantiene favorito contra el marcador?

Si `NEW_FAVORITE_STATUS`:

- Etiqueta: favorito nuevo por correccion de proceso.
- Si AH esta en familia `1.25-1.75` y aparece `VOLUMEN_PERDONA_RESULTADO`,
  la validacion historica actual favorece mas al `NO_FAVORITO`.
- Regla entrenada:
  - `AH_1_25_1_75 + NEW_FAVORITE_STATUS + VOLUMEN_PERDONA_RESULTADO`
  - Direccion: `NO_FAVORITO`
  - Train: 26/37 = 70.27%
  - Validacion: 10/14 = 71.43%

Si `RAISE_PRESSURE_KEEP_FAVORITE`:

- Es la senal mas agresiva de la casa.
- Solo comprar favorito si tambien hay:
  - volumen H2H fuerte
  - previa del favorito cubriendo
  - indirecta no negativa
- Si no, lectura de trampa: `NO_FAVORITO` o `NO BET`.

Si `LOWER_PRESSURE_KEEP_FAVORITE`:

- La casa no abandona al favorito, pero rebaja exigencia.
- Esto suele apuntar a partido corto, no a autoridad limpia.
- Pasar a rama UNDER.

Si `FAVORITE_STATUS_REMOVED`:

- La casa retira el estatus historico.
- AH: `NO_FAVORITO` o `NO BET`.

---

## 2C. Escalera matematica de handicap

No todos los saltos valen lo mismo. La lectura se hace por familias:

- `0 / 0.25`: zona de empate, DNB, medio castigo o media proteccion.
- `0.5 / 0.75`: zona de victoria simple; ganar por 1 casi siempre valida.
- `1 / 1.25`: zona bisagra; ganar por 1 ya no es lo mismo que ganar limpio.
- `1.5 / 1.75`: zona de separacion real; el favorito necesita autoridad.
- `2+`: zona de demolicion; el marcador bruto ya no basta.

Clave dicotomica:

Si el H2H gano pero NO cubre la linea actual:

- En `0.25-0.75`, el volumen puede rescatar al favorito.
- En `1-1.25`, exigir al menos previa o indirecta alineada.
- En `1.5+`, el volumen solo confirma que compitio, pero no paga la linea.
- En `2+`, si no gano por margen amplio, el H2H se lee contra el favorito.

Por tanto:

- `H2H gana 3:2 + AH actual 0.25/0.5` = posible favorito.
- `H2H gana 3:2 + AH actual 2.25` = posible inflacion contra favorito.

---

## 2D. Regla de inflacion por rival comun

Esta es la regla que corrige los falsos favoritos largos.

Pregunta 1: el no favorito viene de una derrota grande contra el rival comun?

Si SI:

- No sumar automaticamente al favorito.
- Etiqueta: `CASTIGO_RIVAL_DESCONTADO`.

Pregunta 2: el favorito jugo contra ese mismo rival comun y no lo separo?

Si SI:

- Activar `INFLACION_POR_RIVAL_COMUN`.
- AH: girar a `NO_FAVORITO` o bajar a `NO BET`.

Estructura:

- Favorito actual con AH `1.5+` o `2+`.
- No favorito viene de goleada encajada.
- Favorito empata, pierde o gana corto contra el mismo rival.
- H2H estadio tampoco cubre la linea actual.

Salida:

- AH: `NO_FAVORITO`.
- O/U: se decide aparte. Si la linea de goles esta en `4+`, puede ser `OVER`
  aunque el favorito largo sea malo.

Ejemplo logico:

- Actual: Union FC Macomb `-2.25` vs Lansing City, O/U `4.5`.
- H2H estadio: Union gana `3:2`, pero contra `-2.25` NO cubre.
- Union vs Oakland: `2:2`.
- Oakland vs Lansing: `6:0`.
- Lectura: la casa puede estar inflando a Union por el derrumbe de Lansing,
  pero Union no valido esa superioridad contra Oakland.
- Salida: `Lansing City +2.25` y el total puede seguir apuntando a `OVER`.

---

## 2E. Fallo entrenado: AH 2+ con O/U 4+

Regla aprendida desde el Explorador terminado:

- Clave: `AH_2_PLUS + BASE_STATS_STRONG_FOR + OU_4_PLUS`.
- Direccion historica: `NO_FAVORITO`.
- Train: `58/93 = 62.37%`.
- Validacion temporal: `11/14 = 78.57%`.
- Lift sobre baseline del no favorito en AH 2+: `+28.98`.

Lectura dicotomica:

Si el favorito tiene AH `2+`:

- Pregunta 1: el O/U esta en `4+`?
- Pregunta 2: el argumento del favorito viene de volumen historico/base, no de
  cubrir claramente la linea actual?

Si ambas son SI:

- No comprar favorito largo por volumen.
- Activar `NO_FAVORITO + handicap`.
- El `OVER` puede seguir vivo, porque una linea de goles alta habla de varianza,
  no necesariamente de goleada limpia del favorito.

Esta regla corrige el error de leer `3:2` como apoyo a un `-2.25`. Ese marcador
gana el partido, pero falla el problema matematico de la linea.

---

## 2F. Capa fina: micro-reglas y bloqueo

La lectura final no debe salir de una sola intuicion. El sistema queda dividido
en dos capas:

1. Motor matematico base:
   - residual H2H contra linea actual
   - movimiento de presion
   - previa favorito / previa no favorito
   - indirectas
   - O/U

2. Micro-reglas entrenadas:
   - solo entran si el patron existio en partidos terminados del Explorador
   - suman poco, no sustituyen al mapa
   - si hay micro-reglas opuestas, se activa conflicto y sube el umbral

Regla de bloqueo:

- Sin micro-regla entrenada, el AH necesita mas margen interno para aparecer.
- Con micro-regla a favor, el umbral baja.
- Con micro-reglas enfrentadas, no se fuerza pick salvo diferencia extrema.

Esto evita que el sistema pinte favoritos por volumen visual cuando el historico
limpio no lo valida.

---

## 3. Tabla contra linea

### 3A. La linea va contra la clasificacion?

Si NO:

- La tabla acompana la linea.
- No es suficiente para apostar, pero no contradice al favorito.

Si SI:

- Activar lectura antiintuitiva.
- Pregunta: el H2H o el volumen explican por que la casa contradice la tabla?

Si SI:

- La casa esta comprando matchup, localia o memoria oculta.
- Mantener la lectura de la rama anterior.

Si NO:

- Posible linea de trampa.
- AH: `NO_FAVORITO` o `NO BET`.

---

## 4. Clave Over/Under

El O/U no se lee aislado. Se lee como confirmacion o freno del mapa AH.

### 4A. O/U actual <= 2.25?

Si SI:

- Etiqueta: `OU_CAPADO`.
- La casa no esta vendiendo partido roto.
- Guion natural: margen corto, empate, push, 1-0, 1-1, 0-0.
- Pasar a 5 UNDER.

Si NO:

- Pasar a 4B.

### 4B. O/U actual entre 2.5 y 2.75?

Si SI:

- Zona neutral.
- Decide el H2H:
  - H2H total bajo -> UNDER leve.
  - H2H total alto + volumen -> OVER leve.
  - contradiccion -> NO BET.

Si NO:

- Pasar a 4C.

### 4C. O/U actual >= 3?

Si SI:

- La casa deja abierta ruptura.
- Pero no comprar OVER automaticamente.
- Pregunta clave: el AH tambien sube fuerte o solo sube el recuerdo goleador?

Si AH sube fuerte y O/U no sube tanto:

- Lectura antiintuitiva: dominio con techo.
- Goles: `UNDER` o `NO BET`, no OVER automatico.

Si AH y O/U suben juntos, y hay H2H de demolicion:

- Goles: `OVER`.

---

## 5. Rama UNDER

### 5A. El H2H quedo por debajo o en push contra el O/U actual?

Si SI:

- Etiqueta: `MEMORIA_H2H_UNDER`.
- Goles: `UNDER`, salvo que previas recientes rompan el mapa.

Si NO:

- Pasar a 5B.

### 5B. Hay favorito castigado por H2H, pero la casa baja presion y mantiene favorito?

Si SI:

- Regla entrenada:
  - `FAVORITO_CASTIGADO_POR_H2H + LOWER_PRESSURE_KEEP_FAVORITE + STATS=LEAN_FOR_TEAM`
  - Direccion: `UNDER`
  - Train: 21/32 = 65.62%
  - Validacion: 12/15 = 80.00%

Lectura:

- La casa no niega al favorito, pero tampoco le compra autoridad amplia.
- El partido se lee por control, no por intercambio.

Si NO:

- Pasar a 5C.

### 5C. Hay goleada reciente, pero el O/U sigue en 2/2.25?

Si SI:

- Etiqueta: `GOLEADA_NO_PERSEGUIDA`.
- La casa separa resultado reciente de total esperado.
- Goles: `UNDER` o `NO BET`, no OVER por memoria visual.

Si NO:

- UNDER solo si tambien hay:
  - OU capado
  - H2H frio
  - Col3 frio
  - indirectas sin volumen ofensivo

---

## 6. Rama OVER

### 6A. El H2H fue over contra la linea actual?

Si NO:

- No hay OVER base.
- Volver a UNDER/NO BET.

Si SI:

- Pasar a 6B.

### 6B. El H2H over tuvo volumen real?

Volumen real:

- tiros a puerta altos
- ataques peligrosos altos
- ambos equipos producen, no solo un marcador raro

Si NO:

- Etiqueta: over visual.
- Goles: `NO BET`; no comprar marcador bruto.

Si SI:

- Pasar a 6C.

### 6C. La casa acompana el over con O/U alto?

Si SI:

- Goles: `OVER`.

Si NO:

- Si el O/U queda bajo pese al H2H over:
  - lectura antiintuitiva: la casa no persigue la memoria de goles.
  - Goles: `UNDER` o `NO BET`.

---

## 7. Indirectas y Col3 como validacion

### 7A. La indirecta del favorito confirma volumen?

Si SI:

- Sube confianza del lado AH.
- En goles solo suma si tambien sube produccion ofensiva.

Si NO:

- Etiqueta: `INDIRECTA_DEBILITA_FAVORITO`.
- Si el favorito ya venia castigado por H2H:
  - AH: `NO_FAVORITO`
  - Goles: depende del O/U; si esta capado, `UNDER`.

### 7B. Col3 enfria el total?

Si SI:

- Etiqueta: `COL3_ENFRIA_TOTAL`.
- Refuerza UNDER si el O/U actual <= 2.25.

Si NO:

- Col3 no manda; vuelve al H2H directo.

---

## 8. Salidas finales

### AH

Salida `FAVORITO` solo si:

- H2H cubre la linea actual, o
- H2H no cubre pero volumen perdona resultado y la casa mantiene/sube presion,
- y no hay indirecta fuerte en contra,
- y el O/U no obliga a bajar por empate/margen corto.

Salida `NO_FAVORITO` si:

- H2H no cubre y tampoco hay volumen,
- o la casa crea favorito nuevo con linea media/alta pero el patron historico
  favorece al no favorito,
- o indirecta contradice claramente al favorito.

Salida `NO BET` si:

- no hay H2H util,
- el H2H y las indirectas se contradicen,
- el O/U capado aumenta mucho el riesgo de empate,
- o la regla no paso validacion historica.

### Goles

Salida `UNDER` si:

- OU <= 2.25,
- H2H frio o en push contra la linea,
- favorito castigado por H2H con presion rebajada,
- la casa no persigue goleadas recientes.

Salida `OVER` si:

- H2H over + volumen real,
- O/U actual acompana,
- el AH no indica dominio con techo,
- previas e indirectas no enfrian el ritmo.

Salida `NO BET` si:

- el over depende solo de marcador bruto,
- el under depende solo de miedo al empate,
- H2H y Col3 chocan,
- la casa deja senales mixtas entre AH y O/U.

---

## 9. Regla de disciplina

Si una rama parece logica pero no existe regla validada por partidos terminados,
no se convierte en pronostico fuerte.

En la auditoria exigente actual:

- Umbral 78% con soporte serio: 0 reglas aceptadas.
- Umbral 65% con validacion temporal: 2 reglas aceptadas.

Por tanto, la clave no debe forzar picks. Debe filtrar hasta quedarse solo con
casos donde la logica de mercado y el historico terminado coinciden.
