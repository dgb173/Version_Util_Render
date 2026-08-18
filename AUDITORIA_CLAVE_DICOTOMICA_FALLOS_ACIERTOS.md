# Auditoria de la clave dicotomica: fallos, aciertos y correcciones

Esta auditoria usa partidos con resultado confirmado por feedback manual. El
objetivo no es celebrar aciertos, sino entender que nodo de la matriz explica
cada salida.

Regla metodologica:

```text
Si una prediccion acierta pero el nodo que la produjo es debil, no se considera
regla fuerte.

Si una prediccion falla, no se ajusta el peso a ojo: se busca que condicion
matematica faltaba en la matriz.
```

---

## 1. Resumen de los casos auditados

| Partido | AH/O-U | Resultado | Salida actual | Auditoria |
|---|---:|---:|---|---|
| Union FC Macomb vs Lansing City | AH `2.25`, OU `4.5` | `2:3` | Lansing `+2.25` + OVER | Acierto tras detectar inflacion |
| Everton CD vs Univ Catolica | AH `-0.25`, OU `2.5` | `1:3` | Univ Catolica `-0.25` + OVER | Acierto por H2H + rebaja de barrera |
| Kahibah FC Reserves vs Adamstown Reserves | AH `-0.75`, OU `4` | `3:0` | NO BET | Fallo evitado tras bloqueo |
| Changchun Masses Properties vs Shandong | AH `0.5`, OU `2.25` | `1:1` | NO BET | Fallo evitado tras bloqueo |
| Missouri Reign vs Lou Fusz Athletic | AH `-1.25`, OU `2.75` | `0:5` | Lou Fusz `-1.25` + OVER | Acierto por ruptura visitante |
| Midlakes United vs Snohomish United | AH `-1.25`, OU `3.25` | `0:4` | Snohomish `-1.25` + OVER | Acierto por ruptura visitante |

---

## 2. Caso Union FC Macomb vs Lansing City

### Resultado

```text
Union FC Macomb 2-3 Lansing City
AH actual: Union -2.25
OU actual: 4.5
```

### Salida actual

```text
Lansing City +2.25
OVER
```

### Nodo que acierta

```text
AH 2+
+ H2H estadio 3:2 no cubre linea actual
+ OU 4+
+ favorito no confirma margen alto
+ rival comun genera inflacion
=> NO FAVORITO + OVER posible
```

### Por que antes fallaba

El sistema leia:

```text
Union gano H2H 3:2
Union tuvo volumen
Lansing venia de 6:0 en contra
```

Pero no normalizaba bastante:

```text
3:2 cubre un -0.25
3:2 NO cubre un -2.25
```

### Correccion

Se anadio:

```text
AH_2_PLUS + BASE_STATS_STRONG_FOR + OU_4_PLUS
=> NO FAVORITO
```

Y:

```text
inflacion por rival comun
```

### Lectura final

El over podia salir por varianza alta, pero eso no obligaba al favorito largo a
cubrir. Esa separacion AH/O-U es clave.

---

## 3. Caso Everton CD vs Univ Catolica

### Resultado

```text
Everton CD 1-3 Univ Catolica
AH actual: Univ Catolica -0.25
OU actual: 2.5
```

### Salida actual

```text
Univ Catolica -0.25
OVER
```

### Nodo que acierta

```text
H2H general falla marcador pero favorece proceso
+ barrera historica 1 -> 0.25
+ H2H estadio cubre
+ favorito nuevo
+ previa favorito cubre
=> FAVORITO
```

### Por que acierta

La casa no estaba pidiendo una goleada. Estaba bajando la barrera a `-0.25`.

Eso cambia la lectura:

```text
con -1 necesita autoridad
con -0.25 necesita ganar
```

El H2H general `2:2` no cubria por marcador, pero el volumen y la rebaja de
exigencia hacian que el favorito siguiera vivo.

### Riesgo presente

```text
Col3 empate
```

Pero el riesgo no bastaba para bloquear porque el H2H estadio y la previa del
favorito estaban alineados.

---

## 4. Caso Kahibah FC Reserves vs Adamstown Rosebud Reserves

### Resultado

```text
Kahibah 3-0 Adamstown
AH actual: Adamstown -0.75
OU actual: 4
```

### Salida actual

```text
NO BET AH
NO BET O/U
```

### Que habria fallado

El sistema anterior podia recomendar:

```text
Adamstown -0.75
```

por leer:

```text
H2H general cubre
no favorito no cubre
```

### Nodo que corrige

```text
cubrio como dog, no valida favorito
```

### Causa raiz

Un equipo puede cubrir un partido anterior recibiendo handicap o en contexto
mas favorable. Eso no significa que hoy pueda cubrir como favorito obligado.

La matriz ahora distingue:

```text
PREV_COVER como favorito
PREV_COVER como no favorito
```

Si:

```text
prev_pressure < 0
current_pressure >= 0.5
```

entonces:

```text
la cobertura previa NO valida favorito
```

### Lectura final

El mapa no cerraba. La salida correcta era no tocar el partido.

---

## 5. Caso Changchun Masses Properties vs Shandong

### Resultado

```text
Changchun 1-1 Shandong
AH actual: Changchun -0.5
OU actual: 2.25
```

### Salida actual

```text
NO BET AH
NO BET O/U
```

### Que habria fallado

El sistema anterior compraba:

```text
Changchun -0.5
OVER
```

### Nodos que corrigen

```text
H2H general/estadio duplicado
cover minimo con OU bajo
AH 0.5/0.75 con OU bajo
OU bajo no persigue goles
favorito necesita ganar con OU bajo
```

### Causa raiz

Habia doble conteo del mismo H2H:

```text
H2H estadio = 4:2
H2H general = 4:2
```

El sistema lo sumaba como dos pruebas independientes. Ya no.

Ademas:

```text
AH -0.5 obliga a ganar
OU 2.25 dice partido corto
```

En ese tipo de mapa, `1-1` es una amenaza estructural. No se debe comprar
favorito ni over sin micro-regla fuerte.

### Lectura final

La clave correcta era bloqueo, no pronostico.

---

## 6. Caso Missouri Reign vs Lou Fusz Athletic

### Resultado

```text
Missouri 0-5 Lou Fusz
AH actual: Lou Fusz -1.25
OU actual: 2.75
```

### Salida actual

```text
Lou Fusz -1.25
OVER
```

### Nodo que acierta

```text
ruptura de favorito visitante
```

Condiciones:

```text
favorito = visitante
AH entre -1 y -1.5
OU entre 2.5 y 3.5
favorito visitante cubre previa
favorito valida rival comun
dog llega peor que resultado por proceso
```

### Por que antes faltaba el OVER

El sistema veia bien el AH pero dejaba `OU neutro`.

La mejora fue entender que en este mapa el favorito visitante no solo cubre,
sino que rompe:

```text
si el dog fue dominado en proceso
y el favorito visitante valida rival comun
=> el -1.25 tiene correlacion con OVER
```

### Lectura final

El over no sale por memoria visual, sale porque el guion de cobertura exige
ruptura.

---

## 7. Caso Midlakes United vs Snohomish United

### Resultado

```text
Midlakes 0-4 Snohomish
AH actual: Snohomish -1.25
OU actual: 3.25
```

### Salida actual

```text
Snohomish -1.25
OVER
```

### Nodo que acierta

```text
ruptura de favorito visitante
```

Se combinan:

```text
H2H general falla marcador pero hay volumen
previa favorito cubre
dog llega peor que resultado
```

### Riesgo detectado

```text
Col3 empate
```

Pero no bloquea porque la ruptura visitante pesa mas:

```text
AH -1.25
OU 3.25
favorito visitante con previa de 3:5
indirecta local 6:1
```

### Lectura final

El over era parte natural del mismo guion del AH. No eran dos picks separados:
eran una lectura correlacionada.

---

## 8. Tipos de fallo detectados

### 8A. Fallo por residual mal normalizado

Ejemplo:

```text
Union 3:2 usado como apoyo a -2.25
```

Correccion:

```text
si H2H no paga linea actual, no usar marcador bruto
```

### 8B. Fallo por doble conteo

Ejemplo:

```text
Changchun 4:2 contado como H2H estadio y general
```

Correccion:

```text
si mismo resultado/fecha, solo cuenta una vez
```

### 8C. Fallo por confundir cover como dog con validacion de favorito

Ejemplo:

```text
Kahibah / Adamstown
```

Correccion:

```text
PREV_COVER debe guardar signo del AH previo
```

### 8D. Fallo por comprar OVER en O/U bajo

Ejemplo:

```text
Changchun -0.5, OU 2.25
```

Correccion:

```text
OU bajo no persigue goles salvo señales recientes fuertes y explicitas
```

### 8E. Fallo por omitir OVER correlacionado con favorito visitante

Ejemplos:

```text
Missouri 0-5 Lou Fusz
Midlakes 0-4 Snohomish
```

Correccion:

```text
favorito visitante -1/-1.5 + OU 2.5/3.5 + previa cubre + dog fragil
=> FAVORITO + OVER
```

---

## 9. Reglas de auditoria permanente

Cada pronostico debe poder responder:

```text
1. Que H2H raiz uso?
2. Cubria la linea actual o no?
3. Hubo movimiento de presion?
4. La familia AH cambia el problema?
5. La previa del favorito cubrio con que signo de AH?
6. La previa del dog cubrio con que signo de AH?
7. El O/U actual persigue o enfria memoria goleadora?
8. Hay rival comun?
9. Hay doble conteo?
10. Hay micro-regla entrenada o solo intuicion?
```

Si una de estas preguntas queda en contradiccion:

```text
NO BET
```

Si todas cierran:

```text
emitir AH/O-U con confianza segun edge y soporte entrenado
```

