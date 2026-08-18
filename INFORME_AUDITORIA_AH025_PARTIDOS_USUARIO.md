# Informe auditoria AH 0.25

Fecha: 2026-06-30

Objeto: revisar partidos historicos con AH inicial `0.25` usando la clave
dicotomica corregida por jerarquia:

1. ultimo H2H general y movimiento de handicap/favorito,
2. ultimo H2H en estadio,
3. previas por localia real,
4. Col3 e indirectas,
5. tabla,
6. estadisticas solo como confirmacion o alerta.

La lectura no busca justificar el resultado despues de verlo, sino detectar que
habria debido pesar antes del partido y que patrones son replicables.

## Regla madre para esta muestra

En `AH 0.25`, el empate ya dana al favorito. Por eso:

```text
H2H_GENERAL + movimiento de favorito manda.
H2H_ESTADIO confirma o bloquea.
OU <= 2.25 aumenta mucho el riesgo de DOG/UNDER.
Una inversion clara de mercado puede salvar al favorito aunque el H2H bruto sea malo.
```

## Lectura partido a partido

| Partido | FT | Salida clave corregida | Lectura dominante |
|---|---:|---|---|
| Ports Authority vs FC Kallon | 2-0 | FAV AH / NO BET OU | H2H bruto anti-Ports, pero el mercado invierte favorito hacia Ports en AH 0.25. Esa inversion es variable dominante. |
| Kamboi Eagles vs Wusum Stars | 2-1 | FAV AH / NO BET OU | Misma estructura: H2H general y estadio eran anti-Kamboi, pero la linea actual convierte a Kamboi en favorito minimo. |
| Abacha City vs Diamond Stars | 0-0 | DOG proteccion / UNDER | H2H general anti-Abacha, estadio pro-Abacha, pero OU 1.75 y AH 0.25 convierten el empate en amenaza central. |
| Kitchee U22 vs HK Rangers U22 | 2-0 | FAV AH / NO BET OU | Inversion de mercado fuerte contra HK Rangers, aunque HK traia senales ofensivas. La linea no mantiene al ganador anterior como favorito. |
| Koninklijke HFC vs Barendrecht | 1-2 | DOG AH / NO BET OU | Inversion de mercado, pero H2H estadio ya mostraba fallo del local y Barendrecht tenia mejor soporte por tabla/indirecta. |
| Sousa PB vs Botafogo PB | 2-0 | FAV AH / NO BET OU | H2H general reciente: Botafogo no confirma como favorito y la casa pasa el mando a Sousa en casa. Inversion/rebaja domina. |
| Sporting Gijon vs Deportivo | 1-1 | DOG/NO BET AH / UNDER | Estadio favorecia Sporting, pero H2H general + tabla favorecian Deportivo y OU 2.25 bloqueaba compra de favorito. |
| Groningen vs AZ Alkmaar | 3-0 | FAV AH / OVER | Inversion de mercado muy fuerte: AZ venia de ganar 4-1 como favorito alto y aun asi la linea pasa a Groningen 0.25. |
| Widzew Lodz vs Gornik Zabrze | 0-0 | DOG AH / UNDER | H2H general y estadio anti-Widzew, tabla muy anti-Widzew y OU 2.25. Favorito local no comprable. |
| FC Wagadou vs Banjul United | 1-1 | DOG/NO BET AH / UNDER | Sin estadio, H2H general anti-Wagadou, Col3/indirectas con empates y OU 2.25. Empate estructural. |
| Parrillas One vs FC Brasilia Rio Lindo | 1-1 | DOG/NO BET AH / UNDER | Estadio pro-Parrillas, pero H2H general fue 2-2 y AH 0.25 + OU 2.25 favorece bloqueo por empate. |
| Dumbarton vs Annan Athletic | 2-1 | FAV AH / OVER | H2H estadio cubre para Dumbarton, dog llega de goleada encajada, indirecta local 3-0. El estadio manda. |
| AD Isidro Metapan Res vs Municipal Limeno Res | 1-2 | DOG AH / UNDER | Mismo favorito no supera H2H general y estadio queda en empate. AH 0.25 no perdona. |
| Operario PR vs Atletico GO | 1-0 | FAV AH / UNDER | H2H estadio con misma linea 0.25 fue 3-0. Aunque el general fue 0-0, el estadio es variable dominante. |
| Suokuaili Moss vs Elche Ilicitano | 2-5 | DOG AH / OVER | Sin estadio, H2H general draw para AH 0.25, tabla/indirectas favorecen Elche y hay senales de ruptura por derrotas 3-0/2-0. |
| CD Guadalajara vs Ourense CF | 1-0 | FAV AH / UNDER | H2H general anti-Guadalajara, pero mercado invierte al local a 0.25 y la previa local 1-0 valida partido corto. |
| Inter F.A vs Platense | 0-0 | DOG/NO BET AH / UNDER | Doble 2-2 en H2H general/estadio. Aunque Inter domina stats y tabla, AH 0.25 + empate repetido bloquea favorito. |
| Gimnastic Tarragona vs Villarreal B | 0-1 | DOG AH / UNDER | H2H general 0-0, dog visitante llega cubriendo con buen proceso, indirecta visitante cubre. Favorito local no confirmado. |
| LPRC Oilers vs BEA Mountain | 0-2 | DOG AH / UNDER | H2H general y estadio son anti-LPRC. La previa local no compensa dos precedentes directos perdidos. |
| SV Ried vs Rheindorf Altach | 3-0 | FAV AH / NO BET OU | H2H bruto anti-Ried, pero barrera baja a 0.25 y Ried trae previa + indirecta local muy fuertes. |
| Bromley U21 vs Colchester U21 | 2-3 | DOG AH / OVER | H2H general anti-Bromley, indirecta local 4-0 en contra y entorno U21/OU 3.5 de ruptura. |
| Kadhimiya SC vs Al Hussein | 3-1 | FAV AH / OVER | H2H general anti-Kadhimiya y estadio 1-1, pero mercado gira a Kadhimiya 0.25. Previa local 1-0 e indirecta local 1-0 sostienen al favorito. |
| Orlando City B vs Columbus Crew B | 2-2 | DOG AH / OVER | DOG correcto por AH 0.25 y Columbus cubriendo fuera. El UNDER era trampa: OU 3.5 seguia alto pese a H2H de 3 goles, Orlando venia de 28 tiros/10 a puerta perdiendo 1-2 y tenia indirecta rota 1-4. |
| Pro Vercelli U20 vs Venezia Youth | 4-0 | FAV AH / OVER | Doble H2H anti-Pro Vercelli, pero mercado invierte a local 0.25 y la indirecta local fuera de casa cubre. En juvenil, el flip + forma reciente pesa mas que memoria directa. |
| Fnjaa vs Al-Msnaa | 0-0 | DOG/NO BET AH / UNDER | Estadio 5-0 antiguo parece pro-Fnjaa, pero la linea cae de 1.0 a 0.25, el H2H general es 1-1 y Col3 tambien empata. Trampa de favorito barato. |
| Carrarese vs Mantova | 0-0 | DOG/NO BET AH / UNDER | H2H general anti-Carrarese, estadio 1-1, Col3 1-1. La tabla y previa local no bastan contra empate repetido en AH 0.25. |
| Wrexham vs Preston | 2-1 | FAV AH / OVER | Preston domino el H2H reciente pero no gano. El mercado gira a Wrexham en casa tras previa 5-3: antiguo volumen sin premio no pesa mas que el flip. |
| Chandigarh FA vs Haryana | 0-2 | DOG AH / UNDER | Sin estadio, H2H general anti-Chandigarh y misma referencia repetida en indirecta. Stats favorables antiguas no borran que perdio la barrera. |
| Perseden Denpasar vs Persiba Bantul | 1-2 | DOG AH / OVER | H2H general 3-0 anti-Perseden y no hay estadio que lo corrija. Las indirectas dan actividad, pero el mando AH va al dog. |
| Kidderminster Harriers vs AFC Telford | 3-0 | FAV AH / OVER | H2H general anti-Kidderminster, pero el estadio en misma localia fue 3-0 y la tabla/localia actual lo refuerzan. Estadio domina. |
| CD Ebro vs CD Tudelano | 2-2 | DOG/NO BET AH / OVER | Estadio antiguo pro-Ebro, pero general 2-2, OU 1.75 y Tudelano llega fuerte fuera. No comprar favorito; over sale por lineas bajas y marcadores 3-1/2-2 del mapa. |
| Crewe Alexandra vs Gillingham | 1-0 | FAV AH / UNDER | H2H general anti-Crewe, pero estadio misma linea 2-0, previa local 1-0 y dog viene fallando. Estadio + forma corta decantan. |
| Santo Andre vs Gremio Prudente | 1-0 | FAV AH / UNDER | H2H general 0-0 con antiguo favorito sin premio. Mercado gira a Santo Andre, tabla mejor y dog viene de 0-0 fallando -0.25. |
| Avenida RS vs Monsoon FC | 1-1 | DOG/NO BET AH / NO BET OU | H2H general 0-0 y Monsoon tambien trae 0-0 reciente. Aunque Avenida gana previa, el mapa es de empate estructural. |

## Patrones replicables detectados

### 1. Inversion de mercado en AH 0.25

Este es el patron mas importante de la muestra.

```text
El rival gano o cubrio el ultimo H2H,
pero el partido actual no mantiene al rival como favorito.
La linea pasa al local actual en AH 0.25.
```

Esto no debe leerse como DOG automatico. Es una pregunta:

```text
Por que la casa convierte en favorito al que el H2H bruto castiga?
```

Cuando la inversion aparece junto a previa local cubierta o rebaja fuerte del
rival anterior, el favorito local queda vivo:

- Ports Authority
- Kamboi Eagles
- Kitchee U22
- Sousa PB
- Groningen
- CD Guadalajara

### 2. Inversion de mercado que NO basta

La inversion no manda si el estadio o el mapa de empate la bloquean.

```text
MARKET_FLIP_025
+ H2H_ESTADIO FAIL/PUSH
+ OU bajo
+ tabla o indirectas contra el favorito
= DOG / NO BET AH
```

Ejemplos:

- Koninklijke HFC vs Barendrecht
- Widzew Lodz vs Gornik Zabrze
- FC Wagadou vs Banjul United
- Gimnastic Tarragona vs Villarreal B

### 3. Empate repetido en AH 0.25

Si el favorito necesita ganar y el arbol trae empates/pushes, el sistema debe
bloquearlo aunque tenga mejores estadisticas.

```text
H2H general draw/push
+ H2H estadio draw/push
+ OU <= 2.25
= DOG por proteccion / UNDER
```

Ejemplos:

- Inter F.A vs Platense: 2-2 y 2-2 previos, termina 0-0.
- Parrillas One vs Brasilia: general 2-2, termina 1-1.
- FC Wagadou vs Banjul: Col3 e indirectas con empate, termina 1-1.

### 4. Estadio con misma familia H025

El H2H estadio pesa mucho cuando repite localia y familia de linea.

```text
H2H_ESTADIO cubre con misma familia H025
+ H2H_GENERAL no lo invalida de forma extrema
= FAV
```

Ejemplos:

- Operario PR vs Atletico GO: estadio 3-0 con 0.25, termina 1-0.
- Dumbarton vs Annan: estadio 2-1, termina 2-1.

Pero si el estadio ya mostro fallo del favorito actual:

```text
H2H_ESTADIO FAIL/PUSH
+ H025
= bloqueo del favorito salvo inversion muy fuerte
```

Ejemplos:

- Koninklijke HFC vs Barendrecht
- Widzew Lodz vs Gornik Zabrze
- AD Isidro Metapan Res vs Municipal Limeno Res

### 5. Resultado + handicap por encima de stats

Muchos casos muestran equipos que ganan o cubren con peores stats. Para la
clave, eso no es ruido: es informacion de handicap.

Ejemplos:

- Ports Authority gana 2-0 aunque el H2H bruto era muy malo.
- Kamboi gana 2-1 pese a previas con proceso pobre.
- Guadalajara gana 1-0 aunque Ourense tenia tabla e H2H general mejores.
- Inter domina proceso en H2H, pero el resultado repetido 2-2 avisaba de empate.

Conclusion:

```text
Primero resultado + handicap.
Despues estadisticas.
Nunca al reves.
```

### 6. OU capado no significa siempre under

En `OU <= 2.25`, el sistema debe tender a UNDER si el mapa trae empates. Pero
si hay derrotas amplias, indirectas rotas o U21/reserves, el OVER puede aparecer
como ruptura.

OVER replicable:

- Suokuaili Moss vs Elche: local venia de indirecta 3-0 en contra y dog tenia ruta de ruptura.
- Bromley U21 vs Colchester U21: U21 + OU 3.5 + H2H/indirectas rotas.
- Groningen vs AZ: H2H general 4-1 y mercado invierte hacia local, partido de ruptura.
- Dumbarton vs Annan: dog llega de 4-0 en contra e indirecta local 3-0.

UNDER replicable:

- Abacha vs Diamond
- Sporting vs Deportivo
- Widzew vs Gornik
- Inter vs Platense
- Gimnastic vs Villarreal B
- LPRC vs BEA

## Ajuste propuesto para la clave

### 7. Estadio antiguo: memoria, no sentencia

Varios casos tienen H2H de estadio muy antiguo. No se debe eliminar, porque tu
jerarquia lo mantiene arriba, pero tampoco debe decidir solo.

```text
H2H estadio antiguo
+ H2H general reciente contrario
+ previas/indirectas actuales contrarias
= baja a confirmacion secundaria
```

Ejemplos:

- Fnjaa vs Al-Msnaa: estadio 5-0, pero general 1-1 + Col3 1-1 + termina 0-0.
- CD Ebro vs Tudelano: estadio 2-1 antiguo, pero general 2-2 + tabla dog + termina 2-2.
- Wrexham vs Preston: estadio 2010 no decide; decide el mercado actual + previa 5-3.

### 8. Rebaja extrema no es favorito gratis

Cuando un precedente cubrio una linea alta y hoy la casa baja a `0.25`, no hay
que leerlo como regalo. La pregunta es por que ya no exige 1 o mas goles.

```text
H2H estadio cubrio H1_PLUS
pero AH actual = 0.25
= cambio de problema
```

Si hay empate en H2H general o Col3, se bloquea favorito.

Ejemplo:

- Fnjaa vs Al-Msnaa: 5-0 historico con linea 1.0, pero hoy 0.25 y termina 0-0.

Si la estructura actual confirma al favorito, si puede valer:

- Kidderminster vs Telford: estadio 3-0 + tabla + localia actual.

### 9. Antiguo favorito con volumen pero sin premio

Este patron aparece varias veces. Un equipo puede dominar tiros en el H2H, pero
si no gano/cubrio, el handicap dice que fallo su obligacion.

```text
Rival tuvo volumen en H2H
pero no gano/cubrio
y el mercado gira al otro lado
= no arrastrar stats antiguas como dog automatico
```

Ejemplos:

- Wrexham vs Preston: Preston domino el 1-1, pero el mercado giro a Wrexham.
- Santo Andre vs Gremio Prudente: Gremio produjo mas en 0-0, pero no gano y luego pierde el favoritismo.
- Ports Authority vs Kallon: Ports habia sufrido H2H malos, pero el mercado no mantuvo a Kallon.

### 10. Over con linea baja si el mapa trae ruptura

`OU <= 2.25` suele empujar a under, pero no siempre. Si la linea es baja y el
mapa trae resultados 3-1, 4-0, 5-3, 2-2 o juvenil/reserves, el over puede ser
la salida correcta aunque el AH sea prudente.

Ejemplos:

- CD Ebro vs Tudelano: OU 1.75 y termina 2-2.
- Wrexham vs Preston: previa 5-3 y termina 2-1.
- Kadhimiya vs Al Hussein: OU 2 y termina 3-1.
- Pro Vercelli U20 vs Venezia Youth: juvenil, termina 4-0.

### 11. Over contraintuitivo con OU alto

Este patron sale muy claro en Orlando City B vs Columbus Crew B.

El error seria pensar:

```text
H2H 2-1 = 3 goles
OU actual 3.5
=> UNDER automatico
```

La lectura correcta es:

```text
Los H2H se quedaron solo medio gol por debajo de la linea.
La casa mantiene 3.5 igualmente.
El local viene de perder 1-2, pero con 28 tiros y 10 a puerta.
El visitante viene de ganar/cubrir fuera con volumen.
La indirecta local fue 1-4.
=> bloquear UNDER; OVER posible.
```

Patron replicable:

```text
OU >= 3.5
+ H2H de 3 goles
+ favorito/local con resultado peor que proceso ofensivo
+ dog visitante cubriendo fuera
+ indirecta local rota
= DOG AH + OVER/NO BET OU
```

### 12. Favorito mantenido tras H2H perdido con proceso igualado

Este patron sale en Miami AC vs Brevard SC y ya no pertenece solo a AH 0.25:
es universal para handicaps bajos, especialmente AH 0.5 / AH 0.75.

El error inicial fue leer:

```text
 local nunca gano al rival
+ H2H directo 2-3
+ visitante gano 1-4 al rival comun
= X2 / dog
```

La lectura corregida es:

```text
H2H 2-3, pero proceso totalmente igualado:
tiros 12-10, tiros a puerta 5-5, ataques 85-85, peligrosos 44-44.
La casa mantiene el mismo AH 0.5 al local/favorito.
Si quisiera aceptar el X2 obvio, bajaria a 0.25, 0 o giraria favorito.
No lo hace.
= rechazo del X2 publico.
```

Patron replicable:

```text
F pierde H2H directo
+ misma linea se mantiene
+ mismo favorito se mantiene
+ H2H tuvo proceso igualado, no dominio del dog
+ dog trae relato publico fuerte por rival comun
+ OU >= 3.0 y H2H directo fue abierto
= FAVORITO AH + OVER
```

Rama aplicada en el motor:

- `U19 MARKET_REJECTS_OBVIOUS_DOG_X2`
- `U19_OVER_REVANCHA_ABIERTA`

### 13. Rebaja brutal desde goleada antigua: dog protegido y under

Este patron sale en Helsinki B (W) vs PK Keski Uusimaa (W). No es AH 0.25,
pero es clave para que la logica sea universal en todos los handicaps.

El fallo fue leer:

```text
H2H estadio 4-1
+ misma pareja de equipos
+ local viene de 4-1
+ OU 4.0
= OVER / favorito
```

La lectura corregida es:

```text
El H2H 4-1 venia con AH historico 3.00.
La linea actual baja a AH 1.25.
Aunque el marcador antiguo cubriria el AH actual, la casa ya no exige
ese margen enorme.
Esa rebaja es informacion de mercado: protege al visitante.
OU 4.0 queda inflado por memoria de goleada antigua.
Col3 1-1 con handicap 1 no cubierto enfria ruptura.
= visitante +1.25 y UNDER/NO BET OVER.
```

Resultado real conocido: 1-0. El patron corrigio dos errores:

- no perseguir la goleada antigua;
- no convertir una linea de goles extrema en over automatico.

Rama aplicada en el motor:

- `U20 HUGE_DROP_PROTECTS_DOG`
- `OU_INFLADO_POR_GOLEADA_ANTIGUA`
- `COL3_ENFRIA_OU_EXTREMO`

### 14. H2H 3-3 antiguo con OU capado: no perseguir el over

Caso modelo: CA Lugano Reserves vs Defensores de Cambaceres Reserves,
AH 1, OU 2.75, resultado real 0-0.

El fallo humano fue leer:

```text
H2H directo 3-3
+ previa visitante 2-2
+ indirecta local 4-0
= OVER
```

La lectura correcta era:

```text
H2H 3-3, pero con AH actual 1 el favorito NO cubria.
La casa sube/mantiene exigencia al favorito, pero no sube el OU:
lo deja en 2.75 pese a una memoria de 6 goles.
Previa local 1-1 con AH 0: sin victoria, baja eficacia.
Previa visitante 2-2 con AH 0: tampoco separa ganador.
El proceso del H2H estaba partido: Cambaceres mas tiros/SOT,
Lugano mas ataques peligrosos.
= dog +1 y UNDER/NO BET OVER.
```

Rama aplicada en el motor:

- `U21 H2H_OVER_ANTIGUO_OU_CAPADO`
- `DOBLE_PUSH_RECIENTE`
- `U21_EMPATE_CONGELADO_UNDER`

### 15. AH 0 validado por dog que gano el H2H reciente

Caso modelo: Virginia United SC (W) vs Logan Lightning (W),
AH 0, OU 3.75, resultado real 2-0.

El fallo de diseno era tratar AH 0 como neutralidad simple:

```text
AH 0
+ local viene de 0-4
+ visitante viene de 9-0 encajado
+ H2H estadio viejo 3-4
= no bet / over posible
```

La lectura correcta era:

```text
El ultimo H2H general fue Logan 0-1 Virginia.
Virginia era el equipo con rol inferior/dog y aun asi gano fuera.
Ahora vuelve a casa y el mercado no la castiga: la deja en AH 0.
Eso no es neutralidad vacia: es validacion DNB.
```

El under venia por otra via:

```text
OU 3.75 esta inflado por 0-4, 9-0 y H2H estadio 3-4.
Pero el H2H reciente fue 0-1.
Virginia y Logan llegan con 0 goles en sus referencias fuertes.
Contra North Lakes, Virginia pierde 3-0 con AH 3 push;
Logan pierde 9-0 con AH 3 no cubierto.
El rival comun fuerte valida mas a Virginia que a Logan.
= Virginia 0 / DNB + UNDER.
```

Rama aplicada en el motor:

- `U22 PICKEM_DOG_WIN_TO_HOME_DNB`
- `OU_ALTO_INFLADO_POR_GOLEADAS_AJENAS`
- `ATAQUE_CERO_BILATERAL`

Ya se ha anadido a `CLAVE_DICOTOMICA_V3_EJECUTABLE.md`:

- `NODO 0 — JERARQUIA UNIVERSAL DE LECTURA`
- regla de variable dominante
- resultado + handicap por encima de estadisticas
- cambio de favorito por mercado en AH bajo
- repeticion de fallo en estadio
- AH bajo + OU bajo como riesgo estructural de empate
- leyes especiales `H025-1` a `H025-9`
- rama universal `U19` para favorito mantenido tras H2H fail de proceso igualado
- rama universal `U20` para rebaja brutal desde goleada antigua: dog protegido + under
- rama universal `U21` para H2H over antiguo capado por OU y doble push reciente
- rama universal `U22` para AH 0 validado por dog que gano el H2H reciente

## Checklist para auditar cada nuevo AH 0.25

```text
1. Quien era favorito en el ultimo H2H general?
2. Quien es favorito ahora?
3. El mercado mantiene, rebaja o invierte favorito?
4. El H2H general cubre/falla la linea actual desde el favorito actual?
5. El H2H estadio confirma o contradice?
6. Hay empate/push repetido?
7. OU <= 2.25 aumenta riesgo de empate?
8. La previa local cubrio como favorito o solo como dog/pickem?
9. La previa visitante cubrio como dog o fallo como favorito?
10. Las indirectas dan margen real o solo stats?
11. Si OU >= 3.5, la casa mantiene linea alta pese a H2H de 3 goles?
12. Hay volumen ofensivo oculto o resultado peor que proceso?
13. Hay una sola variable dominante de nivel alto?
14. Si el dog parece obvio, la casa bajo/quita favorito o mantiene linea contra ese relato?
15. Si no hay variable dominante: NO BET.
```

## Conclusion operativa

La clave debe ser universal para todos los handicaps, pero en `AH 0.25` hay una
ley especial: el favorito no necesita dominar, solo ganar. Por eso el sistema
no debe preguntar "quien fue mejor en stats", sino:

```text
Que equipo esta obligado hoy a ganar por la linea,
que hizo esa misma obligacion en el H2H,
y por que el mercado mantiene, baja o invierte esa obligacion?
```
