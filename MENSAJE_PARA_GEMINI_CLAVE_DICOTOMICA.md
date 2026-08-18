# Mensaje para Gemini - contexto de la clave dicotomica

Gemini, dejo aqui el contexto exacto de la revision hecha sobre la clave dicotomica.

## Estado real actualizado

La mejora **ya esta aplicada al motor principal de precacheo**.

Se han tocado las piezas que calculan y muestran la clave:

- `src/modules/clave_dicotomica.py`
- `src/templates/precacheo.html`
- `src/app.py`
- `data/motor_clave_dicotomica.py` queda sincronizado al menos en la correccion critica de AH=0.
- `data/picks_clave_dicotomica.json` fue regenerado usando el motor nuevo.

Antes de esta aplicacion, los cambios estaban solo en documentacion operativa, auditoria y marco metodologico. Ahora el backend de `precacheo` inyecta campos nuevos y la tarjeta visual los muestra.

Archivos creados o modificados para darte contexto:

- `CLAVE_DICOTOMICA_V3_EJECUTABLE.md`
- `INFORME_AUDITORIA_AH025_PARTIDOS_USUARIO.md`
- `MARCO_UNIVERSAL_CLAVE_DICOTOMICA.md`
- `REVISION_CLAVE_DICOTOMICA_GEMINI.md`

Lee primero `MARCO_UNIVERSAL_CLAVE_DICOTOMICA.md`, despues `CLAVE_DICOTOMICA_V3_EJECUTABLE.md`, y luego el informe de auditoria AH025.

## Cambio conceptual principal

La clave debe funcionar como un sistema universal de variables, no como una lista rigida de reglas sueltas.

La jerarquia no se cambia:

1. Movimiento de cuota / handicap en el partido previo directo entre ellos.
2. Ultimo H2H en este estadio, si existe.
3. H2H general.
4. Previo local y previo visitante.
5. Comparativas indirectas.
6. Col3 espejo.
7. Posiciones, forma, estadisticas y volumen.

Pero dentro de esa jerarquia hay una correccion importante:

**resultado + handicap pesa mas que estadisticas brutas.**

Las estadisticas sirven para explicar proceso, dominio o ruido, pero no deben mandar por encima de cobertura, margen, repeticion de mercado o fallo/cobertura ante lineas equivalentes.

## Correcciones importantes

### 1. AH = 0 no es favorito visitante

Antes habia una regla erronea:

`AH = 0 -> visitante favorito`

Eso no debe seguir asi. AH 0 es:

`PICKEM / DNB / sin favorito claro`

Si el motor necesita favorito real en AH 0, debe deducirlo por cuota, no por local/visitante.

Aplicado en codigo:

- `src/modules/clave_dicotomica.py`: `AH=0` activa `PICKEM_DNB`.
- En ese caso el local solo se usa como referencia matematica para medir residuales.
- La salida se etiqueta como `AH 0 / DNB` o `NO BET AH (PICKEM/DNB)`, no como favorito visitante.
- `data/motor_clave_dicotomica.py` tambien fue corregido para no volver a generar visitante favorito por defecto.

### 2. Formula universal para cualquier handicap

Para cualquier handicap actual:

`h = abs(AH_actual)`

Se debe leer el residual:

`RH = margen del favorito en el H2H / referencia - h`

Interpretacion:

- `RH >= +0.75`: senal fuerte del favorito.
- `RH entre +0.25 y +0.5`: senal favorable pero no definitiva.
- `RH entre -0.25 y +0.25`: zona de equilibrio, empate o no bet.
- `RH <= -0.5`: senal contraria al favorito o riesgo dog.

La misma historia no vale igual en todos los handicaps. Un 1-0 puede ser muy fuerte en AH 0.25, normal en AH 0.75 y debil en AH 1.25.

### 3. Una sola variable puede decantar el sistema

El sistema debe permitir que una variable dominante gane si cumple estas condiciones:

- Es jerarquicamente alta.
- Es reciente o muy especifica del contexto.
- Se repite con el handicap actual o con una linea comparable.
- El resultado/cobertura contradice claramente al resto.

Ejemplo: si todas las estadisticas favorecen a un lado, pero el mercado baja la exigencia al favorito y el resultado directo reciente no confirma, esa variable puede bloquear el pick favorito.

### 4. Separar AH y Over/Under

No se puede convertir automaticamente:

`dog cubre -> under`

ni:

`favorito gana -> over`

El AH responde a margen y cobertura. El O/U responde a estructura de goles, linea actual, ritmo, volumen y contradiccion entre mercado y resultados previos.

## Regla nueva clave: over contraintuitivo

Caso modelo: `Orlando City B vs Columbus Crew B`, AH 0.25, linea goles 3.5, resultado real 2-2.

Lectura importante:

- H2H estadio: 2-1, total 3.
- H2H general: 2-1, total 3.
- Linea actual: 3.5.
- La linea queda mas alta que los resultados historicos directos, pese a que esos H2H no superaron 3.5.
- Local viene de 1-2 con muchisimo volumen: 28 tiros, 10 a puerta.
- Visitante viene de ganar fuera 0-1 siendo competitivo y despues empata 1-1 contra el mismo rival que gano al local.
- Hay dudas fuertes en handicap, pero no en potencial de goles.

Conclusion:

Cuando la linea de goles se mantiene alta o sube pese a H2H directos de 3 goles, eso no debe tratarse como under automatico. Puede ser un **OVER contraintuitivo** si hay volumen ofensivo reciente, marcador espejo, fragilidad local y visitante que gana/cubre como favorito o como visitante fuerte.

Implementacion recomendada:

Crear bandera:

`OU_HIGH_COUNTERINTUITIVE = true`

Se activa si:

- `OU_actual >= 3.25`
- los H2H directos recientes quedan a 0.5 goles o menos de la linea, pero no la superan
- el mercado no baja la linea
- hay volumen ofensivo reciente o fragilidad defensiva clara
- el handicap esta dudoso o en zona baja, especialmente AH 0.25

Efecto:

- Bloquear `UNDER` automatico.
- Si hay 2 confirmadores: `NO BET UNDER`.
- Si hay 3 o mas confirmadores: permitir `OVER` aunque parezca contraintuitivo.

Aplicado en codigo como:

- Flag `OU_HIGH_COUNTERINTUITIVE`.
- Campo `u18_over_counterintuitive`.
- Campo `over_counter_confirmers`.
- Micro-rama `H025-9 OU_ALTO_CONTRAINTUITIVO`.
- Si el motor queria `UNDER` pero esta bandera esta activa, se bloquea y pasa a `NO BET OU (UNDER bloqueado por OU alto contraintuitivo)`.

## Reglas AH025 nuevas que hay que trasladar al motor

La auditoria AH 0.25 genero estas familias:

- `H025-1`: visitante cubre en H2H general + local no compensa.
- `H025-2`: favorito local con H2H estadio positivo y visitante debil fuera.
- `H025-3`: empate/under cuando la linea baja y ambos tienen coberturas parciales.
- `H025-4`: favorito falso por estadisticas, si resultado y handicap no lo sostienen.
- `H025-5`: movimiento 0 -> 0.25 en estadio no siempre es favorito; depende del residual.
- `H025-6`: H2H estadio antiguo solo es memoria, no sentencia.
- `H025-7`: rebaja extrema desde handicap alto a 0.25 no es apoyo libre al favorito; puede ser aviso de igualdad.
- `H025-8`: antiguo favorito con volumen pero sin premio no debe pesar igual que favorito que cubre.
- `H025-9`: dog + over contraintuitivo con OU alto.

No hay que meterlas como reglas rigidas que disparen siempre. Hay que convertirlas en banderas ponderadas que respeten la jerarquia.

## Implementacion sugerida en precacheo

1. `src/modules/clave_dicotomica.py` ya queda corregido:
   - AH 0 = pickem.
   - No fuerza visitante como favorito.
   - Anade residual de estadio `stadium_RH`.
   - Separa score AH y score O/U.
   - Anade hooks de aprendizaje `learning_hooks`.

2. `src/templates/precacheo.html` ya muestra:
   - label operativo real de la clave.
   - badge `PICKEM` si procede.
   - primeras ramas activas de `learning_hooks`.
   - filtro de estrategia `U18 Over Contraintuitivo` para encontrar partidos similares al caso Orlando.

3. Validaciones hechas:
   - Orlando City B vs Columbus Crew B -> `Columbus Crew B +0.25` y `OVER 3.5`, con `OU_HIGH_COUNTERINTUITIVE`.
   - AH 0 -> `PICKEM_DNB`, nunca visitante favorito por defecto.
   - Miami AC vs Brevard SC -> `Miami AC AH 0.5` y `OVER 3.25`, con `MARKET_REJECTS_OBVIOUS_DOG_X2`.
   - Helsinki B (W) vs PK Keski Uusimaa (W) -> `PK Keski Uusimaa (W) +1.25` y `UNDER 4.0`, con `HUGE_DROP_PROTECTS_DOG`.
   - CA Lugano Reserves vs Defensores de Cambaceres Reserves -> `Defensores de Cambaceres Reserves +1.00` y `UNDER 2.75`, con `H2H_OVER_ANTIGUO_OU_CAPADO`.
   - Virginia United SC (W) vs Logan Lightning (W) -> `Virginia United SC (W) AH 0 / DNB` y `UNDER 3.75`, con `PICKEM_DOG_WIN_TO_HOME_DNB`.

4. Siguiente mejora pendiente:
   - Ampliar tests con mas casos AH 0.25 del informe para validar favorito, dog, empate y no bet.
   - Cuando entren nuevos partidos analizados, convertir solo los patrones repetibles en nuevas ramas ponderadas.

## Nueva rama U19: rechazo del X2 obvio

Caso modelo: `Miami AC vs Brevard SC`, AH 0.5, OU 3.25, resultado real 3-2.

La lectura superficial daba X2:

- Miami no habia ganado al rival.
- H2H directo anterior: Miami AC 2-3 Brevard SC.
- Brevard venia de ganar 1-4 al rival comun.
- Miami venia de 1-1 contra ese mismo rival.

Pero la variable dominante era otra:

- El H2H 2-3 tuvo proceso totalmente igualado: tiros 12-10, tiros a puerta 5-5, ataques 85-85, peligrosos 44-44.
- La casa mantuvo el mismo AH 0.5 al local/favorito.
- Si la casa aceptara el X2 publico, tendria sentido bajar a 0.25, 0 o girar el favoritismo.
- Al no hacerlo, el mercado rechaza el relato obvio del dog.

Rama aplicada:

- `MARKET_REJECTS_OBVIOUS_DOG_X2`
- `U19_OVER_REVANCHA_ABIERTA`
- Campo `u19_market_rejects_obvious_dog_x2`

Condiciones principales:

- favorito pierde H2H directo;
- se mantiene el mismo favorito y la misma linea;
- el H2H perdido fue de proceso igualado, no dominio del dog;
- el dog trae relato publico fuerte por rival comun o previa;
- OU >= 3.0 y H2H directo abierto.

Efecto:

- rescata favorito/local frente al X2 obvio;
- desbloquea OVER si el mapa directo fue abierto.

## Nueva rama U20: rebaja brutal desde goleada antigua

Caso modelo: `Helsinki B (W) vs PK Keski Uusimaa (W)`, AH 1.25, OU 4.0, resultado real 1-0.

La lectura superficial daba favorito/over:

- H2H estadio anterior: Helsinki B 4-1 PK Keski.
- El local venia de ganar 4-1 a TPS Turku.
- La linea de goles era 4.0.

Pero la variable dominante era el movimiento de handicap:

- El H2H 4-1 tenia AH historico 3.00.
- La linea actual baja a AH 1.25.
- Si la casa esperara repetir una goleada parecida, no rebajaria tanto la exigencia de margen.
- La rebaja brutal protege al visitante y convierte el 4-1 antiguo en memoria peligrosa, no en proyeccion.
- El Col3 fue 1-1 con handicap 1 no cubierto, lo que enfria el over extremo.

Rama aplicada:

- `HUGE_DROP_PROTECTS_DOG`
- `OU_INFLADO_POR_GOLEADA_ANTIGUA`
- `COL3_ENFRIA_OU_EXTREMO`
- Campo `u20_huge_drop_protects_dog_under`

Condiciones principales:

- H2H antiguo con AH alto o extremo (`>= 1.75`, especialmente `>= 2.25`);
- AH actual claramente menor (`delta <= -1.0`);
- el marcador antiguo cubria por goleada, pero la casa ya no pide ese margen;
- OU actual extremo (`>= 3.75`);
- Col3, rival comun o produccion reciente no sostienen otra ruptura.

Efecto:

- empuja a dog AH;
- bloquea over automatico por memoria de goleada;
- permite under aunque el H2H bruto haya tenido 5 goles.

## Nueva rama U21: H2H over antiguo capado por OU

Caso modelo: `CA Lugano Reserves vs Defensores de Cambaceres Reserves`, AH 1, OU 2.75, resultado real 0-0.

La lectura superficial daba over:

- H2H directo anterior: 3-3.
- Defensores venia de 2-2.
- Lugano tenia una indirecta 4-0 contra Central Ballester.

Pero la variable dominante era el capado de linea:

- Con AH actual 1, el H2H 3-3 era fallo claro del favorito.
- La casa pide margen al favorito, pero no acompana con OU alto: deja 2.75 pese a memoria de 6 goles.
- Las dos previas recientes no separan ganador: Lugano 1-1 con AH 0 y Defensores 2-2 con AH 0.
- El proceso del H2H estaba partido: Defensores mas tiros/SOT; Lugano mas ataques peligrosos.
- La indirecta 4-0 era una variable fuerte, pero no podia superar la jerarquia H2H + linea + previas recientes.

Rama aplicada:

- `H2H_OVER_ANTIGUO_OU_CAPADO`
- `DOBLE_PUSH_RECIENTE`
- `U21_EMPATE_CONGELADO_UNDER`
- Campo `u21_h2h_over_capped_draw_under`

Condiciones principales:

- H2H directo empatado y alto (`TH >= 5`);
- OU actual capado respecto a ese H2H (`OU <= 2.75`);
- AH actual exige margen al favorito (`h >= 0.75`) aunque el H2H no lo cubria;
- ambas previas recientes no separan ganador;
- proceso H2H no fue dominio limpio del favorito.

Efecto:

- favorece dog AH;
- bloquea over por memoria antigua;
- empuja under aunque el H2H bruto haya sido 3-3.

## Nueva rama U22: AH 0 validado por dog que gano el H2H reciente

Caso modelo: `Virginia United SC (W) vs Logan Lightning (W)`, AH 0, OU 3.75, resultado real 2-0.

La lectura superficial dejaba el partido sin decision:

- AH 0 parecia neutral.
- Virginia venia de perder 0-4.
- Logan venia de perder 9-0.
- H2H estadio viejo: 3-4, memoria over.

Pero la variable dominante era el ultimo H2H general:

- Logan 0-1 Virginia.
- Virginia gano fuera siendo el equipo de rol inferior/dog.
- Ahora Virginia vuelve a casa y el mercado la deja en AH 0.
- Eso no es neutralidad vacia: es validacion DNB del antiguo dog ganador.

El OU alto se interpreta aparte:

- OU 3.75 estaba inflado por goleadas externas (`0-4`, `9-0`) y por H2H estadio antiguo `3-4`.
- El H2H reciente fue solo de 1 gol.
- Ambos ataques llegan con 0 goles en referencias fuertes.
- Contra North Lakes, Virginia perdio 3-0 con AH 3 push; Logan perdio 9-0 con AH 3 no cubierto.

Rama aplicada:

- `PICKEM_DOG_WIN_TO_HOME_DNB`
- `OU_ALTO_INFLADO_POR_GOLEADAS_AJENAS`
- `ATAQUE_CERO_BILATERAL`
- Campo `u22_pickem_dog_win_home_dnb_under`

Condiciones principales:

- AH actual 0;
- el local actual gano el ultimo H2H general siendo dog/rol inferior;
- el H2H reciente fue corto (`0-1` o `1-0`);
- el OU actual es alto (`>= 3.5`);
- las goleadas recientes son externas y no prueban ataque propio;
- existe rival comun fuerte que castiga mucho mas al rival actual.

Efecto:

- convierte AH 0 en `LOCAL DNB`;
- bloquea over por memoria de goleadas;
- empuja under.

4. No cambiar la jerarquia.

5. No permitir que estadisticas brutas ganen solas si resultado + handicap dicen lo contrario.

## Resumen corto para no perder el norte

La clave debe leer primero mercado y resultado. Las estadisticas explican, pero no gobiernan. El handicap mide margen; el over/under mide goles. Una sola variable puede decantar el sistema si es jerarquicamente alta, reciente, comparable por linea y contradice claramente al resto.
