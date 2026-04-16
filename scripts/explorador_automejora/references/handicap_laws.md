# Leyes de Handicap

Reglas practicas para leer partidos con el handicap como variable madre.

## Ley 1. Repricing del H2H

Si el mismo H2H en el mismo estadio cubrio una linea mas alta que la actual y el signo del favorito se mantiene, la bajada de linea favorece al mismo lado historico.

- Trigger:
  - mismo estadio
  - mismo lado favorito
  - `hist_ah_abs - current_ah_abs >= 0.50`
  - el precedente cubrio la linea actual reexpresada
- Lectura:
  - el mercado esta pidiendo menos al mismo equipo
- Accion:
  - sesgo hacia el favorito actual
  - si el riesgo de empate es alto, bajar de `-0.25` a `0`

## Ley 2. La Tabla No Manda Contra La Linea

Si un equipo peor posicionado sale favorito, el mercado esta descontando fuerza local, matchup o contexto que la tabla no recoge.

- Trigger:
  - local peor rankeado
  - aun asi `AH > 0`
- Lectura:
  - no se debe ir contra la linea solo por clasificacion
- Accion:
  - usar la tabla solo como filtro secundario

## Ley 3. Residual Reciente del Visitante

La previa fuera del visitante pesa mucho cuando el residual es claramente negativo.

- Formula:
  - `residual_away = margen_visitante - line_team_visitante`
- Trigger:
  - `residual_away <= -1.0`
- Lectura:
  - el visitante no compitio contra la expectativa del mercado
- Accion:
  - refuerzo al lado local, salvo que el H2H y las indirectas digan lo contrario

## Ley 4. Indirecta Empatada No Decide

Si local y visitante dejan un residual parecido contra un rival comun, esa indirecta no debe decidir el pick.

- Trigger:
  - ambos tienen rival comun
  - `abs(residual_local_ind - residual_visitante_ind) <= 0.25`
- Lectura:
  - bloque neutral
- Accion:
  - mandar el peso a H2H, movimiento de linea y draw risk

## Ley 5. El 0.25 Con OU Bajo Se Baja a DNB

Cuando el edge esta del lado del favorito, pero la linea es `0.25` y el riesgo de empate es alto, la mejor traduccion operativa suele ser `0`.

- Trigger:
  - pick inicial `favorito -0.25`
  - `draw_risk >= 0.55`
- Lectura:
  - el modelo ve ventaja, pero no ventaja suficiente para regalar media perdida en empate
- Accion:
  - bajar a `favorito 0`

## Ley 6. La Mejora Frente Al Espejo Tiene Valor Solo Si No La Desmiente El Mercado

Superar al equipo espejo en `H2H Col3` es una buena señal, pero necesita coherencia con la linea actual.

- Trigger:
  - `MEJORA Directa` o equivalente
  - el mercado no invierte favoritismo
- Lectura:
  - el favorito actual esta rindiendo mejor que el espejo historico
- Accion:
  - sumar a favor del favorito
  - no usarla sola para saltar de `0` a `-0.5`

## Ley 7. Mejora Col3 Sin Superioridad De Volumen = Falsa Mejora

Si el favorito "mejora al espejo" en `H2H Col3`, pero en su partido de referencia produce mucho menos volumen que su rival, esa mejora es fragil.

- Trigger:
  - etiqueta `MEJORA Directa` o similar
  - tiros y/o tiros a puerta claramente inferiores
  - ataques peligrosos por debajo o partido muy igualado
- Lectura:
  - la mejora existe en marcador-espejo, pero no en calidad competitiva
- Accion:
  - rebajar confianza al favorito
  - en `-0.25`, preferir `0` o incluso el no favorito `+0.25` si hay mas castigos recientes

## Ley 8. Misma Derrota, Mejor Volumen = Mejor Lectura

Si local y visitante pierden contra el mismo rival o contra rivales espejo por marcador parecido, no se deben igualar esas derrotas. Gana valor el equipo que perdio con mejor volumen.

- Trigger:
  - ambos comparables por rival comun o bloque espejo
  - marcador parecido
  - un equipo genero claramente mas tiros, mas ataques o mas ataques peligrosos
- Lectura:
  - perder `4:2` no vale lo mismo si uno fue superado y el otro compitio de verdad
- Accion:
  - favorecer al equipo con mejor volumen relativo
  - si ese equipo es el no favorito, subir su opcion de cover AH

## Ley 9. Favorito Corto Sin Dominio Reciente No Merece -0.25

Cuando el favorito sale en `0.25`, pero su previa equivalente deja residual neutro o malo, no alcanza para respaldar la media linea en contra del empate.

- Trigger:
  - `AH actual = 0.25`
  - residual reciente del favorito `<= 0`
  - el soporte fuerte viene de un H2H antiguo o aislado
- Lectura:
  - el mercado da favoritismo, pero la forma no sostiene esa exigencia
- Accion:
  - evitar `favorito -0.25`
  - preferir `favorito 0` o `no favorito +0.25` segun el resto de bloques

## Ley 10. El H2H Invertido Se Reexpresa Desde El Equipo Actual

Si el `H2H general` que usamos como precedente tiene a los equipos con localias invertidas respecto al partido actual, no se puede puntuar el marcador ni las stats desde la columna `home` historica.

- Trigger:
  - hay `H2H general`
  - el local actual aparece como visitante en ese H2H
- Lectura:
  - un `0:1` fuera puede ser una señal pro-local actual, no una derrota del local actual
  - un bloque de tiros `4 vs 25` puede estar al reves si no se reorienta
- Accion:
  - invertir el margen del marcador a perspectiva del equipo actual
  - orientar tiros y ataques a la columna del equipo actual antes de sumar soporte
  - si el pick fuerte dependia de esa mala orientacion, bajarlo a `NO BET` o reabrir el caso

## Caso: Fortaleza F.C vs Deportivo Pasto (2026-03-25)

Resumen del caso:

- AH actual: `Fortaleza -0.25`
- OU actual: `2.0`
- H2H estadio/general: `Fortaleza 2:0 Pasto` con `0.75`, hoy baja a `0.25`
- Prev home: `Fortaleza 2:1 Jaguares` con `1.0` => residual `0`
- Prev away: `Once Caldas 4:2 Pasto` con `0.5` => residual visitante `-1.5`
- Ind local: `Once Caldas 4:2 Fortaleza` con `0.5` => residual local `-1.5`

Leyes activadas:

1. `Ley 1`: H2H antiguo cubrio una linea mas dura que la actual.
2. `Ley 3`: Pasto llega con residual visitante muy malo.
3. `Ley 4`: la indirecta contra `Once Caldas` no separa, porque ambos fueron castigados igual.
4. `Ley 5`: como la linea es corta y el riesgo de empate es alto, la salida prudente es `Fortaleza 0`.

Traduccion operativa:

- Pick base: `Fortaleza 0`
- Pick agresivo: `Fortaleza -0.25`
- Si sube el AH a `0.5` sin nueva informacion, el valor baja mucho.

## Caso: Fortaleza F.C vs Deportivo Pasto (revision tras resultado real 1:2)

Correccion posterior:

- El resultado real fue `1:2`.
- El lado correcto en handicap fue el no favorito: `Deportivo Pasto +0.25`.
- La lectura inicial pro-local quedo demasiado apoyada en el H2H `2:0` con `0.75`.

Que ensena este caso:

1. `Ley 7`: la `MEJORA Directa` del favorito no bastaba.
   - Fortaleza mejoraba al espejo en el relato, pero no tenia dominio de volumen claro en su comparativa dura.
2. `Ley 8`: la derrota `4:2` de Pasto ante `Once Caldas` fue mejor que la derrota `4:2` de Fortaleza ante ese mismo rival.
   - Pasto tiro mas (`21 vs 12`) y sostuvo mejor el intercambio.
   - Fortaleza quedo claramente peor en tiros y ataques peligrosos.
3. `Ley 9`: con `AH 0.25`, residual reciente del local igual a `0`, y partido de goles bajos (`OU 2`), no habia base robusta para comprar `Fortaleza -0.25`.

Traduccion corregida:

- Pick AH realista: `Deportivo Pasto +0.25`
- Pick conservador alterno: `NO BET` o `Fortaleza 0` solo si el resto del mercado no castiga mas al local
- Conclusion:
  - en favoritos cortos, una mejora visual de espejo no puede pesar mas que una comparativa reciente de volumen claramente inferior

## Caso: Yirga Chefe Bunaa (W) vs Addis Ababa Ketema (W) (2026-03-26)

Resumen del caso:

- AH actual: `Yirga Chefe Bunaa (W) -0.25`
- OU actual: `2.0`
- H2H general: `Addis Ababa Ketema (W) 0:1 Yirga Chefe Bunaa (W)` con la misma linea `0.25`
- Prev home: `Yirga 1:4 Mechal` con `-1.5`
- Prev away: `Arba Minch 0:2 Addis` con `0.25`
- Ind local: `Yirga 1:1 Arba Minch` con `0`
- Ind visitante: `Addis 0:2 Mechal` con `-2`

Que ocurria antes de la correccion:

- El razonador matematico leia el `0:1` del H2H como si fuera una derrota del local actual.
- Tambien asignaba al local actual las stats del `home` historico (`4` tiros, `1` a puerta) en lugar de las del `away` historico (`25` tiros, `14` a puerta).
- Eso empujaba el edge hasta aprox. `-3.65` y terminaba en una lectura tipo `Addis +0.25`.

Que cambia al reorientar bien el H2H:

- El margen del H2H pasa a favor del local actual.
- El soporte estadistico del H2H tambien pasa a favor del local actual.
- El edge total se comprime hasta aprox. `-0.04`.
- La lectura deja de ser pick visitante y pasa a un caso practicamente `NO BET`.

Que enseña este caso:

1. Antes de crear una ley nueva, hay que asegurar que el `H2H general` esta orientado desde el equipo actual.
2. Corregir la perspectiva elimina un falso pick fuerte, aunque no basta todavia para convertir el partido en pick local.
3. El siguiente ciclo de mejora debe decidir dos cosas:
   - cuanto premio merece un `empate` con aplastamiento de volumen (`14-1` tiros, `8-1` a puerta, `116-62` ataques peligrosos)
   - cuanto debe pesar `H2H Col3` cuando la etiqueta es `INVERSA IGUALA`

## Ley 11. Repeticion Del Handicap Del Visitante Con Memoria Favorable Del Local

Si el handicap actual repite exactamente el ultimo handicap fuera del visitante, pero el local ya habia ganado ese mismo matchup en la misma linea absoluta como no favorito, la repeticion visitante no manda por si sola.

- Trigger:
  - `AH actual = last_away_match.handicap_line_raw`
  - el visitante viene de ganar o cubrir en esa linea repetida
  - el local ya gano al visitante con la misma linea absoluta
  - en ese H2H el local actual era visitante o no favorito
- Lectura:
  - el mercado esta reciclando una linea visitante que ya fue resuelta por el local en condiciones incluso mas duras
  - la memoria del matchup pesa mas que la repeticion ciega del ultimo partido del visitante
- Accion:
  - no saltar automaticamente a `no favorito +0.25`
  - mantener sesgo local si otros bloques no lo contradicen fuerte
  - en `0.25`, preferir `local 0` como base y `local -0.25` si hay apoyo extra de volumen

## Ley 12. Empate Con Superioridad Oculta No Es Bloque Neutro

Si el local empata frente al rival del visitante, pero domina de forma muy clara en tiros o ataques peligrosos, ese empate no debe leerse como bloque neutral.

- Trigger:
  - marcador `draw`
  - y ademas una de estas:
    - diferencia de tiros `>= 8`
    - diferencia de tiros a puerta `>= 5`
    - diferencia de ataques peligrosos `>= 35`
- Lectura:
  - el marcador corto esconde superioridad real
  - el equipo compitio por encima de lo que dice el resultado bruto
- Accion:
  - reclasificar el bloque hacia el lado dominante
  - si el dominante es el local y la linea actual es `0` o `0.25`, subir su opcion de `0` o `-0.25`

## Ley 13. Favorito De Linea Entera Con H2H Anti-Margen Se Baja Un Cuarto

Si el favorito actual sale en `0.75` o `1`, pero los H2H disponibles no sostienen esa exigencia, no hay triangulacion util y el perfil de goles es corto, la lectura correcta es victoria del favorito sin comprar el entero.

- Trigger:
  - `abs(AH actual) >= 0.75`
  - `OU actual <= 2.75`
  - `H2H estadio` no cubre la linea actual reexpresada
  - `H2H general` no cubre la linea actual reexpresada
  - no hay `H2H Col3` ni indirectas utiles
  - el residual reciente del favorito es `<= 0.25`
- Lectura:
  - la casa mantiene superioridad actual del favorito, pero no esta prometiendo margen amplio
  - la linea entera funciona como ancla de jerarquia, no como orden limpia de ganar por dos o mas
- Accion:
  - mantener el lado del favorito
  - bajar una fraccion la exigencia:
    - de `-1` a `-0.75`
    - de `-0.75` a `-0.5`
  - en goles, si el cluster historico cae en `1` o `2` y el `OU` es bajo, evitar el over agresivo

## Caso: Turkey vs Romania (revision tras resultado real 1:0)

Resumen del caso:

- AH actual: `Turkey -1`
- OU actual: `2.75`
- H2H estadio: `Turkey 0:1 Romania` con `0.75`
- H2H general: `Romania 2:0 Turkey` con `0`
- Prev home: `Turkey 2:0 Bulgaria` con `2.25`
- Prev away: `Bosnia and Herzegovina 3:1 Romania` con `0`
- Sin `H2H Col3`
- Sin indirectas utiles

Que se habia leido demasiado fuerte:

- El lado local era correcto.
- Lo incorrecto fue comprar el `-1` entero como si Turquia necesitara o mereciera margen de dos goles.

Que enseña este caso:

1. Dos H2H que no cubren la linea actual no invalidan siempre al favorito, pero si invalidan el margen entero cuando no existe triangulacion que lo rescate.
2. `Turkey 2:0 Bulgaria` con `2.25` confirmaba superioridad, no margen amplio reusable en un `-1`.
3. `Romania 3:1` perdido fuera con `0` reforzaba el lado local, pero no bastaba para comprar el entero.
4. Con `OU 2.75` y cluster historico de `1` y `2` goles, la traduccion correcta era:
   - `Turkey -0.75` o simplemente `Turkey gana`
   - `Under 2.75`

Traduccion corregida:

- Pick AH realista: `Turkey -0.75`
- Pick conservador alterno: `Turkey gana`
- Pick goles: `Under 2.75`
- Conclusion:
  - cuando la casa pone `-1` pero el historial solo valida victoria corta, el edge es del favorito, pero no del margen entero

## Ley 14. El Mismo H2H No Puede Votar Dos Veces En OU

Si `H2H estadio` y `H2H general` son literalmente el mismo precedente, no se deben contar como dos votos independientes para `Over/Under`.

- Trigger:
  - `H2H estadio` y `H2H general` comparten mismo `match_id` o misma fecha/base
  - ambos empujan al mismo lado de goles
- Lectura:
  - no hay dos confirmaciones; hay una sola memoria repetida en dos cajas
  - duplicar ese voto infla falsamente el sesgo `Over` o `Under`
- Accion:
  - contar solo un voto H2H en goles
  - si el resto de bloques van en direccion contraria, el H2H clonado no debe mandar

## Ley 15. Favorito Corto Debil + Col3 Inversa = Under

Si el favorito actual sale en `0.25`, llega mal de su previa, el no favorito llega de porteria a cero y el `H2H Col3` es empate/under, el partido se debe leer mas por control y friccion que por intercambio de goles.

- Trigger:
  - `AH actual = 0.25`
  - `OU actual <= 2.25`
  - `H2H Col3` termina en empate y no supera la linea
  - el favorito actual viene de perder o encajar `2+` goles
  - el no favorito llega de victoria o empate con porteria a cero
- Lectura:
  - el favorito corto no tiene autoridad ofensiva fiable
  - el espejo no abre el partido; lo enfria
  - el no favorito llega con estructura mas estable de marcador
- Accion:
  - en handicap, desconfiar del favorito corto
  - en goles, priorizar `Under`
  - si ademas el H2H pro-favorito es viejo o aislado, el pick base pasa a ser el no favorito `+0.25`

## Caso: Cyprus vs Belarus (revision tras resultado real 0:1)

Resumen del caso:

- AH actual: `Cyprus -0.25`
- OU actual: `2.25`
- H2H estadio/general: mismo precedente `Cyprus 2:1 Belarus` con la misma linea `0.25`
- Prev home: `Cyprus 2:4 Estonia` con `0.5`
- Prev away: `Azerbaijan 0:2 Belarus` con `0`
- H2H Col3: `Azerbaijan 0:0 Estonia` con `0.75`
- Triangulacion: `INVERSA (Empate H2H)`

Que se habia leido mejor en handicap que en goles:

- El lado correcto ya era el visitante.
- Lo que faltaba era endurecer la lectura:
  - no dar demasiado peso al H2H de `2008`
  - no dejar que `stadium` + `general` contaran doble en `OU`
  - reconocer que el guion era de `Belarus + control`, no de partido roto

Que enseña este caso:

1. Un `-0.25` local sostenido casi solo por un H2H remoto es una colocacion debil.
2. Si el local viene de encajar `4` y el visitante viene de ganar `0:2`, el presente manda mas que la memoria.
3. Un `Col3 0:0` con etiqueta `INVERSA` y `OU 2.25` es freno natural al over.
4. El mismo H2H no puede empujar dos veces el `Over` por aparecer en `stadium` y `general`.

Traduccion corregida:

- Pick AH base: `Belarus +0.25`
- Pick AH agresivo: `Belarus 0`
- Pick goles: `Under 2.25`
- Conclusion:
  - favorito corto debil + no favorito en ascenso + espejo frio = partido para el lado visitante y para pocos goles

## Ley 16. Favorito Alto + Total Capado = Dominio Con Techo

Si la casa sube claramente el handicap del favorito, pero deja el `OU` en una linea contenida como `3`, no siempre esta comprando un partido roto. Muchas veces esta comprando una victoria dominante con marcador acotado.

- Trigger:
  - `AH actual >= 1`
  - `OU actual <= 3`
  - los H2H visibles son `over`, pero el mercado actual aprieta mas el margen que el total
  - el favorito actual llega de cubrir su propia linea con dominio claro
  - el no favorito llega de fallar su propia linea
- Lectura:
  - la casa reprecifica jerarquia, no caos
  - el recuerdo de `5:3`, `4:1` o `3:2` puede inflar mal el over si se lee en bruto
  - la firma real del cruce pasa a ser `2:0`, `3:0` o `3:1`, no necesariamente intercambio abierto
- Accion:
  - mantener el lado del favorito alto si el handicap sigue respaldado
  - en goles, no perseguir el `Over 3` por memoria
  - preferir `Under 3`, `push-friendly under`, o como minimo evitar el over agresivo

## Caso: Escorpiones Belen vs CS Uruguay De Coronado (revision tras resultado real 2:0)

Resumen del caso:

- AH actual: `Escorpiones Belen -1.25`
- OU actual: `3`
- Prev home: `Escorpiones Belen 4:1 AD Cariari Pococi` con `0.75`, dominio fuerte de tiros y tiros a puerta
- Prev away: `Santa Ana 5:3 CS Uruguay De Coronado` con `0.25`
- H2H estadio: `Escorpiones Belen 5:3 CS Uruguay De Coronado` con movimiento `0.75 -> 1.25`
- H2H general: `CS Uruguay De Coronado 3:2 Escorpiones Belen` con movimiento `-0.25 -> 1.25`
- H2H Col3: `Santa Ana 1:1 AD Cariari Pococi`
- Indirecta local: `Escorpiones Belen 3:1 Santa Ana`
- Indirecta visitante: `AD Cariari Pococi 1:1 CS Uruguay De Coronado`

Que se habia leido bien y que se habia inflado:

- El lado del handicap era correcto: el favorito local si sostenia `-1.25`.
- Lo incorrecto fue traducir la memoria de partidos altos a un `Over 3` casi automatico.

Que enseña este caso:

1. Cuando el mercado sube mucho el handicap del favorito y no acompana con un `OU` mas alto, la prioridad es el margen, no el intercambio.
2. Un favorito que viene de cubrir con dominio y recibir poco encaja muy bien en guiones de `2:0` o `3:0`.
3. El no favorito puede venir de un `5:3`, pero ese antecedente no obliga a repetir festival si la casa ahora lo coloca mucho mas abajo en jerarquia.
4. `Over` historicos viejos o visualmente llamativos pueden ser trampas si el precio actual concentra el edge en un solo lado del marcador.

Traduccion corregida:

- Pick AH: `Escorpiones Belen -1.25`
- Pick goles: `Under 3` o, como minimo, no entrar al `Over 3`
- Marcador guia: `2:0` / `3:0`
- Conclusion:
  - favorito alto con total contenido no siempre es goleada abierta; muchas veces es victoria controlada

## Ley 17. El Mismo H2H No Puede Castigar Dos Veces El Handicap

Si `H2H estadio` y `H2H general` son en realidad el mismo precedente, no deben hundir ni elevar dos veces la misma lectura de handicap.

- Trigger:
  - `H2H estadio` y `H2H general` comparten fecha/base/partido
  - ambos empujan al mismo lado o castigan al mismo lado
- Lectura:
  - no existen dos pruebas; existe una memoria duplicada en dos cajas
  - duplicarlo deforma el edge y suele castigar demasiado al favorito o inflarlo sin necesidad
- Accion:
  - contar el segundo H2H con peso reducido
  - si ademas hay salto fuerte de mercado actual, la cuota presente manda mas que la repeticion visual

## Ley 18. Repricing Violento Hacia El Favorito Borra Parte De La Memoria Vieja

Cuando el mercado mueve un cruce desde `0` o linea corta hacia `1.5` o mas para el favorito actual, esa reprecificacion no es decorativa. Es una amnistia parcial del H2H antiguo.

- Trigger:
  - `AH actual >= 1.25`
  - el `H2H general` o la referencia directa muestra salto de `1.25+` puntos hacia el favorito actual
  - el H2H antiguo era malo o ambiguo para el favorito actual
- Lectura:
  - la casa esta diciendo que la version actual del cruce ya no es la de aquella memoria
  - una vieja derrota o un viejo empate dejan de mandar si el precio actual reordena fuerte la jerarquia
- Accion:
  - no vender automaticamente al perro por memoria vieja
  - mantener o recuperar el lado del favorito actual si el salto de cuota es violento

## Ley 19. Fracasar Una Linea Gigante Con Volumen Brutal No Es Debilidad

Si el favorito venia de una linea muchisimo mas dura que la actual y no la cubrio, pero genero una avalancha estadistica, ese fallo no se traduce como debilidad. Muchas veces es combustible para la siguiente linea mas baja.

- Trigger:
  - la previa del favorito tenia `1.5+` puntos mas de exigencia que la actual
  - el favorito no cubrio, empato o incluso dejo sensacion rara
  - aun asi produjo dominio brutal de tiros, tiros a puerta o ataques peligrosos
- Lectura:
  - el mercado no esta viendo un tropiezo estructural; esta reciclando superioridad ofensiva a una linea mucho mas amable
- Accion:
  - no bajar un cuarto por reflejo
  - sostener la linea alta actual del favorito
  - en goles, si el `OU` es `2.75+`, abrir la puerta al `Over`

## Ley 20. El No Favorito Ya Revento Esa Misma Linea: Riesgo De Avalancha

Si el no favorito llega de fallar una linea igual o casi igual a la actual, ese dato pesa mas que un espejo bonito o una derrota aislada del favorito.

- Trigger:
  - `AH actual >= 1`
  - el no favorito viene de `NO_COVER` con una linea del mismo rango (`gap <= 0.25`)
  - la derrota del no favorito fue por `2+` goles o con sensacion clara de derrumbe
- Lectura:
  - la cuota no solo mide jerarquia; mide capacidad real del perro para sostener ese castigo
  - si ya exploto en esa misma familia de linea, el riesgo no es solo perder: es hundirse
- Accion:
  - no comprar `+1.25` o `+1.5` por inercia
  - mantener el lado favorito
  - en `OU 2.5+`, valorar `Over` por riesgo de goleada unilateral

## Ley 21. Memoria De Demolicion Barata + OU Alto = Favorito Full + Over

Si existe un H2H de goleada a linea bastante mas dura, la cuota actual baja mucho el handicap pero mantiene el total alto, el partido no debe enfriarse por una previa fea aislada.

- Trigger:
  - H2H previo de `3+` goles de margen para el favorito actual
  - ese H2H se dio con linea igual o mas dura que la actual
  - `OU actual >= 3.25`
- Lectura:
  - la bajada del handicap no niega la memoria de demolicion; solo la hace mas barata
  - el total alto confirma que la casa sigue abierta a partido grande
- Accion:
  - comprar el lado favorito sin rebajar por defecto
  - en goles, preferir `Over`

## Caso: Union Magdalena vs Real Santander (revision tras resultado real 2:0)

Resumen del caso:

- AH actual: `Union Magdalena -1.5`
- OU actual: `2.5`
- H2H estadio/general: mismo `4:0` con `1.5`
- Prev home: `Union 2:1 Patriotas` con `0.5`
- Prev away: `Orsomarso 2:0 Real Santander` con `0.5`
- H2H Col3: `Orsomarso 0:0 Patriotas`
- Indirecta local: `Orsomarso 0:1 Union`
- Indirecta visitante: `Patriotas 2:0 Real Santander`

Que confirma:

1. El mismo H2H favorable no debia inflarse doble, pero incluso contado una sola vez seguia validando el lado local.
2. El `Col3 0:0` y la comparativa indirecta construian mejor un `2:0` que un `3:1`.
3. Este no era partido de duda con el favorito; era partido de favorito limpio y goles cortos.

Traduccion final:

- Pick AH: `Union Magdalena -1.5`
- Pick goles: `Under 2.5`
- Marcador guia: `2:0`

## Caso: Italy vs Northern Ireland (revision tras resultado real 2:0)

Resumen del caso:

- AH actual: `Italy -1.5`
- OU actual: `2.75`
- H2H estadio/general: misma memoria de `2021-03-25`
- Prev home: `Italy 1:4 Norway` con `0.75`
- Prev away: `Slovakia 1:0 Northern Ireland` con `0.5`
- El mercado seguia manteniendo a Italia en linea alta

Que enseña este caso:

1. Una previa fea del favorito no basta para girar la lectura si la cuota sigue colocandolo en `1.5`.
2. El mismo H2H no podia castigar dos veces a Italia.
3. La linea actual seguia diciendo que el rival no tenia techo suficiente para resistir.
4. En goles, no habia guion de intercambio: era favorito + control.

Traduccion corregida:

- Pick AH: `Italy -1.5` o `Italy -1.25`
- Pick goles: `Under 2.75`
- Marcador guia: `2:0`

## Caso: Denmark vs North Macedonia (revision tras resultado real 4:0)

Resumen del caso:

- AH actual: `Denmark -1.25`
- OU actual: `2.75`
- Prev home: `Denmark 2:2 Belarus` con `3.25`, pero con dominio brutal de volumen
- Prev away: `Wales 7:1 North Macedonia` con `1`
- H2H general: `3:0`

Que enseña este caso:

1. `2:2` no significaba debilidad; significaba que Dinamarca habia chocado contra una linea descomunal.
2. Bajar de `3.25` a `1.25` no era para enfriar, sino para comprar con mas comodidad.
3. El `7:1` recibido por el no favorito era alerta de derrumbe, no simple dato defensivo.
4. Aqui no tocaba bajar a `-1`; tocaba sostener `-1.25` y abrir `Over`.

Traduccion corregida:

- Pick AH: `Denmark -1.25`
- Pick goles: `Over 2.75`
- Marcador guia: `3:0` / `4:0`

## Caso: Huracan Reserves vs San Martin de San Juan Reserves (revision tras resultado real 6:0)

Resumen del caso:

- AH actual: `Huracan Reserves -1.5`
- OU actual: `2.5`
- Prev home: `Huracan 0:1 Deportivo Riestra` con `1`
- Prev away: `Boca Juniors Reserve 4:1 San Martin` con `1.5`
- H2H general: `San Martin 3:1 Huracan` con salto `0 -> 1.5`
- H2H Col3: `Boca Juniors Reserve 3:0 Deportivo Riestra`

Que enseña este caso:

1. El salto de `0` a `1.5` hacia Huracan era la senal dominante; el viejo `3:1` no mandaba ya.
2. San Martin venia de reventar exactamente la misma familia de linea.
3. El espejo no debia usarse para comprar al perro; debia leerse como contexto de posible derrumbe.
4. En goles, el riesgo no era empate bajo; era avalancha unilateral.

Traduccion corregida:

- Pick AH: `Huracan Reserves -1.5`
- Pick goles: `Over 2.5`
- Marcador guia: `3:0+`

## Caso: CF Pachuca III vs Club Atletico Toltecas FC (revision tras resultado real 9:0)

Resumen del caso:

- AH actual: `CF Pachuca III -1.5`
- OU actual: `3.25`
- H2H estadio/general: `5:1` con `3.5`
- Prev home: `CF Pachuca III 0:4 CD Muxes`
- Prev away: `Halcones Negros 1:1 Toltecas` con `1.25`
- Indirecta local: `CF Pachuca III 3:2 Halcones`
- Indirecta visitante: `CD Muxes 3:1 Toltecas`

Que enseña este caso:

1. La memoria fuerte no era la previa fea de Pachuca, sino el `5:1` con linea mucho mas dura.
2. Bajar de `3.5` a `1.5` no era un warning; era una oferta.
3. Con `OU 3.25`, la casa seguia dejando abierta la ventana de demolicion.
4. Aqui no tocaba conservadurismo; tocaba favorito full + over.

Traduccion corregida:

- Pick AH: `CF Pachuca III -1.5`
- Pick goles: `Over 3.25`
- Marcador guia: `4:0+`
