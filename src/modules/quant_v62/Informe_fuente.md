# Motor cuantitativo dual: tiros a puerta, compuertas binarias y Python + Gemini
Fecha: 26-10-2026. v6.2 Operativo.


## 01 / Hoja de ruta ejecutiva
LECTURA RÁPIDA · QUÉ HACE EL MOTOR

El motor v6.2 convierte la metodología aportada en un programa ejecutable: estudia los datos de campo, normaliza las líneas asiáticas, calcula doce compuertas y emite una dirección y una línea objetivo en AH y O/U. La evidencia científica orienta las variables; las reglas de decisión permanecen explícitas y versionadas.

| Paso | Cálculo o salida |
| --- | --- |
| 1. Campo | Tiros, tiros a puerta (TP/SOT), precisión, conversión, producción y concesión; cinco partidos activos por equipo. |
| 2. Mercado | Signo NowGoal, cuartos exactos, dos bloques similares, H2H, referencias, rivales comunes y espejo. |
| 3. Votos | b1…b6 apoyan al favorito; g1…g6 apoyan Over. Cada bit lleva datos, explicación y máscara de disponibilidad. |
| 4. Coherencia | Aplicar las políticas en orden, conservar los bits originales y registrar cualquier cambio de dirección. |
| 5. Pick | Equipo + línea canónica; Over/Under + total. Mostrar cuota observada o línea objetivo por cotizar. |
| 6. Feedback | Sellar snapshot, añadir resultado, liquidar, solicitar revisión estructurada a Gemini y probar cambios en partidos posteriores. |

```text
Hq = 4 × AH de plataforma     |     línea canónica local = −Hq
Conversión = Σ goles de tiro / Σ TP     |     Cuota de TP = Σ TP_F / Σ(TP_F + TP_C)
I = Σ bits válidos     |     I_normalizado = 6 × I / número de bits válidos
```

Ejemplo sintético entregado: b=[1,1,1,0,1,1], g=[1,1,1,0,1,1]. Diagnóstico: Local −0,5 y Over 2,5. Los cinco votos son coincidencias de reglas; no una probabilidad del 83,3%.

El paquete incluye cuatro casos reproducibles, 38 pruebas, contrato de datos, motor Python, liquidación, evaluación de registros y conexión preparada con Gemini. La integración con el DOM real requiere conectar el exportador de la plataforma.


## 02 / Evidencia para empezar por los tiros
FUNDAMENTO CIENTÍFICO · APLICACIÓN CONCRETA

Usar tiros a puerta como parte central del análisis tiene respaldo empírico. Su utilidad aumenta al separar volumen, eficacia y capacidad defensiva del rival. La literatura sirve para construir esa descomposición; no proporciona automáticamente los cortes exactos de nuestras compuertas.

| Fuente y diseño | Hallazgo pertinente | Aplicación al motor |
| --- | --- | --- |
| Lago-Peñas et al., 2010 [S01]
380 partidos de la liga española 2008–09. | Los tiros y tiros a puerta distinguieron a ganadores, empatadores y perdedores. Es una asociación con el resultado del mismo partido. | Medir volumen y dominio. No convertir esa asociación en una tasa de acierto prepartido. |
| Wheatcroft, 2020 [S02]
Pronóstico O/U en diez ligas europeas. | Los ratings basados en estadísticas de remate aportaron mayor capacidad predictiva que los basados solo en goles en su evaluación. | Estimar lo que cada equipo puede producir y conceder, en vez de copiar el último marcador. |
| Wheatcroft y Sienkiewicz, 2021 [S03]
Preprint de modelado de éxito de tiro. | Combinar volumen previsto y probabilidad de convertir mejoró pronósticos; la evidencia adicional en rentabilidad fue mixta. | Separar generación de ocasiones y finalización. Evitar asumir una conversión idéntica entre equipos. |
| Mead et al., 2023 [S04]
Modelos de expected goals. | La calidad de las ocasiones aporta información: su xG anticipó rendimiento futuro mejor que estadísticas tradicionales en los datos estudiados. | Mantener abierta una ampliación con calidad de tiro si se dispone de datos. El conteo de TP no describe toda la ocasión. |

Conclusión aplicada: el centro del modelo será TP a favor y en contra + precisión + conversión + contexto de rival y rol. Los umbrales +1,5 TP, 100 AP o 35% pertenecen a la especificación operativa; estas publicaciones no los validan como constantes universales.


## 03 / Qué significa cada magnitud
TELEMETRÍA · CONTRATO DEL PROVEEDOR

| Variable | Lectura y cálculo | Precaución de interpretación |
| --- | --- | --- |
| Tiros totales T | Intentos registrados por partido, a favor y en contra. | Documentar si el proveedor incluye bloqueados. No mezclar denominadores de proveedores distintos. |
| Tiros a puerta TP | Intentos dirigidos a gol según definición de la fuente. | Opta incluye goles, paradas y bloqueos de última línea; un poste sin gol no cuenta como TP. [S05] |
| Precisión A | Σ TP_F / Σ T_F. | La implementación incluye todos los tiros de su entrada. No debe etiquetarse como la precisión oficial Opta si el denominador difiere. |
| Conversión C | Σ goles atribuibles a tiros / Σ TP_F. | Excluir autogoles del numerador. Si no se conoce esta atribución, marcarla como ausente. |
| Concesión C_def | Σ goles recibidos de tiro / Σ TP_C. | Mezcla calidad de ocasiones concedidas, portero y variación; no mide por sí sola una habilidad estable. |
| Balance y cuota de TP | TP_F − TP_C; Σ TP_F / Σ(TP_F + TP_C). | Cuota de TP del 70% es dominio de esa métrica, no 70% de probabilidad de victoria. |
| Ataques peligrosos AP | Media a favor y en contra bajo una definición constante. | El corte 100 es una regla del sistema. AP no certifica por sí solo un Over ni sustituye universalmente a los tiros. |
| Córners / ataques | Contexto de presión y territorio. | Pueden repetir información ya presente en remates; se conservan sin añadir votos ocultos. |

Un cero observado y un dato desconocido son distintos. TP=0 significa ningún tiro a puerta registrado; TP=null significa que falta la medición. Si falta el denominador, no se calcula una conversión de 0%. Un cociente con denominador cero queda ausente.

La plataforma debe registrar proveedor, definición, competición, 90 minutos, fuente y momento de disponibilidad. La homogeneidad de estas etiquetas hace comparable la regla de tres.


## 04 / Volumen, precisión y eficacia
MÉTRICAS · CÓMO LEER UN MARCADOR

Figura 1. Promedios inventados del ejemplo dominio.json; sirven para reproducir el cálculo, no describen equipos reales.

```text
En una muestra compatible: goles de tiro = tiros × (TP / tiros) × (goles de tiro / TP)
Ejemplo A: 16 × 0,4375 × 0,342857 = 2,4 goles por partido.
Ejemplo B: 11 × 0,363636 × 0,30 = 1,2 goles por partido.
```

La identidad es contable sobre la misma muestra. Para proyectar un partido futuro hay que estimar sus tres componentes y considerar lo que permite el rival. Un equipo puede producir mucho y convertir poco, disparar poco y convertir mucho, o producir y convertir bien. Esos estados requieren diagnósticos distintos.

### Leer los goles junto a la amenaza concedida

En el ejemplo, A genera 7 TP y concede 3; B genera 4 y concede 6. A tiene balance +4 y cuota de TP del 70%; B, balance −2 y cuota del 40%. Esto respalda una superioridad de campo. La exigencia concreta de −0,75 todavía debe contrastarse con margen, empates y cuota.

Un 1:1 con muchos remates concedidos se etiqueta como resultado resistente bajo presión. Se conserva el empate real en el registro; la telemetría añade la advertencia. No se reemplaza el marcador por una “derrota estructural” en la base de resultados.


## 05 / Eficacia y efecto presa
CONVERSIÓN · REGLA DE TRES CON REGULARIZACIÓN

El cociente correcto acumula goles y tiros antes de dividir. Si un partido aporta 1 gol de 1 TP y otro 1 gol de 9 TP, la eficacia conjunta es 2/10=20%. Promediar 100% y 11,1% daría 55,6% y exageraría la señal.

```text
C_observada = Σ G_tiro / Σ TP
C_regularizada = (Σ G_tiro + k × C_liga) / (Σ TP + k)
k = 20 tiros equivalentes en la configuración inicial; C_liga procede de datos históricos.
```

La regularización aproxima la tasa a una referencia cuando hay pocos tiros. Es un mecanismo de estabilización propuesto para este motor, con k configurable; 20 no es un valor demostrado por los estudios citados. Se calcula por separado para ataque y concesión, y para los periodos activo e histórico.

| Situación ilustrativa | Lectura bruta | Con referencia 30% y k=20 |
| --- | --- | --- |
| 7 goles de 9 TP | 77,8%: eficacia muy elevada. | (7+6)/(9+20)=44,8%. Sigue alta, pero con menor extrapolación. |
| 1 gol de 12 TP | 8,3%: producción no convertida. | (1+6)/(12+20)=21,9%. No implica que los siguientes goles estén “debidos”. |
| 12 goles de 35 TP | 34,3%: volumen más amplio. | (12+6)/(35+20)=32,7%. La muestra pesa más frente a la referencia. |

### Cómo se implementa la lectura operativa

b5 conserva el efecto presa solicitado: último partido activo con al menos 10 TP, 55 AP y como máximo un gol. La etiqueta burbuja se activa con conversión ≥60% y media de TP ≤4; son cortes añadidos, visibles en config.json. La regularización alimenta la proyección de forma; g6 conserva la conversión observada de la especificación.

“Presa” identifica una discrepancia entre producción y resultado. No es una obligación de compensación en el siguiente partido. La prueba útil es comprobar si este patrón mejora decisiones futuras a la línea y cuota disponibles.


## 06 / Proyectar con reglas de tres
NÚCLEO NUMÉRICO · SIN DISTRIBUCIÓN DE POISSON

La proyección complementa los votos y resuelve empates de puntuación. Se estiman TP de cada equipo frente a la capacidad del rival para concederlos, usando una referencia de liga compatible. Después se aplica una conversión que combina ataque y concesión regularizados.

```text
TP̂_A = TP_F,A × TP_C,B / TP_liga
Ĉ_A = (C_ataque,A + C_concesión,B) / 2
Ĝ_A = TP̂_A × Ĉ_A     |     Ĝ_total = Ĝ_A + Ĝ_B
Margen̂ = Ĝ_favorito − Ĝ_no_favorito
```

La interpretación de la primera fórmula es: si A genera 6 TP, B concede 5 y la liga concede 4,5, el ajuste del rival es 5/4,5=1,111. La proyección es 6,667 TP. Con conversiones regularizadas de 30% y 35%, la media es 32,5% y el indicador goleador resulta 2,167.

| Ejemplo entregado, tras mezclar 65/35 | Favorito A | No favorito B |
| --- | --- | --- |
| TP a favor / TP concedidos por el rival | 6,65 / 6,00 | 3,65 / 3,00 |
| Referencia de liga, por equipo | 4,50 | 4,50 |
| TP proyectados | 8,867 | 2,433 |
| Conversión combinada | 30,880% | 30,989% |
| Indicador goleador | 2,738 | 0,754 |

Salida del ejemplo: total 3,492 y margen del favorito 1,984. Son indicadores de contraste obtenidos por ratios; no son xG de eventos, un marcador exacto ni probabilidades de cubrir. No se utiliza rho ni una matriz Poisson.

Si el rival concediese 4 TP en vez de 6, manteniendo los demás valores, el indicador de A bajaría de 2,738 a 1,825. Esta sensibilidad muestra por qué deben compararse competición, nivel del rival y rol. La multiplicación de ratios es una aproximación de ingeniería que deberá evaluarse con los datos de la plataforma.


## 07 / Ventana activa y peso 65/35
TIEMPO · FILTROS REPRODUCIBLES

Se ordena el historial por fecha y se filtra antes de promediar. La ventana activa contiene como máximo los cinco partidos elegibles más recientes dentro de 21 días. El periodo previo contiene los demás partidos elegibles dentro de 90 días. No se rellenan cinco fichas con datos inventados ni se etiqueta una ventana de dos meses como de tres semanas.

| Filtro | Regla implementada |
| --- | --- |
| Disponibilidad | El partido histórico y su información deben existir antes de la decisión. Se rechazan registros futuros o el resultado del propio evento. |
| Competición | Usar competition_id del partido actual. No mezclar automáticamente copa, fase distinta y liga. |
| Plantillas | En partido de primeras plantillas, excluir rivales reserve/u20 de la telemetría. |
| Rol | Convertir cada línea a fuerza del equipo. Excluir si la diferencia con su rol actual supera tres cuartos. |
| Expulsiones | Excluir filas marcadas con red_card_context por defecto; opción configurable añadida a la especificación. |
| Muestras | Exigir tres partidos válidos para la mayoría de métricas activas. El cerrojo exige cinco, cuatro porterías a cero y TP concedidos ≤2. |

```text
m_forma = 0,65 × m_activa + 0,35 × m_previa
Si solo existe un periodo: usarlo con peso 1 y registrar el peso realmente aplicado.
```

El 65/35 se aplica a TP y conversiones regularizadas de la proyección. b5, g4, g5 y g6 leen la ventana activa según sus reglas. Las seis compuertas de cada módulo tienen votos iguales: un voto entre seis no representa un 65% de la decisión.

Este reparto temporal conserva el énfasis reciente sin atribuir una ponderación falsa a la suma binaria. Si se quisiera asignar literalmente el 65% del voto total a la telemetría, habría que definir y versionar otro agregador. El 65/35 es una elección del modelo, no una constante científica.


## 08 / Signos y cuartos de gol
AH · NORMALIZACIÓN NOWGOAL

Se adopta la convención específica descrita por el usuario. No se supone que todos los proveedores publiquen el mismo signo. La entrada conserva el dato bruto; el normalizador produce la línea de apuesta canónica del equipo seleccionado.

| Campo de origen | Valor | Significado y representación |
| --- | --- | --- |
| Cabecera: AH bruto | +0,75 | Favorito local. Hq=+3; apuesta local −0,75, line_q=−3. |
| Cabecera: AH bruto | −1,50 | Favorito visitante. Hq=−6; apuesta visitante −1,50, line_q=−6; local +1,50. |
| Historial CASA del equipo | +0,50 | Fuerza del equipo +2 cuartos; su línea canónica es −0,50. |
| Historial FUERA del equipo | +0,50 del anfitrión | Invertir perspectiva: fuerza del visitante −2; su línea canónica es +0,50. |
| Historial FUERA del equipo | −0,75 del anfitrión | Fuerza del visitante +3; el visitante daba −0,75. |
| AH bruto cero | 0 | No existe favorito de mercado. El local es ancla técnica y la salida se etiqueta Local/Visitante. |

```text
q_línea = 4 × línea
q_fuerza_equipo = 4 × AH_anfitrión × (+1 si local; −1 si visitante)
q_apuesta_equipo = −q_fuerza_equipo
```

Las líneas deben ser múltiplos exactos de 0,25. La función quarters usa Decimal y rechaza 0,30: redondearlo ocultaría un error. Las medias y brechas estimadas, como 3,4 cuartos, pueden ser reales; solo las líneas negociables son discretas.

Multiplicar por cuatro es una reexpresión exacta de la rejilla de cuartos. No añade información predictiva. La clasificación útil para liquidar es q módulo 4: 0 línea entera, 2 medio gol, 1 o 3 cuarto de gol. “Par” por sí solo no distingue −1,0 de −1,5.


## 09 / Liquidación del hándicap
MATEMÁTICA EXACTA · CINCO RESULTADOS POSIBLES

Una línea de cuarto reparte la unidad apostada entre las dos líneas vecinas de medio gol. Para cada mitad se suma el hándicap canónico al margen del equipo. Positivo gana, cero devuelve y negativo pierde.

```text
Si q es impar: dividir en q−1 y q+1, con media unidad en cada parte.
Si q es par: una sola línea.
Beneficio por parte = cuota−1 si gana; 0 si push; −1 si pierde.
```

| Pick | Marcador / margen | Liquidación | Beneficio a cuota 1,90 |
| --- | --- | --- | --- |
| Local −0,25 | Empate | ½ en 0 devuelve; ½ en −0,5 pierde. | −0,50 |
| Local −0,50 | Gana por 1 | Gana completo. | +0,90 |
| Local −0,75 | Gana por 1 | ½ en −0,5 gana; ½ en −1 devuelve. | +0,45 |
| Local −1,00 | Gana por 1 | Reembolso completo. | 0,00 |
| Local −1,25 | Gana por 1 | ½ en −1 devuelve; ½ en −1,5 pierde. | −0,50 |
| Visitante +0,25 | Empate | ½ en 0 devuelve; ½ en +0,5 gana. | +0,45 |
| No favorito +1,50 | Pierde por 1 | Gana completo. | +0,90 |

Ganar el partido y cubrir no son equivalentes. En la demo A gana su previo 2:1 dando −1,25: pierde media unidad. Por eso b4=0 aunque exista un H2H favorable. El algoritmo debe leer ambas piezas y no sustituir la cobertura por “ganó”.

Pasar de −0,75 a −0,50 mejora el pago cuando se gana por un gol, pero normalmente cambia la cuota. El motor informa la línea objetivo y busca su precio entre las cuotas aportadas; no inventa una mejora gratuita.


## 10 / Goles, protección y precio
O/U · LA LÍNEA ALTERNATIVA TIENE UN COSTE

| Pick | Con 2 goles | Con 3 goles | Con 4 goles |
| --- | --- | --- | --- |
| Over 2,25 | Media pérdida | Ganancia completa | Ganancia completa |
| Over 2,50 | Pérdida completa | Ganancia completa | Ganancia completa |
| Over 2,75 | Pérdida completa | Media ganancia | Ganancia completa |
| Over 3,00 | Pérdida completa | Push | Ganancia completa |
| Under 2,75 | Ganancia completa | Media pérdida | Pérdida completa |
| Under 3,00 | Ganancia completa | Push | Pérdida completa |

Figura 2. Beneficio neto por unidad con cuota ilustrativa 1,90 en todas las curvas. En un mercado real, las líneas alternativas tienen precios distintos.

Under 2,75 devuelve media apuesta y pierde la otra mitad con tres goles: es una media pérdida, no un reembolso favorable. “Cobro completo” significa que gana toda la unidad apostada; con cuota 1,90 el beneficio neto es 0,90, no una rentabilidad del 100%.

```text
EV = (o−1) × [P(W) + 0,5 P(HW)] − P(L) − 0,5 P(HL)
El push aporta 0. Los seis votos no proporcionan estas cinco probabilidades.
```

La salida incluye dirección, línea y cuota. expected_value queda sin estimar hasta disponer de probabilidades de liquidación calibradas. Eso permite estudiar el partido ahora sin confundir un diagnóstico claro con una ventaja económica ya demostrada.


## 11 / Las 24 variables, de V1 a V12
MAPA DE DATOS · PRECIO, CONTEXTO Y SIMILARES

| Variable original | Campo / transformación | Uso efectivo |
| --- | --- | --- |
| V1 · AH inicial | raw_home_ah; 4×; signo de plataforma. | Favorito, fuerza, línea canónica y filtro de rol. |
| V2 · O/U inicial | ou_line y total_q. | Comparaciones g1–g4 y línea objetivo de goles. |
| V3 · Convención de signo | market.strength_q; venue de cada fila. | Inversión FUERA y liquidación del equipo correcto. |
| V4 · O/U histórico | ou_line de filas históricas únicas de la competición. | Media en cuartos y desplazamiento actual frente a histórico. |
| V5 · Ranking | rank_home / rank_away. | Favorito invertido y patrón de discrepancia de tabla. |
| V6 · Registro V/E/D | record_home / record_away; opcionales. | Contexto conservado para auditoría; no añade un voto secreto. |
| V7 · Liga | competition_id en evento y filas. | Separación de competiciones y ventanas comparables. |
| V8 · Tasa de empate | Derivada de hasta diez filas elegibles en la condición actual. | b6, junto con empates de la ventana activa. |
| V9 · Cover actual | similar_current.home/away: n, cover_rate, losses, sample_ids. | b1 y política de doble correlación. |
| V10 · GF/GC actual | gf y ga del bloque actual. | Fusión de totales y acoplamiento Over→AH. |
| V11 · Cover previo al H2H | similar_h2h con muestra y definición de Cover. | b2, doble correlación y filtro histórico del acoplamiento. |
| V12 · GF/GC histórico | gf y ga del bloque histórico. | Fusión ponderada, con detección de solapamiento. |

Cover es un dato de la plataforma con definición registrada. No se confunde “sin derrotas deportivas” con “sin pérdidas asiáticas”. Un 1:0 puede perder media apuesta si se exigía −1,25. Si dos bloques calculan Cover con convenciones diferentes, deben normalizarse antes de compararlos.

No se aplican porcentajes a una muestra vacía. Con N=0, Cover, GF, GC y derrotas se entregan como null. La ausencia queda en la máscara de disponibilidad.


## 12 / Las 24 variables, de V13 a V24
MAPA DE DATOS · RED Y VENTANA DE CAMPO

| Variable original | Campo / transformación | Uso efectivo |
| --- | --- | --- |
| V13 · Correlación nativa | correlation del bloque: home, away, balanced, unknown. | b2 usa la histórica; la actual se conserva con los datos del bloque. |
| V14 · H2H | Filas entre ambos equipos; raw_home_ah y cierre opcional. | b4 y memoria de margen; apertura/cierre visibles en su explicación. |
| V15 · Previo local/visita | Última fila elegible en la misma condición. | Comprobar cobertura real; no equiparar victoria a cover. |
| V16 · Col3 espejo | bridges: encuentro entre dos rivales previos. | Camino de triangulación y referencia de goles única. |
| V17 · Indirectas | history: cruces de cada equipo frente al rival del otro. | Rivales comunes o caminos con puente. |
| V18 · Rivales comunes | Última fila frente a cada rival, hasta 90 días. | b3: brecha ajustada y diferencia de rendimiento. |
| V19 · Tiros | shots_for / shots_against. | Precisión, volumen g6 y contexto defensivo. |
| V20 · Tiros a puerta | sot_for / sot_against. | b5, cerrojo, balance de TP y proyección de ambos equipos. |
| V21 · Ataques totales | attacks_for / attacks_against. | Contexto conservado en perfiles; no se suma como voto adicional. |
| V22 · Ataques peligrosos | ap_for / ap_against. | b5, g5, efecto presa y contraste de territorio. |
| V23 · Córners | corners_for / corners_against. | Contexto de presión para el analista y Gemini. |
| V24 · Conversión | shot_goals_for/against y TP; ratio de sumas. | g6, etiquetas de eficacia y proyección regularizada. |

Cada fila lleva match_id, equipo, rival, condición, fuente, fecha y available_at. La API de entrada contiene también cuotas alternativas y referencia histórica de liga. Estas adiciones hacen posible comprobar el cálculo y pedir feedback sobre datos concretos.

Las variables contextuales siguen disponibles aunque no voten. Añadir votos por ataques, córners y tiros simultáneamente podría contar varias veces una misma presión ofensiva; cualquier cambio de peso debe quedar en una nueva versión.


## 13 / Leer los seis patrones de mercado
CAPA 1 · DISCREPANCIAS OBSERVABLES

Las etiquetas de “trampa” se mantienen como nombres de patrones de estudio. El dato observable es la discrepancia entre tabla, línea, rendimiento y eficacia. Con una pantalla no se observa el libro interno de exposición de la casa ni su intención.

| Patrón | Qué comprobar | Consecuencia operativa |
| --- | --- | --- |
| Cebo de tabla | Favorito mejor clasificado, línea corta y no favorito con balance positivo de TP. | Activar revisión de b1, b3 y b5. El ranking solo no decide. |
| Aversión al perdedor / presa | Marcadores malos frente a TP y AP altos; último activo ≥10 TP, ≥55 AP y ≤1 gol. | b5 puede activarse. Conservar la conversión y el contexto del rival. |
| Burbuja de eficacia | Goles concentrados en pocos TP; comprobar numerador, autogoles y definición de tiros. | Mostrar tasa bruta y regularizada. La etiqueta inicial exige ≥60% y ≤4 TP de media. |
| Cambio de rol / filiales | Volumen conseguido como favorito amplio o ante reservas, distinto del partido actual. | Excluir esas filas de la media activa y explicar cuántas se perdieron. |
| Fricción de empate / margen | Empates ≥40%, dos empates activos o victorias estrechas para una línea larga. | b6=0; si exige −1,5 o más y hay dos empates activos, veto final al favorito largo. |
| Desplazamiento de O/U | Línea actual ≥0,50 por encima de aperturas históricas y ambos con algún 4+ reciente. | g3 y política alcista. Si aparece cerrojo confirmado, registrar el conflicto y aplicar la prioridad establecida. |

Levitt estudió un mercado de fútbol americano y encontró comportamiento de precios compatible con explotar preferencias de apostadores, sin equilibrar siempre los importes. Esto apoya examinar sesgos; no demuestra una regla universal de mover 0,25–0,50 goles en fútbol ni que un favorito invertido gane. [S06]

La variable llamada antes “dislocación de Shin” se presenta como desplazamiento de línea. El trabajo de Shin es una referencia de formación de cuotas con información asimétrica [S07]; el umbral de dos cuartos es una regla propia, no un estimador del dinero informado.


## 14 / Dos bloques de hándicaps similares
CAPA 2 · MUESTRA, COVER Y FUSIÓN

Se leen ambos bloques para ambos equipos. Cada agregado conserva N, porcentaje de Cover, derrotas deportivas, GF, GC, definición y lista de partidos. La correlación nativa se captura como un dato con fuente, no como una conclusión independiente de sus componentes.

```text
S_q = 4 × [Σ_j N_j × (GF_j + GC_j)] / Σ_j N_j
j recorre las muestras válidas de ambos equipos y ambos bloques, sin IDs repetidos.
```

Es una media ponderada de totales por partido, no la suma de dos totales que ya incluyen goles de ambos lados. Por ejemplo, N1=4 con total 3,0 y N2=8 con total 2,25 produce (12+18)/12=2,50 goles, equivalentes a 10 cuartos. No se redondea a un entero antes de contrastarlo.

| Problema de lectura | Tratamiento |
| --- | --- |
| N=0 y Cover 0% en la pantalla | Convertir a muestra ausente. El contrato rechaza tomar ese 0% como una observación. |
| Un bloque repite partidos del otro | Detectar por IDs. La media agregada no permite desduplicar internamente: solicitar filas o entregar el agregado ya limpio. |
| N=2 con buen Cover | Puede alimentar b1 según la regla inicial; la prioridad de doble correlación exige al menos tres por muestra. |
| Invicto con hándicap negativo | No equivale a cubrir. “Derrotas=0” es un indicador deportivo separado del porcentaje asiático. |
| Correlación actual y Cover discrepan | Exponer ambos al auditor. La regla b1 sigue sus umbrales; no se cambia el voto por una etiqueta sin revisar su definición. |

### Jerarquía estructural implementada

Doble correlación: en ambos bloques, no favorito con Cover ≥60%, cero derrotas y N≥3; favorito con Cover ≤40% y N≥3. Se cambia la dirección AH al no favorito. El acoplamiento con Over puede cambiarla después si cumple todos sus requisitos; un simple 0% o una etiqueta aislada no basta.

El tamaño de muestra no elimina la lectura operativa: aparece junto al voto. Su función es impedir que cero observaciones se conviertan en evidencia y que los mismos partidos se cuenten varias veces.


## 15 / Triangulación con localía explícita
CAPA 3 · RIVALES COMUNES, INDIRECTAS Y ESPEJO

Se elige el encuentro más reciente frente a cada rival común dentro de 90 días y la misma competición. En esta capa se permiten roles distintos: justamente se compara cómo fue tasado cada equipo. Los filtros estrictos de rol se aplican a las medias de telemetría.

```text
Fuerza neutral N(A,R) = q_fuerza(A,R) − h × v_A
v_A = +1 si jugó en casa; −1 si jugó fuera. h inicial = 2 cuartos.
Δ_actual(F,D | R) = N(F,R) − N(D,R) + h × v_F_actual
```

Ejemplo: F fue local con fuerza +5; D fue visitante con fuerza −2. Con h=2, sus fuerzas neutrales son +3 y 0. Si hoy F juega en casa, la brecha es 3−0+2=5 cuartos. Frente a una exigencia de 3, el indicador de tasación es favorable. Se comprueba además el rendimiento: diferencia de goles de F ante R menos la de D ante R.

```text
Puente espejo F → R1 → R2 → D:
Δ_actual = N(F,R1) + N(R1,R2) − N(D,R2) + h × v_F_actual
```

La fila bridges almacena R1 contra R2 con marcador, línea, fecha y fuente. El código combina el puente con los cruces de F y D. Los caminos y sus IDs aparecen en b3. Se promedian las brechas y las diferencias de rendimiento; el voto exige brecha ≥AH actual y rendimiento medio estrictamente mejor.

| Decisión | Motivo |
| --- | --- |
| Ajustar cada sede y restaurar la actual | Si uno fue local y otro visitante, neutralizar ambas posiciones puede cambiar cuatro cuartos; no siempre basta sumar dos de forma mecánica. |
| Usar h=2 como parámetro | Es una elección inicial de medio gol. Debe poder cambiar por competición y época, sin atribuirle validez universal. |
| Conservar caminos compartidos | Dos triangulaciones pueden depender del mismo partido. No se presentan como dos pruebas estadísticas independientes. |

Una brecha de 15 cuartos en precios históricos no garantiza ganar por 3,75 goles. La regla de ruptura unilateral se trata como apoyo direccional y exige que el signo favorezca al favorito actual.


## 16 / Las seis compuertas AH
VECTOR b · REGLAS DE APOYO AL FAVORITO

| Bit | Condición para 1 | Condición para 0 / disponibilidad |
| --- | --- | --- |
| b1
Similares actuales | Cover favorito ≥55% y no favorito ≤50%, si no existe apoyo prioritario al no favorito. | Tiene prioridad el 0 si no favorito Cover≥60% o invicto con N≥3. Si no se cumple el apoyo al favorito, 0. Exigir N≥2 y Cover conocido en ambos. |
| b2
Histórico / inversión | Correlación histórica favorable al favorito con muestras válidas; o favorito peor clasificado con /AHq/≥3. | 0 si la condición conocida no se cumple. Si faltan tanto la correlación válida como la inversión positiva, el bit puede quedar no disponible. |
| b3
Triangulación | Brecha media ajustada ≥/AHq/ y rendimiento medio mejor en cruces comunes/espejo. | 0 si la línea exige más que la brecha o el rendimiento no supera al rival. Sin un camino válido, no disponible. |
| b4
H2H / previo | Ganó y obtuvo liquidación positiva en el último H2H; el previo en la misma condición no perdió su liquidación. | 0 si falla cualquiera. Se muestran línea de apertura, cierre si existe y antigüedad del H2H. Sin H2H o previo elegible, no disponible. |
| b5
Dominio activo | Ventaja ≥1,5 TP y ≥10 AP a favor respecto al no favorito; o efecto presa: ≥10 TP, ≥55 AP y ≤1 gol en el último activo. | 0 si no cumple. La comparación exige al menos tres mediciones válidas por equipo; presa puede activar con su fila específica. |
| b6
Margen / empate | No aparece fricción de empate o de margen y hay al menos tres activos. | 0 con empate ≥40% en condición (N≥3), al menos dos empates activos, o exigencia ≥1,25 y mayoría de victorias por un gol entre al menos dos victorias. |

Los bits indican cumplimiento de reglas. b=0 no certifica que exista +EV en el no favorito. El sentido operativo lo determina la combinación, su disponibilidad y las políticas posteriores.

El código conserva seis posiciones incluso cuando falta una fuente. Se muestran dos vectores: bits y máscara. Ejemplo: bits [0,0,1,0,0,1], máscara [0,0,1,1,0,1] significa dos apoyos al favorito entre tres reglas evaluables; no dos apoyos entre seis evidencias contrarias.


## 17 / Goles: escenario y referencias
VECTOR g · COMPUERTAS g1, g2 Y g3

### g1 · Piso condicional 1+2=3 y cerrojo

Vota 1 si el no favorito marca en al menos el 75% de hasta diez partidos elegibles en su condición, hay al menos tres observaciones, la proyección de tiros favorece la victoria del favorito y la línea es ≤3,0. El cerrojo de cualquiera de los equipos fuerza 0: cinco activos, al menos cuatro porterías a cero y media de TP concedidos ≤2.

```text
Si en el futuro D marca ≥1 y F gana: F≥2, por tanto F+D≥3.
La identidad es exacta bajo esas dos condiciones; su ocurrencia futura sigue siendo incierta.
```

El apoyo a un no favorito +1,5 no implica que vaya a ganar; por eso el código no deduce “ganador” de cualquier pick AH. Utiliza el signo del margen proyectado por tiros para el escenario g1. El veto anti-Under que sigue a g1 es una política del motor, no una imposibilidad matemática del Under.

### g2 · Media de referencias

Vota 1 cuando cuatro veces la media de goles de las referencias únicas es ≥Gq. Se incluyen previos, H2H, indirectas y puentes espejo aportados. Un match_id repetido solo entra una vez; resultados incompatibles se rechazan. El porcentaje de referencias con tres o más goles se calcula sobre ese mismo conjunto.

### g3 · Fusión de similares o desplazamiento respaldado

Vota 1 si la media fusionada de similares S_q≥Gq, o si el total actual está al menos dos cuartos por encima del histórico y ambos equipos tienen algún partido de cuatro o más goles en los activos de los últimos quince días. La referencia histórica usa hasta diez aperturas por equipo, en un máximo de 90 días, con IDs únicos.

Si un bloque se solapa y no puede desduplicarse, su media no se inventa. Una rama positiva conocida puede activar la compuerta; cuando no hay una rama positiva y faltan datos para resolver la alternativa, se conserva la máscara de ausencia.

Comparar el total actual con partidos históricos distintos no equivale a observar un movimiento de apertura a cierre de este mismo evento. Diferencias de rival, alineación y sede también pueden explicar el desplazamiento.


## 18 / Goles: ruptura, AP y remate
VECTOR g · COMPUERTAS g4, g5 Y g6

| Bit | Regla de activación | Interpretación y datos |
| --- | --- | --- |
| g4
Ruptura | Unilateral: brecha ajustada a favor del favorito ≥Gq. O bilateral: cada equipo tiene GF≥1,8, GC≥1,8 y BTTS≥60% en sus activos válidos. | Exigir tres partidos por equipo para la rama bilateral. Se usa brecha positiva alineada con el favorito; un valor absoluto negativo no prueba que este pueda golear. |
| g5
AP | Media AP a favor del equipo A + media AP a favor del equipo B ≥100. | Exigir tres registros por equipo. No sumar además AP en contra: se duplicaría el ritmo. La definición debe ser consistente dentro del proveedor. |
| g6
Volumen / eficacia | Tiros a favor combinados ≥22, o conversión observada sobre TP ≥35% en ambos equipos. | La conversión divide sumas, excluye autogoles y requiere tres filas con numerador y denominador conocidos. Una rama positiva basta; si ninguna es positiva, ambas deben ser evaluables para registrar 0. |

### Por qué g6 conserva dos rutas

Un partido puede alcanzar un total alto por mucho remate con eficacia normal o por menos remate con eficacia elevada. El OR reproduce esa intención. La etiqueta de burbuja advierte si la segunda ruta depende de pocos TP y una conversión extrema. La versión actual conserva esa advertencia sin desactivar silenciosamente el voto solicitado.

### Cómo leer un 5/6

Un 5/6 permite identificar la fricción concreta. Si g4=0, no se ha alcanzado el criterio de ruptura unilateral ni bilateral aunque existan ritmo, referencias y escenario de tres goles. Se puede proponer una línea menor; la cuota alternativa debe comprobarse por separado.

g5=1 y g6=1 no son pruebas independientes: AP y remates pueden describir el mismo dominio. El sistema conserva su utilidad como reglas de lectura; no calcula una confianza multiplicando probabilidades de las seis compuertas.


## 19 / Acoplamiento y orden de prioridad
POLÍTICAS · RESOLVER CONFLICTOS SIN REESCRIBIR BITS

| Orden | Política | Efecto |
| --- | --- | --- |
| 1 · Base | Sumar bits disponibles y aplicar desempates numéricos. | Obtener dirección inicial AH y O/U. |
| 2 · AH | Doble correlación: ambos bloques con N≥3, no favorito Cover≥60% y derrotas=0, favorito Cover≤40%. | Cambiar AH al no favorito. |
| 3 · AH | Over normalizado≥5; GF fusionado favorito≥2 y no favorito≤1,35; Cover histórico no favorito=0 con N≥3; BTTS activo de ambos≥80%. | Puede cambiar AH al favorito. Todos los requisitos son conjuntos, no alternativos. |
| 4 · O/U | g1 activo; o referencias ≥80% con 3+ y Mq≥Gq+3; o desplazamiento ≥2 respaldado por ambos con 4+ reciente. | Cambiar dirección a Over y registrar cada motivo. No añadir puntos ficticios por encima de seis. |
| 5 · O/U | Cerrojo confirmado: cinco activos, cuatro porterías a cero y TP concedidos≤2. | Under tiene prioridad; registrar el conflicto si había señal Over. |
| 6 · AH | Exigencia del favorito ≥1,5 y al menos dos empates en una ventana activa de N≥3. | Veto final al favorito largo; dirección al no favorito con el hándicap principal. |

La especificación original contiene reglas de distintas versiones que pueden discrepar. Esta tabla fija una precedencia ejecutable. La prioridad del cerrojo y el desempate en puntuación tres son decisiones de implementación, no resultados establecidos por una publicación.

El favorito invertido participa en b2 y en el diagnóstico de tabla. La ruptura unilateral participa en g4. No se vuelven a sumar como votos extra al final. Los bits permanecen iguales y policy_trace conserva la secuencia de cambios de dirección.

Una media de 1,35 goles no es un techo: ese equipo puede marcar dos o más. La política Over→AH se implementa como hipótesis de selección, nunca como una demostración de que el empatador está “condenado” a perder.


## 20 / De la puntuación al pick
DECISIÓN · DIRECCIÓN, LÍNEA Y DISPONIBILIDAD

| Estado base | AH | O/U |
| --- | --- | --- |
| Puntuación >3 | Favorito. Con puntuación≤4 se reduce un cuarto de exigencia. | Over. Si es menor que 6, se reduce un cuarto; desde 3,0 se propone 2,5. |
| Puntuación <3 | No favorito con el hándicap principal. | Under con la línea principal. |
| Puntuación =3 | Comparar margen proyectado con /AH/. Si falta proyección, usar brecha de triangulación; último desempate: no favorito. | Comparar total proyectado con la línea. Si falta, usar referencias; último desempate: Under. |
| Tras políticas | La dirección final puede cambiar. Un favorito −0,75 con H2H ganado por uno propone −0,50. | Los vetos y el cerrojo deciden la dirección; la puntuación original sigue visible. |
| Cuota alternativa ausente | Conservar línea objetivo y marcar TARGET_UNQUOTED. | Igual: no fabricar cuota, EV ni rentabilidad. |

```text
I_normalizado = 6 × votos positivos / compuertas disponibles
Mostrar siempre: votos positivos, denominador real, máscara y políticas.
```

La zona tres/cuatro deja de ser una abstención genérica: se entrega dirección, se identifica la fricción y se propone una línea. La salida a cuotas desconocidas sigue siendo una propuesta de lectura, no una operación ya ejecutada. El motor no recomienda stakes ni utiliza la etiqueta “apostar fuerte”.

Los errores de datos sí se detienen: signo imposible, futuro conocido, equipos incoherentes o ausencia completa de historial elegible en uno de los equipos. Producir un pick con esos campos inventados no ejecutaría la metodología. Con datos parciales válidos se mantienen dirección y máscara.

No se implementa una entrada Live automática al minuto 18–22. El estado 0:0 cambia el problema y exige datos y cuotas de ese instante. La entrega funciona prepartido FT90; Live necesita otro contrato de entrada y evaluación.


## 21 / Un partido completo, paso a paso
CASO SINTÉTICO · DOMINIO.JSON

| Entrada relevante | Favorito A | No favorito B |
| --- | --- | --- |
| AH de plataforma / total | +0,75 local; O/U 2,75 | Recibe +0,75 |
| Activos: GF / GC | 2,4 / 1,2 | 1,2 / 2,0 |
| Tiros / TP / AP a favor | 16 / 7 / 65 | 11 / 4 / 45 |
| TP concedidos / cuota de TP | 3 / 70% | 6 / 40% |
| Cover en ambos bloques | 75%, N=4 por bloque | 25%, N=4 por bloque |
| Cruces comunes | Brecha media ajustada +5 cuartos | Exigencia actual al favorito: 3 |
| H2H / previo de A | H2H 1:0 dando −0,5 | Previo A 2:1 dando −1,25: media pérdida |

```text
b = [1, 1, 1, 0, 1, 1] → 5/6
g = [1, 1, 1, 0, 1, 1] → 5/6
```

b4=0 conserva la fricción del previo; b5=1 reconoce +3 TP y +20 AP. g4=0 porque la brecha de cinco cuartos no supera el total de once y no se cumplen ambas defensas abiertas. g5=1 con 110 AP; g6=1 con 27 tiros. El escenario g1 favorece Over y queda anotado.

Pick resultante: Local −0,50 a cuota ficticia 1,72; Over 2,50 a cuota ficticia 1,83. El H2H de un gol motiva reducir −0,75; el 5/6 de goles propone bajar 2,75 a 2,50.

El resultado ficticio 2:1 usado para probar postmatch genera +0,72 y +0,83 unidades, respectivamente. Es una prueba de liquidación del ejemplo, no un pronóstico histórico acertado ni evidencia de rentabilidad. El paquete previo y su hash se conservan sin recalcularlo después del resultado.


## 22 / Cuatro escenarios reproducibles
CASOS DE SOFTWARE · DIRECCIÓN Y MÁSCARA

Figura 3. Verde: bit 1. Gris: bit 0 evaluado. Rayado: regla sin datos suficientes, no voto negativo. Todos los escenarios son inventados.

| Escenario | Salida AH | Salida O/U | Qué verifica |
| --- | --- | --- | --- |
| Dominio | Local −0,5 | Over 2,5 | Combinar remates, similares, triangulación y fricción del previo. |
| Cerrojo | Local −0,5 como objetivo | Under 3,0 | Cuatro o más porterías a cero con TP concedidos bajos; g=[0,0,0,0,0,0]. |
| Datos parciales | Local −0,5 como objetivo | Over 2,5 como objetivo | No convertir ausencia de similares/AP en evidencia contraria; mostrar tres bits válidos en cada módulo. |
| Línea larga | Visitante +1,5 como objetivo | Over 2,5 como objetivo | El veto de empates del favorito largo prevalece aunque otras reglas lo apoyen. |

La salida diferencia “QUOTED” de “TARGET_UNQUOTED”. En el escenario cerrojo se aporta precio para Under 3,0 y no para el AH alternativo. En datos parciales y línea larga no se aporta ninguna cuota: las líneas se presentan como objetivos.

Los ejemplos comprueban que las reglas se ejecutan y que su resultado puede explicarse. La evaluación deportiva empieza con snapshots reales registrados antes de conocer el marcador. No se han reconstruido artificialmente las 24 variables de los quince casos narrados por el usuario.


## 23 / Arquitectura de aplicación
PYTHON + PLATAFORMA · EJECUCIÓN BAJO DEMANDA

| Componente | Responsabilidad | Salida |
| --- | --- | --- |
| Exportador de plataforma | Extraer datos del partido seleccionado: cabecera, similares, H2H, listas, espejo y fichas. | Snapshot JSON con IDs, fechas y definiciones. |
| contracts.py | Verificar tipos, signos, muestras, tiempos, coherencia de resultados y telemetría. | Entrada validada o error concreto de datos. |
| market.py + engine.py | Calcular métricas, filtros, proyección por ratios, doce bits, políticas y líneas. | Análisis determinista completo. |
| Modal / prompt | Mostrar cálculo, disponibilidad, fricción y pick; adjuntar el JSON exacto. | Misma salida visible y enviada al auditor. |
| audit_log.py + postmatch.py | Conservar análisis previo y añadir resultado confirmado y liquidación. | Registro enlazado por hash. |
| review_gemini.py | Solicitar revisión estructurada del paquete; validar formato, hash y referencias. | Observaciones y propuestas; ninguna mutación automática. |

El adaptador opcional api.py expone POST /analyze en un servidor local. motor_button.js crea un botón y llama al backend solo al pulsarlo. Se deben conectar exportSnapshot(card) y renderAnalysis(result) a la plataforma real. No se inventan selectores para un DOM que no se ha aportado.

```text
from contracts import Snapshot
from engine import analyze

snapshot = Snapshot.model_validate(datos_del_partido)
resultado = analyze(snapshot)
print(resultado["ah_pick"])
print(resultado["ou_pick"])
```

No se han modificado precacheo.html, explorer.html ni el JavaScript de producción. Se entrega el núcleo y el contrato del adaptador. La clave de Gemini pertenece al backend/entorno, nunca al navegador ni al JSON del partido.


## 24 / Ejecutarlo desde cero
GUÍA REPRODUCIBLE · COMANDOS DEL PAQUETE

### 1. Crear entorno, comprobar y generar ejemplos

```text
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
python demo.py
```

Ejecutar dentro de quant_v62. En Windows se activa con .venv\Scripts\activate. El núcleo solo requiere Pydantic además de la biblioteca estándar. Los esquemas y los cuatro ejemplos se generan en examples; los análisis y peticiones, en outputs.

### 2. Analizar un snapshot de la plataforma

```text
python run_engine.py \
  --input examples/dominio.json \
  --out outputs/mi_analisis.json \
  --ledger outputs/audit.sqlite
```

Sustituir el ejemplo por datos reales y revisar provider_definitions. La opción ledger es opcional; un hash ya insertado no se vuelve a añadir. No se exige un servicio de Google para calcular los bits.

### 3. Preparar feedback sin enviarlo

```text
python review_gemini.py \
  --packet outputs/mi_analisis.json \
  --out outputs/peticion.json
```

### 4. Conectar el botón durante el desarrollo local

```text
python -m pip install -r requirements-web.txt
uvicorn api:app --host 127.0.0.1 --port 8765
```

La ruta web es un adaptador local, no un despliegue público autenticado. El parser debe respetar la convención de signos, los null y la disponibilidad temporal. El README detalla cómo configurar los orígenes del frontend.


## 25 / Pedir feedback útil a Gemini
REVISIÓN DEL MODELO DE GOOGLE · CONTRATO

Se interpreta “modelo de Google” como Gemini mediante API. Se envía el snapshot junto a los cálculos, configuración, fuentes y hash. La documentación oficial permite respuesta estructurada mediante JSON Schema/Pydantic y recomienda validar los valores devueltos. [S09]

Instrucción aplicada: trabaja sobre estas doce compuertas; revisa signos NowGoal, denominadores de TP, eficacia, filtros de rol, triangulación, políticas y liquidaciones. Identifica el dato que hace correcto o incorrecto un cálculo. Distingue error de código, regla discutible y variación deportiva. No sustituyas el motor por Poisson ni por una abstención genérica. No inventes cuotas o probabilidades ni cambies el análisis original.

| Campo de respuesta | Función |
| --- | --- |
| packet_sha256 | Debe coincidir exactamente con el paquete enviado. |
| verdict / summary | Estado de la revisión y explicación; no reemplaza el pick. |
| issues | Gravedad, categoría, afirmación, evidence_refs y prueba sugerida. |
| hypotheses | Regla candidata, prueba que podría refutarla y datos necesarios. |
| limitations | Qué no puede concluirse con el contenido recibido. |

```text
response = client.interactions.create(
    model=os.environ["GEMINI_MODEL"],
    input=request["input"],
    system_instruction=SYSTEM,
    store=False,
    response_format=request["response_format"],
)
review = validate_review(response.output_text, packet)
```

Interactions es la interfaz principal utilizada. store=False desactiva la persistencia del objeto de interacción para encadenarlo; no sustituye las condiciones generales de tratamiento de datos de Google. Se conserva una alternativa explícita generate_content. [S10]

El código permite cuestionar una regla con evidencia. Prohibir toda discrepancia al auditor impediría detectar errores. Lo que se bloquea es la reescritura silenciosa del resultado calculado y la activación automática de una propuesta.


## 26 / Conexión y revisión posterior
GEMINI + RESULTADO · DE LA HIPÓTESIS A LA PRUEBA

### Activar una llamada real

```text
python -m pip install -r requirements-gemini.txt
# Configurar GEMINI_API_KEY y GEMINI_MODEL en el entorno.
python review_gemini.py \
  --packet outputs/mi_analisis.json \
  --out outputs/revision.json --live
```

Elegir un modelo de la cuenta que admita salida estructurada. La revisión puede consumir API. El programa no necesita que la clave se pegue en un prompt ni en el informe. Se verifica el esquema y se rechazan hashes incorrectos o referencias que no estén en el paquete.

### Añadir el resultado sin cambiar el pasado

```text
python postmatch.py \
  --packet outputs/dominio.json \
  --result examples/result_demo.json \
  --out outputs/postmatch_demo.json
python review_gemini.py \
  --packet outputs/postmatch_demo.json \
  --out outputs/postmatch_request.json
```

El resultado lleva event_id, goles local/visitante, FT90 confirmado, fuente y available_at. postmatch conserva el paquete previo y añade liquidaciones. Con una línea no cotizada se informa WIN/HALF_WIN/PUSH/HALF_LOSS/LOSS, pero el beneficio monetario queda null.

| Tipo de feedback | Respuesta adecuada |
| --- | --- |
| Dato o signo erróneo | Corregir el adaptador y registrar la incidencia; no ocultar el análisis original. |
| Cálculo de compuerta incorrecto | Añadir una prueba de regresión y cambiar la versión del software. |
| Pick pierde con cálculo correcto | Examinar si existe un patrón repetible; una derrota no demuestra que deba añadirse una excepción. |
| Umbral candidato | Declarar el cambio y su prueba antes de evaluar el siguiente bloque. |

Estado de esta entrega: petición previa y posterior generadas sin conexión; contrato probado con cliente simulado. No se ha realizado una llamada real a Gemini ni se ha obtenido feedback de su modelo en esta revisión.


## 27 / Aprender conservando la metodología
MEJORA EMPÍRICA · MOTOR VIGENTE Y CANDIDATO

El aprendizaje útil consiste en mejorar reglas concretas manteniendo la trazabilidad. El motor vigente continúa generando sus diagnósticos; una versión candidata calcula en paralelo sobre el siguiente bloque de partidos. La promoción se decide con sus resultados, no reetiquetando los partidos que originaron la idea.

| Etapa | Qué se conserva |
| --- | --- |
| Antes del partido | Snapshot, configuración, código/versionado, doce bits, máscaras, políticas, línea y cuota. |
| Después del partido | Resultado y fuente, pagos por unidad, incidencias del proveedor y explicación de discrepancias. |
| Hipótesis de cambio | Ejemplo: AP≥100 frente a AP≥110; razón y fecha en que se propone. |
| Prueba posterior | Mismos eventos para ambas versiones; separar diferencias de dirección, de línea y de cobertura de cuotas. |
| Decisión de versión | Cambios aceptados, rechazados o pendientes; nunca sobrescribir la historia del motor anterior. |

```text
python evaluate.py \
  --jsonl tus_partidos.jsonl \
  --out outputs/evaluacion.json
python evaluate.py \
  --jsonl bloque_futuro.jsonl --config candidato.json \
  --out outputs/evaluacion_candidato.json
```

Cada línea del JSONL contiene snapshot y result. El evaluador ordena por momento de decisión, rechaza eventos duplicados, ejecuta el motor y calcula liquidaciones y retorno hipotético solo donde hay cuota. Informa también las selecciones cotizadas para comprobar que dos versiones se comparan sobre la misma base.

El bootstrap agrupa AH y O/U del mismo partido, evitando tratarlos como resultados independientes. Es descriptivo: no resuelve toda la dependencia entre jornadas, equipos y ligas. No se fija un número mágico de partidos que impida empezar a registrar el motor.

Cawley y Talbot muestran que también puede sobreajustarse el proceso de selección de modelos. [S08] Aplicación concreta: no elegir el corte ganador entre muchos intentos y presentar esos mismos partidos como su prueba. El código no hace búsqueda automática de umbrales ni promociones silenciosas.


## 28 / Trazabilidad de los casos aportados
REGISTRO DE ORIGEN · QUÉ REGLA MOTIVÓ CADA CASO

Se conservan los quince casos como antecedentes de la especificación. Los nombres, marcadores y lecturas de esta tabla proceden del texto del usuario; no se han verificado aquí los encuentros, cuotas ejecutadas ni snapshots completos. Sirven para organizar futuras pruebas de regresión con sus datos originales.

| Caso aportado | Señal a reproducir con sus datos |
| --- | --- |
| Pachuca / Cruz Azul | Cebo de tabla, línea corta y triangulación. |
| Deportivo Quito W / Bolívar W | Presa, brecha de tasación y ruptura unilateral. |
| SD Atlético Nacional / Águilas UP | Conversión elevada, escenario 1+2=3 y ritmo AP. |
| Fort Lauderdale W / Carolina Ascent W | Margen exigido frente a brecha y cerrojo. |
| Auckland City / Auckland United | Filtro de filiales y cambio de rol. |
| Independiente Luque / Paraguarí | Doble correlación y AP elevados. |
| Albirex Niigata W / JEF Chiba W | Apoyo al no favorito y ritmo bajo. |
| Bucheon / Gwangju | Tabla contra recuperación de remates. |
| Anyang / Jeonbuk | Correlación y fricción de empate. |
| Seoul / Gangwon | Favorito de margen corto y línea de protección. |
| Jeonnam / Daegu | Producción ofensiva y escenario de ambos marcando. |
| Omiya / Yokohama | Fusión de bloques y referencias de 3+. |
| Gimcheon / Daejeon | Acoplamiento Over→AH del empatador. |
| Ponte Preta / Fortaleza | Desplazamiento O/U y veto del favorito largo. |
| Geoje / Jungnang | Favorito invertido y cerrojo activo. |

Para convertir un caso en prueba reproducible hacen falta su JSON previo, fecha de captura, versión aplicada, cuota/mercado exactos y resultado confirmado. Los ejemplos sintéticos entregados son independientes de estos casos; no se han fabricado sus 24 variables.


## 29 / Verificación y alcance de la entrega
SOFTWARE COMPROBADO · SIGUIENTE PASO CONCRETO

| Área | Verificación realizada |
| --- | --- |
| Matemática de mercado | Cuartos exactos, inversión NowGoal, partición negativa y pagos completos/medios/push. Propiedad de compensación de mercados opuestos a cuota 2. |
| Datos | N=0, null frente a cero, referencias repetidas, solapamiento de muestras, datos futuros, equipos y telemetría incompatibles. |
| Reglas | Vectores completos y parciales, límites de TP/AP, prioridad de cerrojo, veto de línea larga, signo de ruptura y camino espejo. |
| Feedback | Paquete previo preservado, evento correcto, beneficio desconocido sin cuota, hash y referencias en respuesta simulada de Gemini. |
| Evaluación | Dos mercados en un evento, ausencia de cuota, rechazo de duplicados y bootstrap reproducible. |
| Trazabilidad | Registro append-only local y rechazo de modificaciones/reinserciones. |

```text
Resultado de pruebas: 38 pruebas automatizadas superadas.
Python 3.12 · Pydantic 2.14.1 en el entorno de verificación.
```

No se ha medido la rentabilidad del motor con un conjunto real de partidos, no se han ejecutado apuestas y no se ha conectado el DOM de producción. El adaptador web es una interfaz de desarrollo; la llamada real a Google queda pendiente de las credenciales y modelo de la cuenta.

### Cómo empezar a usarlo con la plataforma

Exportar un partido completo al esquema entregado. Ejecutarlo y contrastar manualmente el signo de AH, la lista de filas excluidas y los dos bloques similares. Verificar que los doce bits se explican con los valores de la pantalla. Guardar ese paquete antes del inicio y añadir el resultado cuando esté confirmado. Así el primer paso produce a la vez un análisis útil y un registro verificable.

Se conserva el objetivo operativo: lectura de mercado, remates, eficacia y decisiones binarias. La evidencia se utiliza para elegir buenas medidas y el código para ejecutar las reglas con precisión; ninguna de las dos cosas obliga a presentar como certeza un resultado deportivo futuro.


## 30 / Fuentes científicas y de datos
BIBLIOGRAFÍA · ENLACES Y NIVEL DE ACCESO

Fuentes primarias consultadas el 26 de octubre de 2026. Las citas [Sxx] permiten distinguir resultados publicados, preprints, documentación de proveedor y decisiones propias de implementación.

### S01

Lago-Peñas, C.; Lago-Ballesteros, J.; Dellal, A.; Gómez, M. (2010). Game-Related Statistics that Discriminated Winning, Drawing and Losing Teams from the Spanish Soccer League. Journal of Sports Science and Medicine, 9, 288–293. Artículo y texto del editor.

https://jssm.org/jssm-09-288.xml-Fulltext

### S02

Wheatcroft, E. (2020). A profitable model for predicting the over/under market in football. International Journal of Forecasting, 36(3), 916–932. DOI: 10.1016/j.ijforecast.2019.11.001. Resumen editorial y resumen del manuscrito del autor recuperados; acceso directo al PDF restringido en esta consulta.

https://eprints.lse.ac.uk/103712/1/Predict_total_goals_LSE.pdf

### S03

Wheatcroft, E.; Sienkiewicz, E. (2021). A Probabilistic Model for Predicting Shot Success in Football. arXiv:2101.02104v1. Preprint; texto completo consultado.

https://arxiv.org/html/2101.02104v1

### S04

Mead, J.; O’Hare, A.; McMenemy, P. (2023). Expected goals in football: Improving model performance and demonstrating value. PLOS ONE, 18(4), e0282295. Artículo de investigación.

https://doi.org/10.1371/journal.pone.0282295

### S05

Stats Perform. Opta Event Definitions. Definiciones del proveedor para tiros, tiros a puerta, precisión y conversión. Consultado el 26-10-2026.

https://www.statsperform.com/opta-event-definitions/


## 31 / Fuentes de mercado y software
BIBLIOGRAFÍA · ENLACES Y NIVEL DE ACCESO

Fuentes primarias consultadas el 26 de octubre de 2026. Las citas [Sxx] permiten distinguir resultados publicados, preprints, documentación de proveedor y decisiones propias de implementación.

### S06

Levitt, S. D. (2004). Why are Gambling Markets Organised so Differently from Financial Markets? The Economic Journal, 114, 223–246. DOI: 10.1111/j.1468-0297.2004.00207.x. Artículo original.

https://pricetheory.uchicago.edu/levitt/Papers/LevittWhyAreGamblingMarkets2004.pdf

### S07

Shin, H. S. (1991). Optimal Betting Odds Against Insider Traders. The Economic Journal, 101(408), 1179–1185. DOI: 10.2307/2234434. Registro editorial verificado; texto completo no recuperado.

https://academic.oup.com/ej/article-abstract/101/408/1179/5190137

### S08

Cawley, G. C.; Talbot, N. L. C. (2010). On Over-fitting in Model Selection and Subsequent Selection Bias in Performance Evaluation. JMLR, 11, 2079–2107. Artículo metodológico.

https://www.jmlr.org/papers/v11/cawley10a.html

### S09

Google AI for Developers. Structured outputs. JSON Schema, Pydantic y validación de valores. Documentación oficial consultada el 26-10-2026.

https://ai.google.dev/gemini-api/docs/structured-output

### S10

Google AI for Developers. Interactions API. Interfaz, estado y store=false. Documentación oficial consultada el 26-10-2026.

https://ai.google.dev/gemini-api/docs/interactions-overview

Búsqueda complementaria: se intentó consultar Consensus (límite mensual alcanzado) y Elicit (cuenta sin acceso API). No se atribuyen resultados a esas herramientas. Se recurrió a artículos, repositorios de autores y documentación oficial. La especificación del usuario es la fuente de los umbrales operativos y de los quince casos narrados.
