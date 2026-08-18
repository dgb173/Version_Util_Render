# Fallo -> mejora: Union FC Macomb vs Lansing City

## Caso

- Partido: Union FC Macomb vs Lansing City
- AH actual: Union FC Macomb `-2.25`
- O/U actual: `4.5`
- Resultado real indicado: `2:3`
- Resultado AH correcto: Lansing City `+2.25`
- Resultado O/U correcto: `OVER`

## Fallo anterior

El sistema anterior compraba demasiado facil al favorito largo por tres señales
visuales:

- H2H general favorable a Union.
- Lansing venia de derrota grande `6:0`.
- Union tenia volumen historico en el H2H estadio.

El error matematico estaba en no normalizar suficiente contra la linea actual.
Un H2H `3:2` puede servir para `-0.25`, pero no paga un `-2.25`.

## Mejora entrenada

Se busco el patron en partidos terminados del Explorador usando el entrenador
`train_binary_market_system_v2.py`.

Regla encontrada en modo diagnostico:

- Clave: `AH_FAMILY=AH_2_PLUS + BASE_STATS=STATS_STRONG_FOR + OU_4_PLUS`
- Direccion validada: `DOG`
- Train: `58/93 = 62.37%`
- Validacion temporal: `11/14 = 78.57%`
- Lift validacion sobre baseline DOG: `+28.98`

Lectura:

- En AH `2+`, el volumen historico del favorito deja de ser confirmacion.
- Con O/U `4+`, el partido permite varianza y goles, pero no obliga a que el
  favorito cubra una goleada.
- Si el favorito no valida el margen en previas/indirectas, la casa puede estar
  inflando la linea por el castigo reciente al rival.

## Regla incorporada al render

Si se cumple:

- AH actual `2+`
- O/U actual `4+`
- H2H/base con estadistica fuerte para el favorito

Entonces:

- sumar penalizacion contra el favorito
- mostrar razon: `regla entrenada AH2+ OU4+ no fav`
- combinar con frenos existentes:
  - `H2H volumen no paga 2+`
  - `favorito no confirma AH alto`
  - `inflacion por rival comun`

## Resultado despues de la mejora

Salida del motor:

- AH: `Lansing City +2.25`
- O/U: `OVER`
- Motivo central: la linea esta inflada para el favorito largo; el over puede
  salir sin que el favorito cubra.

