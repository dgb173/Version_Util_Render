# Marco universal de la clave dicotomica

Este documento deja clara la lectura del usuario en forma replicable para todos
los handicaps. No es una regla solo para `AH 0.25`: es una forma de ordenar las
variables.

## Idea central

La clave no debe sumar variables como si todas valieran igual. Cada partido es
un juego de barreras numericas.

```text
Primero se define que exige hoy la linea.
Despues se pregunta si el ultimo H2H general resolvio esa exigencia.
Luego se mira si el mercado mantiene, baja, sube o invierte esa exigencia.
Solo despues entran estadio, previas, indirectas, Col3, tabla y estadisticas.
```

## Jerarquia fija

```text
1. Ultimo H2H general entre ellos
   resultado + handicap + favorito historico + cobertura contra linea actual

2. Movimiento de handicap/favorito
   mantiene, baja, sube o invierte la obligacion

3. H2H estadio
   misma localia, misma familia AH, repeticion o contradiccion

4. Previas por localia real
   local en casa, visitante fuera; resultado + handicap + cobertura

5. Comparativas indirectas y Col3
   rival comun, empates, colapsos, margenes y coberturas

6. Tabla
   solo valida o contradice, no manda sola

7. Estadisticas
   explican proceso, volumen oculto o fragilidad; no borran resultado + handicap
```

## Formula para cualquier AH

```text
h = abs(AH_actual)
F = favorito actual
D = dog actual

RH = margen_F_en_H2H_general - h

RH >= +0.25  -> H2H_GENERAL_COVER
RH entre -0.24 y +0.24 -> H2H_GENERAL_PUSH
RH <= -0.25 -> H2H_GENERAL_FAIL
```

El resultado bruto no basta. El mismo marcador puede significar cosas opuestas:

```text
Favorito gana 2-1.

Con AH 0.25 -> cubre.
Con AH 1.25 -> no cubre.
Con AH 2.25 -> fallo fuerte.
```

## Variable dominante

Una sola variable puede decantar todo el sistema, pero solo si explica la linea.

Variables dominantes validas:

- H2H general con residual extremo.
- H2H estadio que repite localia y familia de AH.
- Cambio de favorito respecto al H2H anterior.
- Rebaja/subida de handicap que cambia el problema.
- Empate/push repetido en AH bajo.
- Linea O/U contraintuitiva que se mantiene alta o baja pese a precedentes.
- Resultado peor que proceso cuando afecta al mercado de goles.

Variables que no deben mandar solas:

- Tiros aislados.
- Tabla mejor sin soporte de handicap.
- Goleada previa sin saber si cubrio como favorito o dog.
- H2H estadio muy antiguo si contradice todo lo actual.

## Separacion AH y O/U

El AH y el O/U pueden ir en direcciones distintas.

```text
DOG AH + OVER:
  favorito no supera la barrera,
  pero el partido tiene volumen/varianza/defensas rotas.

DOG AH + UNDER:
  favorito no supera la barrera,
  y el mapa trae empate, push o ritmo bajo.

FAV AH + OVER:
  favorito cubre y el guion exige goles.

FAV AH + UNDER:
  favorito cubre por control minimo.
```

## Regla del over contraintuitivo

No todo H2H under implica under actual.

```text
Si OU actual >= 3.5
y los H2H fueron 2-1 o similares,
la diferencia contra la linea es solo medio gol.
```

Si la casa mantiene la linea alta, preguntar:

```text
Por que no baja el O/U si los H2H no lo superaron?
```

Confirmadores de over:

- Equipo local/favorito perdio o no cubrio, pero genero mucho volumen.
- Visitante/dog viene de ganar o cubrir fuera.
- Indirecta local rota tipo 1-4, 2-5, 3-3.
- Indirecta visitante empata/cubre contra el rival que castigo al local.
- Liga filial, juvenil, reserves o competicion de alta varianza.

Salida:

```text
Con 2 confirmadores -> bloquear UNDER.
Con 3+ confirmadores -> OVER o NO BET OU si hay conflicto.
```

## Checklist universal

```text
1. Que exige hoy el AH?
2. Quien era favorito en el ultimo H2H general?
3. Quien es favorito ahora?
4. El mercado mantiene, baja, sube o invierte?
5. El H2H general cubre/push/falla la linea actual desde F?
6. El H2H estadio confirma o contradice?
7. El estadio es reciente o solo memoria antigua?
8. La previa local cubrio con que handicap?
9. La previa visitante cubrio con que handicap?
10. Las indirectas confirman margen o muestran colapso?
11. Col3 marca empate/push o ruptura?
12. Tabla valida o contradice?
13. Estadisticas explican resultado peor/mejor que proceso?
14. O/U esta alineado con los marcadores o es contraintuitivo?
15. Hay una variable dominante de nivel alto?
16. Si no hay variable dominante clara, NO BET.
```

## Frase guia

```text
No se predice quien parece mejor.
Se resuelve que barrera puso la casa,
quien ya la supero o fallo en contextos comparables,
y que variable explica mejor por que esa barrera se mantiene, baja, sube o se invierte.
```

