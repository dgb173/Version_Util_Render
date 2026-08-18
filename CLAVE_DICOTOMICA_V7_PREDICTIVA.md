# Clave Dicotomica V7 predictiva

## Objetivo

La V7 conserva la estructura creada en la Clave Dicotomica: handicap asiatico
como variable madre, residual contra el H2H, presion de linea, previas,
comparativas indirectas, H2H de estadio, Col3 y separacion estricta entre AH y
O/U.

La mejora principal no consiste en anadir mas reglas, sino en separar dos capas:

- `raw_ah` y `raw_ou`: lectura completa del mapa argumental.
- `ah` y `ou`: prediccion que supera la puerta de publicacion.

Una lectura que no supera la puerta queda como `OBSERVATION` y se publica como
`NO_BET`. Asi se conserva informacion para seguir aprendiendo sin convertir cada
argumento en una apuesta.

## Correcciones de datos

- El H2H de estadio usa el campo real `h2h_stadium` y mantiene el alias antiguo.
- Las indirectas leen `ah_line`, ademas de los nombres legacy.
- Col3 acepta su estructura anidada `col3_data`.
- Se descartan contextos del mismo dia o posteriores al partido analizado.
- Se detectan H2H duplicados para no contar dos veces el mismo encuentro.
- La presion distingue subida, bajada, favorito retirado y favorito nuevo.
- Si faltan nombres en el H2H, el favorito historico se infiere por la orientacion
  de la linea y no se marca automaticamente como favorito nuevo.
- La salida mantiene un contrato V7 completo incluso cuando faltan odds o H2H.

## Puerta AH

Una lectura AH solo pasa a `PRODUCTION` cuando:

1. Hay al menos tres fuentes de evidencia y ninguna procede del presente/futuro.
2. No aparece una bandera historicamente bloqueante.
3. No hay micro-reglas promovidas enfrentadas.
4. El edge estructural absoluto es al menos `3.5`, o una micro-regla promovida
   confirma la misma direccion.

Micro-reglas AH promovidas por estabilidad temporal:

- `MR-F4 H15_INDIRECTA_VALIDA`
- `MR-F9 H05_LINEA_IGUAL_DOG_EMPATA`
- `MR-D15 H2H_OVER_OU4`
- `MR-D16 BASE_COVER_OU4`

## Puerta O/U

O/U es mas restrictivo. Solo se publica `UNDER` cuando se activa
`MR-OU2 H025_H2H_UNDER_IND_DOG_POS`, no existe cambio de favorito y no hay
conflicto entre reglas OVER y UNDER. El resto de lecturas O/U se conserva como
observacion.

## Backtest cronologico

Base: 8.578 partidos, desde 2024-08-10 hasta 2026-06-30. El ultimo 20% temporal
se usa como tramo de comprobacion retrospectiva. No es una garantia futura ni
sustituye una validacion prospectiva con cuotas reales.

| Motor | Mercado | Picks tramo final | Acierto decidido | Wilson 95% | Settlement medio |
|---|---:|---:|---:|---:|---:|
| V6 | AH | 904 | 54,80% | 51,43% | +0,06527 |
| V7 | AH | 427 | 58,50% | 53,61% | +0,11827 |
| V6 | O/U | 737 | 51,17% | 47,43% | -0,00271 |
| V7 | O/U | 22 | 68,18% | 47,32% | +0,31818 |

En AH, la V7 mejora precision y settlement reduciendo cobertura a 25,25%. En
O/U la direccion es favorable en los cinco bloques temporales, pero la muestra
es pequena y su Wilson inferior sigue por debajo del 50%; debe considerarse una
rama selectiva en seguimiento, no una ventaja confirmada.

## Reproduccion

```powershell
python scripts/clave_dicotomica/backtest_clave.py `
  --output-json data/clave_dicotomica/backtest_v7_final.json `
  --output-md data/clave_dicotomica/BACKTEST_V7_FINAL.md
```

Pruebas especificas:

```powershell
python -m unittest tests.test_clave_dicotomica_v7 -v
```

## Uso en precacheo

La tarjeta muestra:

- `PRED ALTA` o `PRED MEDIA`: pick V7 publicable.
- `OBS`: existe una lectura interna, pero no supera el filtro de prediccion.
- `Q`: calidad estructural calculada.
- `B`: numero de bloques de evidencia disponibles.

El tooltip conserva la lectura raw, los motivos de abstencion y las reglas que
han promovido el pick.
