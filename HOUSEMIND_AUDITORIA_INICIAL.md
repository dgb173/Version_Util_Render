# Auditoria inicial HouseMind

Fecha: 2026-07-10

## Estado del proyecto

El proyecto no necesita reconstruirse desde cero. Ya contiene:

- app Flask activa con rutas de explorador, precacheo, scraper y grandes ligas;
- SQLite principal en `data/app_data.db`;
- JSON historicos por familias AH;
- scripts de scraping y sincronizacion;
- motor `scripts/pattern_miner_v2`;
- sistema `scripts/explorador_automejora`;
- modelos `.joblib` y metricas en `models/`;
- reglas y auditorias previas de clave dicotomica.

No se encontro un archivo activo llamado `line_risk.py`. La logica equivalente esta repartida entre:

- `scripts/pattern_miner_v2/settle_asian.py`;
- `src/modules/bookie_decoder.py`;
- `src/modules/clave_dicotomica.py`;
- `src/modules/scah_analyzer.py`;
- `scripts/explorador_automejora/analyze_handicap_math.py`;
- `scripts/explorador_automejora/analyze_bookie_positioning.py`;
- `scripts/explorador_automejora/backtest_by_handicap.py`.

## Base disponible

SQLite:

- `matches`: 13.057 filas;
- `historical`: 12.683;
- `precacheo`: 371;
- `pending_results`: 3;
- `history_cached`: 6.669;
- `history_pending`: 0.

JSON historicos por familia:

- total bruto: 10.106 partidos;
- `match_id` unicos: 10.106;
- con marcador final parseable: 8.411;
- con AH actual: 8.782;
- con O/U actual: 8.782.

Distribucion principal por bucket en SQLite:

- `data_ah_0.5.json`: 5.361;
- `data_ah_1.5.json`: 2.446;
- `data_ah_0.json`: 2.292;
- `data_minus_ah_1.5.json`: 988;
- `data_minus_ah_0.5.json`: 615;
- `data_ah_2_plus.json`: 603;
- `data_precacheo.json`: 371;
- `data_minus_ah_2_plus.json`: 291;
- `data_unknown.json`: 87.

## Riesgos tecnicos detectados

1. Convencion AH no bloqueada

`settle_asian.py` liquida AH desde perspectiva HOME con convencion estandar:

- AH > 0: HOME recibe ventaja;
- AH < 0: HOME concede ventaja.

Pero el helper `get_favorite_side()` conserva comentarios contradictorios sobre la convencion del proyecto:

- AH positivo se interpreta en varias partes como favorito local;
- AH negativo se interpreta como favorito visitante;
- AH 0 aparece tratado como pickem/DNB, aunque algun comentario antiguo lo liga al visitante.

Esto debe cerrarse antes de entrenar HouseMind. Si no, el modelo puede aprender patrones invertidos.

2. Fechas sin normalizar

La consulta SQLite `min(match_date), max(match_date)` devuelve valores lexicograficos (`1/1/2025`, `9/28/2024`), no un rango temporal fiable. Para validacion cronologica hay que convertir todas las fechas a `YYYY-MM-DD` o timestamp.

3. Base no canonica

Los datos estan en tres capas:

- SQLite operativa;
- JSON legacy por buckets;
- objetos compactados `explorer_json`.

Para entrenar no conviene leer directamente de la app. Hace falta una tabla/fichero canonico inmutable con una fila por partido.

4. Settle incompleto para etiquetas finales

Existe liquidacion asiática `W/HW/P/HL/L`, pero HouseMind necesita guardar etiquetas explicitas:

- `FULL_WIN`;
- `HALF_WIN`;
- `PUSH`;
- `HALF_LOSS`;
- `FULL_LOSS`.

Tambien debe calcularse desde la perspectiva exacta del pick: favorito, no favorito, home o away.

5. Tests no ejecutables en el entorno actual

`python -m pytest -q` falla porque `pytest` no esta instalado. Ademas algunos archivos en `tests/` son scripts manuales contra localhost, no pruebas automatizadas reproducibles.

## Lo utilizable ya

Para el primer prototipo serio hay material suficiente:

- unas 8.400 filas con marcador final;
- unas 8.700 filas con AH/O-U;
- H2H estadio/general, Col3, indirectas, ultimos partidos y estadisticas dentro del payload;
- mineros y backtests ya escritos;
- modelos previos con metricas por bucket.

Las metricas actuales de `models/training_metrics.json` muestran que los modelos previos son solo baseline. Ejemplos:

- AH 0.5 / AH: accuracy aprox. 50,6%;
- AH 0.5 / O-U: accuracy aprox. 59,6%;
- AH 1.5 / AH: accuracy aprox. 59,3%;
- AH 2+ / AH: accuracy aprox. 56,8%.

No son aun un HouseMind: sirven como comparador, no como sistema final.

## Siguiente paso recomendado

Crear una capa canonica nueva:

```text
scripts/housemind/build_canonical_dataset.py
data/housemind/canonical_matches.parquet
data/housemind/data_audit_report.json
data/housemind/data_audit_report.md
```

Debe hacer:

- leer SQLite y JSON legacy;
- deduplicar por `match_id`;
- normalizar fecha;
- extraer `home_team`, `away_team`, liga, AH, O/U, resultado y fuentes H2H;
- fijar `fav_side` con una unica convencion;
- calcular settlement AH y O/U con medias victorias;
- marcar calidad/fugas: sin resultado, sin AH, sin O/U, H2H posterior, fecha dudosa, estadisticas ausentes;
- separar `historical`, `precacheo` y `pending_results`;
- dejar lista de partidos excluidos con motivo.

Bloqueo manual restante:

Hace falta confirmar con 2 o 3 ejemplos reales la semantica exacta de `main_match_odds.ah_linea`:

- si `+1.75` significa favorito local concediendo 1.75;
- si `-1.75` significa favorito visitante concediendo 1.75;
- como se debe leer `0`;
- si el movimiento `0.25 -> 1.75` compara linea anterior H2H contra actual o apertura contra cierre del mismo partido.
