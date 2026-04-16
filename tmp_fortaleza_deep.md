# Analisis profundo: Fortaleza F.C vs Deportivo Pasto

- Fecha objetivo: 2026-03-25
- Kickoff detectado: 2026-03-25 01:00
- Match ID: 2922235
- Handicap actual: 0.25
- OU actual: 2.0
- Favorito por linea: LOCAL (favorito)
- Historicos similares usados: 7 (umbral solicitado: 12)

## Pronostico

- OU: **UNDER** (Over=42.78%, Under=57.22%, confianza=BAJA).
- Ganador Handicap: **VISITANTE (no favorito) (fuerte)** (FavCover=41.29%, NoFavCover=58.71%, Push=0.00%, confianza=BAJA).

## Diagnostico handicap y movimiento

- AH exacto: 45.2%
- AH cercano (|gap|<=0.25): 100.0%
- AH cercano (|gap|<=0.50): 100.0%
- Direccion movimiento H2H: 28.3%
- Movimiento numerico (endline/delta): 43.4%

## Diagnostico stats (tiros/ataques)

- Calidad tiros a puerta: 50.8%
- Calidad ataques peligrosos: 45.7%
- Calidad ataques: 46.8%
- Bloques stats bien correlacionados: 64.2%

## Lectura tecnica corta

Se priorizo la correlacion de handicap: AH exacto=45.2% y AH<=0.25=100.0%. El movimiento H2H acompana (dir=28.3%, numerico=43.4%). Las stats de soporte muestran cercania en tiros a puerta=50.8% y ataques peligrosos=45.7%. Con soporte=7, el modelo inclina OU hacia UNDER y AH hacia VISITANTE (no favorito) (fuerte). Contexto AH0.5: draw_risk=22.5% y autolearn_reliability=64.4%.

## Top similares

| # | Partido historico | Fecha | Marcador | AH | OU | Sim | AHcore | Mov | Stats | BlkStats | Over | FavCoverAH |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | Istres vs GRACES | - | 0:0 | 0.25 | 2.25 | 78.516 | 14.5 | 7.0 | 7.358 | 2 | False | NOFAV_COVER |
| 2 | Montpellier vs USL Dunkerque | 2026-01-05 00:00 | 1:3 | 0.25 | 2.25 | 75.5 | 14.5 | 9.0 | 6.314 | 2 | True | NOFAV_COVER |
| 3 | Saarbrucken vs VfB Stuttgart II | 2026-02-14 00:00 | 2:0 | 0.25 | 2.75 | 74.275 | 15.25 | 9.5 | 6.479 | 2 | False | FAV_COVER |
| 4 | Lanus vs Talleres Cordoba | 2026-02-09 00:00 | 1:1 | 0.5 | 1.75 | 71.792 | 11.5 | 7.0 | 8.922 | 2 | True | NOFAV_COVER |
| 5 | Correcaminos II vs Calor de San Pedro | 2026-02-14 00:00 | 0:1 | 0.5 | 2.25 | 69.975 | 11.5 | 8.25 | 7.504 | 2 | False | NOFAV_COVER |
| 6 | Red Star Waasland vs Beerschot Wilrijk | 2025-12-20 00:00 | 4:2 | 0.5 | 2.5 | 68.604 | 11.5 | 5.5 | 7.096 | 2 | True | FAV_COVER |
| 7 | Las Palmas vs Sporting Gijon | 2026-03-22 00:00 | 1:0 | 0.5 | 2.25 | 65.99 | 12.25 | 7.75 | 7.985 | 2 | False | FAV_COVER |

## Stats por bloque

- col3: {"target_total_value": 13.0, "target_total_hist_avg": 10.28, "target_total_close_pct": 79.19, "danger_total_value": 88.0, "danger_total_hist_avg": 90.14, "danger_total_close_pct": 79.53, "attacks_total_value": 174.0, "attacks_total_hist_avg": 178.82, "attacks_total_close_pct": 100.0}
- prev_home: {"target_total_value": 11.0, "target_total_hist_avg": 10.32, "target_total_close_pct": 100.0, "danger_total_value": 49.0, "danger_total_hist_avg": 103.8, "danger_total_close_pct": 17.72, "attacks_total_value": 156.0, "attacks_total_hist_avg": 179.67, "attacks_total_close_pct": 67.05}
- prev_away: {"target_total_value": 12.0, "target_total_hist_avg": 9.4, "target_total_close_pct": 58.1, "danger_total_value": 98.0, "danger_total_hist_avg": 101.6, "danger_total_close_pct": 100.0, "attacks_total_value": 198.0, "attacks_total_hist_avg": 179.83, "attacks_total_close_pct": 71.42}
- ind_left: {"target_total_value": 13.0, "target_total_hist_avg": 10.2, "target_total_close_pct": 60.68, "danger_total_value": 80.0, "danger_total_hist_avg": 89.56, "danger_total_close_pct": 78.6, "attacks_total_value": 153.0, "attacks_total_hist_avg": 170.91, "attacks_total_close_pct": 81.3}

## Autoaprendizaje AH 0.5

- Contexto detectado: `fav_side=HOME|movement_pair=DOWN|DOWN|ou_band=LOW|draw_risk_band=LOW|col3_wdl=DRAW`
- Riesgo de empate del contexto: 22.5%
- Fiabilidad del aprendizaje: 64.4%
- Ajuste FavCover por aprendizaje: -0.1%
- Feedback real: sin_feedback_explicito