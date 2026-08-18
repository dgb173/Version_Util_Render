# HouseMind O/U v1

Sistema probabilistico con validacion cronologica y abstencion. No existe garantia de acierto total.

## Veredicto auditable

- Modelo habilitado: NO
- Umbral de seleccion: 54.0%
- Holdout intocable: 326 picks de 1475 partidos (22.1% cobertura)
- Acierto direccional holdout: 50.92%
- Limite inferior Wilson 95%: 45.51%
- Brier holdout: 0.251387 (base 0.24997)
- AUC holdout: 0.50811
- Liquidacion media a cuota par teorica: 0.02147

La liquidacion a cuota par es una prueba estadistica, no ROI real: la base no conserva cuotas O/U historicas completas.

## Particiones temporales

- train: 4959 partidos, 2024-08-10 a 2026-02-22
- calibration: 1176 partidos, 2026-02-23 a 2026-03-22
- holdout: 1475 partidos, 2026-03-23 a 2026-06-30

## Auditoria de datos

- rows_historical: 12683
- samples_dated_sorted: 7610
- samples_usable: 7610
- reject:feature_quality: 4559
- reject_reason:missing_match_date: 4030
- settlement:negative: 3900
- settlement:positive: 3710
- settlement:half: 988
- reject:ou_push: 514
- reject_reason:fewer_than_3_contexts: 403
- reject_reason:missing_ah: 86
- reject_reason:missing_ou: 86
- reject_reason:no_stats_context: 56
- reject_reason:nonpast_contexts:1: 1
- samples_with_filtered_nonpast_context: 1

## Senales principales hacia Over

- LEAGUE=SPAIN_YOUTH_LEAGUE: +0.1947
- LEAGUE=SPANISH_LA_LIGA_2: +0.1602
- LEAGUE=ENGLAND_LEAGUE_1: +0.1331
- H2H_COL3_AGE=OLD: +0.1249
- PREV_AWAY_TOTAL=PUSH: +0.1226
- PREV_AWAY_AH=UNKNOWN: +0.1216
- LEAGUE=SAUDI_ARABIA_DIVISION_1: +0.1196
- LINE_PAIR=MID|LOW: +0.1181
- LEAGUE=SPAIN_SEGUNDA_DIVISION_RFEF: +0.1162
- LEAGUE=ISRAEL_LIGA_ALEF: +0.0983
- AH_EXACT=M2: +0.0979
- IND_LEFT_AGE=OLD: +0.0975
- LEAGUE=ENGLAND_NATIONAL_LEAGUE_NORTH: +0.0925
- LINE_PAIR=HIGH|EXTREME: +0.0899
- LEAGUE=ENGLAND_NATIONAL_LEAGUE_SOUTH: +0.0898

## Senales principales hacia Under

- LEAGUE=ENGLAND_LEAGUE_2: -0.1955
- CONSENSUS_LINE=OVER_LEAN|EXTREME: -0.1545
- H2H_STADIUM_TOTAL=PUSH: -0.1497
- H2H_STADIUM_DANGER=LOW: -0.1477
- PREV_HOME_AGE=OLD: -0.1237
- LINE_PAIR=LOW|EXTREME: -0.1227
- H2H_COL3_GOALS=2: -0.1162
- AH_EXACT=3_5: -0.1146
- LEAGUE=SPAIN_PRIMERA_DIVISION_RFEF: -0.1099
- AH_EXACT=M2_5: -0.1089
- CONSENSUS_LINE=UNDER_STRONG|EXTREME: -0.1044
- AH_EXACT=M1: -0.1042
- AH_EXACT=M1_5: -0.1016
- PREV_AWAY_SOT=LOW: -0.1000
- LEAGUE=FRANCE_LIGUE_5: -0.0985

## Picks desde 2026-07-11

Seleccionados: 0 | NO BET: 645

- Sin picks que superen el umbral auditable.
