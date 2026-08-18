# Sistema binario real v2

Entrenado con partidos terminados del Explorador y filtro de calidad previo.

## Resumen

- loaded_rows: 8391
- usable_rows: 5084
- quality_rejects: 3307
- train_rows: 3813
- validation_rows: 1271
- validation_favorite: {'bets': 1232, 'wins': 621, 'hit_rate': 50.41}
- validation_dog: {'bets': 1232, 'wins': 611, 'hit_rate': 49.59}
- validation_under: {'bets': 1177, 'wins': 598, 'hit_rate': 50.81}
- validation_over: {'bets': 1177, 'wins': 579, 'hit_rate': 49.19}

## Top reglas AH

- DOG | val 21/28 (75.0%) | train 30/45 (66.67%) | lift +25.41 | AH_FAMILY=AH_0_25 + BASE_STATS=STATS_STRONG_FOR + IND_DOG_STATS_STRONG_FOR
- DOG | val 11/15 (73.33%) | train 32/48 (66.67%) | lift +23.74 | DOG_RECENT_STATS_NEUTRAL + TABLE=TABLE_FAV_WORSE
- DOG | val 13/19 (68.42%) | train 59/88 (67.05%) | lift +18.83 | BASE_STATS=STATS_STRONG_FOR + IND_DOG_COVER_COVER + OU_FAMILY=OU_MID
- DOG | val 19/28 (67.86%) | train 32/49 (65.31%) | lift +18.27 | AH_SUPER=AHS_LOW + BASE_COVER=FAIL + IND_DOG_MARGIN_M_POS2
- DOG | val 20/30 (66.67%) | train 29/44 (65.91%) | lift +17.08 | AH_FAMILY=AH_0_25 + BASE_TOTAL=TOTAL_OVER_LINE + FAV_RECENT_STATS_NEUTRAL
- DOG | val 18/27 (66.67%) | train 41/59 (69.49%) | lift +17.08 | AH_FAMILY=AH_0_25 + BASE_TOTAL=TOTAL_UNDER_LINE + IND_DOG_COVER_COVER
- DOG | val 12/18 (66.67%) | train 43/64 (67.19%) | lift +17.08 | BASE_COVER=COVER + DOG_RECENT_MARGIN_M_POS1 + OU_FAMILY=OU_MID
- DOG | val 23/35 (65.71%) | train 38/55 (69.09%) | lift +16.12 | AH_FAMILY=AH_0_25 + BASE_COVER=COVER + IND_FAV_MISSING

## Top reglas Over/Under

- UNDER | val 23/28 (82.14%) | train 33/48 (68.75%) | lift +31.33 | AH_SUPER=AHS_LOW + BASE_PRESSURE=PRESSURE_NEW_FAV + DOG_RECENT_STATS_LEAN_AGAINST
- UNDER | val 23/31 (74.19%) | train 46/70 (65.71%) | lift +23.38 | BASE_PRESSURE=PRESSURE_NEW_FAV + DOG_RECENT_STATS_LEAN_AGAINST
- UNDER | val 15/21 (71.43%) | train 33/50 (66.0%) | lift +20.62 | IND_DOG_MARGIN_M_POS2 + OU_FAMILY=OU_LOW
- UNDER | val 24/35 (68.57%) | train 37/54 (68.52%) | lift +17.76 | IND_DOG_MARGIN_M_POS2 + TABLE=TABLE_UNKNOWN
- OVER | val 10/15 (66.67%) | train 28/42 (66.67%) | lift +17.48 | BASE_STATS=STATS_NEUTRAL + IND_DOG_STATS_STRONG_AGAINST + OU_FAMILY=OU_MID
- UNDER | val 14/21 (66.67%) | train 43/63 (68.25%) | lift +15.86 | BASE_PRESSURE=PRESSURE_NEW_FAV + H2H_STADIUM_COVER_COVER + OU_FAMILY=OU_LOW
- UNDER | val 10/15 (66.67%) | train 57/86 (66.28%) | lift +15.86 | BASE_PRESSURE=PRESSURE_RAISE_AGGRESSIVE + IND_DOG_GOALS_2_MINUS
- UNDER | val 10/15 (66.67%) | train 26/40 (65.0%) | lift +15.86 | BASE_TOTAL=TOTAL_PUSH_LINE + H2H_GENERAL_COVER_COVER + OU_FAMILY=OU_HIGH
- UNDER | val 17/26 (65.38%) | train 26/40 (65.0%) | lift +14.57 | AH_FAMILY=AH_0_25 + BASE_STATS=STATS_STRONG_FOR + IND_DOG_STATS_STRONG_FOR
- UNDER | val 15/23 (65.22%) | train 28/43 (65.12%) | lift +14.41 | AH_SUPER=AHS_LOW + BASE_PRESSURE=PRESSURE_SAME + IND_DOG_MARGIN_M_POS1

## Filtro de calidad

- deduped_rows: 8391
- reject:quality_lt_5: 3307
- reject:missing_stadium: 2608
- reject:missing_general: 2515
- loaded_file:data_ah_0.5.json: 2500
- reject:no_h2h_base: 2491
- reject:missing_dog_indirect: 2487
- reject:missing_fav_indirect: 2479
- loaded_file:data_ah_1.5.json: 2443
- loaded_file:data_ah_0.json: 2290
- reject:missing_dog_recent: 2056
- reject:missing_fav_recent: 2056
- reject:pickem_no_favorite_side: 2056
- warning:missing_stadium: 1637
- skip:no_final_score: 1329
- warning:missing_fav_indirect: 1242
- reject:obsolete_dog_recent: 1187
- warning:missing_dog_indirect: 1184
- reject:obsolete_fav_recent: 1161
- loaded_file:data_minus_ah_1.5.json: 985
- warning:missing_general: 969
- warning:no_h2h_base: 845
- reject:obsolete_fav_indirect: 814
- reject:obsolete_dog_indirect: 812
- reject:obsolete_general: 791
- reject:obsolete_stadium: 699
- loaded_file:data_minus_ah_0.5.json: 604
- loaded_file:data_ah_2_plus.json: 603
- warning:obsolete_stadium: 415
- loaded_file:data_minus_ah_2_plus.json: 295
- warning:obsolete_general: 185
- warning:obsolete_dog_recent: 94
- warning:obsolete_fav_recent: 62
- warning:obsolete_dog_indirect: 18
- warning:obsolete_fav_indirect: 13
