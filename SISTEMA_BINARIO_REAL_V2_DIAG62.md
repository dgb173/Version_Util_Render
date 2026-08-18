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

- DOG | val 12/15 (80.0%) | train 25/40 (62.5%) | lift +30.41 | BASE_STATS=STATS_LEAN_FOR + FAV_RECENT_MARGIN_M_GE_POS3
- DOG | val 11/14 (78.57%) | train 58/93 (62.37%) | lift +28.98 | AH_FAMILY=AH_2_PLUS + BASE_STATS=STATS_STRONG_FOR + HIGH_AH_WITH_EXTREME_OU_VARIANCE
- DOG | val 11/14 (78.57%) | train 58/93 (62.37%) | lift +28.98 | AH_FAMILY=AH_2_PLUS + BASE_STATS=STATS_STRONG_FOR + OU_4_PLUS
- DOG | val 11/14 (78.57%) | train 58/93 (62.37%) | lift +28.98 | AH_SUPER=AHS_EXTREME + BASE_STATS=STATS_STRONG_FOR + HIGH_AH_WITH_EXTREME_OU_VARIANCE
- DOG | val 11/14 (78.57%) | train 58/93 (62.37%) | lift +28.98 | AH_SUPER=AHS_EXTREME + BASE_STATS=STATS_STRONG_FOR + OU_4_PLUS
- DOG | val 13/17 (76.47%) | train 64/103 (62.14%) | lift +26.88 | BASE_PRESSURE=PRESSURE_RAISE_AGGRESSIVE + DOG_RECENT_GOALS_2_MINUS
- DOG | val 21/28 (75.0%) | train 30/45 (66.67%) | lift +25.41 | AH_FAMILY=AH_0_25 + BASE_STATS=STATS_STRONG_FOR + IND_DOG_STATS_STRONG_FOR
- DOG | val 12/16 (75.0%) | train 54/85 (63.53%) | lift +25.41 | BASE_PRESSURE=PRESSURE_RAISE_AGGRESSIVE + IND_DOG_GOALS_2_MINUS
- FAVORITE | val 15/20 (75.0%) | train 22/35 (62.86%) | lift +24.59 | AH_SUPER=AHS_LOW + BASE_TOTAL=TOTAL_OVER_LINE + IND_FAV_COVER_PUSH
- DOG | val 11/15 (73.33%) | train 32/48 (66.67%) | lift +23.74 | DOG_RECENT_STATS_NEUTRAL + TABLE=TABLE_FAV_WORSE
- DOG | val 11/15 (73.33%) | train 62/99 (62.63%) | lift +23.74 | BASE_STATS=STATS_STRONG_FOR + HIGH_AH_WITH_EXTREME_OU_VARIANCE
- DOG | val 11/15 (73.33%) | train 62/99 (62.63%) | lift +23.74 | BASE_STATS=STATS_STRONG_FOR + HIGH_AH_WITH_EXTREME_OU_VARIANCE + OU_FAMILY=OU_EXTREME
- DOG | val 11/15 (73.33%) | train 30/48 (62.5%) | lift +23.74 | AH_FAMILY=AH_1_1_25 + BASE_PRESSURE=PRESSURE_NEW_FAV + IND_FAV_COVER_FAIL
- DOG | val 11/15 (73.33%) | train 30/48 (62.5%) | lift +23.74 | AH_SUPER=AHS_MID + BASE_PRESSURE=PRESSURE_NEW_FAV + IND_FAV_COVER_FAIL
- FAVORITE | val 19/26 (73.08%) | train 25/40 (62.5%) | lift +22.67 | AH_SUPER=AHS_LOW + BASE_STATS=STATS_NEUTRAL + FAV_RECENT_STATS_STRONG_AGAINST
- DOG | val 10/14 (71.43%) | train 24/37 (64.86%) | lift +21.84 | BASE_TOTAL=TOTAL_PUSH_LINE + IND_FAV_GOALS_2_MINUS + OU_FAMILY=OU_LOW
- DOG | val 10/14 (71.43%) | train 32/50 (64.0%) | lift +21.84 | BASE_PRESSURE=PRESSURE_RAISE + IND_FAV_VALIDATES_CURRENT_AH + OU_FAMILY=OU_LOW
- FAVORITE | val 19/27 (70.37%) | train 24/37 (64.86%) | lift +19.96 | AH_FAMILY=AH_0_5_0_75 + BASE_TOTAL=TOTAL_UNDER_LINE + IND_FAV_STATS_NEUTRAL
- DOG | val 16/23 (69.57%) | train 71/114 (62.28%) | lift +19.98 | BASE_STATS=STATS_STRONG_FOR + IND_DOG_GOALS_4_PLUS + OU_FAMILY=OU_MID
- DOG | val 25/36 (69.44%) | train 42/66 (63.64%) | lift +19.85 | AH_EXACT=0.25 + FAV_RECENT_STATS_LEAN_FOR

## Top reglas Over/Under

- UNDER | val 12/14 (85.71%) | train 32/50 (64.0%) | lift +34.9 | BASE_STATS=STATS_LEAN_FOR + IND_FAV_STATS_STRONG_FOR + OU_FAMILY=OU_MID
- UNDER | val 11/13 (84.62%) | train 28/36 (77.78%) | lift +33.81 | AH_FAMILY=AH_0_25 + BASE_TOTAL=TOTAL_UNDER_LINE + IND_DOG_MARGIN_M_POS1
- UNDER | val 23/28 (82.14%) | train 33/48 (68.75%) | lift +31.33 | AH_SUPER=AHS_LOW + BASE_PRESSURE=PRESSURE_NEW_FAV + DOG_RECENT_STATS_LEAN_AGAINST
- UNDER | val 23/30 (76.67%) | train 32/50 (64.0%) | lift +25.86 | BASE_COVER=FAIL + FAV_RECENT_MARGIN_M_NEG1 + OU_FAMILY=OU_HIGH
- UNDER | val 21/28 (75.0%) | train 29/46 (63.04%) | lift +24.19 | BASE_TOTAL=TOTAL_UNDER_LINE + FAV_RECENT_MARGIN_M_NEG1 + OU_FAMILY=OU_HIGH
- UNDER | val 23/31 (74.19%) | train 46/70 (65.71%) | lift +23.38 | BASE_PRESSURE=PRESSURE_NEW_FAV + DOG_RECENT_STATS_LEAN_AGAINST
- UNDER | val 24/33 (72.73%) | train 47/74 (63.51%) | lift +21.92 | BASE_PRESSURE=PRESSURE_NEW_FAV + BASE_STATS=STATS_STRONG_FOR + OU_FAMILY=OU_LOW
- UNDER | val 21/29 (72.41%) | train 35/54 (64.81%) | lift +21.6 | BASE_STATS=STATS_STRONG_FOR + IND_DOG_STATS_STRONG_FOR + OU_FAMILY=OU_LOW
- UNDER | val 15/21 (71.43%) | train 33/50 (66.0%) | lift +20.62 | IND_DOG_MARGIN_M_POS2 + OU_FAMILY=OU_LOW
- UNDER | val 12/17 (70.59%) | train 28/45 (62.22%) | lift +19.78 | BASE_STATS=STATS_LEAN_FOR + IND_FAV_GOALS_2_MINUS + OU_FAMILY=OU_MID
- OVER | val 14/20 (70.0%) | train 23/37 (62.16%) | lift +20.81 | AH_FAMILY=AH_0_25 + BASE_STATS=STATS_STRONG_AGAINST + FAV_RECENT_GOALS_4_PLUS
- UNDER | val 25/36 (69.44%) | train 47/73 (64.38%) | lift +18.63 | BASE_COVER=FAIL + IND_DOG_STATS_STRONG_FOR + OU_FAMILY=OU_HIGH
- UNDER | val 9/13 (69.23%) | train 27/38 (71.05%) | lift +18.42 | BASE_TOTAL=TOTAL_OVER_LINE + IND_FAV_MARGIN_M_NEG1 + OU_FAMILY=OU_HIGH
- UNDER | val 9/13 (69.23%) | train 61/92 (66.3%) | lift +18.42 | AH_FAMILY=AH_2_PLUS + BASE_STATS=STATS_STRONG_FOR + IND_DOG_GOALS_2_MINUS
- UNDER | val 9/13 (69.23%) | train 61/92 (66.3%) | lift +18.42 | AH_SUPER=AHS_EXTREME + BASE_STATS=STATS_STRONG_FOR + IND_DOG_GOALS_2_MINUS
- UNDER | val 9/13 (69.23%) | train 76/120 (63.33%) | lift +18.42 | AH_FAMILY=AH_2_PLUS + BASE_TOTAL=TOTAL_OVER_LINE + DOG_RECENT_COVER_FAIL
- UNDER | val 9/13 (69.23%) | train 76/120 (63.33%) | lift +18.42 | AH_SUPER=AHS_EXTREME + BASE_TOTAL=TOTAL_OVER_LINE + DOG_RECENT_COVER_FAIL
- UNDER | val 11/16 (68.75%) | train 26/39 (66.67%) | lift +17.94 | BASE_STATS=STATS_LEAN_AGAINST + DOG_RECENT_COVER_COVER
- UNDER | val 24/35 (68.57%) | train 37/54 (68.52%) | lift +17.76 | IND_DOG_MARGIN_M_POS2 + TABLE=TABLE_UNKNOWN
- UNDER | val 13/19 (68.42%) | train 31/50 (62.0%) | lift +17.61 | AH_SUPER=AHS_LOW + BASE_TOTAL=TOTAL_UNDER_LINE + IND_DOG_STATS_LEAN_AGAINST

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
