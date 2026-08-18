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

- DOG | val 22/30 (73.33%) | train 91/149 (61.07%) | lift +23.74 | BASE_PRESSURE=PRESSURE_RAISE_AGGRESSIVE + FAV_RECENT_STATS_STRONG_FOR
- DOG | val 32/44 (72.73%) | train 129/207 (62.32%) | lift +23.14 | BASE_STATS=STATS_STRONG_FOR + IND_DOG_OU_CURRENT_OVER_LINE + OU_FAMILY=OU_MID
- DOG | val 32/44 (72.73%) | train 129/207 (62.32%) | lift +23.14 | BASE_STATS=STATS_STRONG_FOR + IND_DOG_TOTAL_OVER_LINE + OU_FAMILY=OU_MID
- DOG | val 21/31 (67.74%) | train 108/180 (60.0%) | lift +18.15 | AH_FAMILY=AH_1_1_25 + BASE_TOTAL=TOTAL_OVER_LINE + DOG_RECENT_OU_CURRENT_OVER_LINE
- DOG | val 21/31 (67.74%) | train 108/180 (60.0%) | lift +18.15 | AH_FAMILY=AH_1_1_25 + BASE_TOTAL=TOTAL_OVER_LINE + DOG_RECENT_TOTAL_OVER_LINE
- DOG | val 21/31 (67.74%) | train 108/180 (60.0%) | lift +18.15 | AH_SUPER=AHS_MID + BASE_TOTAL=TOTAL_OVER_LINE + DOG_RECENT_OU_CURRENT_OVER_LINE
- DOG | val 21/31 (67.74%) | train 108/180 (60.0%) | lift +18.15 | AH_SUPER=AHS_MID + BASE_TOTAL=TOTAL_OVER_LINE + DOG_RECENT_TOTAL_OVER_LINE
- DOG | val 23/34 (67.65%) | train 58/88 (65.91%) | lift +18.06 | BASE_PRESSURE=PRESSURE_NEW_FAV + IND_FAV_COVER_FAIL + OU_FAMILY=OU_MID
- DOG | val 34/52 (65.38%) | train 56/92 (60.87%) | lift +15.79 | AH_SUPER=AHS_LOW + BASE_COVER=COVER + DOG_RECENT_MARGIN_M_POS1
- DOG | val 22/34 (64.71%) | train 53/84 (63.1%) | lift +15.12 | AH_FAMILY=AH_0_25 + BASE_STATS=STATS_STRONG_FOR + IND_DOG_COVER_FAIL
- FAVORITE | val 22/34 (64.71%) | train 54/87 (62.07%) | lift +14.3 | AH_EXACT=0.75 + IND_FAV_MARGIN_M_DRAW
- DOG | val 32/50 (64.0%) | train 59/97 (60.82%) | lift +14.41 | IND_DOG_STATS_NEUTRAL + TABLE=TABLE_UNKNOWN
- DOG | val 16/25 (64.0%) | train 98/162 (60.49%) | lift +14.41 | AH_FAMILY=AH_2_PLUS + BASE_STATS=STATS_STRONG_FOR + DOG_RECENT_STATS_STRONG_AGAINST
- DOG | val 16/25 (64.0%) | train 98/162 (60.49%) | lift +14.41 | AH_SUPER=AHS_EXTREME + BASE_STATS=STATS_STRONG_FOR + DOG_RECENT_STATS_STRONG_AGAINST
- DOG | val 21/33 (63.64%) | train 113/180 (62.78%) | lift +14.05 | AH_FAMILY=AH_1_1_25 + BASE_COVER=FAIL + IND_DOG_OU_CURRENT_OVER_LINE
- DOG | val 21/33 (63.64%) | train 113/180 (62.78%) | lift +14.05 | AH_FAMILY=AH_1_1_25 + BASE_COVER=FAIL + IND_DOG_TOTAL_OVER_LINE
- DOG | val 21/33 (63.64%) | train 113/180 (62.78%) | lift +14.05 | AH_SUPER=AHS_MID + BASE_COVER=FAIL + IND_DOG_OU_CURRENT_OVER_LINE
- DOG | val 21/33 (63.64%) | train 113/180 (62.78%) | lift +14.05 | AH_SUPER=AHS_MID + BASE_COVER=FAIL + IND_DOG_TOTAL_OVER_LINE
- DOG | val 25/40 (62.5%) | train 86/142 (60.56%) | lift +12.91 | BASE_TOTAL=TOTAL_UNDER_LINE + IND_DOG_STATS_NEUTRAL
- DOG | val 18/29 (62.07%) | train 64/102 (62.75%) | lift +12.48 | BASE_COVER=COVER + IND_DOG_GOALS_4_PLUS + OU_FAMILY=OU_MID

## Top reglas Over/Under

- UNDER | val 37/51 (72.55%) | train 59/98 (60.2%) | lift +21.74 | AH_FAMILY=AH_0_25 + BASE_COVER=FAIL + BASE_STATS=STATS_STRONG_FOR
- UNDER | val 29/41 (70.73%) | train 77/128 (60.16%) | lift +19.92 | BASE_PRESSURE=PRESSURE_RAISE + IND_FAV_SHORT_CURRENT_AH + OU_FAMILY=OU_HIGH
- UNDER | val 17/25 (68.0%) | train 52/84 (61.9%) | lift +17.19 | BASE_TOTAL=TOTAL_UNDER_LINE + IND_FAV_MARGIN_M_NEG2
- UNDER | val 21/31 (67.74%) | train 54/85 (63.53%) | lift +16.93 | BASE_STATS=STATS_STRONG_FOR + IND_FAV_MARGIN_M_DRAW + OU_FAMILY=OU_LOW
- UNDER | val 51/76 (67.11%) | train 87/144 (60.42%) | lift +16.3 | AH_FAMILY=AH_0_25 + BASE_TOTAL=TOTAL_UNDER_LINE + H2H_GENERAL_COVER_FAIL
- UNDER | val 24/36 (66.67%) | train 60/99 (60.61%) | lift +15.86 | IND_FAV_COVER_PUSH + TABLE=TABLE_FAV_BETTER
- UNDER | val 41/62 (66.13%) | train 108/179 (60.34%) | lift +15.32 | BASE_STATS=STATS_STRONG_FOR + FAV_RECENT_OU_CURRENT_UNDER_LINE + OU_FAMILY=OU_LOW
- UNDER | val 41/62 (66.13%) | train 108/179 (60.34%) | lift +15.32 | BASE_STATS=STATS_STRONG_FOR + FAV_RECENT_TOTAL_UNDER_LINE + OU_FAMILY=OU_LOW
- UNDER | val 27/41 (65.85%) | train 62/100 (62.0%) | lift +15.04 | IND_FAV_COVER_FAIL + QUALITY=MID
- UNDER | val 21/32 (65.62%) | train 51/80 (63.75%) | lift +14.81 | BASE_STATS=STATS_STRONG_FOR + IND_FAV_COVER_PUSH
- UNDER | val 19/29 (65.52%) | train 60/97 (61.86%) | lift +14.71 | BASE_PRESSURE=PRESSURE_RAISE + IND_FAV_GOALS_2_MINUS + OU_FAMILY=OU_HIGH
- UNDER | val 32/49 (65.31%) | train 90/149 (60.4%) | lift +14.5 | BASE_STATS=STATS_STRONG_FOR + IND_FAV_SHORT_CURRENT_AH + OU_FAMILY=OU_LOW
- UNDER | val 25/39 (64.1%) | train 67/111 (60.36%) | lift +13.29 | DOG_RECENT_STATS_LEAN_AGAINST + OU_FAMILY=OU_LOW
- UNDER | val 25/39 (64.1%) | train 50/83 (60.24%) | lift +13.29 | AH_FAMILY=AH_0_25 + BASE_STATS=STATS_STRONG_FOR + IND_FAV_GOALS_2_MINUS
- UNDER | val 21/33 (63.64%) | train 55/81 (67.9%) | lift +12.83 | AH_SUPER=AHS_LOW + BASE_TOTAL=TOTAL_UNDER_LINE + IND_DOG_MARGIN_M_POS1
- UNDER | val 29/46 (63.04%) | train 74/123 (60.16%) | lift +12.23 | IND_DOG_OU_CURRENT_UNDER_LINE + TABLE=TABLE_FAV_WORSE
- UNDER | val 29/46 (63.04%) | train 74/123 (60.16%) | lift +12.23 | IND_DOG_TOTAL_UNDER_LINE + TABLE=TABLE_FAV_WORSE
- UNDER | val 53/85 (62.35%) | train 107/171 (62.57%) | lift +11.54 | AH_FAMILY=AH_0_25 + BASE_COVER=FAIL + BASE_TOTAL=TOTAL_UNDER_LINE
- UNDER | val 31/50 (62.0%) | train 54/89 (60.67%) | lift +11.19 | AH_FAMILY=AH_0_25 + BASE_STATS=STATS_STRONG_FOR + FAV_RECENT_OU_CURRENT_UNDER_LINE
- UNDER | val 31/50 (62.0%) | train 54/89 (60.67%) | lift +11.19 | AH_FAMILY=AH_0_25 + BASE_STATS=STATS_STRONG_FOR + FAV_RECENT_TOTAL_UNDER_LINE

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
