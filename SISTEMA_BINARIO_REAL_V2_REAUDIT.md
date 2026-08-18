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


## Top reglas Over/Under

- UNDER | val 23/28 (82.14%) | train 33/48 (68.75%) | lift +31.33 | AH_SUPER=AHS_LOW + BASE_PRESSURE=PRESSURE_NEW_FAV + DOG_RECENT_STATS_LEAN_AGAINST
- UNDER | val 24/35 (68.57%) | train 37/54 (68.52%) | lift +17.76 | IND_DOG_MARGIN_M_POS2 + TABLE=TABLE_UNKNOWN

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
