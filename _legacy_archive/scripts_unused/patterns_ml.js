=== GENERANDO CÓDIGO JS PARA 30 PATRONES AH + 34 O/U ===

// ==================== VARIABLES ADICIONALES PARA PATRONES ML ====================
const tieneH2HCol3 = !!(pc.h2h_col3 && pc.h2h_col3.score);
const h2hCol3Score = ((pc.h2h_col3 || {}).score || '').split(':');
const h2hCol3GanoLocal = parseInt(h2hCol3Score[0] || 0) > parseInt(h2hCol3Score[1] || 0);
const h2hCol3GanoVisita = parseInt(h2hCol3Score[0] || 0) < parseInt(h2hCol3Score[1] || 0);
const h2hCol3Empate = parseInt(h2hCol3Score[0] || 0) === parseInt(h2hCol3Score[1] || 0);
const h2hCol3Goleo = h2hCol3GoalsCalc >= 4;
const h2hCol3PorCero = (parseInt(h2hCol3Score[0] || 0) === 0) || (parseInt(h2hCol3Score[1] || 0) === 0);
const h2hCol3Ambos = parseInt(h2hCol3Score[0] || 0) > 0 && parseInt(h2hCol3Score[1] || 0) > 0;

// ==================== PATRONES AH APRENDIDOS (TOP 30, >85% accuracy) ====================
// ML_1: ir_visita_gano+pa_encajo+ph_goleo+ph_por_cero (100%, 17 muestras)
else if (!pick && (visitaGoalsInd > rivalGoalsIndR) && (paEncajo) && (phGoleo) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 100;
    confidence = 'ultra';
    reason = '🔥🔥🔥 ML1_100% ir_visita_gano+pa_encajo';
    ruleUsed = 'ML1_100';
}

// ML_2: h_mejor_rank+h_top3+ir_under+ph_perdio (95%, 21 muestras)
else if (!pick && (homeRank > 0 && awayRank > 0 && homeRank < awayRank) && (homeRank > 0 && homeRank <= 3) && (indRightGoals <= 2) && (localPerdioPrev)) {
    pick = 'LOCAL';
    probability = 95;
    confidence = 'ultra';
    reason = '🔥🔥🔥 ML2_95% h_mejor_rank+h_top3';
    ruleUsed = 'ML2_95';
}

// ML_3: a_medio+h_top3+ir_ambos+ir_tiene (95%, 40 muestras)
else if (!pick && (awayRank > 5 && awayRank <= 12) && (homeRank > 0 && homeRank <= 3) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (tieneIndRightU)) {
    pick = 'LOCAL';
    probability = 95;
    confidence = 'ultra';
    reason = '🔥🔥🔥 ML3_95% a_medio+h_top3';
    ruleUsed = 'ML3_95';
}

// ML_4: gran_diff_rank+h2h_0goles+h_top5+pa_mucho_gol (94%, 37 muestras)
else if (!pick && (Math.abs(rankDiff) >= 5) && (h2hCol3GoalsCalc === 0) && (homeRank > 0 && homeRank <= 5) && (prevAwayGoals >= 4)) {
    pick = 'LOCAL';
    probability = 94;
    confidence = 'ultra';
    reason = '🔥🔥🔥 ML4_94% gran_diff_rank+h2h_0goles';
    ruleUsed = 'ML4_94';
}

// ML_5: a_medio+h2h_por_cero+h_top5+ir_tiene (94%, 72 muestras)
else if (!pick && (awayRank > 5 && awayRank <= 12) && (h2hCol3PorCero) && (homeRank > 0 && homeRank <= 5) && (tieneIndRightU)) {
    pick = 'LOCAL';
    probability = 94;
    confidence = 'ultra';
    reason = '🔥🔥🔥 ML5_94% a_medio+h2h_por_cero';
    ruleUsed = 'ML5_94';
}

// ML_6: a_medio+ind_alineadas_local+ind_ambas_over (94%, 18 muestras)
else if (!pick && (awayRank > 5 && awayRank <= 12) && (indAlineadasLocal) && (indLeftGoals >= 3 && indRightGoals >= 3)) {
    pick = 'LOCAL';
    probability = 94;
    confidence = 'ultra';
    reason = '🔥🔥🔥 ML6_94% a_medio+ind_alineadas_local';
    ruleUsed = 'ML6_94';
}

// ML_7: gran_diff_rank+h_top3+ir_visita_goleo+ph_mucho_gol (94%, 17 muestras)
else if (!pick && (Math.abs(rankDiff) >= 5) && (homeRank > 0 && homeRank <= 3) && (visitaGoalsInd >= 3) && (prevHomeGoals >= 4)) {
    pick = 'LOCAL';
    probability = 94;
    confidence = 'ultra';
    reason = '🔥🔥🔥 ML7_94% gran_diff_rank+h_top3';
    ruleUsed = 'ML7_94';
}

// ML_8: h2h_0goles+h_top3+ind_ambas_under+pa_tiene (94%, 17 muestras)
else if (!pick && (h2hCol3GoalsCalc === 0) && (homeRank > 0 && homeRank <= 3) && (indLeftGoals <= 2 && indRightGoals <= 2) && (tienePrevAway)) {
    pick = 'LOCAL';
    probability = 94;
    confidence = 'ultra';
    reason = '🔥🔥🔥 ML8_94% h2h_0goles+h_top3';
    ruleUsed = 'ML8_94';
}

// ML_9: h2h_empate+h_top3+ind_ambas_over (93%, 15 muestras)
else if (!pick && (h2hCol3Empate) && (homeRank > 0 && homeRank <= 3) && (indLeftGoals >= 3 && indRightGoals >= 3)) {
    pick = 'LOCAL';
    probability = 93;
    confidence = 'ultra';
    reason = '🔥🔥🔥 ML9_93% h2h_empate+h_top3';
    ruleUsed = 'ML9_93';
}

// ML_10: a_bottom+h_top3+ph_gano (93%, 43 muestras)
else if (!pick && (awayRank >= 15) && (homeRank > 0 && homeRank <= 3) && (localGanoPrev)) {
    pick = 'LOCAL';
    probability = 93;
    confidence = 'ultra';
    reason = '🔥🔥🔥 ML10_93% a_bottom+h_top3';
    ruleUsed = 'ML10_93';
}

// ML_11: ambos_golearon+il_ambos+pa_perdio+ph_gano (92%, 38 muestras)
else if (!pick && (phGoleo && paGoleo) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (visitaPerdioPrev) && (localGanoPrev)) {
    pick = 'LOCAL';
    probability = 92;
    confidence = 'ultra';
    reason = '🔥🔥🔥 ML11_92% ambos_golearon+il_ambos';
    ruleUsed = 'ML11_92';
}

// ML_12: a_bottom+h_top3+momentum_local (91%, 24 muestras)
else if (!pick && (awayRank >= 15) && (homeRank > 0 && homeRank <= 3) && (momentumLocal)) {
    pick = 'LOCAL';
    probability = 91;
    confidence = 'ultra';
    reason = '🔥🔥🔥 ML12_91% a_bottom+h_top3';
    ruleUsed = 'ML12_91';
}

// ML_13: a_medio+h_top5+il_local_gano+ph_ambos (91%, 24 muestras)
else if (!pick && (awayRank > 5 && awayRank <= 12) && (homeRank > 0 && homeRank <= 5) && (localGoalsInd > rivalGoalsIndL) && (parseInt(prevHomeScore[0] || 0) > 0 && parseInt(prevHomeScore[1] || 0) > 0)) {
    pick = 'LOCAL';
    probability = 91;
    confidence = 'ultra';
    reason = '🔥🔥🔥 ML13_91% a_medio+h_top5';
    ruleUsed = 'ML13_91';
}

// ML_14: a_medio+ambos_golearon+h2h_mucho_gol+ha_repetido_cerca (90%, 22 muestras)
else if (!pick && (awayRank > 5 && awayRank <= 12) && (phGoleo && paGoleo) && (h2hCol3GoalsCalc >= 4) && (handicapRepetidoCerca)) {
    pick = 'LOCAL';
    probability = 90;
    confidence = 'ultra';
    reason = '🔥🔥🔥 ML14_90% a_medio+ambos_golearon';
    ruleUsed = 'ML14_90';
}

// ML_15: gran_diff_rank+h2h_por_cero+h_top5 (90%, 139 muestras)
else if (!pick && (Math.abs(rankDiff) >= 5) && (h2hCol3PorCero) && (homeRank > 0 && homeRank <= 5)) {
    pick = 'LOCAL';
    probability = 90;
    confidence = 'ultra';
    reason = '🔥🔥🔥 ML15_90% gran_diff_rank+h2h_por_cero';
    ruleUsed = 'ML15_90';
}

// ML_16: h_mejor_rank+il_local_goleo+ind_alineadas_local+pa_mucho_gol (90%, 53 muestras)
else if (!pick && (homeRank > 0 && awayRank > 0 && homeRank < awayRank) && (localGoleoRivalVisita) && (indAlineadasLocal) && (prevAwayGoals >= 4)) {
    pick = 'LOCAL';
    probability = 90;
    confidence = 'ultra';
    reason = '🔥🔥🔥 ML16_90% h_mejor_rank+il_local_goleo';
    ruleUsed = 'ML16_90';
}

// ML_17: a_medio+datos_frescos+h2h_gano_local+h_top5 (90%, 42 muestras)
else if (!pick && (awayRank > 5 && awayRank <= 12) && (datosFrescos) && (h2hCol3GanoLocal) && (homeRank > 0 && homeRank <= 5)) {
    pick = 'LOCAL';
    probability = 90;
    confidence = 'ultra';
    reason = '🔥🔥🔥 ML17_90% a_medio+datos_frescos';
    ruleUsed = 'ML17_90';
}

// ML_18: a_mejor_rank+ambos_under+h2h_gano_visita+il_local_perdio (90%, 21 muestras)
else if (!pick && (homeRank > 0 && awayRank > 0 && awayRank < homeRank) && (prevHomeLow && prevAwayLow) && (h2hCol3GanoVisita) && (localGoalsInd < rivalGoalsIndL)) {
    pick = 'VISITA';
    probability = 90;
    confidence = 'ultra';
    reason = '🔥🔥🔥 ML18_90% a_mejor_rank+ambos_under';
    ruleUsed = 'ML18_90';
}

// ML_19: datos_frescos+gran_diff_rank+h2h_under+h_top3 (90%, 104 muestras)
else if (!pick && (datosFrescos) && (Math.abs(rankDiff) >= 5) && (h2hCol3GoalsCalc <= 2) && (homeRank > 0 && homeRank <= 3)) {
    pick = 'LOCAL';
    probability = 90;
    confidence = 'ultra';
    reason = '🔥🔥🔥 ML19_90% datos_frescos+gran_diff_rank';
    ruleUsed = 'ML19_90';
}

// ML_20: gran_diff_rank+h2h_under+h_top3 (90%, 104 muestras)
else if (!pick && (Math.abs(rankDiff) >= 5) && (h2hCol3GoalsCalc <= 2) && (homeRank > 0 && homeRank <= 3)) {
    pick = 'LOCAL';
    probability = 90;
    confidence = 'ultra';
    reason = '🔥🔥🔥 ML20_90% gran_diff_rank+h2h_under';
    ruleUsed = 'ML20_90';
}

// ML_21: a_medio+h_top5+pa_mucho_gol+ph_gano (90%, 51 muestras)
else if (!pick && (awayRank > 5 && awayRank <= 12) && (homeRank > 0 && homeRank <= 5) && (prevAwayGoals >= 4) && (localGanoPrev)) {
    pick = 'LOCAL';
    probability = 90;
    confidence = 'ultra';
    reason = '🔥🔥🔥 ML21_90% a_medio+h_top5';
    ruleUsed = 'ML21_90';
}

// ML_22: ah_0+gran_diff_rank+h2h_0goles+h_top3 (89%, 48 muestras)
else if (!pick && (Math.abs(ah_value) < 0.01) && (Math.abs(rankDiff) >= 5) && (h2hCol3GoalsCalc === 0) && (homeRank > 0 && homeRank <= 3)) {
    pick = 'LOCAL';
    probability = 89;
    confidence = 'high';
    reason = '🔥🔥 ML22_89% ah_0+gran_diff_rank';
    ruleUsed = 'ML22_89';
}

// ML_23: h2h_por_cero+h_mejor_rank+ha_repetido_exacto+ind_alineadas_local (88%, 54 muestras)
else if (!pick && (h2hCol3PorCero) && (homeRank > 0 && awayRank > 0 && homeRank < awayRank) && (handicapRepetidoExacto) && (indAlineadasLocal)) {
    pick = 'LOCAL';
    probability = 88;
    confidence = 'high';
    reason = '🔥🔥 ML23_88% h2h_por_cero+h_mejor_rank';
    ruleUsed = 'ML23_88';
}

// ML_24: a_medio+h_top5+il_over+ph_por_cero (88%, 27 muestras)
else if (!pick && (awayRank > 5 && awayRank <= 12) && (homeRank > 0 && homeRank <= 5) && (indLeftGoals >= 3) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 88;
    confidence = 'high';
    reason = '🔥🔥 ML24_88% a_medio+h_top5';
    ruleUsed = 'ML24_88';
}

// ML_25: ambos_golearon+h_mejor_rank+ha_repetido_exacto+ph_perdio (88%, 18 muestras)
else if (!pick && (phGoleo && paGoleo) && (homeRank > 0 && awayRank > 0 && homeRank < awayRank) && (handicapRepetidoExacto) && (localPerdioPrev)) {
    pick = 'LOCAL';
    probability = 88;
    confidence = 'high';
    reason = '🔥🔥 ML25_88% ambos_golearon+h_mejor_rank';
    ruleUsed = 'ML25_88';
}

// ML_26: h2h_ambos+h2h_gano_visita+h_top3+ir_under (88%, 18 muestras)
else if (!pick && (h2hCol3Ambos) && (h2hCol3GanoVisita) && (homeRank > 0 && homeRank <= 3) && (indRightGoals <= 2)) {
    pick = 'LOCAL';
    probability = 88;
    confidence = 'high';
    reason = '🔥🔥 ML26_88% h2h_ambos+h2h_gano_visita';
    ruleUsed = 'ML26_88';
}

// ML_27: a_bottom+h_top10+ir_under+ir_visita_gano (88%, 18 muestras)
else if (!pick && (awayRank >= 15) && (homeRank > 0 && homeRank <= 10) && (indRightGoals <= 2) && (visitaGoalsInd > rivalGoalsIndR)) {
    pick = 'LOCAL';
    probability = 88;
    confidence = 'high';
    reason = '🔥🔥 ML27_88% a_bottom+h_top10';
    ruleUsed = 'ML27_88';
}

// ML_28: a_bottom+h2h_under+h_top5+pa_empato (88%, 18 muestras)
else if (!pick && (awayRank >= 15) && (h2hCol3GoalsCalc <= 2) && (homeRank > 0 && homeRank <= 5) && (paEmpato)) {
    pick = 'LOCAL';
    probability = 88;
    confidence = 'high';
    reason = '🔥🔥 ML28_88% a_bottom+h2h_under';
    ruleUsed = 'ML28_88';
}

// ML_29: ambos_golearon+gran_diff_rank+h_top10 (88%, 35 muestras)
else if (!pick && (phGoleo && paGoleo) && (Math.abs(rankDiff) >= 5) && (homeRank > 0 && homeRank <= 10)) {
    pick = 'LOCAL';
    probability = 88;
    confidence = 'high';
    reason = '🔥🔥 ML29_88% ambos_golearon+gran_diff_rank';
    ruleUsed = 'ML29_88';
}

// ML_30: a_bottom+h2h_under+h_top3 (88%, 43 muestras)
else if (!pick && (awayRank >= 15) && (h2hCol3GoalsCalc <= 2) && (homeRank > 0 && homeRank <= 3)) {
    pick = 'LOCAL';
    probability = 88;
    confidence = 'high';
    reason = '🔥🔥 ML30_88% a_bottom+h2h_under';
    ruleUsed = 'ML30_88';
}


// ==================== PATRONES O/U APRENDIDOS (34 patrones) ====================
// OU_1: h2h_goleo+h2h_por_cero+h_medio+ou_normal (87%, 16 muestras)
else if (!pickOU && (h2hCol3Goleo) && (h2hCol3PorCero) && (homeRank > 5 && homeRank <= 12) && (Math.abs(ou_value - 2.5) < 0.3)) {
    pickOU = 'UNDER';
    probOU = 87;
    reasonOU = '🎯🎯 OU1_87% h2h_goleo+h2h_por_cero';
    ruleUsedOU = 'OU1_87';
}

// OU_2: a_top3+il_local_goleo+ind_ambas_under (86%, 15 muestras)
else if (!pickOU && (awayRank > 0 && awayRank <= 3) && (localGoleoRivalVisita) && (indLeftGoals <= 2 && indRightGoals <= 2)) {
    pickOU = 'UNDER';
    probOU = 86;
    reasonOU = '🎯🎯 OU2_86% a_top3+il_local_goleo';
    ruleUsedOU = 'OU2_86';
}

// OU_3: ind_ambas_under+pa_marco3+ph_marco3 (85%, 20 muestras)
else if (!pickOU && (indLeftGoals <= 2 && indRightGoals <= 2) && (prevAwayGoals >= 3) && (prevHomeGoals >= 3)) {
    pickOU = 'UNDER';
    probOU = 85;
    reasonOU = '🎯🎯 OU3_85% ind_ambas_under+pa_marco3';
    ruleUsedOU = 'OU3_85';
}

// OU_4: ambos_under+h2h_ambos+il_local_goleo+il_under (83%, 30 muestras)
else if (!pickOU && (prevHomeLow && prevAwayLow) && (h2hCol3Ambos) && (localGoleoRivalVisita) && (indLeftGoals <= 2)) {
    pickOU = 'UNDER';
    probOU = 83;
    reasonOU = '🎯 OU4_83% ambos_under+h2h_ambos';
    ruleUsedOU = 'OU4_83';
}

// OU_5: h2h_goleo+ind_ambas_under+ir_visita_perdio+pa_gano (83%, 30 muestras)
else if (!pickOU && (h2hCol3Goleo) && (indLeftGoals <= 2 && indRightGoals <= 2) && (visitaPerdioContraRivalLocal) && (visitaGanoPrev)) {
    pickOU = 'UNDER';
    probOU = 83;
    reasonOU = '🎯 OU5_83% h2h_goleo+ind_ambas_under';
    ruleUsedOU = 'OU5_83';
}

// OU_6: h2h_goleo+momentum_visita+pa_gano+ranks_parejos (81%, 77 muestras)
else if (!pickOU && (h2hCol3Goleo) && (momentumVisita) && (visitaGanoPrev) && (Math.abs(rankDiff) <= 3)) {
    pickOU = 'UNDER';
    probOU = 81;
    reasonOU = '🎯 OU6_81% h2h_goleo+momentum_visita';
    ruleUsedOU = 'OU6_81';
}

// OU_7: ambos_under+il_ambos+ind_alineadas_visita (81%, 16 muestras)
else if (!pickOU && (prevHomeLow && prevAwayLow) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (indAlineadasVisita)) {
    pickOU = 'UNDER';
    probOU = 81;
    reasonOU = '🎯 OU7_81% ambos_under+il_ambos';
    ruleUsedOU = 'OU7_81';
}

// OU_8: ambos_under+il_ambos+ind_alineadas_visita+ph_tiene (81%, 16 muestras)
else if (!pickOU && (prevHomeLow && prevAwayLow) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (indAlineadasVisita) && (tienePrevHome)) {
    pickOU = 'UNDER';
    probOU = 81;
    reasonOU = '🎯 OU8_81% ambos_under+il_ambos';
    ruleUsedOU = 'OU8_81';
}

// OU_9: a_top10+a_top3+h_mejor_rank+ph_empato (81%, 16 muestras)
else if (!pickOU && (awayRank > 0 && awayRank <= 10) && (awayRank > 0 && awayRank <= 3) && (homeRank > 0 && awayRank > 0 && homeRank < awayRank) && (phEmpato)) {
    pickOU = 'UNDER';
    probOU = 81;
    reasonOU = '🎯 OU9_81% a_top10+a_top3';
    ruleUsedOU = 'OU9_81';
}

// OU_10: ambos_under+ir_visita_goleo+ph_ambos+ranks_parejos (81%, 21 muestras)
else if (!pickOU && (prevHomeLow && prevAwayLow) && (visitaGoalsInd >= 3) && (parseInt(prevHomeScore[0] || 0) > 0 && parseInt(prevHomeScore[1] || 0) > 0) && (Math.abs(rankDiff) <= 3)) {
    pickOU = 'UNDER';
    probOU = 81;
    reasonOU = '🎯 OU10_81% ambos_under+ir_visita_goleo';
    ruleUsedOU = 'OU10_81';
}

// OU_11: h2h_gano_visita+h2h_mucho_gol+il_local_perdio+momentum_visita (81%, 21 muestras)
else if (!pickOU && (h2hCol3GanoVisita) && (h2hCol3GoalsCalc >= 4) && (localGoalsInd < rivalGoalsIndL) && (momentumVisita)) {
    pickOU = 'UNDER';
    probOU = 81;
    reasonOU = '🎯 OU11_81% h2h_gano_visita+h2h_mucho_gol';
    ruleUsedOU = 'OU11_81';
}

// OU_12: h2h_mucho_gol+h_top5+ind_ambas_under+ranks_parejos (80%, 30 muestras)
else if (!pickOU && (h2hCol3GoalsCalc >= 4) && (homeRank > 0 && homeRank <= 5) && (indLeftGoals <= 2 && indRightGoals <= 2) && (Math.abs(rankDiff) <= 3)) {
    pickOU = 'UNDER';
    probOU = 80;
    reasonOU = '🎯 OU12_80% h2h_mucho_gol+h_top5';
    ruleUsedOU = 'OU12_80';
}

// OU_13: il_local_goleo+ir_tiene+ir_visita_gano+pa_0goles (80%, 25 muestras)
else if (!pickOU && (localGoleoRivalVisita) && (tieneIndRightU) && (visitaGoalsInd > rivalGoalsIndR) && (prevAwayGoals === 0)) {
    pickOU = 'UNDER';
    probOU = 80;
    reasonOU = '🎯 OU13_80% il_local_goleo+ir_tiene';
    ruleUsedOU = 'OU13_80';
}

// OU_14: ind_alineadas_local+pa_gano+ph_marco3+ranks_parejos (80%, 20 muestras)
else if (!pickOU && (indAlineadasLocal) && (visitaGanoPrev) && (prevHomeGoals >= 3) && (Math.abs(rankDiff) <= 3)) {
    pickOU = 'UNDER';
    probOU = 80;
    reasonOU = '🎯 OU14_80% ind_alineadas_local+pa_gano';
    ruleUsedOU = 'OU14_80';
}

// OU_15: gran_diff_rank+h2h_goleo+h_top3+ph_empato (80%, 15 muestras)
else if (!pickOU && (Math.abs(rankDiff) >= 5) && (h2hCol3Goleo) && (homeRank > 0 && homeRank <= 3) && (phEmpato)) {
    pickOU = 'UNDER';
    probOU = 80;
    reasonOU = '🎯 OU15_80% gran_diff_rank+h2h_goleo';
    ruleUsedOU = 'OU15_80';
}

// OU_16: a_top3+ambos_under+h_mejor_rank+pa_tiene (80%, 15 muestras)
else if (!pickOU && (awayRank > 0 && awayRank <= 3) && (prevHomeLow && prevAwayLow) && (homeRank > 0 && awayRank > 0 && homeRank < awayRank) && (tienePrevAway)) {
    pickOU = 'UNDER';
    probOU = 80;
    reasonOU = '🎯 OU16_80% a_top3+ambos_under';
    ruleUsedOU = 'OU16_80';
}

// OU_17: h2h_mucho_gol+ind_alineadas_local+pa_gano+ph_encajo (79%, 24 muestras)
else if (!pickOU && (h2hCol3GoalsCalc >= 4) && (indAlineadasLocal) && (visitaGanoPrev) && (phEncajo)) {
    pickOU = 'UNDER';
    probOU = 79;
    reasonOU = '🎯 OU17_79% h2h_mucho_gol+ind_alineadas_local';
    ruleUsedOU = 'OU17_79';
}

// OU_18: h2h_0goles+il_under+ph_0goles+ph_under (78%, 80 muestras)
else if (!pickOU && (h2hCol3GoalsCalc === 0) && (indLeftGoals <= 2) && (prevHomeGoals === 0) && (prevHomeGoals <= 2)) {
    pickOU = 'UNDER';
    probOU = 78;
    reasonOU = '🎯 OU18_78% h2h_0goles+il_under';
    ruleUsedOU = 'OU18_78';
}

// OU_19: datos_completos+h2h_0goles+il_under+ph_0goles (78%, 80 muestras)
else if (!pickOU && (datosCompletos) && (h2hCol3GoalsCalc === 0) && (indLeftGoals <= 2) && (prevHomeGoals === 0)) {
    pickOU = 'UNDER';
    probOU = 78;
    reasonOU = '🎯 OU19_78% datos_completos+h2h_0goles';
    ruleUsedOU = 'OU19_78';
}

// OU_20: a_top5+ambos_under+h2h_ambos+h_top3 (78%, 23 muestras)
else if (!pickOU && (awayRank > 0 && awayRank <= 5) && (prevHomeLow && prevAwayLow) && (h2hCol3Ambos) && (homeRank > 0 && homeRank <= 3)) {
    pickOU = 'UNDER';
    probOU = 78;
    reasonOU = '🎯 OU20_78% a_top5+ambos_under';
    ruleUsedOU = 'OU20_78';
}

// OU_21: h2h_goleo+ind_alineadas_local+ph_encajo+ph_goleo (78%, 23 muestras)
else if (!pickOU && (h2hCol3Goleo) && (indAlineadasLocal) && (phEncajo) && (phGoleo)) {
    pickOU = 'UNDER';
    probOU = 78;
    reasonOU = '🎯 OU21_78% h2h_goleo+ind_alineadas_local';
    ruleUsedOU = 'OU21_78';
}

// OU_22: ind_alineadas_local+ph_empato+ph_mucho_gol (78%, 23 muestras)
else if (!pickOU && (indAlineadasLocal) && (phEmpato) && (prevHomeGoals >= 4)) {
    pickOU = 'UNDER';
    probOU = 78;
    reasonOU = '🎯 OU22_78% ind_alineadas_local+ph_empato';
    ruleUsedOU = 'OU22_78';
}

// OU_23: ambos_under+h2h_mucho_gol+pa_encajo+ph_empato (78%, 23 muestras)
else if (!pickOU && (prevHomeLow && prevAwayLow) && (h2hCol3GoalsCalc >= 4) && (paEncajo) && (phEmpato)) {
    pickOU = 'UNDER';
    probOU = 78;
    reasonOU = '🎯 OU23_78% ambos_under+h2h_mucho_gol';
    ruleUsedOU = 'OU23_78';
}

// OU_24: a_top5+h2h_gano_visita+h_top5+pa_ambos (77%, 54 muestras)
else if (!pickOU && (awayRank > 0 && awayRank <= 5) && (h2hCol3GanoVisita) && (homeRank > 0 && homeRank <= 5) && (parseInt(prevAwayScore[0] || 0) > 0 && parseInt(prevAwayScore[1] || 0) > 0)) {
    pickOU = 'UNDER';
    probOU = 77;
    reasonOU = '🎯 OU24_77% a_top5+h2h_gano_visita';
    ruleUsedOU = 'OU24_77';
}

// OU_25: il_tiene+ir_visita_gano+pa_perdio+ph_0goles (77%, 36 muestras)
else if (!pickOU && (tieneIndLeftU) && (visitaGoalsInd > rivalGoalsIndR) && (visitaPerdioPrev) && (prevHomeGoals === 0)) {
    pickOU = 'UNDER';
    probOU = 77;
    reasonOU = '🎯 OU25_77% il_tiene+ir_visita_gano';
    ruleUsedOU = 'OU25_77';
}

// OU_26: ir_ambos+ir_visita_gano+ph_0goles (77%, 44 muestras)
else if (!pickOU && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (visitaGoalsInd > rivalGoalsIndR) && (prevHomeGoals === 0)) {
    pickOU = 'UNDER';
    probOU = 77;
    reasonOU = '🎯 OU26_77% ir_ambos+ir_visita_gano';
    ruleUsedOU = 'OU26_77';
}

// OU_27: h_medio+ind_alineadas_local+ph_goleo (77%, 22 muestras)
else if (!pickOU && (homeRank > 5 && homeRank <= 12) && (indAlineadasLocal) && (phGoleo)) {
    pickOU = 'UNDER';
    probOU = 77;
    reasonOU = '🎯 OU27_77% h_medio+ind_alineadas_local';
    ruleUsedOU = 'OU27_77';
}

// OU_28: h2h_goleo+ou_normal+pa_empato+ph_0goles (77%, 22 muestras)
else if (!pickOU && (h2hCol3Goleo) && (Math.abs(ou_value - 2.5) < 0.3) && (paEmpato) && (prevHomeGoals === 0)) {
    pickOU = 'UNDER';
    probOU = 77;
    reasonOU = '🎯 OU28_77% h2h_goleo+ou_normal';
    ruleUsedOU = 'OU28_77';
}

// OU_29: h2h_empate+h_top5+pa_por_cero+ph_gano (76%, 26 muestras)
else if (!pickOU && (h2hCol3Empate) && (homeRank > 0 && homeRank <= 5) && (paPorCero) && (localGanoPrev)) {
    pickOU = 'UNDER';
    probOU = 76;
    reasonOU = '🎯 OU29_76% h2h_empate+h_top5';
    ruleUsedOU = 'OU29_76';
}

// OU_30: il_over+ir_visita_goleo+pa_por_cero+ranks_parejos (76%, 68 muestras)
else if (!pickOU && (indLeftGoals >= 3) && (visitaGoalsInd >= 3) && (paPorCero) && (Math.abs(rankDiff) <= 3)) {
    pickOU = 'UNDER';
    probOU = 76;
    reasonOU = '🎯 OU30_76% il_over+ir_visita_goleo';
    ruleUsedOU = 'OU30_76';
}

// OU_31: h2h_goleo+ir_tiene+momentum_visita+pa_por_cero (76%, 59 muestras)
else if (!pickOU && (h2hCol3Goleo) && (tieneIndRightU) && (momentumVisita) && (paPorCero)) {
    pickOU = 'UNDER';
    probOU = 76;
    reasonOU = '🎯 OU31_76% h2h_goleo+ir_tiene';
    ruleUsedOU = 'OU31_76';
}

// OU_32: a_top5+ind_alineadas_local+ind_ambas_under+pa_tiene (76%, 21 muestras)
else if (!pickOU && (awayRank > 0 && awayRank <= 5) && (indAlineadasLocal) && (indLeftGoals <= 2 && indRightGoals <= 2) && (tienePrevAway)) {
    pickOU = 'UNDER';
    probOU = 76;
    reasonOU = '🎯 OU32_76% a_top5+ind_alineadas_local';
    ruleUsedOU = 'OU32_76';
}

// OU_33: a_top10+h2h_ambos+h_medio+ind_alineadas_local (76%, 25 muestras)
else if (!pickOU && (awayRank > 0 && awayRank <= 10) && (h2hCol3Ambos) && (homeRank > 5 && homeRank <= 12) && (indAlineadasLocal)) {
    pickOU = 'UNDER';
    probOU = 76;
    reasonOU = '🎯 OU33_76% a_top10+h2h_ambos';
    ruleUsedOU = 'OU33_76';
}

// OU_34: h2h_empate+pa_ambos+pa_empato+ph_empato (76%, 25 muestras)
else if (!pickOU && (h2hCol3Empate) && (parseInt(prevAwayScore[0] || 0) > 0 && parseInt(prevAwayScore[1] || 0) > 0) && (paEmpato) && (phEmpato)) {
    pickOU = 'UNDER';
    probOU = 76;
    reasonOU = '🎯 OU34_76% h2h_empate+pa_ambos';
    ruleUsedOU = 'OU34_76';
}

// === FIN PATRONES ML ===
