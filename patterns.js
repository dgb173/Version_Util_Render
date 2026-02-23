=== GENERANDO CÓDIGO JS PARA 16 PATRONES AH + 0 O/U ===

// ==================== VARIABLES ADICIONALES PARA PATRONES ML ====================
// (Variables pre-calculadas en precacheo.html, se omiten aquí para evitar duplicados)

if (false) { }

// ==================== PATRONES AH APRENDIDOS (ORDENADOS POR VALIDACIÓN) ====================
Total patrones AH raw: 738
Patrones VALIDADOS y ÚNICOS: 16
else if (!pick && (datosFrescos) && (h2hCol3PorCero) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (phGoleo) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 75;
    confidence = 'high';
    reason = '🎯 ML1 Val:75%(4) Train:76%(39) datos_frescos+h2h_por_cero+ir_failed_current...';
    ruleUsed = 'ML1';
}

else if (!pick && (datosCompletos) && (localGoalsInd > rivalGoalsIndL) && (paEncajo) && (phEncajo) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML2 Val:66%(3) Train:71%(35) datos_completos+il_local_gano+pa_encajo...';
    ruleUsed = 'ML2';
}

else if (!pick && (h2hCol3PorCero) && (tieneH2HCol3) && (indLeftGoals <= 2) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (tienePrevAway) && (phSOT >= 5)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML3 Val:66%(3) Train:72%(93) h2h_por_cero+h2h_tiene+il_under...';
    ruleUsed = 'ML3';
}

else if (!pick && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (visitaPerdioContraRivalLocal) && (paEncajo) && (parseInt(prevHomeScore[0] || 0) > 0 && parseInt(prevHomeScore[1] || 0) > 0) && ((phDA >= 50) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]))) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML4 Val:66%(3) Train:75%(73) ind_perf_home_good+ir_visita_perdio+pa_encajo...';
    ruleUsed = 'ML4';
}

else if (!pick && (awayRank > 0 && awayRank <= 10) && (h2hCol3GoalsCalc === 0) && (h2hCol3PorCero) && (h2hCol3GoalsCalc <= 2) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (phDA > paDA) && (phSOT > paSOT) && (phEncajo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML5 Val:66%(3) Train:75%(28) a_top10+h2h_0goles+h2h_por_cero...';
    ruleUsed = 'ML5';
}

else if (!pick && (h2hCol3PorCero) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (tieneIndLeftU) && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML6 Val:66%(3) Train:72%(25) h2h_por_cero+il_ambos+il_tiene...';
    ruleUsed = 'ML6';
}

else if (!pick && (awayRank >= 15) && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (((parseInt((pc.h2h_col3 || {}).goles_home || 0) + parseFloat((pc.h2h_col3 || {}).ah || 0)) > parseInt((pc.h2h_col3 || {}).goles_away || 0))) && (paDA <= 30) && (prevAwayGoals >= 4)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML7 Val:66%(3) Train:77%(58) a_bottom+h2h_ah_eq+h2h_covered...';
    ruleUsed = 'ML7';
}

else if (!pick && (h2hCol3GoalsCalc === 0) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (irAwayDA > (irRivalDA * 1.5)) && (tieneIndRightU) && (prevHomeGoals <= 2)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML8 Val:66%(3) Train:84%(25) h2h_0goles+il_covered_current+ir_dominate_da...';
    ruleUsed = 'ML8';
}

else if (!pick && (datosCompletos) && ((parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0)) > 0) && (momentumLocal) && (Math.abs(ou_value - 2.5) < 0.3) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML9 Val:66%(3) Train:76%(105) datos_completos+il_was_dog+momentum_local...';
    ruleUsed = 'ML9';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (visitaPerdioContraRivalLocal) && (parseInt(prevAwayScore[0] || 0) > 0 && parseInt(prevAwayScore[1] || 0) > 0) && (phDA >= 50)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML10 Val:66%(3) Train:71%(90) h2h_ah_eq+h_top5+ir_visita_perdio...';
    ruleUsed = 'ML10';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (tienePrevAway)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML11 Val:66%(3) Train:70%(194) h2h_ah_eq+h_top5+il_ambos...';
    ruleUsed = 'ML11';
}

else if (!pick && (handicapRepetidoCerca) && (indLeftGoals >= 3 && indRightGoals >= 3) && (phDA > paDA) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML12 Val:66%(3) Train:77%(72) ha_repetido_cerca+ind_ambas_over+ph_better_da...';
    ruleUsed = 'ML12';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (localGoleoRivalVisita) && (momentumLocal) && (phSOT > paSOT) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML13 Val:66%(3) Train:80%(51) h2h_ah_eq+il_local_goleo+momentum_local...';
    ruleUsed = 'ML13';
}

else if (!pick && (datosFrescos) && (homeRank > 0 && homeRank <= 10) && (localGoalsInd > rivalGoalsIndL) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (paEncajo)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML14 Val:60%(5) Train:71%(139) datos_frescos+h_top10+il_local_gano...';
    ruleUsed = 'ML14';
}

else if (!pick && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (paEncajo) && (tienePrevAway) && (phSOT > paSOT)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML15 Val:60%(5) Train:75%(58) ind_ambas_over+ind_perf_home_good+pa_encajo...';
    ruleUsed = 'ML15';
}

else if (!pick && (h2hCol3GoalsCalc <= 2) && ((parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0)) >= 1.0) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (indRightGoals >= 3) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML16 Val:60%(5) Train:74%(35) h2h_under+ir_away_was_strong_fav+ir_failed_current...';
    ruleUsed = 'ML16';
}

Total patrones AH raw: 738
Patrones VALIDADOS y ÚNICOS: 16
else if (!pick && (datosFrescos) && (h2hCol3PorCero) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (phGoleo) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 75;
    confidence = 'high';
    reason = '🎯 ML1 Val:75%(4) Train:76%(39) datos_frescos+h2h_por_cero+ir_failed_current...';
    ruleUsed = 'ML1';
}

else if (!pick && (datosCompletos) && (localGoalsInd > rivalGoalsIndL) && (paEncajo) && (phEncajo) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML2 Val:66%(3) Train:71%(35) datos_completos+il_local_gano+pa_encajo...';
    ruleUsed = 'ML2';
}

else if (!pick && (h2hCol3PorCero) && (tieneH2HCol3) && (indLeftGoals <= 2) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (tienePrevAway) && (phSOT >= 5)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML3 Val:66%(3) Train:72%(93) h2h_por_cero+h2h_tiene+il_under...';
    ruleUsed = 'ML3';
}

else if (!pick && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (visitaPerdioContraRivalLocal) && (paEncajo) && (parseInt(prevHomeScore[0] || 0) > 0 && parseInt(prevHomeScore[1] || 0) > 0) && ((phDA >= 50) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]))) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML4 Val:66%(3) Train:75%(73) ind_perf_home_good+ir_visita_perdio+pa_encajo...';
    ruleUsed = 'ML4';
}

else if (!pick && (awayRank > 0 && awayRank <= 10) && (h2hCol3GoalsCalc === 0) && (h2hCol3PorCero) && (h2hCol3GoalsCalc <= 2) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (phDA > paDA) && (phSOT > paSOT) && (phEncajo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML5 Val:66%(3) Train:75%(28) a_top10+h2h_0goles+h2h_por_cero...';
    ruleUsed = 'ML5';
}

else if (!pick && (h2hCol3PorCero) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (tieneIndLeftU) && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML6 Val:66%(3) Train:72%(25) h2h_por_cero+il_ambos+il_tiene...';
    ruleUsed = 'ML6';
}

else if (!pick && (awayRank >= 15) && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (((parseInt((pc.h2h_col3 || {}).goles_home || 0) + parseFloat((pc.h2h_col3 || {}).ah || 0)) > parseInt((pc.h2h_col3 || {}).goles_away || 0))) && (paDA <= 30) && (prevAwayGoals >= 4)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML7 Val:66%(3) Train:77%(58) a_bottom+h2h_ah_eq+h2h_covered...';
    ruleUsed = 'ML7';
}

else if (!pick && (h2hCol3GoalsCalc === 0) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (irAwayDA > (irRivalDA * 1.5)) && (tieneIndRightU) && (prevHomeGoals <= 2)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML8 Val:66%(3) Train:84%(25) h2h_0goles+il_covered_current+ir_dominate_da...';
    ruleUsed = 'ML8';
}

else if (!pick && (datosCompletos) && ((parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0)) > 0) && (momentumLocal) && (Math.abs(ou_value - 2.5) < 0.3) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML9 Val:66%(3) Train:76%(105) datos_completos+il_was_dog+momentum_local...';
    ruleUsed = 'ML9';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (visitaPerdioContraRivalLocal) && (parseInt(prevAwayScore[0] || 0) > 0 && parseInt(prevAwayScore[1] || 0) > 0) && (phDA >= 50)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML10 Val:66%(3) Train:71%(90) h2h_ah_eq+h_top5+ir_visita_perdio...';
    ruleUsed = 'ML10';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (tienePrevAway)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML11 Val:66%(3) Train:70%(194) h2h_ah_eq+h_top5+il_ambos...';
    ruleUsed = 'ML11';
}

else if (!pick && (handicapRepetidoCerca) && (indLeftGoals >= 3 && indRightGoals >= 3) && (phDA > paDA) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML12 Val:66%(3) Train:77%(72) ha_repetido_cerca+ind_ambas_over+ph_better_da...';
    ruleUsed = 'ML12';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (localGoleoRivalVisita) && (momentumLocal) && (phSOT > paSOT) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML13 Val:66%(3) Train:80%(51) h2h_ah_eq+il_local_goleo+momentum_local...';
    ruleUsed = 'ML13';
}

else if (!pick && (datosFrescos) && (homeRank > 0 && homeRank <= 10) && (localGoalsInd > rivalGoalsIndL) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (paEncajo)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML14 Val:60%(5) Train:71%(139) datos_frescos+h_top10+il_local_gano...';
    ruleUsed = 'ML14';
}

else if (!pick && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (paEncajo) && (tienePrevAway) && (phSOT > paSOT)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML15 Val:60%(5) Train:75%(58) ind_ambas_over+ind_perf_home_good+pa_encajo...';
    ruleUsed = 'ML15';
}

else if (!pick && (h2hCol3GoalsCalc <= 2) && ((parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0)) >= 1.0) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (indRightGoals >= 3) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML16 Val:60%(5) Train:74%(35) h2h_under+ir_away_was_strong_fav+ir_failed_current...';
    ruleUsed = 'ML16';
}

Total patrones AH raw: 738
Patrones VALIDADOS y ÚNICOS: 16
else if (!pick && (datosFrescos) && (h2hCol3PorCero) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (phGoleo) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 75;
    confidence = 'high';
    reason = '🎯 ML1 Val:75%(4) Train:76%(39) datos_frescos+h2h_por_cero+ir_failed_current...';
    ruleUsed = 'ML1';
}

else if (!pick && (datosCompletos) && (localGoalsInd > rivalGoalsIndL) && (paEncajo) && (phEncajo) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML2 Val:66%(3) Train:71%(35) datos_completos+il_local_gano+pa_encajo...';
    ruleUsed = 'ML2';
}

else if (!pick && (h2hCol3PorCero) && (tieneH2HCol3) && (indLeftGoals <= 2) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (tienePrevAway) && (phSOT >= 5)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML3 Val:66%(3) Train:72%(93) h2h_por_cero+h2h_tiene+il_under...';
    ruleUsed = 'ML3';
}

else if (!pick && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (visitaPerdioContraRivalLocal) && (paEncajo) && (parseInt(prevHomeScore[0] || 0) > 0 && parseInt(prevHomeScore[1] || 0) > 0) && ((phDA >= 50) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]))) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML4 Val:66%(3) Train:75%(73) ind_perf_home_good+ir_visita_perdio+pa_encajo...';
    ruleUsed = 'ML4';
}

else if (!pick && (awayRank > 0 && awayRank <= 10) && (h2hCol3GoalsCalc === 0) && (h2hCol3PorCero) && (h2hCol3GoalsCalc <= 2) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (phDA > paDA) && (phSOT > paSOT) && (phEncajo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML5 Val:66%(3) Train:75%(28) a_top10+h2h_0goles+h2h_por_cero...';
    ruleUsed = 'ML5';
}

else if (!pick && (h2hCol3PorCero) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (tieneIndLeftU) && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML6 Val:66%(3) Train:72%(25) h2h_por_cero+il_ambos+il_tiene...';
    ruleUsed = 'ML6';
}

else if (!pick && (awayRank >= 15) && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (((parseInt((pc.h2h_col3 || {}).goles_home || 0) + parseFloat((pc.h2h_col3 || {}).ah || 0)) > parseInt((pc.h2h_col3 || {}).goles_away || 0))) && (paDA <= 30) && (prevAwayGoals >= 4)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML7 Val:66%(3) Train:77%(58) a_bottom+h2h_ah_eq+h2h_covered...';
    ruleUsed = 'ML7';
}

else if (!pick && (h2hCol3GoalsCalc === 0) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (irAwayDA > (irRivalDA * 1.5)) && (tieneIndRightU) && (prevHomeGoals <= 2)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML8 Val:66%(3) Train:84%(25) h2h_0goles+il_covered_current+ir_dominate_da...';
    ruleUsed = 'ML8';
}

else if (!pick && (datosCompletos) && ((parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0)) > 0) && (momentumLocal) && (Math.abs(ou_value - 2.5) < 0.3) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML9 Val:66%(3) Train:76%(105) datos_completos+il_was_dog+momentum_local...';
    ruleUsed = 'ML9';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (visitaPerdioContraRivalLocal) && (parseInt(prevAwayScore[0] || 0) > 0 && parseInt(prevAwayScore[1] || 0) > 0) && (phDA >= 50)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML10 Val:66%(3) Train:71%(90) h2h_ah_eq+h_top5+ir_visita_perdio...';
    ruleUsed = 'ML10';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (tienePrevAway)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML11 Val:66%(3) Train:70%(194) h2h_ah_eq+h_top5+il_ambos...';
    ruleUsed = 'ML11';
}

else if (!pick && (handicapRepetidoCerca) && (indLeftGoals >= 3 && indRightGoals >= 3) && (phDA > paDA) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML12 Val:66%(3) Train:77%(72) ha_repetido_cerca+ind_ambas_over+ph_better_da...';
    ruleUsed = 'ML12';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (localGoleoRivalVisita) && (momentumLocal) && (phSOT > paSOT) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML13 Val:66%(3) Train:80%(51) h2h_ah_eq+il_local_goleo+momentum_local...';
    ruleUsed = 'ML13';
}

else if (!pick && (datosFrescos) && (homeRank > 0 && homeRank <= 10) && (localGoalsInd > rivalGoalsIndL) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (paEncajo)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML14 Val:60%(5) Train:71%(139) datos_frescos+h_top10+il_local_gano...';
    ruleUsed = 'ML14';
}

else if (!pick && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (paEncajo) && (tienePrevAway) && (phSOT > paSOT)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML15 Val:60%(5) Train:75%(58) ind_ambas_over+ind_perf_home_good+pa_encajo...';
    ruleUsed = 'ML15';
}

else if (!pick && (h2hCol3GoalsCalc <= 2) && ((parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0)) >= 1.0) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (indRightGoals >= 3) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML16 Val:60%(5) Train:74%(35) h2h_under+ir_away_was_strong_fav+ir_failed_current...';
    ruleUsed = 'ML16';
}

Total patrones AH raw: 738
Patrones VALIDADOS y ÚNICOS: 16
else if (!pick && (datosFrescos) && (h2hCol3PorCero) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (phGoleo) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 75;
    confidence = 'high';
    reason = '🎯 ML1 Val:75%(4) Train:76%(39) datos_frescos+h2h_por_cero+ir_failed_current...';
    ruleUsed = 'ML1';
}

else if (!pick && (datosCompletos) && (localGoalsInd > rivalGoalsIndL) && (paEncajo) && (phEncajo) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML2 Val:66%(3) Train:71%(35) datos_completos+il_local_gano+pa_encajo...';
    ruleUsed = 'ML2';
}

else if (!pick && (h2hCol3PorCero) && (tieneH2HCol3) && (indLeftGoals <= 2) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (tienePrevAway) && (phSOT >= 5)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML3 Val:66%(3) Train:72%(93) h2h_por_cero+h2h_tiene+il_under...';
    ruleUsed = 'ML3';
}

else if (!pick && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (visitaPerdioContraRivalLocal) && (paEncajo) && (parseInt(prevHomeScore[0] || 0) > 0 && parseInt(prevHomeScore[1] || 0) > 0) && ((phDA >= 50) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]))) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML4 Val:66%(3) Train:75%(73) ind_perf_home_good+ir_visita_perdio+pa_encajo...';
    ruleUsed = 'ML4';
}

else if (!pick && (awayRank > 0 && awayRank <= 10) && (h2hCol3GoalsCalc === 0) && (h2hCol3PorCero) && (h2hCol3GoalsCalc <= 2) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (phDA > paDA) && (phSOT > paSOT) && (phEncajo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML5 Val:66%(3) Train:75%(28) a_top10+h2h_0goles+h2h_por_cero...';
    ruleUsed = 'ML5';
}

else if (!pick && (h2hCol3PorCero) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (tieneIndLeftU) && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML6 Val:66%(3) Train:72%(25) h2h_por_cero+il_ambos+il_tiene...';
    ruleUsed = 'ML6';
}

else if (!pick && (awayRank >= 15) && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (((parseInt((pc.h2h_col3 || {}).goles_home || 0) + parseFloat((pc.h2h_col3 || {}).ah || 0)) > parseInt((pc.h2h_col3 || {}).goles_away || 0))) && (paDA <= 30) && (prevAwayGoals >= 4)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML7 Val:66%(3) Train:77%(58) a_bottom+h2h_ah_eq+h2h_covered...';
    ruleUsed = 'ML7';
}

else if (!pick && (h2hCol3GoalsCalc === 0) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (irAwayDA > (irRivalDA * 1.5)) && (tieneIndRightU) && (prevHomeGoals <= 2)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML8 Val:66%(3) Train:84%(25) h2h_0goles+il_covered_current+ir_dominate_da...';
    ruleUsed = 'ML8';
}

else if (!pick && (datosCompletos) && ((parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0)) > 0) && (momentumLocal) && (Math.abs(ou_value - 2.5) < 0.3) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML9 Val:66%(3) Train:76%(105) datos_completos+il_was_dog+momentum_local...';
    ruleUsed = 'ML9';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (visitaPerdioContraRivalLocal) && (parseInt(prevAwayScore[0] || 0) > 0 && parseInt(prevAwayScore[1] || 0) > 0) && (phDA >= 50)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML10 Val:66%(3) Train:71%(90) h2h_ah_eq+h_top5+ir_visita_perdio...';
    ruleUsed = 'ML10';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (tienePrevAway)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML11 Val:66%(3) Train:70%(194) h2h_ah_eq+h_top5+il_ambos...';
    ruleUsed = 'ML11';
}

else if (!pick && (handicapRepetidoCerca) && (indLeftGoals >= 3 && indRightGoals >= 3) && (phDA > paDA) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML12 Val:66%(3) Train:77%(72) ha_repetido_cerca+ind_ambas_over+ph_better_da...';
    ruleUsed = 'ML12';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (localGoleoRivalVisita) && (momentumLocal) && (phSOT > paSOT) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML13 Val:66%(3) Train:80%(51) h2h_ah_eq+il_local_goleo+momentum_local...';
    ruleUsed = 'ML13';
}

else if (!pick && (datosFrescos) && (homeRank > 0 && homeRank <= 10) && (localGoalsInd > rivalGoalsIndL) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (paEncajo)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML14 Val:60%(5) Train:71%(139) datos_frescos+h_top10+il_local_gano...';
    ruleUsed = 'ML14';
}

else if (!pick && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (paEncajo) && (tienePrevAway) && (phSOT > paSOT)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML15 Val:60%(5) Train:75%(58) ind_ambas_over+ind_perf_home_good+pa_encajo...';
    ruleUsed = 'ML15';
}

else if (!pick && (h2hCol3GoalsCalc <= 2) && ((parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0)) >= 1.0) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (indRightGoals >= 3) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML16 Val:60%(5) Train:74%(35) h2h_under+ir_away_was_strong_fav+ir_failed_current...';
    ruleUsed = 'ML16';
}

Total patrones AH raw: 738
Patrones VALIDADOS y ÚNICOS: 16
else if (!pick && (datosFrescos) && (h2hCol3PorCero) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (phGoleo) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 75;
    confidence = 'high';
    reason = '🎯 ML1 Val:75%(4) Train:76%(39) datos_frescos+h2h_por_cero+ir_failed_current...';
    ruleUsed = 'ML1';
}

else if (!pick && (datosCompletos) && (localGoalsInd > rivalGoalsIndL) && (paEncajo) && (phEncajo) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML2 Val:66%(3) Train:71%(35) datos_completos+il_local_gano+pa_encajo...';
    ruleUsed = 'ML2';
}

else if (!pick && (h2hCol3PorCero) && (tieneH2HCol3) && (indLeftGoals <= 2) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (tienePrevAway) && (phSOT >= 5)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML3 Val:66%(3) Train:72%(93) h2h_por_cero+h2h_tiene+il_under...';
    ruleUsed = 'ML3';
}

else if (!pick && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (visitaPerdioContraRivalLocal) && (paEncajo) && (parseInt(prevHomeScore[0] || 0) > 0 && parseInt(prevHomeScore[1] || 0) > 0) && ((phDA >= 50) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]))) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML4 Val:66%(3) Train:75%(73) ind_perf_home_good+ir_visita_perdio+pa_encajo...';
    ruleUsed = 'ML4';
}

else if (!pick && (awayRank > 0 && awayRank <= 10) && (h2hCol3GoalsCalc === 0) && (h2hCol3PorCero) && (h2hCol3GoalsCalc <= 2) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (phDA > paDA) && (phSOT > paSOT) && (phEncajo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML5 Val:66%(3) Train:75%(28) a_top10+h2h_0goles+h2h_por_cero...';
    ruleUsed = 'ML5';
}

else if (!pick && (h2hCol3PorCero) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (tieneIndLeftU) && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML6 Val:66%(3) Train:72%(25) h2h_por_cero+il_ambos+il_tiene...';
    ruleUsed = 'ML6';
}

else if (!pick && (awayRank >= 15) && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (((parseInt((pc.h2h_col3 || {}).goles_home || 0) + parseFloat((pc.h2h_col3 || {}).ah || 0)) > parseInt((pc.h2h_col3 || {}).goles_away || 0))) && (paDA <= 30) && (prevAwayGoals >= 4)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML7 Val:66%(3) Train:77%(58) a_bottom+h2h_ah_eq+h2h_covered...';
    ruleUsed = 'ML7';
}

else if (!pick && (h2hCol3GoalsCalc === 0) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (irAwayDA > (irRivalDA * 1.5)) && (tieneIndRightU) && (prevHomeGoals <= 2)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML8 Val:66%(3) Train:84%(25) h2h_0goles+il_covered_current+ir_dominate_da...';
    ruleUsed = 'ML8';
}

else if (!pick && (datosCompletos) && ((parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0)) > 0) && (momentumLocal) && (Math.abs(ou_value - 2.5) < 0.3) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML9 Val:66%(3) Train:76%(105) datos_completos+il_was_dog+momentum_local...';
    ruleUsed = 'ML9';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (visitaPerdioContraRivalLocal) && (parseInt(prevAwayScore[0] || 0) > 0 && parseInt(prevAwayScore[1] || 0) > 0) && (phDA >= 50)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML10 Val:66%(3) Train:71%(90) h2h_ah_eq+h_top5+ir_visita_perdio...';
    ruleUsed = 'ML10';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (tienePrevAway)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML11 Val:66%(3) Train:70%(194) h2h_ah_eq+h_top5+il_ambos...';
    ruleUsed = 'ML11';
}

else if (!pick && (handicapRepetidoCerca) && (indLeftGoals >= 3 && indRightGoals >= 3) && (phDA > paDA) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML12 Val:66%(3) Train:77%(72) ha_repetido_cerca+ind_ambas_over+ph_better_da...';
    ruleUsed = 'ML12';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (localGoleoRivalVisita) && (momentumLocal) && (phSOT > paSOT) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML13 Val:66%(3) Train:80%(51) h2h_ah_eq+il_local_goleo+momentum_local...';
    ruleUsed = 'ML13';
}

else if (!pick && (datosFrescos) && (homeRank > 0 && homeRank <= 10) && (localGoalsInd > rivalGoalsIndL) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (paEncajo)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML14 Val:60%(5) Train:71%(139) datos_frescos+h_top10+il_local_gano...';
    ruleUsed = 'ML14';
}

else if (!pick && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (paEncajo) && (tienePrevAway) && (phSOT > paSOT)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML15 Val:60%(5) Train:75%(58) ind_ambas_over+ind_perf_home_good+pa_encajo...';
    ruleUsed = 'ML15';
}

else if (!pick && (h2hCol3GoalsCalc <= 2) && ((parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0)) >= 1.0) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (indRightGoals >= 3) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML16 Val:60%(5) Train:74%(35) h2h_under+ir_away_was_strong_fav+ir_failed_current...';
    ruleUsed = 'ML16';
}

Total patrones AH raw: 738
Patrones VALIDADOS y ÚNICOS: 16
else if (!pick && (datosFrescos) && (h2hCol3PorCero) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (phGoleo) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 75;
    confidence = 'high';
    reason = '🎯 ML1 Val:75%(4) Train:76%(39) datos_frescos+h2h_por_cero+ir_failed_current...';
    ruleUsed = 'ML1';
}

else if (!pick && (datosCompletos) && (localGoalsInd > rivalGoalsIndL) && (paEncajo) && (phEncajo) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML2 Val:66%(3) Train:71%(35) datos_completos+il_local_gano+pa_encajo...';
    ruleUsed = 'ML2';
}

else if (!pick && (h2hCol3PorCero) && (tieneH2HCol3) && (indLeftGoals <= 2) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (tienePrevAway) && (phSOT >= 5)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML3 Val:66%(3) Train:72%(93) h2h_por_cero+h2h_tiene+il_under...';
    ruleUsed = 'ML3';
}

else if (!pick && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (visitaPerdioContraRivalLocal) && (paEncajo) && (parseInt(prevHomeScore[0] || 0) > 0 && parseInt(prevHomeScore[1] || 0) > 0) && ((phDA >= 50) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]))) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML4 Val:66%(3) Train:75%(73) ind_perf_home_good+ir_visita_perdio+pa_encajo...';
    ruleUsed = 'ML4';
}

else if (!pick && (awayRank > 0 && awayRank <= 10) && (h2hCol3GoalsCalc === 0) && (h2hCol3PorCero) && (h2hCol3GoalsCalc <= 2) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (phDA > paDA) && (phSOT > paSOT) && (phEncajo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML5 Val:66%(3) Train:75%(28) a_top10+h2h_0goles+h2h_por_cero...';
    ruleUsed = 'ML5';
}

else if (!pick && (h2hCol3PorCero) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (tieneIndLeftU) && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML6 Val:66%(3) Train:72%(25) h2h_por_cero+il_ambos+il_tiene...';
    ruleUsed = 'ML6';
}

else if (!pick && (awayRank >= 15) && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (((parseInt((pc.h2h_col3 || {}).goles_home || 0) + parseFloat((pc.h2h_col3 || {}).ah || 0)) > parseInt((pc.h2h_col3 || {}).goles_away || 0))) && (paDA <= 30) && (prevAwayGoals >= 4)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML7 Val:66%(3) Train:77%(58) a_bottom+h2h_ah_eq+h2h_covered...';
    ruleUsed = 'ML7';
}

else if (!pick && (h2hCol3GoalsCalc === 0) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (irAwayDA > (irRivalDA * 1.5)) && (tieneIndRightU) && (prevHomeGoals <= 2)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML8 Val:66%(3) Train:84%(25) h2h_0goles+il_covered_current+ir_dominate_da...';
    ruleUsed = 'ML8';
}

else if (!pick && (datosCompletos) && ((parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0)) > 0) && (momentumLocal) && (Math.abs(ou_value - 2.5) < 0.3) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML9 Val:66%(3) Train:76%(105) datos_completos+il_was_dog+momentum_local...';
    ruleUsed = 'ML9';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (visitaPerdioContraRivalLocal) && (parseInt(prevAwayScore[0] || 0) > 0 && parseInt(prevAwayScore[1] || 0) > 0) && (phDA >= 50)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML10 Val:66%(3) Train:71%(90) h2h_ah_eq+h_top5+ir_visita_perdio...';
    ruleUsed = 'ML10';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (tienePrevAway)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML11 Val:66%(3) Train:70%(194) h2h_ah_eq+h_top5+il_ambos...';
    ruleUsed = 'ML11';
}

else if (!pick && (handicapRepetidoCerca) && (indLeftGoals >= 3 && indRightGoals >= 3) && (phDA > paDA) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML12 Val:66%(3) Train:77%(72) ha_repetido_cerca+ind_ambas_over+ph_better_da...';
    ruleUsed = 'ML12';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (localGoleoRivalVisita) && (momentumLocal) && (phSOT > paSOT) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML13 Val:66%(3) Train:80%(51) h2h_ah_eq+il_local_goleo+momentum_local...';
    ruleUsed = 'ML13';
}

else if (!pick && (datosFrescos) && (homeRank > 0 && homeRank <= 10) && (localGoalsInd > rivalGoalsIndL) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (paEncajo)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML14 Val:60%(5) Train:71%(139) datos_frescos+h_top10+il_local_gano...';
    ruleUsed = 'ML14';
}

else if (!pick && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (paEncajo) && (tienePrevAway) && (phSOT > paSOT)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML15 Val:60%(5) Train:75%(58) ind_ambas_over+ind_perf_home_good+pa_encajo...';
    ruleUsed = 'ML15';
}

else if (!pick && (h2hCol3GoalsCalc <= 2) && ((parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0)) >= 1.0) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (indRightGoals >= 3) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML16 Val:60%(5) Train:74%(35) h2h_under+ir_away_was_strong_fav+ir_failed_current...';
    ruleUsed = 'ML16';
}

Total patrones AH raw: 738
Patrones VALIDADOS y ÚNICOS: 16
else if (!pick && (datosFrescos) && (h2hCol3PorCero) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (phGoleo) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 75;
    confidence = 'high';
    reason = '🎯 ML1 Val:75%(4) Train:76%(39) datos_frescos+h2h_por_cero+ir_failed_current...';
    ruleUsed = 'ML1';
}

else if (!pick && (datosCompletos) && (localGoalsInd > rivalGoalsIndL) && (paEncajo) && (phEncajo) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML2 Val:66%(3) Train:71%(35) datos_completos+il_local_gano+pa_encajo...';
    ruleUsed = 'ML2';
}

else if (!pick && (h2hCol3PorCero) && (tieneH2HCol3) && (indLeftGoals <= 2) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (tienePrevAway) && (phSOT >= 5)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML3 Val:66%(3) Train:72%(93) h2h_por_cero+h2h_tiene+il_under...';
    ruleUsed = 'ML3';
}

else if (!pick && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (visitaPerdioContraRivalLocal) && (paEncajo) && (parseInt(prevHomeScore[0] || 0) > 0 && parseInt(prevHomeScore[1] || 0) > 0) && ((phDA >= 50) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]))) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML4 Val:66%(3) Train:75%(73) ind_perf_home_good+ir_visita_perdio+pa_encajo...';
    ruleUsed = 'ML4';
}

else if (!pick && (awayRank > 0 && awayRank <= 10) && (h2hCol3GoalsCalc === 0) && (h2hCol3PorCero) && (h2hCol3GoalsCalc <= 2) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (phDA > paDA) && (phSOT > paSOT) && (phEncajo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML5 Val:66%(3) Train:75%(28) a_top10+h2h_0goles+h2h_por_cero...';
    ruleUsed = 'ML5';
}

else if (!pick && (h2hCol3PorCero) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (tieneIndLeftU) && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML6 Val:66%(3) Train:72%(25) h2h_por_cero+il_ambos+il_tiene...';
    ruleUsed = 'ML6';
}

else if (!pick && (awayRank >= 15) && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (((parseInt((pc.h2h_col3 || {}).goles_home || 0) + parseFloat((pc.h2h_col3 || {}).ah || 0)) > parseInt((pc.h2h_col3 || {}).goles_away || 0))) && (paDA <= 30) && (prevAwayGoals >= 4)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML7 Val:66%(3) Train:77%(58) a_bottom+h2h_ah_eq+h2h_covered...';
    ruleUsed = 'ML7';
}

else if (!pick && (h2hCol3GoalsCalc === 0) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (irAwayDA > (irRivalDA * 1.5)) && (tieneIndRightU) && (prevHomeGoals <= 2)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML8 Val:66%(3) Train:84%(25) h2h_0goles+il_covered_current+ir_dominate_da...';
    ruleUsed = 'ML8';
}

else if (!pick && (datosCompletos) && ((parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0)) > 0) && (momentumLocal) && (Math.abs(ou_value - 2.5) < 0.3) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML9 Val:66%(3) Train:76%(105) datos_completos+il_was_dog+momentum_local...';
    ruleUsed = 'ML9';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (visitaPerdioContraRivalLocal) && (parseInt(prevAwayScore[0] || 0) > 0 && parseInt(prevAwayScore[1] || 0) > 0) && (phDA >= 50)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML10 Val:66%(3) Train:71%(90) h2h_ah_eq+h_top5+ir_visita_perdio...';
    ruleUsed = 'ML10';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (tienePrevAway)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML11 Val:66%(3) Train:70%(194) h2h_ah_eq+h_top5+il_ambos...';
    ruleUsed = 'ML11';
}

else if (!pick && (handicapRepetidoCerca) && (indLeftGoals >= 3 && indRightGoals >= 3) && (phDA > paDA) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML12 Val:66%(3) Train:77%(72) ha_repetido_cerca+ind_ambas_over+ph_better_da...';
    ruleUsed = 'ML12';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (localGoleoRivalVisita) && (momentumLocal) && (phSOT > paSOT) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML13 Val:66%(3) Train:80%(51) h2h_ah_eq+il_local_goleo+momentum_local...';
    ruleUsed = 'ML13';
}

else if (!pick && (datosFrescos) && (homeRank > 0 && homeRank <= 10) && (localGoalsInd > rivalGoalsIndL) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (paEncajo)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML14 Val:60%(5) Train:71%(139) datos_frescos+h_top10+il_local_gano...';
    ruleUsed = 'ML14';
}

else if (!pick && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (paEncajo) && (tienePrevAway) && (phSOT > paSOT)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML15 Val:60%(5) Train:75%(58) ind_ambas_over+ind_perf_home_good+pa_encajo...';
    ruleUsed = 'ML15';
}

else if (!pick && (h2hCol3GoalsCalc <= 2) && ((parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0)) >= 1.0) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (indRightGoals >= 3) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML16 Val:60%(5) Train:74%(35) h2h_under+ir_away_was_strong_fav+ir_failed_current...';
    ruleUsed = 'ML16';
}

Total patrones AH raw: 738
Patrones VALIDADOS y ÚNICOS: 16
else if (!pick && (datosFrescos) && (h2hCol3PorCero) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (phGoleo) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 75;
    confidence = 'high';
    reason = '🎯 ML1 Val:75%(4) Train:76%(39) datos_frescos+h2h_por_cero+ir_failed_current...';
    ruleUsed = 'ML1';
}

else if (!pick && (datosCompletos) && (localGoalsInd > rivalGoalsIndL) && (paEncajo) && (phEncajo) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML2 Val:66%(3) Train:71%(35) datos_completos+il_local_gano+pa_encajo...';
    ruleUsed = 'ML2';
}

else if (!pick && (h2hCol3PorCero) && (tieneH2HCol3) && (indLeftGoals <= 2) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (tienePrevAway) && (phSOT >= 5)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML3 Val:66%(3) Train:72%(93) h2h_por_cero+h2h_tiene+il_under...';
    ruleUsed = 'ML3';
}

else if (!pick && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (visitaPerdioContraRivalLocal) && (paEncajo) && (parseInt(prevHomeScore[0] || 0) > 0 && parseInt(prevHomeScore[1] || 0) > 0) && ((phDA >= 50) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]))) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML4 Val:66%(3) Train:75%(73) ind_perf_home_good+ir_visita_perdio+pa_encajo...';
    ruleUsed = 'ML4';
}

else if (!pick && (awayRank > 0 && awayRank <= 10) && (h2hCol3GoalsCalc === 0) && (h2hCol3PorCero) && (h2hCol3GoalsCalc <= 2) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (phDA > paDA) && (phSOT > paSOT) && (phEncajo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML5 Val:66%(3) Train:75%(28) a_top10+h2h_0goles+h2h_por_cero...';
    ruleUsed = 'ML5';
}

else if (!pick && (h2hCol3PorCero) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (tieneIndLeftU) && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML6 Val:66%(3) Train:72%(25) h2h_por_cero+il_ambos+il_tiene...';
    ruleUsed = 'ML6';
}

else if (!pick && (awayRank >= 15) && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (((parseInt((pc.h2h_col3 || {}).goles_home || 0) + parseFloat((pc.h2h_col3 || {}).ah || 0)) > parseInt((pc.h2h_col3 || {}).goles_away || 0))) && (paDA <= 30) && (prevAwayGoals >= 4)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML7 Val:66%(3) Train:77%(58) a_bottom+h2h_ah_eq+h2h_covered...';
    ruleUsed = 'ML7';
}

else if (!pick && (h2hCol3GoalsCalc === 0) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (irAwayDA > (irRivalDA * 1.5)) && (tieneIndRightU) && (prevHomeGoals <= 2)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML8 Val:66%(3) Train:84%(25) h2h_0goles+il_covered_current+ir_dominate_da...';
    ruleUsed = 'ML8';
}

else if (!pick && (datosCompletos) && ((parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0)) > 0) && (momentumLocal) && (Math.abs(ou_value - 2.5) < 0.3) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML9 Val:66%(3) Train:76%(105) datos_completos+il_was_dog+momentum_local...';
    ruleUsed = 'ML9';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (visitaPerdioContraRivalLocal) && (parseInt(prevAwayScore[0] || 0) > 0 && parseInt(prevAwayScore[1] || 0) > 0) && (phDA >= 50)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML10 Val:66%(3) Train:71%(90) h2h_ah_eq+h_top5+ir_visita_perdio...';
    ruleUsed = 'ML10';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (tienePrevAway)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML11 Val:66%(3) Train:70%(194) h2h_ah_eq+h_top5+il_ambos...';
    ruleUsed = 'ML11';
}

else if (!pick && (handicapRepetidoCerca) && (indLeftGoals >= 3 && indRightGoals >= 3) && (phDA > paDA) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML12 Val:66%(3) Train:77%(72) ha_repetido_cerca+ind_ambas_over+ph_better_da...';
    ruleUsed = 'ML12';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (localGoleoRivalVisita) && (momentumLocal) && (phSOT > paSOT) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML13 Val:66%(3) Train:80%(51) h2h_ah_eq+il_local_goleo+momentum_local...';
    ruleUsed = 'ML13';
}

else if (!pick && (datosFrescos) && (homeRank > 0 && homeRank <= 10) && (localGoalsInd > rivalGoalsIndL) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (paEncajo)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML14 Val:60%(5) Train:71%(139) datos_frescos+h_top10+il_local_gano...';
    ruleUsed = 'ML14';
}

else if (!pick && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (paEncajo) && (tienePrevAway) && (phSOT > paSOT)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML15 Val:60%(5) Train:75%(58) ind_ambas_over+ind_perf_home_good+pa_encajo...';
    ruleUsed = 'ML15';
}

else if (!pick && (h2hCol3GoalsCalc <= 2) && ((parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0)) >= 1.0) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (indRightGoals >= 3) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML16 Val:60%(5) Train:74%(35) h2h_under+ir_away_was_strong_fav+ir_failed_current...';
    ruleUsed = 'ML16';
}

Total patrones AH raw: 738
Patrones VALIDADOS y ÚNICOS: 16
else if (!pick && (datosFrescos) && (h2hCol3PorCero) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (phGoleo) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 75;
    confidence = 'high';
    reason = '🎯 ML1 Val:75%(4) Train:76%(39) datos_frescos+h2h_por_cero+ir_failed_current...';
    ruleUsed = 'ML1';
}

else if (!pick && (datosCompletos) && (localGoalsInd > rivalGoalsIndL) && (paEncajo) && (phEncajo) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML2 Val:66%(3) Train:71%(35) datos_completos+il_local_gano+pa_encajo...';
    ruleUsed = 'ML2';
}

else if (!pick && (h2hCol3PorCero) && (tieneH2HCol3) && (indLeftGoals <= 2) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (tienePrevAway) && (phSOT >= 5)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML3 Val:66%(3) Train:72%(93) h2h_por_cero+h2h_tiene+il_under...';
    ruleUsed = 'ML3';
}

else if (!pick && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (visitaPerdioContraRivalLocal) && (paEncajo) && (parseInt(prevHomeScore[0] || 0) > 0 && parseInt(prevHomeScore[1] || 0) > 0) && ((phDA >= 50) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]))) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML4 Val:66%(3) Train:75%(73) ind_perf_home_good+ir_visita_perdio+pa_encajo...';
    ruleUsed = 'ML4';
}

else if (!pick && (awayRank > 0 && awayRank <= 10) && (h2hCol3GoalsCalc === 0) && (h2hCol3PorCero) && (h2hCol3GoalsCalc <= 2) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (phDA > paDA) && (phSOT > paSOT) && (phEncajo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML5 Val:66%(3) Train:75%(28) a_top10+h2h_0goles+h2h_por_cero...';
    ruleUsed = 'ML5';
}

else if (!pick && (h2hCol3PorCero) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (tieneIndLeftU) && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML6 Val:66%(3) Train:72%(25) h2h_por_cero+il_ambos+il_tiene...';
    ruleUsed = 'ML6';
}

else if (!pick && (awayRank >= 15) && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (((parseInt((pc.h2h_col3 || {}).goles_home || 0) + parseFloat((pc.h2h_col3 || {}).ah || 0)) > parseInt((pc.h2h_col3 || {}).goles_away || 0))) && (paDA <= 30) && (prevAwayGoals >= 4)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML7 Val:66%(3) Train:77%(58) a_bottom+h2h_ah_eq+h2h_covered...';
    ruleUsed = 'ML7';
}

else if (!pick && (h2hCol3GoalsCalc === 0) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (irAwayDA > (irRivalDA * 1.5)) && (tieneIndRightU) && (prevHomeGoals <= 2)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML8 Val:66%(3) Train:84%(25) h2h_0goles+il_covered_current+ir_dominate_da...';
    ruleUsed = 'ML8';
}

else if (!pick && (datosCompletos) && ((parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0)) > 0) && (momentumLocal) && (Math.abs(ou_value - 2.5) < 0.3) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML9 Val:66%(3) Train:76%(105) datos_completos+il_was_dog+momentum_local...';
    ruleUsed = 'ML9';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (visitaPerdioContraRivalLocal) && (parseInt(prevAwayScore[0] || 0) > 0 && parseInt(prevAwayScore[1] || 0) > 0) && (phDA >= 50)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML10 Val:66%(3) Train:71%(90) h2h_ah_eq+h_top5+ir_visita_perdio...';
    ruleUsed = 'ML10';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (tienePrevAway)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML11 Val:66%(3) Train:70%(194) h2h_ah_eq+h_top5+il_ambos...';
    ruleUsed = 'ML11';
}

else if (!pick && (handicapRepetidoCerca) && (indLeftGoals >= 3 && indRightGoals >= 3) && (phDA > paDA) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML12 Val:66%(3) Train:77%(72) ha_repetido_cerca+ind_ambas_over+ph_better_da...';
    ruleUsed = 'ML12';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (localGoleoRivalVisita) && (momentumLocal) && (phSOT > paSOT) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML13 Val:66%(3) Train:80%(51) h2h_ah_eq+il_local_goleo+momentum_local...';
    ruleUsed = 'ML13';
}

else if (!pick && (datosFrescos) && (homeRank > 0 && homeRank <= 10) && (localGoalsInd > rivalGoalsIndL) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (paEncajo)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML14 Val:60%(5) Train:71%(139) datos_frescos+h_top10+il_local_gano...';
    ruleUsed = 'ML14';
}

else if (!pick && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (paEncajo) && (tienePrevAway) && (phSOT > paSOT)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML15 Val:60%(5) Train:75%(58) ind_ambas_over+ind_perf_home_good+pa_encajo...';
    ruleUsed = 'ML15';
}

else if (!pick && (h2hCol3GoalsCalc <= 2) && ((parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0)) >= 1.0) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (indRightGoals >= 3) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML16 Val:60%(5) Train:74%(35) h2h_under+ir_away_was_strong_fav+ir_failed_current...';
    ruleUsed = 'ML16';
}

Total patrones AH raw: 738
Patrones VALIDADOS y ÚNICOS: 16
else if (!pick && (datosFrescos) && (h2hCol3PorCero) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (phGoleo) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 75;
    confidence = 'high';
    reason = '🎯 ML1 Val:75%(4) Train:76%(39) datos_frescos+h2h_por_cero+ir_failed_current...';
    ruleUsed = 'ML1';
}

else if (!pick && (datosCompletos) && (localGoalsInd > rivalGoalsIndL) && (paEncajo) && (phEncajo) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML2 Val:66%(3) Train:71%(35) datos_completos+il_local_gano+pa_encajo...';
    ruleUsed = 'ML2';
}

else if (!pick && (h2hCol3PorCero) && (tieneH2HCol3) && (indLeftGoals <= 2) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (tienePrevAway) && (phSOT >= 5)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML3 Val:66%(3) Train:72%(93) h2h_por_cero+h2h_tiene+il_under...';
    ruleUsed = 'ML3';
}

else if (!pick && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (visitaPerdioContraRivalLocal) && (paEncajo) && (parseInt(prevHomeScore[0] || 0) > 0 && parseInt(prevHomeScore[1] || 0) > 0) && ((phDA >= 50) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]))) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML4 Val:66%(3) Train:75%(73) ind_perf_home_good+ir_visita_perdio+pa_encajo...';
    ruleUsed = 'ML4';
}

else if (!pick && (awayRank > 0 && awayRank <= 10) && (h2hCol3GoalsCalc === 0) && (h2hCol3PorCero) && (h2hCol3GoalsCalc <= 2) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (phDA > paDA) && (phSOT > paSOT) && (phEncajo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML5 Val:66%(3) Train:75%(28) a_top10+h2h_0goles+h2h_por_cero...';
    ruleUsed = 'ML5';
}

else if (!pick && (h2hCol3PorCero) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (tieneIndLeftU) && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML6 Val:66%(3) Train:72%(25) h2h_por_cero+il_ambos+il_tiene...';
    ruleUsed = 'ML6';
}

else if (!pick && (awayRank >= 15) && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (((parseInt((pc.h2h_col3 || {}).goles_home || 0) + parseFloat((pc.h2h_col3 || {}).ah || 0)) > parseInt((pc.h2h_col3 || {}).goles_away || 0))) && (paDA <= 30) && (prevAwayGoals >= 4)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML7 Val:66%(3) Train:77%(58) a_bottom+h2h_ah_eq+h2h_covered...';
    ruleUsed = 'ML7';
}

else if (!pick && (h2hCol3GoalsCalc === 0) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (irAwayDA > (irRivalDA * 1.5)) && (tieneIndRightU) && (prevHomeGoals <= 2)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML8 Val:66%(3) Train:84%(25) h2h_0goles+il_covered_current+ir_dominate_da...';
    ruleUsed = 'ML8';
}

else if (!pick && (datosCompletos) && ((parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0)) > 0) && (momentumLocal) && (Math.abs(ou_value - 2.5) < 0.3) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML9 Val:66%(3) Train:76%(105) datos_completos+il_was_dog+momentum_local...';
    ruleUsed = 'ML9';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (visitaPerdioContraRivalLocal) && (parseInt(prevAwayScore[0] || 0) > 0 && parseInt(prevAwayScore[1] || 0) > 0) && (phDA >= 50)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML10 Val:66%(3) Train:71%(90) h2h_ah_eq+h_top5+ir_visita_perdio...';
    ruleUsed = 'ML10';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (tienePrevAway)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML11 Val:66%(3) Train:70%(194) h2h_ah_eq+h_top5+il_ambos...';
    ruleUsed = 'ML11';
}

else if (!pick && (handicapRepetidoCerca) && (indLeftGoals >= 3 && indRightGoals >= 3) && (phDA > paDA) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML12 Val:66%(3) Train:77%(72) ha_repetido_cerca+ind_ambas_over+ph_better_da...';
    ruleUsed = 'ML12';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (localGoleoRivalVisita) && (momentumLocal) && (phSOT > paSOT) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML13 Val:66%(3) Train:80%(51) h2h_ah_eq+il_local_goleo+momentum_local...';
    ruleUsed = 'ML13';
}

else if (!pick && (datosFrescos) && (homeRank > 0 && homeRank <= 10) && (localGoalsInd > rivalGoalsIndL) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (paEncajo)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML14 Val:60%(5) Train:71%(139) datos_frescos+h_top10+il_local_gano...';
    ruleUsed = 'ML14';
}

else if (!pick && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (paEncajo) && (tienePrevAway) && (phSOT > paSOT)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML15 Val:60%(5) Train:75%(58) ind_ambas_over+ind_perf_home_good+pa_encajo...';
    ruleUsed = 'ML15';
}

else if (!pick && (h2hCol3GoalsCalc <= 2) && ((parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0)) >= 1.0) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (indRightGoals >= 3) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML16 Val:60%(5) Train:74%(35) h2h_under+ir_away_was_strong_fav+ir_failed_current...';
    ruleUsed = 'ML16';
}

Total patrones AH raw: 738
Patrones VALIDADOS y ÚNICOS: 16
else if (!pick && (datosFrescos) && (h2hCol3PorCero) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (phGoleo) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 75;
    confidence = 'high';
    reason = '🎯 ML1 Val:75%(4) Train:76%(39) datos_frescos+h2h_por_cero+ir_failed_current...';
    ruleUsed = 'ML1';
}

else if (!pick && (datosCompletos) && (localGoalsInd > rivalGoalsIndL) && (paEncajo) && (phEncajo) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML2 Val:66%(3) Train:71%(35) datos_completos+il_local_gano+pa_encajo...';
    ruleUsed = 'ML2';
}

else if (!pick && (h2hCol3PorCero) && (tieneH2HCol3) && (indLeftGoals <= 2) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (tienePrevAway) && (phSOT >= 5)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML3 Val:66%(3) Train:72%(93) h2h_por_cero+h2h_tiene+il_under...';
    ruleUsed = 'ML3';
}

else if (!pick && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (visitaPerdioContraRivalLocal) && (paEncajo) && (parseInt(prevHomeScore[0] || 0) > 0 && parseInt(prevHomeScore[1] || 0) > 0) && ((phDA >= 50) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]))) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML4 Val:66%(3) Train:75%(73) ind_perf_home_good+ir_visita_perdio+pa_encajo...';
    ruleUsed = 'ML4';
}

else if (!pick && (awayRank > 0 && awayRank <= 10) && (h2hCol3GoalsCalc === 0) && (h2hCol3PorCero) && (h2hCol3GoalsCalc <= 2) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (phDA > paDA) && (phSOT > paSOT) && (phEncajo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML5 Val:66%(3) Train:75%(28) a_top10+h2h_0goles+h2h_por_cero...';
    ruleUsed = 'ML5';
}

else if (!pick && (h2hCol3PorCero) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (tieneIndLeftU) && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML6 Val:66%(3) Train:72%(25) h2h_por_cero+il_ambos+il_tiene...';
    ruleUsed = 'ML6';
}

else if (!pick && (awayRank >= 15) && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (((parseInt((pc.h2h_col3 || {}).goles_home || 0) + parseFloat((pc.h2h_col3 || {}).ah || 0)) > parseInt((pc.h2h_col3 || {}).goles_away || 0))) && (paDA <= 30) && (prevAwayGoals >= 4)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML7 Val:66%(3) Train:77%(58) a_bottom+h2h_ah_eq+h2h_covered...';
    ruleUsed = 'ML7';
}

else if (!pick && (h2hCol3GoalsCalc === 0) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (irAwayDA > (irRivalDA * 1.5)) && (tieneIndRightU) && (prevHomeGoals <= 2)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML8 Val:66%(3) Train:84%(25) h2h_0goles+il_covered_current+ir_dominate_da...';
    ruleUsed = 'ML8';
}

else if (!pick && (datosCompletos) && ((parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0)) > 0) && (momentumLocal) && (Math.abs(ou_value - 2.5) < 0.3) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML9 Val:66%(3) Train:76%(105) datos_completos+il_was_dog+momentum_local...';
    ruleUsed = 'ML9';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (visitaPerdioContraRivalLocal) && (parseInt(prevAwayScore[0] || 0) > 0 && parseInt(prevAwayScore[1] || 0) > 0) && (phDA >= 50)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML10 Val:66%(3) Train:71%(90) h2h_ah_eq+h_top5+ir_visita_perdio...';
    ruleUsed = 'ML10';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (tienePrevAway)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML11 Val:66%(3) Train:70%(194) h2h_ah_eq+h_top5+il_ambos...';
    ruleUsed = 'ML11';
}

else if (!pick && (handicapRepetidoCerca) && (indLeftGoals >= 3 && indRightGoals >= 3) && (phDA > paDA) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML12 Val:66%(3) Train:77%(72) ha_repetido_cerca+ind_ambas_over+ph_better_da...';
    ruleUsed = 'ML12';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (localGoleoRivalVisita) && (momentumLocal) && (phSOT > paSOT) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML13 Val:66%(3) Train:80%(51) h2h_ah_eq+il_local_goleo+momentum_local...';
    ruleUsed = 'ML13';
}

else if (!pick && (datosFrescos) && (homeRank > 0 && homeRank <= 10) && (localGoalsInd > rivalGoalsIndL) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (paEncajo)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML14 Val:60%(5) Train:71%(139) datos_frescos+h_top10+il_local_gano...';
    ruleUsed = 'ML14';
}

else if (!pick && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (paEncajo) && (tienePrevAway) && (phSOT > paSOT)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML15 Val:60%(5) Train:75%(58) ind_ambas_over+ind_perf_home_good+pa_encajo...';
    ruleUsed = 'ML15';
}

else if (!pick && (h2hCol3GoalsCalc <= 2) && ((parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0)) >= 1.0) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (indRightGoals >= 3) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML16 Val:60%(5) Train:74%(35) h2h_under+ir_away_was_strong_fav+ir_failed_current...';
    ruleUsed = 'ML16';
}

Total patrones AH raw: 738
Patrones VALIDADOS y ÚNICOS: 16
else if (!pick && (datosFrescos) && (h2hCol3PorCero) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (phGoleo) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 75;
    confidence = 'high';
    reason = '🎯 ML1 Val:75%(4) Train:76%(39) datos_frescos+h2h_por_cero+ir_failed_current...';
    ruleUsed = 'ML1';
}

else if (!pick && (datosCompletos) && (localGoalsInd > rivalGoalsIndL) && (paEncajo) && (phEncajo) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML2 Val:66%(3) Train:71%(35) datos_completos+il_local_gano+pa_encajo...';
    ruleUsed = 'ML2';
}

else if (!pick && (h2hCol3PorCero) && (tieneH2HCol3) && (indLeftGoals <= 2) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (tienePrevAway) && (phSOT >= 5)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML3 Val:66%(3) Train:72%(93) h2h_por_cero+h2h_tiene+il_under...';
    ruleUsed = 'ML3';
}

else if (!pick && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (visitaPerdioContraRivalLocal) && (paEncajo) && (parseInt(prevHomeScore[0] || 0) > 0 && parseInt(prevHomeScore[1] || 0) > 0) && ((phDA >= 50) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]))) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML4 Val:66%(3) Train:75%(73) ind_perf_home_good+ir_visita_perdio+pa_encajo...';
    ruleUsed = 'ML4';
}

else if (!pick && (awayRank > 0 && awayRank <= 10) && (h2hCol3GoalsCalc === 0) && (h2hCol3PorCero) && (h2hCol3GoalsCalc <= 2) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (phDA > paDA) && (phSOT > paSOT) && (phEncajo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML5 Val:66%(3) Train:75%(28) a_top10+h2h_0goles+h2h_por_cero...';
    ruleUsed = 'ML5';
}

else if (!pick && (h2hCol3PorCero) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (tieneIndLeftU) && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML6 Val:66%(3) Train:72%(25) h2h_por_cero+il_ambos+il_tiene...';
    ruleUsed = 'ML6';
}

else if (!pick && (awayRank >= 15) && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (((parseInt((pc.h2h_col3 || {}).goles_home || 0) + parseFloat((pc.h2h_col3 || {}).ah || 0)) > parseInt((pc.h2h_col3 || {}).goles_away || 0))) && (paDA <= 30) && (prevAwayGoals >= 4)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML7 Val:66%(3) Train:77%(58) a_bottom+h2h_ah_eq+h2h_covered...';
    ruleUsed = 'ML7';
}

else if (!pick && (h2hCol3GoalsCalc === 0) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (irAwayDA > (irRivalDA * 1.5)) && (tieneIndRightU) && (prevHomeGoals <= 2)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML8 Val:66%(3) Train:84%(25) h2h_0goles+il_covered_current+ir_dominate_da...';
    ruleUsed = 'ML8';
}

else if (!pick && (datosCompletos) && ((parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0)) > 0) && (momentumLocal) && (Math.abs(ou_value - 2.5) < 0.3) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML9 Val:66%(3) Train:76%(105) datos_completos+il_was_dog+momentum_local...';
    ruleUsed = 'ML9';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (visitaPerdioContraRivalLocal) && (parseInt(prevAwayScore[0] || 0) > 0 && parseInt(prevAwayScore[1] || 0) > 0) && (phDA >= 50)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML10 Val:66%(3) Train:71%(90) h2h_ah_eq+h_top5+ir_visita_perdio...';
    ruleUsed = 'ML10';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (tienePrevAway)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML11 Val:66%(3) Train:70%(194) h2h_ah_eq+h_top5+il_ambos...';
    ruleUsed = 'ML11';
}

else if (!pick && (handicapRepetidoCerca) && (indLeftGoals >= 3 && indRightGoals >= 3) && (phDA > paDA) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML12 Val:66%(3) Train:77%(72) ha_repetido_cerca+ind_ambas_over+ph_better_da...';
    ruleUsed = 'ML12';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (localGoleoRivalVisita) && (momentumLocal) && (phSOT > paSOT) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML13 Val:66%(3) Train:80%(51) h2h_ah_eq+il_local_goleo+momentum_local...';
    ruleUsed = 'ML13';
}

else if (!pick && (datosFrescos) && (homeRank > 0 && homeRank <= 10) && (localGoalsInd > rivalGoalsIndL) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (paEncajo)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML14 Val:60%(5) Train:71%(139) datos_frescos+h_top10+il_local_gano...';
    ruleUsed = 'ML14';
}

else if (!pick && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (paEncajo) && (tienePrevAway) && (phSOT > paSOT)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML15 Val:60%(5) Train:75%(58) ind_ambas_over+ind_perf_home_good+pa_encajo...';
    ruleUsed = 'ML15';
}

else if (!pick && (h2hCol3GoalsCalc <= 2) && ((parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0)) >= 1.0) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (indRightGoals >= 3) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML16 Val:60%(5) Train:74%(35) h2h_under+ir_away_was_strong_fav+ir_failed_current...';
    ruleUsed = 'ML16';
}

Total patrones AH raw: 738
Patrones VALIDADOS y ÚNICOS: 16
else if (!pick && (datosFrescos) && (h2hCol3PorCero) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (phGoleo) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 75;
    confidence = 'high';
    reason = '🎯 ML1 Val:75%(4) Train:76%(39) datos_frescos+h2h_por_cero+ir_failed_current...';
    ruleUsed = 'ML1';
}

else if (!pick && (datosCompletos) && (localGoalsInd > rivalGoalsIndL) && (paEncajo) && (phEncajo) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML2 Val:66%(3) Train:71%(35) datos_completos+il_local_gano+pa_encajo...';
    ruleUsed = 'ML2';
}

else if (!pick && (h2hCol3PorCero) && (tieneH2HCol3) && (indLeftGoals <= 2) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (tienePrevAway) && (phSOT >= 5)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML3 Val:66%(3) Train:72%(93) h2h_por_cero+h2h_tiene+il_under...';
    ruleUsed = 'ML3';
}

else if (!pick && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (visitaPerdioContraRivalLocal) && (paEncajo) && (parseInt(prevHomeScore[0] || 0) > 0 && parseInt(prevHomeScore[1] || 0) > 0) && ((phDA >= 50) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]))) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML4 Val:66%(3) Train:75%(73) ind_perf_home_good+ir_visita_perdio+pa_encajo...';
    ruleUsed = 'ML4';
}

else if (!pick && (awayRank > 0 && awayRank <= 10) && (h2hCol3GoalsCalc === 0) && (h2hCol3PorCero) && (h2hCol3GoalsCalc <= 2) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (phDA > paDA) && (phSOT > paSOT) && (phEncajo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML5 Val:66%(3) Train:75%(28) a_top10+h2h_0goles+h2h_por_cero...';
    ruleUsed = 'ML5';
}

else if (!pick && (h2hCol3PorCero) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (tieneIndLeftU) && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML6 Val:66%(3) Train:72%(25) h2h_por_cero+il_ambos+il_tiene...';
    ruleUsed = 'ML6';
}

else if (!pick && (awayRank >= 15) && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (((parseInt((pc.h2h_col3 || {}).goles_home || 0) + parseFloat((pc.h2h_col3 || {}).ah || 0)) > parseInt((pc.h2h_col3 || {}).goles_away || 0))) && (paDA <= 30) && (prevAwayGoals >= 4)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML7 Val:66%(3) Train:77%(58) a_bottom+h2h_ah_eq+h2h_covered...';
    ruleUsed = 'ML7';
}

else if (!pick && (h2hCol3GoalsCalc === 0) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (irAwayDA > (irRivalDA * 1.5)) && (tieneIndRightU) && (prevHomeGoals <= 2)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML8 Val:66%(3) Train:84%(25) h2h_0goles+il_covered_current+ir_dominate_da...';
    ruleUsed = 'ML8';
}

else if (!pick && (datosCompletos) && ((parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0)) > 0) && (momentumLocal) && (Math.abs(ou_value - 2.5) < 0.3) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML9 Val:66%(3) Train:76%(105) datos_completos+il_was_dog+momentum_local...';
    ruleUsed = 'ML9';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (visitaPerdioContraRivalLocal) && (parseInt(prevAwayScore[0] || 0) > 0 && parseInt(prevAwayScore[1] || 0) > 0) && (phDA >= 50)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML10 Val:66%(3) Train:71%(90) h2h_ah_eq+h_top5+ir_visita_perdio...';
    ruleUsed = 'ML10';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (tienePrevAway)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML11 Val:66%(3) Train:70%(194) h2h_ah_eq+h_top5+il_ambos...';
    ruleUsed = 'ML11';
}

else if (!pick && (handicapRepetidoCerca) && (indLeftGoals >= 3 && indRightGoals >= 3) && (phDA > paDA) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML12 Val:66%(3) Train:77%(72) ha_repetido_cerca+ind_ambas_over+ph_better_da...';
    ruleUsed = 'ML12';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (localGoleoRivalVisita) && (momentumLocal) && (phSOT > paSOT) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML13 Val:66%(3) Train:80%(51) h2h_ah_eq+il_local_goleo+momentum_local...';
    ruleUsed = 'ML13';
}

else if (!pick && (datosFrescos) && (homeRank > 0 && homeRank <= 10) && (localGoalsInd > rivalGoalsIndL) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (paEncajo)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML14 Val:60%(5) Train:71%(139) datos_frescos+h_top10+il_local_gano...';
    ruleUsed = 'ML14';
}

else if (!pick && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (paEncajo) && (tienePrevAway) && (phSOT > paSOT)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML15 Val:60%(5) Train:75%(58) ind_ambas_over+ind_perf_home_good+pa_encajo...';
    ruleUsed = 'ML15';
}

else if (!pick && (h2hCol3GoalsCalc <= 2) && ((parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0)) >= 1.0) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (indRightGoals >= 3) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML16 Val:60%(5) Train:74%(35) h2h_under+ir_away_was_strong_fav+ir_failed_current...';
    ruleUsed = 'ML16';
}

Total patrones AH raw: 738
Patrones VALIDADOS y ÚNICOS: 16
else if (!pick && (datosFrescos) && (h2hCol3PorCero) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (phGoleo) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 75;
    confidence = 'high';
    reason = '🎯 ML1 Val:75%(4) Train:76%(39) datos_frescos+h2h_por_cero+ir_failed_current...';
    ruleUsed = 'ML1';
}

else if (!pick && (datosCompletos) && (localGoalsInd > rivalGoalsIndL) && (paEncajo) && (phEncajo) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML2 Val:66%(3) Train:71%(35) datos_completos+il_local_gano+pa_encajo...';
    ruleUsed = 'ML2';
}

else if (!pick && (h2hCol3PorCero) && (tieneH2HCol3) && (indLeftGoals <= 2) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (tienePrevAway) && (phSOT >= 5)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML3 Val:66%(3) Train:72%(93) h2h_por_cero+h2h_tiene+il_under...';
    ruleUsed = 'ML3';
}

else if (!pick && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (visitaPerdioContraRivalLocal) && (paEncajo) && (parseInt(prevHomeScore[0] || 0) > 0 && parseInt(prevHomeScore[1] || 0) > 0) && ((phDA >= 50) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]))) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML4 Val:66%(3) Train:75%(73) ind_perf_home_good+ir_visita_perdio+pa_encajo...';
    ruleUsed = 'ML4';
}

else if (!pick && (awayRank > 0 && awayRank <= 10) && (h2hCol3GoalsCalc === 0) && (h2hCol3PorCero) && (h2hCol3GoalsCalc <= 2) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (phDA > paDA) && (phSOT > paSOT) && (phEncajo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML5 Val:66%(3) Train:75%(28) a_top10+h2h_0goles+h2h_por_cero...';
    ruleUsed = 'ML5';
}

else if (!pick && (h2hCol3PorCero) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (tieneIndLeftU) && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML6 Val:66%(3) Train:72%(25) h2h_por_cero+il_ambos+il_tiene...';
    ruleUsed = 'ML6';
}

else if (!pick && (awayRank >= 15) && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (((parseInt((pc.h2h_col3 || {}).goles_home || 0) + parseFloat((pc.h2h_col3 || {}).ah || 0)) > parseInt((pc.h2h_col3 || {}).goles_away || 0))) && (paDA <= 30) && (prevAwayGoals >= 4)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML7 Val:66%(3) Train:77%(58) a_bottom+h2h_ah_eq+h2h_covered...';
    ruleUsed = 'ML7';
}

else if (!pick && (h2hCol3GoalsCalc === 0) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (irAwayDA > (irRivalDA * 1.5)) && (tieneIndRightU) && (prevHomeGoals <= 2)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML8 Val:66%(3) Train:84%(25) h2h_0goles+il_covered_current+ir_dominate_da...';
    ruleUsed = 'ML8';
}

else if (!pick && (datosCompletos) && ((parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0)) > 0) && (momentumLocal) && (Math.abs(ou_value - 2.5) < 0.3) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML9 Val:66%(3) Train:76%(105) datos_completos+il_was_dog+momentum_local...';
    ruleUsed = 'ML9';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (visitaPerdioContraRivalLocal) && (parseInt(prevAwayScore[0] || 0) > 0 && parseInt(prevAwayScore[1] || 0) > 0) && (phDA >= 50)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML10 Val:66%(3) Train:71%(90) h2h_ah_eq+h_top5+ir_visita_perdio...';
    ruleUsed = 'ML10';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (tienePrevAway)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML11 Val:66%(3) Train:70%(194) h2h_ah_eq+h_top5+il_ambos...';
    ruleUsed = 'ML11';
}

else if (!pick && (handicapRepetidoCerca) && (indLeftGoals >= 3 && indRightGoals >= 3) && (phDA > paDA) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML12 Val:66%(3) Train:77%(72) ha_repetido_cerca+ind_ambas_over+ph_better_da...';
    ruleUsed = 'ML12';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (localGoleoRivalVisita) && (momentumLocal) && (phSOT > paSOT) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML13 Val:66%(3) Train:80%(51) h2h_ah_eq+il_local_goleo+momentum_local...';
    ruleUsed = 'ML13';
}

else if (!pick && (datosFrescos) && (homeRank > 0 && homeRank <= 10) && (localGoalsInd > rivalGoalsIndL) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (paEncajo)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML14 Val:60%(5) Train:71%(139) datos_frescos+h_top10+il_local_gano...';
    ruleUsed = 'ML14';
}

else if (!pick && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (paEncajo) && (tienePrevAway) && (phSOT > paSOT)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML15 Val:60%(5) Train:75%(58) ind_ambas_over+ind_perf_home_good+pa_encajo...';
    ruleUsed = 'ML15';
}

else if (!pick && (h2hCol3GoalsCalc <= 2) && ((parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0)) >= 1.0) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (indRightGoals >= 3) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML16 Val:60%(5) Train:74%(35) h2h_under+ir_away_was_strong_fav+ir_failed_current...';
    ruleUsed = 'ML16';
}

Total patrones AH raw: 738
Patrones VALIDADOS y ÚNICOS: 16
else if (!pick && (datosFrescos) && (h2hCol3PorCero) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (phGoleo) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 75;
    confidence = 'high';
    reason = '🎯 ML1 Val:75%(4) Train:76%(39) datos_frescos+h2h_por_cero+ir_failed_current...';
    ruleUsed = 'ML1';
}

else if (!pick && (datosCompletos) && (localGoalsInd > rivalGoalsIndL) && (paEncajo) && (phEncajo) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML2 Val:66%(3) Train:71%(35) datos_completos+il_local_gano+pa_encajo...';
    ruleUsed = 'ML2';
}

else if (!pick && (h2hCol3PorCero) && (tieneH2HCol3) && (indLeftGoals <= 2) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (tienePrevAway) && (phSOT >= 5)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML3 Val:66%(3) Train:72%(93) h2h_por_cero+h2h_tiene+il_under...';
    ruleUsed = 'ML3';
}

else if (!pick && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (visitaPerdioContraRivalLocal) && (paEncajo) && (parseInt(prevHomeScore[0] || 0) > 0 && parseInt(prevHomeScore[1] || 0) > 0) && ((phDA >= 50) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]))) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML4 Val:66%(3) Train:75%(73) ind_perf_home_good+ir_visita_perdio+pa_encajo...';
    ruleUsed = 'ML4';
}

else if (!pick && (awayRank > 0 && awayRank <= 10) && (h2hCol3GoalsCalc === 0) && (h2hCol3PorCero) && (h2hCol3GoalsCalc <= 2) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (phDA > paDA) && (phSOT > paSOT) && (phEncajo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML5 Val:66%(3) Train:75%(28) a_top10+h2h_0goles+h2h_por_cero...';
    ruleUsed = 'ML5';
}

else if (!pick && (h2hCol3PorCero) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (tieneIndLeftU) && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML6 Val:66%(3) Train:72%(25) h2h_por_cero+il_ambos+il_tiene...';
    ruleUsed = 'ML6';
}

else if (!pick && (awayRank >= 15) && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (((parseInt((pc.h2h_col3 || {}).goles_home || 0) + parseFloat((pc.h2h_col3 || {}).ah || 0)) > parseInt((pc.h2h_col3 || {}).goles_away || 0))) && (paDA <= 30) && (prevAwayGoals >= 4)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML7 Val:66%(3) Train:77%(58) a_bottom+h2h_ah_eq+h2h_covered...';
    ruleUsed = 'ML7';
}

else if (!pick && (h2hCol3GoalsCalc === 0) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (irAwayDA > (irRivalDA * 1.5)) && (tieneIndRightU) && (prevHomeGoals <= 2)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML8 Val:66%(3) Train:84%(25) h2h_0goles+il_covered_current+ir_dominate_da...';
    ruleUsed = 'ML8';
}

else if (!pick && (datosCompletos) && ((parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0)) > 0) && (momentumLocal) && (Math.abs(ou_value - 2.5) < 0.3) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML9 Val:66%(3) Train:76%(105) datos_completos+il_was_dog+momentum_local...';
    ruleUsed = 'ML9';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (visitaPerdioContraRivalLocal) && (parseInt(prevAwayScore[0] || 0) > 0 && parseInt(prevAwayScore[1] || 0) > 0) && (phDA >= 50)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML10 Val:66%(3) Train:71%(90) h2h_ah_eq+h_top5+ir_visita_perdio...';
    ruleUsed = 'ML10';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (tienePrevAway)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML11 Val:66%(3) Train:70%(194) h2h_ah_eq+h_top5+il_ambos...';
    ruleUsed = 'ML11';
}

else if (!pick && (handicapRepetidoCerca) && (indLeftGoals >= 3 && indRightGoals >= 3) && (phDA > paDA) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML12 Val:66%(3) Train:77%(72) ha_repetido_cerca+ind_ambas_over+ph_better_da...';
    ruleUsed = 'ML12';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (localGoleoRivalVisita) && (momentumLocal) && (phSOT > paSOT) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML13 Val:66%(3) Train:80%(51) h2h_ah_eq+il_local_goleo+momentum_local...';
    ruleUsed = 'ML13';
}

else if (!pick && (datosFrescos) && (homeRank > 0 && homeRank <= 10) && (localGoalsInd > rivalGoalsIndL) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (paEncajo)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML14 Val:60%(5) Train:71%(139) datos_frescos+h_top10+il_local_gano...';
    ruleUsed = 'ML14';
}

else if (!pick && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (paEncajo) && (tienePrevAway) && (phSOT > paSOT)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML15 Val:60%(5) Train:75%(58) ind_ambas_over+ind_perf_home_good+pa_encajo...';
    ruleUsed = 'ML15';
}

else if (!pick && (h2hCol3GoalsCalc <= 2) && ((parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0)) >= 1.0) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (indRightGoals >= 3) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML16 Val:60%(5) Train:74%(35) h2h_under+ir_away_was_strong_fav+ir_failed_current...';
    ruleUsed = 'ML16';
}

Total patrones AH raw: 738
Patrones VALIDADOS y ÚNICOS: 16
else if (!pick && (datosFrescos) && (h2hCol3PorCero) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (phGoleo) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 75;
    confidence = 'high';
    reason = '🎯 ML1 Val:75%(4) Train:76%(39) datos_frescos+h2h_por_cero+ir_failed_current...';
    ruleUsed = 'ML1';
}

else if (!pick && (datosCompletos) && (localGoalsInd > rivalGoalsIndL) && (paEncajo) && (phEncajo) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML2 Val:66%(3) Train:71%(35) datos_completos+il_local_gano+pa_encajo...';
    ruleUsed = 'ML2';
}

else if (!pick && (h2hCol3PorCero) && (tieneH2HCol3) && (indLeftGoals <= 2) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (tienePrevAway) && (phSOT >= 5)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML3 Val:66%(3) Train:72%(93) h2h_por_cero+h2h_tiene+il_under...';
    ruleUsed = 'ML3';
}

else if (!pick && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (visitaPerdioContraRivalLocal) && (paEncajo) && (parseInt(prevHomeScore[0] || 0) > 0 && parseInt(prevHomeScore[1] || 0) > 0) && ((phDA >= 50) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]))) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML4 Val:66%(3) Train:75%(73) ind_perf_home_good+ir_visita_perdio+pa_encajo...';
    ruleUsed = 'ML4';
}

else if (!pick && (awayRank > 0 && awayRank <= 10) && (h2hCol3GoalsCalc === 0) && (h2hCol3PorCero) && (h2hCol3GoalsCalc <= 2) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (phDA > paDA) && (phSOT > paSOT) && (phEncajo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML5 Val:66%(3) Train:75%(28) a_top10+h2h_0goles+h2h_por_cero...';
    ruleUsed = 'ML5';
}

else if (!pick && (h2hCol3PorCero) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (tieneIndLeftU) && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML6 Val:66%(3) Train:72%(25) h2h_por_cero+il_ambos+il_tiene...';
    ruleUsed = 'ML6';
}

else if (!pick && (awayRank >= 15) && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (((parseInt((pc.h2h_col3 || {}).goles_home || 0) + parseFloat((pc.h2h_col3 || {}).ah || 0)) > parseInt((pc.h2h_col3 || {}).goles_away || 0))) && (paDA <= 30) && (prevAwayGoals >= 4)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML7 Val:66%(3) Train:77%(58) a_bottom+h2h_ah_eq+h2h_covered...';
    ruleUsed = 'ML7';
}

else if (!pick && (h2hCol3GoalsCalc === 0) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1]) && (irAwayDA > (irRivalDA * 1.5)) && (tieneIndRightU) && (prevHomeGoals <= 2)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML8 Val:66%(3) Train:84%(25) h2h_0goles+il_covered_current+ir_dominate_da...';
    ruleUsed = 'ML8';
}

else if (!pick && (datosCompletos) && ((parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0)) > 0) && (momentumLocal) && (Math.abs(ou_value - 2.5) < 0.3) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML9 Val:66%(3) Train:76%(105) datos_completos+il_was_dog+momentum_local...';
    ruleUsed = 'ML9';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (visitaPerdioContraRivalLocal) && (parseInt(prevAwayScore[0] || 0) > 0 && parseInt(prevAwayScore[1] || 0) > 0) && (phDA >= 50)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML10 Val:66%(3) Train:71%(90) h2h_ah_eq+h_top5+ir_visita_perdio...';
    ruleUsed = 'ML10';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (homeRank > 0 && homeRank <= 5) && (parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (tienePrevAway)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML11 Val:66%(3) Train:70%(194) h2h_ah_eq+h_top5+il_ambos...';
    ruleUsed = 'ML11';
}

else if (!pick && (handicapRepetidoCerca) && (indLeftGoals >= 3 && indRightGoals >= 3) && (phDA > paDA) && (phSOT >= 5) && (phPorCero)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML12 Val:66%(3) Train:77%(72) ha_repetido_cerca+ind_ambas_over+ph_better_da...';
    ruleUsed = 'ML12';
}

else if (!pick && (Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)) && (localGoleoRivalVisita) && (momentumLocal) && (phSOT > paSOT) && (phGoleo)) {
    pick = 'LOCAL';
    probability = 66;
    confidence = 'high';
    reason = '🎯 ML13 Val:66%(3) Train:80%(51) h2h_ah_eq+il_local_goleo+momentum_local...';
    ruleUsed = 'ML13';
}

else if (!pick && (datosFrescos) && (homeRank > 0 && homeRank <= 10) && (localGoalsInd > rivalGoalsIndL) && (parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0) && (paEncajo)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML14 Val:60%(5) Train:71%(139) datos_frescos+h_top10+il_local_gano...';
    ruleUsed = 'ML14';
}

else if (!pick && (indLeftGoals >= 3 && indRightGoals >= 3) && ((((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))) && (paEncajo) && (tienePrevAway) && (phSOT > paSOT)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML15 Val:60%(5) Train:75%(58) ind_ambas_over+ind_perf_home_good+pa_encajo...';
    ruleUsed = 'ML15';
}

else if (!pick && (h2hCol3GoalsCalc <= 2) && ((parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0)) >= 1.0) && ((indRightScoreU[0] + ah_value) > indRightScoreU[1]) && (indRightGoals >= 3) && (prevHomeGoals >= 3)) {
    pick = 'LOCAL';
    probability = 60;
    confidence = 'high';
    reason = '🎯 ML16 Val:60%(5) Train:74%(35) h2h_under+ir_away_was_strong_fav+ir_failed_current...';
    ruleUsed = 'ML16';
}


// ==================== PATRONES O/U APRENDIDOS (34 patrones) ====================
// === FIN PATRONES ML ===
