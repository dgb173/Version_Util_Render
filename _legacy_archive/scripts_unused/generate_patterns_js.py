import json
import sys
import io

# Forzar UTF-8 en la salida para evitar errores con emojis en Windows
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Redirigir salida a archivo patterns.js manteniendo consola
class Tee(object):
    def __init__(self, name, mode):
        self.file = open(name, mode, encoding='utf-8')
        self.stdout = sys.stdout
    def write(self, data):
        self.file.write(data)
        self.stdout.write(data)
    def flush(self):
        self.file.flush()
        self.stdout.flush()

sys.stdout = Tee('patterns.js', 'w')

# Cargar patrones
with open('scripts/mega_patterns.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Mapeo de features a código JavaScript
FEATURE_MAP = {
    # AH y contexto
    'ah_0': 'Math.abs(ah_value) < 0.01',
    'ah_05': 'Math.abs(Math.abs(ah_value) - 0.5) < 0.1',
    'ah_1': 'Math.abs(Math.abs(ah_value) - 1) < 0.1',
    
    # Rankings
    'h_top3': 'homeRank > 0 && homeRank <= 3',
    'h_top5': 'homeRank > 0 && homeRank <= 5',
    'h_top10': 'homeRank > 0 && homeRank <= 10',
    'h_medio': 'homeRank > 5 && homeRank <= 12',
    'h_bottom': 'homeRank >= 15',
    'a_top3': 'awayRank > 0 && awayRank <= 3',
    'a_top5': 'awayRank > 0 && awayRank <= 5',
    'a_top10': 'awayRank > 0 && awayRank <= 10',
    'a_medio': 'awayRank > 5 && awayRank <= 12',
    'a_bottom': 'awayRank >= 15',
    'h_mejor_rank': 'homeRank > 0 && awayRank > 0 && homeRank < awayRank',
    'a_mejor_rank': 'homeRank > 0 && awayRank > 0 && awayRank < homeRank',
    'gran_diff_rank': 'Math.abs(rankDiff) >= 5',
    'ranks_parejos': 'Math.abs(rankDiff) <= 3',
    
    # Datos
    'datos_frescos': 'datosFrescos',
    'datos_completos': 'datosCompletos',
    
    # Prev Home
    'ph_gano': 'localGanoPrev',
    'ph_perdio': 'localPerdioPrev',
    'ph_empato': 'phEmpato',
    'ph_goleo': 'phGoleo',
    'ph_marco3': 'prevHomeGoals >= 3',
    'ph_mucho_gol': 'prevHomeGoals >= 4',
    'ph_under': 'prevHomeGoals <= 2',
    'ph_por_cero': 'phPorCero',
    'ph_0goles': 'prevHomeGoals === 0',
    'ph_encajo': 'phEncajo',
    'ph_ambos': 'parseInt(prevHomeScore[0] || 0) > 0 && parseInt(prevHomeScore[1] || 0) > 0',
    'ph_tiene': 'tienePrevHome',
    
    # Prev Away
    'pa_gano': 'visitaGanoPrev',
    'pa_perdio': 'visitaPerdioPrev',
    'pa_empato': 'paEmpato',
    'pa_goleo': 'paGoleo',
    'pa_marco3': 'prevAwayGoals >= 3',
    'pa_mucho_gol': 'prevAwayGoals >= 4',
    'pa_under': 'prevAwayGoals <= 2',
    'pa_por_cero': 'paPorCero',
    'pa_0goles': 'prevAwayGoals === 0',
    'pa_encajo': 'paEncajo',
    'pa_ambos': 'parseInt(prevAwayScore[0] || 0) > 0 && parseInt(prevAwayScore[1] || 0) > 0',
    'pa_tiene': 'tienePrevAway',
    
    # Momentum
    'momentum_local': 'momentumLocal',
    'momentum_visita': 'momentumVisita',
    
    # H2H Col3
    'h2h_tiene': 'tieneH2HCol3',
    'h2h_gano_local': 'h2hCol3GanoLocal',
    'h2h_gano_visita': 'h2hCol3GanoVisita',
    'h2h_empate': 'h2hCol3Empate',
    'h2h_goleo': 'h2hCol3Goleo',
    'h2h_mucho_gol': 'h2hCol3GoalsCalc >= 4',
    'h2h_under': 'h2hCol3GoalsCalc <= 2',
    'h2h_0goles': 'h2hCol3GoalsCalc === 0',
    'h2h_por_cero': 'h2hCol3PorCero',
    'h2h_ambos': 'h2hCol3Ambos',
    
    # HA repetido
    'ha_repetido_exacto': 'handicapRepetidoExacto',
    'ha_repetido_cerca': 'handicapRepetidoCerca',
    
    # Indirectas
    'il_tiene': 'tieneIndLeftU',
    'ir_tiene': 'tieneIndRightU',
    'il_local_gano': 'localGoalsInd > rivalGoalsIndL',
    'il_local_goleo': 'localGoleoRivalVisita',
    'il_local_perdio': 'localGoalsInd < rivalGoalsIndL',
    'ir_visita_gano': 'visitaGoalsInd > rivalGoalsIndR',
    'ir_visita_goleo': 'visitaGoalsInd >= 3',
    'ir_visita_perdio': 'visitaPerdioContraRivalLocal',
    'ind_alineadas_local': 'indAlineadasLocal',
    'ind_alineadas_visita': 'indAlineadasVisita',
    'il_over': 'indLeftGoals >= 3',
    'il_under': 'indLeftGoals <= 2',
    'ir_over': 'indRightGoals >= 3',
    'ir_under': 'indRightGoals <= 2',
    'il_ambos': 'parseInt(indLeftScoreU[0] || 0) > 0 && parseInt(indLeftScoreU[1] || 0) > 0',
    'ir_ambos': 'parseInt(indRightScoreU[0] || 0) > 0 && parseInt(indRightScoreU[1] || 0) > 0',
    'ind_ambas_over': 'indLeftGoals >= 3 && indRightGoals >= 3',
    'ind_ambas_under': 'indLeftGoals <= 2 && indRightGoals <= 2',
    
    # Combinados ambos equipos
    'ambos_golearon': 'phGoleo && paGoleo',
    'ambos_under': 'prevHomeLow && prevAwayLow',
    
    # O/U línea
    'ou_normal': 'Math.abs(ou_value - 2.5) < 0.3',
    'ou_low': 'ou_value <= 2',
    'ou_high': 'ou_value >= 3',
    
    # NEW STATS (SOT, DA)
    'ph_high_sot': 'phSOT >= 5',
    'ph_low_sot': 'phSOT <= 2',
    'ph_high_da': 'phDA >= 50',
    'ph_low_da': 'phDA <= 30',
    'pa_high_sot': 'paSOT >= 5',
    'pa_low_sot': 'paSOT <= 2',
    'pa_high_da': 'paDA >= 50',
    'pa_low_da': 'paDA <= 30',
    'ph_better_sot': 'phSOT > paSOT',
    'ph_better_da': 'phDA > paDA',
    
    # MOVEMENT
    'mov_stadium_up': "movStadium === 'UP'",
    'mov_stadium_down': "movStadium === 'DOWN'",
    'mov_general_up': "movGeneral === 'UP'",
    'mov_general_down': "movGeneral === 'DOWN'",
    
    # UNDERDOGS
    'home_underdog': 'ah_value > 0',
    'away_underdog': 'ah_value < 0',
    
    # ADVANCED INTERACTIONS
    'il_ah_higher': 'indLeftAH > ah_value',
    'il_ah_lower': 'indLeftAH < ah_value',
    'ir_ah_higher': 'indRightAH > ah_value',
    'ir_ah_lower': 'indRightAH < ah_value',
    
    'il_covered_current': '(indLeftScoreU[0] + ah_value) > indLeftScoreU[1]',
    'il_failed_current': '(indLeftScoreU[0] + ah_value) < indLeftScoreU[1]',
    'ir_covered_current': '(indRightScoreU[0] + ah_value) < indRightScoreU[1]',
    'ir_failed_current': '(indRightScoreU[0] + ah_value) > indRightScoreU[1]',
    
    'ph_da_high_il_covered': '(phDA >= 50) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1])',
    'ph_sot_high_il_covered': '(phSOT >= 5) && ((indLeftScoreU[0] + ah_value) > indLeftScoreU[1])',
    
    'pa_da_high_ir_covered': '(paDA >= 50) && ((indRightScoreU[0] + ah_value) < indRightScoreU[1])',
    'pa_sot_high_ir_covered': '(paSOT >= 5) && ((indRightScoreU[0] + ah_value) < indRightScoreU[1])',
    
    # PREV MATCH CONTEXT
    'ph_was_fav': '((pc.last_home_match?.home_team === home) ? (parseFloat(pc.last_home_match?.ah || 0) < 0) : (parseFloat(pc.last_home_match?.ah || 0) > 0))',
    'ph_was_dog': '((pc.last_home_match?.home_team === home) ? (parseFloat(pc.last_home_match?.ah || 0) > 0) : (parseFloat(pc.last_home_match?.ah || 0) < 0))',
    
    'pa_was_fav': '((pc.last_away_match?.home_team === away) ? (parseFloat(pc.last_away_match?.ah || 0) < 0) : (parseFloat(pc.last_away_match?.ah || 0) > 0))',
    'pa_was_dog': '((pc.last_away_match?.home_team === away) ? (parseFloat(pc.last_away_match?.ah || 0) > 0) : (parseFloat(pc.last_away_match?.ah || 0) < 0))',

    # NEW AH CONTEXT FEATURES
    'h2h_fav_local_strong': 'parseFloat((pc.h2h_col3 || {}).ah || 0) <= -1.0',
    'h2h_fav_local': 'parseFloat((pc.h2h_col3 || {}).ah || 0) < 0',
    'h2h_fav_visita': 'parseFloat((pc.h2h_col3 || {}).ah || 0) > 0',
    'h2h_fav_visita_strong': 'parseFloat((pc.h2h_col3 || {}).ah || 0) >= 1.0',
    'h2h_ah_eq': 'Math.abs(parseFloat((pc.h2h_col3 || {}).ah || 0)) === Math.abs(ah_value)',
    
    'il_was_fav': '(parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0)) < 0',
    'il_was_dog': '(parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0)) > 0',
    'il_fav_strong': '(parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0)) <= -1.0',
    
    'ir_was_fav': '(parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0)) < 0',
    'ir_was_dog': '(parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0)) > 0',
    'h2h_covered': '((parseInt((pc.h2h_col3 || {}).goles_home || 0) + parseFloat((pc.h2h_col3 || {}).ah || 0)) > parseInt((pc.h2h_col3 || {}).goles_away || 0))',
    'h2h_failed': '((parseInt((pc.h2h_col3 || {}).goles_home || 0) + parseFloat((pc.h2h_col3 || {}).ah || 0)) < parseInt((pc.h2h_col3 || {}).goles_away || 0))',
    
    'ind_perf_home_good': '(((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1]) && ((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1]))', # Local Covered AND Rival Covered (Away Failed)
    
    'ir_away_was_strong_fav': '(parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0)) >= 1.0',
    
    'il_covered_actual': '((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) > indLeftScoreU[1])',
    'il_failed_actual': '((indLeftScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).left?.ah_line || 0))) < indLeftScoreU[1])',
    
    'ir_covered_actual': '((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) < indRightScoreU[1])',
    'ir_failed_actual': '((indRightScoreU[0] + (parseFloat((pc.comparativas_indirectas || {}).right?.ah_line || 0))) > indRightScoreU[1])',

    # COMPARATIVE STATS
    'il_better_da': 'ilHomeDA > ilRivalDA',
    'il_better_sot': 'ilHomeSOT > ilRivalSOT',
    'il_dominate_da': 'ilHomeDA > (ilRivalDA * 1.5)',
    
    'ir_better_da': 'irAwayDA > irRivalDA',
    'ir_better_sot': 'irAwaySOT > irRivalSOT',
    'ir_dominate_da': 'irAwayDA > (irRivalDA * 1.5)',
    
    'h2h_better_da': 'h2hHomeDA > h2hAwayDA',
    'h2h_better_sot': 'h2hHomeSOT > h2hAwaySOT',
    'h2h_home_dominate': 'h2hHomeDA > (h2hAwayDA * 1.5)',
    'h2h_away_dominate': 'h2hAwayDA > (h2hHomeDA * 1.5)',
}

# Obtener top patrones AH con VALIDACIÓN
# Prioridad: test_accuracy >= 60% y test_samples >= 3
valid_ah = [p for p in data['ah_patterns'] 
            if p.get('test_accuracy', 0) >= 0.6 and p.get('test_samples', 0) >= 3]

# Si no hay suficientes validados, rellenar con los mejores de training pero marcados
if len(valid_ah) < 10:
    print("// ADVERTENCIA: Pocos patrones validados. Rellenando con training high-acc")
    extra = [p for p in data['ah_patterns'] 
             if p not in valid_ah and p['accuracy'] >= 0.85][:20]
    valid_ah.extend(extra)

top_ah = valid_ah[:200]

# Todos los O/U
top_ou = data['ou_patterns']

print(f"=== GENERANDO CÓDIGO JS PARA {len(top_ah)} PATRONES AH + {len(top_ou)} O/U ===\n")

# Generar variables adicionales necesarias
print("// ==================== VARIABLES ADICIONALES PARA PATRONES ML ====================")
print("// (Variables pre-calculadas en precacheo.html, se omiten aquí para evitar duplicados)")
print()
print("if (false) { }")
print()

# Generar patrones AH
print("// ==================== PATRONES AH APRENDIDOS (ORDENADOS POR VALIDACIÓN) ====================")
for i, p in enumerate(top_ah, 1):
    features_js = []
    for f in p['features']:
        if f in FEATURE_MAP:
            features_js.append(f"({FEATURE_MAP[f]})")
        else:
            print(f"// WARNING: Feature '{f}' no mapeada!")
            features_js.append(f"/* {f} */true")
    
    condition = " && ".join(features_js)
    
    train_acc = int(p['accuracy'] * 100)
    test_acc = int(p.get('test_accuracy', 0) * 100)
    test_n = p.get('test_samples', 0)
    
    # Emoji basado en validación
    # Filter and Sort
    # Priority: High Test Accuracy > High Test Samples > High Train Accuracy
    
    unique_patterns = []
    seen = set()
    
    # Validation Thresholds
    MIN_TEST_SAMPLES = 3
    MIN_TEST_ACCURACY = 0.60
    
    # Validation Thresholds
    MIN_TEST_SAMPLES = 3
    MIN_TEST_ACCURACY = 0.60
    
    # AH Patterns
    ah_patterns = data.get('ah_patterns', [])
    print(f"Total patrones AH raw: {len(ah_patterns)}")
    
    for p in ah_patterns:
        # Check Validation Success
        if p.get('test_samples', 0) < MIN_TEST_SAMPLES: continue
        if p.get('test_accuracy', 0) < MIN_TEST_ACCURACY: continue
        
        # Deduplicate by signature (features + target)
        key = f"{tuple(sorted(p['features']))}_{p['target']}"
        if key in seen: continue
        seen.add(key)
        unique_patterns.append(p)
    
    # Sort
    unique_patterns.sort(key=lambda x: (x.get('test_accuracy', 0), x.get('test_samples', 0)), reverse=True)
    
    print(f"Patrones VALIDADOS y ÚNICOS: {len(unique_patterns)}")
    
    # Generate JS
    for i, p in enumerate(unique_patterns):
        if i >= 100: break
        
        target = p['target']
        features = p['features']
        acc = p['accuracy']
        samples = p['samples']
        test_acc = p['test_accuracy']
        test_samp = p['test_samples']
        
        conds = []
        for f in features:
            if f in FEATURE_MAP: # FEATURE_MAP is upper case in file
                conds.append(f"({FEATURE_MAP[f]})")
            else:
                 pass
        
        if not conds: continue
        condition_str = " && ".join(conds)
        
        # Determine Pick config
        pick_val = target
        
        # Confidence based on test accuracy
        confidence_val = 'ultra' if test_acc >= 0.80 else 'high'
        
        # Probability is test_acc * 100
        probability_val = int(test_acc * 100)
        
        reason = f"🎯 ML{i+1} Val:{int(test_acc*100)}%({test_samp}) Train:{int(acc*100)}%({samples}) {'+'.join(features[:3])}..."
        rule_used = f"ML{i+1}"
        
        print(f"else if (!pick && {condition_str}) {{")
        print(f"    pick = '{pick_val}';")
        print(f"    probability = {probability_val};")
        print(f"    confidence = '{confidence_val}';")
        print(f"    reason = '{reason}';")
        print(f"    ruleUsed = '{rule_used}';")
        print("}")
        print()

print("\n// ==================== PATRONES O/U APRENDIDOS (34 patrones) ====================")
for i, p in enumerate(top_ou, 1):
    features_js = []
    for f in p['features']:
        if f in FEATURE_MAP:
            features_js.append(f"({FEATURE_MAP[f]})")
        else:
            print(f"// WARNING: Feature '{f}' no mapeada!")
            features_js.append(f"/* {f} */true")
    
    condition = " && ".join(features_js)
    acc_pct = int(p['accuracy'] * 100)
    emoji = "🎯🎯" if acc_pct >= 85 else "🎯"
    
    print(f"// OU_{i}: {'+'.join(p['features'])} ({acc_pct}%, {p['samples']} muestras)")
    print(f"else if (!pickOU && {condition}) {{")
    print(f"    pickOU = '{p['target']}';")
    print(f"    probOU = {acc_pct};")
    print(f"    reasonOU = '{emoji} OU{i}_{acc_pct}% {'+'.join(p['features'][:2])}';")
    print(f"    ruleUsedOU = 'OU{i}_{acc_pct}';")
    print("}")
    print()

print("// === FIN PATRONES ML ===")
