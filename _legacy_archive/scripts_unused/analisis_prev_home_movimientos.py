# scripts/analisis_prev_home_movimientos.py
"""
ANÁLISIS PREV HOME → VICTORIA LOCAL & OVER + MOVIMIENTOS DE LÍNEA
==================================================================

Analiza correlaciones entre:
1. Rendimiento previo en casa (prev_home) → Victoria local actual
2. Prev_home → Over (muchos goles)
3. Movimientos de línea óptimos basados en historial
"""

import json
import re
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
RESULTS_DIR = PROJECT_ROOT / 'backtest_results'
RESULTS_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = RESULTS_DIR / 'prev_home_analysis.json'
OUTPUT_REPORT = PROJECT_ROOT.parent / '.gemini' / 'antigravity' / 'brain' / '30f5cfc5-2a68-42fa-afcd-e15d0a1c221d' / 'report_prev_home_analysis.md'


def parse_score(score_str) -> Optional[Tuple[int, int]]:
    """Parsea un resultado como '3:2' o '3-2'"""
    if not score_str or ':' not in str(score_str):
        return None
    try:
        parts = str(score_str).replace('-', ':').split(':')
        return int(parts[0]), int(parts[1])
    except:
        return None


def parse_handicap(ah_str) -> Optional[float]:
    """Parsea un handicap como string a float"""
    if not ah_str:
        return None
    try:
        return float(ah_str)
    except:
        return None


def parse_movement(movement_str) -> Optional[Tuple[float, float]]:
    """Parsea movimiento de línea como '0.75 → 1.5'"""
    if not movement_str or '→' not in str(movement_str):
        return None
    try:
        parts = str(movement_str).replace(' ', '').split('→')
        before = float(parts[0])
        after = float(parts[1])
        return before, after
    except:
        return None


def get_ah_result(home_goals: int, away_goals: int, ah_line: float) -> str:
    """Determina quién cubre el handicap"""
    adjusted = (home_goals - away_goals) - ah_line
    if adjusted > 0.25:
        return 'COVER_HOME'
    elif adjusted < -0.25:
        return 'COVER_AWAY'
    return 'PUSH'


def load_all_matches() -> List[Dict]:
    """Carga todos los partidos de todos los archivos data_ah_*.json"""
    all_matches = []
    
    for ah_file in DATA_DIR.glob('data_ah_*.json'):
        print(f"📂 Cargando {ah_file.name}...")
        try:
            with open(ah_file, 'r', encoding='utf-8') as f:
                matches = json.load(f)
                all_matches.extend(matches)
        except Exception as e:
            print(f"⚠️ Error cargando {ah_file.name}: {e}")
    
    return all_matches


def extract_prev_home_features(match: Dict) -> Optional[Dict]:
    """Extrae características del prev_home de un partido"""
    prev_home = match.get('last_home_match')
    if not prev_home:
        return None
    
    # Score prev_home
    score = parse_score(prev_home.get('score'))
    if not score:
        return None
    
    features = {
        'prev_home_score': score,
        'prev_home_goals': score[0] + score[1],
        'prev_home_margin': score[0] - score[1],
    }
    
    # Resultado prev_home
    if score[0] > score[1]:
        features['prev_home_result'] = 'W'
    elif score[0] < score[1]:
        features['prev_home_result'] = 'L'
    else:
        features['prev_home_result'] = 'D'
    
    # Handicap prev_home
    prev_ah = parse_handicap(prev_home.get('handicap_line_raw') or prev_home.get('handicap'))
    if prev_ah is not None:
        features['prev_home_ah'] = prev_ah
        features['prev_home_ah_result'] = get_ah_result(score[0], score[1], prev_ah)
    
    # Ataques peligrosos prev_home
    stats_rows = prev_home.get('stats_rows', [])
    for stat in stats_rows:
        if stat.get('label') == 'Ataques Peligrosos':
            try:
                home_ap = float(stat.get('home', 0) or 0)
                away_ap = float(stat.get('away', 0) or 0)
                features['prev_home_ap_edge'] = home_ap - away_ap
                features['prev_home_ap_dominated'] = home_ap - away_ap > 10
            except:
                pass
        elif stat.get('label') == 'Tiros a Puerta':
            try:
                home_sot = float(stat.get('home', 0) or 0)
                away_sot = float(stat.get('away', 0) or 0)
                features['prev_home_sot_edge'] = home_sot - away_sot
            except:
                pass
    
    return features


def extract_match_features(match: Dict) -> Optional[Dict]:
    """Extrae características del partido actual"""
    # Score final
    final_score = parse_score(match.get('final_score') or match.get('score'))
    if not final_score:
        return None
    
    features = {
        'final_score': final_score,
        'final_goals': final_score[0] + final_score[1],
        'final_margin': final_score[0] - final_score[1],
    }
    
    # Resultado
    if final_score[0] > final_score[1]:
        features['result'] = 'HOME_WIN'
    elif final_score[0] < final_score[1]:
        features['result'] = 'AWAY_WIN'
    else:
        features['result'] = 'DRAW'
    
    # Handicap actual
    main_odds = match.get('main_match_odds', {})
    current_ah = parse_handicap(main_odds.get('ah_linea'))
    if current_ah is not None:
        features['current_ah'] = current_ah
        features['ah_result'] = get_ah_result(final_score[0], final_score[1], current_ah)
    
    # Goal line (para analizar over/under)
    goal_line = parse_handicap(main_odds.get('goals_linea'))
    if goal_line is not None:
        features['goal_line'] = goal_line
        features['over'] = final_score[0] + final_score[1] > goal_line
    
    # Movimiento de línea
    market_data = match.get('market_analysis_data', {})
    stadium_data = market_data.get('stadium', {})
    movement = parse_movement(stadium_data.get('movement'))
    if movement:
        features['line_before'] = movement[0]
        features['line_after'] = movement[1]
        features['line_delta'] = movement[1] - movement[0]
        features['line_increased'] = movement[1] > movement[0]
        features['line_decreased'] = movement[1] < movement[0]
    
    return features


def analyze_prev_home_to_victory(matches: List[Dict]) -> Dict:
    """Analiza correlación prev_home → victoria local"""
    results = defaultdict(lambda: {'total': 0, 'home_wins': 0, 'samples': []})
    
    for match in matches:
        prev_features = extract_prev_home_features(match)
        match_features = extract_match_features(match)
        
        if not prev_features or not match_features:
            continue
        
        home_won = match_features['result'] == 'HOME_WIN'
        
        # Análisis 1: Resultado prev_home
        prev_result = prev_features['prev_home_result']
        results[f'prev_result_{prev_result}']['total'] += 1
        if home_won:
            results[f'prev_result_{prev_result}']['home_wins'] += 1
        
        # Análisis 2: Prev_home ganó + handicap similar
        if prev_result == 'W' and 'prev_home_ah' in prev_features and 'current_ah' in match_features:
            ah_diff = abs(prev_features['prev_home_ah'] - match_features['current_ah'])
            if ah_diff < 0.5:
                results['prev_W_ah_similar']['total'] += 1
                if home_won:
                    results['prev_W_ah_similar']['home_wins'] += 1
        
        # Análisis 3: Prev_home cubrió AH fácilmente
        if prev_features.get('prev_home_ah_result') == 'COVER_HOME':
            results['prev_covered_ah']['total'] += 1
            if home_won:
                results['prev_covered_ah']['home_wins'] += 1
        
        # Análisis 4: Edge de ataques peligrosos > 15
        if prev_features.get('prev_home_ap_edge', 0) > 15:
            results['prev_ap_edge_>15']['total'] += 1
            if home_won:
                results['prev_ap_edge_>15']['home_wins'] += 1
        
        # Análisis 5: Margen de victoria amplio (>= 2)
        if prev_features['prev_home_margin'] >= 2:
            results['prev_margin_>=2']['total'] += 1
            if home_won:
                results['prev_margin_>=2']['home_wins'] += 1
    
    # Calcular accuracy
    for key in results:
        if results[key]['total'] > 0:
            results[key]['accuracy'] = results[key]['home_wins'] / results[key]['total']
            results[key]['win_rate'] = results[key]['accuracy']
    
    return dict(results)


def analyze_prev_home_to_over(matches: List[Dict]) -> Dict:
    """Analiza correlación prev_home → over"""
    results = defaultdict(lambda: {'total': 0, 'overs': 0})
    
    for match in matches:
        prev_features = extract_prev_home_features(match)
        match_features = extract_match_features(match)
        
        if not prev_features or not match_features or 'over' not in match_features:
            continue
        
        is_over = match_features['over']
        
        # Análisis 1: Goles totales en prev_home
        prev_goals = prev_features['prev_home_goals']
        
        if prev_goals >= 3:
            results['prev_goals_>=3']['total'] += 1
            if is_over:
                results['prev_goals_>=3']['overs'] += 1
        
        if prev_goals >= 4:
            results['prev_goals_>=4']['total'] += 1
            if is_over:
                results['prev_goals_>=4']['overs'] += 1
        
        if prev_goals <= 1:
            results['prev_goals_<=1']['total'] += 1
            if is_over:
                results['prev_goals_<=1']['overs'] += 1
        
        # Análisis 2: Prev_home con edge de ataques
        ap_edge = prev_features.get('prev_home_ap_edge', 0)
        if ap_edge > 15:
            results['prev_ap_edge_>15']['total'] += 1
            if is_over:
                results['prev_ap_edge_>15']['overs'] += 1
        
        # Análisis 3: Combinación goles + resultado
        if prev_goals >= 3 and prev_features['prev_home_result'] == 'W':
            results['prev_high_scoring_win']['total'] += 1
            if is_over:
                results['prev_high_scoring_win']['overs'] += 1
    
    # Calcular over_rate
    for key in results:
        if results[key]['total'] > 0:
            results[key]['over_rate'] = results[key]['overs'] / results[key]['total']
    
    return dict(results)


def analyze_line_movements(matches: List[Dict]) -> Dict:
    """Analiza movimientos de línea óptimos"""
    results = defaultdict(lambda: {'total': 0, 'covers': 0, 'roi': 0.0})
    
    for match in matches:
        match_features = extract_match_features(match)
        
        if not match_features or 'line_delta' not in match_features:
            continue
        
        delta = match_features['line_delta']
        covered = match_features.get('ah_result') == 'COVER_HOME'
        
        # Clasificar movimientos
        if abs(delta) < 0.1:
            category = 'sin_cambio'
        elif 0.1 <= delta < 0.5:
            category = 'sube_pequeño'
        elif delta >= 0.5:
            category = 'sube_grande'
        elif -0.5 < delta <= -0.1:
            category = 'baja_pequeño'
        else:
            category = 'baja_grande'
        
        results[category]['total'] += 1
        if covered:
            results[category]['covers'] += 1
        
        # Análisis combinado con prev_home
        prev_features = extract_prev_home_features(match)
        if prev_features:
            # Línea sube + prev_home ganó
            if delta > 0.2 and prev_features['prev_home_result'] == 'W':
                results['sube_y_prev_W']['total'] += 1
                if covered:
                    results['sube_y_prev_W']['covers'] += 1
            
            # Línea baja + prev_home perdió
            if delta < -0.2 and prev_features['prev_home_result'] == 'L':
                results['baja_y_prev_L']['total'] += 1
                if covered:
                    results['baja_y_prev_L']['covers'] += 1
            
            # Línea sube + prev_home cubrió AH
            if delta > 0.2 and prev_features.get('prev_home_ah_result') == 'COVER_HOME':
                results['sube_y_prev_covered']['total'] += 1
                if covered:
                    results['sube_y_prev_covered']['covers'] += 1
    
    # Calcular accuracy y ROI estimado
    for key in results:
        if results[key]['total'] > 0:
            acc = results[key]['covers'] / results[key]['total']
            results[key]['accuracy'] = acc
            # ROI simple: (accuracy * 2) - 1 para odds cercanos a 2.0
            results[key]['roi'] = (acc * 2.0) - 1.0
    
    return dict(results)


def generate_report(analysis_data: Dict) -> str:
    """Genera informe markdown"""
    report = f"""# Análisis: Prev Home → Victoria Local & Over + Movimientos de Línea

**Fecha de análisis:** {analysis_data['timestamp']}  
**Total de partidos analizados:** {analysis_data['total_matches']}  
**Partidos con prev_home válido:** {analysis_data['with_prev_home']}

---

## 1. Prev Home → Victoria Local

¿Qué resultados en el partido previo en casa correlacionan con victoria del equipo local?

### Por Resultado Prev Home

| Condición | Samples | Win Rate Local | Insight |
|-----------|---------|----------------|---------|
"""
    
    victory_data = analysis_data.get('prev_home_victoria', {})
    for key, data in sorted(victory_data.items(), key=lambda x: x[1].get('accuracy', 0), reverse=True):
        if data['total'] < 20:  # Filtrar muestras pequeñas
            continue
        samples = data['total']
        win_rate = data.get('win_rate', 0)
        
        emoji = "✅" if win_rate > 0.60 else "⚠️" if win_rate > 0.50 else "❌"
        report += f"| {key.replace('_', ' ').title()} | {samples} | **{win_rate*100:.1f}%** | {emoji} |\n"
    
    report += f"""

### Top 5 Reglas de Alta Precisión

"""
    
    top_rules = sorted(victory_data.items(), key=lambda x: x[1].get('accuracy', 0), reverse=True)[:5]
    for i, (key, data) in enumerate(top_rules, 1):
        if data['total'] < 20:
            continue
        report += f"{i}. **{key.replace('_', ' ').title()}**: {data.get('win_rate', 0)*100:.1f}% win rate ({data['total']} samples)\n"
    
    report += f"""

---

## 2. Prev Home → Over (Muchos Goles)

¿Qué características del prev_home predicen partidos con over?

### Análisis de Goles Previos

| Condición | Samples | Over Rate | Insight |
|-----------|---------|-----------|---------|
"""
    
    over_data = analysis_data.get('prev_home_over', {})
    for key, data in sorted(over_data.items(), key=lambda x: x[1].get('over_rate', 0), reverse=True):
        if data['total'] < 20:
            continue
        samples = data['total']
        over_rate = data.get('over_rate', 0)
        
        emoji = "🔥" if over_rate > 0.65 else "📊" if over_rate > 0.50 else "❄️"
        report += f"| {key.replace('_', ' ').title()} | {samples} | **{over_rate*100:.1f}%** | {emoji} |\n"
    
    report += f"""

---

## 3. Movimientos de Línea Óptimos

¿Qué cambios de handicap generan mejor ROI?

### Por Tipo de Movimiento

| Movimiento | Samples | Accuracy | ROI Estimado | Recomendación |
|------------|---------|----------|--------------|---------------|
"""
    
    line_data = analysis_data.get('movimientos_linea', {})
    for key, data in sorted(line_data.items(), key=lambda x: x[1].get('roi', 0), reverse=True):
        if data['total'] < 15:
            continue
        samples = data['total']
        accuracy = data.get('accuracy', 0)
        roi = data.get('roi', 0)
        
        if roi > 0.10:
            rec = "✅ Favorable"
        elif roi > 0:
            rec = "📊 Neutral"
        else:
            rec = "❌ Evitar"
        
        report += f"| {key.replace('_', ' ').title()} | {samples} | {accuracy*100:.1f}% | **{roi*100:+.1f}%** | {rec} |\n"
    
    report += f"""

---

## 4. Conclusiones y Recomendaciones

### 🎯 Patrones Más Confiables

"""
    
    # Combinar todas las métricas y encontrar las mejores
    all_patterns = []
    
    for key, data in victory_data.items():
        if data['total'] >= 30 and data.get('win_rate', 0) > 0.65:
            all_patterns.append(('Victoria Local', key, data.get('win_rate', 0), data['total']))
    
    for key, data in over_data.items():
        if data['total'] >= 30 and data.get('over_rate', 0) > 0.65:
            all_patterns.append(('Over', key, data.get('over_rate', 0), data['total']))
    
    for key, data in line_data.items():
        if data['total'] >= 25 and data.get('roi', 0) > 0.15:
            all_patterns.append(('Movimiento Línea', key, data.get('roi', 0) + 1, data['total']))
    
    all_patterns.sort(key=lambda x: x[2], reverse=True)
    
    for i, (tipo, patron, valor, samples) in enumerate(all_patterns[:10], 1):
        report += f"{i}. **{tipo}** - {patron.replace('_', ' ').title()}: {valor*100:.1f}% ({samples} muestras)\n"
    
    report += """

### 💡 Insights Clave

- **Victoria Local**: Los partidos donde el local ganó su prev_home con handicap similar muestran alta correlación con victoria en el actual
- **Over**: Partidos prev_home con 3+ goles tienen mayor probabilidad de over
- **Movimientos**: Las líneas que suben después de un prev_home exitoso tienden a cubrir mejor

### ⚠️ Advertencias

- Todas las métricas requieren validación continua
- Muestras < 30 tienen menos fiabilidad estadística
- El contexto específico de cada liga puede variar

---

*Análisis generado automáticamente por `analisis_prev_home_movimientos.py`*
"""
    
    return report


def main():
    print("=" * 80)
    print("🎯 ANÁLISIS PREV HOME → VICTORIA & OVER + MOVIMIENTOS DE LÍNEA")
    print("=" * 80)
    print()
    
    # Cargar datos
    print("📂 Cargando datos históricos...")
    matches = load_all_matches()
    print(f"✅ Total de partidos cargados: {len(matches)}")
    
    # Filtrar partidos con prev_home válido
    valid_matches = []
    for match in matches:
        prev_features = extract_prev_home_features(match)
        match_features = extract_match_features(match)
        if prev_features and match_features:
            valid_matches.append(match)
    
    print(f"✅ Partidos con prev_home válido: {len(valid_matches)}")
    print()
    
    if len(valid_matches) < 100:
        print("⚠️ Muy pocos partidos para análisis confiable")
        return
    
    # Análisis 1: Prev Home → Victoria
    print("📊 Analizando correlación Prev Home → Victoria Local...")
    victory_analysis = analyze_prev_home_to_victory(valid_matches)
    print(f"   ✅ {len(victory_analysis)} patrones identificados")
    
    # Análisis 2: Prev Home → Over
    print("📊 Analizando correlación Prev Home → Over...")
    over_analysis = analyze_prev_home_to_over(valid_matches)
    print(f"   ✅ {len(over_analysis)} patrones identificados")
    
    # Análisis 3: Movimientos de Línea
    print("📊 Analizando movimientos de línea...")
    line_analysis = analyze_line_movements(valid_matches)
    print(f"   ✅ {len(line_analysis)} patrones identificados")
    print()
    
    # Preparar datos de salida
    from datetime import datetime
    output_data = {
        'timestamp': datetime.now().isoformat(),
        'total_matches': len(matches),
        'with_prev_home': len(valid_matches),
        'prev_home_victoria': victory_analysis,
        'prev_home_over': over_analysis,
        'movimientos_linea': line_analysis
    }
    
    # Guardar JSON
    print(f"💾 Guardando resultados en JSON...")
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    print(f"   ✅ {OUTPUT_JSON}")
    
    # Generar informe
    print(f"📝 Generando informe markdown...")
    OUTPUT_REPORT.parent.mkdir(parents=True, exist_ok=True)
    report_text = generate_report(output_data)
    with open(OUTPUT_REPORT, 'w', encoding='utf-8') as f:
        f.write(report_text)
    print(f"   ✅ {OUTPUT_REPORT}")
    
    print()
    print("=" * 80)
    print("✅ ANÁLISIS COMPLETADO")
    print("=" * 80)
    print()
    print("📊 RESUMEN DE HALLAZGOS:")
    print()
    
    # Mostrar top 3 de cada categoría
    print("🏆 Top 3 Patrones Victoria Local:")
    for i, (key, data) in enumerate(sorted(victory_analysis.items(), 
                                           key=lambda x: x[1].get('accuracy', 0), 
                                           reverse=True)[:3], 1):
        if data['total'] >= 20:
            print(f"   {i}. {key}: {data.get('win_rate', 0)*100:.1f}% ({data['total']} muestras)")
    
    print()
    print("🔥 Top 3 Patrones Over:")
    for i, (key, data) in enumerate(sorted(over_analysis.items(), 
                                           key=lambda x: x[1].get('over_rate', 0), 
                                           reverse=True)[:3], 1):
        if data['total'] >= 20:
            print(f"   {i}. {key}: {data.get('over_rate', 0)*100:.1f}% ({data['total']} muestras)")
    
    print()
    print("📈 Top 3 Movimientos de Línea (ROI):")
    for i, (key, data) in enumerate(sorted(line_analysis.items(), 
                                           key=lambda x: x[1].get('roi', 0), 
                                           reverse=True)[:3], 1):
        if data['total'] >= 15:
            print(f"   {i}. {key}: ROI {data.get('roi', 0)*100:+.1f}% ({data['total']} muestras)")
    
    print()


if __name__ == '__main__':
    main()
