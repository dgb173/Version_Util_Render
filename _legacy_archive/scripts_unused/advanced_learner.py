"""
SISTEMA DE APRENDIZAJE AVANZADO - FILOSOFÍA COMPLETA
Busca patrones usando TODA la lógica profesional:
- Comparativas indirectas (quién ganó a quién)
- Movimiento de línea (subió/bajó)
- Handicap repetido
- Frescura de datos (<6 meses)
- Contexto de equipos (fuerte/débil, casa/fuera)
- Stats (ataques, disparos)
"""
import json
import os
import random
from datetime import datetime, timedelta
from collections import defaultdict

# Directorio de datos
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
MAX_MESES_VALIDOS = 6

def parse_date(date_str):
    """Parsea fecha en varios formatos"""
    if not date_str:
        return None
    for fmt in ['%Y-%m-%d', '%Y-%m-%d %H:%M', '%d-%m-%Y', '%d/%m/%Y']:
        try:
            return datetime.strptime(str(date_str)[:10], fmt[:10])
        except:
            continue
    return None

def meses_desde(fecha_str):
    """Calcula meses desde una fecha"""
    fecha = parse_date(fecha_str)
    if not fecha:
        return 999
    diff = datetime.now() - fecha
    return diff.days / 30

def is_fresh(fecha_str, max_meses=MAX_MESES_VALIDOS):
    """Verifica si una fecha es fresca"""
    return meses_desde(fecha_str) <= max_meses

def load_all_matches():
    """Carga todos los partidos"""
    all_matches = []
    
    if not os.path.exists(DATA_DIR):
        print(f"[!] No existe {DATA_DIR}")
        return []
    
    files = [f for f in os.listdir(DATA_DIR) if f.endswith('.json')]
    print(f"Cargando {len(files)} archivos...")
    
    for filename in files:
        filepath = os.path.join(DATA_DIR, filename)
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    all_matches.extend(data)
                elif isinstance(data, dict) and 'partidos' in data:
                    all_matches.extend(data['partidos'])
        except Exception as e:
            pass
    
    print(f"Total partidos cargados: {len(all_matches)}")
    return all_matches

def safe_int(val, default=0):
    """Convierte a int de forma segura"""
    try:
        return int(val)
    except:
        return default

def safe_float(val, default=0.0):
    """Convierte a float de forma segura"""
    try:
        return float(val)
    except:
        return default

def extract_features(match):
    """Extrae features profesionales de un partido"""
    features = {}
    
    # Datos básicos
    ah = safe_float(match.get('ah', 0))
    ou = safe_float(match.get('ou', 2.5))
    
    # Rango de handicap
    features['ah_0'] = abs(ah) < 0.01
    features['ah_05'] = 0 < abs(ah) <= 0.5
    features['ah_1'] = 0.5 < abs(ah) <= 1
    features['ah_15_plus'] = abs(ah) > 1
    features['fav_local'] = ah > 0
    features['fav_visita'] = ah < 0
    
    # ==================== PREV HOME ====================
    prev_home = match.get('last_home_match') or {}
    ph_fecha = prev_home.get('date', '') or prev_home.get('fecha', '')
    features['ph_fresco'] = is_fresh(ph_fecha)
    
    ph_score_str = (prev_home.get('score', '') or '').replace('-', ':')
    ph_score = ph_score_str.split(':') if ph_score_str else []
    if len(ph_score) == 2:
        ph_h = safe_int(ph_score[0])
        ph_a = safe_int(ph_score[1])
        features['ph_gano_local'] = ph_h > ph_a
        features['ph_empato'] = ph_h == ph_a
        features['ph_perdio_local'] = ph_h < ph_a
        features['ph_goleo'] = (ph_h + ph_a) >= 4
        features['ph_under'] = (ph_h + ph_a) <= 2
        features['ph_porteria_0'] = ph_a == 0
        features['ph_encajo'] = ph_a >= 2
    
    # ==================== PREV AWAY ====================
    prev_away = match.get('last_away_match') or {}
    pa_fecha = prev_away.get('date', '') or prev_away.get('fecha', '')
    features['pa_fresco'] = is_fresh(pa_fecha)
    
    pa_score_str = (prev_away.get('score', '') or '').replace('-', ':')
    pa_score = pa_score_str.split(':') if pa_score_str else []
    if len(pa_score) == 2:
        pa_h = safe_int(pa_score[0])
        pa_a = safe_int(pa_score[1])
        features['pa_gano_visita'] = pa_a > pa_h
        features['pa_empato'] = pa_h == pa_a
        features['pa_perdio_visita'] = pa_a < pa_h
        features['pa_goleo'] = (pa_h + pa_a) >= 4
        features['pa_under'] = (pa_h + pa_a) <= 2
        features['pa_porteria_0'] = pa_h == 0
        features['pa_encajo'] = pa_h >= 2
    
    # ==================== H2H COL3 ====================
    h2h_col3 = match.get('h2h_col3') or {}
    h2h_fecha = h2h_col3.get('date', '') or h2h_col3.get('fecha', '')
    features['h2h_fresco'] = is_fresh(h2h_fecha)
    
    h2h_ah = safe_float(h2h_col3.get('ah', 0))
    h2h_goals = safe_int(h2h_col3.get('goles_home', 0)) + safe_int(h2h_col3.get('goles_away', 0))
    features['h2h_goleo'] = h2h_goals >= 4
    features['h2h_under'] = h2h_goals <= 2
    
    # Handicap repetido
    features['ha_repetido_exacto'] = abs(abs(ah) - abs(h2h_ah)) <= 0.1
    features['ha_repetido_cerca'] = abs(abs(ah) - abs(h2h_ah)) <= 0.25
    
    # ==================== INDIRECTAS ====================
    comp_ind = match.get('comparativas_indirectas') or {}
    ind_left = comp_ind.get('left') or {}
    ind_right = comp_ind.get('right') or {}
    
    # Indirecta izquierda (LOCAL vs rival del visitante)
    il_score_str = (ind_left.get('score', '') or '').replace('-', ':')
    il_score = il_score_str.split(':') if ':' in il_score_str else []
    if len(il_score) == 2:
        il_h = safe_int(il_score[0])
        il_a = safe_int(il_score[1])
        features['ind_local_goleo'] = il_h >= 3 or (il_h - il_a) >= 2
        features['ind_local_gano'] = il_h > il_a
        features['ind_local_perdio'] = il_h < il_a
    
    # Indirecta derecha (rival del local vs VISITANTE)
    ir_score_str = (ind_right.get('score', '') or '').replace('-', ':')
    ir_score = ir_score_str.split(':') if ':' in ir_score_str else []
    if len(ir_score) == 2:
        ir_h = safe_int(ir_score[0])
        ir_a = safe_int(ir_score[1])
        features['ind_visita_perdio'] = ir_a < ir_h
        features['ind_visita_gano'] = ir_a > ir_h
        features['ind_visita_goleo'] = ir_a >= 3 or (ir_a - ir_h) >= 2
    
    # Indirectas alineadas
    if 'ind_local_goleo' in features and 'ind_visita_perdio' in features:
        features['ind_alineadas_local'] = features.get('ind_local_goleo') and features.get('ind_visita_perdio')
    if 'ind_visita_goleo' in features and 'ind_local_perdio' in features:
        features['ind_alineadas_visita'] = features.get('ind_visita_goleo') and features.get('ind_local_perdio')
    
    # ==================== STANDINGS ====================
    home_st = match.get('home_standings') or {}
    away_st = match.get('away_standings') or {}
    
    h_rank = safe_int(home_st.get('ranking', 99)) or 99
    a_rank = safe_int(away_st.get('ranking', 99)) or 99
    
    features['h_top5'] = 0 < h_rank <= 5
    features['a_top5'] = 0 < a_rank <= 5
    features['h_top10'] = 0 < h_rank <= 10
    features['a_top10'] = 0 < a_rank <= 10
    features['h_bottom'] = h_rank >= 15
    features['a_bottom'] = a_rank >= 15
    features['h_mejor_rank'] = h_rank < a_rank and h_rank > 0 and a_rank > 0
    features['a_mejor_rank'] = a_rank < h_rank and h_rank > 0 and a_rank > 0
    
    # Forma (W-D-L)
    h_form = (home_st.get('form', '') or '0-0-0').split('-')
    a_form = (away_st.get('form', '') or '0-0-0').split('-')
    if len(h_form) >= 3:
        h_played = safe_int(h_form[0]) + safe_int(h_form[1]) + safe_int(h_form[2])
        if h_played > 0:
            h_winrate = safe_int(h_form[0]) / h_played
            features['h_fuerte'] = h_winrate >= 0.6
            features['h_debil'] = h_winrate <= 0.3
    if len(a_form) >= 3:
        a_played = safe_int(a_form[0]) + safe_int(a_form[1]) + safe_int(a_form[2])
        if a_played > 0:
            a_winrate = safe_int(a_form[0]) / a_played
            features['a_fuerte'] = a_winrate >= 0.5
            features['a_debil'] = a_winrate <= 0.2
    
    # ==================== COMBINACIONES CLAVE ====================
    # Momentum
    if 'ph_gano_local' in features and 'pa_perdio_visita' in features:
        features['momentum_local'] = features.get('ph_gano_local') and features.get('pa_perdio_visita')
    if 'pa_gano_visita' in features and 'ph_perdio_local' in features:
        features['momentum_visita'] = features.get('pa_gano_visita') and features.get('ph_perdio_local')
    
    # Datos frescos completos
    features['datos_frescos'] = features.get('ph_fresco', False) and features.get('pa_fresco', False)
    
    return features

def get_result(match):
    """Obtiene resultado del partido (LOCAL/VISITA/EMPATE)"""
    final_score = match.get('final_score', '')
    if not final_score or ':' not in str(final_score):
        return None
    
    parts = str(final_score).split(':')
    if len(parts) != 2:
        return None
    
    try:
        h = int(parts[0])
        a = int(parts[1])
        ah = safe_float(match.get('ah', 0))
        
        # Determinar si cubrió el handicap
        if ah > 0:  # Local favorito
            if h + ah > a:
                return 'LOCAL'  # Local cubrió
            elif h + ah < a:
                return 'VISITA'  # Visita cubrió
        elif ah < 0:  # Visita favorito
            if a + abs(ah) > h:
                return 'VISITA'  # Visita cubrió
            elif a + abs(ah) < h:
                return 'LOCAL'  # Local cubrió
        else:
            if h > a:
                return 'LOCAL'
            elif a > h:
                return 'VISITA'
        return 'PUSH'
    except:
        return None

class AdvancedPatternLearner:
    """Sistema de aprendizaje avanzado"""
    
    def __init__(self):
        self.patterns = []
        self.min_accuracy = 0.74
        self.min_samples = 12
        self.all_features = set()
        
    def train(self, matches, generations=5000):
        """Entrena buscando patrones ganadores"""
        print(f"\n{'='*60}")
        print("ENTRENANDO PATRONES AVANZADOS")
        print(f"{'='*60}")
        
        # Preprocesar solo partidos con datos frescos y resultado
        valid_matches = []
        for m in matches:
            result = get_result(m)
            if result in ['LOCAL', 'VISITA']:
                features = extract_features(m)
                if features.get('datos_frescos', False):
                    valid_matches.append({
                        'features': features,
                        'result': result,
                        'ah': safe_float(m.get('ah', 0))
                    })
                    self.all_features.update(features.keys())
        
        print(f"Partidos válidos (datos frescos): {len(valid_matches)}")
        print(f"Features disponibles: {len(self.all_features)}")
        
        if len(valid_matches) < 50:
            print("[!] Insuficientes partidos para entrenar")
            return
        
        # Separar por rango de handicap
        groups = {
            'AH_0': [m for m in valid_matches if m['features'].get('ah_0', False)],
            'AH_05': [m for m in valid_matches if m['features'].get('ah_05', False)],
            'AH_1': [m for m in valid_matches if m['features'].get('ah_1', False)],
            'AH_15+': [m for m in valid_matches if m['features'].get('ah_15_plus', False)]
        }
        
        all_features = list(self.all_features)
        
        for target in ['LOCAL', 'VISITA']:
            print(f"\n--- Buscando patrones {target} ---")
            
            for gen in range(generations):
                # Seleccionar 2-4 features aleatorias
                n_features = random.randint(2, min(4, len(all_features)))
                selected = random.sample(all_features, n_features)
                
                # Evaluar para cada grupo de handicap
                for group_name, group_matches in groups.items():
                    if len(group_matches) < 15:
                        continue
                    
                    # Contar matches que cumplen patrón
                    matches_pattern = [m for m in group_matches if all(m['features'].get(f, False) for f in selected)]
                    
                    if len(matches_pattern) < self.min_samples:
                        continue
                    
                    # Calcular accuracy
                    wins = sum(1 for m in matches_pattern if m['result'] == target)
                    accuracy = wins / len(matches_pattern)
                    
                    if accuracy >= self.min_accuracy:
                        pattern = {
                            'features': selected,
                            'target': target,
                            'accuracy': round(accuracy, 3),
                            'samples': len(matches_pattern),
                            'wins': wins,
                            'ah_group': group_name
                        }
                        
                        # Evitar duplicados
                        is_dup = False
                        for p in self.patterns:
                            if set(p['features']) == set(selected) and p['target'] == target and p['ah_group'] == group_name:
                                is_dup = True
                                break
                        
                        if not is_dup:
                            self.patterns.append(pattern)
                            stars = '🔥' * min(3, 1 + int((accuracy - 0.74) * 10))
                            print(f"{stars} {target} {accuracy*100:.1f}% ({wins}/{len(matches_pattern)}) [{group_name}] <- {selected}")
                
                if (gen + 1) % 1000 == 0:
                    print(f"  Generación {gen+1}/{generations}...")
        
        # Ordenar por accuracy
        self.patterns.sort(key=lambda x: (-x['accuracy'], -x['samples']))
        
        print(f"\n{'='*60}")
        print(f"PATRONES ENCONTRADOS: {len(self.patterns)}")
        print(f"{'='*60}")
        
        # Top 30
        for i, p in enumerate(self.patterns[:30], 1):
            print(f"{i}. {p['target']} {p['accuracy']*100:.1f}% ({p['wins']}/{p['samples']}) [{p['ah_group']}] <- {p['features']}")
    
    def save(self, filepath):
        """Guarda patrones a archivo JSON"""
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump({
                'generated': datetime.now().isoformat(),
                'total_patterns': len(self.patterns),
                'min_accuracy': self.min_accuracy,
                'min_samples': self.min_samples,
                'patterns': self.patterns
            }, f, indent=2, ensure_ascii=False)
        print(f"\nPatrones guardados en: {filepath}")
    
    def generate_js_code(self):
        """Genera código JavaScript para precacheo.html"""
        print("\n// ========== PATRONES APRENDIDOS ==========")
        for i, p in enumerate(self.patterns[:20], 1):
            features_str = ' && '.join([f"features.{f}" for f in p['features']])
            print(f"""
// PATRÓN {i}: {p['target']} {p['accuracy']*100:.0f}% [{p['ah_group']}] ({p['wins']}/{p['samples']})
// Features: {p['features']}
else if ({features_str}) {{
    pick = '{p['target']}';
    probability = {int(p['accuracy']*100)};
    confidence = '{'ultra' if p['accuracy'] >= 0.85 else 'high'}';
    reason = '{('🔥🔥🔥' if p['accuracy'] >= 0.85 else '🔥🔥' if p['accuracy'] >= 0.80 else '🔥')} LEARNED_{i} {int(p['accuracy']*100)}%';
    ruleUsed = 'LEARNED_{i}_{int(p['accuracy']*100)}';
}}""")

def main():
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║     SISTEMA DE APRENDIZAJE AVANZADO - FILOSOFÍA COMPLETA ║
    ║     - Indirectas alineadas                               ║
    ║     - Handicap repetido                                  ║
    ║     - Datos frescos (<6 meses)                           ║
    ║     - Contexto equipos                                   ║
    ║     - Momentum                                           ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    # Cargar partidos
    matches = load_all_matches()
    
    if not matches:
        print("[!] No hay partidos para analizar")
        return
    
    # Entrenar
    learner = AdvancedPatternLearner()
    learner.train(matches, generations=5000)
    
    # Guardar
    output_path = os.path.join(os.path.dirname(__file__), 'advanced_learned_patterns.json')
    learner.save(output_path)
    
    # Generar código JS
    learner.generate_js_code()
    
    print("\n¡Entrenamiento avanzado completado!")

if __name__ == '__main__':
    main()
