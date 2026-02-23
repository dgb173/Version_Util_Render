"""
SISTEMA DE APRENDIZAJE CONTINUO - OVER/UNDER Y HANDICAP
Analiza partidos finalizados y descubre patrones ganadores
"""
import json
import os
import random
from datetime import datetime
from collections import defaultdict

# Directorios de datos - usar data/ directamente
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')

def load_all_matches():
    """Carga todos los partidos históricos"""
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
            print(f"Error en {filename}: {e}")
    
    print(f"Total partidos cargados: {len(all_matches)}")
    return all_matches

def extract_features_ou(match):
    """Extrae features para predicción O/U"""
    features = {}
    
    # Línea O/U
    ou = float(match.get('ou', 0) or 0)
    features['ou_bajo'] = ou <= 2.25
    features['ou_normal'] = 2.25 < ou <= 2.75
    features['ou_alto'] = ou > 2.75
    features['ou_muy_alto'] = ou > 3
    
    # Handicap como contexto
    ah = abs(float(match.get('ah', 0) or 0))
    features['ah_bajo'] = ah <= 0.5
    features['ah_medio'] = 0.5 < ah <= 1
    features['ah_alto'] = ah > 1
    features['ah_muy_alto'] = ah >= 1.5
    
    # Partido previo LOCAL
    prev_home = match.get('last_home_match') or {}
    ph_score_str = (prev_home.get('score', '') or '').replace('-', ':')
    ph_score = ph_score_str.split(':') if ph_score_str else []
    if len(ph_score) == 2:
        try:
            ph_goals = int(ph_score[0] or 0) + int(ph_score[1] or 0)
            features['ph_over'] = ph_goals > 2.5
            features['ph_goleada'] = ph_goals >= 4
            features['ph_under'] = ph_goals <= 2
            features['ph_0goles'] = ph_goals == 0
            features['ph_ambos'] = int(ph_score[0] or 0) > 0 and int(ph_score[1] or 0) > 0
        except:
            pass
    
    # Partido previo VISITANTE
    prev_away = match.get('last_away_match') or {}
    pa_score_str = (prev_away.get('score', '') or '').replace('-', ':')
    pa_score = pa_score_str.split(':') if pa_score_str else []
    if len(pa_score) == 2:
        try:
            pa_goals = int(pa_score[0] or 0) + int(pa_score[1] or 0)
            features['pa_over'] = pa_goals > 2.5
            features['pa_goleada'] = pa_goals >= 4
            features['pa_under'] = pa_goals <= 2
            features['pa_0goles'] = pa_goals == 0
            features['pa_ambos'] = int(pa_score[0] or 0) > 0 and int(pa_score[1] or 0) > 0
        except:
            pass
    
    # Combinaciones previas
    if 'ph_over' in features and 'pa_over' in features:
        features['ambos_prev_over'] = features['ph_over'] and features['pa_over']
        features['ambos_prev_under'] = features.get('ph_under', False) and features.get('pa_under', False)
        features['mix_over_under'] = features['ph_over'] != features['pa_over']
    
    # H2H Col3
    h2h_col3 = match.get('h2h_col3', {})
    h2h_goals = int(h2h_col3.get('goles_home', 0) or 0) + int(h2h_col3.get('goles_away', 0) or 0)
    features['h2h_over'] = h2h_goals > 2.5
    features['h2h_goleada'] = h2h_goals >= 4
    features['h2h_under'] = h2h_goals <= 2
    features['h2h_0goles'] = h2h_goals == 0
    
    # H2H Estadio
    h2h_stadium = match.get('h2h_stadium', {})
    if h2h_stadium:
        stadium_goals = int(h2h_stadium.get('goles_home', 0) or 0) + int(h2h_stadium.get('goles_away', 0) or 0)
        features['stadium_over'] = stadium_goals > 2.5
        features['stadium_under'] = stadium_goals <= 2
    
    # Comparativas indirectas
    comp_ind = match.get('comparativas_indirectas', {})
    ind_left = comp_ind.get('left', {})
    ind_right = comp_ind.get('right', {})
    
    if ind_left.get('score'):
        il_score = ind_left['score'].split(':')
        if len(il_score) == 2:
            il_goals = int(il_score[0] or 0) + int(il_score[1] or 0)
            features['ind_l_over'] = il_goals > 2.5
            features['ind_l_under'] = il_goals <= 2
    
    if ind_right.get('score'):
        ir_score = ind_right['score'].split(':')
        if len(ir_score) == 2:
            ir_goals = int(ir_score[0] or 0) + int(ir_score[1] or 0)
            features['ind_r_over'] = ir_goals > 2.5
            features['ind_r_under'] = ir_goals <= 2
    
    # Rankings
    home_standings = match.get('home_standings', {})
    away_standings = match.get('away_standings', {})
    try:
        h_rank = int(home_standings.get('ranking', 99) or 99)
    except (ValueError, TypeError):
        h_rank = 99
    try:
        a_rank = int(away_standings.get('ranking', 99) or 99)
    except (ValueError, TypeError):
        a_rank = 99
    
    features['h_top5'] = 0 < h_rank <= 5
    features['a_top5'] = 0 < a_rank <= 5
    features['h_bottom'] = h_rank >= 15
    features['a_bottom'] = a_rank >= 15
    features['top_vs_bottom'] = (features['h_top5'] and features['a_bottom']) or (features['a_top5'] and features['h_bottom'])
    features['ambos_top'] = features['h_top5'] and features['a_top5']
    features['ambos_bottom'] = features['h_bottom'] and features['a_bottom']
    
    # Form (W-D-L)
    h_form = home_standings.get('form', '0-0-0').split('-')
    a_form = away_standings.get('form', '0-0-0').split('-')
    if len(h_form) >= 3 and len(a_form) >= 3:
        h_played = int(h_form[0] or 0) + int(h_form[1] or 0) + int(h_form[2] or 0)
        a_played = int(a_form[0] or 0) + int(a_form[1] or 0) + int(a_form[2] or 0)
        if h_played > 0 and a_played > 0:
            h_winrate = int(h_form[0] or 0) / h_played
            a_winrate = int(a_form[0] or 0) / a_played
            features['h_fuerte'] = h_winrate >= 0.6
            features['a_fuerte'] = a_winrate >= 0.5
            features['h_debil'] = h_winrate <= 0.3
            features['a_debil'] = a_winrate <= 0.2
            features['ambos_fuertes'] = features['h_fuerte'] and features['a_fuerte']
            features['ambos_debiles'] = features['h_debil'] and features['a_debil']
    
    # Goles de equpos (si disponible)
    h_gf = int(home_standings.get('gf', 0) or 0)
    h_gc = int(home_standings.get('gc', 0) or 0)
    a_gf = int(away_standings.get('gf', 0) or 0)
    a_gc = int(away_standings.get('gc', 0) or 0)
    
    if h_gf > 0 or a_gf > 0:
        h_pj = int(home_standings.get('pj', 1) or 1)
        a_pj = int(away_standings.get('pj', 1) or 1)
        features['h_goleador'] = h_gf / h_pj >= 1.5 if h_pj > 0 else False
        features['a_goleador'] = a_gf / a_pj >= 1.5 if a_pj > 0 else False
        features['h_defensivo'] = h_gc / h_pj <= 1 if h_pj > 0 else False
        features['a_defensivo'] = a_gc / a_pj <= 1 if a_pj > 0 else False
        features['ambos_goleadores'] = features['h_goleador'] and features['a_goleador']
        features['ambos_defensivos'] = features['h_defensivo'] and features['a_defensivo']
    
    return features

def get_ou_result(match):
    """Obtiene resultado O/U del partido"""
    final_score = match.get('final_score', '')
    if not final_score or ':' not in str(final_score):
        return None
    
    parts = str(final_score).split(':')
    if len(parts) != 2:
        return None
    
    try:
        goals = int(parts[0]) + int(parts[1])
        ou_line = float(match.get('ou', 2.5) or 2.5)
        
        if goals > ou_line:
            return 'OVER'
        elif goals < ou_line:
            return 'UNDER'
        else:
            return 'PUSH'
    except:
        return None

class OUPatternLearner:
    """Sistema de aprendizaje de patrones O/U"""
    
    def __init__(self):
        self.patterns = []
        self.min_accuracy = 0.72
        self.min_samples = 15
        self.all_features = set()
        
    def train(self, matches, generations=3000):
        """Entrena buscando patrones ganadores"""
        print(f"\n{'='*60}")
        print("ENTRENANDO PATRONES O/U")
        print(f"{'='*60}")
        
        # Preprocesar
        valid_matches = []
        for m in matches:
            result = get_ou_result(m)
            if result in ['OVER', 'UNDER']:
                features = extract_features_ou(m)
                if features:
                    valid_matches.append({
                        'features': features,
                        'result': result,
                        'ou': float(m.get('ou', 2.5) or 2.5)
                    })
                    self.all_features.update(features.keys())
        
        print(f"Partidos válidos: {len(valid_matches)}")
        print(f"Features disponibles: {len(self.all_features)}")
        
        if len(valid_matches) < 50:
            print("[!] Insuficientes partidos para entrenar")
            return
        
        # Separar por línea O/U
        groups = {
            'bajo': [m for m in valid_matches if m['ou'] <= 2.25],
            'normal': [m for m in valid_matches if 2.25 < m['ou'] <= 2.75],
            'alto': [m for m in valid_matches if m['ou'] > 2.75]
        }
        
        all_features = list(self.all_features)
        
        for target in ['OVER', 'UNDER']:
            print(f"\n--- Buscando patrones {target} ---")
            
            for gen in range(generations):
                # Seleccionar 2-4 features aleatorias
                n_features = random.randint(2, min(4, len(all_features)))
                selected = random.sample(all_features, n_features)
                
                # Evaluar para cada grupo de línea
                for group_name, group_matches in groups.items():
                    if len(group_matches) < 20:
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
                            'accuracy': accuracy,
                            'samples': len(matches_pattern),
                            'wins': wins,
                            'ou_group': group_name
                        }
                        
                        # Evitar duplicados
                        is_dup = False
                        for p in self.patterns:
                            if set(p['features']) == set(selected) and p['target'] == target and p['ou_group'] == group_name:
                                is_dup = True
                                break
                        
                        if not is_dup:
                            self.patterns.append(pattern)
                            stars = '*' * (1 + int((accuracy - 0.72) * 10))
                            print(f"{stars} OU {target} {accuracy*100:.1f}% ({wins}/{len(matches_pattern)}) <- {selected} [{group_name}]")
                
                if (gen + 1) % 500 == 0:
                    print(f"  Generación {gen+1}/{generations}...")
        
        # Ordenar por accuracy
        self.patterns.sort(key=lambda x: (-x['accuracy'], -x['samples']))
        
        print(f"\n{'='*60}")
        print(f"PATRONES O/U ENCONTRADOS: {len(self.patterns)}")
        print(f"{'='*60}")
        
        # Top 20
        for i, p in enumerate(self.patterns[:20], 1):
            print(f"{i}. {p['target']} {p['accuracy']*100:.1f}% ({p['wins']}/{p['samples']}) [{p['ou_group']}] <- {p['features']}")
    
    def save(self, filepath):
        """Guarda patrones a archivo JSON"""
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump({
                'generated': datetime.now().isoformat(),
                'total_patterns': len(self.patterns),
                'patterns': self.patterns
            }, f, indent=2, ensure_ascii=False)
        print(f"\nPatrones guardados en: {filepath}")

def main():
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║     SISTEMA DE APRENDIZAJE CONTINUO - OVER/UNDER         ║
    ║     Analizando partidos finalizados para patrones        ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    # Cargar partidos
    matches = load_all_matches()
    
    if not matches:
        print("[!] No hay partidos para analizar")
        return
    
    # Entrenar O/U
    learner = OUPatternLearner()
    learner.train(matches, generations=3000)
    
    # Guardar
    output_path = os.path.join(os.path.dirname(__file__), 'ou_learned_patterns.json')
    learner.save(output_path)
    
    print("\n¡Entrenamiento O/U completado!")

if __name__ == '__main__':
    main()
