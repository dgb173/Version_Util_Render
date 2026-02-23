"""
Precacheo Loader v2 - Runtime para /precacheo

Carga patrones v2 y los aplica a partidos upcoming para generar picks.
"""

import json
from pathlib import Path
from typing import Dict, List, Any, Optional

from .features_builder_v2 import build_match_features, discretize_features
from .rule_miner import evaluate_pattern


class PrecacheoLoaderV2:
    """
    Loader de patrones v2 para runtime en /precacheo.
    """
    
    def __init__(self, patterns_dir: str = None):
        """
        Inicializa el loader.
        
        Args:
            patterns_dir: Directorio con los archivos patterns_v2_*.json
        """
        self.patterns_dir = patterns_dir
        self.ah_patterns = []
        self.ou_patterns = []
        self.loaded = False
        
        if patterns_dir:
            self.load_all_patterns()
    
    def load_all_patterns(self, patterns_dir: str = None) -> bool:
        """
        Carga todos los patrones v2 de los archivos JSON.
        """
        if patterns_dir:
            self.patterns_dir = patterns_dir
        
        if not self.patterns_dir:
            return False
        
        patterns_path = Path(self.patterns_dir)
        
        if not patterns_path.exists():
            print(f"[WARN] Directorio no encontrado: {patterns_path}")
            return False
        
        self.ah_patterns = []
        self.ou_patterns = []
        
        # Cargar patrones AH (v2)
        for f in patterns_path.glob('patterns_v2_AH_*.json'):
            try:
                with open(f, 'r', encoding='utf-8') as file:
                    data = json.load(file)
                    self.ah_patterns.extend(data.get('patterns', []))
            except Exception as e:
                print(f"[WARN] Error cargando {f.name}: {e}")
        
        # Cargar patrones OU (v2)
        for f in patterns_path.glob('patterns_v2_OU_*.json'):
            try:
                with open(f, 'r', encoding='utf-8') as file:
                    data = json.load(file)
                    self.ou_patterns.extend(data.get('patterns', []))
            except Exception as e:
                print(f"[WARN] Error cargando {f.name}: {e}")
        
        # Cargar especialistas AH por línea específica
        for f in patterns_path.glob('specialist_ah_*.json'):
            try:
                with open(f, 'r', encoding='utf-8') as file:
                    data = json.load(file)
                    patterns = data.get('patterns', [])
                    line = data.get('line', 0)
                    # Añadir metadata de línea a cada patrón
                    for p in patterns:
                        p['specialist_line'] = line
                        p['market'] = 'AH'
                    self.ah_patterns.extend(patterns)
            except Exception as e:
                print(f"[WARN] Error cargando {f.name}: {e}")
        
        # Cargar especialistas O/U por línea específica
        for f in patterns_path.glob('specialist_ou_*.json'):
            try:
                with open(f, 'r', encoding='utf-8') as file:
                    data = json.load(file)
                    patterns = data.get('patterns', [])
                    line = data.get('line', 2.5)
                    # Añadir metadata de línea a cada patrón
                    for p in patterns:
                        p['specialist_line'] = line
                        p['market'] = 'OU'
                    self.ou_patterns.extend(patterns)
            except Exception as e:
                print(f"[WARN] Error cargando {f.name}: {e}")
        
        # Cargar patrones Sistema Qwen ML (especialistas por familia)
        qwen_count = 0
        for f in patterns_path.glob('qwen_*.json'):
            try:
                with open(f, 'r', encoding='utf-8') as file:
                    data = json.load(file)
                    patterns = data.get('patterns', [])
                    family = data.get('meta', {}).get('family', '')
                    for p in patterns:
                        p['algorithm'] = 'QWEN_ML'
                        p['market'] = 'AH'
                        if not p.get('family'):
                            p['family'] = family
                    self.ah_patterns.extend(patterns)
                    qwen_count += len(patterns)
            except Exception as e:
                print(f"[WARN] Error cargando {f.name}: {e}")
        
        if qwen_count > 0:
            print(f"[OK] Sistema Qwen: {qwen_count} patrones cargados")
        
        # Ordenar por ROI de test (soporta ambos formatos)
        def get_test_roi(p):
            # Formato v2: stats.roi_test
            roi = p.get('stats', {}).get('roi_test', 0)
            if roi == 0:
                # Formato especialista: test.roi
                roi = p.get('test', {}).get('roi', 0)
            return -roi
        
        self.ah_patterns.sort(key=get_test_roi)
        self.ou_patterns.sort(key=get_test_roi)
        
        self.loaded = True
        print(f"[OK] Patrones cargados: {len(self.ah_patterns)} AH, {len(self.ou_patterns)} OU")
        
        return True
    
    def reload(self) -> bool:
        """
        Recarga todos los patrones en caliente.
        Útil para cargar nuevos patrones sin reiniciar el servidor.
        """
        print("🔄 Recargando patrones...")
        self.loaded = False
        self.ah_patterns = []
        self.ou_patterns = []
        return self.load_all_patterns()
    
    def check_pattern_applies(self, features: Dict, pattern: Dict) -> bool:
        """
        Verifica si un patrón aplica a las features de un partido.
        Soporta patrones v2 (por familia) y especialistas (por línea específica).
        
        IMPORTANTE: El filtrado por familia es ESTRICTO.
        Las familias del Sistema Qwen v3 usan signo:
        - H0.5, H1.0, etc: Local es favorito (AH > 0)
        - A0.5, A1.0, etc: Away es favorito (AH < 0)
        """
        market = pattern.get('market', 'AH')
        pattern_family = pattern.get('family', '')
        algorithm = pattern.get('algorithm', 'PATTERN_V2')
        
        # Normalizar familia: H2.0_plus -> H2_0_plus
        pattern_family_norm = pattern_family.replace('.', '_')
        
        # ============================================================
        # FILTRADO POR FAMILIA (ESTRICTO)
        # ============================================================
        if market == 'AH':
            current_ah = features.get('current_ah')
            if current_ah is None:
                return False
            
            # Para Sistema Qwen v3: usar familias CON SIGNO
            if algorithm == 'QWEN_ML':
                # Determinar prefijo: H si local favorito, A si away favorito
                prefix = 'H' if current_ah >= 0 else 'A'
                match_ah_abs = abs(current_ah)
                
                if match_ah_abs < 0.01:
                    match_family = 'H0'  # Pick'em siempre es H0
                elif match_ah_abs <= 0.75:
                    match_family = f'{prefix}0_5'
                elif match_ah_abs <= 1.0:
                    match_family = f'{prefix}1_0'
                elif match_ah_abs <= 1.75:
                    match_family = f'{prefix}1_5'
                else:
                    match_family = f'{prefix}2_0_plus'
            else:
                # Para patrones v2 antiguos: usar solo valor absoluto
                match_ah_abs = abs(current_ah)
                
                if match_ah_abs < 0.01:
                    match_family = 'H0'
                elif match_ah_abs <= 0.75:
                    match_family = 'H0_5'
                elif match_ah_abs <= 1.0:
                    match_family = 'H1_0'
                elif match_ah_abs <= 1.75:
                    match_family = 'H1_25_1_75'
                else:
                    match_family = 'H2_0_plus'
            
            # El patrón SOLO aplica si su familia coincide exactamente
            if pattern_family_norm != match_family:
                return False
        else:
            current_ou = features.get('current_ou')
            if current_ou is None:
                return False
            
            # Mapear familia OU a rango
            ou_ranges = {
                '2_0': (1.75, 2.25),
                '2_25': (2.0, 2.5),
                '2_5': (2.25, 2.75),
                '2_75': (2.5, 3.0),
                '3_0_plus': (2.75, 10.0),
            }
            
            if pattern_family_norm in ou_ranges:
                min_ou, max_ou = ou_ranges[pattern_family_norm]
                if not (min_ou <= current_ou <= max_ou):
                    return False
        
        # Verificar condiciones
        conditions = pattern.get('conditions', [])
        
        for cond in conditions:
            try:
                if '==' in cond:
                    col, val = cond.split('==')
                    if str(features.get(col)) != val:
                        return False
                elif '>=' in cond:
                    col, val = cond.split('>=')
                    feat_val = features.get(col)
                    if feat_val is None or float(feat_val) < float(val):
                        return False
                elif '<=' in cond:
                    col, val = cond.split('<=')
                    feat_val = features.get(col)
                    if feat_val is None or float(feat_val) > float(val):
                        return False
                elif '>' in cond:
                    col, val = cond.split('>')
                    feat_val = features.get(col)
                    if feat_val is None or float(feat_val) <= float(val):
                        return False
                elif '<' in cond:
                    col, val = cond.split('<')
                    feat_val = features.get(col)
                    if feat_val is None or float(feat_val) >= float(val):
                        return False
                else:
                    # Booleano o señal de correlación
                    # Para patrones v8, las condiciones son señales como 'UNDERDOG_VALUE'
                    signal_value = features.get(cond, False)
                    if not signal_value:
                        return False
            except Exception:
                return False
        
        return True
    
    def _generate_correlation_signals(self, features: Dict) -> List[str]:
        """
        Genera señales de correlación basadas en lógica de handicap.
        Estas señales se usan para patrones Qwen v8.
        """
        signals = []
        
        # UNDERDOG_VALUE: Favorito NO cubrió H2H + línea igual o subió
        h2h_failed = features.get('H2H_Driver_Failed', False)
        h2h_line_higher = features.get('H2H_Line_Higher', False)
        tag_iguala = features.get('TAG_Iguala', False)
        if h2h_failed and (h2h_line_higher or tag_iguala):
            signals.append('UNDERDOG_VALUE')
        
        # DOMINANT_UNDERDOG: Triangulación favorable
        ind_strong = features.get('IND_Strong_DA', False)
        triang_dom = features.get('TRIANG_Home_Dom', False)
        prev_solid = features.get('PREV_Solid', False)
        if ind_strong and (triang_dom or prev_solid):
            signals.append('DOMINANT_UNDERDOG')
        
        # SMART_MONEY_AGAINST: Dinero va contra favorito
        money_away = features.get('movement_dir') == 'UP'
        exp_unreliable = features.get('EXP_Unreliable', False)
        if money_away and exp_unreliable:
            signals.append('SMART_MONEY_AGAINST')
        
        # FALSE_FAVORITE: Contexto contradice mercado
        away_bottom = features.get('CTX_Away_Bottom', False)
        home_top = features.get('CTX_Home_Top', False)
        current_ah = features.get('current_ah', 0)
        if (current_ah < 0 and away_bottom) or (current_ah > 0 and home_top and away_bottom):
            signals.append('FALSE_FAVORITE')
        
        # H2H_FAV_WINS
        if features.get('H2H_Driver_Covered', False):
            signals.append('H2H_FAV_WINS')
        
        # Otros
        if features.get('FALSE_LOSER', False):
            signals.append('FALSE_LOSER')
        if features.get('TRIGGER_Sniper', False):
            signals.append('SNIPER')
        
        return signals
    
    def evaluate_match(self, match: Dict) -> Dict[str, List[Dict]]:
        """
        Evalúa un partido y encuentra todos los patrones que aplican.
        
        Args:
            match: Dict con datos del partido (formato data_precacheo.json)
            
        Returns:
            Dict con {
                'ah_picks': [pick1, pick2, ...],
                'ou_picks': [pick1, pick2, ...]
            }
        """
        if not self.loaded:
            return {'ah_picks': [], 'ou_picks': []}
        
        # Construir features
        features = build_match_features(match)
        if not features:
            return {'ah_picks': [], 'ou_picks': []}
        
        features = discretize_features(features)
        
        # Las features booleanas ya vienen de discretize_features
        # Solo añadimos las señales de correlación adicionales
        correlation_signals = self._generate_correlation_signals(features)
        for signal in correlation_signals:
            features[signal] = True
        
        ah_picks = []
        ou_picks = []
        
        # Evaluar patrones AH
        for pattern in self.ah_patterns:
            if self.check_pattern_applies(features, pattern):
                pick = self._format_pick(pattern, features, 'AH')
                ah_picks.append(pick)
        
        # Evaluar patrones OU
        for pattern in self.ou_patterns:
            if self.check_pattern_applies(features, pattern):
                pick = self._format_pick(pattern, features, 'OU')
                ou_picks.append(pick)
        
        # Limitar a top picks
        ah_picks = ah_picks[:5]  # Max 5 picks AH
        ou_picks = ou_picks[:5]  # Max 5 picks OU
        
        return {
            'ah_picks': ah_picks,
            'ou_picks': ou_picks
        }
    
    def _format_pick(self, pattern: Dict, features: Dict, pick_type: str = 'AH') -> Dict:
        """
        Formatea un pick para mostrar en frontend.
        Soporta tanto formato v2 (stats) como formato especialista (train/test).
        """
        # Formato especialista: pattern['train'] y pattern['test']
        train_data = pattern.get('train', {})
        test_data = pattern.get('test', {})
        
        # Formato v2: pattern['stats']
        stats = pattern.get('stats', {})
        
        # Obtener valores (priorizar formato especialista)
        n_train = train_data.get('n', 0) or stats.get('n_train', 0)
        n_test = test_data.get('n', 0) or stats.get('n_test', 0)
        roi_train = train_data.get('roi', 0) or stats.get('roi_train', 0)
        roi_test = test_data.get('roi', 0) or stats.get('roi_test', 0)
        acc_train = train_data.get('accuracy', 0) or stats.get('accuracy_train', 0)
        acc_test = test_data.get('accuracy', 0) or stats.get('accuracy_test', 0)
        
        # Detectar algoritmo
        algorithm = pattern.get('algorithm', 'PATTERN_V2')
        
        # Generar nombre legible
        conditions = pattern.get('conditions', [])
        target = pattern.get('target', '')
        name = pattern.get('name', '')
        
        # Para Sistema Qwen, usar nombre descriptivo con perspectiva
        if algorithm == 'QWEN_ML':
            # Usar perspectiva del patrón si existe, sino calcular
            perspective = pattern.get('perspective', '')
            if not perspective:
                perspective = 'Local' if target == 'HOME' else 'Visitante'
            name = f"Sistema Qwen (ROI={int(roi_train*100)}%({n_train} muestras) {perspective})"
        elif not name and conditions:
            name = f"{target}: {' & '.join(conditions[:2])}"
            if len(conditions) > 2:
                name += f" +{len(conditions)-2}"
        
        return {
            'pick': pattern.get('pick', target),
            'pattern_id': pattern.get('pattern_id', ''),
            'pattern_name': name or 'Pattern',
            'confidence': 'HIGH',
            'roi': roi_train,
            'accuracy': acc_train,
            'n_samples': n_train,
            'algorithm': algorithm,
            'type': pick_type,
            'family': pattern.get('family', ''),
            'target': target,
            'market': pattern.get('market', 'AH'),
            'n_train': n_train,
            'n_test': n_test,
            'roi_train': roi_train,
            'roi_test': roi_test,
            'accuracy_train': acc_train,
            'accuracy_test': acc_test,
            'conditions_readable': conditions[:5],
            'requires': pattern.get('requires', []),
            'explanation': self._generate_explanation(pattern, features)
        }

    
    def _generate_explanation(self, pattern: Dict, features: Dict) -> str:
        """
        Genera una explicación corta del pick.
        """
        parts = []
        
        target = pattern.get('target', '')
        family = pattern.get('family', '')
        market = pattern.get('market', 'AH')
        
        if market == 'AH':
            parts.append(f"Patrón detectado: {target} en {family}")
        else:
            parts.append(f"Patrón O/U: {target} en {family}")
        
        # Agregar métricas clave
        stats = pattern.get('stats', {})
        roi_test = stats.get('roi_test', 0)
        n_test = stats.get('n_test', 0)
        
        if roi_test > 0 and n_test > 0:
            parts.append(f"ROI Test: {roi_test*100:.1f}% ({n_test} muestras)")
        
        # Agregar condiciones clave cumplidas
        conditions = pattern.get('conditions_readable', [])[:2]
        if conditions:
            parts.append(f"Condiciones: {', '.join(conditions)}")
        
        return ' | '.join(parts)
    
    def get_picks(self, match: Dict) -> List[Dict]:
        """
        Convenience method: retorna todos los picks combinados.
        """
        result = self.evaluate_match(match)
        return result['ah_picks'] + result['ou_picks']
    
    def get_best_ah_pick(self, match: Dict) -> Optional[Dict]:
        """
        Retorna el mejor pick AH para un partido.
        """
        result = self.evaluate_match(match)
        return result['ah_picks'][0] if result['ah_picks'] else None
    
    def get_best_ou_pick(self, match: Dict) -> Optional[Dict]:
        """
        Retorna el mejor pick O/U para un partido.
        """
        result = self.evaluate_match(match)
        return result['ou_picks'][0] if result['ou_picks'] else None


# Singleton para uso global
_loader_instance = None


def get_loader(patterns_dir: str = None) -> PrecacheoLoaderV2:
    """
    Obtiene instancia singleton del loader.
    """
    global _loader_instance
    
    if _loader_instance is None:
        if patterns_dir is None:
            # Default path
            patterns_dir = str(Path(__file__).parent.parent.parent / 'data' / 'patterns_v2')
        _loader_instance = PrecacheoLoaderV2(patterns_dir)
    
    return _loader_instance


def reload_loader() -> bool:
    """
    Fuerza recarga del loader singleton.
    Útil cuando se han añadido nuevos patrones.
    """
    global _loader_instance
    
    if _loader_instance is not None:
        return _loader_instance.reload()
    else:
        # Crear nueva instancia con recarga
        get_loader()
        return True


loader = get_loader()
