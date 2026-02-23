"""
Pattern Exporter - Formato profesional y exportación por batches

Genera:
- Nombres descriptivos para patrones
- Formato JSON profesional con metadata
- Exportación en lotes de 20 patrones
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any


def generate_pattern_name(pattern: Dict) -> str:
    """
    Genera un nombre descriptivo para el patrón.
    
    Formato: {TYPE}_{FAMILY}_{TARGET}_{KEY_FEATURES}_{ODDS}
    
    Ejemplo: AH_H05_HOME_DOMEXP_BRIDGE_180
    """
    ptype = pattern.get('type', 'AH')
    family = pattern.get('family', 'H0').replace('.', '').replace('_', '')
    target = pattern.get('target', 'HOME')[:4].upper()
    
    # Extraer features clave de las condiciones
    conditions = pattern.get('conditions', [])
    
    key_parts = []
    has_exp = any('exp' in c.lower() for c in conditions)
    has_dom = any('dom' in c.lower() or 'dsot' in c.lower() or 'dda' in c.lower() for c in conditions)
    has_bridge = any('bridge' in c.lower() for c in conditions)
    has_mov = any('movement' in c.lower() or 'delta' in c.lower() for c in conditions)
    has_rank = any('rank' in c.lower() for c in conditions)
    
    if has_exp:
        key_parts.append('EXP')
    if has_dom:
        key_parts.append('DOM')
    if has_bridge:
        key_parts.append('BRG')
    if has_mov:
        key_parts.append('MOV')
    if has_rank:
        key_parts.append('RNK')
    
    if not key_parts:
        key_parts.append('MIX')
    
    features_str = ''.join(key_parts[:3])  # Max 3 partes
    
    # Agregar indicador de ROI (180 = 1.80 odds target)
    odds_str = '180'
    
    name = f"{ptype}_{family}_{target}_{features_str}_{odds_str}"
    
    return name


def generate_pattern_id(pattern: Dict, index: int) -> str:
    """
    Genera un ID único para el patrón.
    """
    ptype = pattern.get('type', 'AH')
    family = pattern.get('family', 'H0').replace('.', '').replace('_', '')
    timestamp = datetime.now().strftime('%Y%m%d')
    
    return f"{ptype}_{family}_{timestamp}_{index:04d}"


def format_conditions_readable(conditions: List[str]) -> List[str]:
    """
    Formatea condiciones en formato más legible.
    """
    readable = []
    
    for cond in conditions:
        # Traducir nombres de columnas
        cond_readable = cond
        
        translations = {
            'prev_home': 'PrevHome',
            'prev_away': 'PrevAway',
            'h2h_stadium': 'H2H_Est',
            'h2h_general': 'H2H_Gen',
            'h2h_col3': 'H2H_Col3',
            'ind_left': 'IndLocal',
            'ind_right': 'IndVisita',
            'exp_': 'Exp:',
            'dom_': 'Dom:',
            'bridge_': 'Puente:',
            'dSOT': 'dSOT',
            'dDA': 'dDA',
            '_bin': '',
            '==': '=',
            '>=': '≥',
            '<=': '≤'
        }
        
        for old, new in translations.items():
            cond_readable = cond_readable.replace(old, new)
        
        readable.append(cond_readable)
    
    return readable


def export_pattern(pattern: Dict, index: int) -> Dict:
    """
    Exporta un patrón en formato profesional.
    
    Returns:
        Dict con formato:
        {
            pattern_id: str,
            name: str,
            market: 'AH' | 'OU',
            family: str,
            pick: str,
            conditions: list,
            conditions_readable: list,
            requires: list (gates),
            stats: {
                n_train, n_test,
                roi_train, roi_test,
                accuracy_train, accuracy_test,
                avg_odds,
                breakdown
            },
            generated: str
        }
    """
    pattern_id = generate_pattern_id(pattern, index)
    name = generate_pattern_name(pattern)
    
    # Determinar pick
    ptype = pattern.get('type', 'AH')
    target = pattern.get('target', '')
    family = pattern.get('family', '')
    
    if ptype == 'AH':
        pick = f"{target}_AH_{family}"
    else:
        pick = f"{target}_{family}"
    
    # Extraer requirements (gates implícitos)
    conditions = pattern.get('conditions', [])
    requires = []
    
    if any('da_ok' in c.lower() or 'dda' in c.lower() for c in conditions):
        requires.append('DA_ok >= 1')
    if any('bridge' in c.lower() for c in conditions):
        requires.append('bridge_count >= 1')
    
    # Stats
    train = pattern.get('train', {})
    test = pattern.get('test', {})
    
    stats = {
        'n_train': train.get('n', 0),
        'n_test': test.get('n', 0),
        'accuracy_train': train.get('accuracy', 0),
        'accuracy_test': test.get('accuracy', 0),
        'roi_train': train.get('roi', 0),
        'roi_test': test.get('roi', 0),
        'avg_odds': 1.80,  # Default
        'breakdown': train.get('breakdown', {})
    }
    
    return {
        'pattern_id': pattern_id,
        'name': name,
        'market': ptype,
        'family': family,
        'pick': pick,
        'conditions': conditions,
        'conditions_readable': format_conditions_readable(conditions),
        'requires': requires,
        'stats': stats,
        'generated': datetime.now().isoformat()
    }


def export_patterns_batch(
    patterns: List[Dict],
    output_dir: str,
    batch_size: int = 20,
    prefix: str = 'patterns_v2'
) -> List[str]:
    """
    Exporta patrones en lotes.
    
    Args:
        patterns: Lista de patrones
        output_dir: Directorio de salida
        batch_size: Tamaño de cada lote (default: 20)
        prefix: Prefijo para archivos
        
    Returns:
        Lista de paths a archivos creados
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    files_created = []
    
    for batch_idx in range(0, len(patterns), batch_size):
        batch = patterns[batch_idx:batch_idx + batch_size]
        batch_num = (batch_idx // batch_size) + 1
        
        # Exportar cada patrón del batch
        exported = []
        for i, pattern in enumerate(batch):
            global_idx = batch_idx + i + 1
            exported.append(export_pattern(pattern, global_idx))
        
        # Guardar batch
        filename = f"{prefix}_{batch_num:03d}.json"
        filepath = output_path / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump({
                'batch': batch_num,
                'count': len(exported),
                'generated': datetime.now().isoformat(),
                'patterns': exported
            }, f, indent=2, ensure_ascii=False)
        
        files_created.append(str(filepath))
        print(f"  📦 Batch {batch_num}: {len(exported)} patrones -> {filename}")
    
    return files_created


def export_all_patterns(
    ah_patterns: List[Dict],
    ou_patterns: List[Dict],
    output_dir: str,
    batch_size: int = 20
) -> Dict[str, List[str]]:
    """
    Exporta todos los patrones (AH y OU) en batches separados.
    """
    print(f"\n📤 Exportando patrones a {output_dir}...")
    
    ah_files = export_patterns_batch(
        ah_patterns, output_dir, batch_size, 'patterns_v2_AH'
    )
    
    ou_files = export_patterns_batch(
        ou_patterns, output_dir, batch_size, 'patterns_v2_OU'
    )
    
    # Crear archivo índice
    index = {
        'generated': datetime.now().isoformat(),
        'total_ah': len(ah_patterns),
        'total_ou': len(ou_patterns),
        'ah_files': [Path(f).name for f in ah_files],
        'ou_files': [Path(f).name for f in ou_files]
    }
    
    index_path = Path(output_dir) / 'patterns_v2_index.json'
    with open(index_path, 'w', encoding='utf-8') as f:
        json.dump(index, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ Exportación completada:")
    print(f"   AH: {len(ah_files)} archivos ({len(ah_patterns)} patrones)")
    print(f"   OU: {len(ou_files)} archivos ({len(ou_patterns)} patrones)")
    print(f"   Índice: {index_path}")
    
    return {
        'ah_files': ah_files,
        'ou_files': ou_files,
        'index': str(index_path)
    }


def clean_old_patterns(output_dir: str, pattern: str = 'patterns_v2*.json'):
    """
    Elimina patrones antiguos antes de generar nuevos.
    """
    output_path = Path(output_dir)
    
    if not output_path.exists():
        return
    
    old_files = list(output_path.glob(pattern))
    
    for f in old_files:
        try:
            f.unlink()
            print(f"  🗑️ Eliminado: {f.name}")
        except Exception as e:
            print(f"  ⚠️ Error eliminando {f.name}: {e}")
    
    print(f"Limpiados {len(old_files)} archivos antiguos")
