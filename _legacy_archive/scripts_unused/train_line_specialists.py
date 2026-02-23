#!/usr/bin/env python
"""
Train Line Specialists - Entrena un modelo experto por cada línea ESPECÍFICA.

Entrena patrones para:
- Cada línea de Asian Handicap (-2.5, -2.0, -1.5, -1.0, -0.75, -0.5, -0.25, 0, +0.25, +0.5, +0.75, +1.0, etc.)
- Cada línea de Over/Under (2.0, 2.25, 2.5, 2.75, 3.0, 3.5, etc.)

Criterios:
- Min ROI Test: 20%
- Min Samples Test: 15

GUARDADO INCREMENTAL: Guarda patrones inmediatamente después de cada línea.
"""

import sys
import os
import json
from pathlib import Path
from datetime import datetime
from collections import Counter

# Agregar path del proyecto
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.pattern_miner_v2.features_builder_v2 import (
    load_all_training_data, 
    build_training_dataframe
)
from scripts.pattern_miner_v2.rule_miner import PatternMinerV2

# Configuracion
MIN_SAMPLES_TEST = 15
MIN_ROI_TEST = 0.20  # 20%
MIN_MATCHES_PER_LINE = 50  # Minimo de partidos para entrenar una linea
GENERATIONS = 5000  # Reducido para velocidad


def get_ah_line_key(ah_value) -> str:
    """Convierte valor AH o familia a key de archivo."""
    # Si es string (familia como 'H0.5'), limpiar
    if isinstance(ah_value, str):
        return ah_value.replace('.', '_').replace('+', 'plus').replace('-', 'minus')
    # Si es numero
    if ah_value == 0:
        return "0"
    sign = "plus" if ah_value > 0 else "minus"
    abs_val = str(abs(ah_value)).replace('.', '_')
    return f"{sign}_{abs_val}"


def get_ou_line_key(ou_value) -> str:
    """Convierte valor OU a key de archivo (ej: 2.5 -> '2_5')."""
    return str(ou_value).replace('.', '_')


def save_specialist_patterns(patterns: list, line_type: str, line_value, output_dir: Path):
    """
    Guarda patrones de un especialista en un archivo JSON.
    
    Args:
        patterns: Lista de patrones validados
        line_type: 'AH' o 'OU'
        line_value: Valor de la linea (ej: -0.5, 2.5) o familia (ej: 'H0.5')
        output_dir: Directorio de salida
    """
    if line_type == 'AH':
        line_key = get_ah_line_key(line_value)
        filename = f"specialist_ah_{line_key}.json"
    else:
        line_key = get_ou_line_key(line_value)
        filename = f"specialist_ou_{line_key}.json"
    
    filepath = output_dir / filename
    
    # Formatear para compatibilidad con precacheo_loader
    output_data = {
        'id': f"specialist_{line_type.lower()}_{line_key}",
        'market': line_type,
        'line': line_value,
        'line_key': line_key,
        'criteria': f"ROI>={MIN_ROI_TEST*100:.0f}%, N>={MIN_SAMPLES_TEST}",
        'patterns': patterns,
        'count': len(patterns),
        'generated': datetime.now().isoformat()
    }
    
    # Crear directorio si no existe
    output_dir.mkdir(parents=True, exist_ok=True)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, default=str, ensure_ascii=False)
    
    print(f"    [SAVED] {filename} ({len(patterns)} patrones)")
    return filepath


def train_ah_specialists(df, output_dir: Path) -> int:
    """Entrena especialistas para cada FAMILIA de AH (H0, H0.5, H1.0, etc.)."""
    
    print("\n" + "="*60)
    print("ENTRENANDO ESPECIALISTAS POR FAMILIA DE ASIAN HANDICAP")
    print("="*60)
    
    # Obtener familias unicas de ah_family
    if 'ah_family' not in df.columns:
        print("[ERROR] No se encontro columna ah_family")
        return 0
    
    # Contar partidos por familia
    ah_counts = df['ah_family'].value_counts().sort_index()
    
    print(f"\n[INFO] Familias AH encontradas: {len(ah_counts)}")
    print(f"[INFO] Familias con >={MIN_MATCHES_PER_LINE} partidos:")
    
    valid_families = []
    for family, count in ah_counts.items():
        if count >= MIN_MATCHES_PER_LINE and family is not None:
            print(f"   {family}: {count} partidos")
            valid_families.append(family)
    
    total_patterns = 0
    
    # Configuracion del minador
    config = {
        'min_samples': MIN_SAMPLES_TEST,
        'min_samples_test': MIN_SAMPLES_TEST,
        'min_accuracy': 0.55,
        'min_roi_train': 0.15,
        'min_roi_oos': MIN_ROI_TEST,
        'generations': GENERATIONS,
        'min_features': 2,
        'max_features': 5,
        'max_degradation': 0.5  # Permitir hasta 50% degradacion
    }
    
    # Entrenar por cada familia
    for family in sorted(valid_families):
        print(f"\n{'-'*50}")
        print(f"[TARGET] Entrenando AH {family}")
        print(f"{'-'*50}")
        
        # Filtrar datos para esta familia
        family_df = df[df['ah_family'] == family].copy()
        print(f"[STATS] Partidos: {len(family_df)}")
        
        # Instanciar minador
        miner = PatternMinerV2(config)
        
        try:
            # Minar patrones AH
            miner.mine_ah_patterns(family_df, generations=GENERATIONS)
            
            # Filtrar patrones validos
            valid_patterns = []
            for p in miner.ah_patterns:
                test_stats = p.get('test', {})
                if (test_stats.get('roi', 0) >= MIN_ROI_TEST and 
                    test_stats.get('n', 0) >= MIN_SAMPLES_TEST):
                    # Anadir metadata de familia
                    p['family'] = family
                    p['market'] = 'AH'
                    valid_patterns.append(p)
            
            print(f"[RESULT] Patrones validos: {len(valid_patterns)}")
            
            if valid_patterns:
                # GUARDAR INMEDIATAMENTE
                # Usar familia como "linea" conceptual
                family_key = family.replace('.', '_').replace('+', 'plus').replace('-', 'minus')
                save_specialist_patterns(valid_patterns, 'AH', family, output_dir)
                total_patterns += len(valid_patterns)
                
                # Mostrar top 3
                sorted_patterns = sorted(valid_patterns, key=lambda x: -x.get('test', {}).get('roi', 0))
                print("    Top 3:")
                for i, p in enumerate(sorted_patterns[:3], 1):
                    roi = p.get('test', {}).get('roi', 0) * 100
                    n = p.get('test', {}).get('n', 0)
                    print(f"      {i}. ROI: {roi:.1f}% (N={n})")
            else:
                print(f"[SKIP] Sin patrones validos para AH {family}")
                
        except Exception as e:
            print(f"[ERROR] Error entrenando AH {family}: {e}")
    
    return total_patterns


def train_ou_specialists(df, output_dir: Path) -> int:
    """Entrena especialistas para cada línea de O/U."""
    
    print("\n" + "="*60)
    print("ENTRENANDO ESPECIALISTAS POR LÍNEA DE OVER/UNDER")
    print("="*60)
    
    # Obtener líneas únicas de current_ou
    if 'current_ou' not in df.columns:
        print("[ERROR] No se encontró columna current_ou")
        return 0
    
    # Contar partidos por línea exacta
    ou_counts = df['current_ou'].value_counts().sort_index()
    
    print(f"\n[INFO] Líneas O/U encontradas: {len(ou_counts)}")
    print(f"[INFO] Líneas con >={MIN_MATCHES_PER_LINE} partidos:")
    
    valid_lines = []
    for line, count in ou_counts.items():
        if count >= MIN_MATCHES_PER_LINE and line is not None:
            print(f"   O/U {line:.2f}: {count} partidos")
            valid_lines.append(line)
    
    total_patterns = 0
    
    # Configuración del minador
    config = {
        'min_samples': MIN_SAMPLES_TEST,
        'min_samples_test': MIN_SAMPLES_TEST,
        'min_accuracy': 0.55,
        'min_roi_train': 0.15,
        'min_roi_oos': MIN_ROI_TEST,
        'generations': GENERATIONS,
        'min_features': 2,
        'max_features': 5
    }
    
    # Entrenar por cada línea
    for line in sorted(valid_lines):
        print(f"\n{'-'*50}")
        print(f"[TARGET] Entrenando O/U {line:.2f}")
        print(f"{'-'*50}")
        
        # Filtrar datos para esta línea
        line_df = df[df['current_ou'] == line].copy()
        print(f"[STATS] Partidos: {len(line_df)}")
        
        # Instanciar minador
        miner = PatternMinerV2(config)
        
        try:
            # Minar patrones O/U
            miner.mine_ou_patterns(line_df, generations=GENERATIONS)
            
            # Filtrar patrones válidos
            valid_patterns = []
            for p in miner.ou_patterns:
                test_stats = p.get('test', {})
                if (test_stats.get('roi', 0) >= MIN_ROI_TEST and 
                    test_stats.get('n', 0) >= MIN_SAMPLES_TEST):
                    # Añadir metadata de línea
                    p['line'] = line
                    p['market'] = 'OU'
                    valid_patterns.append(p)
            
            print(f"[RESULT] Patrones válidos: {len(valid_patterns)}")
            
            if valid_patterns:
                # GUARDAR INMEDIATAMENTE
                save_specialist_patterns(valid_patterns, 'OU', line, output_dir)
                total_patterns += len(valid_patterns)
                
                # Mostrar top 3
                sorted_patterns = sorted(valid_patterns, key=lambda x: -x.get('test', {}).get('roi', 0))
                print("    Top 3:")
                for i, p in enumerate(sorted_patterns[:3], 1):
                    roi = p.get('test', {}).get('roi', 0) * 100
                    n = p.get('test', {}).get('n', 0)
                    print(f"      {i}. ROI: {roi:.1f}% (N={n})")
            else:
                print(f"[SKIP] Sin patrones válidos para O/U {line:.2f}")
                
        except Exception as e:
            print(f"[ERROR] Error entrenando O/U {line:.2f}: {e}")
    
    return total_patterns


def main():
    print("""
    ==============================================================
             ENTRENAMIENTO DE ESPECIALISTAS POR LINEA (v1)               
                                                                  
      Criterios:                                                      
      - ROI Minimo (Test): 20%                                        
      - Muestras Minimas: 15                                          
      - Estrategia: Un experto por linea ESPECIFICA                  
                                                                      
      >> GUARDADO INCREMENTAL: Cada linea se guarda inmediatamente    
    ==============================================================
    """)
    
    data_dir = str(PROJECT_ROOT / 'data')
    output_dir = PROJECT_ROOT / 'data' / 'patterns_v2'
    
    # Crear directorio de salida
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Cargar Datos
    print("[STEP 1] Cargando datos...")
    matches = load_all_training_data(data_dir)
    
    if len(matches) < 100:
        print(f"[ERROR] Muy pocos partidos: {len(matches)}")
        return 1
    
    # 2. Construir DataFrame
    print("\n[STEP 2] Construyendo features...")
    df = build_training_dataframe(matches)
    print(f"[OK] DataFrame listo: {len(df)} partidos con resultado")
    
    # 3. Entrenar especialistas AH
    ah_patterns = train_ah_specialists(df, output_dir)
    
    # 4. Entrenar especialistas O/U
    ou_patterns = train_ou_specialists(df, output_dir)
    
    # Resumen final
    print("\n" + "="*60)
    print("[OK] ENTRENAMIENTO FINALIZADO")
    print("="*60)
    print(f"   Patrones AH: {ah_patterns}")
    print(f"   Patrones O/U: {ou_patterns}")
    print(f"   TOTAL: {ah_patterns + ou_patterns}")
    print(f"\n   Guardados en: {output_dir}")
    print("="*60)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
