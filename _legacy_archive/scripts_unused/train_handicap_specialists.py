#!/usr/bin/env python
"""
Train Handicap Specialists - Entrena un modelo experto por cada línea de handicap.

Criterios:
- Min ROI Test: 20%
- Min Samples Test: 15
"""

import sys
import os
import json
from pathlib import Path
from datetime import datetime

# Agregar path del proyecto
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.pattern_miner_v2.features_builder_v2 import (
    load_all_training_data, 
    build_training_dataframe
)
from scripts.pattern_miner_v2.rule_miner import PatternMinerV2

def main():
    print("""
    ==================================================================
            ENTRENAMIENTO DE EXPERTOS POR HANDICAP (v3)               
                                                                  
      Criterios:                                                      
      - ROI Mínimo (Test): 20%                                        
      - Muestras Mínimas: 15                                          
      - Estrategia: Un experto por línea de handicap                  
    ==================================================================
    """)
    
    data_dir = str(PROJECT_ROOT / 'data')
    output_dir = str(PROJECT_ROOT / 'data' / 'patterns_v2')
    
    # 1. Cargar Datos
    print("[INFO] Cargando datos...")
    matches = load_all_training_data(data_dir)
    df = build_training_dataframe(matches)
    print(f"[OK] Datos cargados: {len(df)} partidos")
    
    # 2. Identificar Familias de Handicap
    if 'ah_family' not in df.columns:
        print("[ERROR] No se encontró columna ah_family")
        return 1
        
    families = sorted(df['ah_family'].unique())
    print(f"[INFO] Handicaps encontrados: {len(families)}")
    print(f"   {', '.join(families)}")
    
    # Configuración estricta
    config = {
        'min_samples': 15,          # Muestras totales mínimas
        'min_samples_test': 15,     # Muestras en test mínimas
        'min_accuracy': 0.60,       # Accuracy base razonable
        'min_roi_train': 0.20,      # ROI Train 20%
        'min_roi_oos': 0.20,        # ROI Test 20%
        'generations': 10000,       # 10k gen por handicap es suficiente (espacio búsqueda reducido)
        'min_features': 2,
        'max_features': 5
    }
    
    total_patterns = 0
    
    # 3. Iterar por cada handicap
    for family in families:
        print(f"\n" + "="*50)
        print(f"[TARGET] Entrenando Experto: {family}")
        print("="*50)
        
        # Filtrar datos solo para esta familia
        family_df = df[df['ah_family'] == family].copy()
        
        if len(family_df) < 50:
            print(f"[WARN] Pocos datos para {family} ({len(family_df)} partidos). Saltando.")
            continue
            
        print(f"[STATS] Partidos: {len(family_df)}")
        
        # Instanciar minador
        miner = PatternMinerV2(config)
        
        # Minar (solo AH)
        miner.mine_ah_patterns(family_df, generations=config['generations'])
        
        # Filtrar patrones válidos (ROI Test >= 20%)
        valid_patterns = [p for p in miner.ah_patterns if p['test']['roi'] >= config['min_roi_oos']]
        
        print(f"[RESULT] Patrones encontrados para {family}: {len(valid_patterns)}")
        
        if valid_patterns:
            # Guardar archivo del experto
            filename = f"specialist_ah_{family.replace('.', '_')}.json"
            filepath = os.path.join(output_dir, filename)
            
            output_data = {
                'id': f"specialist_{family}",
                'market': 'AH',
                'line': family,
                'criteria': "ROI>=20%, N>=15",
                'patterns': valid_patterns,
                'generated': datetime.now().isoformat()
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, default=str)
                
            print(f"[SAVED] Guardado: {filename}")
            total_patterns += len(valid_patterns)
        else:
            print(f"[FAIL] Ningún patrón cumplió los criterios estrictos para {family}")

    # =========================================================================
    # 4. Iterar por cada línea de OVER/UNDER
    # =========================================================================
    if 'ou_family' in df.columns:
        ou_families = sorted(df['ou_family'].unique())
        print(f"\n[INFO] Líneas O/U encontradas: {len(ou_families)}")
        print(f"   {', '.join(ou_families)}")
        
        for family in ou_families:
            print(f"\n" + "="*50)
            print(f"[TARGET] Entrenando Experto O/U: {family}")
            print("="*50)
            
            # Filtrar datos solo para esta familia
            family_df = df[df['ou_family'] == family].copy()
            
            if len(family_df) < 50:
                print(f"[WARN] Pocos datos para {family} ({len(family_df)} partidos). Saltando.")
                continue
                
            print(f"[STATS] Partidos: {len(family_df)}")
            
            miner = PatternMinerV2(config)
            
            # Minar patrones O/U
            miner.mine_ou_patterns(family_df, generations=config['generations'])
            
            # Filtrar patrones válidos (ROI Test >= 20%)
            valid_patterns = [p for p in miner.ou_patterns if p['test']['roi'] >= config['min_roi_oos']]
            
            print(f"[RESULT] Patrones encontrados para {family}: {len(valid_patterns)}")
            
            if valid_patterns:
                filename = f"specialist_ou_{family.replace('.', '_')}.json"
                filepath = os.path.join(output_dir, filename)
                
                output_data = {
                    'id': f"specialist_ou_{family}",
                    'market': 'OU',
                    'line': family,
                    'criteria': "ROI>=20%, N>=15",
                    'patterns': valid_patterns,
                    'generated': datetime.now().isoformat()
                }
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(output_data, f, indent=2, default=str)
                    
                print(f"[SAVED] Guardado: {filename}")
                total_patterns += len(valid_patterns)
            else:
                print(f"[FAIL] Ningún patrón cumplió los criterios estrictos para {family}")


    print("\n" + "="*60)
    print("✅ ENTRENAMIENTO FINALIZADO")
    print(f"Total patrones de alta calidad: {total_patterns}")
    print("="*60)

if __name__ == "__main__":
    main()
