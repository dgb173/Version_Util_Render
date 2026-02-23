#!/usr/bin/env python
"""
Train Patterns v2 - Script Principal de Entrenamiento

Este script:
1. Carga TODOS los datos de training (combina todos los JSON)
2. Construye features con el nuevo builder v2
3. Mina patrones con validación temporal
4. Exporta en batches de 20
5. Actualiza /precacheo con los nuevos patrones

Uso:
    py scripts/train_patterns_v2.py
    py scripts/train_patterns_v2.py --generations 100000
    py scripts/train_patterns_v2.py --clean  # Limpia patrones antiguos primero
"""

import sys
import os
import argparse
from pathlib import Path
from datetime import datetime

# Agregar path del proyecto
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Imports del módulo
from scripts.pattern_miner_v2.features_builder_v2 import (
    load_all_training_data, 
    build_training_dataframe
)
from scripts.pattern_miner_v2.rule_miner import PatternMinerV2
from scripts.pattern_miner_v2.pattern_exporter import (
    export_all_patterns,
    clean_old_patterns
)


def parse_args():
    parser = argparse.ArgumentParser(description='Train PatternMiner v2')
    parser.add_argument('--generations', type=int, default=50000,
                        help='Número de generaciones por target (default: 50000)')
    parser.add_argument('--min-samples', type=int, default=25,
                        help='Muestras mínimas por patrón (default: 25)')
    parser.add_argument('--min-accuracy', type=float, default=0.70,
                        help='Accuracy mínima (default: 0.70)')
    parser.add_argument('--min-roi-train', type=float, default=0.15,
                        help='ROI mínimo en train (default: 0.15)')
    parser.add_argument('--min-roi-test', type=float, default=0.05,
                        help='ROI mínimo en test (default: 0.05)')
    parser.add_argument('--batch-size', type=int, default=20,
                        help='Tamaño de batch para exportación (default: 20)')
    parser.add_argument('--clean', action='store_true',
                        help='Limpiar patrones antiguos antes de generar')
    parser.add_argument('--data-dir', type=str, default=None,
                        help='Directorio de datos (default: data/)')
    parser.add_argument('--output-dir', type=str, default=None,
                        help='Directorio de salida (default: data/patterns_v2/)')
    return parser.parse_args()


def main():
    args = parse_args()
    
    print("""
    ╔══════════════════════════════════════════════════════════════════╗
    ║            PATTERN MINER v2 - ENTRENAMIENTO                      ║
    ║                                                                  ║
    ║  Features:                                                       ║
    ║  - Expectativas del mercado (handicaps previos)                  ║
    ║  - Dominancia real (dSOT, dDA)                                   ║
    ║  - Puentes comparativos (M_gap vs D_gap)                         ║
    ║  - ROI real con liquidación asiática                             ║
    ║  - Validación temporal (train/test split)                        ║
    ╚══════════════════════════════════════════════════════════════════╝
    """)
    
    # Configurar paths
    data_dir = args.data_dir or str(PROJECT_ROOT / 'data')
    output_dir = args.output_dir or str(PROJECT_ROOT / 'data' / 'patterns_v2')
    
    print(f"📂 Data dir: {data_dir}")
    print(f"📂 Output dir: {output_dir}")
    print(f"🔄 Generaciones: {args.generations}")
    print(f"📊 Min samples: {args.min_samples}")
    print(f"🎯 Min accuracy: {args.min_accuracy}")
    print(f"💰 Min ROI train: {args.min_roi_train}")
    print(f"💰 Min ROI test: {args.min_roi_test}")
    
    # Limpiar patrones antiguos si se solicita
    if args.clean:
        print("\n🗑️ Limpiando patrones antiguos...")
        clean_old_patterns(output_dir)
    
    # Paso 1: Cargar todos los datos
    print("\n" + "="*60)
    print("PASO 1: Cargando datos de entrenamiento...")
    print("="*60)
    
    matches = load_all_training_data(data_dir)
    
    if len(matches) < 100:
        print("❌ Muy pocos partidos para entrenar")
        return 1
    
    # Paso 2: Construir features
    print("\n" + "="*60)
    print("PASO 2: Construyendo features v2...")
    print("="*60)
    
    df = build_training_dataframe(matches)
    
    print(f"\n📊 DataFrame final:")
    print(f"   Partidos: {len(df)}")
    print(f"   Features: {len(df.columns)}")
    
    # Mostrar distribución por familia
    if 'ah_family' in df.columns:
        print(f"\n   Familias AH:")
        for fam, count in df['ah_family'].value_counts().items():
            print(f"     {fam}: {count}")
    
    if 'ou_family' in df.columns:
        print(f"\n   Familias O/U:")
        for fam, count in df['ou_family'].value_counts().items():
            print(f"     {fam}: {count}")
    
    # Paso 3: Minar patrones
    print("\n" + "="*60)
    print("PASO 3: Minando patrones...")
    print("="*60)
    
    config = {
        'min_samples': args.min_samples,
        'min_accuracy': args.min_accuracy,
        'min_roi_train': args.min_roi_train,
        'min_roi_oos': args.min_roi_test,
        'generations': args.generations
    }
    
    miner = PatternMinerV2(config)
    
    start_time = datetime.now()
    
    try:
        result = miner.mine_all(df, args.generations)
    except KeyboardInterrupt:
        print("\n\n⚠️ Interrupción detectada, guardando progreso...")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print(f"\n⏱️ Tiempo de entrenamiento: {duration/60:.1f} minutos")
    
    # Filtrar por ROI de test
    ah_patterns = [p for p in miner.ah_patterns if p['test']['roi'] >= args.min_roi_test]
    ou_patterns = [p for p in miner.ou_patterns if p['test']['roi'] >= args.min_roi_test]
    
    print(f"\n📈 Patrones que pasan filtro ROI test >= {args.min_roi_test}:")
    print(f"   AH: {len(ah_patterns)} de {len(miner.ah_patterns)}")
    print(f"   OU: {len(ou_patterns)} de {len(miner.ou_patterns)}")
    
    # Paso 4: Exportar
    print("\n" + "="*60)
    print("PASO 4: Exportando patrones...")
    print("="*60)
    
    if len(ah_patterns) + len(ou_patterns) == 0:
        print("⚠️ No hay patrones que cumplan los criterios de ROI")
        # Guardar de todos modos para debug
        miner.save(str(Path(output_dir).parent / 'patterns_v2_all.json'))
        return 0
    
    export_all_patterns(
        ah_patterns,
        ou_patterns,
        output_dir,
        batch_size=args.batch_size
    )
    
    # Guardar también versión completa
    miner.save(str(Path(output_dir) / 'patterns_v2_complete.json'))
    
    # Paso 5: Mostrar top patrones
    print("\n" + "="*60)
    print("TOP PATRONES ENCONTRADOS")
    print("="*60)
    
    miner.print_top(10)
    
    print("\n" + "="*60)
    print("✅ ENTRENAMIENTO COMPLETADO")
    print("="*60)
    print(f"\nPatrones guardados en: {output_dir}")
    print(f"Para usar en /precacheo, los patrones se cargarán automáticamente.")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
