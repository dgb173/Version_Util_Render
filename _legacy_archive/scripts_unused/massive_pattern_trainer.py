"""
Massive Pattern Trainer
"""
print("DEBUG: Script iniciando...")
import sys

# Descripción del script
# 1. Extracción de features (features_builder.py)
# 2. Generación de reglas (rule_miner.py)
# 3. Exporta top_rules.json para uso en producción

import sys
from pathlib import Path

# Agregar path del proyecto
sys.path.insert(0, str(Path(__file__).parent.parent))

# Force UTF-8 output for Windows console
# if sys.platform == 'win32':
#     sys.stdout.reconfigure(encoding='utf-8')

from scripts.features_builder import build_features_dataframe, discretize_features
from scripts.rule_miner import (
    generate_combinatorial_rules,
    generate_tree_rules,
    filter_and_rank_rules,
    assign_creative_names,
    save_rules_to_json,
    generate_report
)


def main():
    """Pipeline completo de entrenamiento de reglas."""
    print("=" * 60, flush=True)
    print("🚀 MASSIVE PATTERN TRAINER - Sistema de Reglas AH/OU", flush=True)
    print("=" * 60, flush=True)
    
    base_path = Path(__file__).parent.parent
    
    # Buscar archivo de datos
    data_files = [
        base_path / 'training_data_1465.json',
        base_path / 'validation_data_39_2024-2025.json',
    ]
    
    json_path = None
    for df in data_files:
        if df.exists():
            json_path = df
            break
    
    if not json_path:
        print("❌ No se encontró archivo de training data", flush=True)
        return
    
    print(f"\n📂 Usando datos de: {json_path.name}", flush=True)
    
    # Fase 1: Extraer features
    print("\n" + "=" * 40, flush=True)
    print("📊 FASE 1: Extracción de Features", flush=True)
    print("=" * 40, flush=True)
    
    df = build_features_dataframe(str(json_path))
    df = discretize_features(df)
    
    # Fase 2: Generar reglas
    print("\n" + "=" * 40, flush=True)
    print("🔧 FASE 2: Generación de Reglas", flush=True)
    print("=" * 40, flush=True)
    
    rules = []
    # Usar max_conditions=3 para generar volumen suficiente
    rules.extend(generate_combinatorial_rules(df, max_conditions=3))
    # rules.extend(generate_tree_rules(df)) # Disabled for stability check
    
    print(f"📝 Total reglas candidatas: {len(rules)}", flush=True)
    
    # Fase 3: Filtrar y rankear
    print("\n" + "=" * 40, flush=True)
    print("🎯 FASE 3: Filtrado de Calidad", flush=True)
    print("=" * 40, flush=True)
    
    # Umbral más bajo inicial para garantizar volumen
    print("   Filtrando con min_samples=20, min_accuracy=0.75...", flush=True)
    valid_rules = filter_and_rank_rules(rules, df, min_samples=20, min_accuracy=0.75)
    
    if len(valid_rules) < 200:
        print(f"\n⚠️ Pocas reglas ({len(valid_rules)}), bajando a 70%...", flush=True)
        valid_rules = filter_and_rank_rules(rules, df, min_samples=20, min_accuracy=0.70)
        
    if len(valid_rules) < 200:
        print(f"\n⚠️ Bajando muestras a 15 y accuracy a 65%...", flush=True)
        valid_rules = filter_and_rank_rules(rules, df, min_samples=15, min_accuracy=0.65)
    
    if len(valid_rules) == 0:
        print("\n❌ No se encontraron reglas que pasen los filtros", flush=True)
        return
    
    # Asignar nombres creativos
    valid_rules = assign_creative_names(valid_rules)
    
    # Fase 4: Guardar output
    print("\n" + "=" * 40, flush=True)
    print("💾 FASE 4: Guardando Resultados", flush=True)
    print("=" * 40, flush=True)
    
    output_dir = base_path / 'models'
    output_dir.mkdir(exist_ok=True)
    
    rules_path = output_dir / 'top_rules.json'
    report_path = output_dir / 'rules_report.md'
    
    save_rules_to_json(valid_rules, str(rules_path))
    generate_report(valid_rules, str(report_path))
    
    print("\n✅ ENTRENAMIENTO COMPLETADO", flush=True)
    print(f"✅ Reglas válidas: {len(valid_rules)}", flush=True)


if __name__ == '__main__':
    main()
