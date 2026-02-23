"""
Filtra los archivos de reglas para quedarse SOLO con las que tienen 100% de acierto.
"""
import json
from pathlib import Path

RESULTS_DIR = Path('backtest_results')

def filter_rules():
    print("🔫 FILTRANDO REGLAS: Solo 100% Accuracy...")
    
    total_kept = 0
    total_removed = 0
    
    for f in RESULTS_DIR.glob('specialist_ah_*.json'):
        try:
            with open(f, 'r', encoding='utf-8') as fh:
                rules = json.load(fh)
            
            if not rules:
                continue
                
            # Filtrar solo las de 100%
            good_rules = [r for r in rules if r.get('accuracy', 0) >= 100.0]
            bad_count = len(rules) - len(good_rules)
            
            # Guardar solo las buenas
            with open(f, 'w', encoding='utf-8') as fh:
                json.dump(good_rules, fh, indent=2)
            
            print(f"   {f.name}: {len(good_rules)} reglas (eliminadas {bad_count})")
            total_kept += len(good_rules)
            total_removed += bad_count
            
        except Exception as e:
            print(f"   Error en {f.name}: {e}")
    
    print(f"\n✅ COMPLETADO: {total_kept} reglas al 100% conservadas, {total_removed} eliminadas.")

if __name__ == "__main__":
    filter_rules()
