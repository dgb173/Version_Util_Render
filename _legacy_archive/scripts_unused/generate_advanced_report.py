
import json
from pathlib import Path

RESULTS_DIR = Path("backtest_results")
RULES_FILE = RESULTS_DIR / "advanced_rules_col3.json"

def main():
    if not RULES_FILE.exists():
        print("No rules file found.")
        return

    with open(RULES_FILE, 'r', encoding='utf-8') as f:
        rules = json.load(f)

    print(f"\n📊 REPORTE DE REGLAS AVANZADAS ({len(rules)} reglas encontradas)\n")
    print("="*60)

    for i, r in enumerate(rules):
        print(f"[{i+1}] {r['name']}")
        print(f"   🎯 Predicción: {r['prediction']}")
        print(f"   ✅ Precisión: {r['accuracy']}% ({r.get('samples', 0)} muestras)")
        print(f"   📋 Condiciones:")
        
        conds = r.get('conditions', {})
        for k, v in conds.items():
            # Translate keys to readable text
            readable_k = k.replace('_', ' ').title()
            # Special translations
            if k == 'col3_mejora': readable_k = "H2H Col3 Mejora"
            if k == 'col3_empeora': readable_k = "H2H Col3 Empeora"
            if k == 'col3_iguala': readable_k = "H2H Col3 Iguala"
            if k == 'ph_covers_current': readable_k = "Prev Home CUBRE Handicap Actual"
            if k == 'pa_covers_current': readable_k = "Prev Away CUBRE Handicap Actual"
            if k == 'ph_loses_current': readable_k = "Prev Home NO CUBRE (Loss)"
            if k == 'pa_loses_current': readable_k = "Prev Away NO CUBRE (Loss)"
            if k == 'is_heavy_fav': readable_k = "Es Favorito Fuerte (AH >= 1.0)"
            if k == 'fav_is_home': readable_k = "Favorito es LOCAL"
            
            val_str = "SÍ" if v is True else ("NO" if v is False else str(v))
            print(f"      - {readable_k}: {val_str}")
        print("-" * 60)

if __name__ == "__main__":
    main()
