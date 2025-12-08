"""
Script para eliminar partidos con score '??' de los archivos del Explorador.
"""
import json
from pathlib import Path

DATA_DIR = Path("data")

def clean_invalid_matches():
    # Archivos a limpiar (los buckets del explorador)
    bucket_files = [
        "data_ah_0.json",
        "data_ah_0.5.json",
        "data_ah_1.5.json",
        "data_ah_2_plus.json",
        "data_minus_ah_0.5.json",
        "data_minus_ah_1.5.json",
        "data_minus_ah_2_plus.json",
        "data_unknown.json"
    ]
    
    total_removed = 0
    
    for filename in bucket_files:
        filepath = DATA_DIR / filename
        if not filepath.exists():
            print(f"⏭️  {filename} no existe, saltando...")
            continue
            
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                matches = json.load(f)
            
            original_count = len(matches)
            
            # Filtrar partidos con score válido
            valid_matches = []
            removed_matches = []
            
            for m in matches:
                score = m.get('score') or m.get('final_score') or ''
                # Scores inválidos: '??', '?:?', '?-?', vacío, o sin ':'
                if score in ['??', '?:?', '?-?', '? : ?', '', None] or ':' not in str(score):
                    removed_matches.append({
                        'id': m.get('match_id'),
                        'teams': f"{m.get('home_name', '?')} vs {m.get('away_name', '?')}",
                        'score': score
                    })
                else:
                    valid_matches.append(m)
            
            removed_count = original_count - len(valid_matches)
            
            if removed_count > 0:
                # Guardar archivo limpio
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(valid_matches, f, indent=2, ensure_ascii=False)
                
                print(f"✅ {filename}: Eliminados {removed_count} partidos inválidos (quedan {len(valid_matches)})")
                for rm in removed_matches[:5]:  # Mostrar primeros 5
                    print(f"   - {rm['teams']} (score: {rm['score']})")
                if len(removed_matches) > 5:
                    print(f"   ... y {len(removed_matches) - 5} más")
                    
                total_removed += removed_count
            else:
                print(f"✅ {filename}: Sin partidos inválidos ({original_count} partidos OK)")
                
        except Exception as e:
            print(f"❌ Error procesando {filename}: {e}")
    
    print(f"\n{'='*50}")
    print(f"TOTAL ELIMINADOS: {total_removed} partidos con score inválido")
    print(f"{'='*50}")

if __name__ == "__main__":
    clean_invalid_matches()
