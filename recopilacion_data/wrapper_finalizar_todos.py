import sys
import os
from pathlib import Path

# Add project root to sys.path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from src.modules import data_manager

def finalize_all_with_results():
    print("Buscando partidos para finalizar (Mover a Explorador)...")
    try:
        # 1. Load precacheo matches
        precacheo_matches = data_manager.load_precacheo_matches()
        
        matches_with_results = []
        for m in precacheo_matches:
            # Check for valid score
            score = m.get('score') or m.get('final_score') or ''
            # Simple validation same as JS: has score, not ??, and contains :
            if score and score not in ['??', '?:?'] and ':' in score:
                matches_with_results.append(str(m.get('match_id')))
        
        if not matches_with_results:
            print("No hay partidos con resultado para finalizar.")
            return

        print(f"Se encontraron {len(matches_with_results)} partidos para finalizar.")
        
        # 2. Batch finalize
        success, failed, errors = data_manager.finalize_precacheo_batch(matches_with_results)
        
        print(f"Proceso finalizado.")
        print(f"Exitosos: {success}")
        print(f"Fallidos: {failed}")
        if failed > 0:
            print("Errores:")
            for err in errors:
                print(f" - {err}")
                
    except Exception as e:
        print(f"Error fatal finalizando partidos: {e}")

if __name__ == "__main__":
    finalize_all_with_results()
