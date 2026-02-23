import sys
import os
from pathlib import Path

# Add src to sys.path
script_dir = Path(__file__).resolve().parent
project_dir = script_dir.parent
src_dir = project_dir / 'src'
sys.path.insert(0, str(src_dir))

from modules import data_manager

if __name__ == "__main__":
    print("Iniciando LIMPIEZA PROFUNDA de data_precacheo.json...")
    
    # threshold=0 significa eliminar todo lo que tenga fecha anterior a HOY (2026-01-06)
    removed = data_manager.clean_old_precacheo_matches(days_threshold=0)
    
    print(f"Limpieza completada. Se eliminaron {removed} partidos antiguos.")
    
    # Mostrar estado final
    matches = data_manager.load_precacheo_matches()
    print(f"Partidos restantes en precacheo: {len(matches)}")

    pending_count = 0
    finished_count = 0
    for m in matches:
        score = m.get('score') or m.get('final_score')
        if score and score not in ['??', '?:?', '? - ?', '?-?'] and ':' in score:
            finished_count += 1
        else:
            pending_count += 1
            
    print(f"  - Finalizados (hoy/futuro): {finished_count}")
    print(f"  - Pendientes (hasta 3 dias atras): {pending_count}")
    
    if len(matches) > 0:
        print("\nProximos partidos (muestreo):")
        for m in matches[:5]:
            print(f"  - {m.get('match_date')} | {m.get('home_name')} vs {m.get('away_name')}")
