import sys
from pathlib import Path

# Add src to sys.path
script_dir = Path(__file__).resolve().parent
project_dir = script_dir.parent
src_dir = project_dir / 'src'
sys.path.insert(0, str(src_dir))

from modules import data_manager

if __name__ == "__main__":
    print("-" * 40)
    print("VERIFICACIÓN DE LIMPIEZA DE PRECACHEO")
    print("-" * 40)
    
    # 1. Cargar antes
    initial_matches = data_manager.load_precacheo_matches()
    print(f"Partidos iniciales: {len(initial_matches)}")
    
    # 2. Ejecutar limpieza (threshold 1 día)
    # Si hoy es 2026-01-06, threshold es 2026-01-05
    removed = data_manager.clean_old_precacheo_matches(days_threshold=1)
    
    # 3. Cargar después
    final_matches = data_manager.load_precacheo_matches()
    print(f"Partidos eliminados: {removed}")
    print(f"Partidos restantes: {len(final_matches)}")
    
    if len(final_matches) > 0:
        print(f"Ejemplo de partido restante: {final_matches[0].get('match_date')} - {final_matches[0].get('home_name')}")
    
    print("-" * 40)
    print("FIN DE VERIFICACIÓN")
    print("-" * 40)
