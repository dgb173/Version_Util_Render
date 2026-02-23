import sys
from pathlib import Path

# Add project root to sys.path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from src.app import process_all_finished_matches_background_with_workers
from src.modules import sql_store

# Configuración de workers
DEFAULT_WORKERS = 10  # Óptimo para velocidad sin riesgo de bloqueo
DEFAULT_FLUSH_EVERY = 5  # Export incremental cada 5 partidos

if __name__ == "__main__":
    print("=" * 50)
    print("CACHEAR TERMINADOS (como botón Cachear)")
    print("=" * 50)
    print("Scrapea partidos terminados de NowGoal")
    print("y los guarda en buckets SQL historicos (data_ah_*)")
    print(f"DB SQL: {sql_store.get_db_path()}")
    print()
    
    # Argumentos: [handicap] [ou] [workers]
    handicap_filter = None
    goal_line_filter = None
    workers = DEFAULT_WORKERS
    flush_every = DEFAULT_FLUSH_EVERY
    
    if len(sys.argv) > 1:
        handicap_filter = sys.argv[1] if sys.argv[1] != 'all' else None
    if len(sys.argv) > 2:
        goal_line_filter = sys.argv[2] if sys.argv[2] != 'all' else None
    if len(sys.argv) > 3:
        try:
            workers = int(sys.argv[3])
        except:
            pass
    if len(sys.argv) > 4:
        try:
            flush_every = int(sys.argv[4])
            if flush_every <= 0:
                flush_every = DEFAULT_FLUSH_EVERY
        except:
            pass
    
    print(f"Configuración:")
    print(f"  • Filtro AH: {handicap_filter or 'Todos'}")
    print(f"  • Filtro OU: {goal_line_filter or 'Todos'}")
    print(f"  • Workers: {workers}")
    print(f"  • Flush incremental: cada {flush_every} partidos")
    print()
    
    try:
        process_all_finished_matches_background_with_workers(
            handicap_filter=handicap_filter,
            goal_line_filter=goal_line_filter,
            workers=workers,
            flush_every=flush_every
        )
        print()
        print("Proceso de cacheo completado.")
    except Exception as e:
        print(f"Error durante el cacheo: {e}")
