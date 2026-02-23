import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'src'))

from modules import data_manager

def main():
    print("[CLEANUP] Iniciando limpieza de datos explorador...")
    print("-----------------------------------------------------")
    
    total_removed, stats = data_manager.clean_all_buckets()
    
    if total_removed > 0:
        print(f"Se eliminaron {total_removed} partidos incompletos.")
        for bucket, msg in stats.items():
            print(f"   - {bucket}: {msg}")
    else:
        print("No se encontraron partidos invalidos. Todo limpio.")
        
    print("-----------------------------------------------------")

if __name__ == "__main__":
    main()
