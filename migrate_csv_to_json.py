import csv
import json
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).resolve().parent
STUDIED_MATCHES_DIR = BASE_DIR / 'studied_matches'
CSV_FILE = STUDIED_MATCHES_DIR / 'history.csv'
JSON_FILE = STUDIED_MATCHES_DIR / 'history.json'

def migrate():
    if not CSV_FILE.exists():
        print(f"No se encontró el archivo CSV en: {CSV_FILE}")
        return

    print(f"Leyendo CSV desde: {CSV_FILE}")
    matches = []
    
    try:
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                matches.append(row)
        
        print(f"Se leyeron {len(matches)} partidos del CSV.")
        
        # Guardar en JSON
        with open(JSON_FILE, 'w', encoding='utf-8') as f:
            json.dump(matches, f, ensure_ascii=False, indent=2)
            
        print(f"Migración completada. Datos guardados en: {JSON_FILE}")
        
    except Exception as e:
        print(f"Error durante la migración: {e}")

if __name__ == "__main__":
    migrate()
