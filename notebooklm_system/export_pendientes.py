import sys
import os
import json
import sqlite3
from pathlib import Path

# Add src directory to python path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT / "src"))

from modules import llm_exporter, sql_store

def main():
    print("========================================================")
    print("EXPORTADOR DE PARTIDOS PENDIENTES PARA NOTEBOOKLM")
    print("========================================================")
    
    db_path = PROJECT_ROOT / "data" / "app_data.db"
    if not db_path.exists():
        print(f"ERROR: No se encontro la base de datos en {db_path}")
        return 1
        
    print(f"Conectando a {db_path}...")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Query matches in precache state (data_precacheo.json)
    # We fetch matches where bucket is data_precacheo.json or state is precacheo
    cursor.execute("""
        SELECT match_id, bucket, state, payload_json 
        FROM matches 
        WHERE bucket = 'data_precacheo.json' OR state = 'precacheo'
        ORDER BY match_date ASC
    """)
    rows = cursor.fetchall()
    
    print(f"Encontrados {len(rows)} partidos en el precacheo/pendientes.")
    if len(rows) == 0:
        print("No hay partidos en precacheo para exportar.")
        conn.close()
        return 0
        
    output_dir = PROJECT_ROOT / "notebooklm"
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / "pendientes_hoy.md"
    
    md_content = []
    md_content.append("# Partidos de Precacheo - Resultados Pendientes\n")
    md_content.append(f"Documento generado el: {sql_store.now_iso()}\n")
    md_content.append(f"Contiene {len(rows)} partidos listos para ser analizados en NotebookLM.\n")
    md_content.append("=" * 60 + "\n")
    
    exported_count = 0
    for idx, row in enumerate(rows):
        try:
            payload = json.loads(row["payload_json"])
            # Format using our corrected llm_exporter
            match_md = llm_exporter.generate_notebooklm_match_format(payload)
            if match_md:
                md_content.append(match_md)
                md_content.append("\n" + "="*80 + "\n")
                exported_count += 1
        except Exception as e:
            print(f"Error procesando partido {row['match_id']}: {e}")
            
    with open(output_file, "w", encoding="utf-8") as fh:
        fh.write("\n".join(md_content))
        
    print(f"\n¡Exportacion completada con exito!")
    print(f"Se exportaron {exported_count} de {len(rows)} partidos.")
    print(f"Archivo generado en: {output_file}")
    
    conn.close()
    return 0

if __name__ == "__main__":
    sys.exit(main())
