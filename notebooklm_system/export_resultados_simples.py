import sys
import os
import json
import sqlite3
from pathlib import Path
from datetime import datetime
import argparse

# Configurar rutas
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT / "src"))

from modules import sql_store

def parse_handicap(val):
    if val is None or val == "" or val == "N/A" or val == "-":
        return None
    try:
        val_str = str(val).replace(",", ".").strip()
        if "→" in val_str:
            val_str = val_str.split("→")[-1].strip()
        return float(val_str)
    except:
        return None

def evaluate_handicap_cover(score_str, ah_line_raw, is_home_subject):
    if not score_str or score_str in ("-", "?:?", "?-?"):
        return "Desconocido"
    ah_num = parse_handicap(ah_line_raw)
    if ah_num is None:
        return "Desconocido"
    try:
        parts = score_str.replace('-', ':').split(':')
        goles_h = int(parts[0])
        goles_a = int(parts[1])
        
        if ah_num > 0:
            fav_is_local = True
            abs_ah = ah_num
        elif ah_num < 0:
            fav_is_local = False
            abs_ah = abs(ah_num)
        else:
            if goles_h > goles_a:
                return "CUBRIÓ" if is_home_subject else "NO CUBRIÓ"
            elif goles_a > goles_h:
                return "NO CUBRIÓ" if is_home_subject else "CUBRIÓ"
            else:
                return "PUSH (Igualó)"

        diff_local = goles_h - goles_a
        
        if fav_is_local:
            fav_margin = diff_local - abs_ah
        else:
            fav_margin = -diff_local - abs_ah
            
        if fav_margin > 0.01:
            fav_covered = True
            fav_push = False
        elif fav_margin < -0.01:
            fav_covered = False
            fav_push = False
        else:
            fav_covered = False
            fav_push = True
            
        subject_is_fav = (fav_is_local and is_home_subject) or (not fav_is_local and not is_home_subject)
        
        if fav_push:
            return "PUSH (Igualó)"
        
        if subject_is_fav:
            return "CUBRIÓ" if fav_covered else "NO CUBRIÓ"
        else:
            return "CUBRIÓ" if not fav_covered else "NO CUBRIÓ"
    except Exception:
        return "Desconocido"

def es_partido_util(payload):
    lhm = payload.get("last_home_match") or {}
    lam = payload.get("last_away_match") or {}
    
    date_h = lhm.get("date")
    date_a = lam.get("date")
    
    if not date_h or not date_a or date_h == "N/A" or date_a == "N/A":
        return False
        
    try:
        # Extraer el año del partido previo (ej: "2025-06-07" -> 2025)
        year_h = int(str(date_h).split("-")[0])
        year_a = int(str(date_a).split("-")[0])
        if year_h < 2026 or year_a < 2026:
            return False
    except Exception:
        return False
        
    return True

def main():
    print("========================================================")
    print("EXPORTADOR DE RESULTADOS SIMPLIFICADOS PARA CHATGPT")
    print("========================================================")
    
    parser = argparse.ArgumentParser(description="Exporta lista compacta de resultados finales para ChatGPT.")
    parser.add_argument(
        "--date", 
        type=str, 
        default=None, 
        help="Fecha específica para exportar (formato M/D/YYYY, ej: 6/18/2026). Por defecto es hoy."
    )
    parser.add_argument(
        "--all", 
        action="store_true", 
        help="Exportar todos los partidos finalizados."
    )
    args = parser.parse_args()
    
    # Determinar fecha
    if args.all:
        target_date = None
        date_msg = "Todos los partidos terminados"
    else:
        if args.date:
            target_date = args.date
        else:
            today = datetime.now()
            target_date = f"{today.month}/{today.day}/{today.year}"
        date_msg = f"Fecha: {target_date}"
        
    db_path = PROJECT_ROOT / "data" / "app_data.db"
    if not db_path.exists():
        print(f"ERROR: No se encontro la base de datos en {db_path}")
        return 1
        
    print(f"Conectando a {db_path}...")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Query finished matches
    if target_date:
        print(f"Filtrando por {date_msg}...")
        cursor.execute("""
            SELECT match_id, bucket, state, payload_json 
            FROM matches 
            WHERE state = 'historical' AND match_date = ?
            ORDER BY match_date DESC
        """, (target_date,))
    else:
        print("Exportando todos los partidos finalizados...")
        cursor.execute("""
            SELECT match_id, bucket, state, payload_json 
            FROM matches 
            WHERE state = 'historical'
            ORDER BY match_date DESC
        """)
        
    rows = cursor.fetchall()
    
    print(f"Encontrados {len(rows)} partidos terminados para {date_msg}.")
    if len(rows) == 0:
        print(f"No hay partidos terminados para {date_msg} para exportar.")
        conn.close()
        return 0
        
    output_dir = PROJECT_ROOT / "notebooklm"
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / "resultados_hoy_simples.md"
    
    md_content = []
    md_content.append("# Resumen Compacto de Resultados Finales - Automejora ChatGPT\n")
    md_content.append(f"Documento generado el: {sql_store.now_iso()}\n")
    md_content.append(f"Usa esta lista para evaluar tus predicciones y realizar automejoras rápidas en base al hándicap.\n")
    md_content.append("=" * 60 + "\n")
    
    exported_count = 0
    for idx, row in enumerate(rows):
        try:
            payload = json.loads(row["payload_json"])
            
            # Filtrar partidos inútiles (con previos de 2025 o antes)
            if not es_partido_util(payload):
                continue
                
            home = payload.get("home_name") or payload.get("home_team")
            away = payload.get("away_name") or payload.get("away_team")
            score = payload.get("final_score") or payload.get("score")
            handicap = payload.get("handicap") or (payload.get("main_match_odds") or {}).get("ah_linea")
            league = payload.get("league_name") or payload.get("league")
            date = payload.get("match_date") or payload.get("date")
            
            # Calcular la cobertura del hándicap
            cover_status = evaluate_handicap_cover(score, handicap, True)
            
            entry = f"""- **Partido**: {home} vs {away} (ID: {row['match_id']})
  * Fecha: {date} | Liga: {league}
  * Hándicap Inicial: {handicap}
  * Marcador Final (FT): {score}
  * Cobertura Hándicap Local: **{cover_status}**
"""
            md_content.append(entry)
            exported_count += 1
        except Exception as e:
            print(f"Error procesando partido {row['match_id']}: {e}")
            
    with open(output_file, "w", encoding="utf-8") as fh:
        fh.write("\n".join(md_content))
        
    print(f"\n¡Exportacion completada con exito!")
    print(f"Se exportaron {exported_count} partidos terminados simplificados.")
    print(f"Archivo generado en: {output_file}")
    
    conn.close()
    return 0

if __name__ == "__main__":
    sys.exit(main())
