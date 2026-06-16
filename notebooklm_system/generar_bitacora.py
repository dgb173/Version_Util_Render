import sys
import os
import json
import sqlite3
import re
from pathlib import Path

# Add src directory to python path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT / "src"))

from modules import sql_store

def parse_score(score_str):
    if not score_str or score_str == "?:?":
        return None, None
    try:
        clean_score = score_str.replace("-", ":").replace(" ", "")
        parts = clean_score.split(":")
        if len(parts) == 2:
            return int(parts[0]), int(parts[1])
    except Exception:
        pass
    return None, None

def evaluate_asian_handicap(home_goals, away_goals, team_pick, handicap_val):
    """
    Calcula el resultado de una apuesta de handicap asiatico.
    team_pick: 'Local' o 'Visitante'
    handicap_val: float (ej: 0.0, -0.25, 0.25, -0.5, etc.)
    Retorna: (estado, ganancia_coef)
    """
    # Diferencia de goles desde la perspectiva del equipo elegido
    if team_pick == 'Local':
        diff = home_goals - away_goals
    else:
        diff = away_goals - home_goals
        
    net_result = diff + handicap_val
    
    if net_result > 0.25:
        return "ACIERTO", 1.0
    elif net_result == 0.25:
        return "MITAD ACIERTO", 0.5
    elif net_result == 0.0:
        return "NULO (PUSH)", 0.0
    elif net_result == -0.25:
        return "MITAD FALLO", -0.5
    else:
        return "FALLO", -1.0

def parse_pick(pick_str):
    """
    Parsea strings del tipo 'Local -0.25', 'Visitante 0', 'NO BET', 'Local +0.5'
    Retorna: (team, value) o (None, None)
    """
    if not pick_str or not isinstance(pick_str, str):
        return None, None
    
    clean_pick = pick_str.strip().lower()
    if 'no bet' in clean_pick or 'nobet' in clean_pick or clean_pick == '-':
        return 'NO BET', 0.0
        
    # Expresion regular para buscar Local/Visitante y un numero opcional con signo
    match = re.search(r'(local|visitante)\s*([+-]?\d*(?:\.\d+)?)?', clean_pick)
    if not match:
        return None, None
        
    team_raw = match.group(1)
    team = 'Local' if team_raw == 'local' else 'Visitante'
    
    val_raw = match.group(2)
    val = 0.0
    if val_raw:
        try:
            val = float(val_raw)
        except ValueError:
            pass
            
    return team, val

def main():
    print("========================================================")
    print("GENERADOR DE BITACORA DE AUTO-MEJORA PARA NOTEBOOKLM")
    print("========================================================")
    
    system_dir = PROJECT_ROOT / "notebooklm_system"
    system_dir.mkdir(exist_ok=True)
    
    pred_file = system_dir / "predictions.json"
    bitacora_file = PROJECT_ROOT / "notebooklm" / "bitacora_aprendizajes.md"
    
    # Crear archivo de predicciones de ejemplo si no existe
    if not pred_file.exists():
        example_data = {
            "2997255": "Local 0",
            "2995922": "NO BET",
            "PON_AQUI_EL_ID_DEL_PARTIDO": "Local -0.25"
        }
        with open(pred_file, "w", encoding="utf-8") as fh:
            json.dump(example_data, fh, indent=2)
        print(f"Creado archivo de plantilla de predicciones en: {pred_file}")
        print("Edita ese archivo JSON colocando los IDs de partidos y tus pronósticos sugeridos por NotebookLM.")
        return 0
        
    try:
        with open(pred_file, "r", encoding="utf-8") as fh:
            predictions = json.load(fh)
    except Exception as e:
        print(f"ERROR leyendo {pred_file}: {e}")
        return 1
        
    if not predictions:
        print("El archivo predictions.json esta vacio.")
        return 0
        
    db_path = PROJECT_ROOT / "data" / "app_data.db"
    if not db_path.exists():
        print(f"ERROR: No se encontro la base de datos en {db_path}")
        return 1
        
    print(f"Cargando predicciones desde {pred_file}...")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Leer bitacora existente para evitar duplicar partidos
    existing_ids = set()
    if bitacora_file.exists():
        try:
            with open(bitacora_file, "r", encoding="utf-8") as fh:
                content = fh.read()
                # Buscar patrones del tipo (ID: 123456)
                matches_found = re.findall(r'\(ID:\s*(\d+)\)', content)
                existing_ids = set(matches_found)
            print(f"Leidos {len(existing_ids)} partidos ya registrados en la bitacora.")
        except Exception as e:
            print(f"Advertencia leyendo bitacora existente: {e}")

    new_entries = []
    evaluated_count = 0
    pending_count = 0
    
    for m_id, pick in predictions.items():
        if m_id == "PON_AQUI_EL_ID_DEL_PARTIDO":
            continue
            
        m_id_str = str(m_id)
        if m_id_str in existing_ids:
            # Ya esta en la bitacora, no duplicamos
            continue
            
        # Buscar en la DB
        cursor.execute("SELECT payload_json, score FROM matches WHERE match_id = ?", (m_id_str,))
        row = cursor.fetchone()
        
        if not row:
            print(f"Advertencia: Partido ID {m_id_str} no encontrado en la base de datos.")
            continue
            
        try:
            payload = json.loads(row["payload_json"])
        except Exception:
            print(f"Error parseando JSON del partido {m_id_str}.")
            continue
            
        score = row["score"] or payload.get("score") or payload.get("final_score")
        hg, ag = parse_score(score)
        
        if hg is None or ag is None:
            # El partido aun no tiene resultado definitivo
            pending_count += 1
            continue
            
        # Parsea el pick y evalua
        team_pick, handicap_val = parse_pick(pick)
        
        if not team_pick:
            print(f"Advertencia: No se pudo parsear el pick '{pick}' para el partido {m_id_str}.")
            continue
            
        status = "NO BET"
        coef = 0.0
        if team_pick != "NO BET":
            status, coef = evaluate_asian_handicap(hg, ag, team_pick, handicap_val)
            
        # Estadisticas del partido
        home_name = payload.get("home_name") or payload.get("home_team") or "Local"
        away_name = payload.get("away_name") or payload.get("away_team") or "Visitante"
        league = payload.get("league_name") or payload.get("league") or "Liga"
        date = payload.get("match_date") or payload.get("date") or "N/A"
        initial_ah = payload.get("handicap") or (payload.get("main_match_odds") or {}).get("ah_linea") or "-"
        
        # Obtener estadisticas del partido actual (si estuvieran cargadas en el scraper o vacias)
        # Nota: Usamos las estadisticas directas si el scraper final las tiene.
        stats_list = payload.get("stats_rows") or []
        stats_lines = []
        if stats_list:
            for s in stats_list:
                label = s.get("label") or s.get("name") or ""
                h_val = s.get("home") or "-"
                a_val = s.get("away") or "-"
                if label:
                    stats_lines.append(f"      * {label}: {h_val} ({home_name}) vs {a_val} ({away_name})")
        else:
            stats_lines.append("      * (No hay estadisticas detalladas del partido en vivo)")
            
        stats_block = "\n".join(stats_lines)
        
        entry = f"""- **Partido**: {home_name} vs {away_name} (ID: {m_id_str})
  * Liga: {league} | Fecha: {date}
  * Pronostico Evaluado: `{pick}` (AH Inicial: {initial_ah})
  * Marcador Final: {score}
  * Estado del Pronostico: **{status}**
  * Estadisticas de Rendimiento Reales:
{stats_block}
"""
        new_entries.append(entry)
        evaluated_count += 1
        
    if new_entries:
        # Escribir o anexar a la bitacora al principio
        new_content_str = "\n".join(new_entries)
        
        if bitacora_file.exists():
            with open(bitacora_file, "r", encoding="utf-8") as fh:
                old_content = fh.read()
            # Insertar los nuevos resultados despues del titulo principal
            title_match = re.match(r'(#\s+Bitacora[^\n]*\n+)', old_content, re.IGNORECASE)
            if title_match:
                header = title_match.group(1)
                body = old_content[len(header):]
                final_content = f"{header}{new_content_str}\n\n{body}"
            else:
                final_content = f"{new_content_str}\n\n{old_content}"
        else:
            final_content = f"# Bitacora de Aciertos, Errores y Aprendizajes\n\nEste archivo acumula los resultados y sirve como fuente de auto-mejora para NotebookLM.\n\n{new_content_str}"
            
        with open(bitacora_file, "w", encoding="utf-8") as fh:
            fh.write(final_content)
            
        print(f"\n¡Bitacora actualizada con exito!")
        print(f"Se agregaron {evaluated_count} nuevos resultados finalizados.")
        print(f"Archivo guardado/actualizado en: {bitacora_file}")
    else:
        print("\nNo se encontraron nuevos partidos finalizados para agregar a la bitacora.")
        
    if pending_count > 0:
        print(f"Nota: Hay {pending_count} partidos en predictions.json que todavia estan pendientes de jugar o no tienen marcador definitivo en SQLite.")
        print("Ejecuta tu script de cachear terminados antes de correr este script para que se actualicen los resultados.")
        
    conn.close()
    return 0

if __name__ == "__main__":
    sys.exit(main())
