#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script para limpiar los archivos JSON de datos:
1. Eliminar partidos de equipos U19/U21/U20/U18/U17 (juveniles)
2. Eliminar partidos con resultado ?? (solo en datos históricos, NO en precacheo)

Uso: py clean_data.py
"""

import json
import os
import re
from pathlib import Path

# Patrones a excluir (equipos juveniles)
EXCLUDE_PATTERNS = [
    r'\bu19\b', r'sub-19', r'sub 19', r'under 19',
]

# Ligas juveniles que SÍ queremos ver (excepciones)
ALLOWED_YOUTH_LEAGUES = [
    'algeria u20 league',
    # Agregar más ligas aquí si es necesario
]

def is_youth_match(match):
    """Verifica si el partido es de equipos juveniles (excepto ligas permitidas)"""
    home = (match.get('home_team') or match.get('home') or match.get('home_name') or '').lower()
    away = (match.get('away_team') or match.get('away') or match.get('away_name') or '').lower()
    league = (match.get('league') or match.get('liga') or match.get('league_name') or '').lower()
    
    # Si la liga está en las permitidas, NO filtrar
    for allowed in ALLOWED_YOUTH_LEAGUES:
        if allowed in league:
            return False
    
    text_to_check = f"{home} {away} {league}"
    
    for pattern in EXCLUDE_PATTERNS:
        if re.search(pattern, text_to_check, re.IGNORECASE):
            return True
    return False

def has_invalid_score(match):
    """Verifica si el partido tiene resultado inválido (??)"""
    score = match.get('score') or match.get('final_score') or ''
    if not score:
        return True
    if '?' in score:
        return True
    return False

def clean_json_file(filepath, remove_invalid_scores=True):
    """Limpia un archivo JSON individual"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"  Error leyendo {filepath}: {e}")
        return 0, 0
    
    original_count = 0
    removed_count = 0
    
    # Manejar diferentes estructuras de datos
    if isinstance(data, list):
        # Es una lista de partidos
        original_count = len(data)
        cleaned = []
        for match in data:
            if is_youth_match(match):
                removed_count += 1
                continue
            if remove_invalid_scores and has_invalid_score(match):
                removed_count += 1
                continue
            cleaned.append(match)
        data = cleaned
    elif isinstance(data, dict):
        # Es un diccionario con partidos como valores
        if 'matches' in data:
            # Formato: {"matches": [...]}
            original_count = len(data['matches'])
            cleaned = []
            for match in data['matches']:
                if is_youth_match(match):
                    removed_count += 1
                    continue
                if remove_invalid_scores and has_invalid_score(match):
                    removed_count += 1
                    continue
                cleaned.append(match)
            data['matches'] = cleaned
        else:
            # Formato: {match_id: match_data, ...}
            original_count = len(data)
            keys_to_remove = []
            for key, match in data.items():
                if isinstance(match, dict):
                    if is_youth_match(match):
                        keys_to_remove.append(key)
                    elif remove_invalid_scores and has_invalid_score(match):
                        keys_to_remove.append(key)
            for key in keys_to_remove:
                del data[key]
                removed_count += 1
    
    if removed_count > 0:
        # Guardar archivo limpio
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"  ✓ {filepath}: {removed_count}/{original_count} eliminados")
    else:
        print(f"  - {filepath}: Sin cambios ({original_count} partidos)")
    
    return original_count, removed_count

def main():
    # Directorio de datos
    data_dir = Path(__file__).parent / 'data'
    
    if not data_dir.exists():
        print(f"Error: Directorio {data_dir} no existe")
        return
    
    print("=" * 60)
    print("LIMPIEZA DE DATOS - Eliminando U19/U21 y resultados ??")
    print("=" * 60)
    
    total_original = 0
    total_removed = 0
    
    # Archivos a procesar (excluyendo precacheo)
    json_files = list(data_dir.glob('*.json'))
    
    for filepath in json_files:
        filename = filepath.name
        
        # Saltar precacheo (los partidos sin resultado son normales ahí)
        if 'precacheo' in filename.lower():
            print(f"\n[SKIP] {filename} (Pre-cacheo - se permiten ??)")
            # Solo limpiar U19, no scores inválidos
            orig, removed = clean_json_file(filepath, remove_invalid_scores=False)
            total_original += orig
            total_removed += removed
            continue
        
        print(f"\n[CLEAN] {filename}")
        orig, removed = clean_json_file(filepath, remove_invalid_scores=True)
        total_original += orig
        total_removed += removed
    
    print("\n" + "=" * 60)
    print(f"TOTAL: {total_removed} partidos eliminados de {total_original}")
    print("=" * 60)

if __name__ == "__main__":
    main()
