"""
Script de depuración para verificar que la triangulación se está generando correctamente.
Ejecutar desde la raíz del proyecto: py debug_triangulacion.py
"""
import sys
import os

# Añadir el directorio src al path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from modules.estudio_scraper import analizar_partido_completo

def debug_match(match_id):
    print(f"\n{'='*60}")
    print(f"📊 Analizando partido: {match_id}")
    print('='*60)
    
    # Forzar refresh para obtener datos nuevos
    result = analizar_partido_completo(match_id, force_refresh=True)
    
    if 'error' in result:
        print(f"❌ Error: {result['error']}")
        return
    
    print(f"✅ Partido: {result.get('home_name', '?')} vs {result.get('away_name', '?')}")
    print(f"   Liga: {result.get('league_name', '?')}")
    
    # Verificar H2H Col3
    h2h_col3 = result.get('h2h_col3', {})
    print(f"\n📋 H2H Col3:")
    print(f"   status: {h2h_col3.get('status', 'N/A')}")
    if h2h_col3.get('status') == 'found':
        print(f"   goles_home: {h2h_col3.get('goles_home')}")
        print(f"   goles_away: {h2h_col3.get('goles_away')}")
        print(f"   handicap: {h2h_col3.get('handicap')}")
    
    # Verificar partidos previos
    print(f"\n📋 Partidos Previos:")
    lhm = result.get('last_home_match')
    lam = result.get('last_away_match')
    print(f"   last_home_match: {'✓' if lhm else '✗'}", end='')
    if lhm:
        print(f" - {lhm.get('score')}, AH: {lhm.get('handicap_line_raw')}")
    else:
        print()
    print(f"   last_away_match: {'✓' if lam else '✗'}", end='')
    if lam:
        print(f" - {lam.get('score')}, AH: {lam.get('handicap_line_raw')}")
    else:
        print()
    
    # Verificar market_analysis_data
    mad = result.get('market_analysis_data', {})
    print(f"\n📋 Market Analysis Data:")
    print(f"   stadium: {'✓' if mad.get('stadium') else '✗'}")
    print(f"   general: {'✓' if mad.get('general') else '✗'}")
    
    # Verificar triangulación
    tri = mad.get('triangulacion', {})
    print(f"\n🎯 TRIANGULACIÓN:")
    if tri:
        print(f"   Caso: {tri.get('caso')}")
        print(f"   Diagnóstico: {tri.get('diagnostico')}")
        print(f"   Señal: {tri.get('senal')}")
        print(f"   Prioridad: {tri.get('prioridad')}")
        print(f"   Recomendación AH: {tri.get('recomendacion_ah')}")
        print(f"   Explicación: {tri.get('explicacion')}")
        print(f"   Etiqueta Inversa: {tri.get('etiqueta_inversa')}")
    else:
        print(f"   ❌ NO HAY DATOS DE TRIANGULACIÓN")
    
    print('='*60)

if __name__ == '__main__':
    if len(sys.argv) > 1:
        match_id = sys.argv[1]
    else:
        match_id = input("Ingresa un match_id para debuguear (ej: 2751234): ")
    
    debug_match(match_id)
