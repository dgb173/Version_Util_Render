"""
Valida el sistema en una liga específica y calcula ganancias.
Liga: 1465 (subleague)
Apuesta: 5€ por pick
Cuota: 1.8
"""
import sys
import os
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
from modules import league_scraper, estudio_scraper, specialist_validator

SEASON = "2024-2025"
LEAGUE_ID = 1465
BET_AMOUNT = 5.0
ODDS = 1.8

def main():
    print(f"🎯 VALIDACIÓN LIGA {LEAGUE_ID} (Temporada {SEASON})")
    print(f"💰 Apuesta: {BET_AMOUNT}€ por pick | Cuota: {ODDS}")
    print("=" * 60)
    
    # 1. Extraer IDs de partidos
    print("\n📋 Extrayendo IDs de partidos...")
    try:
        result = league_scraper.extract_ids_by_params(SEASON, LEAGUE_ID)
        match_data = result.get('match_data', [])
        ids = [str(m['id']) for m in match_data]
        print(f"✅ Encontrados {len(ids)} partidos")
    except Exception as e:
        print(f"❌ Error extrayendo IDs: {e}")
        return

    if not ids:
        print("❌ No se encontraron partidos")
        return

    # 2. Cargar validador
    validator = specialist_validator.SpecialistValidator()
    print(f"📊 Cargadas {len(validator.rules)} reglas")

    # 3. Procesar partidos (máximo 100 para velocidad)
    max_matches = min(100, len(ids))
    print(f"\n🔍 Analizando {max_matches} partidos...")
    
    total_bets = 0
    wins = 0
    losses = 0
    no_pick = 0
    
    for i, mid in enumerate(ids[:max_matches]):
        try:
            # Scrape match
            match_data = estudio_scraper.analizar_partido_completo(mid)
            if not match_data:
                continue
            
            # Get score
            score = match_data.get('final_score') or match_data.get('score')
            if not score or score in ['??', '?-?']:
                continue
            
            try:
                parts = score.replace(':', '-').split('-')
                hg, ag = int(parts[0].strip()), int(parts[1].strip())
            except:
                continue
            
            # Get AH line
            odds_data = match_data.get('main_match_odds', {})
            ah_str = odds_data.get('ah_linea', '0')
            try:
                ah = float(ah_str) if ah_str and ah_str != 'N/A' else 0.0
            except:
                ah = 0.0
                continue
            
            # Get prediction
            picks = validator.evaluate_match(match_data)
            ah_picks = [p for p in picks if p.get('type') == 'AH']
            
            if not ah_picks:
                no_pick += 1
                continue
            
            # Take best pick
            best_pick = max(ah_picks, key=lambda x: x.get('accuracy', 0))
            prediction = best_pick.get('prediction', '')
            
            # Calculate result
            diff = (hg - ag) + ah
            actual_winner = 'LOCAL' if diff > 0.25 else ('VISITA' if diff < -0.25 else 'PUSH')
            
            if actual_winner == 'PUSH':
                continue  # Devuelve apuesta
            
            total_bets += 1
            is_hit = (prediction == actual_winner)
            
            if is_hit:
                wins += 1
                status = "✅"
            else:
                losses += 1
                status = "❌"
            
            if (i+1) % 10 == 0:
                print(f"   Procesados {i+1}/{max_matches}...")
                
        except Exception as e:
            continue
    
    # 4. Calcular ganancias
    print("\n" + "=" * 60)
    print("📊 RESULTADOS")
    print("=" * 60)
    
    total_staked = total_bets * BET_AMOUNT
    gross_wins = wins * BET_AMOUNT * ODDS
    net_profit = gross_wins - total_staked
    roi = (net_profit / total_staked * 100) if total_staked > 0 else 0
    accuracy = (wins / total_bets * 100) if total_bets > 0 else 0
    
    print(f"Partidos analizados: {max_matches}")
    print(f"Picks realizados: {total_bets}")
    print(f"Sin pick (sin regla aplicable): {no_pick}")
    print(f"Aciertos: {wins}")
    print(f"Fallos: {losses}")
    print(f"Accuracy: {accuracy:.1f}%")
    print()
    print(f"💵 DINERO APOSTADO: {total_staked:.2f}€")
    print(f"💰 DINERO GANADO (bruto): {gross_wins:.2f}€")
    print(f"📈 BENEFICIO NETO: {net_profit:+.2f}€")
    print(f"📊 ROI: {roi:+.1f}%")
    
    if net_profit > 0:
        print(f"\n🎉 ¡HUBIERAS GANADO {net_profit:.2f}€!")
    else:
        print(f"\n😔 Hubieras perdido {abs(net_profit):.2f}€")

if __name__ == "__main__":
    main()
