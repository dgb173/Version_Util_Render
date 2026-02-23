"""
DESGLOSE CORREGIDO V2 - Backtest Australia W-League 2024-2025
Lógica: Siempre apostamos al FAVORITO
"""

matches = [
    {"date": "2024-11-01", "home": "Western United (W)", "away": "Wellington Phoenix (W)", "score": "4:2", "ah": 1.0},
    {"date": "2024-11-02", "home": "Central Coast Mariners (W)", "away": "Sydney FC (W)", "score": "3:1", "ah": -1.25},
    {"date": "2024-11-02", "home": "Canberra United (W)", "away": "Brisbane Roar (W)", "score": "3:2", "ah": 0.5},
    {"date": "2024-11-02", "home": "Adelaide United (W)", "away": "Melbourne Victory (W)", "score": "2:3", "ah": -1.25},
    {"date": "2024-11-03", "home": "Melbourne City (W)", "away": "Perth Glory (W)", "score": "5:2", "ah": 1.5},
    {"date": "2024-11-03", "home": "Newcastle Jets (W)", "away": "WS Wanderers (W)", "score": "2:2", "ah": 0.25},
    {"date": "2024-11-09", "home": "Melbourne Victory (W)", "away": "Melbourne City (W)", "score": "2:3", "ah": -0.25},
    {"date": "2024-11-09", "home": "Brisbane Roar (W)", "away": "Sydney FC (W)", "score": "1:0", "ah": -0.75},
    {"date": "2024-11-10", "home": "Wellington Phoenix (W)", "away": "Canberra United (W)", "score": "0:1", "ah": 0.75},
    {"date": "2024-11-10", "home": "WS Wanderers (W)", "away": "Adelaide United (W)", "score": "0:2", "ah": 1.0},
    {"date": "2024-11-10", "home": "Perth Glory (W)", "away": "Newcastle Jets (W)", "score": "3:2", "ah": -0.5},
    {"date": "2024-11-15", "home": "Brisbane Roar (W)", "away": "Perth Glory (W)", "score": "3:0", "ah": 1.25},
    {"date": "2024-11-16", "home": "Sydney FC (W)", "away": "WS Wanderers (W)", "score": "1:0", "ah": 1.0},
    {"date": "2024-11-16", "home": "Canberra United (W)", "away": "Adelaide United (W)", "score": "0:2", "ah": -0.25},
    {"date": "2024-11-16", "home": "Melbourne City (W)", "away": "Central Coast Mariners (W)", "score": "2:2", "ah": 0.75},
    {"date": "2025-04-18", "home": "Adelaide United (W)", "away": "Sydney FC (W)", "score": "2:1", "ah": 0},
    {"date": "2025-04-18", "home": "Perth Glory (W)", "away": "Melbourne City (W)", "score": "1:5", "ah": -1.25},
    {"date": "2025-04-19", "home": "Melbourne Victory (W)", "away": "Brisbane Roar (W)", "score": "2:0", "ah": 0.75},
    {"date": "2025-04-19", "home": "Newcastle Jets (W)", "away": "Central Coast Mariners (W)", "score": "1:2", "ah": -0.5},
    {"date": "2025-04-19", "home": "WS Wanderers (W)", "away": "Canberra United (W)", "score": "1:3", "ah": 0},
    {"date": "2025-04-20", "home": "Wellington Phoenix (W)", "away": "Western United (W)", "score": "1:1", "ah": 0},
]

stake = 5
odds = 1.85

def calculate_ah_result_v2(home_goals, away_goals, ah_line):
    """
    Calcula si el FAVORITO cubrió el hándicap.
    
    ah_line positivo: LOCAL es favorito, necesita ganar por más del hándicap
    ah_line negativo: VISITANTE es favorito, necesita ganar por más del hándicap invertido
    
    Retorna: (resultado, descripción, equipo_favorito)
    """
    is_home_fav = ah_line >= 0
    fav_team = "HOME" if is_home_fav else "AWAY"
    
    # Calcular diferencia desde perspectiva del FAVORITO
    if is_home_fav:
        # LOCAL es favorito con hándicap positivo (da goles)
        # Necesita ganar por más del hándicap
        fav_diff = home_goals - away_goals
        fav_handicap = ah_line  # Da estos goles
    else:
        # VISITANTE es favorito con hándicap negativo (da goles)
        # Necesita ganar por más del valor absoluto del hándicap
        fav_diff = away_goals - home_goals
        fav_handicap = abs(ah_line)  # Da estos goles
    
    # Aplicar hándicap (el favorito DA goles, así que restamos)
    adjusted_diff = fav_diff - fav_handicap
    
    # Manejar líneas de cuartos
    if abs(fav_handicap % 0.5) == 0.25:
        line1 = fav_handicap - 0.25
        line2 = fav_handicap + 0.25
        
        adj1 = fav_diff - line1
        adj2 = fav_diff - line2
        
        r1 = 1 if adj1 > 0 else (-1 if adj1 < 0 else 0)
        r2 = 1 if adj2 > 0 else (-1 if adj2 < 0 else 0)
        avg = (r1 + r2) / 2
        
        desc = f"FavDiff={fav_diff} -{line1}/{line2} = {adj1:.1f}/{adj2:.1f}"
        return avg, desc, fav_team
    else:
        result = 1 if adjusted_diff > 0 else (-1 if adjusted_diff < 0 else 0)
        desc = f"FavDiff={fav_diff} -{fav_handicap} = {adjusted_diff:.1f}"
        return result, desc, fav_team

print("=" * 150)
print("DESGLOSE CORREGIDO V2 - BACKTEST AUSTRALIA W-LEAGUE 2024-2025")
print("=" * 150)
print(f"Stake: €{stake} | Cuotas: {odds}")
print(f"ESTRATEGIA: Siempre apostar al FAVORITO")
print()
print(f"{'Fecha':<12} | {'Local':<22} | {'Score':^5} | {'Visitante':<22} | {'AH':>6} | {'Fav':>5} | {'Cálculo':<35} | {'Resultado':>10} | {'€':>7}")
print("-" * 150)

total_profit = 0
wins = 0
losses = 0
pushes = 0

for m in matches:
    parts = m['score'].split(':')
    hg, ag = int(parts[0]), int(parts[1])
    ah = m['ah']
    
    result, calc_desc, fav = calculate_ah_result_v2(hg, ag, ah)
    
    # Calcular profit
    if result > 0:
        if result == 1:
            profit = stake * (odds - 1)
            resultado = "WIN"
            wins += 1
        else:
            profit = stake * (odds - 1) / 2
            resultado = "HALF WIN"
            wins += 0.5
    elif result < 0:
        if result == -1:
            profit = -stake
            resultado = "LOSS"
            losses += 1
        else:
            profit = -stake / 2
            resultado = "HALF LOSS"
            losses += 0.5
    else:
        profit = 0
        resultado = "PUSH"
        pushes += 1
    
    total_profit += profit
    
    print(f"{m['date']:<12} | {m['home'][:22]:<22} | {m['score']:^5} | {m['away'][:22]:<22} | {ah:>6} | {fav:>5} | {calc_desc:<35} | {resultado:>10} | {profit:>7.2f}")

print("-" * 150)
print()
print("=" * 60)
print("RESUMEN FINAL")
print("=" * 60)
print(f"Total apuestas: {len(matches)}")
print(f"Ganadas: {wins} | Perdidas: {losses} | Push: {pushes}")
print(f"Win Rate: {wins/len(matches)*100:.1f}%")
print()
print(f"💰 PROFIT TOTAL: €{total_profit:.2f}")
print(f"📊 ROI: {total_profit/(len(matches)*stake)*100:.1f}%")
