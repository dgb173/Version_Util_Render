"""
Script para verificar la lógica de Asian Handicap con casos concretos.
"""

def test_ah_logic():
    """
    Asian Handicap en Nowgoal:
    
    El valor AH que nos da Nowgoal es DESDE LA PERSPECTIVA DEL LOCAL.
    
    - AH = +0.5 significa: LOCAL recibe +0.5 de ventaja
      → Por tanto, VISITANTE es favorito (da ventaja)
      → Para que LOCAL cubra: (home - away) + 0.5 > 0
      
    - AH = -0.5 significa: LOCAL da -0.5 de ventaja  
      → Por tanto, LOCAL es favorito
      → Para que LOCAL cubra: (home - away) - 0.5 > 0, es decir home > away + 0.5
      
    - AH = 0 significa: Pick'em (ni favorito ni underdog)
    """
    
    print("=" * 70)
    print("VERIFICACION DE LOGICA ASIAN HANDICAP")
    print("=" * 70)
    
    # Casos de prueba: (home_goals, away_goals, ah_line, expected_local_cover, expected_away_cover)
    test_cases = [
        # Caso 1: Local gana 2-1, AH +0.5 (local recibe ventaja, away favorito)
        # diff = (2-1) + 0.5 = 1.5 > 0 → LOCAL CUBRE
        (2, 1, 0.5, True, False, "Local gana 2-1, recibe +0.5"),
        
        # Caso 2: Local pierde 0-1, AH +0.5
        # diff = (0-1) + 0.5 = -0.5 < 0 → AWAY CUBRE
        (0, 1, 0.5, False, True, "Local pierde 0-1, recibe +0.5"),
        
        # Caso 3: Empate 1-1, AH +0.5
        # diff = (1-1) + 0.5 = 0.5 > 0 → LOCAL CUBRE (tiene la ventaja del handicap)
        (1, 1, 0.5, True, False, "Empate 1-1, local recibe +0.5"),
        
        # Caso 4: Local gana 2-1, AH -0.5 (local da ventaja, es favorito)
        # diff = (2-1) - 0.5 = 0.5 > 0 → LOCAL CUBRE
        (2, 1, -0.5, True, False, "Local gana 2-1, da -0.5"),
        
        # Caso 5: Empate 1-1, AH -0.5
        # diff = (1-1) - 0.5 = -0.5 < 0 → AWAY CUBRE
        (1, 1, -0.5, False, True, "Empate 1-1, local da -0.5"),
        
        # Caso 6: Local pierde 1-3, AH -1.0
        # diff = (1-3) - 1.0 = -3 < 0 → AWAY CUBRE
        (1, 3, -1.0, False, True, "Local pierde 1-3, da -1.0"),
        
        # Caso 7: Local pierde 1-3, AH +1.0
        # diff = (1-3) + 1.0 = -1 < 0 → AWAY CUBRE
        (1, 3, 1.0, False, True, "Local pierde 1-3, recibe +1.0"),
        
        # Caso 8: Local pierde 2-3, AH +1.0
        # diff = (2-3) + 1.0 = 0 = PUSH
        (2, 3, 1.0, None, None, "Local pierde 2-3, recibe +1.0 (PUSH)"),
        
        # Caso 9: Local gana 3-1, AH -1.0
        # diff = (3-1) - 1.0 = 1 > 0 → LOCAL CUBRE
        (3, 1, -1.0, True, False, "Local gana 3-1, da -1.0"),
        
        # Caso 10: Local gana 2-1, AH -1.0
        # diff = (2-1) - 1.0 = 0 = PUSH
        (2, 1, -1.0, None, None, "Local gana 2-1, da -1.0 (PUSH)"),
    ]
    
    def settle_ah(home_g, away_g, ah_line):
        """Calcula settlement desde perspectiva del LOCAL."""
        diff = (home_g - away_g) + ah_line
        if diff > 0.25:
            return 'LOCAL_WIN', 1.0
        elif diff > 0:
            return 'LOCAL_HALF_WIN', 0.5
        elif diff == 0:
            return 'PUSH', 0.0
        elif diff >= -0.25:
            return 'LOCAL_HALF_LOSS', -0.5
        else:
            return 'LOCAL_LOSS', -1.0
    
    print("\nPRUEBAS DE SETTLEMENT:")
    print("-" * 70)
    
    all_pass = True
    for home, away, ah, exp_local, exp_away, desc in test_cases:
        result, payout = settle_ah(home, away, ah)
        
        # Verificar
        if exp_local is None:  # PUSH
            passed = 'PUSH' in result
        elif exp_local:
            passed = 'LOCAL' in result and 'LOSS' not in result
        else:
            passed = 'LOCAL' in result and 'LOSS' in result
        
        emoji = "[OK]" if passed else "[XX]"
        print(f"{emoji} {desc}")
        print(f"    Score: {home}-{away}, AH: {ah:+.2f}")
        print(f"    diff = ({home}-{away}) + ({ah}) = {(home-away)+ah:.2f}")
        print(f"    Resultado: {result}")
        print()
        
        if not passed:
            all_pass = False
    
    print("=" * 70)
    if all_pass:
        print("TODAS LAS PRUEBAS PASARON")
    else:
        print("ALGUNAS PRUEBAS FALLARON - REVISAR LOGICA")
    print("=" * 70)
    
    # Ahora verificar la logica de apostar
    print("\n\nVERIFICACION DE LOGICA DE APUESTAS:")
    print("-" * 70)
    
    print("""
CUANDO APOSTAMOS AL LOCAL (HOME):
- Ganamos si el LOCAL cubre el handicap
- Es decir, si (home - away) + AH > 0

CUANDO APOSTAMOS AL VISITANTE (AWAY):
- Ganamos si el LOCAL NO cubre el handicap
- Es decir, si (home - away) + AH < 0

QUIEN ES FAVORITO:
- Si AH < 0: LOCAL es favorito (da ventaja)
- Si AH > 0: AWAY es favorito (LOCAL recibe ventaja)
- Si AH = 0: Pick'em

EJEMPLO:
- Partido con AH = -1.0 (LOCAL da 1 gol de ventaja, ES FAVORITO)
- Si apostamos al AWAY, ganamos si el local no cubre su -1.0
- Score 1-0: diff = (1-0) + (-1.0) = 0 = PUSH
- Score 1-1: diff = (1-1) + (-1.0) = -1 < 0 = AWAY GANA
- Score 2-1: diff = (2-1) + (-1.0) = 0 = PUSH
- Score 3-1: diff = (3-1) + (-1.0) = 1 > 0 = LOCAL GANA

LA REGLA MEGA_110%_A dice:
  fav_side = HOME + apuesta AWAY
  Significa: Cuando el LOCAL es favorito (AH negativo), apostar al VISITANTE
  Esto tiene sentido si el patron detecta que los favoritos no cubren.
""")


if __name__ == '__main__':
    test_ah_logic()
