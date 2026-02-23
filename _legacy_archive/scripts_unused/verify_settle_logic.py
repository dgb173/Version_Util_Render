#!/usr/bin/env python
"""
Verificación de la lógica de settle_ah
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.pattern_miner_v2.settle_asian import settle_ah

print("="*60)
print("VERIFICACIÓN DE LÓGICA SETTLE_AH")
print("="*60)

# Caso: Local Favorito con AH -0.5 (convención settle_ah)
# Para cubrir, el Local DEBE ganar
print("\n[CASO 1] AH = -0.5 (Local favorito, convención settle_ah)")
print("-"*40)

# Victoria Local 2-1
result, profit = settle_ah(2, 1, -0.5)
print(f"Local 2-1 Away: {result}, profit={profit} (debería ser WIN)")

# Empate 1-1
result, profit = settle_ah(1, 1, -0.5)
print(f"Local 1-1 Away: {result}, profit={profit} (debería ser LOSS)")

# Derrota Local 0-1
result, profit = settle_ah(0, 1, -0.5)
print(f"Local 0-1 Away: {result}, profit={profit} (debería ser LOSS)")

# Caso: NowGoal usa AH +0.5 para Local Favorito?
print("\n[CASO 2] AH = +0.5 (¿convención NowGoal?)")
print("-"*40)

# Victoria Local 2-1
result, profit = settle_ah(2, 1, 0.5)
print(f"Local 2-1 Away: {result}, profit={profit}")

# Empate 1-1
result, profit = settle_ah(1, 1, 0.5)
print(f"Local 1-1 Away: {result}, profit={profit}")

# Derrota Local 0-1
result, profit = settle_ah(0, 1, 0.5)
print(f"Local 0-1 Away: {result}, profit={profit}")

print("\n" + "="*60)
print("CONCLUSIÓN:")
print("Si NowGoal usa +0.5 para Local Favorito, entonces:")
print("- profit > 0 = Local cubrió")
print("- profit < 0 = Local NO cubrió")
print("Por tanto NO debo invertir el profit en el entrenamiento.")
print("="*60)
