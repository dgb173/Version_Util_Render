# scripts/quick_trainer.py
"""
ENTRENADOR RAPIDO - Version simple que FUNCIONA
"""

import json
import sys
from pathlib import Path

# Flush output inmediatamente
sys.stdout.reconfigure(line_buffering=True)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / 'data'

print("=" * 50)
print("QUICK TRAINER - Iniciando...")
print("=" * 50)
sys.stdout.flush()

# Cargar un archivo
data_file = DATA_DIR / 'data_ah_0.5.json'
print(f"Cargando: {data_file}")
sys.stdout.flush()

if not data_file.exists():
    print(f"ERROR: No existe {data_file}")
    sys.exit(1)

with open(data_file, 'r', encoding='utf-8') as f:
    matches = json.load(f)

print(f"Partidos cargados: {len(matches)}")
sys.stdout.flush()

# Contadores simples
local_wins = 0
visit_wins = 0
overs = 0
unders = 0

for m in matches:
    score = m.get('final_score') or m.get('score', '')
    if ':' not in str(score):
        continue
    
    try:
        parts = str(score).replace('-', ':').split(':')
        h, a = int(parts[0]), int(parts[1])
    except:
        continue
    
    # AH 0.5 = local da 0.5
    adjusted = (h - a) - 0.5
    if adjusted > 0:
        local_wins += 1
    elif adjusted < 0:
        visit_wins += 1
    
    total = h + a
    if total > 2.5:
        overs += 1
    else:
        unders += 1

print(f"\nResultados AH 0.5:")
print(f"  LOCAL cubre: {local_wins} ({local_wins*100/(local_wins+visit_wins):.1f}%)")
print(f"  VISITA cubre: {visit_wins} ({visit_wins*100/(local_wins+visit_wins):.1f}%)")
print(f"\nResultados O/U 2.5:")
print(f"  OVER: {overs} ({overs*100/(overs+unders):.1f}%)")
print(f"  UNDER: {unders} ({unders*100/(overs+unders):.1f}%)")

# Ahora analizar por patrones simples
print("\n" + "=" * 50)
print("Analizando patrones...")
print("=" * 50)
sys.stdout.flush()

patterns = {
    'home_won_prev': {'local': 0, 'visita': 0},
    'away_won_prev': {'local': 0, 'visita': 0},
    'h2h_covered': {'local': 0, 'visita': 0},
    'line_up': {'local': 0, 'visita': 0},
    'line_down': {'local': 0, 'visita': 0},
}

for m in matches:
    score = m.get('final_score') or m.get('score', '')
    if ':' not in str(score):
        continue
    
    try:
        parts = str(score).replace('-', ':').split(':')
        h, a = int(parts[0]), int(parts[1])
    except:
        continue
    
    adjusted = (h - a) - 0.5
    if adjusted == 0:
        continue
    
    result = 'local' if adjusted > 0 else 'visita'
    
    # Prev home
    prev_home = m.get('last_home_match') or {}
    prev_score = prev_home.get('score', '')
    if ':' in str(prev_score):
        try:
            ps = str(prev_score).replace('-', ':').split(':')
            ph, pa = int(ps[0]), int(ps[1])
            if ph > pa:
                patterns['home_won_prev'][result] += 1
        except:
            pass
    
    # Prev away
    prev_away = m.get('last_away_match') or {}
    prev_score = prev_away.get('score', '')
    if ':' in str(prev_score):
        try:
            ps = str(prev_score).replace('-', ':').split(':')
            ph, pa = int(ps[0]), int(ps[1])
            if pa > ph:  # Visitante gano
                patterns['away_won_prev'][result] += 1
        except:
            pass
    
    # H2H covered
    market = m.get('market_analysis_data') or {}
    stadium = market.get('stadium') or {}
    if stadium.get('is_covered') == True:
        patterns['h2h_covered'][result] += 1
    
    # Line movement
    mov = stadium.get('movement', '')
    if '->' in mov or '>' in mov:
        parts = mov.replace('>', '->').split('->')
        if len(parts) == 2:
            try:
                before = float(parts[0].strip())
                after = float(parts[1].strip())
                if after > before:
                    patterns['line_up'][result] += 1
                elif after < before:
                    patterns['line_down'][result] += 1
            except:
                pass

print("\nPatrones encontrados:")
for name, data in patterns.items():
    total = data['local'] + data['visita']
    if total >= 20:
        local_pct = data['local'] * 100 / total
        visita_pct = data['visita'] * 100 / total
        winner = 'LOCAL' if local_pct > visita_pct else 'VISITA'
        pct = max(local_pct, visita_pct)
        print(f"  {name}: {winner} {pct:.1f}% (n={total})")
    else:
        print(f"  {name}: n={total} (pocos datos)")

print("\n[OK] Terminado!")
sys.stdout.flush()
