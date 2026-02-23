import json
from pathlib import Path

patterns_dir = Path('data/patterns_v2')

# Analizar distribución
home_total = 0
away_total = 0
home_high_roi = 0  # ROI >= 0.30
away_high_roi = 0

for f in patterns_dir.glob('specialist_ah_*.json'):
    with open(f, 'r', encoding='utf-8') as file:
        data = json.load(file)
        for p in data.get('patterns', []):
            target = p.get('target', '').upper()
            test_roi = p.get('test', {}).get('roi', 0)
            
            if target == 'HOME':
                home_total += 1
                if test_roi >= 0.30:
                    home_high_roi += 1
            elif target == 'AWAY':
                away_total += 1
                if test_roi >= 0.30:
                    away_high_roi += 1

print(f"=== Distribución de Patrones AH ===")
print(f"HOME total: {home_total}")
print(f"AWAY total: {away_total}")
print(f"\nCon ROI >= 30%:")
print(f"HOME: {home_high_roi}")
print(f"AWAY: {away_high_roi}")

# Revisar condiciones más comunes en AWAY
print("\n=== Condiciones más frecuentes en patrones AWAY ===")
away_conditions = {}
for f in patterns_dir.glob('specialist_ah_*.json'):
    with open(f, 'r', encoding='utf-8') as file:
        data = json.load(file)
        for p in data.get('patterns', []):
            if p.get('target', '').upper() == 'AWAY':
                for cond in p.get('conditions', []):
                    away_conditions[cond] = away_conditions.get(cond, 0) + 1

sorted_conds = sorted(away_conditions.items(), key=lambda x: x[1], reverse=True)[:10]
for cond, count in sorted_conds:
    print(f"  {count}x: {cond}")
