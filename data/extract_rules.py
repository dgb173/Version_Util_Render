import json

data=json.load(open('sistema_binario_real_v2_diag62.json',encoding='utf-8'))
rules_side=data['rules']['side']
by_ah=data['by_ah']
csv_data=open('../data/features_matrix.csv').read()

print('=== BASELINES POR FAMILIA AH ===')
for fam in by_ah:
    fav_pct=fam['favorite']['hit_rate']
    dog_pct=fam['dog']['hit_rate']
    und_pct=fam['under']['hit_rate']
    ov_pct=fam['over']['hit_rate']
    rows=fam['rows']
    print(f"{fam['ah_family']:20} | n={rows:4} | FAV={fav_pct:.1f}% | DOG={dog_pct:.1f}% | UNDER={und_pct:.1f}% | OVER={ov_pct:.1f}%")

print('\n=== TOP 80 REGLAS SIDE (ordenadas por validacion) ===')
for r in sorted(rules_side, key=lambda x:-x['validation']['hit_rate']):
    pct=r['validation']['hit_rate']
    w=r['validation']['wins']
    n=r['validation']['bets']
    lift=r.get('validation_lift',0)
    keys=' + '.join(r['key'])
    direction=r['direction']
    train_pct=r['train']['hit_rate']
    gap=round(abs(pct-train_pct),1)
    print(direction + ' | ' + str(round(pct,1)) + '% | ' + str(w) + '/' + str(n) + ' | train=' + str(round(train_pct,1)) + '% | gap=' + str(gap) + ' | lift=' + str(round(lift,1)) + ' | ' + keys)
