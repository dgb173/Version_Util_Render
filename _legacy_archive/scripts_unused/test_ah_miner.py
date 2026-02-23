import sys
sys.path.insert(0, '.')
from scripts.pattern_miner_v2.features_builder_v2 import load_all_training_data, build_training_dataframe
from scripts.pattern_miner_v2.rule_miner import PatternMinerV2

print('Cargando...')
matches = load_all_training_data('data')
df = build_training_dataframe(matches)

# Filtrar solo familia H0.5 (tiene 1345 partidos)
family = 'H0.5'
family_df = df[df['ah_family'] == family].copy()
print(f'Familia {family}: {len(family_df)} partidos')

# Criterios mas relajados para encontrar patrones
config = {
    'min_samples': 15,
    'min_samples_test': 15,
    'min_accuracy': 0.55,  # Reducido a 55%
    'min_roi_train': 0.10,  # Reducido a 10%
    'min_roi_oos': 0.15,   # Reducido a 15%
    'generations': 2000,   # Mas generaciones
    'min_features': 2,
    'max_features': 4,
    'max_degradation': 0.6  # Mas tolerante
}

miner = PatternMinerV2(config)
print('Minando patrones AH con criterios relajados...')
try:
    patterns = miner.mine_ah_patterns(family_df, generations=2000)
    print(f'Patrones encontrados: {len(patterns)}')
    
    # Filtrar por ROI >= 20% en test
    good_patterns = [p for p in patterns if p['test']['roi'] >= 0.20 and p['test']['n'] >= 15]
    print(f'Patrones con ROI>=20% y N>=15: {len(good_patterns)}')
    
    if good_patterns:
        for p in good_patterns[:5]:
            roi_train = p['train']['roi']
            roi_test = p['test']['roi']
            n_test = p['test']['n']
            print(f"  {p['target']}: ROI test={roi_test*100:.1f}% (N={n_test})")
except Exception as e:
    print(f'ERROR: {e}')
    import traceback
    traceback.print_exc()
