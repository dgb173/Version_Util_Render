import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.rule_miner import Rule
import pandas as pd
import numpy as np

print("Testing Rule evaluation...")
df = pd.DataFrame({'a': [1, 2, 3], 'ah_payout': [1.0, -0.5, 0.0]})
rule = Rule([{'feature': 'a', 'op': '>', 'value': 0}], 'AH', 'HOME')
print("Evaluating...")
res = rule.evaluate(df)
print(f"Result: {res}")
print("Test Done")
