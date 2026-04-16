# Backtest por Handicap

- Partidos evaluados: 209
- Picks OU: 141 (hit=49.65%)
- Picks AH: 172 (hit=50.58%)
- Doble pick (OU+AH): 114 (hit conjunto=28.95%)

## Resultados por handicap

| Handicap | Test | OU Bets | OU Hit% | AH Bets | AH Hit% | Doble Bets | Doble Hit% |
|---:|---:|---:|---:|---:|---:|---:|---:|
| -1.5 | 2 | 2 | 50.0 | 2 | 0.0 | 2 | 0.0 |
| -1 | 1 | 1 | 0.0 | 0 | None | 0 | None |
| 0.5 | 176 | 118 | 49.15 | 147 | 48.3 | 95 | 25.26 |
| 1 | 10 | 5 | 40.0 | 7 | 71.43 | 5 | 40.0 |
| 1.5 | 20 | 15 | 60.0 | 16 | 68.75 | 12 | 58.33 |

## Config usada

```json
{
  "max_matches": 500,
  "min_history": 500,
  "min_support": 10,
  "min_similarity": 32.0,
  "max_similars": 450,
  "ah_max_gap": 0.25,
  "ou_max_gap": 0.75,
  "min_handicap_score": 8.0,
  "min_stats_blocks": 2,
  "conversation_strength": 0.65,
  "conversation_learning_rate": 0.35
}
```