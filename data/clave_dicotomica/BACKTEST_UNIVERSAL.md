# Backtest Clave Dicotomica

Motor: {'V7.0': 8617}
Rango: 2024-08-10 a 2026-07-11 (8617 partidos)

## Holdout cronologico 20%

- AH: 64.71% | 207 picks | Wilson 95% 57.62% | settlement medio 0.20773
- O/U: 68.18% | 22 picks | Wilson 95% 47.32% | settlement medio 0.31818

## Bloques temporales

- F1 2024-08-10..2025-12-27: AH 59.03% (255) | OU 66.67% (13)
- F2 2025-12-28..2026-01-20: AH 58.29% (215) | OU 60.0% (10)
- F3 2026-01-22..2026-02-22: AH 58.33% (312) | OU 80.0% (10)
- F4 2026-02-23..2026-03-23: AH 58.6% (167) | OU 77.78% (12)
- F5 2026-03-28..2026-07-11: AH 64.71% (207) | OU 68.18% (22)

## Micro-reglas AH en holdout

- MR-F4 H15_INDIRECTA_VALIDA: 81.25% | n=16 | Wilson=56.99% | settlement=0.625
- MR-D14 AH025+LEAN 69.4%: 73.91% | n=23 | Wilson=53.53% | settlement=0.30435
- MR-F1 LOW+OVER+IND_PUSH 75%: 64.52% | n=37 | Wilson=46.95% | settlement=0.27027
- MR-D7 DOG_NEUTRAL+TABLE 73.3%: 57.58% | n=42 | Wilson=40.81% | settlement=0.05952

## AH por linea exacta en holdout

| Linea local | Picks | Acierto decidido | Push | Wilson 95% | Settlement medio |
|---:|---:|---:|---:|---:|---:|
| +0.25 | 81 | 62.96% | 0 | 52.08% | 0.12963 |
| +0.00 | 58 | 57.89% | 20 | 42.19% | 0.10345 |
| +1.50 | 22 | 72.73% | 0 | 51.85% | 0.45455 |
| +1.25 | 21 | 76.19% | 0 | 54.91% | 0.47619 |
| +0.50 | 17 | 64.71% | 0 | 41.3% | 0.29412 |
| +1.75 | 8 | 62.5% | 0 | 30.57% | 0.1875 |

## AH por nivel de publicacion

- PRODUCTION: 64.71% | n=190 | Wilson=57.27% | settlement=0.2
- PRODUCTION_EXPANSION: 64.71% | n=17 | Wilson=41.3% | settlement=0.29412

## Dependencia de Col3 en holdout

- AH WITH_COL3: 63.19% | n=160 | Wilson=55.07% | settlement=0.1875
- AH WITHOUT_COL3: 69.77% | n=47 | Wilson=54.89% | settlement=0.2766
- O/U WITH_COL3: 64.71% | n=17 | Wilson=41.3% | settlement=0.23529
- O/U WITHOUT_COL3: 80.0% | n=5 | Wilson=37.55% | settlement=0.6

## Confirmacion casa y concordancia Col3

- Casa NEUTRAL: 60.91% | n=125 | Wilson=51.57% | settlement=0.144
- Casa CONFIRM: 69.23% | n=40 | Wilson=53.58% | settlement=0.2625
- Casa CONFLICT: 62.5% | n=28 | Wilson=42.71% | settlement=0.26786
- Casa STRONG_CONFIRM: 85.71% | n=14 | Wilson=60.06% | settlement=0.5
- COL3_NO_BRANCH: 64.43% | n=165 | Wilson=56.47% | settlement=0.2
- COL3_AGREES: 74.07% | n=28 | Wilson=55.32% | settlement=0.39286
- COL3_CONFLICTS: 45.45% | n=14 | Wilson=21.27% | settlement=-0.07143

## Micro-reglas O/U en holdout

- MR-OU2 H025_H2H_UNDER_IND_DOG_POS: 68.18% | n=22 | Wilson=47.32% | settlement=0.31818
