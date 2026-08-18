# Backtest Clave Dicotomica

Motor: {'V7.0': 8578}
Rango: 2024-08-10 a 2026-06-30 (8578 partidos)

## Holdout cronologico 20%

- AH: 58.5% | 427 picks | Wilson 95% 53.61% | settlement medio 0.11827
- O/U: 68.18% | 22 picks | Wilson 95% 47.32% | settlement medio 0.31818

## Bloques temporales

- F1 2024-08-10..2025-12-27: AH 54.43% (445) | OU 66.67% (13)
- F2 2025-12-28..2026-01-19: AH 54.94% (385) | OU 60.0% (10)
- F3 2026-01-20..2026-02-22: AH 54.91% (584) | OU 80.0% (10)
- F4 2026-02-23..2026-03-22: AH 55.59% (354) | OU 77.78% (12)
- F5 2026-03-23..2026-06-30: AH 58.5% (427) | OU 68.18% (22)

## Micro-reglas AH en holdout

- MR-D14 AH025+LEAN 69.4%: 76.0% | n=25 | Wilson=56.57% | settlement=0.34
- MR-F9 H05_LINEA_IGUAL_DOG_EMPATA: 72.22% | n=18 | Wilson=49.13% | settlement=0.38889
- MR-F4 H15_INDIRECTA_VALIDA: 68.18% | n=22 | Wilson=47.32% | settlement=0.36364
- MR-F2 LOW+NEUTRAL+STRONG_AGAINST 73.1%: 75.0% | n=13 | Wilson=46.77% | settlement=0.34615
- MR-F3 H05+UNDER+IND_NEUTRAL 70.4%: 61.54% | n=39 | Wilson=45.9% | settlement=0.16667
- MR-D7 DOG_NEUTRAL+TABLE 73.3%: 57.78% | n=56 | Wilson=43.3% | settlement=0.08036
- MR-F1 LOW+OVER+IND_PUSH 75%: 53.45% | n=64 | Wilson=40.8% | settlement=0.08594

## Micro-reglas O/U en holdout

- MR-OU2 H025_H2H_UNDER_IND_DOG_POS: 68.18% | n=22 | Wilson=47.32% | settlement=0.31818
