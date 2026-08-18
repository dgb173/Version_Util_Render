# Backtest Clave Dicotomica

Motor: {'V6': 7231, 'V7.0': 1347}
Rango: 2024-08-10 a 2026-06-30 (8578 partidos)

## Holdout cronologico 20%

- AH: 54.04% | 910 picks | Wilson 95% 50.69% | settlement medio 0.0522
- O/U: 50.75% | 788 picks | Wilson 95% 47.13% | settlement medio -0.00635

## Bloques temporales

- F1 2024-08-10..2025-12-27: AH 50.25% (898) | OU 48.59% (825)
- F2 2025-12-28..2026-01-19: AH 51.41% (832) | OU 53.7% (727)
- F3 2026-01-20..2026-02-22: AH 52.05% (1206) | OU 49.66% (1107)
- F4 2026-02-23..2026-03-22: AH 53.36% (726) | OU 50.63% (665)
- F5 2026-03-23..2026-06-30: AH 54.04% (910) | OU 50.75% (788)

## Micro-reglas AH en holdout

- MR-D15 H2H_OVER+OU4 69.2%: 80.0% | n=10 | Wilson=49.02% | settlement=0.45
- MR-D14 AH025+LEAN 69.4%: 62.26% | n=53 | Wilson=48.81% | settlement=0.13208
- MR-F3 H05+UNDER+IND_NEUTRAL 70.4%: 58.14% | n=86 | Wilson=47.58% | settlement=0.12209
- MR-F4 H15+IND_VALIDA 68.8%: 66.67% | n=24 | Wilson=46.71% | settlement=0.33333
- MR-F1 LOW+OVER+IND_PUSH 75%: 52.94% | n=148 | Wilson=44.59% | settlement=0.06757
- MR-F9 H05+SAME+DOG_DRAW 65.6%: 65.0% | n=20 | Wilson=43.29% | settlement=0.25
- MR-F2 LOW+NEUTRAL+STRONG_AGAINST 73.1%: 57.14% | n=45 | Wilson=42.21% | settlement=0.12222
- MR-D5 AH025+STATS+IND_D 75%: 64.71% | n=17 | Wilson=41.3% | settlement=0.23529
- MR-D16 COVER+OU4 69.2%: 66.67% | n=13 | Wilson=39.06% | settlement=0.23077
- MR-D7 DOG_NEUTRAL+TABLE 73.3%: 47.5% | n=95 | Wilson=36.92% | settlement=-0.06316
- MR-D8 NEW_FAV+IND_FAIL 73.3%: 61.54% | n=14 | Wilson=35.52% | settlement=0.21429
- MR-D4 RAISE+DOG_CERRADO 76.5%: 51.61% | n=32 | Wilson=34.84% | settlement=0.0
- MR-D1 TRAMPA NARRATIVA 80%: 47.62% | n=21 | Wilson=28.34% | settlement=-0.09524

## Micro-reglas O/U en holdout

- MR-OU3 LOW+NEWFAV+DOG_UNDER 82.1%: 55.66% | n=114 | Wilson=46.17% | settlement=0.08772
- MR-OU2 AH025+UNDER+IND_D_POS1 84.6%: 61.54% | n=41 | Wilson=45.9% | settlement=0.15854
- MR-OU7 NEWFAV+STATS+OULOW 72.7%: 54.1% | n=68 | Wilson=41.72% | settlement=0.0
- MR-OV1 H025+GOALS4+ 70%: 58.33% | n=26 | Wilson=38.83% | settlement=0.11538
- MR-OU1 UNDER 85.7%: 54.84% | n=31 | Wilson=37.77% | settlement=0.12903
- H025-9 OU_ALTO_CONTRAINTUITIVO: 46.67% | n=45 | Wilson=32.93% | settlement=-0.02222
- MR-OU5 FAIL+IND_D+OUHIGH 75%: 46.67% | n=20 | Wilson=24.81% | settlement=-0.05
