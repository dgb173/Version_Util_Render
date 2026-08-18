# Backtest Clave Dicotomica

Motor: {'V6': 8578}
Rango: 2024-08-10 a 2026-06-30 (8578 partidos)

## Holdout cronologico 20%

- AH: 54.8% | 904 picks | Wilson 95% 51.43% | settlement medio 0.06527
- O/U: 51.17% | 737 picks | Wilson 95% 47.43% | settlement medio -0.00271

## Bloques temporales

- F1 2024-08-10..2025-12-27: AH 51.93% (881) | OU 48.91% (773)
- F2 2025-12-28..2026-01-19: AH 51.61% (831) | OU 53.21% (677)
- F3 2026-01-20..2026-02-22: AH 52.79% (1216) | OU 49.9% (1024)
- F4 2026-02-23..2026-03-22: AH 51.59% (734) | OU 50.17% (613)
- F5 2026-03-23..2026-06-30: AH 54.8% (904) | OU 51.17% (737)

## Micro-reglas AH en holdout

- MR-D15 H2H_OVER+OU4 69.2%: 83.33% | n=13 | Wilson=55.2% | settlement=0.5
- MR-D14 AH025+LEAN 69.4%: 62.75% | n=51 | Wilson=49.02% | settlement=0.14706
- MR-F3 H05+UNDER+IND_NEUTRAL 70.4%: 57.65% | n=85 | Wilson=47.04% | settlement=0.11176
- MR-D4 RAISE+DOG_CERRADO 76.5%: 64.52% | n=32 | Wilson=46.95% | settlement=0.23438
- MR-F4 H15+IND_VALIDA 68.8%: 66.67% | n=24 | Wilson=46.71% | settlement=0.33333
- MR-F1 LOW+OVER+IND_PUSH 75%: 52.11% | n=154 | Wilson=43.95% | settlement=0.05519
- MR-F9 H05+SAME+DOG_DRAW 65.6%: 55.56% | n=54 | Wilson=42.38% | settlement=0.08333
- MR-D16 COVER+OU4 69.2%: 69.23% | n=15 | Wilson=42.37% | settlement=0.23333
- MR-F2 LOW+NEUTRAL+STRONG_AGAINST 73.1%: 54.55% | n=48 | Wilson=40.07% | settlement=0.07292
- MR-D3 INFLACION H2+OU4 78.6%: 64.29% | n=15 | Wilson=38.76% | settlement=0.16667
- MR-D5 AH025+STATS+IND_D 75%: 62.5% | n=16 | Wilson=38.64% | settlement=0.21875
- MR-D2 INFLACION H2+ 78.6%: 61.11% | n=19 | Wilson=38.62% | settlement=0.13158
- MR-D7 DOG_NEUTRAL+TABLE 73.3%: 46.67% | n=90 | Wilson=35.82% | settlement=-0.06667
- MR-D1 TRAMPA NARRATIVA 80%: 36.84% | n=19 | Wilson=19.15% | settlement=-0.31579

## Micro-reglas O/U en holdout

- MR-OU2 AH025+UNDER+IND_D_POS1 84.6%: 61.54% | n=41 | Wilson=45.9% | settlement=0.15854
- MR-OV1 H025+GOALS4+ 70%: 60.87% | n=24 | Wilson=40.79% | settlement=0.16667
- MR-OU1 UNDER 85.7%: 54.84% | n=31 | Wilson=37.77% | settlement=0.12903
- MR-OU5 FAIL+IND_D+OUHIGH 75%: 53.33% | n=20 | Wilson=30.12% | settlement=0.05
- H025-9 OU_ALTO_CONTRAINTUITIVO: 44.12% | n=34 | Wilson=28.88% | settlement=-0.07353
