# Sistema de Lectura de Partidos (Explorador + AutoMejora)

Este sistema crea argumentos tipo conversacion ("jerarquia + mercado + volumen") y se autoajusta con feedback real.

## 1) Analisis profundo de un partido (modo conversacion v4.4)

```bash
py scripts/explorador_automejora/analyze_single_match_deep.py --project-root . --date today --team-query "hask" --output-md report_hask.md --output-json report_hask.json
```

Opciones utiles:

- `--match-id 2872047` para fijar partido exacto.
- `--conversation-strength 0.65` para subir/bajar peso de la regla `volumen > eficiencia`.
- `--actual-score "2:2"` para registrar feedback real y autocalibrar el modelo.
- `--learning-state data/explorador_automejora_state.json` para cambiar archivo de estado.

## 2) Razonamiento matematico puro de handicap

Pensado para partidos donde quieres priorizar la linea como variable madre y
explicar el pick por residuales de handicap, movimiento de mercado, rival comun
y sesgo de empate.

```bash
py scripts/explorador_automejora/analyze_handicap_math.py --project-root . --team-query "cd choloma" --output-md report_choloma_math.md --output-json report_choloma_math.json
```

Salida:

- pick recomendado (`0`, `-0.25`, `+0.25` o `NO BET`)
- edge local vs visitante
- riesgo de empate
- tabla de bloques con contribucion numerica
- residuales de handicap por `prev_home`, `prev_away`, `h2h` e indirectas

## 3) Top diario Favorito + Over

```bash
py scripts/explorador_automejora/generate_today_fav_over_report.py --project-root . --date today --top 15 --min-support 12
```

## 4) Top diario No Favorito + Over

```bash
py scripts/explorador_automejora/generate_today_underdog_over_report.py --project-root . --date today --top 15 --min-support 12
```

## 5) Estado de aprendizaje

El estado persistente queda en:

- `data/explorador_automejora_state.json`

Incluye:

- calibracion AH 0.5 por contexto.
- calibracion OU por patrones de volumen (0 goles + alto volumen, bajo volumen, etc.).
- historial de feedback para evitar doble conteo por `match_id`.

## 6) Backtest por handicap (rigor por linea)

```bash
py scripts/explorador_automejora/backtest_by_handicap.py --project-root . --max-matches 500 --min-history 500
```

Salidas:

- `scripts/explorador_automejora/backtest_handicap.json`
- `scripts/explorador_automejora/backtest_handicap.md`

## 7) Leyes practicas de handicap

Referencia viva para convertir casos practicos en reglas reutilizables:

- `scripts/explorador_automejora/references/handicap_laws.md`
