# Autoentrenador universal AH + O/U

Este módulo convierte la lógica de lectura de expectativas en un proceso repetible y auditable. No promete pronósticos infalibles: un sistema honesto debe poder devolver `NO BET`.

## Qué aprende

- Convención de la base: AH positivo = favorito local; AH negativo = favorito visitante.
- Residual de expectativa: `margen del equipo - fuerza exigida por su línea`.
- Revalorización frente a H2H comparables.
- Líneas contra rivales comunes desde la perspectiva de cada equipo y respetando localía.
- Clasificación general y rendimiento específico local/visitante.
- Victoria como no favorito con dominio real o con eficacia inflada.
- Resistencia del visitante fuera y debilidad del local en casa.
- Inflación del total por marcadores extremos y producción ofensiva real.

AH y O/U se entrenan por separado. Los tiros y ataques peligrosos confirman sostenibilidad; no sustituyen al precio del mercado.

## Protección contra autoengaño

1. Deduplicación por `match_id`.
2. Bloques históricos posteriores al partido actual se ignoran.
3. División cronológica 64% entrenamiento, 16% validación y 20% prueba intocable.
4. El umbral de apuesta se elige solo en validación.
5. Si la prueba intocable no es positiva o no tiene soporte suficiente, el mercado queda desactivado y produce `NO BET`.
6. Los beneficios publicados suponen cuota decimal 2.00 porque la base no conserva todas las cuotas históricas. No deben llamarse ROI real.

## Entrenar

```powershell
py scripts/universal_autotrainer/train.py
```

Artefactos:

- `models/universal_autotrainer/universal_market_model.joblib`
- `models/universal_autotrainer/metrics.json`
- `models/universal_autotrainer/patterns.json`
- `models/universal_autotrainer/features.json`

## Predecir

```powershell
py scripts/universal_autotrainer/predict.py --match-id 3032193
```

También admite `--team-query "nombre"` y `--output-json archivo.json`.

## Autoentrenamiento periódico

El proceso seguro es: actualizar resultados en `app_data.db`, volver a ejecutar `train.py` y aceptar el nuevo artefacto solo si la prueba temporal intocable mantiene beneficio positivo y soporte. No se modifican reglas manualmente después de mirar el test.

Ciclo completo (entrenar + auditar + pronosticar todo el precacheo):

```powershell
py scripts/universal_autotrainer/run_cycle.py
```

Para generar el informe diario sin volver a entrenar:

```powershell
py scripts/universal_autotrainer/run_cycle.py --skip-train
```

Los informes se guardan en `reports/universal_autotrainer/<fecha_hora>/`.

Para rentabilidad real falta guardar por partido las cuotas AH de ambos lados y las cuotas Over/Under en el instante exacto de la predicción. Hasta entonces, la métrica es beneficio unitario simulado a cuota 2.00.
