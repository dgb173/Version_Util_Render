# Motor dual entero-binario v6.2 OPERATIVO

Motor determinista de 12 compuertas para hándicap asiático y totales FT90. Usa los datos de la plataforma: tiros, tiros a puerta, eficacia, roles, dos bloques de similares, referencias, H2H, rivales comunes y puentes espejo. Incluye diagnóstico direccional, línea objetivo, liquidación y feedback de Gemini. No contiene un modelo de Poisson ni devuelve `ABSTAIN_UNVALIDATED_MODEL`.

Los ejemplos son **sintéticos**. Un voto binario es una condición cumplida; 5/6 no significa 83,3% de probabilidad ni beneficio esperado positivo. La cuota y su disponibilidad se muestran por separado. No se ejecutan apuestas.

## Ejecutar

Python 3.11 o superior. Desde esta carpeta:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
python demo.py
python run_engine.py --input examples/dominio.json --out outputs/mi_analisis.json
```

En Windows, activar con `.venv\Scripts\activate`.

La demo genera cuatro snapshots completos y sus salidas. En `dominio` se obtienen b=[1,1,1,0,1,1], g=[1,1,1,0,1,1], Local -0.5 y Over 2.5. Sus cuotas 1.72 y 1.83 son ficticias. `cerrojo` produce Under 3.0. `linea_larga` aplica el veto al favorito -1.5. `datos_parciales` conserva dirección y muestra la máscara de datos ausentes.

## Datos propios

1. Usar `examples/snapshot.schema.json` y `examples/dominio.json` como contrato y ejemplo.
2. Sustituir event_id, equipos, competición, fechas, proveedor, líneas y filas. Marcar `synthetic: false` solamente para datos reales.
3. Registrar el momento en que cada dato estuvo disponible. Todos los datos deben existir antes de `decision_at`, que debe ser anterior a `kickoff`.
4. `raw_home_ah` conserva el signo de la plataforma: positivo significa favorito local. Las cuotas `AH_HOME`/`AH_AWAY` usan la línea canónica del equipo seleccionado: -0.75 se guarda como `line_q: -3`.
5. Una fila histórica conserva el AH del anfitrión. Su campo `venue` indica si el equipo analizado fue local o visitante; Python invierte la perspectiva automáticamente.
6. `shot_goals_for` y `shot_goals_against` son goles atribuibles a tiros; excluir autogoles. Si no se sabe, usar null: el código no supone que todos los goles del marcador sean tiros registrados.
7. Los bloques similares requieren N y sus IDs. No asignar 0% a N=0. Si hay solapamiento entre muestras, desduplicar las filas antes de agregarlas; el motor no puede reconstruir una media desduplicada desde totales agregados.
8. Las referencias duplicadas se cuentan una sola vez. `bridges` permite incluir el partido espejo entre los rivales previos. Los IDs, resultados y líneas deben ser coherentes.
9. `league_sot_per_team` y `league_conversion` deben provenir de datos históricos del mismo proveedor, competición y definición, disponibles antes del partido. Los valores 4.5 y 0.30 de la demo no son constantes universales.
10. Las cuotas alternativas se aportan explícitamente. Si falta una cuota para la línea objetivo, se devuelve `TARGET_UNQUOTED`; el diagnóstico sigue presente.

## Qué decide el motor

- Cada compuerta tiene `bit`, `available`, `reason` y `values`. Un bit 0 con `available: false` no vota. Se muestran votos, número de compuertas válidas y puntuación normalizada a seis.
- En AH, puntuación >3 favorece al favorito; <3 al no favorito. En 3 exactos, se compara el margen proyectado por tiros con la exigencia AH; después se usa triangulación y, por último, el no favorito como política de desempate.
- En O/U, puntuación >3 favorece Over; <3 Under. En 3 exactos, se usa la proyección total por tiros; después referencias y, finalmente, Under como política residual. Estos desempates son decisiones de ingeniería explícitas.
- Las políticas posteriores no reescriben bits: la traza identifica doble correlación, acoplamiento Over→AH, piso condicional, referencias, desplazamiento de línea, cerrojo y veto de línea larga.
- El cerrojo confirmado tiene prioridad sobre un veto anti-Under. El veto de favorito largo con empates tiene prioridad final en AH. Son resoluciones explícitas de conflictos que el documento original no ordenaba.
- El peso 65/35 se aplica a las estimaciones de forma usadas en la proyección de tiros. Las seis compuertas mantienen votos iguales; no se afirma que b5 tenga por sí sola el 65% de la decisión.
- Las proyecciones de goles son aproximaciones por ratios, no xG ni una distribución de marcadores. La condición 1+2=3 es un escenario; una frecuencia pasada de marcar del 75% no garantiza un gol futuro.

## Configuración y alcance

`examples/config.json` contiene los parámetros editables: ventanas 21/90 días, máximo de cinco partidos activos, peso .65, tolerancia de rol 3 cuartos, ventaja local 2 cuartos, prior de conversión 20 tiros, mínimo activo 3 y cortes de SOT/AP/tiros/eficacia. Los demás umbrales del documento están fijados en `engine.py` y pertenecen a la versión del motor. Modificarlos requiere una versión nueva.

Una ventana de cinco partidos puede abarcar menos de 21 días; si hay menos de cinco partidos dentro del plazo, no se inventan los restantes. Se excluyen filiales/U20 para encuentros de primera plantilla y, por defecto, partidos marcados con contexto de expulsión. Esta última exclusión es una elección añadida y configurable.

La triangulación usa hasta 90 días y conserva roles distintos para medir la brecha de tasación. Usa el cruce más reciente con cada rival; el filtro de rol se reserva a la telemetría. Los caminos espejo pueden compartir partidos y no son observaciones independientes.

Para AH=0 no existe favorito de mercado: se usa el local como ancla técnica (`pickem_anchor: true`). Las salidas deben presentarse como Local/Visitante en ese caso.

## Feedback de Gemini

Preparar la petición sin conexión:

```bash
python review_gemini.py --packet outputs/mi_analisis.json --out outputs/peticion.json
```

Ejecutar una revisión real, una vez configuradas `GEMINI_API_KEY` y `GEMINI_MODEL` en el entorno:

```bash
python -m pip install -r requirements-gemini.txt
python review_gemini.py --packet outputs/mi_analisis.json --out outputs/revision.json --live
```

Elegir en Google AI Studio un modelo disponible con salida estructurada. No colocar la clave en el JSON, en JavaScript ni en el informe. La API puede generar consumo. La llamada principal usa Interactions, `store=False`, JSON Schema y Pydantic. Se conserva una alternativa explícita `--transport generate_content`; no hay cambio automático de transporte. El código no implementa reintentos propios.

Gemini revisa exactamente las compuertas calculadas, aporta referencias a datos del paquete y propone pruebas. No modifica el motor ni sustituye sus picks. El hash y las referencias se validan en Python. El SDK/conexión real no se han probado con una cuenta; el contrato se probó con un cliente simulado. El JSON bien formado no garantiza que el razonamiento sea correcto.

## Resultado y aprendizaje

```bash
python postmatch.py --packet outputs/dominio.json --result examples/result_demo.json --out outputs/postmatch_demo.json
python review_gemini.py --packet outputs/postmatch_demo.json --out outputs/postmatch_request.json
```

`postmatch.py` conserva el paquete previo y añade el resultado y su liquidación. Si no existía cuota, califica WIN/HALF_WIN/PUSH/HALF_LOSS/LOSS, pero deja el beneficio monetario en null. El ejemplo 2:1 también es inventado.

Para un registro SQLite inmutable a nivel de aplicación, añadir `--ledger outputs/audit.sqlite` a `run_engine.py` o `postmatch.py`. Insertar el mismo hash dos veces produce un error deliberado. Los hashes y triggers no sustituyen un sellado de tiempo externo ni permisos seguros del servidor.

`evaluate.py` procesa un JSONL con una pareja `{snapshot, result}` por línea, ordena las decisiones y calcula retornos hipotéticos a las cuotas capturadas:

```bash
python evaluate.py --jsonl tus_partidos.jsonl --out outputs/evaluacion.json
python evaluate.py --jsonl bloque_futuro.jsonl --config candidato.json --out outputs/candidato.json
```

No selecciona umbrales con los mismos partidos de evaluación. Congelar un candidato antes del siguiente bloque; comparar mismos eventos y cobertura de cuotas. Agrupa AH y O/U por partido en un bootstrap descriptivo, sin resolver toda la dependencia entre jornadas. El motor no autoaprende una regla nueva después de cada derrota.

## Integración bajo demanda

```bash
python -m pip install -r requirements-web.txt
uvicorn api:app --host 127.0.0.1 --port 8765
```

`motor_button.js` monta un botón y solicita `/analyze` solo al pulsarlo. Hay que conectar `exportSnapshot(card)` con el DOM o estado real de la plataforma y `renderAnalysis(result)` con su modal. Ajustar los orígenes CORS al frontend de desarrollo. `api.py` es un adaptador local de ejemplo, sin autenticación ni despliegue público. No se han modificado `precacheo.html`, `explorer.html` ni el archivo JavaScript de producción porque no se aportaron.

## Archivos

`contracts.py`: entradas y esquemas; `market.py`: signos y liquidación; `engine.py`: métricas, compuertas, políticas y picks; `run_engine.py`: CLI y paquete previo; `postmatch.py`: resultado sin reescritura; `evaluate.py`: evaluación de registros; `review_gemini.py`: auditoría estructurada; `audit_log.py`: trazabilidad; `demo.py`: cuatro ejemplos; `tests/`: verificaciones; `Informe_fuente.md` y `bibliografia.json`: informe y fuentes.

El informe PDF explica las ecuaciones, la correspondencia V1–V24, las decisiones añadidas a la especificación y la evidencia científica. Las pruebas verifican software y liquidación; el rendimiento deportivo del motor requiere registrar sus salidas con datos reales.
