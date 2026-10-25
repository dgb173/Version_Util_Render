# Fechas y horario España: revisión 25/10/2026

## Causa confirmada

El main de GitHub inspeccionado (6f183618cbf317a6f8761ce54f82779b5828e065)
trataba todo octubre como UTC+2 en el scraper H2H; la lista principal ya usaba
Europe/Madrid. El precacheo mezclaba así dos relojes para el mismo ID.
El fallback HTML sumaba una hora fija y las fechas inválidas podían convertirse
en un partido ficticio del 16/06/2026. El job de terminados seguía a las 03:00 UTC,
que en invierno corresponde a las 04:00 de España, no a las 05:00 previstas.

## Corrección

- UTC para instantes y Europe/Madrid para presentación, con zona explícita.
- El origen de los feeds de ligas se interpreta en Asia/Shanghai antes de filtrar.
- Mismo instante para hora, fecha, start_time y UTC; se conserva el cruce de medianoche.
- Los jobs y los registros reutilizados sincronizan el reloj y los índices SQL,
  sin modificar goles, cuotas, estadísticas o análisis.
- El calendario del navegador calcula «mañana» por fecha civil, no sumando 24 h.
- GitHub programa terminados a las 05:00 Europe/Madrid y precacheo cada tres horas
  locales al minuto 10, con ajuste estacional automático.

La reparación local verificó IDs y equipos frente al snapshot principal:
702 registros sincronizados, 494 diferencias de hora y 8 cambios de fecha.
Se guardaron las filas originales y los JSON anteriores en artifacts/timezone_audit/.
Cuatro IDs se excluyeron por discrepancia de identidad; no se alteraron a ciegas.
Los datos locales y los modelos no forman parte de este cambio de código.

## Verificación

Pruebas de octubre y marzo de 2026, dos instantes distintos para las 02:30 del
cambio de octubre, conversión de China con cambio de día, rechazo de fechas
inválidas, sincronización SQL preservando estadísticas y navegador con zonas
UTC, Madrid y Nueva York. Se comprueba también la reutilización de un partido
sin volver a scrapearlo ni conservar el día equivocado en el índice.

El test antiguo test_build_jobs_caps_snapshot_without_reading_uninitialized_queue
espera 200 jobs, pero el código de main ya exporta 250 con su fixture. Ese fallo
previo no pertenece a la conversión horaria y no se cambia el límite de datos aquí.

Fuentes: [BOE, calendario 2022–2026](https://www.boe.es/buscar/doc.php?id=BOE-A-2022-4026)
y [GitHub Actions, schedule y timezone](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule).
