# Version Util Render (SQL-Only Core)

Core funcional mantenido:
- `/explorador`
- `/precacheo`
- `/grandes_ligas`
- `/scraper`

Base de datos principal:
- SQLite en `data/app_data.db`

Scripts operativos activos:
- `scripts/run_scraper.py` (actualiza snapshot upcoming/finished en SQL)
- `scripts/scrape_grandes_ligas.py`
- `scripts/sync_storage.py` (utilidades import/export/status)
- `scripts/pattern_miner_v2/*` (motor de picks para precacheo)

Scripts legacy/no usados:
- Movidos a `_legacy_archive/scripts_unused/`

## Deploy en Render (Free)

La app está lista para deploy con `render.yaml`.

- Runtime: Python 3.11
- Start command: `gunicorn -w 1 ... wsgi:app` (worker único, más estable en Free)
- Health check: `/precacheo`
- DB SQLite en free: `/tmp/app_data.db` (almacenamiento efímero)

Variables ya declaradas en blueprint:
- `APP_SQLITE_PATH=/tmp/app_data.db`
- `APP_SQLITE_BOOTSTRAP_LOCK=/tmp/app_data_bootstrap.lock`
- `DATA_LEGACY_SYNC=0`
- `SQL_BOOTSTRAP_MODE=none` (evita importar JSON legacy gigantes al iniciar en Free)
- `EMPTY_SNAPSHOT_REFRESH_COOLDOWN_SECONDS=300`
- `LIBSQL_SYNC_INTERVAL_SECONDS=60`
- `LIBSQL_URL` opcional (`sync: false`)
- `LIBSQL_AUTH_TOKEN` opcional (`sync: false`)
- `EXPLORER_CACHE_TTL_SECONDS=60`
- `GROQ_API_KEY` opcional (`sync: false`)

### Pasos

1. Sube este repositorio a GitHub.
2. En Render: `New +` -> `Blueprint`.
3. Selecciona tu repo y deploy.
4. Render leerá `render.yaml` automáticamente.

## Notas importantes en plan Free

- El disco es efímero: al reiniciar/suspender el servicio se reconstruye la DB en `/tmp`.
- El bootstrap legacy pesado se desactiva en Free (`SQL_BOOTSTRAP_MODE=none`) para evitar OOM.
- Si el snapshot principal está vacío, la app intenta recargar partidos automáticamente desde origen.
- El endpoint IA (`/api/ai_prediction`) requiere que configures `GROQ_API_KEY`.

## Turso (persistencia remota recomendada)

La app soporta `libsql` (Turso) con réplica local embebida:
- Si defines `LIBSQL_URL` y `LIBSQL_AUTH_TOKEN`, la DB local en `/tmp` se sincroniza con Turso.
- Si no defines esas variables, sigue en SQLite local normal.

### Bootstrap inicial de datos hacia Turso (una sola vez)

1. Crea tu base en Turso y obtén URL + token.
2. Exporta variables en tu terminal:
   - `LIBSQL_URL=libsql://...`
   - `LIBSQL_AUTH_TOKEN=...`
3. Ejecuta:

```bash
py scripts/bootstrap_turso.py
```

Ese script importa los JSON legacy a SQL y fuerza sync hacia Turso.
