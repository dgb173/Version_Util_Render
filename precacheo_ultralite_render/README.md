## Precacheo Ultralite Render

Version separada y minima para Render que expone solo la vista de `precacheo`.

### Objetivo

- Una sola pagina: `/precacheo`
- Sin explorador
- Sin scraping
- Sin IA
- Sin `pandas`, `numpy` ni modulos pesados del proyecto principal

### Fuentes de datos

Orden de prioridad:

1. SQLite indicado por `APP_SQLITE_PATH`
2. Replica `libsql` si defines `LIBSQL_URL`
3. Fallback a `../data/data_precacheo.json`
4. Fallback a `../data/data_precacheo.json.bak`

### Deploy en Render

Usa esta carpeta como `Root Directory`:

`precacheo_ultralite_render`

Build:

`pip install --no-cache-dir -r requirements.txt`

Start:

`gunicorn -w 1 -b 0.0.0.0:$PORT --timeout 120 wsgi:app`

### Variables utiles

- `APP_SQLITE_PATH=/tmp/app_data.db`
- `LIBSQL_URL=...`
- `LIBSQL_AUTH_TOKEN=...`
- `PRECACHEO_UI_ITEMS_PER_PAGE=100`
- `PRECACHEO_UI_MAX_PER_PAGE=250`
