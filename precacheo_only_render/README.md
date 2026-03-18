# Precacheo Only (Render Free)

Este proyecto está pensado para desplegar **solo Pre-Cacheo** desde el mismo repositorio,
con configuración ligera para Render Free.

## Carpeta de despliegue

`precacheo_only_render`

## Qué incluye

- Mismo backend/base funcional de Pre-Cacheo.
- Modo `APP_PRECACHEO_ONLY=1` (solo ruta y APIs de pre-cacheo).
- Sin bloque visual de `Offline Pre-Cacheo`.
- Configuración lite para bajar consumo de memoria.

## Deploy en Render (mismo repo)

1. Sube cambios a GitHub.
2. En Render crea un nuevo servicio web apuntando al mismo repo.
3. Usa como **Root Directory**: `precacheo_only_render`.
4. Si usas Blueprint, toma la configuración de `precacheo_only_render/render.yaml`.

## Start/Build (si haces deploy manual)

- Build: `pip install --no-cache-dir -r requirements.txt`
- Start: `gunicorn -w 1 -b 0.0.0.0:$PORT --timeout 300 --graceful-timeout 30 --keep-alive 5 wsgi:app`
