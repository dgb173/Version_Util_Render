@echo off
cd ..
echo ==========================================
echo 1. Actualizando lista de partidos (run_scraper)...
echo ==========================================
py -u replicacion_data/run_scraper.py

echo.
echo ==========================================
echo 2. Scrapeando detalles de pendientes (Pre-Cacheo)...
echo ==========================================
py -u recopilacion_data/wrapper_scrapear_pendientes.py

echo.
echo Proceso completado.
pause
