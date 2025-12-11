@echo off
cd ..
echo ==========================================
echo 1. Scrapeando detalles de pendientes (Pre-Cacheo)...
echo ==========================================
py -u recopilacion_data/wrapper_scrapear_pendientes.py

echo.
echo Proceso completado.
pause
