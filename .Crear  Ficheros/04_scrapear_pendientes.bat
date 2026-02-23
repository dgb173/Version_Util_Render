@echo off
cd /d "%~dp0.."
echo ==========================================
echo 1. Scrapeando detalles de pendientes (Pre-Cacheo SQL)...
echo ==========================================
py -u recopilacion_data/wrapper_scrapear_pendientes.py

echo.
echo Proceso completado.
pause
