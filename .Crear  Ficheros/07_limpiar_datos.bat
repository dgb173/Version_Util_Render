@echo off
cd /d "%~dp0.."
echo ==========================================
echo Limpiando datos incompletos del Explorador (SQL)
echo ==========================================
echo.
py scripts/limpiar_datos.py
echo.
pause
