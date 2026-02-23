@echo off
cd /d "%~dp0.."
echo ==========================================
echo Iniciando app en modo local (SQL)...
echo ==========================================

echo Por favor, asegurate de cerrar otras ventanas de terminal que esten ejecutando la app para evitar errores de puerto.

echo Abriendo navegador en http://localhost:5000 ...
timeout /t 3 >nul
start http://localhost:5000

echo Ejecutando servidor Flask...
py src/app.py

pause
