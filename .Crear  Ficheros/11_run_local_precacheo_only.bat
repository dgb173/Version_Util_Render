@echo off
cd /d "%~dp0.."
echo ==========================================
echo Iniciando app LOCAL solo PRE-CACHEO...
echo ==========================================

if not exist "precacheo_only_render\src\app.py" (
    echo ERROR: No existe precacheo_only_render\src\app.py
    pause
    exit /b 1
)

echo Abriendo navegador en http://localhost:5000/precacheo ...
timeout /t 2 >nul
start http://localhost:5000/precacheo

echo Ejecutando servidor Flask (precacheo_only_render)...
py precacheo_only_render\src\app.py

pause
