@echo off
cd /d "%~dp0.."
echo ==========================================
echo Iniciando app LOCAL (vista PRE-CACHEO)...
echo ==========================================

if not exist "src\app.py" (
    echo ERROR: No existe src\app.py
    pause
    exit /b 1
)

echo Abriendo navegador en http://localhost:5000/precacheo ...
timeout /t 2 >nul
start http://localhost:5000/precacheo

echo Ejecutando servidor Flask (core)...
py src\app.py

pause
