@echo off
setlocal
chcp 65001 >nul
title Cachear Terminados desde GitHub Bot
cd /d "%~dp0"

echo ============================================================
echo   IMPORTAR PARTIDOS TERMINADOS DEL BOT DE GITHUB
echo ============================================================
echo.
echo Flujo:
echo   1. Actualiza repositorio (git pull)
echo   2. Sincroniza los partidos terminados del bot a tu SQLite local
echo.

set "PYTHON_CMD="
if exist ".venv\Scripts\python.exe" set "PYTHON_CMD=.venv\Scripts\python.exe"
if not defined PYTHON_CMD (
    py --version >nul 2>&1 && set "PYTHON_CMD=py"
)
if not defined PYTHON_CMD (
    python --version >nul 2>&1 && set "PYTHON_CMD=python"
)
if not defined PYTHON_CMD (
    echo [ERROR] No se encontro Python en el sistema.
    pause
    exit /b 1
)

echo [1/3] Descargando datos del bot desde GitHub (git pull)...
git pull origin main
if errorlevel 1 (
    echo [AVISO] git pull reporto algun problema, continuando de todos modos...
    echo         (Si es un conflicto de merge, resuelvelo manualmente con GitHub Desktop)
    echo.
)

echo.
echo [2/3] Sincronizando partidos terminados del bot a tu SQLite local...
echo.
%PYTHON_CMD% "scripts\sync_finished_artifacts_local.py"
if errorlevel 1 (
    echo [AVISO] Probando importacion alternativa...
    %PYTHON_CMD% "scripts\import_terminados_local.py"
)
if errorlevel 1 (
    echo [ERROR] Fallo al importar los terminados. Revisa los mensajes superiores.
    pause
    exit /b 1
)

echo.
echo [3/3] Exportando precacheo actualizado a JSON portables...
%PYTHON_CMD% "scripts\export_precacheo_json.py" --include-pending
if errorlevel 1 (
    echo [AVISO] La exportacion tuvo algun problema menor.
)

echo.
echo ============================================================
echo [OK] Partidos terminados del bot importados correctamente.
echo      El Explorador de la app tiene los datos actualizados.
echo ============================================================
echo.
pause
