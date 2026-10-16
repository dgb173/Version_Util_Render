@echo off
setlocal
chcp 65001 >nul
cd /d "%~dp0"
title Importar Partidos Terminados del Bot de GitHub

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

echo ============================================================
echo   IMPORTAR PARTIDOS TERMINADOS DEL BOT DE GITHUB A LOCAL
echo ============================================================
echo.
echo [1/3] Actualizando repositorio desde GitHub (git pull)...
git pull origin main
if errorlevel 1 (
    echo [AVISO] git pull reporto algun aviso, continuando con la importacion...
)

echo.
echo [2/3] Descargando e importando artefactos de terminados de GitHub...
%PYTHON_CMD% scripts\sync_finished_artifacts_local.py
if errorlevel 1 (
    echo.
    echo [ERROR] No se pudieron importar los terminados de GitHub.
    pause
    exit /b 1
)

echo.
echo [3/3] Actualizando caches y JSONs locales...
%PYTHON_CMD% scripts\export_precacheo_json.py --include-pending >nul 2>&1

echo.
echo ============================================================
echo [OK] Partidos terminados del bot de GitHub importados a SQLite.
echo      El Explorador local ya dispone de los resultados actualizados.
echo ============================================================
echo.
pause
exit /b 0

