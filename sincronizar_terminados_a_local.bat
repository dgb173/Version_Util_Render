@echo off
setlocal
chcp 65001 >nul
cd /d "%~dp0"

echo ============================================================
echo   SINCRONIZAR PARTIDOS DESDE GITHUB / RENDER A TU PC
echo ============================================================
echo.

set "PYTHON_CMD="
if exist ".venv\Scripts\python.exe" (
    set "PYTHON_CMD=.venv\Scripts\python.exe"
    goto :RUN_IMPORT
)

py --version >nul 2>&1
if %errorlevel% EQU 0 (
    set "PYTHON_CMD=py"
    goto :RUN_IMPORT
)

python --version >nul 2>&1
if %errorlevel% EQU 0 (
    set "PYTHON_CMD=python"
    goto :RUN_IMPORT
)

echo [ERROR] No se encontro Python en el sistema.
pause
exit /b 1

:RUN_IMPORT
echo [1/3] Actualizando repositorio desde GitHub (git pull)...
git pull origin main
if errorlevel 1 (
    echo [AVISO] git pull reporto algun aviso, continuando...
)

echo.
echo [2/3] Sincronizando partidos terminados a tu SQLite local...
%PYTHON_CMD% "scripts\sync_finished_artifacts_local.py"
if errorlevel 1 (
    echo [AVISO] Probando metodo alternativo de JSONs...
    %PYTHON_CMD% "scripts\import_terminados_local.py"
)

echo.
echo [3/3] Exportando precacheo actualizado a JSON portables...
%PYTHON_CMD% "scripts\export_precacheo_json.py" --include-pending

echo.
echo ============================================================
echo [OK] Partidos terminados sincronizados correctamente a tu PC.
echo ============================================================
echo.
pause
exit /b 0
