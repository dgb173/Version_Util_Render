@echo off
setlocal
chcp 65001 >NUL
set "PYTHONUTF8=1"
cd /d "%~dp0.."

echo ========================================================
echo   SINCRONIZAR DATOS DE GITHUB A BASE DE DATOS LOCAL
echo   (Descarga el precacheo de la nube sin usar CPU/RAM)
echo ========================================================
echo.

if exist ".venv\Scripts\python.exe" (
    set "PYTHON_CMD=.venv\Scripts\python.exe"
    goto :FOUND_PYTHON
)

py --version >NUL 2>&1
if %errorlevel% EQU 0 (
    set "PYTHON_CMD=py"
    goto :FOUND_PYTHON
)

python --version >NUL 2>&1
if %errorlevel% EQU 0 (
    set "PYTHON_CMD=python"
    goto :FOUND_PYTHON
)

echo ERROR: No se encontro Python instalado.
pause
exit /b 1

:FOUND_PYTHON
echo Usando interprete: %PYTHON_CMD%
echo.
echo Sincronizando datos descargados de GitHub hacia tu SQLite local...
echo.

"%PYTHON_CMD%" "scripts\sync_cloud_to_local.py"
set "RUN_EXIT=%ERRORLEVEL%"

if not "%RUN_EXIT%"=="0" (
    echo.
    echo ERROR: La sincronizacion termino con error.
    pause
    exit /b %RUN_EXIT%
)

echo.
echo ========================================================
echo ¡Sincronizacion completada con exito!
echo Los datos de GitHub han sido importados en data\app_data.db
echo ========================================================
pause
exit /b 0
