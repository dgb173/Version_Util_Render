@echo off
setlocal
cd /d "%~dp0.."

echo ========================================================
echo GENERAR DATA + ANALISIS PREVIO (JSON)
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

echo ERROR: No se encontro Python.
pause
exit /b 1

:FOUND_PYTHON
echo Usando interprete: %PYTHON_CMD%
echo.
echo [1/2] Ejecutando generar data (snapshot SQL)...
"%PYTHON_CMD%" "scripts\run_scraper.py"
if %errorlevel% NEQ 0 (
    echo.
    echo ERROR: Fallo el paso de generar data.
    pause
    exit /b %errorlevel%
)

echo.
echo Exportando snapshot SQL a data.json...
"%PYTHON_CMD%" "scripts\export_snapshot_to_json.py"
if %errorlevel% NEQ 0 (
    echo.
    echo ERROR: No se pudo generar data.json desde SQL.
    pause
    exit /b %errorlevel%
)

echo.
echo [2/2] Ejecutando analisis previo cargando partidos desde JSON...
if "%~1"=="" (
    call "%~dp002_analisis_previo_desde_json.bat"
) else (
    call "%~dp002_analisis_previo_desde_json.bat" "%~1"
)

set "RUN_EXIT=%ERRORLEVEL%"
if not "%RUN_EXIT%"=="0" (
    echo.
    echo ERROR: El analisis previo termino con error.
    exit /b %RUN_EXIT%
)

echo.
echo Flujo completo finalizado con exito.
exit /b 0
