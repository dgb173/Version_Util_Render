@echo off
setlocal
cd /d "%~dp0.."

echo ========================================================
echo GENERAR DATA (PRECACHEO_ONLY_RENDER)
echo ========================================================
echo.

if not exist "precacheo_only_render\scripts\run_scraper.py" (
    echo ERROR: No existe precacheo_only_render\scripts\run_scraper.py
    pause
    exit /b 1
)

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
echo [1/2] Ejecutando run_scraper.py (precacheo_only_render)...
"%PYTHON_CMD%" "precacheo_only_render\scripts\run_scraper.py"
if %errorlevel% NEQ 0 (
    echo.
    echo ERROR: Fallo run_scraper.py
    pause
    exit /b %errorlevel%
)

echo.
echo [2/2] Exportando snapshot a JSON...
"%PYTHON_CMD%" "precacheo_only_render\scripts\export_snapshot_to_json.py"
if %errorlevel% NEQ 0 (
    echo.
    echo ERROR: No se pudo exportar snapshot.
    pause
    exit /b %errorlevel%
)

echo.
echo Proceso finalizado correctamente (precacheo_only_render).
pause
exit /b 0
