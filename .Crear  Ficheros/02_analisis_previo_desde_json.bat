@echo off
setlocal
cd /d "%~dp0.."

echo ========================================================
echo ANALISIS PREVIO DESDE JSON
echo ========================================================
echo.

if "%~1"=="" (
    set "WORKERS=10"
) else (
    set "WORKERS=%~1"
)

for /f "delims=0123456789" %%A in ("%WORKERS%") do set "WORKERS=10"
if %WORKERS% LSS 1 set "WORKERS=10"

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
echo Workers de analisis previo: %WORKERS%
set "FLUSH_EVERY=5"

if exist "data\data_precacheo.json" (
    echo.
    echo Sincronizando data_precacheo.json al almacenamiento SQL...
    "%PYTHON_CMD%" "scripts\import_json_to_sql.py" "data\data_precacheo.json"
    if %errorlevel% NEQ 0 (
        echo ADVERTENCIA: No se pudo sincronizar data_precacheo.json. Continuando...
    )
)

set "JOB_FILE=%CD%\temp_matches_job.json"
echo.
echo Construyendo JSON de partidos desde snapshot SQL...
"%PYTHON_CMD%" "scripts\build_job_from_snapshot.py" --db "data\app_data.db" --cache-key "app_main_page_cache_v1" --out "%JOB_FILE%"
set "EXPORT_EXIT=%ERRORLEVEL%"

if not "%EXPORT_EXIT%"=="0" (
    echo.
    echo ERROR: No se pudo crear el JSON de partidos.
    pause
    exit /b %EXPORT_EXIT%
)

echo.
echo Ejecutando analisis previo desde el JSON generado...
"%PYTHON_CMD%" "background_runner.py" --job_file "%JOB_FILE%" --concurrency %WORKERS% --flush_every %FLUSH_EVERY%
set "RUN_EXIT=%ERRORLEVEL%"

if exist "%JOB_FILE%" del "%JOB_FILE%" >NUL 2>&1

if not "%RUN_EXIT%"=="0" (
    echo.
    echo ERROR: El analisis previo termino con error.
    pause
    exit /b %RUN_EXIT%
)

echo.
echo Analisis previo finalizado correctamente.
echo.
echo Exportando snapshot SQL de pre-cacheo a JSON...
"%PYTHON_CMD%" "scripts\export_precacheo_json.py"
if %errorlevel% NEQ 0 (
    echo ADVERTENCIA: No se pudo exportar data_precacheo.json desde SQL. Se omite push.
    goto :START_LOCAL_APP
)

echo.
echo Iniciando app local automaticamente...
call "%~dp008_run_local.bat"
exit /b %ERRORLEVEL%
