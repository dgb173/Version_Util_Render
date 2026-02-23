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
echo Sincronizando archivos de pre-cacheo hacia GitHub/Render...

git rev-parse --is-inside-work-tree >NUL 2>&1
if %errorlevel% NEQ 0 (
    echo ADVERTENCIA: No se detecto un repositorio git. Se omite push.
    goto :START_LOCAL_APP
)

git remote get-url origin >NUL 2>&1
if %errorlevel% NEQ 0 (
    echo ADVERTENCIA: No existe remoto origin. Se omite push.
    goto :START_LOCAL_APP
)

set "STAGED_ANY=0"
set "FILES_FOR_COMMIT="
if exist "data\data_precacheo.json" (
    set "STAGED_ANY=1"
    set "FILES_FOR_COMMIT=%FILES_FOR_COMMIT% data\data_precacheo.json"
)
if exist "data\data_pending_results.json" (
    set "STAGED_ANY=1"
    set "FILES_FOR_COMMIT=%FILES_FOR_COMMIT% data\data_pending_results.json"
)

if "%STAGED_ANY%"=="0" (
    echo ADVERTENCIA: No se encontraron archivos de pre-cacheo para sincronizar.
    goto :START_LOCAL_APP
)

git add %FILES_FOR_COMMIT% >NUL 2>&1
git diff --cached --quiet --exit-code -- %FILES_FOR_COMMIT%
if %errorlevel% EQU 0 (
    echo No hay cambios de pre-cacheo para subir.
    goto :START_LOCAL_APP
)

for /f %%T in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd_HH-mm-ss"') do set "SYNC_TS=%%T"
if "%SYNC_TS%"=="" set "SYNC_TS=manual"

git commit -m "chore: sync precacheo %SYNC_TS%" -- %FILES_FOR_COMMIT% >NUL 2>&1
if %errorlevel% NEQ 0 (
    echo ADVERTENCIA: No se pudo crear el commit de pre-cacheo. Se omite push.
    goto :START_LOCAL_APP
)

echo Push a origin/main para actualizar Render...
git push origin main
if %errorlevel% NEQ 0 (
    echo ADVERTENCIA: Fallo el push. Render no se actualizo.
) else (
    echo Push completado. Render iniciara deploy automatico.
)

:START_LOCAL_APP
echo Iniciando app local automaticamente...
call "%~dp008_run_local.bat"
exit /b %ERRORLEVEL%
