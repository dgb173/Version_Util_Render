@echo off
TITLE Actualizador de Snapshot SQL

echo --------------------------------------------------------
echo       ACTUALIZADOR DE SNAPSHOT SQL
echo --------------------------------------------------------
echo.

REM Ir a la raiz del proyecto
cd /d "%~dp0.."

REM Configuración de Python
REM Prioridad 1: Entorno virtual en la raíz del proyecto
if exist ".venv\Scripts\python.exe" (
    set "PYTHON_CMD=.venv\Scripts\python.exe"
    goto :FOUND_PYTHON
)

REM Prioridad 2: Comando 'py' (Launcher de Python para Windows)
py --version >NUL 2>&1
if %errorlevel% EQU 0 (
    set "PYTHON_CMD=py"
    goto :FOUND_PYTHON
)

REM Prioridad 3: Comando 'python' (puede fallar si es el alias de la Store)
python --version >NUL 2>&1
if %errorlevel% EQU 0 (
    set "PYTHON_CMD=python"
    goto :FOUND_PYTHON
)

echo.
echo ERROR: No se ha encontrado Python instalado en el sistema.
echo Por favor, instala Python para continuar.
echo.
pause
exit /b 1

:FOUND_PYTHON
echo Usando interprete: %PYTHON_CMD%
set "PRECACHEO_PENDING_MAX_AGE_DAYS=1"
echo Politica activa: limpiar resultados pendientes con mas de 1 dia en cada generar data.

echo Ejecutando scraper principal para actualizar snapshot SQL...
echo.

"%PYTHON_CMD%" "scripts\run_scraper.py"

IF %errorlevel% NEQ 0 (
    echo.
    echo ***********************************************************
    echo *  ERROR: El script de scraping ha fallado.                *
    echo *  Revisa los mensajes de error en esta ventana.          *
    echo ***********************************************************
    echo.
    pause
    exit /b %errorlevel%
)

echo.
echo Exportando snapshot SQL a data.json...
"%PYTHON_CMD%" "scripts\export_snapshot_to_json.py"

IF %errorlevel% NEQ 0 (
    echo.
    echo ***********************************************************
    echo *  ERROR: No se pudo exportar data.json desde SQL.        *
    echo ***********************************************************
    echo.
    pause
    exit /b %errorlevel%
)

echo.
echo --------------------------------------------------------
echo Proceso finalizado con exito.
echo Snapshot principal actualizado en SQL (kv_store).
echo data.json actualizado.
echo Base de datos: data\app_data.db
echo --------------------------------------------------------

echo.
echo Lanzando automaticamente Step 2: Analisis previo desde JSON...
set "SKIP_AUTO_RUN_LOCAL=1"
if "%~1"=="" (
    call "%~dp002_analisis_previo_desde_json.bat"
) else (
    call "%~dp002_analisis_previo_desde_json.bat" "%~1"
)

set "STEP2_EXIT=%ERRORLEVEL%"
set "SKIP_AUTO_RUN_LOCAL="
if not "%STEP2_EXIT%"=="0" (
    echo.
    echo ***********************************************************
    echo *  ERROR: El Step 2 (analisis previo) ha fallado.         *
    echo ***********************************************************
    echo.
    pause
    exit /b %STEP2_EXIT%
)

echo.
echo Flujo completo finalizado con exito (Step 1 + Step 2).

echo.
echo Exportando JSON de pre-cacheo (incluye pending_results)...
"%PYTHON_CMD%" "scripts\export_precacheo_json.py" --include-pending
IF %errorlevel% NEQ 0 (
    echo.
    echo ***********************************************************
    echo *  ERROR: No se pudieron exportar los JSON de pre-cacheo. *
    echo ***********************************************************
    echo.
    pause
    exit /b %errorlevel%
)

echo.
echo [3/3] Sincronizando repositorio con los datos actuales...
if /I "%AUTO_GIT_SYNC%"=="0" (
    echo AUTO_GIT_SYNC=0 detectado. Se omite la sincronizacion git.
    goto :END
)

where git >NUL 2>&1
if %errorlevel% NEQ 0 (
    echo Git no esta disponible en PATH. Se omite la sincronizacion.
    goto :END
)

git rev-parse --is-inside-work-tree >NUL 2>&1
if %errorlevel% NEQ 0 (
    echo Esta carpeta no es un repositorio git. Se omite la sincronizacion.
    goto :END
)

git add data.json data\data.json
set "MAX_JSON_BYTES=99000000"
set "PRECACHEO_SIZE=0"
if exist "data\data_precacheo.json" (
    for %%I in ("data\data_precacheo.json") do set "PRECACHEO_SIZE=%%~zI"
)

if %PRECACHEO_SIZE% GTR 0 (
    if %PRECACHEO_SIZE% LEQ %MAX_JSON_BYTES% (
        git add data\data_precacheo.json
        echo data_precacheo.json incluido en el commit automatico.
    ) else (
        echo data_precacheo.json omitido por tamano elevado (%PRECACHEO_SIZE% bytes).
    )
)
set "PENDING_SIZE=0"
if exist "data\data_pending_results.json" (
    for %%I in ("data\data_pending_results.json") do set "PENDING_SIZE=%%~zI"
)

if %PENDING_SIZE% GTR 0 (
    if %PENDING_SIZE% LEQ %MAX_JSON_BYTES% (
        git add data\data_pending_results.json
        echo data_pending_results.json incluido en el commit automatico.
    ) else (
        echo data_pending_results.json omitido por tamano elevado (%PENDING_SIZE% bytes).
    )
)
git diff --cached --quiet
if %errorlevel% EQU 0 (
    echo No hay cambios de datos para commitear.
    goto :END
)

set "SYNC_STAMP=%DATE%_%TIME%"
set "SYNC_STAMP=%SYNC_STAMP:/=-%"
set "SYNC_STAMP=%SYNC_STAMP::=-%"
set "SYNC_STAMP=%SYNC_STAMP:,=-%"
set "SYNC_STAMP=%SYNC_STAMP: =0%"

git commit -m "chore: sync data %SYNC_STAMP%"
if %errorlevel% NEQ 0 (
    echo No se pudo crear el commit automatico.
    goto :END
)

git push origin HEAD:main
if %errorlevel% NEQ 0 (
    echo El push automatico ha fallado. Revisa credenciales/conexion.
    goto :END
)

echo Sincronizacion git completada correctamente.

:END
pause
