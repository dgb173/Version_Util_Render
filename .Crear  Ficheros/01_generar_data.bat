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
if "%~1"=="" (
    call "%~dp002_analisis_previo_desde_json.bat"
) else (
    call "%~dp002_analisis_previo_desde_json.bat" "%~1"
)

set "STEP2_EXIT=%ERRORLEVEL%"
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
pause
