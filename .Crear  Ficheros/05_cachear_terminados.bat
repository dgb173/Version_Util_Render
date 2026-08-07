@echo off
setlocal
chcp 65001 >NUL
set "PYTHONUTF8=1"
cd /d "%~dp0.."

echo ========================================================
echo   CACHEAR / SINCRONIZAR PARTIDOS TERMINADOS
echo ========================================================
echo.
echo Selecciona la opcion deseada:
echo   [1] Descargar y sincronizar desde GitHub (RECOMENDADO - 0%% uso de CPU/RAM de tu PC)
echo   [2] Scrapear localmente en este ordenador (10 workers Playwright)
echo.

set /p MOPC=Opcion [1]: 
if "%MOPC%"=="" set "MOPC=1"

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
if "%MOPC%"=="1" (
    echo.
    echo Ejecutando sincronizacion rapida desde GitHub...
    "%PYTHON_CMD%" "scripts\sync_cloud_to_local.py"
    goto :FIN
)

echo.
echo Ejecutando cacheo local en tu PC...
"%PYTHON_CMD%" -u "scripts\cache_finished_matches.py" all all 10 5

:FIN
set "RUN_EXIT=%ERRORLEVEL%"
if not "%RUN_EXIT%"=="0" (
    echo.
    echo ERROR en el proceso.
    pause
    exit /b %RUN_EXIT%
)

echo.
echo Proceso finalizado correctamente.
pause
exit /b 0
