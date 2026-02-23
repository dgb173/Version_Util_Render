@echo off
setlocal
cd /d "%~dp0.."

echo ========================================================
echo CACHEAR PARTIDOS TERMINADOS
echo (Misma metodologia que Analisis Previo desde JSON)
echo ========================================================
echo.

if "%~1"=="" (
    set /p WORKERS=Workers para cachear terminados [10]: 
    if "%WORKERS%"=="" set "WORKERS=10"
) else (
    set "WORKERS=%~1"
)

if "%~2"=="" (
    set /p FLUSH_EVERY=Flush incremental cada N partidos [5]: 
    if "%FLUSH_EVERY%"=="" set "FLUSH_EVERY=5"
) else (
    set "FLUSH_EVERY=%~2"
)

for /f "delims=0123456789" %%A in ("%WORKERS%") do set "WORKERS=10"
if %WORKERS% LSS 1 set "WORKERS=10"

for /f "delims=0123456789" %%A in ("%FLUSH_EVERY%") do set "FLUSH_EVERY=5"
if %FLUSH_EVERY% LSS 1 set "FLUSH_EVERY=5"

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
echo Configuracion:
echo   - Workers: %WORKERS%
echo   - Flush incremental: cada %FLUSH_EVERY% partidos
echo.
echo Ejecutando cacheo de terminados...

"%PYTHON_CMD%" -u "recopilacion_data\wrapper_cachear_terminados.py" all all %WORKERS% %FLUSH_EVERY%
set "RUN_EXIT=%ERRORLEVEL%"

if not "%RUN_EXIT%"=="0" (
    echo.
    echo ERROR: El proceso de cachear terminados termino con error.
    pause
    exit /b %RUN_EXIT%
)

echo.
echo Cachear terminados finalizado correctamente.
pause
exit /b 0
