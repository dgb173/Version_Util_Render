@echo off
setlocal
chcp 65001 >NUL
set "PYTHONUTF8=1"
cd /d "%~dp0"

echo ========================================================
echo GENERAR BITACORA DE AUTO-MEJORA (RESULTADOS)
echo ========================================================
echo.

if exist "..\.venv\Scripts\python.exe" (
    set "PYTHON_CMD=..\.venv\Scripts\python.exe"
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

echo ERROR: No se encontro Python en el sistema.
pause
exit /b 1

:FOUND_PYTHON
echo Ejecutando generacion de bitacora...
"%PYTHON_CMD%" generar_bitacora.py
set "RUN_EXIT=%ERRORLEVEL%"

if not "%RUN_EXIT%"=="0" (
    echo.
    echo ERROR: La generacion de bitacora ha fallado.
    pause
    exit /b %RUN_EXIT%
)

echo.
echo Proceso finalizado correctamente.
pause
exit /b 0
