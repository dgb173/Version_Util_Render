@echo off
setlocal
chcp 65001 >NUL
set "PYTHONUTF8=1"
cd /d "%~dp0"

echo ========================================================
3. EXPORTAR PARTIDOS TERMINADOS PARA AUTO-MEJORA (CHATGPT/NOTEBOOKLM)
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
echo Ejecutando exportacion de partidos terminados...
"%PYTHON_CMD%" export_terminados.py
set "RUN_EXIT=%ERRORLEVEL%"

if not "%RUN_EXIT%"=="0" (
    echo.
    echo ERROR: La exportacion de terminados ha fallado.
    pause
    exit /b %RUN_EXIT%
)

echo.
echo Proceso finalizado correctamente. Los partidos terminados de hoy se han exportado.
pause
exit /b 0
