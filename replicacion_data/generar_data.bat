@echo off
TITLE Generador de Data JSON

echo --------------------------------------------------------
echo       GENERADOR DE DATA.JSON
echo --------------------------------------------------------
echo.

REM Usar el directorio del propio script
cd /d "%~dp0"

REM Configuración de Python
REM Prioridad 1: Entorno virtual en la raíz del proyecto
if exist "..\.venv\Scripts\python.exe" (
    set "PYTHON_CMD=..\.venv\Scripts\python.exe"
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

echo Ejecutando el scraper para crear data.json...
echo.

"%PYTHON_CMD%" "run_scraper.py"

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
echo --------------------------------------------------------
echo Proceso finalizado con exito.
echo El archivo data.json se ha generado en:
echo %~dp0data.json
echo --------------------------------------------------------
pause
