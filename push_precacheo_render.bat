@echo off
setlocal

cd /d "%~dp0"

git rev-parse --is-inside-work-tree >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Esta carpeta no es un repositorio git.
    pause
    exit /b 1
)

if exist ".venv\Scripts\python.exe" (
    set "PYTHON_CMD=.venv\Scripts\python.exe"
) else (
    py --version >nul 2>&1
    if not errorlevel 1 (
        set "PYTHON_CMD=py"
    ) else (
        python --version >nul 2>&1
        if not errorlevel 1 (
            set "PYTHON_CMD=python"
        ) else (
            echo [ERROR] No se encontro Python para exportar el precacheo desde SQL.
            pause
            exit /b 1
        )
    )
)

set "MSG=%*"
if "%MSG%"=="" set "MSG=chore: refresh precacheo snapshot for render"

echo.
echo [1/5] Exportando precacheo actual desde SQL...
"%PYTHON_CMD%" "scripts\export_precacheo_json.py" --include-pending
if errorlevel 1 (
    echo [ERROR] No se pudo exportar data_precacheo.json desde SQL.
    pause
    exit /b 1
)

echo [2/5] Preparando solo archivos de precacheo...
git add -- "data.json" "data/data.json" "data/data_precacheo.json"
if errorlevel 1 (
    echo [ERROR] No se pudieron preparar los archivos de precacheo.
    pause
    exit /b 1
)

git diff --cached --quiet -- "data.json" "data/data.json" "data/data_precacheo.json"
set "DIFF_EXIT=%ERRORLEVEL%"
if "%DIFF_EXIT%"=="0" (
    echo [INFO] No hay cambios nuevos en los archivos de precacheo.
    echo [INFO] No se crea commit ni se hace push.
    pause
    exit /b 0
)
if not "%DIFF_EXIT%"=="1" (
    echo [ERROR] No se pudo comprobar el diff de precacheo.
    pause
    exit /b 1
)

echo [3/5] Creando commit...
git commit --only -m "%MSG%" -- "data.json" "data/data.json" "data/data_precacheo.json"
if errorlevel 1 (
    echo [ERROR] El commit ha fallado.
    pause
    exit /b 1
)

echo [4/5] Enviando a origin/main...
git push origin HEAD:main
if errorlevel 1 (
    echo [ERROR] El push ha fallado.
    echo [ERROR] Puede que origin/main tenga cambios nuevos y necesites actualizar primero.
    pause
    exit /b 1
)

echo [5/5] Hecho. Render deberia desplegar este commit.
git log --oneline -1
echo.
pause
exit /b 0
