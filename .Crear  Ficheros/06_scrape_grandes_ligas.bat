@echo off
REM ==========================================================
REM  SCRAPER AUTOMÁTICO GRANDES LIGAS
REM  Ejecuta scraping de partidos de las 5 grandes ligas europeas
REM  en segundo plano con 10 workers paralelos
REM ==========================================================

echo ========================================
echo  SCRAPER GRANDES LIGAS - Inicio
echo  %date% %time%
echo ========================================

cd /d "%~dp0.."

REM Verificar que Python esté disponible
py --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python no encontrado. Instale Python o use 'py' en lugar de 'python'.
    pause
    exit /b 1
)

echo.
echo [1/2] Ejecutando scraper de Grandes Ligas...
echo.

py scripts/scrape_grandes_ligas.py

echo.
echo [2/2] Proceso completado.
echo  %date% %time%
echo ========================================
echo.

pause
