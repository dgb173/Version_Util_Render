@echo off
chcp 65001 >nul
cd /d "%~dp0"
python scripts\export_cloud_cache.py
if errorlevel 1 echo ERROR: No se pudo completar la exportacion.
pause
