@echo off
cd ..
echo ==========================================
echo 1. Buscando Resultados Pendientes (+2h)...
echo ==========================================
py -u recopilacion_data/wrapper_buscar_resultados.py
echo.
echo ==========================================
echo 2. Finalizando Partidos con Resultado...
echo ==========================================
py -u recopilacion_data/wrapper_finalizar_todos.py
echo.
echo Proceso completado.
pause
