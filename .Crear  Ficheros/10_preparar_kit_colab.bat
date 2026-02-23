@echo off
cd /d "%~dp0.."
echo ===================================================
echo PREPARADOR DE KIT PARA GOOGLE COLAB
echo ===================================================
echo.
echo Este script comprimira tu codigo y datos en 'project_code.zip'
echo para que puedas subirlo a Google Colab.
echo.
echo PASO 1: Copiando archivos a carpeta temporal...
echo (Esto evita errores si tienes archivos abiertos)

if exist temp_colab rmdir /s /q temp_colab
mkdir temp_colab
mkdir temp_colab\src
mkdir temp_colab\recopilacion_data
mkdir temp_colab\data

xcopy src temp_colab\src /s /e /i /y >nul
xcopy recopilacion_data temp_colab\recopilacion_data /s /e /i /y >nul
xcopy data temp_colab\data /s /e /i /y >nul
copy .env temp_colab\ >nul
copy requirements.txt temp_colab\ >nul

echo.
echo PASO 2: Comprimiendo...

del project_code.zip 2>nul
powershell -Command "Compress-Archive -Path temp_colab\* -DestinationPath project_code.zip -Force"

echo.
echo PASO 3: Limpiando temporales...
rmdir /s /q temp_colab

echo.
if exist project_code.zip (
    echo [OK] Archivo 'project_code.zip' creado exitosamente.
    echo.
    echo LISTO! Sube este archivo a Google Colab.
) else (
    echo [ERROR] No se pudo crear el archivo zip.
)
echo.
pause
