# Guía de Uso: Scraper en Google Colab

Esta guía te explica cómo ejecutar tu scraper de NowGoal en la nube usando Google Colab para obtener mayor velocidad (10-20x) y liberar tu PC.

## 1. Preparar tu Código
Antes de ir a la nube, necesitas empaquetar tu código actual.

1.  Ve a la carpeta raíz de tu proyecto en tu PC.
2.  Haz doble clic en el archivo **`preparar_kit_colab.bat`**.
3.  Esperar a que termine. Se creará un archivo llamado **`project_code.zip`**.

## 2. Abrir Google Colab
1.  Abre [Google Colab](https://colab.research.google.com/).
2.  Haz clic en **Archivo** -> **Subir notebook**.
3.  Arrastra el archivo `google_colab/Scraper_Optimizado.ipynb` que tienes en tu carpeta.

## 3. Ejecutar el Scraper
Una vez abierto el notebook en el navegador:

1.  **Instalar Dependencias**: Haz clic en el botón de "Play" ▶️ de la primera celda.
2.  **Cargar Código**: 
    *   Ejecuta la celda "2. Cargar/Descomprimir Código".
    *   Haz clic en el botón "Elegir Archivos" que aparecerá.
    *   Selecciona el archivo **`project_code.zip`** que creaste en el paso 1.
3.  **Ejecutar Tarea**:
    *   Elige la opción que quieras (Cachear Terminados o Precacheo).
    *   Ajusta los sliders de configuración (Workers, Filtros).
    *   Dale al Play ▶️.
    *   Verás el progreso en tiempo real.

## 4. Descargar Resultados
Cuando el scraper termine:

1.  Ejecuta la última celda "4. Descargar Resultados".
2.  Automáticamente se descargará un archivo ZIP con todos los datos actualizados.
3.  Descomprime ese archivo en tu carpeta `data/` local para actualizar tu base de datos.
