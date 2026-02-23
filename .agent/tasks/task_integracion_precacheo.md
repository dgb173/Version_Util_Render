# Tarea: Verificación e Integración de Patrones AH en Precacheo

## Descripción
Asegurar que los nuevos patrones especialistas de AH (especialmente el grupo unificado 0.25/0.5/0.75) se carguen y visualicen correctamente en la interfaz de Pre-Cacheo, permitiendo al usuario identificar las mejores oportunidades de apuesta.

## Pasos

### 1. Limpieza de datos obsoletos
- Eliminar `specialist_ah_H0_25.json` y `specialist_ah_H0_75.json`. [HECHO]
- Asegurar que `specialist_ah_H0_5.json` sea el único referente para esa familia. [HECHO]

### 2. Actualización del Frontend (JavaScript en `precacheo.html`)
- Localizar la función de renderizado de filas (`renderTableRows` o similar). [HECHO]
- Modificar la lógica de la columna "Pick" para que:
  - Detecte `specialist_picks`. [HECHO]
  - Muestre los patrones con ROI > 20% con un diseño destacado (borde rojo/glow). [HECHO]
  - Incluya el nombre del patrón, el ROI y el número de muestras (N). [HECHO]

### 3. Validación de Equivalencia [PENDIENTE]
- Cargar un partido con AH 0.25 o 0.75 en la página.
- Verificar que el sistema asocie correctamente los patrones de la familia H0.5.
