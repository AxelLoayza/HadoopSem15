# Plan de modelado para consumo electrico de Puno

## 1. Objetivo
Definir una ruta de modelado clara y defendible a partir de la salida de `modify_puno.py`, priorizando pronostico de consumo futuro por zona y periodo.

## 2. Criterio principal de analisis
La prioridad no es empezar por el algoritmo, sino por la pregunta de negocio:
- Si se quiere explicar o predecir consumo futuro, la ruta principal es regresion lineal.
- Si se quiere segmentar zonas, KMeans sirve como comparacion exploratoria.
- Si se detecta no linealidad fuerte o poca capacidad explicativa, se prueba Random Forest Regressor como benchmark opcional.

## 3. Estructura de scripts propuesta

### `scripts/build_model_dataset_puno.py`
**Funcion:** preparar la tabla analitica que alimentara los modelos.

**Entrada:**
- Parquet transformado por `modify_puno.py`.

**Salida:**
- Tabla agregada por `departamento`, `provincia`, `distrito`, `ubigeo` y periodo.

**Transformaciones esperadas:**
- Agrupar por zona y periodo.
- Calcular consumo total, importe total, promedio de consumo y cantidad de registros.
- Crear variables temporales si corresponde.
- Generar un dataset limpio para entrenamiento y prueba.

### `scripts/model_regression_puno.py`
**Funcion:** entrenar regresion lineal para estimar consumo futuro.

**Objetivo:**
- Predecir `consumo_kwh` como variable continua.

**Variables recomendadas:**
- Temporales: `anio_corte`, `mes_corte`, `anio_facturacion`, `mes_facturacion`.
- Geograficas: `departamento`, `provincia`, `distrito`, `ubigeo`.
- De apoyo: `importe_soles`, `nombre_empresa`, `nombre_unidad_negocio` si tienen estabilidad suficiente.

**Evaluacion:**
- `RMSE`
- `MAE`
- `R2`
- Grafico de real vs predicho
- Grafico de residuales

### `scripts/model_kmeans_puno.py`
**Funcion:** segmentar zonas por comportamiento de consumo.

**Objetivo:**
- Agrupar zonas similares por consumo e importe.

**Variables recomendadas:**
- Consumo total
- Importe total
- Promedio por periodo
- Numero de suministros

**Evaluacion:**
- Silhouette score
- Elbow chart
- Perfil de cluster
- Dispersión por componentes o por dos variables principales

### `scripts/model_random_forest_puno.py`
**Funcion:** benchmark opcional con Random Forest Regressor.

**Uso:**
- Solo si la regresion lineal no explica bien la variacion o si hay patrones no lineales.

**Evaluacion:**
- `RMSE`
- `MAE`
- `R2`
- Importancia de variables
- Comparacion contra la regresion lineal

### `scripts/assess_model_puno.py`
**Funcion:** consolidar resultados, comparar modelos y generar evidencia para el informe.

**Contenido esperado:**
- Tabla comparativa de metricas.
- Conclusión sobre modelo elegido.
- Gráficos clave por modelo.
- Interpretacion de negocio y sostenibilidad.

## 4. Orden de ejecucion
1. Ejecutar `modify_puno.py`.
2. Verificar la salida con `preview_modify_puno.py`.
3. Construir la tabla agregada con `build_model_dataset_puno.py`.
4. Entrenar regresion lineal con `model_regression_puno.py`.
5. Probar KMeans como comparacion con `model_kmeans_puno.py`.
6. Si hace falta, probar Random Forest con `model_random_forest_puno.py`.
7. Consolidar todo en `assess_model_puno.py`.

## 5. Graficos requeridos en Assess
Los graficos deben aparecer principalmente en la evaluacion final:
- Real vs predicho.
- Residuales.
- Elbow chart.
- Silhouette o dispersion de clusters.
- Importancia de variables para Random Forest.
- Comparativa de metricas entre modelos.

## 6. Criterio de decision final
Se elegira el modelo que mejor combine:
- precision,
- interpretabilidad,
- y utilidad para la gestion energetica.

Si la regresion lineal logra un error razonable y es interpretable, se conserva como modelo principal.
Si KMeans ofrece una segmentacion util para explicar zonas, se presenta como resultado complementario.
Si Random Forest mejora claramente la prediccion, se documenta como benchmark superior, pero solo si la mejora es real y justificada.

## 7. Relacion con el informe
Este flujo permite mostrar:
- como se segmentan o pronostican consumos,
- como cambian por zona y periodo,
- y como estos resultados apoyan decisiones de eficiencia energetica y planificacion.

## 8. Recomendacion final
No mezclar todo en un solo script.
Separar la preparacion del dataset, el entrenamiento de cada modelo y la evaluacion final ayuda a mantener trazabilidad y facilita la evidencia para el PDF.
