# Plan de análisis para datos de consumo eléctrico de Puno

## 1. Propósito
Definir una ruta clara para explorar el CSV de consumo eléctrico, validar la calidad de los datos y decidir si conviene una agrupación, clasificación o solo una fase de preparación y ordenamiento antes del análisis final.

## 2. Idea central
Antes de pensar en modelado, primero hay que responder estas preguntas:
- ¿El CSV tiene columnas limpias y consistentes?
- ¿Las categorías como ubigeo, región, distrito o sector están completas?
- ¿Existen registros desordenados, duplicados o nulos?
- ¿El objetivo es clasificar, agrupar o solo describir el comportamiento del consumo?

La decisión del modelo depende de la respuesta a estas preguntas.

## 3. Hipótesis de trabajo
Es posible que el dataset permita dos enfoques distintos:

### Enfoque A: Ordenamiento y preparación
Si el objetivo principal es organizar la información por ubigeo, región o categoría territorial, entonces no hace falta un modelo complejo al inicio. En ese caso conviene:
- Normalizar columnas.
- Ordenar registros.
- Agrupar por región, provincia o distrito.
- Detectar registros incompletos o inconsistentes.

### Enfoque B: Segmentación con KMeans
Si el dataset contiene variables numéricas útiles como consumo, potencia, medidores, periodos o valores de demanda, KMeans puede servir para descubrir grupos de comportamiento.

Este enfoque es útil cuando se quiere responder preguntas como:
- Qué zonas tienen consumos similares.
- Qué patrones de consumo se repiten.
- Qué grupos pueden recibir acciones de ahorro o incentivo.

### Enfoque C: Clasificación con árbol de decisión
Si existe una etiqueta o categoría que se quiera predecir, un árbol de decisión puede ser más apropiado.

Sirve cuando quieres justificar decisiones como:
- Identificar perfiles de consumo alto o bajo.
- Clasificar usuarios o zonas según su comportamiento.
- Apoyar estrategias de focalización o incentivos energéticos.

## 4. Recomendación inicial
Para esta actividad, la ruta más sólida es:

1. Primero leer una muestra con Python para entender la estructura real del CSV.
2. Luego revisar qué columnas son útiles, cuáles son solo cualitativas y cuáles no aportan al análisis.
3. Después preparar el script de Spark con lo relevante para la fase Explore.
4. Solo al final decidir si conviene segmentar con KMeans o clasificar con un árbol de decisión.

## 5. Criterio técnico para elegir el método

### Elegir KMeans si:
- Hay varias variables numéricas.
- Quieres agrupar sin una clase objetivo.
- Buscas segmentación de consumidores o zonas.

### Elegir árbol de decisión si:
- Existe una categoría a predecir.
- Quieres reglas interpretables.
- Necesitas justificar decisiones por condiciones claras.

### Elegir solo preparación y análisis descriptivo si:
- El dataset es pequeño o muy incompleto.
- Las variables no están bien definidas.
- El objetivo del trabajo se centra más en Hadoop, HDFS y Spark que en el modelado.

## 6. Hallazgos que conviene buscar
- Registros duplicados.
- Ubigeos fuera de rango.
- Regiones escritas de forma distinta.
- Valores vacíos en columnas clave.
- Fechas o textos mal formateados.
- Consumos atípicos o extremos.

## 6.1. Referencia UBIGEO
Usar [ubigeo_map.json](ubigeo_map.json) para traducir y agrupar la ubicación geográfica:
- 2 primeros dígitos: región o departamento.
- 4 primeros dígitos: provincia.
- 6 dígitos completos: distrito.

Para segmentación inicial, conviene trabajar por región o provincia. No usar el UBIGEO completo como variable numérica continua sin transformarlo.

## 7. Casos de uso posibles

### Caso 1: Focalización de ahorro energético
Agrupar zonas con alto consumo para priorizar campañas de eficiencia energética.

### Caso 2: Monitoreo territorial
Ordenar y agrupar por ubigeo o región para visualizar dónde se concentra la demanda.

### Caso 3: Detección de patrones
Usar clustering para encontrar grupos de consumo similares y compararlos.

### Caso 4: Reglas de decisión
Usar un árbol de decisión para explicar bajo qué condiciones una zona podría clasificarse como de alto consumo o mayor prioridad de intervención.

## 8. Relación con SEMMA

### Sample
Seleccionar una muestra o subconjunto confiable del CSV y revisarlo con Python antes de cargar todo a Spark.

### Explore
Revisar tipos de datos, nulos, duplicados, estadísticas y distribución por categorías usando Spark una vez que ya sepamos qué campos sí importan.

### Modify
Limpiar datos, transformar tipos, estandarizar texto y preparar variables en un script separado para no mezclar la exploración con la preparación.

Acciones mínimas para Modify:
- Convertir `fecha_corte`, `fecha_vencimiento`, `fecha_emision`, `fecha_inicio_facturacion` y `fecha_fin_facturacion` a fecha.
- Convertir `importe_soles` y `consumo_kwh` a numérico.
- Normalizar textos y eliminar espacios sobrantes.
- Mantener `departamento`, `provincia`, `distrito` y `ubigeo` como variables categóricas o de agrupación.
- Crear variables derivadas útiles para modelado, si corresponden.

### Model
La etapa de Model debe seguir un criterio de prioridad:

1. **Ruta principal: regresión lineal** sobre datos agregados por zona y periodo para estimar consumo futuro.
2. **Ruta exploratoria: KMeans** solo para segmentar zonas y comparar si los grupos encontrados ayudan a interpretar el consumo.
3. **Benchmark opcional: Random Forest Regressor** si luego se detecta no linealidad o se quiere comparar contra la regresión lineal.

Regla técnica:
- Si el objetivo es predecir consumo continuo, el modelo base debe ser regresión.
- Si el objetivo es segmentar zonas, KMeans sirve como apoyo, no como predictor final.
- Si se convierte el consumo en clases alto/medio/bajo, entonces podría evaluarse Random Forest Classifier, pero solo como escenario alternativo.

Variables recomendadas para modelado:
- `departamento`, `provincia`, `distrito`, `ubigeo` como categorías o llaves de agrupación.
- `fecha_corte`, `periodo_facturado`, `anio_corte`, `mes_corte`, `anio_facturacion`, `mes_facturacion` como variables temporales.
- `importe_soles`, `consumo_kwh` como variables numéricas centrales.
- `nombre_empresa` y `nombre_unidad_negocio` solo si aportan patrón estable y no demasiada dispersión.

Diseño del flujo de modelado:
- Agregar la data por zona y periodo.
- Construir variables rezagadas o históricas si se va a pronosticar.
- Probar regresión lineal como línea base.
- Evaluar KMeans sobre las mismas agregaciones para comparar segmentación.
- Si la relación es no lineal o la regresión pierde capacidad explicativa, probar Random Forest Regressor como comparación.

### Assess
La etapa Assess debe incluir comparación de **métricas**, **gráficos** y **utilidad de negocio**.

#### Para regresión lineal
- Métricas: `RMSE`, `MAE`, `R2`.
- Gráficos: real vs predicho, residuales vs predicho, distribución de errores.
- Criterio de aceptación: menor error y mejor explicación del consumo futuro por zona.

#### Para KMeans
- Métricas: `silhouette score`, `within-cluster SSE`, y si aplica `Davies-Bouldin`.
- Gráficos: elbow chart, dispersión de clusters y perfil promedio por cluster.
- Criterio de aceptación: clusters interpretables que separen zonas con comportamientos distintos.

#### Para Random Forest Regressor, si se usa
- Métricas: `RMSE`, `MAE`, `R2`.
- Gráficos: importancia de variables y real vs predicho.
- Criterio de aceptación: solo se conserva si mejora claramente a la regresión lineal sin sacrificar interpretabilidad.

#### Decisión final de Assess
- Si la predicción es la prioridad, se elige el modelo con menor error y mejor `R2`.
- Si la segmentación aporta más valor explicativo que la predicción, se documenta KMeans como resultado complementario.
- Los gráficos no deben quedar aislados: deben servir para justificar por qué un modelo es mejor que otro.

## 9. Decisión provisional
Por ahora no conviene empezar directamente con KMeans, árbol de decisión o Random Forest sin antes revisar el dataset y sin antes definir si el objetivo final es segmentar o predecir.

La secuencia correcta es:
- leer una muestra,
- ordenar y limpiar,
- explorar,
- identificar variables útiles,
- agregar por zona y periodo,
- y recién elegir entre regresión lineal, KMeans o Random Forest.

Conclusión directa: para este tamaño y esta etapa del trabajo, primero conviene entender la data; después se usa Spark para la exploración formal.

## 10. Resultado esperado
Al final se debe tener:
- una descripción clara del dataset,
- hallazgos sobre calidad de datos,
- una decisión justificada sobre el modelo,
- y una explicación de cómo el análisis aporta a la gestión energética.

## 11. Separación de archivos
Conviene mantener dos scripts distintos:
- `scripts/sample/inspect_sample.py` para el muestreo e inspección local.
- `scripts/explore/explore_puno.py` para la exploración descriptiva.
- `scripts/modify/modify_puno.py` para la limpieza y transformación.
- `scripts/modify/preview_modify_puno.py` para validar en terminal la salida transformada.

Así se conserva la trazabilidad de SEMMA y resulta más fácil documentar cada fase por separado.

## 12. Plan de modelado propuesto
Para no dispersar el trabajo, el modelado debe ordenarse así:

### Script 1: preparación para modelado
- Leer la salida de `scripts/modify/modify_puno.py`.
- Agrupar por `departamento`, `provincia`, `distrito`, `ubigeo` y periodo.
- Crear una tabla analítica por zona-periodo.

### Script 2: regresión lineal
- Definir `consumo_kwh` como variable objetivo continua.
- Usar variables temporales y geográficas como predictores.
- Dividir en entrenamiento y prueba.
- Evaluar con `RMSE`, `MAE` y `R2`.

### Script 3: KMeans de contraste
- Usar las mismas agregaciones.
- Agrupar zonas por comportamiento de consumo.
- Evaluar con `silhouette` y `elbow`.

### Script 4: benchmark opcional con Random Forest Regressor
- Solo si la regresión lineal no explica bien la variación.
- Comparar errores y conservar solo si aporta mejora real.

### Script 5: Assess final
- Comparar resultados en tablas y gráficos.
- Elegir el enfoque que mejor ayude a interpretar consumo y a justificar decisiones.

## 13. Estructura de carpetas recomendada
La estructura canónica del proyecto queda así:

- `scripts/sample/` -> inspección local.
- `scripts/explore/` -> exploración descriptiva.
- `scripts/modify/` -> limpieza y transformación.
- `scripts/model/` -> modelado.
- `scripts/assess/` -> evaluación y comparación final.

Esto deja el flujo SEMMA explícito y facilita la entrega de evidencia por etapa.