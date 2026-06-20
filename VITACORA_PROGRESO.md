# Bitácora de progreso - Análisis de consumo eléctrico de Puno

## Proyecto
Análisis de datos de consumo eléctrico de Puno con enfoque SEMMA usando Hadoop, HDFS y Spark.

## Objetivo general
Explorar, limpiar y analizar el CSV para identificar patrones de consumo y justificar posibles estrategias de ahorro energético o focalización.

## Registro de avances

### Fecha: 2026-06-18
### Estado: Inicio de planificación

#### Actividades realizadas
- Revisión inicial de `docker-compose.yml`.
- Confirmación de que HDFS y los contenedores principales pueden levantarse.
- Validación de la ruta de carga del CSV en HDFS.
- Definición preliminar del flujo SEMMA.
- Creación del plan base de análisis.

#### Hallazgos iniciales
- El archivo local detectado es `data.csv.csv`, no `data.csv`.
- El entorno está orientado a análisis distribuido con Hadoop y Spark.
- La etapa crítica ahora es explorar la calidad del dataset antes de modelar.

#### Decisiones provisionales
- No iniciar todavía con modelado directo.
- Primero revisar estructura, nulos, duplicados y consistencia de categorías.
- Evaluar si el mejor enfoque será:
  - agrupación descriptiva,
  - KMeans para segmentación,
  - o árbol de decisión si existe una variable objetivo clara.

#### Próximo paso
- Ejecutar exploración inicial con Spark.
- Revisar columnas del CSV.
- Identificar si ubigeo, región u otra categoría puede ser usada para análisis territorial.

### Fecha: 2026-06-19
### Estado: Exploración completada

#### Actividades realizadas
- Se ejecutó la fase Explore con Spark sobre el CSV cargado en HDFS.
- Se actualizó el script para usar nombres semánticos de acuerdo con la descripción del dataset.
- Se incorporó el mapeo de UBIGEO para enriquecer región, provincia y distrito.
- Se añadieron salidas de nulos, porcentaje de nulos, columnas más repetidas y filas duplicadas.

#### Resultados observados
- El dataset tiene 25 columnas y el script ya las expone con nombres semánticos.
- El análisis detectó 199 ubigeos distintos, 13 regiones y 64 provincias observadas en la data.
- La muestra final ya muestra campos útiles como fecha_corte, nombre_empresa, departamento, provincia, distrito, ubigeo, importe_soles y consumo_kwh.
- La exploración confirmó que la data está fuertemente concentrada en zonas como Piura, pero también contiene otras regiones.

#### Conclusiones preliminares
- La fase Explore quedó lista para documentar en el informe.
- La siguiente etapa natural es Modify.
- Modify debe enfocarse en tipado, limpieza, estandarización y creación de variables útiles para segmentación o clasificación.

#### Próximo paso
- Crear un script separado para Modify.
- Convertir fechas y campos numéricos a tipos correctos.
- Preparar variables derivadas para el modelado posterior.

### Fecha: 2026-06-19
### Estado: Preparación de Modify y reflexión técnica

#### Actividades realizadas
- Se creó un script de Modify separado para limpiar, tipar y enriquecer la data.
- Se creó un script de vista previa de Modify para validar en terminal la salida transformada.
- Se verificó que la escritura a HDFS quedó materializada en formato Parquet.
- Se confirmó que la transformación produce un dataset listo para futuras etapas de modelado.

#### Hallazgos de la etapa Modify
- La salida transformada ya existe en HDFS y contiene archivos Parquet con éxito de escritura.
- `fecha_corte` y las fechas de facturación quedan listas para trabajar como fechas reales.
- `importe_soles` y `consumo_kwh` quedan convertidos a numérico para análisis posterior.
- Se agregan variables derivadas como `anio_corte`, `mes_corte`, `anio_facturacion`, `mes_facturacion` y `categoria_consumo`.

#### Reflexión sobre la arquitectura distribuida
- Hadoop aporta la base de almacenamiento distribuido mediante HDFS, mientras que YARN coordina los recursos y Spark actúa como motor de análisis.
- Esta arquitectura es importante en datos masivos porque permite separar almacenamiento, coordinación y cómputo, evitando depender de una sola máquina.
- La distribución del procesamiento facilita explorar grandes volúmenes de registros sin perder trazabilidad ni escalabilidad.
- En este caso, la data de consumo eléctrico se puede leer, transformar y preparar de forma ordenada antes de modelar, lo que sería mucho más costoso en un entorno no distribuido.

#### Reflexión sobre transformación digital
- El uso de Hadoop y Spark apoya la transformación digital porque convierte datos crudos en información accionable para la toma de decisiones.
- En una empresa o institución, esta lógica permite pasar de registros aislados a indicadores de consumo, segmentación geográfica y priorización de atención.
- El flujo desarrollado demuestra cómo una organización puede modernizar su análisis sin depender de procesos manuales o fragmentados.

#### Reflexión sobre desarrollo sostenible
- El análisis de consumo eléctrico tiene relación directa con eficiencia energética, uso responsable de recursos y mejor focalización de intervenciones.
- Identificar zonas o patrones de alto consumo puede apoyar campañas de ahorro y reducir desperdicios energéticos.
- La analítica distribuida no solo procesa datos, también ayuda a producir decisiones con impacto en sostenibilidad, especialmente en gestión de energía y planificación territorial.

#### Próximo paso
- Usar la salida de Modify como base para el modelado.
- Definir si el siguiente enfoque será KMeans o clasificación supervisada.
- Redactar las conclusiones técnicas para el PDF de entrega.

### Fecha: 2026-06-19
### Estado: Plan de modelado definido

#### Decisión técnica
- El objetivo principal del modelado será pronosticar consumo futuro por zona y periodo.
- La ruta principal será regresión lineal sobre datos agregados por zona-periodo.
- KMeans quedará como comparación exploratoria para segmentación de zonas.
- Random Forest solo se considerará como benchmark opcional si la relación es no lineal o si la regresión lineal no alcanza suficiente capacidad explicativa.

#### Criterio de evaluación
- Para regresión: `RMSE`, `MAE`, `R2` y gráficos de real vs predicho y residuales.
- Para KMeans: silhouette, elbow y perfiles de cluster.
- Para Random Forest: comparación de error, `R2` e importancia de variables.

#### Criterio de cierre en Assess
- Se elegirá el enfoque que mejor combine precisión, interpretabilidad y utilidad para la gestión energética.
- Los gráficos se usarán en Assess para justificar la elección final del modelo, no solo para mostrar resultados aislados.

#### Próximo paso
- Diseñar los scripts de modelado con esa secuencia.
- Preparar primero la tabla agregada zona-periodo.
- Luego implementar regresión lineal y, si corresponde, KMeans como contraste.

### Fecha: 2026-06-19
### Estado: Estructura de scripts por etapa

#### Decisión organizativa
- Se reorganizó la carpeta `scripts/` por etapas SEMMA.
- La ruta canónica ahora queda dividida en `sample`, `explore`, `modify`, `model` y `assess`.
- Se conservarán scripts separados para inspección local, exploración Spark, modificación y vista previa de la salida transformada.

#### Estructura creada
- `scripts/sample/inspect_sample.py`
- `scripts/explore/explore_puno.py`
- `scripts/modify/modify_puno.py`
- `scripts/modify/preview_modify_puno.py`
- `scripts/model/README.md`
- `scripts/assess/README.md`

#### Criterio de uso
- `sample` valida la estructura real del CSV.
- `explore` documenta calidad y distribución.
- `modify` limpia, tipa y enriquece.
- `model` quedará para regresión, KMeans y benchmark de Random Forest.
- `assess` consolidará gráficas y métricas para la decisión final.

#### Próximo paso
- Implementar los scripts de modelado dentro de la carpeta correspondiente.
- Mantener el PDF y la bitácora alineados con esta estructura por etapas.

### Fecha: 2026-06-19
### Estado: Modelado inicial implementado

#### Actividades realizadas
- Se crearon los scripts de modelado en `scripts/model/`.
- Se implementó la construcción del dataset agregado para modelado.
- Se creó la regresion lineal como ruta principal de pronostico.
- Se incorporo KMeans como segmentacion exploratoria.
- Se agrego Random Forest como benchmark comparativo.
- Se preparo un script de Assess para comparar metricas y cerrar la decision tecnica.

#### Estructura implementada
- `scripts/model/build_model_dataset_puno.py`
- `scripts/model/model_regression_puno.py`
- `scripts/model/model_kmeans_puno.py`
- `scripts/model/model_random_forest_puno.py`
- `scripts/model/assess_model_puno.py`

#### Salidas esperadas
- Dataset agregado en HDFS para el entrenamiento y la segmentacion.
- Predicciones de regresion con errores por fila.
- Predicciones de Random Forest con errores por fila.
- Clusters de KMeans con vector de features para evaluar silhouette.
- Reporte comparativo final en Assess.

#### Próximo paso
- Ejecutar primero la construccion del dataset de modelado.
- Luego correr la regresion lineal.
- Comparar contra KMeans y Random Forest en Assess.

## Evidencias que se deben guardar
- Capturas de la configuración de Hadoop.
- Capturas de `hdfs dfs -ls` y carga del CSV.
- Salidas de Spark al leer el dataset.
- Resultados de limpieza y agrupación.
- Conclusiones finales para el PDF.

## Observaciones para futuras versiones
- Registrar cada cambio importante de datos o script.
- Anotar decisiones de modelado con su justificación.
- Guardar resultados que sirvan como evidencia para la rúbrica.
- Separar Explore y Modify en scripts distintos para mantener trazabilidad y evitar mezclar limpieza con análisis descriptivo.