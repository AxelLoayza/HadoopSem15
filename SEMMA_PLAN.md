# Plan de trabajo SEMMA para el análisis de consumo eléctrico de Puno

## 1. Objetivo inmediato
Preparar un flujo de trabajo claro para validar la configuración de Hadoop/Spark y luego explorar el CSV de consumo eléctrico, siguiendo la metodología SEMMA.

Regla de trabajo actual:
- primero entender la estructura real del CSV con una muestra local,
- después pasar a Spark para la fase Explore,
- y usar MapReduce solo como parte de la arquitectura/procesamiento de Hadoop, no como el modelo final.

## 2. Decisión sobre el archivo de Spark
No es obligatorio crear un `spark.py` por ahora.

Recomendación práctica:
- Si solo vas a hacer pruebas rápidas, puedes usar `pyspark` interactivo dentro del contenedor `spark-master`.
- Si necesitas evidencias para el PDF y reutilizar el análisis, sí conviene crear un script, por ejemplo `explore_puno.py`.
- Para la entrega, un script es mejor que trabajar solo de forma interactiva porque deja trazabilidad.

## 3. Orden de trabajo propuesto

### S - Sample
- Leer una muestra del CSV con Python para verificar cabecera, separador, cantidad de columnas y estructura real.
- Si se trabaja con varios meses, ejecutar `inspect_sample.py` sobre cada archivo para confirmar que todos tienen exactamente 25 columnas y el mismo orden.
- Confirmar que los CSV están cargados correctamente en HDFS bajo `/user/hadoop/electropuno/raw/`.
- **Script:** `scripts/sample/inspect_sample.py`

### E - Explore (etapa con MapReduce integrado)

El Explore tiene dos sub-pasos cuando se trabaja con datos multi-mes.

#### E.1 - MapReduce: agregacion por zona y mes
- Ejecutar un job MapReduce con Hadoop Streaming sobre la carpeta `/user/hadoop/electropuno/raw/`.
- El mapper extrae `ubigeo` y `periodo_facturado` de cada linea cruda y emite clave `ubigeo|mes` con consumo e importe.
- El reducer suma `consumo_kwh`, `importe_soles` y cuenta registros por clave.
- Salida en `/user/hadoop/electropuno/mapreduce_agg/` (formato TSV, pocas miles de filas).
- **Proposito analitico:**
  - Ver que zonas consumen mas por mes.
  - Comparar como evoluciona el consumo de cada zona a lo largo de los meses disponibles.
  - Detectar meses con pocos registros (posible archivo incompleto o corte distinto).
  - Identificar zonas que aparecen o desaparecen entre periodos.
- **Proposito tecnico:** reduce el volumen que Spark procesa en E.2 y demuestra uso real de MapReduce.
- **Scripts implementados:** `scripts/explore/mapreduce/mapper.py` y `scripts/explore/mapreduce/reducer.py`

#### E.2 - Explore Spark sobre la salida de MapReduce
- Spark lee la salida ligera del job MapReduce, no los CSV crudos completos.
- Esto evita que el worker con 1 nucleo y 1GB RAM colapse bajo el peso de multiples archivos grandes.
- Detecta distribuciones de consumo, valores nulos y comportamiento por zona.
- **Script:** `scripts/explore/explore_puno.py` (sin cambios en su cuerpo, solo cambia el `--input`)

#### Compatibilidad garantizada
El Explore con MapReduce **no alimenta a Modify**. Son ramas separadas:
- MapReduce produce un reporte de exploración.
- Modify sigue leyendo los CSV crudos completos con las 25 columnas originales.

### M - Modify
- Lee los CSV crudos desde HDFS (25 columnas originales).
- Tipa, limpia, enriquece UBIGEO y deriva variables.
- **No cambia** respecto al diseño actual.
- **Script:** `scripts/modify/modify_puno.py`

### M - Model
- Lee el output de Modify en Parquet.
- `build_model_dataset_puno.py` agrega por zona y periodo.
- Regresión lineal como ruta principal, KMeans como exploratorio, Random Forest como benchmark.
- **No cambia** respecto al diseño actual.
- **Scripts:** `scripts/model/`

### A - Assess
- Compara métricas entre modelos.
- Cierra con la decisión técnica sobre el modelo elegido.
- **Script:** `scripts/model/assess_model_puno.py`

## 3.1 Diagrama del flujo completo

```
CSV multi-mes en HDFS (/raw/)
        |
        +--- [MapReduce: mapper.py + reducer.py]
        |         Salida: /mapreduce_agg/  (tabla ligera)
        |         Uso: Explore Spark lee esta salida (no los crudos)
        |
        +--- [Modify: modify_puno.py]
                  Lee: CSV crudos (25 col)
                  Salida: /modified_puno/ (Parquet)
                       |
                  [build_model_dataset_puno.py]
                       |
                  [model_regression / kmeans / random_forest]
                       |
                  [assess_model_puno.py]
```

**Regla clave:** MapReduce y Modify leen la misma fuente cruda pero producen salidas distintas para propósitos distintos. Nunca se encadenan uno al otro.

## 4. Entregables que conviene preparar
- Un archivo de script o notebook con los comandos usados.
- Capturas de pantalla de la configuración y ejecución.
- Salidas de `hdfs dfs -ls`, carga del CSV y resultados de Spark.
- Un PDF con explicación breve de cada fase SEMMA.

## 5. Evidencias mínimas para la rúbrica
### Instalación y configuración de Hadoop
- Mostrar que los contenedores levantan correctamente.
- Mostrar acceso a NameNode, YARN y Spark.
- Mostrar que HDFS acepta directorios y archivos.

### Arquitectura distribuida de Hadoop
- Explicar el rol de HDFS, YARN y MapReduce.
- Indicar cómo interactúan con Spark en el flujo de análisis.
- Aclarar que MapReduce es un motor de procesamiento distribuido de Hadoop y que Spark puede usarse como capa de análisis más flexible encima de HDFS.

### Aplicabilidad en datos masivos
- Explicar por qué Hadoop sirve para datos grandes y distribuidos.
- Relacionar el caso con el análisis de consumo eléctrico.

### Impacto en desarrollo sostenible
- Vincular el análisis con eficiencia energética.
- Explicar cómo la analítica ayuda a tomar mejores decisiones de recursos.

## 6. Flujo técnico recomendado
1. Leer una muestra local del CSV con Python.
2. Levantar contenedores con Docker Compose.
3. Cargar el CSV a HDFS.
4. Abrir Spark en modo interactivo o ejecutar un script.
5. Explorar el dataset.
6. Limpiar y preparar datos.
7. Generar hallazgos.
8. Redactar conclusiones para el PDF.

## 7. Próximo paso sugerido
Crear un script de exploración, por ejemplo `explore_puno.py`, para ejecutar la fase E de SEMMA con evidencia reproducible.

## 8. Criterio práctico final
Para esta actividad no hace falta complicar el proyecto desde el inicio. Primero valida que:
- la estructura del CSV sea comprensible,
- Hadoop levanta bien.
- HDFS recibe el CSV.
- Spark puede leer el archivo desde HDFS.

Cuando eso funcione, recién pasas a la exploración formal.
