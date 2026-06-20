# Guia de ejecucion del proyecto HadoopSem15

Este documento ordena la ejecucion real del proyecto, desde el levantamiento de contenedores hasta la carga de datos, el analisis con Spark y la publicacion del repositorio.

## Recursos empleados

- Docker Desktop para levantar el entorno local.
- `docker-compose.yml` para definir Hadoop, HDFS, YARN y Spark.
- Contenedor `namenode` para almacenar archivos en HDFS.
- Contenedor `spark-master` para ejecutar los scripts de Spark.
- Archivo local `data_<Mes>_2026.csv` como fuente de datos crudos (ej. `data_Mayo_2026.csv`).
- Carpeta `scripts/` para los scripts organizados por etapa SEMMA.
- Repositorio GitHub `https://github.com/AxelLoayza/HadoopSem15` para publicar el proyecto.

## 1. Levantar los contenedores

### Proposito
Iniciar la infraestructura distribuida local para disponer de almacenamiento HDFS y motor de procesamiento Spark.

### Comando

```powershell
Set-Location "c:\Users\axtev\Documents\Proyects\Int de Negocios"
docker compose -f .\docker-compose.yml up -d --force-recreate namenode datanode resourcemanager nodemanager spark-master spark-worker
```

### Que hace
- Crea o recrea los servicios del cluster.
- Deja HDFS listo para guardar datos.
- Deja Spark listo para ejecutar los scripts de exploracion, modificacion y modelado.

## 2. Verificar el estado del cluster

### Proposito
Confirmar que los servicios principales quedaron activos antes de cargar datos.

### Comandos

```powershell
docker ps
docker exec namenode hdfs dfs -ls /
```

### Que hace
- `docker ps` confirma que los contenedores estan arriba.
- `hdfs dfs -ls /` valida que HDFS responde dentro del contenedor `namenode`.

## 3. Crear la estructura de carpetas en HDFS

### Proposito
Preparar las rutas donde se almacenaran CSVs crudos, la salida del MapReduce y las salidas Parquet.

### Comando

```powershell
docker exec namenode hdfs dfs -mkdir -p /user/hadoop/electropuno/raw
docker exec namenode hdfs dfs -mkdir -p /user/hadoop/electropuno/mapreduce_agg
```

### Que hace
- Crea `/raw/` para alojar los CSVs crudos mensuales (un archivo por mes).
- Crea `/mapreduce_agg/` para la salida del job MapReduce (agregado por ubigeo y mes).

## 4. Ejecutar la etapa Sample (local, antes de subir a HDFS)

### Proposito
Revisar estructura y calidad del CSV localmente antes de enviarlo al cluster.

### Comando

```powershell
# Desde la raiz del proyecto, con Python local
python scripts/sample/inspect_sample.py
```

> **Importante:** este script se ejecuta localmente con `python`, NO con `spark-submit` ni dentro de ningun contenedor. No requiere que el cluster este activo.

### Que hace
- Detecta el separador del archivo.
- Cuenta columnas y muestra filas de ejemplo.
- Permite detectar problemas de estructura antes de cargar a HDFS.

## 5. Subir el CSV a HDFS

### Proposito
Centralizar el archivo mensual en almacenamiento distribuido para que todo el flujo trabaje sobre la misma fuente.

### Comando

```powershell
docker cp .\data_Mayo_2026.csv namenode:/tmp/data_Mayo_2026.csv
docker exec namenode hdfs dfs -put -f /tmp/data_Mayo_2026.csv /user/hadoop/electropuno/raw/
```

> Para cargar otro mes, repetir el proceso reemplazando el nombre del archivo (ej. `data_Junio_2026.csv`). Todos los meses coexisten en `/raw/`.

### Que hace
- Copia el CSV local al contenedor `namenode`.
- Lo registra en HDFS bajo `/user/hadoop/electropuno/raw/`.
- Para multiples meses, la carpeta `/raw/` acumula todos los archivos y el MapReduce los procesa juntos.

### Verificacion

```powershell
docker exec namenode hdfs dfs -ls /user/hadoop/electropuno/raw/
```

## 6. Ejecutar el job MapReduce — E.1 de Explore

### Proposito
Agregar el consumo por zona (ubigeo) y mes antes de que Spark explore los datos.
Permite ver de forma inmediata que zonas consumen mas, como evoluciona el consumo entre meses
y si hay periodos con pocos registros o zonas que aparecen solo en algunos meses.

### Comando

```powershell
# 1. Copiar los scripts al namenode (no tiene el mount de ./scripts)
docker cp scripts/explore/mapreduce/mapper.py namenode:/tmp/mapper.py
docker cp scripts/explore/mapreduce/reducer.py namenode:/tmp/reducer.py

# 2. Lanzar el job MapReduce desde el namenode
docker exec namenode bash -c "
  hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
  -input /user/hadoop/electropuno/raw/ \
  -output /user/hadoop/electropuno/mapreduce_agg \
  -mapper 'python3 mapper.py' \
  -reducer 'python3 reducer.py' \
  -file /tmp/mapper.py \
  -file /tmp/reducer.py
"
```

> `-file` distribuye el script al directorio de trabajo de cada tarea YARN; por eso se referencia solo por nombre (`mapper.py`), no por ruta absoluta.

> Si la carpeta ya existe de una corrida anterior, borrarla primero:
> `docker exec namenode hdfs dfs -rm -r /user/hadoop/electropuno/mapreduce_agg`

### Que hace
- Lanza un job YARN MapReduce sobre toda la carpeta `/raw/` (todos los meses a la vez).
- El mapper extrae ubigeo, mes, consumo_kwh e importe_soles de cada linea cruda.
- El reducer suma consumo, importe y cuenta registros por zona y mes.
- Produce una tabla TSV de pocas miles de filas: `ubigeo | mes | total_consumo_kwh | total_importe_soles | registros`.

### Verificacion

```powershell
docker exec namenode hdfs dfs -ls /user/hadoop/electropuno/mapreduce_agg/
docker exec namenode hdfs dfs -cat /user/hadoop/electropuno/mapreduce_agg/part-00000 | head -20
```

## 7. Ejecutar la etapa Explore — E.2 (analisis del agregado)

### Proposito
Describir la distribucion de consumo por zona y periodo usando la tabla compacta producida por MapReduce.

### Comando

```powershell
docker exec spark-master /spark/bin/spark-submit /opt/work/scripts/explore/explore_mapreduce_output.py --input hdfs://namenode:9000/user/hadoop/electropuno/mapreduce_agg/
```

### Que hace
- Lee la salida TSV del MapReduce (5 columnas: ubigeo, mes, total_consumo_kwh, total_importe_soles, registros).
- Muestra las zonas con mayor y menor consumo.
- Compara la evolucion del consumo por mes.
- Detecta meses con pocos registros (posible archivo incompleto).
- Enriquece con UBIGEO para mostrar nombres de region, provincia y distrito.

## 8. Ejecutar la etapa Modify

### Proposito
Limpiar, tipar y transformar la informacion para dejarla lista para modelado.

### Comando

```powershell
docker exec spark-master /spark/bin/spark-submit \
  /opt/work/scripts/modify/modify_puno.py \
  --input hdfs://namenode:9000/user/hadoop/electropuno/raw/
```

> **Importante:** Modify lee los CSVs crudos desde `/user/hadoop/electropuno/raw/`, NO la salida del MapReduce. Esta es la fuente que garantiza las 25 columnas necesarias para el enriquecimiento completo.

### Que hace
- Convierte fechas a tipo fecha.
- Convierte importes y consumo a numerico.
- Agrega variables derivadas como `anio_corte`, `mes_corte`, `anio_facturacion`, `mes_facturacion` y `categoria_consumo`.
- Guarda la salida transformada en HDFS como Parquet.

### Verificacion

```powershell
docker exec namenode hdfs dfs -ls /user/hadoop/electropuno/modified_puno
docker exec namenode hdfs dfs -count /user/hadoop/electropuno/modified_puno
```

## 9. Construir el dataset para modelado

### Proposito
Agrupar la informacion por zona y periodo para crear una tabla analitica compacta.

### Comando

```powershell
docker exec spark-master /spark/bin/spark-submit /opt/work/scripts/model/build_model_dataset_puno.py
```

### Que hace
- Agrupa por `anio_facturacion`, `mes_facturacion`, `region`, `provincia`, `distrito` y `ubigeo`.
- Calcula consumo total, consumo promedio e importe total.
- Genera el insumo principal de Model.

## 10. Ejecutar el modelado

### Proposito
Comparar el comportamiento del consumo con distintos enfoques de ML.

### Comandos

```powershell
docker exec spark-master /spark/bin/spark-submit /opt/work/scripts/model/model_regression_puno.py
docker exec spark-master /spark/bin/spark-submit /opt/work/scripts/model/model_kmeans_puno.py
docker exec spark-master /spark/bin/spark-submit /opt/work/scripts/model/model_random_forest_puno.py
```

### Que hace
- `model_regression_puno.py` entrena la ruta principal de pronostico.
- `model_kmeans_puno.py` segmenta zonas por comportamiento de consumo.
- `model_random_forest_puno.py` sirve como benchmark comparativo.

## 11. Ejecutar Assess

### Proposito
Comparar metricas entre todos los modelos, revisar residuos y exportar los datos necesarios para generar graficos localmente.

### Comando (en Docker)

```powershell
docker exec spark-master /spark/bin/spark-submit /opt/work/scripts/model/assess_model_puno.py
```

### Que hace
- Revisa RMSE, MAE y R2 para regresion lineal y Random Forest.
- Revisa silhouette para KMeans.
- Imprime la tabla comparativa con interpretacion automatica del modelo ganador.
- Exporta CSVs a `./output/` (via el bind mount):
  - `output/metrics_summary.csv` — metricas de los tres modelos
  - `output/regression_predictions.csv` — predicciones de regresion con error
  - `output/rf_predictions.csv` — predicciones de Random Forest con error
  - `output/kmeans_predictions.csv` — asignaciones de cluster por zona

## 12. Generar graficos localmente

### Proposito
Visualizar los resultados del Assess con graficos guardados como PNG en `./output/plots/`.

### Requisito previo (una sola vez)

```powershell
pip install -r requirements.txt
```

### Comando

```powershell
python scripts/assess/plot_results.py
```

### Que genera

| Archivo | Contenido |
|---------|-----------|
| `comparativa_modelos.png` | Barras RMSE / MAE / R2 para regresion vs RF |
| `regresion_predicho_vs_real.png` | Scatter predicho vs real con linea perfecta |
| `regresion_residuos.png` | Histograma de residuos de regresion |
| `rf_predicho_vs_real.png` | Scatter predicho vs real para Random Forest |
| `kmeans_clusters.png` | Distribucion de zonas y consumo promedio por cluster |

Todos los graficos quedan en `./output/plots/` y se pueden abrir directamente en Windows.

## 13. Publicar el proyecto en GitHub

### Proposito
Dejar el material documentado y compartible en el repositorio remoto.

### Comandos

```powershell
git init -b main
git remote add origin https://github.com/AxelLoayza/HadoopSem15
git add .
git commit -m "Publish project without local dataset"
git push -u origin main --force-with-lease
```

### Que hace
- Inicializa el repositorio local.
- Conecta el remoto.
- Registra los cambios del proyecto.
- Sube el contenido al repositorio GitHub.

## 14. Observacion importante

No se debe subir `data_<Mes>_2026.csv` ni `.venv` al repositorio.
La data grande queda local o en HDFS, y el entorno de Python se excluye con `.gitignore`.
