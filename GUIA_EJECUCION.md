# Guia de ejecucion del proyecto HadoopSem15

Este documento ordena la ejecucion real del proyecto, desde el levantamiento de contenedores hasta la carga de datos, el analisis con Spark y la publicacion del repositorio.

## Recursos empleados

- Docker Desktop para levantar el entorno local.
- `docker-compose.yml` para definir Hadoop, HDFS, YARN y Spark.
- Contenedor `namenode` para almacenar archivos en HDFS.
- Contenedor `spark-master` para ejecutar los scripts de Spark.
- Archivo local `data.csv` como fuente de datos crudos.
- Carpeta `scripts/` para los scripts organizados por etapa SEMMA.
- Repositorio GitHub `https://github.com/AxelLoayza/HadoopSem15` para publicar el proyecto.

## 1. Levantar los contenedores

## ".\ejecutar_pipeline.ps1"


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

## 3. Crear la carpeta destino en HDFS

### Proposito
Preparar la ubicacion donde se almacenara el CSV para que Spark lo lea desde HDFS.

### Comando

```powershell
docker exec namenode hdfs dfs -mkdir -p /user/hadoop/electropuno
```

### Que hace
- Crea la ruta base del proyecto dentro de HDFS.
- Permite organizar la data por dominio y evitar mezclarla con otras cargas.

## 4. Subir el CSV a HDFS

### Proposito
Centralizar el archivo de entrada en almacenamiento distribuido para que todo el flujo trabaje sobre la misma fuente.

### Comando

```powershell
docker cp .\data.csv namenode:/tmp/electropuno.csv
docker exec namenode hdfs dfs -put -f /tmp/electropuno.csv /user/hadoop/electropuno/
```

### Que hace
- Copia el archivo local al contenedor `namenode`.
- Lo registra en HDFS bajo `/user/hadoop/electropuno/`.

### Verificacion

```powershell
docker exec namenode hdfs dfs -ls /user/hadoop/electropuno/
```

## 5. Ejecutar la etapa Sample

### Proposito
Revisar una muestra local antes de lanzar Spark sobre toda la data.

### Comando sample primer paso

```powershell
docker exec spark-master /spark/bin/spark-submit /opt/work/scripts/sample/inspect_sample.py --input data.csv
```

### Que hace
- Detecta separador.
- Revisa cantidad de columnas.
- Muestra filas de ejemplo.
- Permite detectar problemas de estructura antes del analisis distribuido.

## 6. Ejecutar la etapa Explore

### Proposito
Describir el dataset, detectar nulos, duplicados, categorias dominantes y enriquecer UBIGEO.

### Comando

```powershell
docker exec spark-master /spark/bin/spark-submit /opt/work/scripts/explore/explore_puno.py --input hdfs://namenode:9000/user/hadoop/electropuno/electropuno.csv
```

### Que hace
- Lee el CSV desde HDFS.
- Asigna nombres semanticos a las columnas.
- Limpia texto.
- Calcula nulos, repetidos y conteos distintos.
- Enriquce la zona geografica con UBIGEO.

## 7. Ejecutar la etapa Modify

### Proposito
Limpiar, tipar y transformar la informacion para dejarla lista para modelado.

### Comando

```powershell
docker exec spark-master /spark/bin/spark-submit /opt/work/scripts/modify/modify_puno.py
```

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

## 8. Construir el dataset para modelado

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

## 9. Ejecutar el modelado

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

## 10. Ejecutar Assess

### Proposito
Comparar metricas, revisar residuos y cerrar la seleccion del modelo.

### Comando

```powershell
docker exec spark-master /spark/bin/spark-submit /opt/work/scripts/model/assess_model_puno.py
```

### Que hace
- Revisa RMSE, MAE y R2 para regresion y Random Forest.
- Revisa silhouette para KMeans cuando corresponde.
- Resume la conclusion tecnica de la etapa.

## 11. Publicar el proyecto en GitHub

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

## 12. Observacion importante

No se debe subir `data.csv` ni `.venv` al repositorio.
La data grande queda local o en HDFS, y el entorno de Python se excluye con `.gitignore`.
