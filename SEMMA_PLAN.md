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
- Confirmar que el CSV está cargado correctamente en HDFS.
- Verificar la ruta final del archivo dentro de HDFS.
- Revisar que la muestra sea suficiente para el análisis inicial.

### E - Explore
- Cargar el dataset desde HDFS con Spark.
- Revisar esquema, tipos de datos y primeras filas.
- Identificar columnas clave para el análisis.
- Hacer filtros básicos por región, ubigeo, distrito, fecha o variables similares si existen.
- Generar conteos, nulos y estadísticas descriptivas.

### M - Modify
- Limpiar nombres de columnas si es necesario.
- Convertir tipos de datos incorrectos.
- Tratar valores nulos o duplicados.
- Filtrar registros irrelevantes para el análisis.
- Crear variables derivadas si ayudan a la interpretación.

### M - Model
- Definir una técnica simple de modelado según la estructura del CSV.
- Si los datos lo permiten, probar segmentación o agrupamiento básico.
- Si el objetivo del curso es más exploratorio que predictivo, esta fase puede centrarse en patrones y agrupaciones simples.
- MapReduce no se usa aquí como modelo; en este trabajo se considera componente de procesamiento distribuido dentro de Hadoop.

### A - Assess
- Interpretar resultados.
- Verificar si los hallazgos ayudan a entender el consumo eléctrico.
- Relacionar conclusiones con eficiencia, gestión de recursos o toma de decisiones.
- Preparar capturas de pantalla y salidas terminales para el PDF.

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
