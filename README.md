# HadoopSem15

Proyecto de analisis de consumo electrico de Puno con Hadoop, HDFS, Spark y flujo SEMMA.

## Estructura

- `docker-compose.yml` define el cluster local.
- `data.csv` es la data base de trabajo.
- `scripts/` contiene los scripts organizados por etapa.
- `PLAN_ANALISIS_PUNO.md` y `PLAN_MODELO_PUNO.md` documentan la estrategia.
- `VITACORA_PROGRESO.md` registra avances y decisiones.

## Scripts por etapa

- `scripts/sample/README.md` y `scripts/sample/inspect_sample.py` para muestreo local.
- `scripts/explore/explore_puno.py` para exploracion con Spark.
- `scripts/modify/modify_puno.py` y `scripts/modify/preview_modify_puno.py` para limpieza y validacion.
- `scripts/model/README.md` y los scripts de modelado para regresion, KMeans, Random Forest y Assess.
- `scripts/assess/README.md` para la evaluacion final.

## Nota

El entorno virtual local de Python se excluye del repositorio mediante `.gitignore`.
