# MapReduce en la etapa Explore

## Proposito
Este directorio contiene el mapper y el reducer de Hadoop Streaming para la sub-etapa E.1 del flujo SEMMA.

El objetivo tiene dos dimensiones:
1. **Reduccion de costo computacional:** agrega antes de que Spark procese los datos, evitando
   que el worker de 1 core y 1GB RAM opere sobre millones de filas crudas.
2. **Visibilidad analitica:** produce una tabla de consumo total, importe total y cantidad de
   registros **por zona geografica (ubigeo) y por mes**. Eso permite ver de forma inmediata:
   - que zonas consumen mas o menos por periodo,
   - como evoluciona el consumo a lo largo de los meses disponibles,
   - que meses tienen mas o menos registros (posible dato faltante o corte distinto),
   - si hay zonas que aparecen solo en algunos meses.

## Posicion en el flujo

```
CSV multi-mes en HDFS (/raw/)
        |
        [mapper.py] -> emite clave ubigeo|mes y valores consumo|importe
        [reducer.py] -> agrega suma y conteo por clave
        |
Salida: /user/hadoop/electropuno/mapreduce_agg/
        |
[explore_puno.py] lee esta salida con Spark
```

MapReduce NO alimenta a Modify ni a Model. Es una rama de exploración y validación paralela.

## Columnas que produce el reducer

| Campo | Tipo | Descripcion |
|-------|------|-------------|
| ubigeo | string | codigo de 6 digitos |
| mes | string | periodo YYYYMM extraido de `periodo_facturado` |
| total_consumo_kwh | float | suma de consumo del mes por ubigeo |
| total_importe_soles | float | suma de importe del mes por ubigeo |
| registros | int | cantidad de filas del mes por ubigeo |

## Que se puede analizar con este agregado

- **Por zona:** ordenar ubigeos por `total_consumo_kwh` para identificar las zonas de mayor demanda.
- **Por mes:** comparar como varia el consumo de una misma zona entre meses (tendencia, estacionalidad).
- **Cobertura temporal:** ver que meses tienen datos y si alguna zona desaparece o aparece en periodos especificos.
- **Consistencia:** si un mes tiene muchos menos registros que los demas, puede indicar un archivo incompleto o un corte distinto.

## Scripts implementados

- `mapper.py`: lee lineas del CSV crudo (25 columnas, separador `;`), extrae ubigeo y periodo, emite clave `ubigeo|mes` con consumo e importe.
- `reducer.py`: acumula suma de consumo, suma de importe y conteo por clave, emite una linea TSV por zona y mes.

## Comando de ejecucion

```bash
docker exec namenode hadoop jar \
  /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
  -input /user/hadoop/electropuno/raw/ \
  -output /user/hadoop/electropuno/mapreduce_agg \
  -mapper "python3 /tmp/mapper.py" \
  -reducer "python3 /tmp/reducer.py" \
  -file /opt/work/scripts/explore/mapreduce/mapper.py \
  -file /opt/work/scripts/explore/mapreduce/reducer.py
```

## Verificacion

```bash
docker exec namenode hdfs dfs -ls /user/hadoop/electropuno/mapreduce_agg
docker exec namenode hdfs dfs -cat /user/hadoop/electropuno/mapreduce_agg/part-00000 | head -20
```
