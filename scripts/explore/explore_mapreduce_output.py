#!/usr/bin/env python3
"""Explore E.2: analisis de la salida TSV del MapReduce.

Lee la tabla agregada producida por el job MapReduce (sub-etapa E.1)
y produce estadisticas de distribucion de consumo por zona y mes.

Columnas de entrada (TSV, sin cabecera):
  ubigeo  mes  total_consumo_kwh  total_importe_soles  registros

Uso:
  spark-submit explore_mapreduce_output.py \
    --input hdfs://namenode:9000/user/hadoop/electropuno/mapreduce_agg/
"""

import argparse
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


# --------------------------------------------------------------------------- #
# Schema del archivo TSV producido por el reducer
# --------------------------------------------------------------------------- #
SCHEMA = StructType([
    StructField("ubigeo",              StringType(),  True),
    StructField("mes",                 StringType(),  True),
    StructField("total_consumo_kwh",   DoubleType(),  True),
    StructField("total_importe_soles", DoubleType(),  True),
    StructField("registros",           IntegerType(), True),
])


def load_ubigeo_map(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Ruta HDFS a la carpeta mapreduce_agg/")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("Explore_MapReduce_Output_Puno")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ----------------------------------------------------------------------- #
    # 1. Leer la salida TSV del reducer
    # ----------------------------------------------------------------------- #
    df = (
        spark.read
        .option("sep", "\t")
        .option("header", False)
        .schema(SCHEMA)
        .csv(args.input)
    )

    total_filas = df.count()
    print("\n=== Explore E.2: salida MapReduce ===")
    print("Total de filas (zona-mes): {}".format(total_filas))

    # Descartar filas con mes invalido (producto de campos CSV con ; dentro de comillas)
    df_invalidos = df.filter(~F.col("mes").rlike("^[0-9]{6}$"))
    n_invalidos = df_invalidos.count()
    if n_invalidos > 0:
        print("[INFO] {} filas con mes invalido descartadas (desalineamiento CSV por ; en campo de nombre).".format(n_invalidos))
    df = df.filter(F.col("mes").rlike("^[0-9]{6}$"))

    # ----------------------------------------------------------------------- #
    # 2. Cobertura temporal: que meses estan presentes
    # ----------------------------------------------------------------------- #
    print("\n--- Meses presentes en el dataset ---")
    df.select("mes").distinct().orderBy("mes").show(50, truncate=False)

    # ----------------------------------------------------------------------- #
    # 3. Registros por mes (detectar meses con pocos datos)
    # ----------------------------------------------------------------------- #
    print("--- Registros totales y consumo total por mes ---")
    df.groupBy("mes").agg(
        F.sum("registros").alias("registros_totales"),
        F.round(F.sum("total_consumo_kwh"), 2).alias("consumo_total_kwh"),
        F.round(F.sum("total_importe_soles"), 2).alias("importe_total_soles"),
    ).orderBy("mes").show(50, truncate=False)

    # ----------------------------------------------------------------------- #
    # 4. Top 10 zonas por consumo total (todos los meses sumados)
    # ----------------------------------------------------------------------- #
    print("--- Top 10 zonas por consumo total (todos los meses) ---")
    df.groupBy("ubigeo").agg(
        F.round(F.sum("total_consumo_kwh"), 2).alias("consumo_kwh_total"),
        F.round(F.sum("total_importe_soles"), 2).alias("importe_total"),
        F.sum("registros").alias("registros"),
    ).orderBy(F.desc("consumo_kwh_total")).show(10, truncate=False)

    # ----------------------------------------------------------------------- #
    # 5. Bottom 10 zonas (menor consumo)
    # ----------------------------------------------------------------------- #
    print("--- 10 zonas con menor consumo total ---")
    df.groupBy("ubigeo").agg(
        F.round(F.sum("total_consumo_kwh"), 2).alias("consumo_kwh_total"),
    ).orderBy("consumo_kwh_total").show(10, truncate=False)

    # ----------------------------------------------------------------------- #
    # 6. Enriquecimiento UBIGEO (si el mapa esta disponible)
    # ----------------------------------------------------------------------- #
    ubigeo_map = load_ubigeo_map("/opt/work/ubigeo_map.json")
    if ubigeo_map:
        print("--- Consumo por region (todos los meses) ---")
        broadcast_map = spark.sparkContext.broadcast(ubigeo_map)

        def get_region(ubigeo):
            districts = broadcast_map.value.get("districts", {})
            entry = districts.get(ubigeo, {})
            return entry.get("department", "DESCONOCIDO")

        get_region_udf = F.udf(get_region, StringType())
        df_enrich = df.withColumn("region", get_region_udf(F.col("ubigeo")))
        df_enrich.groupBy("region").agg(
            F.round(F.sum("total_consumo_kwh"), 2).alias("consumo_kwh_total"),
            F.countDistinct("ubigeo").alias("zonas"),
        ).orderBy(F.desc("consumo_kwh_total")).show(20, truncate=False)
    else:
        print("[AVISO] ubigeo_map.json no encontrado. Saltando enriquecimiento.")

    # ----------------------------------------------------------------------- #
    # 7. Nulos
    # ----------------------------------------------------------------------- #
    print("--- Valores nulos por columna ---")
    for col_name in df.columns:
        nulos = df.filter(F.col(col_name).isNull()).count()
        if nulos > 0:
            print(f"  {col_name}: {nulos} nulos")
    print("  (columnas sin nulos no se muestran)")

    print("\n=== Explore E.2 completado ===")
    spark.stop()


if __name__ == "__main__":
    main()
