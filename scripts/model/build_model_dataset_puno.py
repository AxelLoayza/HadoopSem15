"""Construye el dataset agregado para la etapa de modelado.

Toma la salida de Modify, agrupa por ubicacion y periodo, y genera una tabla
analitica lista para regresion, clustering o evaluacion comparativa.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F


DEFAULT_INPUT = "hdfs://namenode:9000/user/hadoop/electropuno/modified_puno"
DEFAULT_OUTPUT = "hdfs://namenode:9000/user/hadoop/electropuno/model_dataset_puno"

GROUP_COLUMNS = [
    "anio_facturacion",
    "mes_facturacion",
    "region_code",
    "region_name",
    "province_code",
    "province_name",
    "departamento",
    "provincia",
    "distrito",
    "ubigeo",
]


def build_spark(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Construccion del dataset para modelado en Puno")
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Ruta de entrada en HDFS o local")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Ruta de salida en HDFS o local")
    parser.add_argument("--format", default="parquet", choices=["parquet", "csv"], help="Formato de salida")
    return parser.parse_args()


def read_frame(spark: SparkSession, input_path: str):
    return spark.read.format("parquet").load(input_path)


def required_columns(df) -> list[str]:
    columns = [
        "anio_facturacion",
        "mes_facturacion",
        "region_code",
        "region_name",
        "province_code",
        "province_name",
        "departamento",
        "provincia",
        "distrito",
        "ubigeo",
        "consumo_kwh",
        "importe_soles",
    ]
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas necesarias para el modelado: {missing}")
    return columns


def build_dataset(df):
    _ = required_columns(df)

    cleaned = df.filter(
        F.col("anio_facturacion").isNotNull()
        & F.col("mes_facturacion").isNotNull()
        & F.col("ubigeo").isNotNull()
        & F.col("consumo_kwh").isNotNull()
    )

    return (
        cleaned.groupBy(*GROUP_COLUMNS)
        .agg(
            F.count(F.lit(1)).alias("registros"),
            F.sum(F.col("consumo_kwh")).alias("consumo_total_kwh"),
            F.avg(F.col("consumo_kwh")).alias("consumo_promedio_kwh"),
            F.sum(F.col("importe_soles")).alias("importe_total_soles"),
            F.avg(F.col("importe_soles")).alias("importe_promedio_soles"),
        )
        .orderBy("anio_facturacion", "mes_facturacion", "region_code", "province_code", "ubigeo")
    )


def main() -> None:
    args = parse_args()
    spark = build_spark("Build Model Dataset Puno")

    try:
        source_df = read_frame(spark, args.input)
        model_df = build_dataset(source_df)

        print("\n=== ESQUEMA DATASET MODELO ===")
        model_df.printSchema()

        print("\n=== MUESTRA DATASET MODELO ===")
        model_df.show(10, truncate=False)

        print("\n=== ESTADISTICAS DATASET MODELO ===")
        print(f"Filas: {model_df.count()}")
        print(f"Columnas: {len(model_df.columns)}")

        writer = model_df.write.mode("overwrite")
        if args.format == "csv":
            writer = writer.option("header", "true").option("delimiter", ";")
        writer.format(args.format).save(args.output)

        print(f"\nDataset de modelado guardado en: {args.output}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()