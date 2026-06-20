"""Vista previa de la etapa Modify para el consumo electrico de Puno.

Este script lee la salida transformada por `modify_puno.py` y muestra en terminal
el esquema limpio, una muestra de filas, conteos basicos y categorias derivadas.
Sirve como evidencia de la fase Modify en SEMMA.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F


DEFAULT_INPUT = "hdfs://namenode:9000/user/hadoop/electropuno/modified_puno"


def build_spark(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Vista previa de la etapa Modify en Puno")
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT,
        help="Ruta de entrada del dataset modificado en HDFS o local",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Cantidad de filas a mostrar",
    )
    return parser.parse_args()


def read_frame(spark: SparkSession, input_path: str):
    return spark.read.parquet(input_path)


def show_null_summary(df):
    total_rows = df.count()
    null_counts = df.select([
        F.sum(F.when(F.col(column).isNull() | (F.col(column) == ""), 1).otherwise(0)).alias(column)
        for column in df.columns
    ])
    print("\n=== NULOS POR COLUMNA (MODIFY) ===")
    null_counts.show(truncate=False)

    print("\n=== PORCENTAJE DE NULOS (MODIFY) ===")
    null_percentage = null_counts.select([
        (F.col(column) / F.lit(total_rows) * 100).alias(column)
        for column in null_counts.columns
    ])
    null_percentage.show(truncate=False)


def show_key_counts(df):
    print("\n=== VALORES DISTINCTOS CLAVE (MODIFY) ===")
    key_columns = [
        "region_name",
        "province_name",
        "district_name",
        "departamento",
        "provincia",
        "distrito",
        "tarifa",
        "categoria_consumo",
    ]
    for column in key_columns:
        if column in df.columns:
            print(f"- {column}: {df.select(column).distinct().count()}")


def show_repeated_values(df):
    total_rows = df.count()
    print("\n=== COLUMNAS MAS REPETIDAS (MODIFY) ===")
    repeated_columns = [
        "nombre_empresa",
        "nombre_unidad_negocio",
        "departamento",
        "provincia",
        "distrito",
        "ubigeo",
        "categoria_consumo",
    ]
    data = []
    for column in repeated_columns:
        if column in df.columns:
            distinct_count = df.select(column).distinct().count()
            data.append((column, distinct_count, total_rows - distinct_count))

    if data:
        spark = SparkSession.getActiveSession()
        summary = spark.createDataFrame(data, ["columna", "valores_distintos", "valores_repetidos"]).orderBy(F.desc("valores_repetidos"))
        summary.show(truncate=False)


def show_sample(df, limit: int):
    print("\n=== MUESTRA TRANSFORMADA (MODIFY) ===")
    columns = [
        "fecha_corte",
        "nombre_empresa",
        "nombre_unidad_negocio",
        "departamento",
        "provincia",
        "distrito",
        "ubigeo",
        "region_name",
        "province_name",
        "district_name",
        "anio_corte",
        "mes_corte",
        "anio_facturacion",
        "mes_facturacion",
        "importe_soles",
        "consumo_kwh",
        "categoria_consumo",
    ]
    columns = [column for column in columns if column in df.columns]
    df.select(*columns).show(limit, truncate=False)


def main() -> None:
    args = parse_args()
    spark = build_spark("Preview Modify Puno")

    try:
        df = read_frame(spark, args.input)

        print("\n=== ESQUEMA MODIFICADO ===")
        df.printSchema()

        print("\n=== ESTADISTICAS GENERALES MODIFY ===")
        print(f"Filas: {df.count()}")
        print(f"Columnas: {len(df.columns)}")

        show_null_summary(df)
        show_repeated_values(df)
        show_key_counts(df)
        show_sample(df, args.limit)

        print("\n=== ORDEN DE EJEMPLO ===")
        order_columns = [column for column in ["departamento", "provincia", "distrito", "ubigeo"] if column in df.columns]
        if order_columns:
            df.orderBy(*[F.col(column) for column in order_columns]).select(
                "fecha_corte",
                "departamento",
                "provincia",
                "distrito",
                "ubigeo",
                "importe_soles",
                "consumo_kwh",
                "categoria_consumo",
            ).show(args.limit, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
