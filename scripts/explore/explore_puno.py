"""Exploracion inicial del CSV de consumo electrico de Puno con Spark.

Este script asume un archivo sin cabecera y delimitado por punto y coma (;).
Se puede ejecutar con una ruta local o con una ruta HDFS.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


COLUMN_NAMES = [
    "fecha_corte",
    "numero_servicio_anonimizado",
    "nombre_servicio_anonimizado",
    "direccion_anonimizada",
    "numero_identidad_anonimizado",
    "tipo_documento_identificacion",
    "codigo_empresa",
    "nombre_empresa",
    "codigo_unidad_negocio",
    "nombre_unidad_negocio",
    "periodo_facturado",
    "tarifa",
    "departamento",
    "provincia",
    "distrito",
    "ubigeo",
    "tipo_documento_facturacion",
    "numero_documento_facturacion",
    "tipo_cartera",
    "fecha_vencimiento",
    "fecha_emision",
    "fecha_inicio_facturacion",
    "fecha_fin_facturacion",
    "importe_soles",
    "consumo_kwh",
]

UBIGEO_MAP_PATH = Path(__file__).resolve().parents[2] / "ubigeo_map.json"


def load_ubigeo_map() -> dict:
    if not UBIGEO_MAP_PATH.exists():
        return {}

    with UBIGEO_MAP_PATH.open("r", encoding="utf-8") as file:
        return json.load(file)


def build_spark(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Exploracion inicial del CSV de Puno con Spark")
    parser.add_argument(
        "--input",
        default="data.csv",
        help="Ruta del CSV. Puede ser local o HDFS. Ejemplo: hdfs://namenode:9000/user/hadoop/electropuno/electropuno.csv",
    )
    parser.add_argument(
        "--order-by",
        default="departamento,provincia,distrito,ubigeo",
        help="Columnas para ordenar la vista inicial, separadas por coma",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Cantidad de filas a mostrar en la exploracion inicial",
    )
    return parser.parse_args()


def read_raw_frame(spark: SparkSession, input_path: str):
    return (
        spark.read.option("header", "false")
        .option("sep", ";")
        .option("inferSchema", "false")
        .option("encoding", "UTF-8")
        .csv(input_path)
    )


def clean_frame(df):
    cleaned = df
    for column_name in cleaned.columns:
        cleaned = cleaned.withColumn(column_name, F.trim(F.col(column_name)))
    return cleaned


def enrich_with_ubigeo(df, ubigeo_payload: dict):
    districts = ubigeo_payload.get("districts", {}) if isinstance(ubigeo_payload, dict) else {}
    regions = ubigeo_payload.get("regions", {}) if isinstance(ubigeo_payload, dict) else {}

    region_lookup = {
        code: info.get("department", info.get("name", ""))
        for code, info in regions.items()
        if isinstance(info, dict)
    }
    province_lookup = {}
    district_lookup = {}
    for ubigeo_code, info in districts.items():
        if not isinstance(info, dict):
            continue
        province_code = info.get("province_code", ubigeo_code[:4])
        province_lookup[province_code] = {
            "department": info.get("department", ""),
            "province": info.get("province", ""),
        }
        district_lookup[ubigeo_code] = info

    def map_region(ubigeo_code: str) -> str:
        if not ubigeo_code:
            return ""
        return region_lookup.get(ubigeo_code[:2], ubigeo_code[:2])

    def map_province(ubigeo_code: str) -> str:
        if not ubigeo_code:
            return ""
        province_code = ubigeo_code[:4]
        province_info = province_lookup.get(province_code)
        if province_info:
            return province_info.get("province", province_code)
        return province_code

    def map_department(ubigeo_code: str) -> str:
        if not ubigeo_code:
            return ""
        return region_lookup.get(ubigeo_code[:2], ubigeo_code[:2])

    def map_district(ubigeo_code: str) -> str:
        if not ubigeo_code:
            return ""
        district_info = district_lookup.get(ubigeo_code)
        if district_info:
            return district_info.get("district", ubigeo_code)
        return ubigeo_code

    map_region_udf = F.udf(map_region)
    map_province_udf = F.udf(map_province)
    map_department_udf = F.udf(map_department)
    map_district_udf = F.udf(map_district)

    enriched = df.withColumn("ubigeo_code", F.col("ubigeo"))
    enriched = enriched.withColumn("department_name", map_department_udf(F.col("ubigeo_code")))
    enriched = enriched.withColumn("province_name", map_province_udf(F.col("ubigeo_code")))
    enriched = enriched.withColumn("district_name", map_district_udf(F.col("ubigeo_code")))
    enriched = enriched.withColumn("region_code", F.substring(F.col("ubigeo_code"), 1, 2))
    enriched = enriched.withColumn("province_code", F.substring(F.col("ubigeo_code"), 1, 4))
    enriched = enriched.withColumn("region_name", map_region_udf(F.col("ubigeo_code")))
    return enriched


def main() -> None:
    args = parse_args()
    spark = build_spark("Exploracion Puno")
    ubigeo_map = load_ubigeo_map()

    try:
        raw_df = read_raw_frame(spark, args.input)

        if len(raw_df.columns) != len(COLUMN_NAMES):
            raise ValueError(
                f"El CSV tiene {len(raw_df.columns)} columnas, pero se esperaban {len(COLUMN_NAMES)}."
            )

        df = raw_df.toDF(*COLUMN_NAMES)
        df = clean_frame(df)
        if ubigeo_map:
            df = enrich_with_ubigeo(df, ubigeo_map)

        print("\n=== ESQUEMA ===")
        df.printSchema()

        print("\n=== PRIMERAS FILAS ORDENADAS ===")
        order_columns = [F.col(column.strip()) for column in args.order_by.split(",") if column.strip() and column.strip() in df.columns]
        df.orderBy(*order_columns).show(args.limit, truncate=False)

        print("\n=== ESTADISTICAS GENERALES ===")
        print(f"Filas: {df.count()}")
        print(f"Columnas: {len(df.columns)}")

        print("\n=== NULOS POR COLUMNA ===")
        total_rows = df.count()
        null_counts = df.select([
            F.sum(F.when(F.col(column).isNull() | (F.col(column) == ""), 1).otherwise(0)).alias(column)
            for column in df.columns
        ])
        null_counts.show(truncate=False)

        print("\n=== PORCENTAJE DE NULOS ===")
        null_percentage_df = null_counts.select([
            (F.col(column) / F.lit(total_rows) * 100).alias(column) for column in null_counts.columns
        ])
        null_percentage_df.show(truncate=False)

        print("\n=== COLUMNAS MAS REPETIDAS ===")
        distinct_counts = []
        for column in [
            "nombre_empresa",
            "nombre_unidad_negocio",
            "departamento",
            "provincia",
            "distrito",
            "ubigeo",
            "tarifa",
            "tipo_cartera",
            "tipo_documento_facturacion",
        ]:
            if column in df.columns:
                distinct_value_count = df.select(column).distinct().count()
                repeated_value_count = total_rows - distinct_value_count
                distinct_counts.append((column, distinct_value_count, repeated_value_count))

        if distinct_counts:
            distinct_summary = spark.createDataFrame(
                distinct_counts,
                ["columna", "valores_distintos", "valores_repetidos"]
            ).orderBy(F.desc("valores_repetidos"))
            distinct_summary.show(truncate=False)

        print("\n=== FILAS DUPLICADAS COMPLETAS ===")
        duplicate_rows = df.groupBy(df.columns).count().filter(F.col("count") > 1).orderBy(F.desc("count"))
        duplicate_rows.show(args.limit, truncate=False)

        print("\n=== VALORES DISTINTOS DE CATEGORIAS CLAVE ===")
        for column in ["department_name", "region_name", "province_name", "district_name", "nombre_empresa", "nombre_unidad_negocio", "tarifa", "departamento", "provincia", "distrito", "ubigeo"]:
            if column in df.columns:
                print(f"- {column}: {df.select(column).distinct().count()}")

        print("\n=== MUESTRA PARA VALIDACION ===")
        selected_columns = [
            "fecha_corte",
            "nombre_empresa",
            "nombre_unidad_negocio",
            "tarifa",
            "departamento",
            "provincia",
            "distrito",
            "ubigeo",
            "region_name",
            "province_name",
            "district_name",
            "importe_soles",
            "consumo_kwh",
        ]
        selected_columns = [column for column in selected_columns if column in df.columns]
        df.select(*selected_columns).show(args.limit, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
