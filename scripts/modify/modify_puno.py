"""Modificacion y limpieza inicial del CSV de consumo electrico de Puno.

Este script toma el esquema semantico definido en Explore y aplica limpieza,
conversion de tipos y algunas variables derivadas utiles para la siguiente fase
analitica.
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
    parser = argparse.ArgumentParser(description="Modificacion y limpieza del CSV de Puno con Spark")
    parser.add_argument(
        "--input",
        default="hdfs://namenode:9000/user/hadoop/electropuno/electropuno.csv",
        help="Ruta del CSV de entrada, local o HDFS",
    )
    parser.add_argument(
        "--output",
        default="hdfs://namenode:9000/user/hadoop/electropuno/modified_puno",
        help="Ruta de salida en HDFS o local",
    )
    parser.add_argument(
        "--format",
        default="parquet",
        choices=["parquet", "csv"],
        help="Formato de salida",
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


def clean_text_columns(df):
    cleaned = df
    for column_name in cleaned.columns:
        cleaned = cleaned.withColumn(column_name, F.trim(F.col(column_name)))
    return cleaned


def enrich_ubigeo_columns(df, ubigeo_payload: dict):
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

    def map_district(ubigeo_code: str) -> str:
        if not ubigeo_code:
            return ""
        district_info = district_lookup.get(ubigeo_code)
        if district_info:
            return district_info.get("district", ubigeo_code)
        return ubigeo_code

    region_udf = F.udf(map_region)
    province_udf = F.udf(map_province)
    district_udf = F.udf(map_district)

    enriched = df.withColumn("region_name", region_udf(F.col("ubigeo")))
    enriched = enriched.withColumn("province_name", province_udf(F.col("ubigeo")))
    enriched = enriched.withColumn("district_name", district_udf(F.col("ubigeo")))
    enriched = enriched.withColumn("region_code", F.substring(F.col("ubigeo"), 1, 2))
    enriched = enriched.withColumn("province_code", F.substring(F.col("ubigeo"), 1, 4))
    return enriched


def add_casts(df):
    return (
        df.withColumn("fecha_corte", F.to_date(F.col("fecha_corte"), "yyyyMMdd"))
        .withColumn("fecha_vencimiento", F.to_date(F.col("fecha_vencimiento"), "yyyyMMdd"))
        .withColumn("fecha_emision", F.to_date(F.col("fecha_emision"), "yyyyMMdd"))
        .withColumn("fecha_inicio_facturacion", F.to_date(F.col("fecha_inicio_facturacion"), "yyyyMMdd"))
        .withColumn("fecha_fin_facturacion", F.to_date(F.col("fecha_fin_facturacion"), "yyyyMMdd"))
        .withColumn("periodo_facturado", F.col("periodo_facturado").cast("string"))
        .withColumn("importe_soles", F.regexp_replace(F.col("importe_soles"), ",", ".").cast("double"))
        .withColumn("consumo_kwh", F.regexp_replace(F.col("consumo_kwh"), ",", ".").cast("double"))
    )


def add_derived_columns(df):
    return (
        df.withColumn("anio_corte", F.year(F.col("fecha_corte")))
        .withColumn("mes_corte", F.month(F.col("fecha_corte")))
        .withColumn("anio_facturacion", F.substring(F.col("periodo_facturado"), 1, 4).cast("int"))
        .withColumn("mes_facturacion", F.substring(F.col("periodo_facturado"), 5, 2).cast("int"))
        .withColumn(
            "categoria_consumo",
            F.when(F.col("consumo_kwh") >= 150, F.lit("alto"))
             .when(F.col("consumo_kwh") >= 50, F.lit("medio"))
             .otherwise(F.lit("bajo"))
        )
    )


def main() -> None:
    args = parse_args()
    spark = build_spark("Modify Puno")
    ubigeo_map = load_ubigeo_map()

    try:
        raw_df = read_raw_frame(spark, args.input)

        if len(raw_df.columns) != len(COLUMN_NAMES):
            raise ValueError(
                f"El CSV tiene {len(raw_df.columns)} columnas, pero se esperaban {len(COLUMN_NAMES)}."
            )

        df = raw_df.toDF(*COLUMN_NAMES)
        df = clean_text_columns(df)
        if ubigeo_map:
            df = enrich_ubigeo_columns(df, ubigeo_map)
        df = add_casts(df)
        df = add_derived_columns(df)

        print("\n=== ESQUEMA LIMPIO ===")
        df.printSchema()

        print("\n=== MUESTRA MODIFICADA ===")
        df.select(
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
            "importe_soles",
            "consumo_kwh",
            "categoria_consumo",
        ).show(10, truncate=False)

        writer = df.write.mode("overwrite")
        if args.format == "csv":
            writer = writer.option("header", "true").option("delimiter", ";")
        writer.format(args.format).save(args.output)

        print(f"\nSalida guardada en: {args.output}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
