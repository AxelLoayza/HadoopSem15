"""Segmentacion exploratoria con KMeans para consumo electrico de Puno."""

from __future__ import annotations

import argparse

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession, functions as F


DEFAULT_INPUT = "hdfs://namenode:9000/user/hadoop/electropuno/model_dataset_puno"
DEFAULT_OUTPUT = "hdfs://namenode:9000/user/hadoop/electropuno/model_outputs/kmeans_predictions"

NUMERIC_COLUMNS = [
    "anio_facturacion",
    "mes_facturacion",
    "registros",
    "importe_total_soles",
    "importe_promedio_soles",
    "consumo_promedio_kwh",
]


def build_spark(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="KMeans exploratorio para consumo electrico de Puno")
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Ruta del dataset de modelado")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Ruta de salida para clusters")
    parser.add_argument("--k", type=int, default=4, help="Numero de clusters")
    parser.add_argument("--seed", type=int, default=42, help="Semilla aleatoria")
    return parser.parse_args()


def read_frame(spark: SparkSession, input_path: str):
    return spark.read.parquet(input_path)


def prepare_frame(df):
    required = NUMERIC_COLUMNS + ["ubigeo"]
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas para KMeans: {missing}")

    prepared = df
    for column in NUMERIC_COLUMNS:
        prepared = prepared.withColumn(column, F.col(column).cast("double"))

    for column in NUMERIC_COLUMNS:
        prepared = prepared.filter(F.col(column).isNotNull())
    return prepared


def build_pipeline(k: int, seed: int) -> Pipeline:
    assembler = VectorAssembler(inputCols=NUMERIC_COLUMNS, outputCol="raw_features")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)
    kmeans = KMeans(featuresCol="features", predictionCol="prediction", k=k, seed=seed)
    return Pipeline(stages=[assembler, scaler, kmeans])


def main() -> None:
    args = parse_args()
    spark = build_spark("Model KMeans Puno")

    try:
        df = prepare_frame(read_frame(spark, args.input))
        pipeline = build_pipeline(args.k, args.seed)
        model = pipeline.fit(df)
        predictions = model.transform(df).cache()

        evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="prediction", metricName="silhouette")
        silhouette = evaluator.evaluate(predictions)

        print("\n=== KMEANS ===")
        print(f"k: {args.k}")
        print(f"Silhouette: {silhouette:.4f}")

        predictions.select(
            "ubigeo",
            *NUMERIC_COLUMNS,
            "features",
            "prediction",
        ).write.mode("overwrite").parquet(args.output)

        print(f"\nClusters guardados en: {args.output}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()