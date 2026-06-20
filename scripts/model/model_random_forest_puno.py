"""Benchmark con Random Forest Regressor para consumo electrico de Puno."""

from __future__ import annotations

import argparse

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import SparkSession, functions as F


DEFAULT_INPUT = "hdfs://namenode:9000/user/hadoop/electropuno/model_dataset_puno"
DEFAULT_OUTPUT = "hdfs://namenode:9000/user/hadoop/electropuno/model_outputs/random_forest_predictions"

FEATURE_COLUMNS = [
    "anio_facturacion",
    "mes_facturacion",
    "registros",
    "importe_total_soles",
    "importe_promedio_soles",
]
CATEGORICAL_COLUMN = "ubigeo"
LABEL_COLUMN = "consumo_total_kwh"


def build_spark(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Random Forest para consumo electrico de Puno")
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Ruta del dataset de modelado")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Ruta de salida para predicciones")
    parser.add_argument("--seed", type=int, default=42, help="Semilla aleatoria")
    parser.add_argument("--num-trees", type=int, default=80, help="Numero de arboles")
    parser.add_argument("--max-depth", type=int, default=10, help="Profundidad maxima")
    parser.add_argument("--train-ratio", type=float, default=0.8, help="Proporcion de entrenamiento")
    return parser.parse_args()


def read_frame(spark: SparkSession, input_path: str):
    return spark.read.parquet(input_path)


def prepare_frame(df):
    required = FEATURE_COLUMNS + [CATEGORICAL_COLUMN, LABEL_COLUMN]
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas para Random Forest: {missing}")

    prepared = df
    for column in FEATURE_COLUMNS + [LABEL_COLUMN]:
        prepared = prepared.withColumn(column, F.col(column).cast("double"))

    prepared = prepared.filter(
        F.col(LABEL_COLUMN).isNotNull() & F.col(CATEGORICAL_COLUMN).isNotNull()
    )
    for column in FEATURE_COLUMNS:
        prepared = prepared.filter(F.col(column).isNotNull())
    return prepared


def build_pipeline(seed: int, num_trees: int, max_depth: int) -> Pipeline:
    indexer = StringIndexer(
        inputCol=CATEGORICAL_COLUMN,
        outputCol=f"{CATEGORICAL_COLUMN}_index",
        handleInvalid="keep",
    )
    encoder = OneHotEncoder(
        inputCols=[f"{CATEGORICAL_COLUMN}_index"],
        outputCols=[f"{CATEGORICAL_COLUMN}_ohe"],
    )
    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS + [f"{CATEGORICAL_COLUMN}_ohe"],
        outputCol="features",
    )
    regressor = RandomForestRegressor(
        featuresCol="features",
        labelCol=LABEL_COLUMN,
        predictionCol="prediction",
        seed=seed,
        numTrees=num_trees,
        maxDepth=max_depth,
    )
    return Pipeline(stages=[indexer, encoder, assembler, regressor])


def main() -> None:
    args = parse_args()
    spark = build_spark("Model Random Forest Puno")

    try:
        df = prepare_frame(read_frame(spark, args.input))
        train_df, test_df = df.randomSplit([args.train_ratio, 1 - args.train_ratio], seed=args.seed)

        pipeline = build_pipeline(args.seed, args.num_trees, args.max_depth)
        model = pipeline.fit(train_df)
        predictions = model.transform(test_df).cache()

        evaluator_rmse = RegressionEvaluator(labelCol=LABEL_COLUMN, predictionCol="prediction", metricName="rmse")
        evaluator_mae = RegressionEvaluator(labelCol=LABEL_COLUMN, predictionCol="prediction", metricName="mae")
        evaluator_r2 = RegressionEvaluator(labelCol=LABEL_COLUMN, predictionCol="prediction", metricName="r2")

        rmse = evaluator_rmse.evaluate(predictions)
        mae = evaluator_mae.evaluate(predictions)
        r2 = evaluator_r2.evaluate(predictions)

        print("\n=== RANDOM FOREST ===")
        print(f"RMSE: {rmse:.4f}")
        print(f"MAE: {mae:.4f}")
        print(f"R2: {r2:.4f}")

        rf_model = model.stages[-1]
        print("\n=== IMPORTANCIA DE VARIABLES ===")
        print(rf_model.featureImportances)

        predictions.select(
            CATEGORICAL_COLUMN,
            "anio_facturacion",
            "mes_facturacion",
            LABEL_COLUMN,
            "prediction",
            (F.col(LABEL_COLUMN) - F.col("prediction")).alias("error"),
            F.abs(F.col(LABEL_COLUMN) - F.col("prediction")).alias("absolute_error"),
        ).write.mode("overwrite").parquet(args.output)

        print(f"\nPredicciones guardadas en: {args.output}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()