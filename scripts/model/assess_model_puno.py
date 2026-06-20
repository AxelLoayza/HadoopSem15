"""Comparacion final de los modelos de Puno.

Lee las predicciones guardadas por regresion y Random Forest, y los clusters de
KMeans, para reportar metricas comparables y una conclusion tecnica.
"""

from __future__ import annotations

import argparse

from pyspark.ml.evaluation import ClusteringEvaluator, RegressionEvaluator
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


DEFAULT_REGRESSION_INPUT = "hdfs://namenode:9000/user/hadoop/electropuno/model_outputs/regression_predictions"
DEFAULT_RF_INPUT = "hdfs://namenode:9000/user/hadoop/electropuno/model_outputs/random_forest_predictions"
DEFAULT_KMEANS_INPUT = "hdfs://namenode:9000/user/hadoop/electropuno/model_outputs/kmeans_predictions"


def build_spark(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Assess final de los modelos de Puno")
    parser.add_argument("--regression-input", default=DEFAULT_REGRESSION_INPUT, help="Predicciones de regresion")
    parser.add_argument("--rf-input", default=DEFAULT_RF_INPUT, help="Predicciones de random forest")
    parser.add_argument("--kmeans-input", default=DEFAULT_KMEANS_INPUT, help="Predicciones de kmeans")
    return parser.parse_args()


def safe_read_parquet(spark: SparkSession, input_path: str):
    try:
        return spark.read.parquet(input_path)
    except Exception:
        return None


def regression_metrics(df):
    evaluator_rmse = RegressionEvaluator(labelCol="consumo_total_kwh", predictionCol="prediction", metricName="rmse")
    evaluator_mae = RegressionEvaluator(labelCol="consumo_total_kwh", predictionCol="prediction", metricName="mae")
    evaluator_r2 = RegressionEvaluator(labelCol="consumo_total_kwh", predictionCol="prediction", metricName="r2")
    return {
        "rmse": evaluator_rmse.evaluate(df),
        "mae": evaluator_mae.evaluate(df),
        "r2": evaluator_r2.evaluate(df),
        "rows": df.count(),
    }


def kmeans_metrics(df):
    evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="prediction", metricName="silhouette")
    return {
        "silhouette": evaluator.evaluate(df),
        "clusters": df.select("prediction").distinct().count(),
        "rows": df.count(),
    }


def main() -> None:
    args = parse_args()
    spark = build_spark("Assess Model Puno")

    try:
        regression_df = safe_read_parquet(spark, args.regression_input)
        rf_df = safe_read_parquet(spark, args.rf_input)
        kmeans_df = safe_read_parquet(spark, args.kmeans_input)

        print("\n=== ASSESS FINAL ===")

        report_rows = []
        if regression_df is not None and regression_df.columns:
            regression_result = regression_metrics(regression_df)
            report_rows.append(("regresion_lineal", regression_result["rmse"], regression_result["mae"], regression_result["r2"], None, regression_result["rows"]))
        if rf_df is not None and rf_df.columns:
            rf_result = regression_metrics(rf_df)
            report_rows.append(("random_forest", rf_result["rmse"], rf_result["mae"], rf_result["r2"], None, rf_result["rows"]))
        if kmeans_df is not None and kmeans_df.columns:
            kmeans_result = kmeans_metrics(kmeans_df)
            report_rows.append(("kmeans", None, None, None, kmeans_result["silhouette"], kmeans_result["rows"]))

        if report_rows:
            report_schema = StructType(
                [
                    StructField("modelo", StringType(), False),
                    StructField("rmse", DoubleType(), True),
                    StructField("mae", DoubleType(), True),
                    StructField("r2", DoubleType(), True),
                    StructField("silhouette", DoubleType(), True),
                    StructField("filas", LongType(), False),
                ]
            )
            report_df = spark.createDataFrame(report_rows, report_schema)
            report_df.show(truncate=False)
        else:
            print("No se encontraron salidas de modelos para evaluar.")

        print("\nConclusion sugerida:")
        print("- Elegir el modelo con menor error y mayor estabilidad en R2 o silhouette, segun el objetivo.")
        print("- Si la regresion domina en RMSE y MAE, debe ser la ruta principal.")
        print("- KMeans solo debe usarse como segmentacion exploratoria.")

        if regression_df is not None and "error" in regression_df.columns:
            print("\n=== RESIDUOS REGRESION ===")
            regression_df.select(
                F.avg(F.abs(F.col("error"))).alias("error_abs_promedio"),
                F.max(F.abs(F.col("error"))).alias("error_abs_maximo"),
            ).show(truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()