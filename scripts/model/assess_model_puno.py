"""Comparacion final de los modelos de Puno.

Lee las predicciones guardadas por regresion y Random Forest, y los clusters de
KMeans, para reportar metricas comparables y una conclusion tecnica.

Ademas exporta CSVs a /opt/work/output/ para que el script local
scripts/assess/plot_results.py genere los graficos de comparativa.
"""

from __future__ import annotations

import argparse
import csv
import os

from pyspark.ml.evaluation import ClusteringEvaluator, RegressionEvaluator
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


DEFAULT_REGRESSION_INPUT = "hdfs://namenode:9000/user/hadoop/electropuno/model_outputs/regression_predictions"
DEFAULT_RF_INPUT = "hdfs://namenode:9000/user/hadoop/electropuno/model_outputs/random_forest_predictions"
DEFAULT_KMEANS_INPUT = "hdfs://namenode:9000/user/hadoop/electropuno/model_outputs/kmeans_predictions"
DEFAULT_LOCAL_OUTPUT = "/opt/work/output"


def build_spark(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Assess final de los modelos de Puno")
    parser.add_argument("--regression-input", default=DEFAULT_REGRESSION_INPUT)
    parser.add_argument("--rf-input", default=DEFAULT_RF_INPUT)
    parser.add_argument("--kmeans-input", default=DEFAULT_KMEANS_INPUT)
    parser.add_argument("--local-output", default=DEFAULT_LOCAL_OUTPUT,
                        help="Carpeta local montada donde se exportan los CSVs para graficar")
    return parser.parse_args()


def safe_read_parquet(spark: SparkSession, path: str):
    try:
        return spark.read.parquet(path)
    except Exception:
        return None


def regression_metrics(df):
    eval_rmse = RegressionEvaluator(labelCol="consumo_total_kwh", predictionCol="prediction", metricName="rmse")
    eval_mae  = RegressionEvaluator(labelCol="consumo_total_kwh", predictionCol="prediction", metricName="mae")
    eval_r2   = RegressionEvaluator(labelCol="consumo_total_kwh", predictionCol="prediction", metricName="r2")
    return {
        "rmse": eval_rmse.evaluate(df),
        "mae":  eval_mae.evaluate(df),
        "r2":   eval_r2.evaluate(df),
        "rows": df.count(),
    }


def kmeans_metrics(df):
    evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="prediction", metricName="silhouette")
    return {
        "silhouette": evaluator.evaluate(df),
        "clusters":   df.select("prediction").distinct().count(),
        "rows":        df.count(),
    }


def write_csv(path: str, header: list, rows: list) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    print(f"  -> exportado: {path}")


def export_predictions(df, path: str, model_name: str) -> None:
    """Exporta predicciones de regresion/RF a CSV local."""
    rows = df.select(
        "ubigeo", "anio_facturacion", "mes_facturacion",
        "consumo_total_kwh", "prediction", "error", "absolute_error",
    ).collect()
    write_csv(
        path,
        ["ubigeo", "anio_facturacion", "mes_facturacion",
         "consumo_total_kwh", "prediction", "error", "absolute_error"],
        [[r.ubigeo, r.anio_facturacion, r.mes_facturacion,
          r.consumo_total_kwh, r.prediction, r.error, r.absolute_error]
         for r in rows],
    )


def export_kmeans(df, path: str) -> None:
    """Exporta asignaciones de cluster a CSV local."""
    rows = df.select(
        "ubigeo", "anio_facturacion", "mes_facturacion",
        "consumo_total_kwh", "consumo_promedio_kwh", "importe_total_soles",
        "registros", "prediction",
    ).collect()
    write_csv(
        path,
        ["ubigeo", "anio_facturacion", "mes_facturacion",
         "consumo_total_kwh", "consumo_promedio_kwh", "importe_total_soles",
         "registros", "cluster"],
        [[r.ubigeo, r.anio_facturacion, r.mes_facturacion,
          r.consumo_total_kwh, r.consumo_promedio_kwh, r.importe_total_soles,
          r.registros, r.prediction]
         for r in rows],
    )


def print_comparative_table(report_rows: list) -> None:
    """Imprime la tabla comparativa con interpretacion."""
    header = f"{'Modelo':<22} {'RMSE':>12} {'MAE':>12} {'R2':>8} {'Silhouette':>12} {'Filas':>6}"
    print("\n" + "=" * 76)
    print("  TABLA COMPARATIVA FINAL DE MODELOS")
    print("=" * 76)
    print(header)
    print("-" * 76)
    for row in report_rows:
        modelo, rmse, mae, r2, sil, filas = row
        rmse_s = f"{rmse:>12.2f}" if rmse is not None else f"{'—':>12}"
        mae_s  = f"{mae:>12.2f}"  if mae  is not None else f"{'—':>12}"
        r2_s   = f"{r2:>8.4f}"   if r2   is not None else f"{'—':>8}"
        sil_s  = f"{sil:>12.4f}" if sil  is not None else f"{'—':>12}"
        print(f"{modelo:<22} {rmse_s} {mae_s} {r2_s} {sil_s} {filas:>6}")
    print("=" * 76)

    # Interpretacion automatica
    regression_rows = [(r[1], r[3], r[0]) for r in report_rows if r[1] is not None]
    if len(regression_rows) >= 2:
        best = min(regression_rows, key=lambda x: x[0])
        print(f"\n  Mejor RMSE: {best[2]}  ({best[0]:.2f})")
        best_r2 = max(regression_rows, key=lambda x: x[1])
        print(f"  Mejor R2:   {best_r2[2]}  ({best_r2[1]:.4f})")
        if best[2] == best_r2[2]:
            print(f"\n  CONCLUSION: '{best[2]}' domina en RMSE y R2. Usar como modelo principal.")
        else:
            print(f"\n  CONCLUSION: '{best[2]}' tiene menor error absoluto; '{best_r2[2]}' explica mejor la varianza.")
            print("  Evaluar segun si el objetivo es minimizar error puntual o capturar tendencia.")

    kmeans_row = next((r for r in report_rows if r[1] is None), None)
    if kmeans_row:
        sil = kmeans_row[4]
        if sil is not None:
            nivel = "buena" if sil > 0.5 else ("aceptable" if sil > 0.3 else "debil")
            print(f"\n  KMeans silhouette {sil:.4f} -> separacion {nivel}.")
            print("  KMeans es exploratorio: segmenta zonas, no predice consumo.")
    print("=" * 76)


def main() -> None:
    args = parse_args()
    spark = build_spark("Assess Model Puno")
    spark.sparkContext.setLogLevel("WARN")

    try:
        regression_df = safe_read_parquet(spark, args.regression_input)
        rf_df         = safe_read_parquet(spark, args.rf_input)
        kmeans_df     = safe_read_parquet(spark, args.kmeans_input)

        report_rows = []

        if regression_df is not None and regression_df.columns:
            m = regression_metrics(regression_df)
            report_rows.append(("regresion_lineal", m["rmse"], m["mae"], m["r2"], None, m["rows"]))

        if rf_df is not None and rf_df.columns:
            m = regression_metrics(rf_df)
            report_rows.append(("random_forest", m["rmse"], m["mae"], m["r2"], None, m["rows"]))

        if kmeans_df is not None and kmeans_df.columns:
            m = kmeans_metrics(kmeans_df)
            report_rows.append(("kmeans", None, None, None, m["silhouette"], m["rows"]))

        if report_rows:
            print_comparative_table(report_rows)
        else:
            print("No se encontraron salidas de modelos para evaluar.")

        # --------------------------------------------------------------- #
        # Residuos de regresion
        # --------------------------------------------------------------- #
        if regression_df is not None and "error" in regression_df.columns:
            print("\n=== RESIDUOS REGRESION ===")
            regression_df.select(
                F.avg(F.abs(F.col("error"))).alias("error_abs_promedio"),
                F.max(F.abs(F.col("error"))).alias("error_abs_maximo"),
                F.stddev(F.col("error")).alias("desviacion_error"),
            ).show(truncate=False)

        # --------------------------------------------------------------- #
        # Exportar CSVs al directorio local montado
        # --------------------------------------------------------------- #
        print("\n=== EXPORTANDO CSVs para visualizacion local ===")
        out = args.local_output

        # Metricas resumen
        metrics_path = os.path.join(out, "metrics_summary.csv")
        write_csv(
            metrics_path,
            ["modelo", "rmse", "mae", "r2", "silhouette", "filas"],
            [[r[0], r[1] or "", r[2] or "", r[3] or "", r[4] or "", r[5]]
             for r in report_rows],
        )

        if regression_df is not None and regression_df.columns:
            export_predictions(regression_df, os.path.join(out, "regression_predictions.csv"), "regresion")

        if rf_df is not None and rf_df.columns:
            export_predictions(rf_df, os.path.join(out, "rf_predictions.csv"), "random_forest")

        if kmeans_df is not None and kmeans_df.columns:
            export_kmeans(kmeans_df, os.path.join(out, "kmeans_predictions.csv"))

        print(f"\nCSVs disponibles en '{out}'.")
        print("Ejecutar localmente: python scripts/assess/plot_results.py")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()