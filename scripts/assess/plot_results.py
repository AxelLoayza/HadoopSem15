#!/usr/bin/env python3
"""Visualizacion local de resultados del Assess.

Requiere que assess_model_puno.py haya exportado los CSVs a ./output/.
Genera graficos PNG en ./output/plots/.

Instalar dependencias:
  pip install pandas matplotlib

Uso:
  python scripts/assess/plot_results.py
"""

import os
import sys
from pathlib import Path

try:
    import matplotlib
    matplotlib.use("Agg")          # no necesita ventana grafica
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
    import pandas as pd
except ImportError as e:
    print(f"Error: falta dependencia — {e}")
    print("Instalar con:  pip install pandas matplotlib")
    sys.exit(1)


OUTPUT_DIR = Path(__file__).resolve().parents[2] / "output"
PLOTS_DIR  = OUTPUT_DIR / "plots"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def load(filename: str) -> pd.DataFrame | None:
    path = OUTPUT_DIR / filename
    if not path.exists():
        print(f"  [omitido] no encontrado: {path}")
        return None
    df = pd.read_csv(path)
    print(f"  cargado: {path}  ({len(df)} filas)")
    return df


def save(fig, name: str) -> None:
    path = PLOTS_DIR / name
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  guardado: {path}")


# --------------------------------------------------------------------------- #
# 1. Tabla de metricas y comparativa de barras
# --------------------------------------------------------------------------- #
def plot_metrics_comparison(metrics: pd.DataFrame) -> None:
    # Filtrar solo modelos de regresion (tienen RMSE)
    reg = metrics[metrics["rmse"].notna()].copy()
    if reg.empty:
        print("  [omitido] sin metricas de regresion disponibles")
        return

    # --- Tabla en consola ---
    print("\n" + "=" * 60)
    print("  COMPARATIVA FINAL DE MODELOS")
    print("=" * 60)
    print(metrics.to_string(index=False))
    print("=" * 60)

    fig, axes = plt.subplots(1, 3, figsize=(12, 4))
    fig.suptitle("Comparativa de modelos — ElectroPuno", fontsize=13, fontweight="bold")

    colores = ["#3B7DD8", "#E87040"]

    for ax, (metric, label, fmt) in zip(axes, [
        ("rmse", "RMSE (menor es mejor)", "{:.0f}"),
        ("mae",  "MAE  (menor es mejor)",  "{:.0f}"),
        ("r2",   "R²   (mayor es mejor)",  "{:.4f}"),
    ]):
        bars = ax.bar(reg["modelo"], reg[metric], color=colores[:len(reg)], edgecolor="white")
        ax.set_title(label, fontsize=10)
        ax.set_ylabel(metric.upper())
        ax.tick_params(axis="x", rotation=15, labelsize=8)
        for bar, val in zip(bars, reg[metric]):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() * 1.01,
                    fmt.format(val), ha="center", va="bottom", fontsize=8)

    plt.tight_layout()
    save(fig, "comparativa_modelos.png")


# --------------------------------------------------------------------------- #
# 2. Predicho vs real + residuos para un modelo de regresion
# --------------------------------------------------------------------------- #
def plot_regression_analysis(df: pd.DataFrame, model_name: str, prefix: str) -> None:
    if df is None:
        return
    df = df.dropna(subset=["consumo_total_kwh", "prediction", "error"])
    if df.empty:
        return

    # Scatter: predicho vs real
    fig, ax = plt.subplots(figsize=(6, 5))
    ax.scatter(df["consumo_total_kwh"], df["prediction"], alpha=0.7, color="#3B7DD8", edgecolors="white", s=60)
    mn = min(df["consumo_total_kwh"].min(), df["prediction"].min())
    mx = max(df["consumo_total_kwh"].max(), df["prediction"].max())
    ax.plot([mn, mx], [mn, mx], "r--", linewidth=1, label="Prediccion perfecta")
    ax.set_xlabel("Consumo real (kWh)")
    ax.set_ylabel("Consumo predicho (kWh)")
    ax.set_title(f"{model_name} — Predicho vs Real")
    ax.legend(fontsize=8)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    plt.tight_layout()
    save(fig, f"{prefix}_predicho_vs_real.png")

    # Histograma de residuos
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.hist(df["error"], bins=15, color="#5BA85B", edgecolor="white", alpha=0.85)
    ax.axvline(0, color="red", linewidth=1.2, linestyle="--", label="Error = 0")
    ax.set_xlabel("Error (real - predicho)")
    ax.set_ylabel("Frecuencia")
    ax.set_title(f"{model_name} — Distribucion de residuos")
    ax.legend(fontsize=8)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    plt.tight_layout()
    save(fig, f"{prefix}_residuos.png")


# --------------------------------------------------------------------------- #
# 3. KMeans: distribucion de clusters y consumo promedio por cluster
# --------------------------------------------------------------------------- #
def plot_kmeans_analysis(df: pd.DataFrame) -> None:
    if df is None:
        return
    df = df.dropna(subset=["cluster"])
    if df.empty:
        return

    cluster_stats = df.groupby("cluster").agg(
        zonas=("ubigeo", "count"),
        consumo_promedio=("consumo_total_kwh", "mean"),
        importe_promedio=("importe_total_soles", "mean"),
    ).reset_index()
    cluster_stats["cluster"] = cluster_stats["cluster"].astype(int)
    cluster_stats = cluster_stats.sort_values("cluster")
    labels = [f"Cluster {c}" for c in cluster_stats["cluster"]]

    fig, axes = plt.subplots(1, 2, figsize=(11, 4))
    fig.suptitle("KMeans — Segmentacion de zonas por consumo", fontsize=12, fontweight="bold")

    # Cantidad de zonas por cluster
    colores = plt.cm.Set2.colors
    axes[0].bar(labels, cluster_stats["zonas"], color=colores, edgecolor="white")
    axes[0].set_title("Zonas por cluster")
    axes[0].set_ylabel("Cantidad de registros")
    for bar, val in zip(axes[0].patches, cluster_stats["zonas"]):
        axes[0].text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                     str(val), ha="center", va="bottom", fontsize=9)

    # Consumo promedio por cluster
    axes[1].bar(labels, cluster_stats["consumo_promedio"], color=colores, edgecolor="white")
    axes[1].set_title("Consumo promedio por cluster (kWh)")
    axes[1].set_ylabel("Consumo promedio (kWh)")
    axes[1].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    for bar, val in zip(axes[1].patches, cluster_stats["consumo_promedio"]):
        axes[1].text(bar.get_x() + bar.get_width() / 2, bar.get_height() * 1.01,
                     f"{val:,.0f}", ha="center", va="bottom", fontsize=8)

    plt.tight_layout()
    save(fig, "kmeans_clusters.png")

    # Tabla resumen en consola
    print("\n  KMeans — resumen por cluster:")
    print(cluster_stats.to_string(index=False))


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    print(f"\n=== plot_results.py ===")
    print(f"Leyendo CSVs desde: {OUTPUT_DIR}")
    print(f"Guardando graficos en: {PLOTS_DIR}\n")

    metrics   = load("metrics_summary.csv")
    regression = load("regression_predictions.csv")
    rf         = load("rf_predictions.csv")
    kmeans     = load("kmeans_predictions.csv")

    if metrics is not None:
        plot_metrics_comparison(metrics)

    plot_regression_analysis(regression, "Regresion Lineal", "regresion")
    plot_regression_analysis(rf,         "Random Forest",    "rf")
    plot_kmeans_analysis(kmeans)

    print(f"\nGraficos generados en {PLOTS_DIR}/")
    print("Archivos producidos:")
    for f in sorted(PLOTS_DIR.iterdir()):
        print(f"  {f.name}")


if __name__ == "__main__":
    main()
