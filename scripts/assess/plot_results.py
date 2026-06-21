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
# 4. Top N zonas por consumo total (todos los meses sumados)
# --------------------------------------------------------------------------- #
def plot_top_zonas(df: pd.DataFrame, top_n: int = 15) -> None:
    if df is None:
        return
    df = df.dropna(subset=["ubigeo", "consumo_total_kwh"])
    por_zona = (
        df.groupby("ubigeo")["consumo_total_kwh"]
        .sum()
        .nlargest(top_n)
        .reset_index()
        .rename(columns={"consumo_total_kwh": "consumo_sum"})
    )
    if por_zona.empty:
        return

    palette = [plt.cm.Blues_r(v) for v in [i / (top_n + 2) for i in range(1, top_n + 1)]]
    fig, ax = plt.subplots(figsize=(9, 6))
    bars = ax.barh(por_zona["ubigeo"], por_zona["consumo_sum"], color=palette, edgecolor="white")
    ax.set_xlabel("Consumo total (kWh)")
    ax.set_title("Top {} zonas por consumo — todos los meses".format(top_n), fontweight="bold")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: "{:,.0f}".format(x)))
    ax.invert_yaxis()
    for bar, val in zip(bars, por_zona["consumo_sum"]):
        ax.text(bar.get_width() * 1.005, bar.get_y() + bar.get_height() / 2,
                "{:,.0f}".format(val), va="center", fontsize=7)
    plt.tight_layout()
    save(fig, "top_zonas_consumo.png")

    print("\n  Top {} zonas por consumo total:".format(top_n))
    print(por_zona.to_string(index=False))


# --------------------------------------------------------------------------- #
# 5. Error proxy: error porcentual vs nivel de consumo real
# --------------------------------------------------------------------------- #
def plot_error_proxy(df: pd.DataFrame, model_name: str, prefix: str) -> None:
    """Scatter |error|/real*100 vs consumo real.
    Permite ver si el modelo pierde precision en niveles altos o bajos."""
    if df is None:
        return
    df = df.dropna(subset=["consumo_total_kwh", "absolute_error"]).copy()
    df = df[df["consumo_total_kwh"] > 0]
    if df.empty:
        return
    df["pct_error"] = df["absolute_error"] / df["consumo_total_kwh"] * 100
    mediana = df["pct_error"].median()
    media   = df["pct_error"].mean()
    p90     = df["pct_error"].quantile(0.9)

    fig, ax = plt.subplots(figsize=(7, 5))
    ax.scatter(df["consumo_total_kwh"], df["pct_error"],
               alpha=0.6, color="#E87040", edgecolors="white", s=45)
    ax.axhline(mediana, color="navy", linestyle="--", linewidth=1.2,
               label="Mediana: {:.1f}%".format(mediana))
    ax.axhline(media, color="#5BA85B", linestyle=":", linewidth=1.2,
               label="Media: {:.1f}%".format(media))
    # Limitar eje Y al percentil 95 para evitar que outliers extremos compriman el grafico
    y_cap = df["pct_error"].quantile(0.95) * 1.3
    ax.set_ylim(0, max(y_cap, mediana * 3))
    n_fuera = int((df["pct_error"] > y_cap).sum())
    if n_fuera > 0:
        ax.text(0.98, 0.96, "{} punto(s) fuera del eje (consumo muy bajo)".format(n_fuera),
                transform=ax.transAxes, ha="right", va="top", fontsize=7, color="gray")
    ax.set_xlabel("Consumo real (kWh)")
    ax.set_ylabel("|error| / real × 100  (%)")
    ax.set_title("{} — Proxy de error por nivel de consumo".format(model_name), fontweight="bold")
    ax.legend(fontsize=8)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: "{:,.0f}".format(x)))
    plt.tight_layout()
    save(fig, "{}_error_proxy.png".format(prefix))

    print("\n  {} — error proxy:".format(model_name))
    print("    Mediana error%: {:.2f}%".format(mediana))
    print("    Media error%:   {:.2f}%".format(media))
    print("    p90 error%:     {:.2f}%".format(p90))


# --------------------------------------------------------------------------- #
# 6. Tabla visual: zonas con mayor error de prediccion
# --------------------------------------------------------------------------- #
def plot_tabla_peores_predicciones(df: pd.DataFrame, model_name: str, prefix: str,
                                   top_n: int = 12) -> None:
    """Imagen tipo tabla con las N zonas donde el modelo comete mas error absoluto."""
    if df is None:
        return
    df = df.dropna(subset=["ubigeo", "consumo_total_kwh", "prediction", "absolute_error"]).copy()
    if df.empty:
        return
    df["pct_error"] = (df["absolute_error"] / df["consumo_total_kwh"].clip(lower=1) * 100).round(1)
    peores = df.nlargest(top_n, "absolute_error")[
        ["ubigeo", "anio_facturacion", "mes_facturacion",
         "consumo_total_kwh", "prediction", "absolute_error", "pct_error"]
    ].copy()
    peores["consumo_total_kwh"] = peores["consumo_total_kwh"].map("{:,.0f}".format)
    peores["prediction"]        = peores["prediction"].map("{:,.0f}".format)
    peores["absolute_error"]    = peores["absolute_error"].map("{:,.0f}".format)
    peores["pct_error"]         = peores["pct_error"].map("{:.1f}%".format)

    col_labels = ["Ubigeo", "Ano", "Mes", "Real (kWh)", "Predicho (kWh)", "Error abs.", "Error %"]
    fig, ax = plt.subplots(figsize=(13, 0.5 + top_n * 0.42))
    ax.axis("off")
    table = ax.table(
        cellText=peores.values.tolist(),
        colLabels=col_labels,
        loc="center",
        cellLoc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(8.5)
    table.scale(1, 1.6)
    for j in range(len(col_labels)):
        table[0, j].set_facecolor("#2C5F9B")
        table[0, j].set_text_props(color="white", fontweight="bold")
    for i in range(1, top_n + 1):
        for j in range(len(col_labels)):
            table[i, j].set_facecolor("#EEF2F9" if i % 2 == 0 else "white")
    ax.set_title("{} — Zonas con mayor error de prediccion (Top {})".format(model_name, top_n),
                 fontweight="bold", fontsize=11, pad=16)
    plt.tight_layout()
    save(fig, "{}_tabla_peores.png".format(prefix))
    print("  {} — tabla de peores predicciones guardada.".format(model_name))


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

    # --- nuevos graficos ---
    plot_top_zonas(rf if rf is not None else regression)
    plot_error_proxy(regression, "Regresion Lineal", "regresion")
    plot_error_proxy(rf,         "Random Forest",    "rf")
    plot_tabla_peores_predicciones(regression, "Regresion Lineal", "regresion")
    plot_tabla_peores_predicciones(rf,         "Random Forest",    "rf")

    print(f"\nGraficos generados en {PLOTS_DIR}/")
    print("Archivos producidos:")
    for f in sorted(PLOTS_DIR.iterdir()):
        print(f"  {f.name}")


if __name__ == "__main__":
    main()
