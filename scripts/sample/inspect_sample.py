"""Inspeccion local de una muestra del CSV antes de usar Spark.

Objetivo:
- detectar separador,
- contar columnas,
- revisar una muestra de filas,
- y marcar posibles problemas de estructura.
"""

from __future__ import annotations

import argparse
import csv
from collections import Counter
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspeccion local de muestra del CSV")
    parser.add_argument(
        "--input",
        default="data.csv",
        help="Ruta del CSV local",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=25,
        help="Cantidad de filas a revisar",
    )
    parser.add_argument(
        "--has-header",
        action="store_true",
        help="Indica si el CSV tiene cabecera",
    )
    return parser.parse_args()


def detect_delimiter(lines: list[str]) -> str:
    candidates = [";", ",", "\t", "|"]
    scores = {delimiter: 0 for delimiter in candidates}
    for line in lines:
        for delimiter in candidates:
            scores[delimiter] += line.count(delimiter)
    return max(scores, key=scores.get)


def read_sample(path: Path, sample_size: int) -> tuple[str, list[list[str]]]:
    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as file:
        preview_lines = [line for _, line in zip(range(5), file) if line.strip()]

    if not preview_lines:
        raise ValueError("El archivo esta vacio o no tiene lineas validas para inspeccionar.")

    delimiter = detect_delimiter(preview_lines)

    rows: list[list[str]] = []
    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as file:
        reader = csv.reader(file, delimiter=delimiter)
        for row in reader:
            if not row:
                continue
            rows.append(row)
            if len(rows) >= sample_size:
                break

    return delimiter, rows


def main() -> None:
    args = parse_args()
    path = Path(args.input)

    if not path.exists():
        raise FileNotFoundError(f"No existe el archivo: {path}")

    delimiter, rows = read_sample(path, args.sample_size)

    print(f"Archivo: {path}")
    print(f"Separador detectado: {repr(delimiter)}")
    print(f"Filas muestreadas: {len(rows)}")

    field_counts = Counter(len(row) for row in rows)
    print(f"Distribucion de columnas por fila: {dict(field_counts)}")

    if args.has_header and rows:
        header = rows[0]
        data_rows = rows[1:]
        print("Cabecera detectada:")
        print(header)
    else:
        header = [f"campo_{index:02d}" for index in range(1, max(len(row) for row in rows) + 1)]
        data_rows = rows
        print("No se asumio cabecera. Columnas sugeridas:")
        print(header)

    print("\nPrimeras filas:")
    for index, row in enumerate(data_rows[:5], start=1):
        print(f"{index}: {row}")

    if rows:
        expected_fields = max(len(row) for row in rows)
        inconsistent_rows = [i + 1 for i, row in enumerate(rows) if len(row) != expected_fields]
        if inconsistent_rows:
            print(f"\nFilas con cantidad de columnas distinta al maximo ({expected_fields}): {inconsistent_rows}")
        else:
            print(f"\nTodas las filas muestreadas tienen {expected_fields} columnas.")

    print("\nSugerencia tecnica:")
    print("- Si la estructura es consistente, pasa el archivo a Spark para el Explore formal.")
    print("- Si encuentras campos inutiles o textos cualitativos repetitivos, descartar o agrupar antes del modelado.")


if __name__ == "__main__":
    main()
