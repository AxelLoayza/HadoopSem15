"""Inspeccion local de una muestra del CSV antes de usar Spark.

Objetivo:
- detectar separador,
- contar columnas,
- revisar una muestra de filas,
- y marcar posibles problemas de estructura.

Nota sobre cabecera: el dataset oficial de Datos Abiertos Peru no incluye
cabecera en el CSV. El script asigna nombres de referencia segun el
catalogo oficial del dataset de consumo electrico.
"""

from __future__ import annotations

import argparse
import csv
from collections import Counter
from pathlib import Path


# Nombres de columnas segun el catalogo oficial del dataset abierto.
# El CSV fisico NO las incluye, pero se asignan en los scripts de Explore.
OFFICIAL_COLUMN_NAMES = [
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspeccion local de muestra del CSV")
    parser.add_argument(
        "--input",
        default=None,
        help="Ruta del CSV local. Si se omite, busca automaticamente archivos data_*.csv en el directorio actual.",
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
        help="Indica si el CSV tiene cabecera (por defecto: sin cabecera)",
    )
    return parser.parse_args()


def find_csv_file() -> Path:
    """Busca automaticamente un archivo data_*.csv en el directorio actual."""
    cwd = Path.cwd()
    candidates = sorted(cwd.glob("data_*.csv"))
    if candidates:
        return candidates[0]
    # Fallback: cualquier CSV en el directorio
    any_csv = sorted(cwd.glob("*.csv"))
    if any_csv:
        return any_csv[0]
    raise FileNotFoundError(
        "No se encontro ningun archivo CSV en el directorio actual.\n"
        "Especifica la ruta con:  python scripts/sample/inspect_sample.py --input <archivo>.csv"
    )


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

    if args.input:
        path = Path(args.input)
        if not path.exists():
            raise FileNotFoundError(f"No existe el archivo: {path}")
    else:
        path = find_csv_file()
        print(f"[auto-deteccion] usando: {path.name}")

    delimiter, rows = read_sample(path, args.sample_size)

    print(f"\nArchivo     : {path}")
    print(f"Separador   : {repr(delimiter)}")
    print(f"Filas leidas: {len(rows)}")

    field_counts = Counter(len(row) for row in rows)
    print(f"Columnas por fila: {dict(field_counts)}")

    if args.has_header and rows:
        header = rows[0]
        data_rows = rows[1:]
        print("\nCabecera detectada en el archivo:")
        for i, col in enumerate(header, 1):
            print(f"  {i:>2}. {col}")
    else:
        n_cols = max(len(row) for row in rows)
        data_rows = rows

        print("\nEl CSV no tiene cabecera (comportamiento esperado del dataset oficial).")
        print("Nombres de campo segun catalogo oficial:")
        print(f"{'#':>4}  {'Nombre oficial':<40}  {'Muestra (fila 1)'}")
        print("-" * 75)
        first_row = data_rows[0] if data_rows else []
        for i in range(n_cols):
            nombre = OFFICIAL_COLUMN_NAMES[i] if i < len(OFFICIAL_COLUMN_NAMES) else f"campo_{i+1:02d}"
            muestra = first_row[i][:35] if i < len(first_row) else ""
            print(f"  {i+1:>2}. {nombre:<40}  {muestra}")

    print("\nPrimeras 3 filas completas:")
    for index, row in enumerate(data_rows[:3], start=1):
        print(f"  Fila {index}: {row}")

    if rows:
        expected_fields = max(len(row) for row in rows)
        inconsistent = [i + 1 for i, row in enumerate(rows) if len(row) != expected_fields]
        if inconsistent:
            print(f"\nFILAS CON COLUMNAS DISTINTAS al maximo ({expected_fields}): {inconsistent}")
        else:
            print(f"\nTodas las filas muestreadas tienen {expected_fields} columnas. Estructura consistente.")

    print("\nSugerencia:")
    print("- Si la estructura es consistente y el separador es ';', el CSV esta listo para subir a HDFS.")
    print("- Sube el archivo con:  docker cp <archivo>.csv namenode:/tmp/<archivo>.csv")


if __name__ == "__main__":
    main()

