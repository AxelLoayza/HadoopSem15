#!/usr/bin/env python3
"""Mapper para agregacion de consumo electrico por ubigeo y mes.

Lee cada linea del CSV crudo (separador ;, sin cabecera, 25 columnas).
Emite clave ubigeo|mes y valores consumo_kwh|importe_soles para que el
reducer pueda sumar y contar por zona y periodo.

Posicion en SEMMA: sub-etapa E.1 dentro de Explore.
Proposito: reducir el volumen que Spark debe procesar y dar visibilidad
por zona geografica y rango de fechas sin cargar los 25 campos completos.
"""

import sys


# Indices de columnas en el CSV crudo (0-indexado, sin cabecera)
IDX_PERIODO_FACTURADO = 10   # formato YYYYMM
IDX_UBIGEO = 15              # codigo de 6 digitos
IDX_IMPORTE_SOLES = 23
IDX_CONSUMO_KWH = 24


def parse_line(line: str):
    """Devuelve (ubigeo, mes, consumo_kwh, importe_soles) o None si la linea es invalida."""
    line = line.rstrip("\n").strip()
    if not line:
        return None

    parts = line.split(";")
    if len(parts) < 25:
        return None

    ubigeo = parts[IDX_UBIGEO].strip()
    mes = parts[IDX_PERIODO_FACTURADO].strip()

    try:
        consumo = float(parts[IDX_CONSUMO_KWH].strip().replace(",", "."))
    except ValueError:
        consumo = 0.0

    try:
        importe = float(parts[IDX_IMPORTE_SOLES].strip().replace(",", "."))
    except ValueError:
        importe = 0.0

    if not ubigeo or not mes:
        return None

    return ubigeo, mes, consumo, importe


def main():
    for line in sys.stdin:
        result = parse_line(line)
        if result is None:
            continue
        ubigeo, mes, consumo, importe = result
        # Clave: ubigeo|mes  Valor: consumo_kwh|importe_soles|1
        print(f"{ubigeo}|{mes}\t{consumo:.4f}|{importe:.4f}|1")


if __name__ == "__main__":
    main()
