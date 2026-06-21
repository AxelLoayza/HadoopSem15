#!/usr/bin/env python3
"""Reducer para agregacion de consumo electrico por ubigeo y mes.

Lee pares clave-valor ordenados por Hadoop Streaming (salida del mapper).
Por cada clave ubigeo|mes acumula:
  - suma de consumo_kwh
  - suma de importe_soles
  - conteo de registros

Salida (TSV, una linea por zona y mes):
  ubigeo  mes  total_consumo_kwh  total_importe_soles  registros

Esta salida es la que Spark consume en la sub-etapa E.2 de Explore.
Permite ver distribucion de consumo por zona geografica y rango de fechas.
"""

import sys


def main():
    current_key = None
    total_consumo = 0.0
    total_importe = 0.0
    total_registros = 0

    for line in sys.stdin:
        line = line.rstrip("\n").strip()
        if not line:
            continue

        parts = line.split("\t", 1)
        if len(parts) != 2:
            continue

        key = parts[0]
        value_part = parts[1]

        value_fields = value_part.split("|")
        if len(value_fields) != 3:
            continue

        try:
            consumo = float(value_fields[0])
            importe = float(value_fields[1])
            count = int(value_fields[2])
        except ValueError:
            continue

        if key == current_key:
            total_consumo += consumo
            total_importe += importe
            total_registros += count
        else:
            if current_key is not None:
                ubigeo, mes = current_key.split("|", 1)
                print("{}\t{}\t{:.4f}\t{:.4f}\t{}".format(ubigeo, mes, total_consumo, total_importe, total_registros))
            current_key = key
            total_consumo = consumo
            total_importe = importe
            total_registros = count

    if current_key is not None:
        ubigeo, mes = current_key.split("|", 1)
        print("{}\t{}\t{:.4f}\t{:.4f}\t{}".format(ubigeo, mes, total_consumo, total_importe, total_registros))


if __name__ == "__main__":
    main()
