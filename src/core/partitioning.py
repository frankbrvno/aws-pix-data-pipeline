"""
partitioning.py
---------------
Responsável por definir a partição (coluna e valor) usada no S3.

Por quê?
- Alguns datasets são mensais: 'YYYY-MM' -> ano_mes=YYYYMM
- Outros são diários: 'YYYY-MM-DD' -> data=YYYYMMDD
"""

from __future__ import annotations

from datetime import datetime


def build_partition(ds: dict, database: str) -> tuple[str, str]:
    """
    Retorna (partition_col, partition_value) com base no dataset (ds) e no valor informado.

    Config no datasets.yaml (opcional):
      partition:
        type: "month" | "day"
        column: "ano_mes" | "data"
        format: "%Y%m%d"   # só usado para day
    """
    part = ds.get("partition") or {}
    part_type = (part.get("type") or "month").lower()

    if part_type == "day":
        part_col = part.get("column") or "data"
        fmt = part.get("format") or "%Y%m%d"
        dt = datetime.strptime(database, "%Y-%m-%d")
        return part_col, dt.strftime(fmt)

    # default: month
    part_col = part.get("column") or "ano_mes"
    dt = datetime.strptime(database, "%Y-%m")
    return part_col, dt.strftime("%Y%m")