"""
partitioning.py
---------------
Responsável por definir a partição (nome e valor) usada no S3.

Por quê?
- Alguns datasets são mensais: 'YYYY-MM'     -> ano_mes=YYYYMM
- Outros são diários:  'YYYY-MM-DD' -> data_part=YYYYMMDD

Importante:
- O nome da partição no S3 NÃO precisa ser igual a uma coluna do dataframe.
  Ex: dataset tem coluna "data", mas queremos particionar como "data_part"
"""

from __future__ import annotations

from datetime import datetime


def build_partition(ds: dict, database: str) -> tuple[str, str]:
    """
    Retorna (partition_key, partition_value) com base no dataset (ds) e no valor informado.

    Config no datasets.yaml (opcional):
      partition:
        type: "month" | "day"
        key_name: "ano_mes" | "data_part"      # nome da partição no S3 (recomendado)
        column: "ano_mes" | "data"             # legado (se key_name não existir)
        format: "%Y%m%d"                       # só usado para day

    Exemplos:
    - month: database="2026-03"    -> ("ano_mes", "202603")
    - day:   database="2025-01-01" -> ("data_part", "20250101")
    """
    part = ds.get("partition") or {}
    part_type = (part.get("type") or "month").lower()

    # Nome da partição no S3:
    # - Preferimos key_name (novo)
    # - Se não existir, usamos column (legado)
    if part_type == "day":
        part_key = part.get("key_name") or part.get("column") or "data_part"
        fmt = part.get("format") or "%Y%m%d"
        dt = datetime.strptime(database, "%Y-%m-%d")
        return part_key, dt.strftime(fmt)

    # default: month
    part_key = part.get("key_name") or part.get("column") or "ano_mes"
    dt = datetime.strptime(database, "%Y-%m")
    return part_key, dt.strftime("%Y%m")