"""
athena_partitions.py
--------------------
Registra partições no Athena sem MSCK REPAIR (mais rápido e controlado).

Exemplo:
ALTER TABLE pix_dw.tabela ADD IF NOT EXISTS
PARTITION (ano_mes='202501') LOCATION 's3://.../ano_mes=202501/';
"""

from __future__ import annotations

from src.core.athena import run_athena_query
from src.core.logger import get_logger

logger = get_logger(__name__)


def add_partition_if_not_exists(
    database: str,
    workgroup: str,
    output_location: str,
    full_table: str,          # ex: pix_dw.transacoes_pix_por_municipio_silver
    part_key: str,            # ano_mes | data_part
    part_value: str,          # 202501 | 20250101
    s3_location: str,         # .../ano_mes=202501/ (termina com /)
) -> None:
    sql = (
        f"ALTER TABLE {full_table} "
        f"ADD IF NOT EXISTS PARTITION ({part_key}='{part_value}') "
        f"LOCATION '{s3_location}'"
    )
    logger.info(f"Registrando partição Athena: {full_table} {part_key}={part_value}")
    run_athena_query(
        sql=sql,
        database=database,
        output_location=output_location,
        workgroup=workgroup,
    )