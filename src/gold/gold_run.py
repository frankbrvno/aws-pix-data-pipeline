"""
gold_run.py
-----------
Camada GOLD (Agregações).

- Lê configs do projeto + datasets (YAML)
- Para um job_id, lê o dataset SILVER correspondente
- Calcula agregações (GOLD)
- Salva 1 arquivo por partição (overwrite)
- (opcional) registra partição no Athena
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd

from src.core.config import load_app_config, load_datasets_config
from src.core.logger import get_logger
from src.core.partitioning import build_partition
from src.core.s3_io import latest_key, download, upload, delete_prefix
from src.core.athena_partitions import add_partition_if_not_exists

from src.gold.jobs import (
    job_gold_pix_uf_mes,
    job_gold_fraudes_mes,
    job_gold_chaves_tipo_dia,
)

logger = get_logger(__name__)


def _save_gold_idempotent(
    bucket: str,
    gold_root: str,
    job_id: str,
    part_key: str,
    part_value: str,
    df: pd.DataFrame,
) -> tuple[str, str]:
    """
    Salva GOLD de forma idempotente:
      gold/<job_id>/<part_key>=<part_value>/<job_id>.parquet
    """
    gold_prefix = f"{gold_root}/{job_id}/{part_key}={part_value}/"
    out_key = f"{gold_prefix}{job_id}.parquet"  # <-- fixo

    # remove qualquer coisa antiga dessa partição
    delete_prefix(bucket, gold_prefix)

    local_out = "/tmp/gold_output.parquet"
    df.to_parquet(local_out, index=False)
    uri = upload(bucket, out_key, local_out)
    os.remove(local_out)

    return uri, gold_prefix


def _maybe_register_athena_partition(
    app: dict,
    full_table: str | None,
    part_key: str,
    part_value: str,
    s3_location: str,
) -> None:
    """
    Registra partição no Athena se houver config e permissão.
    Não quebra o pipeline se falhar (MVP-friendly).
    """
    if not full_table:
        return

    ath = app.get("athena") or {}
    if not ath.get("database") or not ath.get("output_location"):
        logger.warning("Athena config ausente no app.yaml. Pulando registro de partição.")
        return

    try:
        add_partition_if_not_exists(
            database=ath["database"],
            workgroup=ath.get("workgroup", "primary"),
            output_location=ath["output_location"],
            full_table=full_table,
            part_key=part_key,
            part_value=part_value,
            s3_location=s3_location,
        )
        logger.info(f"📌 Partição registrada no Athena: {full_table} {part_key}={part_value}")
    except Exception as e:
        logger.warning(f"Não foi possível registrar partição no Athena agora (ok no MVP). Motivo: {e}")


def run_gold(job_id: str, database: str) -> None:
    """
    job_id:
      - gold_pix_uf_mes
      - gold_fraudes_mes
      - gold_chaves_tipo_dia

    database:
      - jobs mensais: YYYY-MM
      - jobs diários: YYYY-MM-DD
    """
    app = load_app_config()
    datasets = load_datasets_config()

    bucket = app["aws"]["bucket"]
    silver_root = app["paths"]["silver"]
    gold_root = app["paths"]["gold"]

    logger.info(f"GOLD job_id={job_id} database={database}")

    # ---------- JOB 1: UF por mês (usa pix_municipio) ----------
    if job_id == "gold_pix_uf_mes":
        ds = datasets["pix_municipio"]
        part_key, part_value = build_partition(ds, database)

        silver_dataset = ds.get("silver_dataset") or ds["bronze_parquet"]
        silver_prefix = f"{silver_root}/{silver_dataset}/{part_key}={part_value}/"
        silver_key = latest_key(bucket, silver_prefix)

        local_in = "/tmp/silver_input.parquet"
        download(bucket, silver_key, local_in)
        df = pd.read_parquet(local_in)
        os.remove(local_in)

        df_gold = job_gold_pix_uf_mes(df)

        uri, gold_prefix = _save_gold_idempotent(bucket, gold_root, job_id, part_key, part_value, df_gold)
        logger.info(f"✅ GOLD salvo: {uri}")

        # se você tiver tabela no athena, configure aqui (ou no YAML depois)
        full_table = f"{(app.get('athena') or {}).get('database','pix_dw')}.gold_pix_uf_mes"
        _maybe_register_athena_partition(app, full_table, part_key, part_value, f"s3://{bucket}/{gold_prefix}")
        return

    # ---------- JOB 2: fraudes por mês (usa pix_fraudes_med) ----------
    if job_id == "gold_fraudes_mes":
        ds = datasets["pix_fraudes_med"]
        part_key, part_value = build_partition(ds, database)

        silver_dataset = ds.get("silver_dataset") or ds["bronze_parquet"]
        silver_prefix = f"{silver_root}/{silver_dataset}/{part_key}={part_value}/"
        silver_key = latest_key(bucket, silver_prefix)

        local_in = "/tmp/silver_input.parquet"
        download(bucket, silver_key, local_in)
        df = pd.read_parquet(local_in)
        os.remove(local_in)

        df_gold = job_gold_fraudes_mes(df)

        uri, gold_prefix = _save_gold_idempotent(bucket, gold_root, job_id, part_key, part_value, df_gold)
        logger.info(f"✅ GOLD salvo: {uri}")

        full_table = f"{(app.get('athena') or {}).get('database','pix_dw')}.gold_fraudes_mes"
        _maybe_register_athena_partition(app, full_table, part_key, part_value, f"s3://{bucket}/{gold_prefix}")
        return

    # ---------- JOB 3: chaves por tipo por dia (usa pix_chaves) ----------
    if job_id == "gold_chaves_tipo_dia":
        ds = datasets["pix_chaves"]
        part_key, part_value = build_partition(ds, database)  # aqui deve virar data_part=YYYYMMDD

        silver_dataset = ds.get("silver_dataset") or ds["bronze_parquet"]
        silver_prefix = f"{silver_root}/{silver_dataset}/{part_key}={part_value}/"
        silver_key = latest_key(bucket, silver_prefix)

        local_in = "/tmp/silver_input.parquet"
        download(bucket, silver_key, local_in)
        df = pd.read_parquet(local_in)
        os.remove(local_in)

        df_gold = job_gold_chaves_tipo_dia(df)

        uri, gold_prefix = _save_gold_idempotent(bucket, gold_root, job_id, part_key, part_value, df_gold)
        logger.info(f"✅ GOLD salvo: {uri}")

        full_table = f"{(app.get('athena') or {}).get('database','pix_dw')}.gold_chaves_tipo_dia"
        _maybe_register_athena_partition(app, full_table, part_key, part_value, f"s3://{bucket}/{gold_prefix}")
        return

    raise SystemExit(f"job_id inválido: {job_id}")