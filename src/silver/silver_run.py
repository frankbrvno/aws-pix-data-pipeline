"""
silver_run.py
-------------
Camada SILVER (Transformação).

O que este módulo faz?
- Lê configs do projeto + regras do dataset (YAML)
- Busca o parquet mais recente do BRONZE (por partição)
- Aplica transformações (tipagem, nulos, normalização etc.)
- Salva um parquet "idempotente" no prefixo SILVER:
    1 partição = 1 arquivo (overwrite)

Por que pegar "o arquivo mais recente" do bronze?
- Porque o ingest grava com timestamp no nome
- Então o último é o da última execução (mais simples pro MVP)

Extra (opcional):
- Registra a partição no Athena automaticamente (ALTER TABLE ADD PARTITION)
  se existir no YAML:
    athena:
      silver_table: <nome_da_tabela_no_athena>
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd

from src.core.config import load_app_config, load_datasets_config
from src.core.logger import get_logger
from src.core.partitioning import build_partition
from src.core.s3_io import latest_key, download, upload, delete_prefix
from src.silver.pipelines import apply_pipeline_from_rules

# opcional: só usa se tiver tabela configurada no YAML
from src.core.athena_partitions import add_partition_if_not_exists

logger = get_logger(__name__)


def run_silver(dataset_id: str, database: str | None = None) -> None:
    # 1) carregar configs
    app = load_app_config()
    datasets = load_datasets_config()

    if dataset_id not in datasets:
        raise SystemExit(f"Dataset '{dataset_id}' não existe em configs/datasets.yaml")

    ds = datasets[dataset_id]
    rules = ds.get("silver", {})

    bucket = app["aws"]["bucket"]
    bronze_root = app["paths"]["bronze"]
    silver_root = app["paths"]["silver"]

    # 2) resolver partição
    database = database or app["defaults"]["database"]
    part_key, part_value = build_partition(ds, database)

    bronze_dataset = ds["bronze_parquet"]  # nome do dataset no bronze
    silver_dataset = ds.get("silver_dataset") or bronze_dataset  # nome no silver (default = mesmo)

    # 3) encontrar o parquet mais recente no bronze (na partição)
    bronze_prefix = f"{bronze_root}/{bronze_dataset}/{part_key}={part_value}/"
    bronze_key = latest_key(bucket, bronze_prefix)
    logger.info(f"Bronze latest: s3://{bucket}/{bronze_key}")

    # 4) baixar e ler parquet bronze
    local_in = "/tmp/bronze_input.parquet"
    download(bucket, bronze_key, local_in)
    df = pd.read_parquet(local_in)
    os.remove(local_in)

    logger.info(f"Linhas bronze: {len(df)} | Colunas: {len(df.columns)}")

    # 5) aplicar pipeline silver
    df2 = apply_pipeline_from_rules(df, rules)

    # debug básico (caso exista)
    for c in ["municipio_ibge", "estado_ibge"]:
        if c in df2.columns:
            logger.info(f"dtype {c}: {df2[c].dtype} | nulos={int(df2[c].isna().sum())}")

    # 6) salvar SILVER de forma idempotente: 1 partição = 1 arquivo
    silver_prefix = f"{silver_root}/{silver_dataset}/{part_key}={part_value}/"
    out_key = f"{silver_prefix}{silver_dataset}.parquet"  # <-- FIXO (overwrite)

    # limpa tudo que existir nessa partição (remove duplicidade)
    deleted = delete_prefix(bucket, silver_prefix)
    if deleted:
        logger.info(f"🧹 Removidos {deleted} arquivos antigos de s3://{bucket}/{silver_prefix}")

    # escreve local e sobe
    local_out = "/tmp/silver_output.parquet"
    df2.to_parquet(local_out, index=False)
    uri = upload(bucket, out_key, local_out)
    os.remove(local_out)

    logger.info(f"✅ Silver salvo: {uri}")

        # 7) (opcional) registrar partição no Athena automaticamente
    ath = app.get("athena") or {}
    ds_ath = ds.get("athena") or {}
    silver_table = ds_ath.get("silver_table")

    if silver_table:
        try:
            if not ath.get("database") or not ath.get("output_location"):
                logger.warning("Athena config ausente no app.yaml. Pulando registro de partição.")
                return

            full_table = f"{ath['database']}.{silver_table}"
            s3_location = f"s3://{bucket}/{silver_prefix}"

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