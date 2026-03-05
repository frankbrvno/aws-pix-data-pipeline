"""
silver_run.py
-------------
Camada SILVER (Transformação).

O que este módulo faz?
- Lê configs do projeto + regras do dataset (YAML)
- Busca o parquet mais recente do BRONZE (por mês/partição)
- Aplica transformações (tipagem, nulos, normalização etc.)
- Salva um novo parquet no prefixo SILVER

Por que pegar "o arquivo mais recente" do bronze?
- Porque seu ingest grava com timestamp no nome
- Então o último é o da última execução (mais simples pro MVP)
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd

from src.core.partitioning import build_partition
from src.core.config import load_app_config, load_datasets_config
from src.core.logger import get_logger
from src.core.s3_io import latest_key, download, upload
from src.silver.pipelines import apply_pipeline_from_rules

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

    database = database or app["defaults"]["database"]
    part_col, part_value = build_partition(ds, database)
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    bronze_dataset = ds["bronze_parquet"]          # nome do dataset no bronze
    silver_dataset = ds.get("silver_dataset") or bronze_dataset  # nome no silver (default = mesmo)

    # 2) encontrar o parquet mais recente do bronze (na partição do mês)
    bronze_prefix = f"{bronze_root}/{bronze_dataset}/{part_col}={part_value}/"
    bronze_key = latest_key(bucket, bronze_prefix)
    logger.info(f"Bronze latest: s3://{bucket}/{bronze_key}")

    # 3) baixar e ler parquet
    local_in = "/tmp/bronze_input.parquet"
    download(bucket, bronze_key, local_in)
    df = pd.read_parquet(local_in)
    os.remove(local_in)

    logger.info(f"Linhas bronze: {len(df)} | Colunas: {len(df.columns)}")

    # 4) aplicar pipeline silver
    df2 = apply_pipeline_from_rules(df, rules)

    # debug básico de tipos (principalmente IBGE)
    for c in ["municipio_ibge", "estado_ibge"]:
        if c in df2.columns:
            logger.info(f"dtype {c}: {df2[c].dtype} | nulos={int(df2[c].isna().sum())}")

    # 5) salvar no S3 (silver)
    silver_prefix = f"{silver_root}/{silver_dataset}/{part_col}={part_value}/"
    out_key = f"{silver_prefix}{silver_dataset}_{part_value}_{run_ts}.parquet"

    local_out = "/tmp/silver_output.parquet"
    df2.to_parquet(local_out, index=False)

    uri = upload(bucket, out_key, local_out)
    os.remove(local_out)

    logger.info(f"✅ Silver salvo: {uri}")