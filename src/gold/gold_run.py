from __future__ import annotations

import os
from datetime import datetime, timezone

import boto3
import pandas as pd

from src.core.config import load_app_config, load_datasets_config
from src.core.logger import get_logger
from src.core.partitioning import build_partition
from src.core.s3_io import latest_key, download, upload
from src.gold.aggregations import gold_pix_uf_mes, gold_fraudes_mes, gold_chaves_tipo_dia

logger = get_logger(__name__)
S3 = boto3.client("s3")


GOLD_JOBS = {
    "gold_pix_uf_mes": {
        "source_dataset_id": "pix_municipio",
        "source_silver_dataset": "transacoes_pix_por_municipio",
        "out_dataset": "gold_pix_uf_mes",
        "fn": gold_pix_uf_mes,
        "partition_type": "month",  # usa o mesmo database YYYY-MM
    },
    "gold_fraudes_mes": {
        "source_dataset_id": "pix_fraudes_med",
        "source_silver_dataset": "estatisticas_fraudes_pix",
        "out_dataset": "gold_fraudes_mes",
        "fn": gold_fraudes_mes,
        "partition_type": "month",
    },
    "gold_chaves_tipo_dia": {
        "source_dataset_id": "pix_chaves",
        "source_silver_dataset": "estoque_chaves_pix",
        "out_dataset": "gold_chaves_tipo_dia",
        "fn": gold_chaves_tipo_dia,
        "partition_type": "day",  # database YYYY-MM-DD
    },
}


def run_gold(job_id: str, database: str) -> None:
    app = load_app_config()
    datasets = load_datasets_config()

    if job_id not in GOLD_JOBS:
        raise SystemExit(f"Gold job '{job_id}' não existe. Opções: {list(GOLD_JOBS)}")

    job = GOLD_JOBS[job_id]
    source_dataset_id = job["source_dataset_id"]
    if source_dataset_id not in datasets:
        raise SystemExit(f"Dataset fonte '{source_dataset_id}' não existe em configs/datasets.yaml")

    # config base
    bucket = app["aws"]["bucket"]
    silver_root = app["paths"]["silver"]
    gold_root = app["paths"].get("gold", "gold")  # se não tiver no app.yaml, usa 'gold'

    # usa o particionamento do DATASET fonte (pix_municipio/pix_fraudes_med/pix_chaves)
    ds_cfg = datasets[source_dataset_id]
    part_col, part_value = build_partition(ds_cfg, database)

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    src_silver_dataset = job["source_silver_dataset"]
    out_dataset = job["out_dataset"]
    fn = job["fn"]

    # 1) encontra parquet mais recente no SILVER da partição
    silver_prefix = f"{silver_root}/{src_silver_dataset}/{part_col}={part_value}/"
    silver_key = latest_key(bucket, silver_prefix)
    logger.info(f"GOLD [{job_id}] | lendo silver: s3://{bucket}/{silver_key}")

    local_in = "/tmp/gold_input.parquet"
    download(bucket, silver_key, local_in)
    df = pd.read_parquet(local_in)
    os.remove(local_in)

    logger.info(f"linhas silver: {len(df)}")

    # 2) roda agregação
    out = fn(df)

    # 3) grava no GOLD (mantém mesma partição)
    gold_prefix = f"{gold_root}/{out_dataset}/{part_col}={part_value}/"
    out_key = f"{gold_prefix}{out_dataset}_{part_value}_{run_ts}.parquet"

    local_out = "/tmp/gold_output.parquet"
    out.to_parquet(local_out, index=False)
    uri = upload(bucket, out_key, local_out)
    os.remove(local_out)

    logger.info(f"✅ GOLD salvo: {uri}")