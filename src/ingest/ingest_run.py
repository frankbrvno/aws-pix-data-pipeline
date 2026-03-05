"""
ingest_run.py
-------------
Camada BRONZE (Ingestão).

O que este módulo faz?
- Lê configs (bucket, paths, dataset) nos arquivos YAML.
- Monta a URL do endpoint Olinda (BCB Pix Dados Abertos).
- Faz o download do JSON (payload) via HTTP.
- Salva:
  1) RAW -> S3 (json com payload inteiro)
  2) PARQUET -> S3 (somente payload["value"] como tabela)

Por que salvar RAW + Parquet?
- RAW preserva o dado exatamente como veio (auditoria / reprocessamento).
- Parquet é otimizado para leitura e transformação nas próximas camadas.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

import boto3
import pandas as pd

from src.core.partitioning import build_partition
from src.core.config import load_app_config, load_datasets_config
from src.core.logger import get_logger
from src.core.olinda import fetch_json
from src.core.s3_io import put_json


logger = get_logger(__name__)

# cliente S3 (usado apenas para upload do parquet)
S3 = boto3.client("s3")


def build_url(base: str, endpoint: str, param_name: str, database: str, top: int) -> str:
    """
    Monta a URL OData no padrão do Swagger do Olinda.

    Exemplo final:
    .../TransacoesPixPorMunicipio(DataBase=@DataBase)?%40DataBase='2026-03'&%24format=json&%24top=50

    - base: base url do serviço olinda
    - endpoint: nome do endpoint (com parâmetros entre parênteses)
    - param_name: nome do parâmetro (ex: DataBase)
    - database: valor do parâmetro (ex: '2026-03')
    - top: limite de linhas ($top)
    """
    return (
        f"{base}/{endpoint}"
        f"?%40{param_name}='{database}'"
        f"&%24format=json"
        f"&%24top={top}"
    )


def run_ingest(dataset_id: str, database: str | None = None, top: int | None = None) -> None:
    """
    Executa ingestão para um dataset_id configurado no YAML.

    - dataset_id: chave do dataset no configs/datasets.yaml (ex: pix_municipio)
    - database: mês alvo YYYY-MM (se None, pega defaults do app.yaml)
    - top: limite de linhas (se None, pega defaults do app.yaml)

    Saídas (S3):
    - bronze/<dataset_raw>/ano_mes=YYYYMM/*.json
    - bronze/<dataset_parquet>/ano_mes=YYYYMM/*.parquet
    """
    # 1) carregar configs gerais e do dataset
    app = load_app_config()
    datasets = load_datasets_config()

    if dataset_id not in datasets:
        raise SystemExit(f"Dataset '{dataset_id}' não existe em configs/datasets.yaml")

    ds = datasets[dataset_id]

    # 2) valores fixos do projeto
    bucket = app["aws"]["bucket"]
    bronze_root = app["paths"]["bronze"]

    # base_url pode vir do dataset, mas se não tiver usa padrão do serviço Pix
    base = ds.get("base_url") or "https://olinda.bcb.gov.br/olinda/servico/Pix_DadosAbertos/versao/v1/odata"

    # 3) defaults (caso o usuário não passe na CLI)
    database = database or app["defaults"]["database"]
    top = int(top or app["defaults"]["top"])

    endpoint = ds["endpoint"]
    param_name = ds["param_name"]

    # nomes do dataset no S3
    bronze_parquet = ds["bronze_parquet"]
    bronze_raw = ds["bronze_raw"]

   # particionamento no S3 (month ou day, dependendo do dataset)
    part_col, part_value = build_partition(ds, database)
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # 4) montar URL e chamar API
    url = build_url(base, endpoint, param_name, database, top)
    logger.info(f"INGEST [{dataset_id}] database={database} top={top}")
    logger.info(f"URL: {url}")

    payload = fetch_json(url)

    # na Olinda, dados tabulares vêm normalmente em payload["value"]
    rows = payload.get("value", []) or []
    logger.info(f"Rows retornadas: {len(rows)}")

    # 5) salvar RAW (payload inteiro, em JSON)
    raw_key = (
    f"{bronze_root}/{bronze_raw}/{part_col}={part_value}/"
    f"{bronze_raw}_{part_value}_{run_ts}.json"
    )
    raw_uri = put_json(bucket, raw_key, payload)
    logger.info(f"RAW salvo: {raw_uri}")

    # 6) salvar PARQUET (somente rows)
    df = pd.DataFrame(rows)
    if df.empty:
        logger.warning("Sem dados. Parquet não gerado.")
        return

    # metadados úteis para rastreabilidade
    df["ingestion_ts_utc"] = datetime.now(timezone.utc).isoformat()
    df["source_database"] = database

    parquet_key = (
    f"{bronze_root}/{bronze_parquet}/{part_col}={part_value}/"
    f"{bronze_parquet}_{part_value}_{run_ts}.parquet"
    )
    local = f"/tmp/{os.path.basename(parquet_key)}"

    # escreve local e sobe pro S3
    df.to_parquet(local, index=False)
    S3.upload_file(local, bucket, parquet_key)
    os.remove(local)

    logger.info(f"PARQUET salvo: s3://{bucket}/{parquet_key}")