"""
s3_io.py
--------
Funções utilitárias para interação com Amazon S3.

Centraliza:
- listagem de arquivos
- download
- upload
- gravação de JSON

Isso evita repetir boto3 em vários lugares do código.
"""

import os
from typing import List, Dict, Any

import boto3

from src.core.logger import get_logger

logger = get_logger(__name__)

# Cliente S3 reutilizado por todas funções
S3 = boto3.client("s3")


def list_keys(bucket: str, prefix: str) -> List[Dict[str, Any]]:
    """
    Lista arquivos em um prefixo do S3.
    """

    logger.info(f"Listando objetos em s3://{bucket}/{prefix}")

    resp = S3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    return resp.get("Contents", []) or []


def latest_key(bucket: str, prefix: str) -> str:
    """
    Retorna o arquivo mais recente de um prefixo.
    """

    files = list_keys(bucket, prefix)

    if not files:
        logger.error(f"Nenhum arquivo encontrado em s3://{bucket}/{prefix}")
        raise FileNotFoundError(prefix)

    latest = sorted(files, key=lambda x: x["LastModified"])[-1]["Key"]

    logger.info(f"Arquivo mais recente: {latest}")

    return latest


def download(bucket: str, key: str, local_path: str) -> str:
    """
    Baixa um arquivo do S3 para o disco local.
    """

    logger.info(f"Baixando s3://{bucket}/{key}")

    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    S3.download_file(bucket, key, local_path)

    return local_path


def upload(bucket: str, key: str, local_path: str) -> str:
    """
    Faz upload de um arquivo local para o S3.
    """

    logger.info(f"Enviando arquivo para s3://{bucket}/{key}")

    S3.upload_file(local_path, bucket, key)

    return f"s3://{bucket}/{key}"


def put_json(bucket: str, key: str, payload: dict) -> str:
    """
    Salva um JSON diretamente no S3.

    Usado principalmente para salvar dados RAW da API.
    """

    import json

    logger.info(f"Salvando JSON em s3://{bucket}/{key}")

    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    S3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )

    return f"s3://{bucket}/{key}"