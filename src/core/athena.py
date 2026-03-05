"""
athena.py
---------
Helpers para executar SQL no Amazon Athena via boto3.

Usado para automatizar registro de partições (ALTER TABLE ADD PARTITION...).
"""

from __future__ import annotations

import time
import boto3

from src.core.logger import get_logger

logger = get_logger(__name__)


def run_athena_query(
    sql: str,
    database: str,
    output_location: str,
    workgroup: str = "primary",
    poll_seconds: float = 1.0,
    timeout_seconds: int = 60,
) -> str:
    """
    Executa uma query no Athena e espera finalizar.

    Retorna: QueryExecutionId
    """
    client = boto3.client("athena")

    resp = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_location},
        WorkGroup=workgroup,
    )
    qid = resp["QueryExecutionId"]
    logger.info(f"Athena start: {qid}")

    start = time.time()
    while True:
        q = client.get_query_execution(QueryExecutionId=qid)
        state = q["QueryExecution"]["Status"]["State"]

        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break

        if time.time() - start > timeout_seconds:
            raise TimeoutError(f"Athena timeout ({timeout_seconds}s). QueryExecutionId={qid}")

        time.sleep(poll_seconds)

    if state != "SUCCEEDED":
        reason = q["QueryExecution"]["Status"].get("StateChangeReason", "")
        raise RuntimeError(f"Athena query {state}. Reason={reason}. SQL={sql}")

    logger.info("Athena ok")
    return qid