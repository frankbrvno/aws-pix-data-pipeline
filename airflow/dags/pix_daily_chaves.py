from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

# DAG diário: roda ingest -> silver -> gold para pix_chaves
with DAG(
    dag_id="pix_daily_chaves",
    start_date=datetime(2026, 3, 1),
    schedule="@daily",
    catchup=False,
    tags=["pix", "daily", "chaves"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_pix_chaves",
        bash_command=(
            "cd /opt/airflow/pipeline && "
            "python -m src.cli ingest pix_chaves --database {{ ds }} --top 200"
        ),
    )

    silver = BashOperator(
        task_id="silver_pix_chaves",
        bash_command=(
            "cd /opt/airflow/pipeline && "
            "python -m src.cli silver pix_chaves --database {{ ds }}"
        ),
    )

    gold = BashOperator(
        task_id="gold_chaves_tipo_dia",
        bash_command=(
            "cd /opt/airflow/pipeline && "
            "python -m src.cli gold gold_chaves_tipo_dia --database {{ ds }}"
        ),
    )

    ingest >> silver >> gold