from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


# helper pra usar YYYY-MM no CLI a partir do ds (YYYY-MM-DD)
def month_from_ds(ds: str) -> str:
    return ds[:7]


with DAG(
    dag_id="pix_monthly_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@monthly",
    catchup=False,
    tags=["pix", "monthly"],
) as dag:

    # Nota: no BashOperator, fazemos {{ ds }}[:7] no próprio shell com cut
    # Alternativa simples: usar 'cut' pra pegar YYYY-MM.
    month = "$(echo {{ ds }} | cut -c1-7)"

    ingest_municipio = BashOperator(
        task_id="ingest_pix_municipio",
        bash_command=(
            "cd /opt/airflow/pipeline && "
            f"python -m src.cli ingest pix_municipio --database {month} --top 500"
        ),
    )

    silver_municipio = BashOperator(
        task_id="silver_pix_municipio",
        bash_command=(
            "cd /opt/airflow/pipeline && "
            f"python -m src.cli silver pix_municipio --database {month}"
        ),
    )

    gold_uf_mes = BashOperator(
        task_id="gold_pix_uf_mes",
        bash_command=(
            "cd /opt/airflow/pipeline && "
            f"python -m src.cli gold gold_pix_uf_mes --database {month}"
        ),
    )

    ingest_fraudes = BashOperator(
        task_id="ingest_pix_fraudes_med",
        bash_command=(
            "cd /opt/airflow/pipeline && "
            f"python -m src.cli ingest pix_fraudes_med --database {month} --top 200"
        ),
    )

    silver_fraudes = BashOperator(
        task_id="silver_pix_fraudes_med",
        bash_command=(
            "cd /opt/airflow/pipeline && "
            f"python -m src.cli silver pix_fraudes_med --database {month}"
        ),
    )

    gold_fraudes_mes = BashOperator(
        task_id="gold_fraudes_mes",
        bash_command=(
            "cd /opt/airflow/pipeline && "
            f"python -m src.cli gold gold_fraudes_mes --database {month}"
        ),
    )

    ingest_transacoes = BashOperator(
        task_id="ingest_estatisticas_transacoes",
        bash_command=(
            "cd /opt/airflow/pipeline && "
            f"python -m src.cli ingest pix_transacoes_estatisticas --database {month} --top 200"
        ),
    )

    silver_transacoes = BashOperator(
        task_id="silver_estatisticas_transacoes",
        bash_command=(
            "cd /opt/airflow/pipeline && "
            f"python -m src.cli silver pix_transacoes_estatisticas --database {month}"
        ),
    )

    # Dependências (exemplo):
    ingest_municipio >> silver_municipio >> gold_uf_mes
    ingest_fraudes >> silver_fraudes >> gold_fraudes_mes
    ingest_transacoes >> silver_transacoes