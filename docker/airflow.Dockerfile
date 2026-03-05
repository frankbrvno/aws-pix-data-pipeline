FROM apache/airflow:2.9.3

USER root

# (Opcional) dependências do sistema que às vezes ajudam com wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

COPY docker/requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt