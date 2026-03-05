# AWS Pix Data Engineering Pipeline

Pipeline de engenharia de dados construído em Python utilizando dados abertos do **Banco Central do Brasil (Pix)**.

O projeto implementa um **Data Lake em camadas (Bronze / Silver / Gold)** com ingestão via API, armazenamento em **AWS S3**, consultas analíticas via **Amazon Athena** e **orquestração com Apache Airflow**.

Este projeto simula uma arquitetura moderna de **Data Engineering em cloud**, seguindo boas práticas de ingestão, transformação e modelagem de dados.

---

# Arquitetura

Banco Central API (Olinda)  
↓  
Python Ingestion  
↓  
S3 Bronze (dados brutos)  
↓  
Transformação (Pandas)  
↓  
S3 Silver (dados tratados)  
↓  
Agregações  
↓  
S3 Gold (dados analíticos)  
↓  
Amazon Athena  
↓  
SQL Analytics / BI  

A orquestração do pipeline é realizada utilizando **Apache Airflow**, executado em containers **Docker**.

---

# Tecnologias utilizadas

- Python  
- AWS S3 (Data Lake Storage)  
- Amazon Athena (Query Engine)  
- Apache Airflow (Orquestração de pipelines)  
- Docker / Docker Compose  
- Pandas  
- Parquet  
- YAML Configurations  
- REST APIs  
- Git / GitHub

---

# Datasets utilizados

Dados públicos disponíveis via **API Olinda do Banco Central**:

- Estatísticas de Transações Pix
- Transações Pix por Município
- Estoque de Chaves Pix
- Estatísticas de Fraudes Pix

Documentação oficial:

https://olinda.bcb.gov.br/olinda/servico/Pix_DadosAbertos/versao/v1/aplicacao#!/resources
