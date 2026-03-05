
# AWS Pix Data Engineering Pipeline

Pipeline de engenharia de dados construído em Python utilizando dados abertos do Banco Central do Brasil (Pix).

O projeto implementa um **Data Lake em camadas (Bronze / Silver)** com ingestão via API, armazenamento em **AWS S3** e consultas analíticas via **Amazon Athena**.

---

# Arquitetura

Banco Central API (Olinda)
        ↓
Python Ingestion
        ↓
S3 Bronze (raw + parquet)
        ↓
Transformação (Pandas)
        ↓
S3 Silver (dados tratados)
        ↓
Amazon Athena (SQL analytics)

---

# Tecnologias utilizadas

- Python
- AWS S3
- AWS Athena
- Pandas
- Parquet
- YAML Configs
- REST API
- Git / GitHub

---

# Estrutura do projeto
aws-pix-data-engineering-pipeline/
│
├── configs/
│ ├── app.yaml
│ └── datasets.yaml
│
├── src/
│ ├── core/
│ │ ├── config.py
│ │ ├── naming.py
│ │ ├── olinda.py
│ │ └── s3_io.py
│ │
│ ├── ingest/
│ │ └── ingest_run.py
│ │
│ ├── silver/
│ │ ├── pipelines.py
│ │ └── transforms.py
│ │
│ └── cli.py
│
├── requirements.txt
└── README.md


---

# Datasets utilizados

Dados públicos do Banco Central disponíveis via API Estatísticas do Pix.
