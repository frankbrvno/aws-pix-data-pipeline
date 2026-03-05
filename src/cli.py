"""
cli.py
------
Entrada única do projeto.

Por que existe?
- Para você rodar com comandos padrão:
  - python -m src.cli ingest <dataset_id> ...
  - python -m src.cli silver <dataset_id> ...

Isso é "airflow-ready", porque no Airflow cada task pode chamar o mesmo comando.
"""

from __future__ import annotations

import argparse

from src.ingest.ingest_run import run_ingest
from src.silver.silver_run import run_silver
from src.gold.gold_run import run_gold


def main():
    parser = argparse.ArgumentParser(prog="pix-pipeline")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # comando: ingest
    p_ingest = sub.add_parser("ingest", help="Executa ingestão (Bronze) para um dataset")
    p_ingest.add_argument("dataset_id", help="ID do dataset (ex: pix_municipio)")
    p_ingest.add_argument("--database", help="YYYY-MM (ex: 2026-03)")
    p_ingest.add_argument("--top", type=int, help="Limite de linhas ($top) no Olinda")

    # comando: silver
    p_silver = sub.add_parser("silver", help="Executa transformação (Silver) para um dataset")
    p_silver.add_argument("dataset_id", help="ID do dataset (ex: pix_municipio)")
    p_silver.add_argument("--database", help="YYYY-MM (ex: 2026-03)")
    
    # gold
    p_gold = sub.add_parser("gold", help="Executa jobs da camada GOLD")
    p_gold.add_argument("job_id", help="gold_pix_uf_mes | gold_fraudes_mes | gold_chaves_tipo_dia")
    p_gold.add_argument("--database", required=True, help="YYYY-MM (month) ou YYYY-MM-DD (day)")

    args = parser.parse_args()

    if args.cmd == "ingest":
        run_ingest(args.dataset_id, database=args.database, top=args.top)

    elif args.cmd == "silver":
        run_silver(args.dataset_id, database=args.database)

    elif args.cmd == "gold":
        run_gold(args.job_id, database=args.database)


if __name__ == "__main__":
    main()