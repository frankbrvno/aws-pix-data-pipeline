"""
logger.py
---------
Configuração central de logging do projeto.

Por que existe?
- Evita usar print()
- Permite controlar níveis de log (INFO, WARNING, ERROR)
- Facilita integração com Airflow / CloudWatch no futuro
"""

import logging


def get_logger(name: str) -> logging.Logger:
    """
    Retorna um logger configurado.

    name normalmente será __name__ do módulo.
    """

    logger = logging.getLogger(name)

    # Evita configurar o logger mais de uma vez
    if not logger.handlers:

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        )

    return logger