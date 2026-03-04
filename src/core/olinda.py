"""
olinda.py
---------
Responsável por fazer requisições para a API Olinda do Banco Central.

Centralizar essa lógica permite:
- mudar headers facilmente
- adicionar retry no futuro
"""

import requests

from src.core.logger import get_logger

logger = get_logger(__name__)


# Headers usados nas requisições
HEADERS = {
    "accept": "application/json;odata.metadata=minimal",
    "user-agent": "curl/8.0",
    "accept-encoding": "identity",
    "connection": "close",
}


def fetch_json(url: str, timeout: int = 60) -> dict:
    """
    Faz uma requisição HTTP GET e retorna JSON.

    Se a API retornar erro HTTP, levanta exceção.
    """

    logger.info(f"Chamando API Olinda")

    resp = requests.get(url, headers=HEADERS, timeout=timeout)

    logger.info(f"Status HTTP: {resp.status_code}")

    resp.raise_for_status()

    return resp.json()