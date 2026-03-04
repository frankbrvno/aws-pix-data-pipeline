"""
config.py
---------
Responsável por carregar arquivos de configuração YAML do projeto.

Arquivos carregados:
- configs/app.yaml
- configs/datasets.yaml

Isso evita hardcode de:
- bucket
- paths bronze/silver
- endpoints
"""

from pathlib import Path
from typing import Dict, Any

import yaml

from src.core.logger import get_logger

logger = get_logger(__name__)


def load_yaml(path: str) -> Dict[str, Any]:
    """
    Lê um arquivo YAML e retorna um dict Python.
    """

    p = Path(path)

    logger.info(f"Carregando config: {p}")

    if not p.exists():
        logger.error(f"Arquivo de config não encontrado: {p}")
        raise FileNotFoundError(p)

    with p.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    if not isinstance(data, dict):
        raise ValueError(f"YAML inválido: {p}")

    return data


def load_app_config() -> Dict[str, Any]:
    """
    Carrega configs gerais do projeto.
    """
    return load_yaml("configs/app.yaml")


def load_datasets_config() -> Dict[str, Any]:
    """
    Carrega configs específicas de cada dataset.
    """
    return load_yaml("configs/datasets.yaml")