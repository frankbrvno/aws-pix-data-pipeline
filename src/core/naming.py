"""
naming.py
---------
Funções utilitárias para padronização de nomes.

Principal uso:
- transformar nomes de colunas em snake_case
"""

import re

from src.core.logger import get_logger

logger = get_logger(__name__)


def to_snake(name: str) -> str:
    """
    Converte string para snake_case.

    Exemplos:

    Municipio_Ibge -> municipio_ibge
    VL_PagadorPF -> vl_pagadorpf
    Sigla Regiao -> sigla_regiao
    """

    logger.debug(f"Convertendo coluna para snake_case: {name}")

    name = name.strip()
    name = re.sub(r"[^\w]+", "_", name)
    name = re.sub(r"__+", "_", name)

    return name.lower().strip("_")