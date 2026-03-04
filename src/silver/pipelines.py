"""
pipelines.py
------------
Define como aplicar transformações SILVER de forma genérica usando regras do YAML.

- As regras de cada dataset ficam em configs/datasets.yaml (sessão "silver")
- Isso te permite adicionar novos endpoints sem mexer no código.
"""

from __future__ import annotations

import pandas as pd

from src.core.logger import get_logger
from src.silver import transforms as T

logger = get_logger(__name__)


def apply_pipeline_from_rules(df: pd.DataFrame, rules: dict) -> pd.DataFrame:
    """
    Aplica pipeline Silver baseado no dict 'rules' do YAML.

    Regras esperadas (todas opcionais):
    - int_cols
    - float_prefixes
    - int_prefixes
    - required
    - normalize_text
    """
    logger.info("Aplicando pipeline SILVER (rules do YAML)")

    df2 = (
        df
        .pipe(T.rename_snake_case)
        .pipe(
            T.cast_numbers,
            int_cols=rules.get("int_cols", []),
            float_prefixes=rules.get("float_prefixes", []),
            int_prefixes=rules.get("int_prefixes", []),
        )
        .pipe(T.cast_anomes)
        .pipe(T.drop_nulls, required=rules.get("required", []))
        .pipe(T.normalize_text, cols=rules.get("normalize_text", []))
        .pipe(T.add_metadata)
    )

    return df2
