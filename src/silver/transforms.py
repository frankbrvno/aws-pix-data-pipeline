"""
transforms.py
-------------
Funções pequenas e reutilizáveis para limpar/padronizar dados na camada SILVER.

Ideia:
- Cada função faz 1 coisa (tipagem, nulos, normalização, etc.)
- Depois você “encadeia” com .pipe() no pipeline.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pandas as pd

from src.core.logger import get_logger
from src.core.naming import to_snake

logger = get_logger(__name__)


def rename_snake_case(df: pd.DataFrame) -> pd.DataFrame:
    """Padroniza nomes de colunas para snake_case."""
    df = df.copy()
    df.columns = [to_snake(c) for c in df.columns]
    return df


def drop_nulls(df: pd.DataFrame, required: List[str]) -> pd.DataFrame:
    """
    Remove linhas que tenham nulos em colunas obrigatórias.
    Ex: municipio/estado/ibge etc.
    """
    df = df.copy()
    for c in required:
        if c in df.columns:
            before = len(df)
            df = df[df[c].notna()]
            removed = before - len(df)
            if removed:
                logger.info(f"drop_nulls: removidos {removed} registros por nulo em '{c}'")
    return df


def normalize_text(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """Padroniza texto (UPPER + strip) em colunas definidas."""
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.upper().str.strip()
    return df


def cast_numbers(
    df: pd.DataFrame,
    int_cols: List[str],
    float_prefixes: List[str],
    int_prefixes: List[str],
) -> pd.DataFrame:
    """
    Faz tipagem:
    - int_cols: colunas específicas -> Int64 (inteiro nullable)
    - float_prefixes: colunas com prefixo -> float64
    - int_prefixes: colunas com prefixo -> Int64 (inteiro nullable)

    Importante:
    - Int64 (pandas nullable) evita virar float no Parquet quando tem nulos.
    """
    df = df.copy()

    # inteiros específicos (nullable)
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    # floats por prefixo (vl_ normalmente)
    for c in df.columns:
        if any(c.startswith(p) for p in float_prefixes):
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")

    # ints por prefixo (qt_ normalmente)
    for c in df.columns:
        if any(c.startswith(p) for p in int_prefixes):
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    return df


def cast_anomes(df: pd.DataFrame, col: str = "anomes") -> pd.DataFrame:
    """
    Garante que 'anomes' esteja como string.
    (Você pode transformar em DATE depois no Gold ou no Athena)
    """
    df = df.copy()
    if col in df.columns:
        df[col] = df[col].astype(str)
    return df


def add_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """Adiciona timestamp de transformação silver."""
    df = df.copy()
    df["silver_ts_utc"] = datetime.now(timezone.utc).isoformat()
    return df