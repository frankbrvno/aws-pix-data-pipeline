"""
jobs.py
-------
Transformações específicas da camada GOLD (agregações).

Cada função recebe um DataFrame SILVER e devolve um DataFrame pronto pra GOLD.
"""

from __future__ import annotations

import pandas as pd


def job_gold_pix_uf_mes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega transações por UF (estado) no mês.
    Espera colunas do pix_municipio (silver):
      anomes, estado_ibge, estado, sigla_regiao, regiao,
      vl_*, qt_*
    """
    group_cols = ["anomes", "estado_ibge", "estado", "sigla_regiao", "regiao"]

    agg = {
        "vl_pagadorpf": "sum",
        "qt_pagadorpf": "sum",
        "vl_pagadorpj": "sum",
        "qt_pagadorpj": "sum",
        "vl_recebedorpf": "sum",
        "qt_recebedorpf": "sum",
        "vl_recebedorpj": "sum",
        "qt_recebedorpj": "sum",
        "qt_pes_pagadorpf": "sum",
        "qt_pes_pagadorpj": "sum",
        "qt_pes_recebedorpf": "sum",
        "qt_pes_recebedorpj": "sum",
    }

    # só agrega colunas que existirem (evita quebrar)
    agg = {k: v for k, v in agg.items() if k in df.columns}

    out = df.groupby(group_cols, dropna=False, as_index=False).agg(agg)
    return out


def job_gold_fraudes_mes(df: pd.DataFrame) -> pd.DataFrame:
    """
    GOLD pra fraudes (mensal). Aqui como o dataset vem 1 linha por mês,
    o gold pode ser basicamente uma seleção/ordenção + garantir tipos.

    Espera colunas (silver):
      anomes, ... métricas ...
    """
    # ordena por mês e retorna (pode evoluir depois)
    if "anomes" in df.columns:
        return df.sort_values("anomes").reset_index(drop=True)
    return df


def job_gold_chaves_tipo_dia(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega estoque de chaves por tipo/participante no dia.
    Espera colunas do pix_chaves (silver):
      data, ispb, nome, naturezausuario, tipochave, qtdchaves
    """
    group_cols = ["data", "ispb", "nome", "naturezausuario", "tipochave"]
    value_col = "qtdchaves"

    if value_col not in df.columns:
        # fallback pra bronze case: qtdChaves
        if "qtdchaves" in df.columns:
            value_col = "qtdchaves"
        elif "qtdChaves" in df.columns:
            value_col = "qtdChaves"

    out = df.groupby(group_cols, dropna=False, as_index=False)[value_col].sum()
    out = out.rename(columns={value_col: "qtdchaves"})
    return out