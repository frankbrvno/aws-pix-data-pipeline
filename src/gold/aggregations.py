from __future__ import annotations
import pandas as pd


def gold_pix_uf_mes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega o Pix por UF/mês a partir do silver de transacoes_pix_por_municipio.
    Espera colunas (snake_case):
      - anomes (str), estado_ibge (Int64), estado (str), sigla_regiao (str), regiao (str)
      - vl_*, qt_* (numéricas)
    """
    # garante colunas de grupo
    group_cols = ["anomes", "estado_ibge", "estado", "sigla_regiao", "regiao"]
    for c in group_cols:
        if c not in df.columns:
            raise ValueError(f"Coluna obrigatória ausente: {c}")

    # colunas métricas (só as que existirem)
    metric_cols = [c for c in df.columns if c.startswith("vl_") or c.startswith("qt_")]
    if not metric_cols:
        raise ValueError("Nenhuma coluna métrica (vl_* / qt_*) encontrada.")

    agg = {c: "sum" for c in metric_cols}

    out = (
        df.groupby(group_cols, dropna=False)
          .agg(agg)
          .reset_index()
    )

    return out


def gold_fraudes_mes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Silver de fraudes já é mensal; aqui a gente padroniza e adiciona 2 métricas úteis.
    Espera:
      - anomes (str)
      - qtdepixcontestados (int/Int64) etc.
      - percentualdedevolucao (float) pode existir
    """
    if "anomes" not in df.columns:
        raise ValueError("Coluna obrigatória ausente: anomes")

    out = df.copy()

    # exemplos de métricas derivadas (só se colunas existirem)
    if "valorpixdevolvidosintegralmente" in out.columns and "valorpixdevolvidosparcialmente" in out.columns:
        out["valorpixdevolvido_total"] = (
            pd.to_numeric(out["valorpixdevolvidosintegralmente"], errors="coerce").fillna(0)
            + pd.to_numeric(out["valorpixdevolvidosparcialmente"], errors="coerce").fillna(0)
        )

    if "valorpixcontestadosaceitos" in out.columns and "valorpixdevolvido_total" in out.columns:
        denom = pd.to_numeric(out["valorpixcontestadosaceitos"], errors="coerce")
        num = pd.to_numeric(out["valorpixdevolvido_total"], errors="coerce")
        out["taxa_devolucao_valor_calc"] = (num / denom).where(denom.notna() & (denom != 0))

    return out


def gold_chaves_tipo_dia(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega estoque de chaves por dia + tipochave + naturezausuario.
    Espera:
      - data (str)  (do parquet)
      - tipochave (str)
      - naturezausuario (str)
      - qtdchaves (int/Int64)
    """
    needed = ["data", "tipochave", "naturezausuario", "qtdchaves"]
    for c in needed:
        if c not in df.columns:
            raise ValueError(f"Coluna obrigatória ausente: {c}")

    out = (
        df.groupby(["data", "tipochave", "naturezausuario"], dropna=False)["qtdchaves"]
          .sum(min_count=1)
          .reset_index()
    )
    return out