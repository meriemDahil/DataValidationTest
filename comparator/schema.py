"""
comparator/schema.py
--------------------
Schema inference and DataFrame normalization.
Called once before the pipeline; result is passed to every layer.
"""

import pandas as pd
from .config import MAX_CATEGORICAL_RATIO


def infer_schema(df: pd.DataFrame) -> dict:
    """
    Auto-detect column roles from dtype and cardinality.

    Returns
    -------
    dict with keys:
        numeric_cols     – float / int columns
        categorical_cols – object columns where unique/total <= MAX_CATEGORICAL_RATIO
        sort_key         – first fully-unique column (PK candidate), else first column
        all_cols         – all column names
        n_rows           – row count
    """
    n = len(df)
    numeric_cols = [
        c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])
    ]
    categorical_cols = [
        c for c in df.columns
        if df[c].dtype == object
        and n > 0
        and df[c].nunique() / n <= MAX_CATEGORICAL_RATIO
    ]

    sort_key = None
    for col in df.columns:
        if df[col].nunique() == n:
            sort_key = col
            break
    if sort_key is None and len(df.columns) > 0:
        sort_key = df.columns[0]

    return {
        "numeric_cols":     numeric_cols,
        "categorical_cols": categorical_cols,
        "sort_key":         sort_key,
        "all_cols":         list(df.columns),
        "n_rows":           n,
    }


def normalize(df: pd.DataFrame, sort_key: str | None) -> pd.DataFrame:
    """
    Minimal normalization:
      - lowercase / strip column names
      - strip whitespace from string values
      - cast int columns to str (neutralises SQLite vs CSV type differences)
      - sort by sort_key so row-order comparisons are deterministic
    """
    df = df.copy()
    df.columns = df.columns.str.lower().str.strip()
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip()
    for col in df.select_dtypes(include=["int64", "int32"]).columns:
        df[col] = df[col].astype(str).str.strip()
    if sort_key and sort_key in df.columns:
        df = df.sort_values(sort_key).reset_index(drop=True)
    return df