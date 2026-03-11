"""
comparator/schema.py
--------------------
Schema inference and DataFrame normalization.
Called once before the pipeline; result is passed to every layer.

Null-handling contract
----------------------
After normalize() both DataFrames must represent "missing" the same way.
The problem: pandas uses numpy NaN (float) for missing numerics and
Python None for missing objects; SQLite returns None for every NULL;
CSV parsing returns NaN for empty cells.  If we don't unify them before
hashing, identical nulls produce different hash values and every null row
fails Layer 2.

Rule applied here:
  - Numeric columns  → NaN   (pd.NA / None → np.nan via pd.to_numeric)
  - String  columns  → ""    (NaN / None / "nan" / "None" / "NULL" → "")
  - The string sentinel "" is chosen over np.nan so that string columns
    keep dtype object and downstream .str operations never raise.
"""

import numpy as np
import pandas as pd

from .config import MAX_CATEGORICAL_RATIO

# Strings that should be treated as NULL in any column coming from CSV / SQLite
_NULL_STRINGS = {"nan", "none", "null", "na", "n/a", "<na>", ""}


# ---------------------------------------------------------------------------
# Schema inference
# ---------------------------------------------------------------------------

def infer_schema(df: pd.DataFrame) -> dict:
    """
    Auto-detect column roles from dtype and cardinality.

    Returns
    -------
    dict with keys:
        numeric_cols     – float / int columns (after normalization)
        categorical_cols – object columns where unique/total <= MAX_CATEGORICAL_RATIO
        sort_key         – first fully-unique column (PK candidate), else first column
        all_cols         – all column names
        n_rows           – row count

    Edge cases
    ----------
    - Empty DataFrame (n=0): sort_key falls back to first column, no
      categorical cols inferred (division by zero guard).
    - All-null column: nunique()==0, ratio==0 → classified as categorical.
      This is intentional: we cannot know the intended type from nulls alone,
      and categorical is the safer classification for downstream checks.
    - Columns with mixed numeric/string content (object dtype): treated as
      categorical, not numeric.  Callers should cast before passing in.
    """
    n = len(df)

    numeric_cols = [
        c for c in df.columns
        if pd.api.types.is_numeric_dtype(df[c])
    ]

    categorical_cols = [
        c for c in df.columns
        if df[c].dtype == object
        and n > 0
        and df[c].nunique(dropna=True) / n <= MAX_CATEGORICAL_RATIO
    ]

    # Sort key: prefer the first column whose non-null values are all unique
    sort_key = None
    for col in df.columns:
        non_null = df[col].dropna()
        if len(non_null) == n and non_null.nunique() == n:
            sort_key = col
            break
    if sort_key is None and len(df.columns) > 0:
        sort_key = df.columns[0]

    return {
        "numeric_cols"    : numeric_cols,
        "categorical_cols": categorical_cols,
        "sort_key"        : sort_key,
        "all_cols"        : list(df.columns),
        "n_rows"          : n,
    }


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

def normalize(df: pd.DataFrame, sort_key: str | None) -> pd.DataFrame:
    """
    Bring a DataFrame into a canonical form so that two DataFrames
    representing the same data produce identical values cell-by-cell.

    Steps (in order)
    ----------------
    1. Copy — never mutate the caller's DataFrame.
    2. Normalize column names: lowercase + strip whitespace.
    3. Normalize string columns:
         a. Strip leading/trailing whitespace.
         b. Replace all null-sentinel strings with pd.NA, then fill with "".
            This unifies: None, NaN, "nan", "None", "NULL", "null", "na", ""
            → all become "" in the output.
    4. Normalize numeric columns:
         a. Coerce to numeric with pd.to_numeric(errors="coerce") so that
            "None" / "nan" strings from SQLite become np.nan.
         b. This also handles the case where a numeric column was stored
            as object dtype because SQLite returned mixed None/str values.
    5. Cast integer-like columns to str ONLY if both sides have them as
       int64/int32 — this is now done per-column in pipeline._prepare()
       after both sides are loaded, so we skip it here to avoid masking
       real type mismatches.
    6. Sort by sort_key for deterministic row-order comparison.

    Parameters
    ----------
    df       : raw DataFrame (from CSV or SQLite)
    sort_key : column name to sort by, or None

    Returns
    -------
    Normalized DataFrame with reset index.
    """
    df = df.copy()

    # ── Step 2: column names ─────────────────────────────────────────
    df.columns = df.columns.str.lower().str.strip()

    # Recompute sort_key in case it changed case
    if sort_key:
        sort_key = sort_key.lower().strip()

    # ── Step 3: string columns ───────────────────────────────────────
    for col in df.select_dtypes(include=["object"]).columns:
        # Strip whitespace first
        df[col] = df[col].astype(str).str.strip()
        # Replace all null-sentinel strings (case-insensitive) with ""
        df[col] = df[col].apply(
            lambda v: "" if str(v).lower() in _NULL_STRINGS else v
        )

    # ── Step 4: numeric columns ──────────────────────────────────────
    # Coerce object columns that are numeric or all-null to float64/NaN.
    #
    # Two cases:
    #   a) Mixed numeric+null: SQLite returned "None" alongside numbers.
    #      Coerce if 90%+ of non-empty values parse as numeric.
    #   b) All-null: every cell is "" after step 3 (e.g. tax_rate when the
    #      SQL query produced no tax rows).  non_null_original is empty so
    #      the 90% check would skip it — but we MUST coerce to float64 so
    #      that both sides represent null the same way (NaN, not "").
    #      Without this, Talend has float64/NaN and SQL has object/"",
    #      they hash differently, and Layer 2 fails for all rows.
    for col in df.columns:
        if df[col].dtype != object:
            continue

        non_null_original = df[col].replace("", pd.NA).dropna()

        # Case b: entirely null column -> cast to float64 unconditionally
        if len(non_null_original) == 0:
            df[col] = np.nan
            df[col] = df[col].astype("float64")
            continue

        # Case a: mixed content -> coerce only if 90%+ values are numeric
        coerced          = pd.to_numeric(df[col].replace("", np.nan), errors="coerce")
        non_null_coerced = coerced.dropna()
        conversion_rate  = len(non_null_coerced) / len(non_null_original)
        if conversion_rate >= 0.9:
            df[col] = coerced

    # ── Step 6: sort ─────────────────────────────────────────────────
    if sort_key and sort_key in df.columns:
        df = df.sort_values(sort_key).reset_index(drop=True)
    else:
        df = df.reset_index(drop=True)

    return df