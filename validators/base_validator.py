# core/base_validator.py

import pandas as pd
import numpy as np
from typing import List, Optional


class Canonicalizer:
    """
    Canonical normalization of datasets before validation.
    """

    @staticmethod
    def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = [col.lower().strip() for col in df.columns]
        return df

    @staticmethod
    def normalize_nulls(df: pd.DataFrame) -> pd.DataFrame:
        df = df.replace({np.nan: None})
        return df

    @staticmethod
    def normalize_floats(df: pd.DataFrame, precision: int = 6) -> pd.DataFrame:
        df = df.copy()

        float_cols = df.select_dtypes(include=["float"]).columns

        for col in float_cols:
            df[col] = df[col].round(precision)

        return df

    @staticmethod
    def normalize_timestamps(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        datetime_cols = df.select_dtypes(include=["datetime"]).columns

        for col in datetime_cols:
            df[col] = pd.to_datetime(df[col], utc=True)

        return df

    @staticmethod
    def sort_rows(df: pd.DataFrame, keys: Optional[List[str]]) -> pd.DataFrame:
        if not keys:
            return df

        return df.sort_values(by=keys).reset_index(drop=True)

    @staticmethod
    def align_column_order(df: pd.DataFrame, reference_columns: List[str]) -> pd.DataFrame:
        return df[reference_columns]

    @staticmethod
    def normalize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
        """
        Harmonize dataframe dtypes into canonical types
        so equivalent schemas don't fail validation.
        """

        df = df.copy()

        for col in df.columns:

            dtype = df[col].dtype

            # integer family
            if pd.api.types.is_integer_dtype(dtype):
                df[col] = df[col].astype("int64")

            # float family
            elif pd.api.types.is_float_dtype(dtype):
                df[col] = df[col].astype("float64")

            # boolean
            elif pd.api.types.is_bool_dtype(dtype):
                df[col] = df[col].astype("bool")

            # datetime
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                df[col] = pd.to_datetime(df[col], utc=True)

            # everything else -> string
            else:
                df[col] = df[col].astype("string")

        return df


    @staticmethod
    def canonicalize(
        df: pd.DataFrame,
        reference_columns: Optional[List[str]] = None,
        sort_keys: Optional[List[str]] = None,
    ) -> pd.DataFrame:

        df = Canonicalizer.normalize_column_names(df)

        df = Canonicalizer.normalize_nulls(df)

        df = Canonicalizer.normalize_dtypes(df) 

        df = Canonicalizer.normalize_floats(df)

        df = Canonicalizer.normalize_timestamps(df)

        if reference_columns is not None:
            df = Canonicalizer.align_column_order(df, list(reference_columns))
        df = Canonicalizer.sort_rows(df, sort_keys)

        return df