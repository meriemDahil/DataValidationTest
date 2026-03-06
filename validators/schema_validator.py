# validators/structural_validator.py

import pandas as pd
from typing import Dict, Any


class StructuralValidator:
    """
    Validates structural compatibility between two datasets.
    """

    @staticmethod
    def validate(df_a: pd.DataFrame, df_b: pd.DataFrame) -> Dict[str, Any]:

        result = {
            "valid": True,
            "metrics": {},
            "errors": [],
        }

        # column count
        if len(df_a.columns) != len(df_b.columns):
            result["valid"] = False
            result["errors"].append("Column count mismatch")

        # column names
        cols_a = list(df_a.columns)
        cols_b = list(df_b.columns)

        if cols_a != cols_b:
            result["valid"] = False
            result["errors"].append("Column name/order mismatch")

        # dtype comparison
        dtype_mismatch = {}

        for col in cols_a:
            if col not in df_b.columns:
                continue

            type_a = str(df_a[col].dtype)
            type_b = str(df_b[col].dtype)

            if type_a != type_b:
                dtype_mismatch[col] = {"dataset_a": type_a, "dataset_b": type_b}

        if dtype_mismatch:
            result["valid"] = False
            result["errors"].append("Datatype mismatch")

        result["metrics"] = {
            "dataset_a_columns": len(cols_a),
            "dataset_b_columns": len(cols_b),
            "dtype_mismatch": dtype_mismatch,
        }

        return result