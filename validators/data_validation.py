"""validate()

    ↓

row_count_check()

    ↓ (if mismatch → continue)

dataset_hash_check()

    ↓ (if mismatch → continue)

row_diff_analysis()

"""

import pandas as pd
import hashlib
from typing import Dict, Any


class DataValidator:

    
    @staticmethod
    def row_count_check(df_a: pd.DataFrame, df_b: pd.DataFrame) -> Dict[str, Any]:

        count_a = len(df_a)
        count_b = len(df_b)

        return {
            "match": count_a == count_b,
            "dataset_a_rows": count_a,
            "dataset_b_rows": count_b,
        }

    @staticmethod
    def dataset_hash(df: pd.DataFrame) -> str:
        """
        Create deterministic dataset hash
        """

        # convert dataframe → deterministic string
        data_string = df.to_csv(index=False)

        return hashlib.sha256(data_string.encode()).hexdigest()

    @staticmethod
    def dataset_hash_check(df_a: pd.DataFrame, df_b: pd.DataFrame) -> Dict[str, Any]:

        hash_a = DataValidator.dataset_hash(df_a)
        hash_b = DataValidator.dataset_hash(df_b)

        return {
            "match": hash_a == hash_b,
            "hash_a": hash_a,
            "hash_b": hash_b,
        }

    @staticmethod
    def row_diff(df_a: pd.DataFrame, df_b: pd.DataFrame):

        merged = df_a.merge(
            df_b,
            how="outer",
            indicator=True
        )

        mismatched_rows = merged[merged["_merge"] != "both"]

        return mismatched_rows

    @staticmethod
    def validate(df_a: pd.DataFrame, df_b: pd.DataFrame) -> Dict[str, Any]:

        result = {
            "valid": True,
            "metrics": {},
            "mismatched_rows": None
        }

        # ---------- Phase 1: Row Count ----------
        row_check = DataValidator.row_count_check(df_a, df_b)

        result["metrics"]["row_count"] = row_check

        if not row_check["match"]:
            result["valid"] = False
            result["metrics"]["reason"] = "ROW_COUNT_MISMATCH"
            return result

        # ---------- Phase 2: Dataset Hash ----------
        hash_check = DataValidator.dataset_hash_check(df_a, df_b)

        result["metrics"]["dataset_hash"] = hash_check

        if hash_check["match"]:
            return result

        # ---------- Phase 3: Deep Diff ----------
        mismatches = DataValidator.row_diff(df_a, df_b)

        result["valid"] = False
        result["metrics"]["reason"] = "CONTENT_MISMATCH"
        result["metrics"]["mismatch_count"] = len(mismatches)
        result["mismatched_rows"] = mismatches.head(50).to_dict("records")

        return result