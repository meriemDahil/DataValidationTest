"""
comparator/pipeline.py
----------------------
ComparatorPipeline – orchestrates the 4-layer validation run.

Loads data, infers schema, normalises, runs layers in order,
computes the final decision, and returns the full result dict.
"""

import json
import sys
import os
from datetime import datetime

import pandas as pd
from loguru import logger

from .config import (
    PASS_THRESHOLD,
    SEPARATOR,
    STAGING_TABLE,
    TALEND_REFERENCE_PATH,
)
from .schema  import infer_schema, normalize
from .layer1  import Layer1Structural
from .layer2  import Layer2Data
from .layer3  import Layer3BusinessRules
from .layer4  import Layer4Statistical

# ---------------------------------------------------------------------------
# Lazy import of the DB loader so the package stays importable in unit tests
# even if the agent tools are absent.
# ---------------------------------------------------------------------------
def _load_sql_table(table_name: str) -> pd.DataFrame:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")
    from agent.tools.db import load_table_as_dataframe  # noqa: PLC0415
    return load_table_as_dataframe(table_name)


class ComparatorPipeline:
    """
    High-level orchestrator for the 4-layer Talend-to-SQL validation pipeline.

    Parameters
    ----------
    talend_path : str, optional
        Override the CSV path from config (useful in tests).
    staging_table : str, optional
        Override the SQL table name from config.
    sql_loader : callable, optional
        Override the SQL loader (``callable(table_name) -> pd.DataFrame``).
        Defaults to the project's ``load_table_as_dataframe``.
    """

    def __init__(
        self,
        talend_path:   str      = TALEND_REFERENCE_PATH,
        staging_table: str      = STAGING_TABLE,
        sql_loader:    callable = None,
    ):
        self.talend_path   = talend_path
        self.staging_table = staging_table
        self._sql_loader   = sql_loader or _load_sql_table

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self) -> dict:
        self._print_banner()

        talend_df, sql_df = self._load_data()
        talend_norm, sql_norm, schema = self._prepare(talend_df, sql_df)

        # ── Layer 1 (fail-fast) ────────────────────────────────────────
        l1 = Layer1Structural(talend_norm, sql_norm).run()
        if l1.get("fatal"):
            decision = "FAIL"
            reasons  = ["L1: Structural mismatch – pipeline stopped"]
            self._print_final(decision, reasons, l1, None, None, None)
            return self._build_output(
                decision, reasons, talend_df, sql_df, l1, None, None, None
            )

        # ── Layers 2-4 ────────────────────────────────────────────────
        l2 = Layer2Data(talend_norm, sql_norm).run()
        l3 = Layer3BusinessRules(talend_norm, sql_norm, schema).run()
        l4 = Layer4Statistical(talend_norm, sql_norm, schema).run()

        decision, reasons = self._compute_final_decision(l1, l2, l3, l4)
        self._print_final(decision, reasons, l1, l2, l3, l4)
        return self._build_output(decision, reasons, talend_df, sql_df, l1, l2, l3, l4)

    # ------------------------------------------------------------------
    # Data loading & preparation
    # ------------------------------------------------------------------

    def _load_data(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        logger.info("Loading datasets...")
        talend_df = pd.read_csv(self.talend_path, encoding="utf-8")
        sql_df    = self._sql_loader(self.staging_table)
        logger.info(f"  Talend : {len(talend_df)} rows x {len(talend_df.columns)} cols")
        logger.info(f"  SQL    : {len(sql_df)} rows x {len(sql_df.columns)} cols")
        return talend_df, sql_df

    @staticmethod
    def _prepare(
        talend_df: pd.DataFrame, sql_df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
        logger.info("Inferring schema from Talend reference...")
        raw_schema  = infer_schema(talend_df)
        logger.info(f"  Sort key         : {raw_schema['sort_key']}")
        logger.info(f"  Numeric cols     : {raw_schema['numeric_cols']}")
        logger.info(f"  Categorical cols : {raw_schema['categorical_cols']}")

        sort_key    = raw_schema["sort_key"]
        talend_norm = normalize(talend_df, sort_key)
        sql_norm    = normalize(sql_df,    sort_key)

        # Re-infer on normalised data (types may change after normalisation)
        schema = infer_schema(talend_norm)
        return talend_norm, sql_norm, schema

    # ------------------------------------------------------------------
    # Final decision logic
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_final_decision(l1, l2, l3, l4) -> tuple[str, list]:
        reasons = []
        if not l1["passed"]:
            reasons.append("L1: Structural mismatch (fatal)")
        if l2 and not l2["passed"]:
            reasons.append(
                f"L2: Data mismatch (match_rate={l2.get('match_rate', 0):.2%})"
            )
        if l3 and not l3["passed"]:
            for check, res in l3["checks"].items():
                if isinstance(res, dict) and not res.get("passed", True):
                    reasons.append(f"L3: {check} failed")
                elif isinstance(res, list):
                    for item in res:
                        if isinstance(item, dict) and not item.get("passed", True):
                            reasons.append(f"L3: {item.get('label', check)} failed")
        if l4 and not l4["passed"]:
            reasons.append("L4: Statistical distribution divergence")
        return ("PASS" if not reasons else "FAIL"), reasons

    # ------------------------------------------------------------------
    # Output builder
    # ------------------------------------------------------------------

    def _build_output(self, decision, reasons, talend_df, sql_df, l1, l2, l3, l4) -> dict:
        l2c        = l2["checks"] if l2 else {}
        l3c        = l3["checks"] if l3 else {}
        diff_data  = l2c.get("2.4_targeted_diff", {})
        match_rate = l2.get("match_rate", 0.0) if l2 else 0.0
        result = {
            "decision":    decision,
            "match_rate":  match_rate,
            "threshold":   PASS_THRESHOLD,
            "talend_rows": len(talend_df),
            "sql_rows":    len(sql_df),
            "run_date":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "talend_path": self.talend_path,
            "sql_table":   self.staging_table,
            "fail_reasons": reasons,
            "structure": {
                "talend_row_count":    len(talend_df),
                "sql_row_count":       len(sql_df),
                "row_count_match":     l2c.get("2.1_row_count", {}).get("passed", False) if l2 else False,
                "column_match":        l1["checks"].get("1.1_column_naming", {}).get("passed", False),
                "columns_only_in_talend": l1["checks"].get("1.1_column_naming", {}).get("missing_in_sql", []),
                "columns_only_in_sql":    l1["checks"].get("1.1_column_naming", {}).get("extra_in_sql",   []),
                "common_columns":         l1["checks"].get("1.1_column_naming", {}).get("common_columns", []),
            },
            
            "exact":   l3c.get("3.2_exact_match", {}) if l3 else {},
            "rounding": {
                col: {
                    "passed":        r.get("passed", False),
                    "mismatches":    r.get("mismatches", 0),
                    "reason":        f"Relative tolerance {r.get('tolerance', 'N/A')}",
                    "talend_sample": [],
                    "sql_sample":    [],
                }
                for col, r in l3c.get("3.3_relative_tolerance", {}).items()
            } if l3 else {},
            "row_level": {
                "total_rows":   max(len(talend_df), len(sql_df)),
                "matched_rows": l2c.get("2.2_row_hash", {}).get("matched_hashes", 0) if l2 else 0,
                "diff_rows":    l2c.get("2.2_row_hash", {}).get("only_in_talend",  0) if l2 else 0,
                "match_rate":   match_rate,
                "sample_diffs": diff_data.get("sample_diffs", []),
                "full_diffs":   diff_data.get("full_diffs",   []),
            },
            "layers": {"layer1": l1, "layer2": l2, "layer3": l3, "layer4": l4},
        }
        def _save_result_as_json(self, result: dict, output_path: str = None) -> str:
            """Save the result dictionary to a JSON file."""
            if output_path is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_path = f"validation_result_{timestamp}.json"
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            logger.info(f"Result saved to {output_path}")
            return output_path
        
        _save_result_as_json(self, result, output_path="validation_result.json")
        return result

    # ------------------------------------------------------------------
    # Display helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _print_banner():
        print("\n" + SEPARATOR)
        print("  VALIDATION AGENT - 4-LAYER COMPARATOR  [generic mode]")
        print(SEPARATOR + "\n")

    @staticmethod
    def _print_final(decision, reasons, l1, l2, l3, l4):
        print()
        print(SEPARATOR)
        if decision == "PASS":
            logger.success("FINAL DECISION : PASS")
        else:
            logger.error("FINAL DECISION : FAIL")
            for r in reasons:
                logger.error(f"  Reason: {r}")
        if l2:
            logger.info(f"Match rate     : {l2.get('match_rate', 'N/A'):.2%}")
        logger.info(
            "Layers run     : "
            f"L1={'OK' if l1['passed'] else 'FAIL'} | "
            f"L2={'OK' if l2 and l2['passed'] else ('FAIL' if l2 else 'SKIPPED')} | "
            f"L3={'OK' if l3 and l3['passed'] else ('FAIL' if l3 else 'SKIPPED')} | "
            f"L4={'OK' if l4 and l4['passed'] else ('FAIL' if l4 else 'SKIPPED')}"
        )
        print(SEPARATOR + "\n")