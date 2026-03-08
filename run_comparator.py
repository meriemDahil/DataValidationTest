"""
run_comparator.py
-----------------
Standalone script to compare stg_output (SQL result)
against talend_reference.csv (Talend ground truth).

Run AFTER run_executor.py has completed successfully.

Usage:
    python run_comparator.py
"""

import sys
import os
import pandas as pd
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

# Add project root to path so imports work
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from agent.tools.db import load_table_as_dataframe

# ------------------------------------------------------------------
# Configure logger
# ------------------------------------------------------------------
logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}",
    colorize=True,
    level="DEBUG",
)

# ------------------------------------------------------------------
# Configuration
# Change these if your file paths are different
# ------------------------------------------------------------------
TALEND_REFERENCE_PATH = "data/talend_reference.csv"
STAGING_TABLE         = "stg_output"
PASS_THRESHOLD        = 0.95    # 95% of rows must match to PASS
NUMERIC_TOLERANCE     = 2       # decimal places for rounding comparison

# Columns to check for specific rules
COLS_NO_BLOCKED       = ["payment_status"]   # must not contain BLOCKED/CANCELLED
COLS_EXACT_MATCH      = ["payment_status", "invoice_id", "vendor_id", "currency"]
COLS_ROUNDED          = ["net_amount", "gross_amount", "tax_rate"]

SEPARATOR  = "=" * 60
STEP_LINE  = "-" * 60


# ------------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------------

def load_datasets() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load both datasets:
    - talend_df : the reference CSV (ground truth from Talend)
    - sql_df    : the stg_output table from SQLite (SQL result)
    """
    logger.info("Loading talend_reference.csv ...")
    talend_df = pd.read_csv(TALEND_REFERENCE_PATH, encoding="utf-8")
    logger.info(f"  Talend reference : {len(talend_df)} rows x {len(talend_df.columns)} columns")

    logger.info("Loading stg_output from SQLite ...")
    sql_df = load_table_as_dataframe(STAGING_TABLE)
    logger.info(f"  SQL output       : {len(sql_df)} rows x {len(sql_df.columns)} columns")

    return talend_df, sql_df


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize a DataFrame before comparison.
    This prevents false negatives from trivial differences
    like column name casing, extra whitespace, or float precision.
    """
    df = df.copy()

    # Normalize column names: lowercase + strip spaces
    df.columns = df.columns.str.lower().str.strip()

    # Normalize string columns: strip whitespace
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip()

    # Normalize numeric columns: round to tolerance
    for col in df.select_dtypes(include=["float64", "float32"]).columns:
        df[col] = df[col].round(NUMERIC_TOLERANCE)

    # Sort rows by invoice_id for consistent comparison
    if "invoice_id" in df.columns:
        df = df.sort_values("invoice_id").reset_index(drop=True)

    return df


def check_structure(talend_df: pd.DataFrame, sql_df: pd.DataFrame) -> dict:
    """
    CHECK 1 - Structural check
    Compares schemas and row counts before doing any row-level comparison.
    """
    talend_cols = set(talend_df.columns)
    sql_cols    = set(sql_df.columns)

    return {
        "talend_row_count"        : len(talend_df),
        "sql_row_count"           : len(sql_df),
        "row_count_match"         : len(talend_df) == len(sql_df),
        "column_match"            : talend_cols == sql_cols,
        "columns_only_in_talend"  : sorted(talend_cols - sql_cols),
        "columns_only_in_sql"     : sorted(sql_cols - talend_cols),
        "common_columns"          : sorted(talend_cols & sql_cols),
    }


def check_no_forbidden_statuses(sql_df: pd.DataFrame) -> dict:
    """
    CHECK 2 - No BLOCKED or CANCELLED rows in output
    The Talend job filters these out -- SQL must do the same.
    """
    forbidden = ["BLOCKED", "CANCELLED"]
    results   = {}

    for col in COLS_NO_BLOCKED:
        if col not in sql_df.columns:
            results[col] = {"passed": False, "reason": f"Column '{col}' not found"}
            continue

        found = sql_df[sql_df[col].isin(forbidden)]
        if len(found) == 0:
            results[col] = {
                "passed"       : True,
                "reason"       : "No forbidden statuses found",
                "forbidden_count" : 0,
            }
        else:
            results[col] = {
                "passed"          : False,
                "reason"          : f"Found {len(found)} forbidden rows",
                "forbidden_count" : len(found),
                "sample"          : found[col].value_counts().to_dict(),
            }

    return results


def check_exact_columns(talend_df: pd.DataFrame, sql_df: pd.DataFrame) -> dict:
    """
    CHECK 3 - Exact match on critical columns
    payment_status, invoice_id, vendor_id, currency must match exactly.
    """
    results = {}

    for col in COLS_EXACT_MATCH:
        if col not in talend_df.columns or col not in sql_df.columns:
            results[col] = {"passed": False, "reason": f"Column '{col}' missing in one dataset"}
            continue

        # Compare value by value after sorting
        talend_vals = talend_df[col].sort_values().reset_index(drop=True)
        sql_vals    = sql_df[col].sort_values().reset_index(drop=True)

        mismatches = (talend_vals != sql_vals).sum()

        results[col] = {
            "passed"     : mismatches == 0,
            "mismatches" : int(mismatches),
            "reason"     : "All values match" if mismatches == 0
                           else f"{mismatches} values differ",
        }

    return results


def check_net_amount_rounding(talend_df: pd.DataFrame, sql_df: pd.DataFrame) -> dict:
    """
    CHECK 4 - net_amount rounded to 2 decimal places
    Verifies the calculation net_amount = gross_amount * (1 - tax_rate)
    is correct and properly rounded.
    """
    results = {}

    for col in COLS_ROUNDED:
        if col not in talend_df.columns or col not in sql_df.columns:
            results[col] = {"passed": False, "reason": f"Column '{col}' missing"}
            continue

        talend_vals = talend_df[col].round(NUMERIC_TOLERANCE).sort_values().reset_index(drop=True)
        sql_vals    = sql_df[col].round(NUMERIC_TOLERANCE).sort_values().reset_index(drop=True)

        mismatches = (talend_vals != sql_vals).sum()

        results[col] = {
            "passed"         : mismatches == 0,
            "mismatches"     : int(mismatches),
            "talend_sample"  : talend_vals.head(3).tolist(),
            "sql_sample"     : sql_vals.head(3).tolist(),
            "reason"         : "All values match within tolerance" if mismatches == 0
                               else f"{mismatches} values differ after rounding",
        }

    return results


def check_row_level(talend_df: pd.DataFrame, sql_df: pd.DataFrame) -> dict:
    """
    CHECK 5 - Full row-level comparison
    Merges both DataFrames and finds rows that differ.
    Returns match rate and a sample of mismatched rows.
    """
    common_cols = sorted(set(talend_df.columns) & set(sql_df.columns))

    talend_sorted = talend_df[common_cols].sort_values(by=common_cols).reset_index(drop=True)
    sql_sorted    = sql_df[common_cols].sort_values(by=common_cols).reset_index(drop=True)

    # Find rows that differ
    if len(talend_sorted) == len(sql_sorted):
        # Same number of rows -- compare position by position
        diff_mask    = (talend_sorted != sql_sorted).any(axis=1)
        matched_rows = int((~diff_mask).sum())
        diff_rows    = talend_sorted[diff_mask].copy()
        diff_rows["source"] = "talend"
        sql_diff_rows = sql_sorted[diff_mask].copy()
        sql_diff_rows["source"] = "sql"

        # Build sample showing what differs
        sample_diffs = []
        diff_indices = diff_mask[diff_mask].index[:5]  # first 5 mismatches
        for idx in diff_indices:
            for col in common_cols:
                talend_val = talend_sorted.loc[idx, col]
                sql_val    = sql_sorted.loc[idx, col]
                if talend_val != sql_val:
                    sample_diffs.append({
                        "row"          : int(idx),
                        "column"       : col,
                        "talend_value" : str(talend_val),
                        "sql_value"    : str(sql_val),
                    })
    else:
        # Different row counts -- use merge to find unmatched rows
        talend_sorted["_source"] = "talend"
        sql_sorted["_source"]    = "sql"
        merged       = pd.merge(talend_sorted, sql_sorted,
                                on=common_cols, how="outer", indicator=True)
        matched_rows = int((merged["_merge"] == "both").sum())
        sample_diffs = []

    total_rows  = max(len(talend_df), len(sql_df))
    match_rate  = round(matched_rows / total_rows, 4) if total_rows > 0 else 1.0

    return {
        "total_rows"   : total_rows,
        "matched_rows" : matched_rows,
        "diff_rows"    : total_rows - matched_rows,
        "match_rate"   : match_rate,
        "sample_diffs" : sample_diffs,
    }


def print_section(title: str):
    print()
    print(STEP_LINE)
    print(f"  {title}")
    print(STEP_LINE)


def print_check_result(name: str, passed: bool, detail: str = ""):
    status = "PASS" if passed else "FAIL"
    if passed:
        logger.success(f"[{status}] {name}  {detail}")
    else:
        logger.error(f"[{status}] {name}  {detail}")


# ------------------------------------------------------------------
# Main comparison runner
# ------------------------------------------------------------------

def run_comparison() -> bool:
    print("\n" + SEPARATOR)
    print("  VALIDATION AGENT - COMPARISON STEP")
    print(SEPARATOR + "\n")

    all_checks_passed = True

    # --------------------------------------------------------------
    # STEP 1: Load datasets
    # --------------------------------------------------------------
    print_section("STEP 1: Load Datasets")
    try:
        talend_df, sql_df = load_datasets()
    except FileNotFoundError as e:
        logger.error(f"Could not load dataset: {e}")
        logger.error("Make sure run_executor.py ran successfully first.")
        return False
    except Exception as e:
        logger.error(f"Unexpected error loading datasets: {e}")
        return False

    # --------------------------------------------------------------
    # STEP 2: Normalize both datasets
    # --------------------------------------------------------------
    print_section("STEP 2: Normalize Datasets")
    talend_norm = normalize(talend_df)
    sql_norm    = normalize(sql_df)
    logger.info("Column names lowercased and stripped")
    logger.info("String values stripped of whitespace")
    logger.info(f"Numeric values rounded to {NUMERIC_TOLERANCE} decimal places")
    logger.info("Both datasets sorted by invoice_id")

    # --------------------------------------------------------------
    # STEP 3: Structural check
    # --------------------------------------------------------------
    print_section("STEP 3: Structural Check (rows + columns)")
    structure = check_structure(talend_norm, sql_norm)

    row_match = structure["row_count_match"]
    print_check_result(
        "Row count",
        row_match,
        f"Talend={structure['talend_row_count']} | SQL={structure['sql_row_count']}"
    )
    if not row_match:
        all_checks_passed = False

    col_match = structure["column_match"]
    print_check_result(
        "Column match",
        col_match,
        f"{len(structure['common_columns'])} common columns"
    )
    if not col_match:
        all_checks_passed = False
        if structure["columns_only_in_talend"]:
            logger.warning(f"  Missing in SQL   : {structure['columns_only_in_talend']}")
        if structure["columns_only_in_sql"]:
            logger.warning(f"  Extra in SQL     : {structure['columns_only_in_sql']}")

    # --------------------------------------------------------------
    # STEP 4: No BLOCKED / CANCELLED rows
    # --------------------------------------------------------------
    print_section("STEP 4: No Forbidden Statuses (BLOCKED / CANCELLED)")
    forbidden_results = check_no_forbidden_statuses(sql_norm)
    for col, result in forbidden_results.items():
        print_check_result(
            f"No forbidden values in '{col}'",
            result["passed"],
            result["reason"]
        )
        if not result["passed"]:
            all_checks_passed = False

    # --------------------------------------------------------------
    # STEP 5: Exact match on critical columns
    # --------------------------------------------------------------
    print_section("STEP 5: Exact Match on Critical Columns")
    exact_results = check_exact_columns(talend_norm, sql_norm)
    for col, result in exact_results.items():
        print_check_result(
            f"Exact match: '{col}'",
            result["passed"],
            result["reason"]
        )
        if not result["passed"]:
            all_checks_passed = False

    # --------------------------------------------------------------
    # STEP 6: Numeric rounding check
    # --------------------------------------------------------------
    print_section("STEP 6: Numeric Rounding Check (net_amount, gross_amount)")
    rounding_results = check_net_amount_rounding(talend_norm, sql_norm)
    for col, result in rounding_results.items():
        print_check_result(
            f"Rounding check: '{col}'",
            result["passed"],
            result["reason"]
        )
        if result.get("talend_sample"):
            logger.debug(f"  Talend sample : {result['talend_sample']}")
            logger.debug(f"  SQL sample    : {result['sql_sample']}")
        if not result["passed"]:
            all_checks_passed = False

    # --------------------------------------------------------------
    # STEP 7: Full row-level comparison
    # --------------------------------------------------------------
    print_section("STEP 7: Full Row-Level Comparison")
    row_results = check_row_level(talend_norm, sql_norm)

    match_rate   = row_results["match_rate"]
    final_decision = "PASS" if match_rate >= PASS_THRESHOLD else "FAIL"

    logger.info(f"Total rows    : {row_results['total_rows']}")
    logger.info(f"Matched rows  : {row_results['matched_rows']}")
    logger.info(f"Diff rows     : {row_results['diff_rows']}")
    logger.info(f"Match rate    : {match_rate:.2%}")
    logger.info(f"Threshold     : {PASS_THRESHOLD:.0%}")

    if row_results["sample_diffs"]:
        print()
        logger.warning("Sample of differences found:")
        for diff in row_results["sample_diffs"]:
            logger.warning(
                f"  Row {diff['row']:>3} | Column: {diff['column']:<20} | "
                f"Talend: {diff['talend_value']:<20} | "
                f"SQL: {diff['sql_value']}"
            )

    # --------------------------------------------------------------
    # Final decision
    # --------------------------------------------------------------
    print()
    print(SEPARATOR)
    if final_decision == "PASS":
        logger.success(f"FINAL DECISION : PASS")
        logger.success(f"Match rate     : {match_rate:.2%} (threshold: {PASS_THRESHOLD:.0%})")
    else:
        logger.error(f"FINAL DECISION : FAIL")
        logger.error(f"Match rate     : {match_rate:.2%} (threshold: {PASS_THRESHOLD:.0%})")

    all_checks_passed = all_checks_passed and (final_decision == "PASS")
    logger.info(f"All checks passed : {all_checks_passed}")
    print(SEPARATOR + "\n")

    return all_checks_passed


if __name__ == "__main__":
    success = run_comparison()
    sys.exit(0 if success else 1)
