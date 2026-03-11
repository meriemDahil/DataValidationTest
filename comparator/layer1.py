"""
comparator/layer1.py
--------------------
LAYER 1 – Structural Validation  (fail-fast)

Checks:
  1.1  Column naming  (set equality)
  1.2  Column count
  1.3  Data types     (compatible dtypes)
  1.4  Nullability    (null-ratio diff <= NULL_RATIO_THRESHOLD)
  1.5  Column order   (warning only)
"""

import pandas as pd
from loguru import logger

from .config import NULL_RATIO_THRESHOLD, STEP_LINE


class Layer1Structural:
    """
    Layer 1 – Fail-fast structural validation.
    
    Compares two data sources (Talend reference vs SQL output) for structural compatibility:
    - Column names must match (set equality)
    - Column count must match
    - Data types must be compatible
    - Null ratios must be within tolerance
    - Column order is checked (warning only, does not fail)
    
    If any of the first four checks fail, the comparison stops immediately (fatal).
    """

    def __init__(self, talend_df: pd.DataFrame, sql_df: pd.DataFrame):
        """
        Initialize the structural validator with reference and output DataFrames.
        
        Args:
            talend_df: Talend reference data (expected/baseline)
            sql_df:    SQL output data (actual transformation result)
        """
        self.talend_df = talend_df
        self.sql_df    = sql_df

    # ------------------------------------------------------------------
    # Public entry point – Execute all structural checks
    # ------------------------------------------------------------------

    def run(self) -> dict:
        """
        Execute all structural validation checks in sequence.
        
        Performs a fail-fast approach: stops immediately on first structural failure.
        Note: Column order check is warning-only and does not trigger fatal failure.
        
        Returns:
            dict: Validation results with structure:
                {
                    "layer": 1,
                    "passed": bool,
                    "fatal": bool,  # True if structural failure prevents further checks
                    "checks": {
                        "1.1_column_naming": {...},
                        "1.2_column_count": {...},
                        "1.3_data_types": {...},
                        "1.4_nullability": {...},
                        "1.5_column_order": {...}
                    }
                }
        """
        # Print section header for visibility in log output
        self._print_section()
        
        # Initialize results structure with layer info and empty checks
        results = {"layer": 1, "passed": True, "checks": {}, "fatal": False}

        # Extract column names from both DataFrames for comparison
        t_cols = list(self.talend_df.columns)  # Preserve order from Talend
        s_cols = list(self.sql_df.columns)     # Preserve order from SQL
        t_set  = set(t_cols)                   # For set operations (comparison)
        s_set  = set(s_cols)                   # For set operations (comparison)

        # ============================================================
        # Check 1.1: Column naming (set equality – no extra/missing)
        # ============================================================
        ok, payload = self._check_column_naming(t_set, s_set)
        results["checks"]["1.1_column_naming"] = payload
        if not ok:
            # Fatal failure: column names must match exactly
            results["passed"] = False
            results["fatal"]  = True
            logger.error("STRUCTURAL FAIL – stopping pipeline. Fix column names first.")
            return results

        # ============================================================
        # Check 1.2: Column count (total number of columns)
        # ============================================================
        ok, payload = self._check_column_count(t_cols, s_cols)
        results["checks"]["1.2_column_count"] = payload
        if not ok:
            # Fatal failure: column counts must match
            results["passed"] = False
            results["fatal"]  = True
            logger.error("STRUCTURAL FAIL – stopping pipeline.")
            return results

        # ============================================================
        # Check 1.3: Data types (compatible dtypes for each column)
        # ============================================================
        ok, payload = self._check_data_types(t_set, s_set)
        results["checks"]["1.3_data_types"] = payload
        if not ok:
            # Fatal failure: data types must be compatible
            results["passed"] = False
            results["fatal"]  = True
            logger.error("STRUCTURAL FAIL – stopping pipeline.")
            return results

        # ============================================================
        # Check 1.4: Nullability (null ratio difference tolerance)
        # ============================================================
        ok, payload = self._check_nullability(t_set, s_set)
        results["checks"]["1.4_nullability"] = payload
        if not ok:
            # Fatal failure: null ratios must be within threshold
            results["passed"] = False
            results["fatal"]  = True
            logger.error("STRUCTURAL FAIL – stopping pipeline.")
            return results

        # ============================================================
        # Check 1.5: Column order (warning only – does not fail)
        # ============================================================
        _ok, payload = self._check_column_order(t_cols, s_cols)
        results["checks"]["1.5_column_order"] = payload
        # Note: Column order mismatch is logged as a warning but does not set "fatal"

        # All structural checks passed
        results["passed"] = True
        return results

    # ------------------------------------------------------------------
    # Individual validation checks (1.1 through 1.5)
    # ------------------------------------------------------------------

    def _check_column_naming(self, t_set: set, s_set: set) -> tuple[bool, dict]:
        """
        Check 1.1: Verify column sets are identical (no missing/extra columns).
        
        Uses set operations to identify:
        - missing_in_sql: columns in Talend but not in SQL
        - extra_in_sql: columns in SQL but not in Talend
        - common_columns: columns in both DataFrames
        
        Args:
            t_set: Set of Talend column names
            s_set: Set of SQL column names
            
        Returns:
            (bool, dict): (True if sets are equal, detailed results)
        """
        # Calculate set differences to identify missing and extra columns
        missing = sorted(t_set - s_set)  # Columns expected but not found
        extra   = sorted(s_set - t_set)  # Unexpected columns in SQL
        ok      = t_set == s_set         # Sets must be identical
        
        # Log the result with detail about common, missing, and extra columns
        self._log("1.1 Column naming", ok,
                  f"{len(t_set & s_set)} common | "
                  f"missing={missing or 'none'} | extra={extra or 'none'}")
        
        # Return pass/fail status and detailed breakdown
        return ok, {
            "passed":          ok,
            "missing_in_sql":  missing,
            "extra_in_sql":    extra,
            "common_columns":  sorted(t_set & s_set),
        }

    def _check_column_count(self, t_cols: list, s_cols: list) -> tuple[bool, dict]:
        """
        Check 1.2: Verify both DataFrames have the same number of columns.
        
        A quick sanity check before deeper structural analysis.
        Note: Uses lists (to check count) but naming check already verified set equality.
        
        Args:
            t_cols: List of Talend column names (preserves order)
            s_cols: List of SQL column names (preserves order)
            
        Returns:
            (bool, dict): (True if counts match, detailed results)
        """
        # Compare total column counts
        ok = len(t_cols) == len(s_cols)
        
        # Log the result with actual counts
        self._log("1.2 Column count", ok,
                  f"Talend={len(t_cols)} | SQL={len(s_cols)}")
        
        # Return pass/fail status and counts
        return ok, {"passed": ok, "talend_count": len(t_cols), "sql_count": len(s_cols)}

    def _check_data_types(self, t_set: set, s_set: set) -> tuple[bool, dict]:
        """
        Check 1.3: Verify data types are compatible for all common columns.
        
        Compatibility rules:
        - Same type: always compatible (e.g., int64 == int64)
        - Different integer types: compatible (int64, int32 are both numeric)
        - Object/string types: compatible with other object/string types
        - Otherwise: incompatible (e.g., int64 vs float64)
        
        Args:
            t_set: Set of Talend column names
            s_set: Set of SQL column names
            
        Returns:
            (bool, dict): (True if all types are compatible, detailed issues)
        """
        # Collect type incompatibilities for reporting
        issues = []
        
        # Check type compatibility for each common column
        for col in sorted(t_set & s_set):
            # Get dtype as string for both DataFrames
            td = str(self.talend_df[col].dtype)
            sd = str(self.sql_df[col].dtype)
            
            # Determine compatibility:
            # - Same type is always compatible
            # - Both in {object, int64, int32} set are compatible (numeric/text)
            compatible = td == sd or {td, sd} <= {"object", "int64", "int32"}
            
            # Record incompatibilities for detailed error reporting
            if not compatible:
                issues.append({"column": col, "talend_type": td, "sql_type": sd})
        
        # Check passed if no incompatibilities found
        ok = len(issues) == 0
        
        # Log result with summary or list of issues
        self._log("1.3 Data types", ok,
                  "all types compatible" if ok
                  else f"{len(issues)} incompatible columns")
        
        # Log details of each incompatibility for user investigation
        if not ok:
            for i in issues:
                logger.warning(
                    f"  {i['column']:<22} Talend={i['talend_type']} | SQL={i['sql_type']}"
                )
        
        # Return pass/fail status and detailed type issues
        return ok, {"passed": ok, "type_issues": issues}

    def _check_nullability(self, t_set: set, s_set: set) -> tuple[bool, dict]:
        """
        Check 1.4: Verify null ratios are within tolerance for all columns.
        
        Compares the proportion of NULL values in each column between Talend and SQL.
        If the difference exceeds NULL_RATIO_THRESHOLD (from config), flags as failure.
        
        Example: If Talend has 5% NULLs and SQL has 12% NULLs, diff is 7%.
        If NULL_RATIO_THRESHOLD is 5%, this would fail.
        
        Args:
            t_set: Set of Talend column names
            s_set: Set of SQL column names
            
        Returns:
            (bool, dict): (True if all null ratios within tolerance, detailed issues)
        """
        # Collect null ratio discrepancies for reporting
        issues = []
        
        # Check null ratio consistency for each common column
        for col in sorted(t_set & s_set):
            # Calculate proportion of NULL values in each DataFrame
            tn = self.talend_df[col].isnull().mean()  # Talend null ratio (0.0 to 1.0)
            sn = self.sql_df[col].isnull().mean()     # SQL null ratio (0.0 to 1.0)
            
            # Check if difference exceeds threshold
            if abs(tn - sn) > NULL_RATIO_THRESHOLD:
                # Record the discrepancy for detailed error reporting
                issues.append({
                    "column":          col,
                    "talend_null_pct": round(tn * 100, 2),  # Convert to percentage
                    "sql_null_pct":    round(sn * 100, 2),  # Convert to percentage
                    "diff_pct":        round(abs(tn - sn) * 100, 2),  # Absolute difference
                })
        
        # Check passed if no null ratio discrepancies found
        ok = len(issues) == 0
        
        # Log result with summary or count of issues
        self._log("1.4 Nullability", ok,
                  "null ratios consistent" if ok
                  else f"{len(issues)} columns with null diff > {NULL_RATIO_THRESHOLD:.0%}")
        
        # Log details of each null ratio discrepancy for investigation
        if not ok:
            for i in issues:
                logger.warning(
                    f"  {i['column']:<22} "
                    f"Talend={i['talend_null_pct']}% | "
                    f"SQL={i['sql_null_pct']}% | "
                    f"diff={i['diff_pct']}%"
                )
        
        # Return pass/fail status and detailed null issues
        return ok, {"passed": ok, "null_issues": issues}

    def _check_column_order(self, t_cols: list, s_cols: list) -> tuple[bool, dict]:
        """
        Check 1.5: Verify column order is the same (warning only, does not fail).
        
        Unlike other structural checks, a column order mismatch does not prevent
        further validation. The comparison logic handles column reordering.
        This check is informational – alerts the user to potential inconsistencies
        between source and target systems.
        
        Args:
            t_cols: Ordered list of Talend column names
            s_cols: Ordered list of SQL column names
            
        Returns:
            (bool, dict): (True if order matches, detailed results)
        """
        # Check if column order is identical using list equality
        ok = t_cols == s_cols
        
        # Log result with appropriate message
        self._log("1.5 Column order", ok,
                  "same order" if ok
                  else "different order (warning only – comparison proceeds)")
        
        # Log additional context if order differs (informational only)
        if not ok:
            logger.warning("  Column order differs but data comparison will proceed.")
        
        # Return pass/fail status and both column orders for inspection
        return ok, {"passed": ok, "talend_order": t_cols, "sql_order": s_cols}

    # ------------------------------------------------------------------
    # Helper methods for output formatting and logging
    # ------------------------------------------------------------------

    @staticmethod
    def _print_section():
        """
        Print a formatted section header for Layer 1 validation.
        
        Improves readability of log output by clearly demarcating the start
        of Layer 1 validation with visual separators.
        """
        # Print blank line for visual separation from previous output
        print()
        # Print separator line from config (usually 60 hyphens)
        print(STEP_LINE)
        # Print section title
        print("  LAYER 1 – Structural Validation")
        # Print closing separator line
        print(STEP_LINE)

    @staticmethod
    def _log(name: str, passed: bool, detail: str = ""):
        """
        Log a check result with appropriate severity level.
        
        Uses logger.success() for passing checks (visible as pass marker)
        and logger.error() for failing checks (visible as failure marker).
        
        Args:
            name: Name of the check (e.g., "1.1 Column naming")
            passed: True if check passed, False if check failed
            detail: Optional detail message with specific failure/success info
        """
        if passed:
            # Log passing checks as success (will show with success color/marker)
            logger.success(f"[PASS] {name}  {detail}")
        else:
            # Log failing checks as error (will show with error color/marker)
            logger.error(f"[FAIL] {name}  {detail}")