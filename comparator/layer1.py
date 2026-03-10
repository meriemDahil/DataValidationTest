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
    """Fail-fast structural validation between two DataFrames."""

    def __init__(self, talend_df: pd.DataFrame, sql_df: pd.DataFrame):
        self.talend_df = talend_df
        self.sql_df    = sql_df

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self) -> dict:
        self._print_section()
        results = {"layer": 1, "passed": True, "checks": {}, "fatal": False}

        t_cols = list(self.talend_df.columns)
        s_cols = list(self.sql_df.columns)
        t_set  = set(t_cols)
        s_set  = set(s_cols)

        # Each check returns (ok, payload); on fatal failure we return early.
        ok, payload = self._check_column_naming(t_set, s_set)
        results["checks"]["1.1_column_naming"] = payload
        if not ok:
            results["passed"] = False
            results["fatal"]  = True
            logger.error("STRUCTURAL FAIL – stopping pipeline. Fix column names first.")
            return results

        ok, payload = self._check_column_count(t_cols, s_cols)
        results["checks"]["1.2_column_count"] = payload
        if not ok:
            results["passed"] = False
            results["fatal"]  = True
            logger.error("STRUCTURAL FAIL – stopping pipeline.")
            return results

        ok, payload = self._check_data_types(t_set, s_set)
        results["checks"]["1.3_data_types"] = payload
        if not ok:
            results["passed"] = False
            results["fatal"]  = True
            logger.error("STRUCTURAL FAIL – stopping pipeline.")
            return results

        ok, payload = self._check_nullability(t_set, s_set)
        results["checks"]["1.4_nullability"] = payload
        if not ok:
            results["passed"] = False
            results["fatal"]  = True
            logger.error("STRUCTURAL FAIL – stopping pipeline.")
            return results

        _ok, payload = self._check_column_order(t_cols, s_cols)
        results["checks"]["1.5_column_order"] = payload
        # order mismatch is a warning only – does not set fatal

        results["passed"] = True
        return results

    # ------------------------------------------------------------------
    # Individual checks
    # ------------------------------------------------------------------

    def _check_column_naming(self, t_set: set, s_set: set) -> tuple[bool, dict]:
        missing = sorted(t_set - s_set)
        extra   = sorted(s_set - t_set)
        ok      = t_set == s_set
        self._log("1.1 Column naming", ok,
                  f"{len(t_set & s_set)} common | "
                  f"missing={missing or 'none'} | extra={extra or 'none'}")
        return ok, {
            "passed":          ok,
            "missing_in_sql":  missing,
            "extra_in_sql":    extra,
            "common_columns":  sorted(t_set & s_set),
        }

    def _check_column_count(self, t_cols: list, s_cols: list) -> tuple[bool, dict]:
        ok = len(t_cols) == len(s_cols)
        self._log("1.2 Column count", ok,
                  f"Talend={len(t_cols)} | SQL={len(s_cols)}")
        return ok, {"passed": ok, "talend_count": len(t_cols), "sql_count": len(s_cols)}

    def _check_data_types(self, t_set: set, s_set: set) -> tuple[bool, dict]:
        issues = []
        for col in sorted(t_set & s_set):
            td = str(self.talend_df[col].dtype)
            sd = str(self.sql_df[col].dtype)
            compatible = td == sd or {td, sd} <= {"object", "int64", "int32"}
            if not compatible:
                issues.append({"column": col, "talend_type": td, "sql_type": sd})
        ok = len(issues) == 0
        self._log("1.3 Data types", ok,
                  "all types compatible" if ok
                  else f"{len(issues)} incompatible columns")
        if not ok:
            for i in issues:
                logger.warning(
                    f"  {i['column']:<22} Talend={i['talend_type']} | SQL={i['sql_type']}"
                )
        return ok, {"passed": ok, "type_issues": issues}

    def _check_nullability(self, t_set: set, s_set: set) -> tuple[bool, dict]:
        issues = []
        for col in sorted(t_set & s_set):
            tn = self.talend_df[col].isnull().mean()
            sn = self.sql_df[col].isnull().mean()
            if abs(tn - sn) > NULL_RATIO_THRESHOLD:
                issues.append({
                    "column":          col,
                    "talend_null_pct": round(tn * 100, 2),
                    "sql_null_pct":    round(sn * 100, 2),
                    "diff_pct":        round(abs(tn - sn) * 100, 2),
                })
        ok = len(issues) == 0
        self._log("1.4 Nullability", ok,
                  "null ratios consistent" if ok
                  else f"{len(issues)} columns with null diff > {NULL_RATIO_THRESHOLD:.0%}")
        if not ok:
            for i in issues:
                logger.warning(
                    f"  {i['column']:<22} "
                    f"Talend={i['talend_null_pct']}% | "
                    f"SQL={i['sql_null_pct']}% | "
                    f"diff={i['diff_pct']}%"
                )
        return ok, {"passed": ok, "null_issues": issues}

    def _check_column_order(self, t_cols: list, s_cols: list) -> tuple[bool, dict]:
        ok = t_cols == s_cols
        self._log("1.5 Column order", ok,
                  "same order" if ok
                  else "different order (warning only – comparison proceeds)")
        if not ok:
            logger.warning("  Column order differs but data comparison will proceed.")
        return ok, {"passed": ok, "talend_order": t_cols, "sql_order": s_cols}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _print_section():
        print()
        print(STEP_LINE)
        print("  LAYER 1 – Structural Validation")
        print(STEP_LINE)

    @staticmethod
    def _log(name: str, passed: bool, detail: str = ""):
        if passed:
            logger.success(f"[PASS] {name}  {detail}")
        else:
            logger.error(f"[FAIL] {name}  {detail}")