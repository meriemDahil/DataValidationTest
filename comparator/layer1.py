"""
comparator/layer1.py
--------------------
LAYER 1 – Structural Validation  (fail-fast)

Checks:
  1.1  Column naming  (set equality — missing / extra columns)
  1.2  Data types     (family-based compatibility)
  1.3  Column order   (warning only — does not fail)

Design decisions:
  - Nullability (null-ratio diff) was moved to Layer 4 where it belongs.
    It is a data-quality check, not a structural one, and should not
    block the pipeline as a fatal error.
  - Column count is intentionally NOT a separate check: if 1.1 (set
    equality) passes, both DataFrames have identical column sets and
    therefore identical counts. A separate count check would be
    unreachable dead code.
  - "fatal" has been removed from the result dict. It was always equal
    to `not passed`, making it redundant. Callers should check `passed`.
  - Warnings are collected in a top-level "warnings" list so callers
    can surface them without treating them as failures.

Null handling:
  - NULL/NaN column names (e.g. from a bad CSV parse) are detected in
    1.1 before set comparison, because NaN != NaN in Python so they
    cause silent false PASSes or KeyErrors downstream.
  - All-null columns get dtype float64 in pandas (NaN is a float).
    In 1.2 we detect these and skip the type comparison for them,
    recording a warning instead of a false FAIL.
"""

import pandas as pd
from loguru import logger

from .config import STEP_LINE


# ---------------------------------------------------------------------------
# Type-family mapping used by check 1.2
# ---------------------------------------------------------------------------

_NUMERIC_TYPES = {
    "int8", "int16", "int32", "int64",
    "uint8", "uint16", "uint32", "uint64",
    "float16", "float32", "float64",
    # pandas nullable integer / float extensions (capital-I / capital-F)
    "Int8", "Int16", "Int32", "Int64",
    "UInt8", "UInt16", "UInt32", "UInt64",
    "Float32", "Float64",
}

_STRING_TYPES   = {"object", "string", "StringDtype"}
_BOOL_TYPES     = {"bool", "boolean"}  # "boolean" = pandas nullable BooleanDtype


def _type_family(dtype_str: str) -> str:
    """
    Map a pandas dtype string to a broad compatibility family.

    Families
    --------
    numeric  – any integer or float variant (signed, unsigned, nullable)
    string   – object / StringDtype
    bool     – bool / nullable BooleanDtype
    datetime – any datetime64 resolution
    timedelta– timedelta64
    category – CategoricalDtype
    complex  – complex64 / complex128
    <dtype>  – unknown types are their own family (no cross-type compat)
    """
    if dtype_str in _NUMERIC_TYPES:
        return "numeric"
    if dtype_str in _STRING_TYPES:
        return "string"
    if dtype_str in _BOOL_TYPES:
        return "bool"
    if dtype_str.startswith("datetime"):
        return "datetime"
    if dtype_str.startswith("timedelta"):
        return "timedelta"
    if dtype_str.startswith("complex"):
        return "complex"
    if dtype_str.startswith("category") or dtype_str == "CategoricalDtype":
        return "category"
    # Fallback: treat the raw dtype string as its own family so unknown
    # types only match themselves.
    return dtype_str


# ---------------------------------------------------------------------------
# Layer 1 class
# ---------------------------------------------------------------------------

class Layer1Structural:
    """
    Layer 1 – Fail-fast structural validation.

    Validates that the SQL output and the Talend reference have a compatible
    *shape* before any data-level comparison is attempted.  Only schema/
    structure concerns live here; data-quality concerns (null ratios,
    distributions) belong in Layers 2–4.

    Result schema
    -------------
    {
        "layer"   : 1,
        "passed"  : bool,
        "warnings": [str, ...],   # non-fatal issues (e.g. column order)
        "checks"  : {
            "1.1_column_naming": {...},
            "1.2_data_types"   : {...},
            "1.3_column_order" : {...},
        }
    }
    """

    def __init__(self, talend_df: pd.DataFrame, sql_df: pd.DataFrame):
        self.talend_df = talend_df
        self.sql_df    = sql_df

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self) -> dict:
        """
        Execute all structural checks in sequence.

        Fail-fast: the first hard failure returns immediately so that
        misleading downstream errors are not reported.
        Check 1.3 (column order) is warning-only and never triggers a
        hard failure.
        """
        print()
        print(STEP_LINE)
        print("  LAYER 1 – Structural Validation")
        print(STEP_LINE)

        results = {
            "layer"   : 1,
            "passed"  : True,
            "warnings": [],
            "checks"  : {},
        }

        t_cols = list(self.talend_df.columns)
        s_cols = list(self.sql_df.columns)

        # ── 1.1 Column naming ────────────────────────────────────────
        ok, payload = self._check_column_naming(t_cols, s_cols)
        results["checks"]["1.1_column_naming"] = payload
        if not ok:
            results["passed"] = False
            logger.error("Layer 1 FAILED at 1.1 – aborting structural checks.")
            return results

        # ── 1.2 Data types ───────────────────────────────────────────
        # Safe to iterate self.talend_df.columns directly: 1.1 guarantees
        # both DataFrames have the same column set.
        ok, payload = self._check_data_types()
        results["checks"]["1.2_data_types"] = payload
        if not ok:
            results["passed"] = False
            logger.error("Layer 1 FAILED at 1.2 – aborting structural checks.")
            return results
        # Surface all-null column warnings at the top level
        results["warnings"].extend(payload.get("warnings", []))

        # ── 1.3 Column order (warning only) ──────────────────────────
        ok, payload, warning = self._check_column_order(t_cols, s_cols)
        results["checks"]["1.3_column_order"] = payload
        if not ok and warning:
            results["warnings"].append(warning)

        return results

    # ------------------------------------------------------------------
    # Check 1.1 – Column naming
    # ------------------------------------------------------------------

    def _check_column_naming(self, t_cols: list, s_cols: list) -> tuple[bool, dict]:
        """
        Verify the two DataFrames expose exactly the same column names.

        Uses set arithmetic so that column *order* does not affect the result
        (order is evaluated separately in 1.3).

        Edge cases handled
        ------------------
        - Duplicate column names in either DataFrame are detected and reported
          before the set comparison, because duplicates collapse in a set and
          would give a false PASS.
        - Case-sensitive comparison (SQL engines are usually case-sensitive).
        """
        # ── Null/NaN column name detection ───────────────────────────
        # NaN column names arise from bad CSV parses (e.g. a trailing
        # comma adds an unnamed column).  NaN != NaN in Python, so they
        # survive set comparison silently and cause KeyErrors downstream.
        t_null_cols = [i for i, c in enumerate(t_cols) if _is_null_name(c)]
        s_null_cols = [i for i, c in enumerate(s_cols) if _is_null_name(c)]

        if t_null_cols or s_null_cols:
            msg = (
                f"NULL/NaN column names detected — "
                f"Talend positions: {t_null_cols or 'none'} | "
                f"SQL positions: {s_null_cols or 'none'}"
            )
            logger.error(f"[FAIL] 1.1 Column naming  {msg}")
            return False, {
                "passed"          : False,
                "null_name_talend": t_null_cols,
                "null_name_sql"   : s_null_cols,
                "missing_in_sql"  : [],
                "extra_in_sql"    : [],
                "common_columns"  : [],
                "error"           : msg,
            }

        t_set = set(t_cols)
        s_set = set(s_cols)

        # ── Duplicate detection ──────────────────────────────────────
        t_dupes = _find_duplicates(t_cols)
        s_dupes = _find_duplicates(s_cols)

        if t_dupes or s_dupes:
            msg = (
                f"Duplicate column names detected – "
                f"Talend: {t_dupes or 'none'} | SQL: {s_dupes or 'none'}"
            )
            logger.error(f"[FAIL] 1.1 Column naming  {msg}")
            return False, {
                "passed"         : False,
                "duplicate_talend": t_dupes,
                "duplicate_sql"   : s_dupes,
                "missing_in_sql"  : [],
                "extra_in_sql"    : [],
                "common_columns"  : [],
                "error"           : msg,
            }

        # ── Set comparison ───────────────────────────────────────────
        missing = sorted(t_set - s_set)   # in Talend but not SQL
        extra   = sorted(s_set - t_set)   # in SQL but not Talend
        common  = sorted(t_set & s_set)
        ok      = not missing and not extra

        self._log(
            "1.1 Column naming", ok,
            f"{len(common)} common | "
            f"missing_in_sql={missing or 'none'} | "
            f"extra_in_sql={extra or 'none'}",
        )

        return ok, {
            "passed"        : ok,
            "missing_in_sql": missing,
            "extra_in_sql"  : extra,
            "common_columns": common,
        }

    # ------------------------------------------------------------------
    # Check 1.2 – Data types
    # ------------------------------------------------------------------

    def _check_data_types(self) -> tuple[bool, dict]:
        """
        Verify dtype compatibility for every column.

        Compatibility rule: two dtypes are compatible when they belong to
        the same *family* (numeric, string, bool, datetime, …).  Exact dtype
        equality is not required — int32 and int64 are both numeric and
        therefore compatible.

        Edge cases handled
        ------------------
        - pandas nullable extension types (Int64, Float32, boolean, string)
          are mapped to the same families as their numpy counterparts.
        - datetime64 with different resolutions (ns, us, ms) are compatible.
        - CategoricalDtype columns: only compatible with other category columns
          (not with plain object/string) because the underlying representation
          and allowed values differ.
        - Unknown/custom dtypes fall back to exact string comparison so they
          never silently pass.
        - All-null columns: pandas infers float64 for a column that is entirely
          NULL (NaN is a float). When one side is all-null and the other is not,
          a dtype mismatch (e.g. float64 vs object) would be a false FAIL.
          We detect this, skip the incompatibility, and record a warning instead
          so the engineer knows to verify that column manually.
        """
        issues   = []
        warnings = []

        for col in self.talend_df.columns:
            td = str(self.talend_df[col].dtype)
            sd = str(self.sql_df[col].dtype)

            if td == sd:
                continue  # identical dtypes are always compatible

            # ── All-null column guard ────────────────────────────────
            # If either side is entirely null pandas reports float64
            # regardless of the intended type. Comparing float64 vs object
            # would be a false FAIL, so we skip the hard check and warn.
            t_all_null = self.talend_df[col].isnull().all()
            s_all_null = self.sql_df[col].isnull().all()

            if t_all_null or s_all_null:
                side = "Talend" if t_all_null else "SQL"
                warnings.append(
                    f"{col}: {side} column is entirely NULL — "
                    f"dtype comparison skipped (Talend={td}, SQL={sd})"
                )
                logger.warning(
                    f"  [WARN] {col:<25} {side} is all-NULL, "
                    f"dtype check skipped (Talend={td}, SQL={sd})"
                )
                continue

            tf = _type_family(td)
            sf = _type_family(sd)

            if tf != sf:
                issues.append({
                    "column"       : col,
                    "talend_type"  : td,
                    "sql_type"     : sd,
                    "talend_family": tf,
                    "sql_family"   : sf,
                })

        ok = len(issues) == 0
        self._log(
            "1.2 Data types", ok,
            "all types compatible" if ok
            else f"{len(issues)} incompatible column(s)",
        )

        if not ok:
            for i in issues:
                logger.warning(
                    f"  {i['column']:<25} "
                    f"Talend={i['talend_type']} ({i['talend_family']})  "
                    f"SQL={i['sql_type']} ({i['sql_family']})"
                )

        return ok, {"passed": ok, "type_issues": issues, "warnings": warnings}

    # ------------------------------------------------------------------
    # Check 1.3 – Column order  (warning only)
    # ------------------------------------------------------------------

    def _check_column_order(
        self, t_cols: list, s_cols: list
    ) -> tuple[bool, dict, str | None]:
        """
        Check whether column order is identical (informational only).

        A mismatch does not fail the pipeline — downstream comparison logic
        aligns columns by name.  The warning is surfaced so engineers are
        aware of the discrepancy between source and target DDL.

        Returns a third element (warning string | None) so the caller can
        append it to results["warnings"] without duplicating logic here.
        """
        ok      = t_cols == s_cols
        warning = None

        self._log(
            "1.3 Column order", ok,
            "same order" if ok
            else "different order (warning only — comparison will proceed)",
        )

        if not ok:
            warning = (
                f"Column order differs: "
                f"Talend={t_cols} | SQL={s_cols}"
            )
            logger.warning(f"  {warning}")

        return ok, {"passed": ok, "talend_order": t_cols, "sql_order": s_cols}, warning

    # ------------------------------------------------------------------
    # Logging helper
    # ------------------------------------------------------------------

    @staticmethod
    def _log(name: str, passed: bool, detail: str = "") -> None:
        if passed:
            logger.success(f"[PASS] {name:<30} {detail}")
        else:
            logger.error(f"[FAIL] {name:<30} {detail}")


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _find_duplicates(cols: list) -> list[str]:
    """Return sorted list of column names that appear more than once."""
    seen  = set()
    dupes = set()
    for c in cols:
        if c in seen:
            dupes.add(c)
        seen.add(c)
    return sorted(dupes)


def _is_null_name(col) -> bool:
    """
    Return True if a column name is None or NaN.

    NaN column names arise from trailing commas in CSV files or malformed
    DDL. They cannot be safely used as dict keys or in set operations
    (NaN != NaN), so they must be caught before any comparison logic runs.
    """
    if col is None:
        return True
    try:
        # float('nan') and numpy NaN both satisfy this check
        return col != col   # NaN is the only value not equal to itself
    except TypeError:
        return False