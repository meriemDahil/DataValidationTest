"""
comparator/layer3.py
--------------------
LAYER 3 – Business Rule Validation  (fully inferred)

Checks:
  3.2  Exact match on inferred categorical columns
  3.3  Relative tolerance on all inferred numeric columns
  3.4  Aggregation: every (numeric x categorical) pair, capped at MAX_AGG_PAIRS
  3.5  Referential integrity: SQL must not introduce values absent from Talend

"""

import pandas as pd
from loguru import logger

from .config import MAX_AGG_PAIRS, RELATIVE_TOLERANCE, STEP_LINE


class Layer3BusinessRules:
    """
    Fully-inferred business rule checks.
    All targets (numeric cols, categorical cols, sort key) come from
    the schema dict produced by ``SchemaInferrer``.
    """

    def __init__(
        self,
        talend_df: pd.DataFrame,
        sql_df:    pd.DataFrame,
        schema:    dict,
    ):
        self.talend_df = talend_df
        self.sql_df    = sql_df
        self.schema    = schema

        sort_key = schema["sort_key"]
        self.t_sorted = (
            talend_df.sort_values(sort_key).reset_index(drop=True)
            if sort_key else talend_df.copy()
        )
        self.s_sorted = (
            sql_df.sort_values(sort_key).reset_index(drop=True)
            if sort_key else sql_df.copy()
        )

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self) -> dict:
        self._print_section()
        results = {"layer": 3, "passed": True, "checks": {}}

        logger.info(f"  Inferred numeric cols     : {self.schema['numeric_cols']}")
        logger.info(f"  Inferred categorical cols : {self.schema['categorical_cols']}")
        logger.info(f"  Sort key                  : {self.schema['sort_key']}")

        ok, payload = self._check_exact_match()
        results["checks"]["3.2_exact_match"] = payload
        if not ok:
            results["passed"] = False

        ok, payload = self._check_relative_tolerance()
        results["checks"]["3.3_relative_tolerance"] = payload
        if not ok:
            results["passed"] = False

        ok, payload = self._check_aggregations()
        results["checks"]["3.4_aggregations"] = payload
        if not ok:
            results["passed"] = False

        ok, payload = self._check_referential_integrity()
        results["checks"]["3.5_referential_integrity"] = payload
        if not ok:
            results["passed"] = False

        return results

    # ------------------------------------------------------------------
    # 3.2 – Exact match on categorical columns
    # ------------------------------------------------------------------

    def _check_exact_match(self) -> tuple[bool, dict]:
        cat_cols    = self.schema["categorical_cols"]
        layer_ok    = True
        col_results = {}

        for col in cat_cols:
            if col not in self.talend_df.columns or col not in self.sql_df.columns:
                continue
            tv = self.t_sorted[col].reset_index(drop=True)
            sv = self.s_sorted[col].reset_index(drop=True)
            mn = min(len(tv), len(sv))
            mm = int((tv[:mn] != sv[:mn]).sum())
            passed = mm == 0
            reason = "All values match" if passed else f"{mm} values differ"
            col_results[col] = {"passed": passed, "mismatches": mm, "reason": reason}
            self._log(f"3.2 Exact match '{col}'", passed, reason)
            if not passed:
                layer_ok = False

        return layer_ok, col_results

    # ------------------------------------------------------------------
    # 3.3 – Relative tolerance on numeric columns
    # ------------------------------------------------------------------

    def _check_relative_tolerance(self) -> tuple[bool, dict]:
        numeric_cols = self.schema["numeric_cols"]
        layer_ok     = True
        tol_results  = {}

        for col in numeric_cols:
            if col not in self.talend_df.columns or col not in self.sql_df.columns:
                continue
            diffs = []
            for i, (tv, sv) in enumerate(zip(self.t_sorted[col], self.s_sorted[col])):
                try:
                    tvf, svf = float(tv), float(sv)
                except (ValueError, TypeError):
                    continue
                rd = abs(tvf - svf) / abs(tvf) if tvf != 0 else abs(svf)
                if rd > RELATIVE_TOLERANCE:
                    diffs.append({
                        "row":           i,
                        "talend_value":  round(tvf, 6),
                        "sql_value":     round(svf, 6),
                        "rel_diff_pct":  round(rd * 100, 4),
                        "tolerance_pct": round(RELATIVE_TOLERANCE * 100, 2),
                    })

            ok = len(diffs) == 0
            tol_results[col] = {
                "passed":        ok,
                "tolerance":     f"{RELATIVE_TOLERANCE:.1%}",
                "mismatches":    len(diffs),
                "sample":        diffs[:5],
                "talend_sample": [],
                "sql_sample":    [],
            }
            self._log(
                f"3.3 Relative tolerance '{col}' (tol={RELATIVE_TOLERANCE:.1%})", ok,
                "all within tolerance" if ok else f"{len(diffs)} values exceed tolerance",
            )
            if not ok:
                for d in diffs[:3]:
                    logger.warning(
                        f"  Row {d['row']:>3} | "
                        f"Talend={d['talend_value']} | "
                        f"SQL={d['sql_value']} | "
                        f"rel_diff={d['rel_diff_pct']}%"
                    )
                layer_ok = False

        return layer_ok, tol_results

    # ------------------------------------------------------------------
    # 3.4 – Aggregation validation
    # ------------------------------------------------------------------

    def _check_aggregations(self) -> tuple[bool, dict]:
        numeric_cols = self.schema["numeric_cols"]
        cat_cols     = self.schema["categorical_cols"]
        layer_ok     = True
        agg_results  = []
        pairs_run    = 0

        for agg_col in numeric_cols:
            for grp_col in cat_cols:
                if not self._col_present(agg_col, grp_col):
                    continue
                if pairs_run >= MAX_AGG_PAIRS:
                    break

                t_agg = self._group_agg(self.talend_df, agg_col, grp_col)
                s_agg = self._group_agg(self.sql_df,    agg_col, grp_col)

                ok, issues = self._compare_agg(t_agg, s_agg)
                label = f"{agg_col} by {grp_col}"
                agg_results.append({
                    "label":     label,
                    "agg_col":   agg_col,
                    "group_col": grp_col,
                    "passed":    ok,
                    "issues":    issues,
                })
                self._log(
                    f"3.4 Aggregation: {label}", ok,
                    "all groups match" if ok else f"{len(issues)} issues",
                )
                for issue in issues:
                    logger.warning(f"  {issue}")
                if not ok:
                    layer_ok = False
                pairs_run += 1

            if pairs_run >= MAX_AGG_PAIRS:
                break

        if pairs_run == 0:
            logger.info("3.4 Aggregation: no (numeric x categorical) pairs found – skipped")
        elif pairs_run >= MAX_AGG_PAIRS:
            logger.info(f"3.4 Aggregation: capped at {MAX_AGG_PAIRS} pairs")

        return layer_ok, agg_results

    def _col_present(self, agg_col: str, grp_col: str) -> bool:
        return (
            agg_col in self.talend_df.columns and grp_col in self.talend_df.columns
            and agg_col in self.sql_df.columns and grp_col in self.sql_df.columns
        )

    @staticmethod
    def _to_float(series: pd.Series) -> pd.Series:
        try:
            return series.astype(float)
        except Exception:
            return series

    def _group_agg(self, df: pd.DataFrame, agg_col: str, grp_col: str) -> pd.DataFrame:
        return (
            df.assign(**{agg_col: self._to_float(df[agg_col])})
            .groupby(grp_col)[agg_col]
            .agg(["sum", "count", "mean"])
            .round(4)
            .sort_index()
        )

    @staticmethod
    def _compare_agg(
        t_agg: pd.DataFrame, s_agg: pd.DataFrame
    ) -> tuple[bool, list]:
        ok, issues = True, []
        for grp in t_agg.index:
            if grp not in s_agg.index:
                issues.append(f"Group '{grp}' missing in SQL")
                ok = False
                continue
            for metric in ["sum", "count", "mean"]:
                tv = t_agg.loc[grp, metric]
                sv = s_agg.loc[grp, metric]
                rd = abs(tv - sv) / abs(tv) if tv != 0 else abs(sv)
                if rd > RELATIVE_TOLERANCE:
                    issues.append(
                        f"Group='{grp}' {metric}: "
                        f"Talend={tv} | SQL={sv} | rel_diff={rd:.2%}"
                    )
                    ok = False
        return ok, issues

    # ------------------------------------------------------------------
    # 3.5 – Referential integrity
    # ------------------------------------------------------------------

    def _check_referential_integrity(self) -> tuple[bool, dict]:
        common_cols = sorted(set(self.talend_df.columns) & set(self.sql_df.columns))
        layer_ok    = True
        ref_results = {}

        for col in common_cols:
            t_vals  = set(self.talend_df[col].astype(str).unique())
            s_vals  = set(self.sql_df[col].astype(str).unique())
            orphans = s_vals - t_vals
            if orphans:
                ref_results[col] = {
                    "passed":          False,
                    "orphan_in_sql":   sorted(orphans),
                    "missing_in_sql":  sorted(t_vals - s_vals),
                }
                self._log(
                    f"3.5 Referential integrity '{col}'", False,
                    f"{len(orphans)} values in SQL not present in Talend: "
                    f"{sorted(orphans)[:5]}",
                )
                layer_ok = False

        if not ref_results:
            logger.success(
                "[PASS] 3.5 Referential integrity  all columns – no orphan values in SQL"
            )

        return layer_ok, ref_results

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _print_section():
        print()
        print(STEP_LINE)
        print("  LAYER 3 – Business Rule Validation  [fully inferred]")
        print(STEP_LINE)

    @staticmethod
    def _log(name: str, passed: bool, detail: str = ""):
        if passed:
            logger.success(f"[PASS] {name}  {detail}")
        else:
            logger.error(f"[FAIL] {name}  {detail}")