"""
comparator/layer4.py
--------------------
LAYER 4 – Statistical Validation

Checks:
  4.1/4.2  Distributions – min, max, mean, std (within relative tolerance)
  4.3      Null ratio consistency
  4.4      Cardinality match
  4.5      Percentiles  (P50, P95, P99 – informational)
"""

import pandas as pd
from loguru import logger

from .config import NULL_RATIO_THRESHOLD, PERCENTILES, RELATIVE_TOLERANCE, STEP_LINE


class Layer4Statistical:
    """Distribution, null-ratio, cardinality, and percentile checks."""

    def __init__(
        self,
        talend_df: pd.DataFrame,
        sql_df:    pd.DataFrame,
        schema:    dict,
    ):
        self.talend_df   = talend_df
        self.sql_df      = sql_df
        self.schema      = schema
        self.common_cols = sorted(
            set(talend_df.columns) & set(sql_df.columns)
        )

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self) -> dict:
        self._print_section()
        results     = {"layer": 4, "passed": True, "checks": {}}
        all_stats   = self._build_all_stats()

        stat_issues = self._collect_stat_issues(all_stats)
        null_issues = self._collect_null_issues()
        card_issues = self._collect_card_issues()

        ok = len(stat_issues) == 0
        results["checks"]["4.1_4.2_distributions"] = {
            "passed": ok, "issues": stat_issues, "all_stats": all_stats
        }
        self._log("4.1/4.2 Distributions (min/max/mean/std)", ok,
                  "all within tolerance" if ok
                  else f"{len(stat_issues)} distribution issues")
        if not ok:
            for i in stat_issues[:5]:
                logger.warning(
                    f"  {i['column']:<22} {i['stat']:<6} "
                    f"Talend={i['talend']} | SQL={i['sql']} | "
                    f"rel_diff={i['rel_diff']}%"
                )
            results["passed"] = False

        ok = len(null_issues) == 0
        results["checks"]["4.3_null_ratio"] = {"passed": ok, "issues": null_issues}
        self._log("4.3 Null ratio", ok,
                  "all consistent" if ok
                  else f"{len(null_issues)} columns with null ratio diff")
        if not ok:
            results["passed"] = False

        ok = len(card_issues) == 0
        results["checks"]["4.4_cardinality"] = {"passed": ok, "issues": card_issues}
        self._log("4.4 Cardinality", ok,
                  "all match" if ok
                  else f"{len(card_issues)} columns with different unique counts")
        if not ok:
            for i in card_issues:
                logger.warning(
                    f"  {i['column']:<22} "
                    f"Talend={i['talend']} unique | SQL={i['sql']} unique"
                )
            results["passed"] = False

        pct_data = {
            col: s["percentiles"]
            for col, s in all_stats.items()
            if "percentiles" in s
        }
        results["checks"]["4.5_percentiles"] = {"data": pct_data}
        self._print_percentiles(pct_data)

        results["all_col_stats"] = all_stats
        return results

    # ------------------------------------------------------------------
    # Per-column statistics builder
    # ------------------------------------------------------------------

    def _build_all_stats(self) -> dict:
        numeric_cols = self.schema["numeric_cols"]
        all_stats    = {}

        for col in self.common_cols:
            tc     = self.talend_df[col]
            sc     = self.sql_df[col]
            is_num = col in numeric_cols
            cstats = {"column": col, "numeric": is_num}

            tn = round(tc.isnull().mean() * 100, 2)
            sn = round(sc.isnull().mean() * 100, 2)
            cstats["null_ratio"] = {"talend": tn, "sql": sn, "diff": round(abs(tn - sn), 2)}

            tc_n = tc.nunique()
            sc_n = sc.nunique()
            cstats["cardinality"] = {"talend": tc_n, "sql": sc_n, "diff": abs(tc_n - sc_n)}

            if is_num:
                cstats = self._add_numeric_stats(cstats, tc, sc)

            all_stats[col] = cstats

        return all_stats

    @staticmethod
    def _add_numeric_stats(cstats: dict, tc: pd.Series, sc: pd.Series) -> dict:
        try:
            tn_num = pd.to_numeric(tc, errors="coerce").dropna()
            sn_num = pd.to_numeric(sc, errors="coerce").dropna()

            sv = {
                "min":  (round(float(tn_num.min()),  4), round(float(sn_num.min()),  4)),
                "max":  (round(float(tn_num.max()),  4), round(float(sn_num.max()),  4)),
                "mean": (round(float(tn_num.mean()), 4), round(float(sn_num.mean()), 4)),
                "std":  (round(float(tn_num.std()),  4), round(float(sn_num.std()),  4)),
            }
            cstats["stats"] = {k: {"talend": v[0], "sql": v[1]} for k, v in sv.items()}
            cstats["percentiles"] = {
                f"P{int(p * 100)}": {
                    "talend": round(float(tn_num.quantile(p)), 4),
                    "sql":    round(float(sn_num.quantile(p)), 4),
                }
                for p in PERCENTILES
            }
        except Exception as exc:
            cstats["stats_error"] = str(exc)
        return cstats

    # ------------------------------------------------------------------
    # Issue collectors
    # ------------------------------------------------------------------

    def _collect_stat_issues(self, all_stats: dict) -> list:
        issues = []
        for col, cstats in all_stats.items():
            for sname, vals in cstats.get("stats", {}).items():
                tv, sv = vals["talend"], vals["sql"]
                rd = abs(tv - sv) / abs(tv) if tv != 0 else abs(sv)
                if rd > RELATIVE_TOLERANCE:
                    issues.append({
                        "column":  col,
                        "stat":    sname,
                        "talend":  tv,
                        "sql":     sv,
                        "rel_diff": round(rd * 100, 4),
                        "tol_pct": round(RELATIVE_TOLERANCE * 100, 2),
                    })
        return issues

    def _collect_null_issues(self) -> list:
        issues = []
        for col in self.common_cols:
            tn = self.talend_df[col].isnull().mean()
            sn = self.sql_df[col].isnull().mean()
            if abs(tn - sn) > NULL_RATIO_THRESHOLD:
                issues.append({
                    "column":      col,
                    "talend_pct":  round(tn * 100, 2),
                    "sql_pct":     round(sn * 100, 2),
                    "diff_pct":    round(abs(tn - sn) * 100, 2),
                })
        return issues

    def _collect_card_issues(self) -> list:
        issues = []
        for col in self.common_cols:
            tc_n = self.talend_df[col].nunique()
            sc_n = self.sql_df[col].nunique()
            if tc_n != sc_n:
                issues.append({
                    "column": col,
                    "talend": tc_n,
                    "sql":    sc_n,
                    "diff":   abs(tc_n - sc_n),
                })
        return issues

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _print_percentiles(pct_data: dict):
        if not pct_data:
            return
        logger.info("4.5 Percentiles (informational):")
        for col, pcts in pct_data.items():
            for pname, vals in pcts.items():
                marker = "OK" if vals["talend"] == vals["sql"] else "!!"
                logger.info(
                    f"  [{marker}] {col:<22} {pname}: "
                    f"Talend={vals['talend']} | SQL={vals['sql']}"
                )

    @staticmethod
    def _print_section():
        print()
        print(STEP_LINE)
        print("  LAYER 4 – Statistical Validation")
        print(STEP_LINE)

    @staticmethod
    def _log(name: str, passed: bool, detail: str = ""):
        if passed:
            logger.success(f"[PASS] {name}  {detail}")
        else:
            logger.error(f"[FAIL] {name}  {detail}")