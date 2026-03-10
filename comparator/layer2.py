"""
comparator/layer2.py
--------------------
LAYER 2 – Data-Level Validation

Checks:
  2.1  Row count
  2.2  Full row hash  (set-based)
  2.3  Column-level hash
  2.4  Targeted row diff  (cell-level)
"""

import hashlib
import pandas as pd
from loguru import logger

from .config import PASS_THRESHOLD, STEP_LINE


class Layer2Data:
    """Row- and column-level data integrity checks."""

    def __init__(self, talend_df: pd.DataFrame, sql_df: pd.DataFrame):
        self.talend_df = talend_df
        self.sql_df    = sql_df

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self) -> dict:
        self._print_section()
        results     = {"layer": 2, "passed": True, "checks": {}}
        common_cols = sorted(set(self.talend_df.columns) & set(self.sql_df.columns))

        # Sort both frames identically for row-order comparisons
        ts = self.talend_df[common_cols].sort_values(by=common_cols).reset_index(drop=True)
        ss = self.sql_df[common_cols].sort_values(by=common_cols).reset_index(drop=True)

        ok, payload = self._check_row_count()
        results["checks"]["2.1_row_count"] = payload
        if not ok:
            results["passed"] = False

        ok, payload, match_rate = self._check_row_hash(ts, ss, common_cols)
        results["checks"]["2.2_row_hash"] = payload
        if not ok:
            results["passed"] = False

        ok, payload, diff_cols = self._check_column_hash(ts, ss, common_cols)
        results["checks"]["2.3_column_hash"] = payload
        if not ok:
            results["passed"] = False

        ok, payload = self._check_targeted_diff(ts, ss, diff_cols)
        results["checks"]["2.4_targeted_diff"] = payload
        if not ok:
            results["passed"] = False

        results["match_rate"] = match_rate
        return results

    # ------------------------------------------------------------------
    # Individual checks
    # ------------------------------------------------------------------

    def _check_row_count(self) -> tuple[bool, dict]:
        nt, ns = len(self.talend_df), len(self.sql_df)
        ok = nt == ns
        self._log("2.1 Row count", ok, f"Talend={nt} | SQL={ns}")
        return ok, {
            "passed": ok, "talend_rows": nt,
            "sql_rows": ns, "diff": abs(nt - ns),
        }

    def _check_row_hash(
        self, ts: pd.DataFrame, ss: pd.DataFrame, common_cols: list
    ) -> tuple[bool, dict, float]:
        th = set(self._row_hashes(ts, common_cols))
        sh = set(self._row_hashes(ss, common_cols))
        matched    = len(th & sh)
        total      = max(len(th), len(sh))
        match_rate = round(matched / total, 4) if total > 0 else 1.0
        ok         = match_rate >= PASS_THRESHOLD
        self._log("2.2 Full row hash", ok,
                  f"match={match_rate:.2%} | matched={matched} | "
                  f"only_talend={len(th - sh)} | only_sql={len(sh - th)}")
        return ok, {
            "passed":          ok,
            "matched_hashes":  matched,
            "only_in_talend":  len(th - sh),
            "only_in_sql":     len(sh - th),
            "match_rate":      match_rate,
        }, match_rate

    def _check_column_hash(
        self, ts: pd.DataFrame, ss: pd.DataFrame, common_cols: list
    ) -> tuple[bool, dict, list]:
        diff_cols = [
            c for c in common_cols
            if self._col_hash(ts[c]) != self._col_hash(ss[c])
        ]
        ok = len(diff_cols) == 0
        self._log("2.3 Column-level hash", ok,
                  "all columns match" if ok
                  else f"{len(diff_cols)} columns differ: {diff_cols}")
        return ok, {
            "passed":            ok,
            "differing_columns": diff_cols,
            "clean_columns":     [c for c in common_cols if c not in diff_cols],
        }, diff_cols

    def _check_targeted_diff(
        self, ts: pd.DataFrame, ss: pd.DataFrame, diff_cols: list
    ) -> tuple[bool, dict]:
        sample_diffs = []
        full_diffs   = []

        if diff_cols and len(ts) == len(ss):
            mask = (ts[diff_cols] != ss[diff_cols]).any(axis=1)
            for idx in mask[mask].index:
                for col in diff_cols:
                    tv, sv = ts.loc[idx, col], ss.loc[idx, col]
                    if str(tv) != str(sv):
                        entry = {
                            "row":          int(idx),
                            "column":       col,
                            "talend_value": str(tv),
                            "sql_value":    str(sv),
                        }
                        full_diffs.append(entry)
                        if len(sample_diffs) < 10:
                            sample_diffs.append(entry)

            if sample_diffs:
                logger.warning("  Sample differences (first 10):")
                for d in sample_diffs:
                    logger.warning(
                        f"  Row {d['row']:>3} | {d['column']:<22} | "
                        f"Talend: {d['talend_value']:<25} | SQL: {d['sql_value']}"
                    )

        ok = len(full_diffs) == 0
        self._log("2.4 Targeted row diff", ok,
                  "no differences" if ok
                  else f"{len(full_diffs)} cell-level diffs across {len(diff_cols)} columns")
        return ok, {
            "passed":       ok,
            "total_diffs":  len(full_diffs),
            "sample_diffs": sample_diffs,
            "full_diffs":   full_diffs,
        }

    # ------------------------------------------------------------------
    # Hashing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _row_hashes(df: pd.DataFrame, cols: list) -> list[str]:
        return df[cols].apply(
            lambda r: hashlib.sha256(
                "|".join(str(v) for v in r).encode()
            ).hexdigest(),
            axis=1,
        ).tolist()

    @staticmethod
    def _col_hash(series: pd.Series) -> str:
        return hashlib.sha256(
            "|".join(sorted(series.astype(str).tolist())).encode()
        ).hexdigest()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _print_section():
        print()
        print(STEP_LINE)
        print("  LAYER 2 – Data-Level Validation")
        print(STEP_LINE)

    @staticmethod
    def _log(name: str, passed: bool, detail: str = ""):
        if passed:
            logger.success(f"[PASS] {name}  {detail}")
        else:
            logger.error(f"[FAIL] {name}  {detail}")