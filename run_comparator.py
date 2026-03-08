"""
run_comparator.py
-----------------
4-layer validation pipeline for Talend-to-SQL migration.

LAYER 1 - Structural Validation   (fail-fast: stops pipeline if failed)
LAYER 2 - Data-Level Validation   (row count, hashing, targeted diff)
LAYER 3 - Business Rule Validation (aggregations, relative tolerance, invariants)
LAYER 4 - Statistical Validation  (distributions, nulls, cardinality, percentiles)

Usage:
    python run_comparator.py

Run AFTER run_executor.py has completed successfully.
"""

import sys
import os
#import hashlib
import pandas as pd
import numpy as np
from datetime import datetime
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from agent.tools.db import load_table_as_dataframe

logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}",
    colorize=True,
    level="DEBUG",
)

TALEND_REFERENCE_PATH = "data/talend_reference.csv"
STAGING_TABLE         = "stg_output"
PASS_THRESHOLD        = 0.95

RELATIVE_TOLERANCE_DEFAULT = 0.01
RELATIVE_TOLERANCE_PER_COL = {
    "net_amount"   : 0.001,
    "gross_amount" : 0.001,
    "tax_rate"     : 0.01,
}

FORBIDDEN_VALUES = {
    "payment_status": ["BLOCKED", "CANCELLED"]
}
AGGREGATION_CHECKS = [
    ("gross_amount", "payment_status", "Total gross per payment status"),
    ("net_amount",   "payment_status", "Total net per payment status"),
    ("gross_amount", "currency",       "Total gross per currency"),
]
EXACT_MATCH_COLS  = ["payment_status", "invoice_id", "vendor_id", "currency"]
NUMERIC_COLS      = ["net_amount", "gross_amount", "tax_rate"]
PERCENTILES       = [0.50, 0.95, 0.99]
NULL_RATIO_THRESHOLD = 0.05

SEPARATOR = "=" * 60
STEP_LINE = "-" * 60


def normalize(df):
    df = df.copy()
    df.columns = df.columns.str.lower().str.strip()
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip()
    for col in df.select_dtypes(include=["int64", "int32"]).columns:
        df[col] = df[col].astype(str).str.strip()
    if "invoice_id" in df.columns:
        df = df.sort_values("invoice_id").reset_index(drop=True)
    return df


def print_section(title):
    print()
    print(STEP_LINE)
    print(f"  {title}")
    print(STEP_LINE)


def log_check(name, passed, detail=""):
    if passed:
        logger.success(f"[PASS] {name}  {detail}")
    else:
        logger.error(f"[FAIL] {name}  {detail}")


def layer1_structural(talend_df, sql_df):
    print_section("LAYER 1 -- Structural Validation")
    results   = {"layer": 1, "passed": True, "checks": {}, "fatal": False}
    t_cols    = list(talend_df.columns)
    s_cols    = list(sql_df.columns)
    t_col_set = set(t_cols)
    s_col_set = set(s_cols)

    # 1.1 Column naming
    missing_in_sql    = sorted(t_col_set - s_col_set)
    extra_in_sql      = sorted(s_col_set - t_col_set)
    col_naming_passed = (t_col_set == s_col_set)
    results["checks"]["1.1_column_naming"] = {
        "passed"         : col_naming_passed,
        "missing_in_sql" : missing_in_sql,
        "extra_in_sql"   : extra_in_sql,
        "common_columns" : sorted(t_col_set & s_col_set),
    }
    log_check("1.1 Column naming", col_naming_passed,
              f"{len(t_col_set & s_col_set)} common | missing={missing_in_sql or 'none'} | extra={extra_in_sql or 'none'}")
    if not col_naming_passed:
        results["passed"] = False
        results["fatal"]  = True
        logger.error("STRUCTURAL FAIL -- stopping pipeline. Fix column names first.")
        return results

    # 1.2 Column count
    col_count_passed = len(t_cols) == len(s_cols)
    results["checks"]["1.2_column_count"] = {
        "passed": col_count_passed, "talend_count": len(t_cols), "sql_count": len(s_cols)
    }
    log_check("1.2 Column count", col_count_passed, f"Talend={len(t_cols)} | SQL={len(s_cols)}")
    if not col_count_passed:
        results["passed"] = False
        results["fatal"]  = True
        logger.error("STRUCTURAL FAIL -- stopping pipeline.")
        return results

    # 1.3 Data types
    type_issues = []
    common_cols = sorted(t_col_set & s_col_set)
    for col in common_cols:
        t_dtype = str(talend_df[col].dtype)
        s_dtype = str(sql_df[col].dtype)
        compatible = (t_dtype == s_dtype or {t_dtype, s_dtype} <= {"object", "int64", "int32"})
        if not compatible:
            type_issues.append({"column": col, "talend_type": t_dtype, "sql_type": s_dtype})
    dtype_passed = len(type_issues) == 0
    results["checks"]["1.3_data_types"] = {"passed": dtype_passed, "type_issues": type_issues}
    log_check("1.3 Data types", dtype_passed,
              f"{len(type_issues)} incompatible columns" if type_issues else "all types compatible")
    if not dtype_passed:
        for issue in type_issues:
            logger.warning(f"  {issue['column']:<20} Talend={issue['talend_type']} | SQL={issue['sql_type']}")
        results["passed"] = False
        results["fatal"]  = True
        logger.error("STRUCTURAL FAIL -- stopping pipeline.")
        return results

    # 1.4 Nullability
    null_issues = []
    for col in common_cols:
        t_null = talend_df[col].isnull().mean()
        s_null = sql_df[col].isnull().mean()
        diff   = abs(t_null - s_null)
        if diff > NULL_RATIO_THRESHOLD:
            null_issues.append({
                "column": col,
                "talend_null_pct": round(t_null * 100, 2),
                "sql_null_pct":    round(s_null * 100, 2),
                "diff_pct":        round(diff   * 100, 2),
            })
    null_passed = len(null_issues) == 0
    results["checks"]["1.4_nullability"] = {"passed": null_passed, "null_issues": null_issues}
    log_check("1.4 Nullability", null_passed,
              f"{len(null_issues)} columns with null ratio diff > {NULL_RATIO_THRESHOLD:.0%}" if null_issues else "null ratios consistent")
    if not null_passed:
        for issue in null_issues:
            logger.warning(f"  {issue['column']:<20} Talend={issue['talend_null_pct']}% | SQL={issue['sql_null_pct']}% | diff={issue['diff_pct']}%")
        results["passed"] = False
        results["fatal"]  = True
        logger.error("STRUCTURAL FAIL -- stopping pipeline.")
        return results

    # 1.5 Column order (warning only)
    order_passed = t_cols == s_cols
    results["checks"]["1.5_column_order"] = {
        "passed": order_passed, "talend_order": t_cols, "sql_order": s_cols
    }
    log_check("1.5 Column order", order_passed,
              "columns in same order" if order_passed else "columns in different order (warning only)")
    if not order_passed:
        logger.warning("  Column order differs but data comparison will proceed.")

    results["passed"] = True
    return results


def _row_hash(df, cols):
    def hash_row(row):
        return hashlib.sha256("|".join(str(v) for v in row).encode()).hexdigest()
    return df[cols].apply(hash_row, axis=1)


def _col_hash(series):
    return hashlib.sha256("|".join(sorted(series.astype(str).tolist())).encode()).hexdigest()


def layer2_data(talend_df, sql_df):
    print_section("LAYER 2 -- Data-Level Validation")
    results     = {"layer": 2, "passed": True, "checks": {}}
    common_cols = sorted(set(talend_df.columns) & set(sql_df.columns))

    # 2.1 Row count
    row_count_passed = len(talend_df) == len(sql_df)
    results["checks"]["2.1_row_count"] = {
        "passed": row_count_passed, "talend_rows": len(talend_df),
        "sql_rows": len(sql_df), "diff": abs(len(talend_df) - len(sql_df))
    }
    log_check("2.1 Row count", row_count_passed, f"Talend={len(talend_df)} | SQL={len(sql_df)}")
    if not row_count_passed:
        results["passed"] = False

    # 2.2 Full row hash
    t_sorted   = talend_df[common_cols].sort_values(by=common_cols).reset_index(drop=True)
    s_sorted   = sql_df[common_cols].sort_values(by=common_cols).reset_index(drop=True)
    t_hashes   = set(_row_hash(t_sorted, common_cols))
    s_hashes   = set(_row_hash(s_sorted, common_cols))
    matched    = len(t_hashes & s_hashes)
    total      = max(len(t_hashes), len(s_hashes))
    match_rate = round(matched / total, 4) if total > 0 else 1.0
    rh_passed  = match_rate >= PASS_THRESHOLD
    results["checks"]["2.2_row_hash"] = {
        "passed": rh_passed, "matched_hashes": matched,
        "only_in_talend": len(t_hashes - s_hashes),
        "only_in_sql": len(s_hashes - t_hashes),
        "match_rate": match_rate,
    }
    log_check("2.2 Full row hash", rh_passed,
              f"match={match_rate:.2%} | matched={matched} | only_talend={len(t_hashes-s_hashes)} | only_sql={len(s_hashes-t_hashes)}")
    if not rh_passed:
        results["passed"] = False

    # 2.3 Column-level hash
    col_hash_issues = [c for c in common_cols if _col_hash(t_sorted[c]) != _col_hash(s_sorted[c])]
    ch_passed = len(col_hash_issues) == 0
    results["checks"]["2.3_column_hash"] = {
        "passed": ch_passed,
        "differing_columns": col_hash_issues,
        "clean_columns": [c for c in common_cols if c not in col_hash_issues],
    }
    log_check("2.3 Column-level hash", ch_passed,
              "all columns match" if ch_passed else f"{len(col_hash_issues)} columns differ: {col_hash_issues}")
    if not ch_passed:
        results["passed"] = False

    # 2.4 Targeted row diff
    sample_diffs = []
    full_diffs   = []
    if not ch_passed and len(t_sorted) == len(s_sorted):
        diff_mask = (t_sorted != s_sorted).any(axis=1)
        for idx in diff_mask[diff_mask].index:
            for col in col_hash_issues:
                tv = t_sorted.loc[idx, col]
                sv = s_sorted.loc[idx, col]
                if str(tv) != str(sv):
                    entry = {"row": int(idx), "column": col, "talend_value": str(tv), "sql_value": str(sv)}
                    full_diffs.append(entry)
                    if len(sample_diffs) < 10:
                        sample_diffs.append(entry)
        if sample_diffs:
            logger.warning("  Sample differences (first 10):")
            for d in sample_diffs:
                logger.warning(f"  Row {d['row']:>3} | {d['column']:<20} | Talend: {d['talend_value']:<25} | SQL: {d['sql_value']}")
    results["checks"]["2.4_targeted_diff"] = {
        "passed": len(full_diffs) == 0, "total_diffs": len(full_diffs),
        "sample_diffs": sample_diffs, "full_diffs": full_diffs,
    }
    log_check("2.4 Targeted row diff", len(full_diffs) == 0,
              "no differences" if not full_diffs else f"{len(full_diffs)} cell-level diffs across {len(col_hash_issues)} columns")

    results["match_rate"] = match_rate
    return results


def _relative_tolerance_check(t_series, s_series, col):
    tol   = RELATIVE_TOLERANCE_PER_COL.get(col, RELATIVE_TOLERANCE_DEFAULT)
    diffs = []
    for i, (tv, sv) in enumerate(zip(t_series, s_series)):
        try:
            tv_f, sv_f = float(tv), float(sv)
        except (ValueError, TypeError):
            continue
        rel_diff = abs(tv_f - sv_f) / abs(tv_f) if tv_f != 0 else abs(sv_f)
        if rel_diff > tol:
            diffs.append({
                "row": i, "talend_value": round(tv_f, 6), "sql_value": round(sv_f, 6),
                "rel_diff_pct": round(rel_diff * 100, 4), "tolerance_pct": round(tol * 100, 4),
            })
    return {"passed": len(diffs) == 0, "tolerance": f"{tol:.1%}", "mismatches": len(diffs), "sample": diffs[:5]}


def layer3_business_rules(talend_df, sql_df):
    print_section("LAYER 3 -- Business Rule Validation")
    results  = {"layer": 3, "passed": True, "checks": {}}
    t_sorted = talend_df.sort_values("invoice_id").reset_index(drop=True) if "invoice_id" in talend_df.columns else talend_df.copy()
    s_sorted = sql_df.sort_values("invoice_id").reset_index(drop=True) if "invoice_id" in sql_df.columns else sql_df.copy()

    # 3.1 Forbidden values
    forbidden_results = {}
    for col, forbidden_vals in FORBIDDEN_VALUES.items():
        if col not in sql_df.columns:
            forbidden_results[col] = {"passed": False, "reason": f"Column '{col}' not found", "count": 0}
            continue
        found = sql_df[sql_df[col].isin(forbidden_vals)]
        forbidden_results[col] = {
            "passed": len(found) == 0, "count": len(found),
            "reason": "No forbidden values" if len(found) == 0 else f"{len(found)} rows with forbidden values",
            "sample": found[col].value_counts().to_dict() if len(found) > 0 else {},
        }
        log_check(f"3.1 Forbidden values in '{col}'", forbidden_results[col]["passed"], forbidden_results[col]["reason"])
        if not forbidden_results[col]["passed"]:
            results["passed"] = False
    results["checks"]["3.1_forbidden_values"] = forbidden_results

    # 3.2 Exact match
    exact_results = {}
    for col in EXACT_MATCH_COLS:
        if col not in talend_df.columns or col not in sql_df.columns:
            exact_results[col] = {"passed": False, "mismatches": 0, "reason": f"Column '{col}' missing"}
            log_check(f"3.2 Exact match '{col}'", False, "column missing")
            results["passed"] = False
            continue
        t_vals     = t_sorted[col].reset_index(drop=True)
        s_vals     = s_sorted[col].reset_index(drop=True)
        min_len    = min(len(t_vals), len(s_vals))
        mismatches = int((t_vals[:min_len] != s_vals[:min_len]).sum())
        exact_results[col] = {
            "passed": mismatches == 0, "mismatches": mismatches,
            "reason": "All values match" if mismatches == 0 else f"{mismatches} values differ",
        }
        log_check(f"3.2 Exact match '{col}'", exact_results[col]["passed"], exact_results[col]["reason"])
        if not exact_results[col]["passed"]:
            results["passed"] = False
    results["checks"]["3.2_exact_match"] = exact_results

    # 3.3 Relative tolerance
    tolerance_results = {}
    for col in NUMERIC_COLS:
        if col not in talend_df.columns or col not in sql_df.columns:
            tolerance_results[col] = {"passed": False, "reason": f"Column '{col}' missing", "mismatches": 0, "talend_sample": [], "sql_sample": []}
            continue
        result = _relative_tolerance_check(t_sorted[col], s_sorted[col], col)
        tolerance_results[col] = {**result, "talend_sample": [], "sql_sample": []}
        log_check(f"3.3 Relative tolerance '{col}' (tol={result['tolerance']})", result["passed"],
                  "all within tolerance" if result["passed"] else f"{result['mismatches']} values exceed tolerance")
        if result["sample"] and not result["passed"]:
            for s in result["sample"]:
                logger.warning(f"  Row {s['row']:>3} | Talend={s['talend_value']} | SQL={s['sql_value']} | rel_diff={s['rel_diff_pct']}%")
        if not result["passed"]:
            results["passed"] = False
    results["checks"]["3.3_relative_tolerance"] = tolerance_results

    # 3.4 Aggregation validation
    agg_results = []
    for agg_col, group_col, label in AGGREGATION_CHECKS:
        if not all(c in talend_df.columns for c in [agg_col, group_col]):
            continue
        if not all(c in sql_df.columns for c in [agg_col, group_col]):
            continue
        def to_float(s):
            try: return s.astype(float)
            except: return s
        t_agg = talend_df.copy().assign(**{agg_col: to_float(talend_df[agg_col])}).groupby(group_col)[agg_col].agg(["sum","count","mean"]).round(2).sort_index()
        s_agg = sql_df.copy().assign(**{agg_col: to_float(sql_df[agg_col])}).groupby(group_col)[agg_col].agg(["sum","count","mean"]).round(2).sort_index()
        groups_match = True
        issues = []
        for grp in t_agg.index:
            if grp not in s_agg.index:
                issues.append(f"Group '{grp}' missing in SQL")
                groups_match = False
                continue
            for metric in ["sum","count","mean"]:
                tv = t_agg.loc[grp, metric]
                sv = s_agg.loc[grp, metric]
                rd = abs(tv-sv)/abs(tv) if tv != 0 else abs(sv)
                if rd > RELATIVE_TOLERANCE_DEFAULT:
                    issues.append(f"Group='{grp}' {metric}: Talend={tv} | SQL={sv} | rel_diff={rd:.2%}")
                    groups_match = False
        agg_results.append({"label": label, "agg_col": agg_col, "group_col": group_col, "passed": groups_match, "issues": issues})
        log_check(f"3.4 Aggregation: {label}", groups_match, "all groups match" if groups_match else f"{len(issues)} issues")
        if not groups_match:
            for issue in issues:
                logger.warning(f"  {issue}")
            results["passed"] = False
    results["checks"]["3.4_aggregations"] = agg_results

    # 3.5 Referential integrity
    ref_results = {}
    if "vendor_id" in talend_df.columns and "vendor_id" in sql_df.columns:
        t_v = set(talend_df["vendor_id"].astype(str).unique())
        s_v = set(sql_df["vendor_id"].astype(str).unique())
        orphans = s_v - t_v
        ref_passed = len(orphans) == 0
        ref_results["vendor_id"] = {
            "passed": ref_passed,
            "orphan_in_sql":  sorted(orphans),
            "missing_in_sql": sorted(t_v - s_v),
        }
        log_check("3.5 Referential integrity 'vendor_id'", ref_passed,
                  "all vendor_ids valid" if ref_passed else f"{len(orphans)} unknown vendor_ids in SQL")
        if not ref_passed:
            logger.warning(f"  Unknown: {sorted(orphans)}")
            results["passed"] = False
    results["checks"]["3.5_referential_integrity"] = ref_results
    return results


def layer4_statistical(talend_df, sql_df):
    print_section("LAYER 4 -- Statistical Validation")
    results     = {"layer": 4, "passed": True, "checks": {}}
    common_cols = sorted(set(talend_df.columns) & set(sql_df.columns))
    stat_issues = []
    null_issues = []
    card_issues = []
    all_stats   = {}

    for col in common_cols:
        t_col      = talend_df[col]
        s_col      = sql_df[col]
        is_numeric = col in NUMERIC_COLS
        col_stats  = {"column": col, "numeric": is_numeric}

        t_null = round(t_col.isnull().mean() * 100, 2)
        s_null = round(s_col.isnull().mean() * 100, 2)
        diff   = abs(t_null - s_null)
        col_stats["null_ratio"] = {"talend": t_null, "sql": s_null, "diff": round(diff, 2)}
        if diff > NULL_RATIO_THRESHOLD * 100:
            null_issues.append({"column": col, "talend_pct": t_null, "sql_pct": s_null, "diff_pct": round(diff, 2)})

        t_card = t_col.nunique()
        s_card = s_col.nunique()
        col_stats["cardinality"] = {"talend": t_card, "sql": s_card, "diff": abs(t_card - s_card)}
        if t_card != s_card:
            card_issues.append({"column": col, "talend": t_card, "sql": s_card, "diff": abs(t_card-s_card)})

        if is_numeric:
            try:
                t_num = pd.to_numeric(t_col, errors="coerce").dropna()
                s_num = pd.to_numeric(s_col, errors="coerce").dropna()
                tol   = RELATIVE_TOLERANCE_PER_COL.get(col, RELATIVE_TOLERANCE_DEFAULT)
                stats_vals = {
                    "min":  (round(float(t_num.min()),4),  round(float(s_num.min()),4)),
                    "max":  (round(float(t_num.max()),4),  round(float(s_num.max()),4)),
                    "mean": (round(float(t_num.mean()),4), round(float(s_num.mean()),4)),
                    "std":  (round(float(t_num.std()),4),  round(float(s_num.std()),4)),
                }
                col_stats["stats"] = {k: {"talend": v[0], "sql": v[1]} for k,v in stats_vals.items()}
                pct_stats = {}
                for p in PERCENTILES:
                    t_p = round(float(t_num.quantile(p)), 4)
                    s_p = round(float(s_num.quantile(p)), 4)
                    pct_stats[f"P{int(p*100)}"] = {"talend": t_p, "sql": s_p}
                col_stats["percentiles"] = pct_stats
                for sname, (tv, sv) in stats_vals.items():
                    rd = abs(tv-sv)/abs(tv) if tv != 0 else abs(sv)
                    if rd > tol:
                        stat_issues.append({"column": col, "stat": sname, "talend": tv, "sql": sv,
                                            "rel_diff": round(rd*100,4), "tol_pct": round(tol*100,2)})
            except Exception as e:
                col_stats["stats_error"] = str(e)
        all_stats[col] = col_stats

    stat_passed = len(stat_issues) == 0
    results["checks"]["4.1_4.2_distributions"] = {"passed": stat_passed, "issues": stat_issues, "all_stats": all_stats}
    log_check("4.1/4.2 Distributions (min/max/mean/std)", stat_passed,
              "all within tolerance" if stat_passed else f"{len(stat_issues)} distribution issues")
    if not stat_passed:
        for issue in stat_issues[:5]:
            logger.warning(f"  {issue['column']:<20} {issue['stat']:<6} Talend={issue['talend']} | SQL={issue['sql']} | rel_diff={issue['rel_diff']}%")
        results["passed"] = False

    null_passed = len(null_issues) == 0
    results["checks"]["4.3_null_ratio"] = {"passed": null_passed, "issues": null_issues}
    log_check("4.3 Null ratio", null_passed, "all columns consistent" if null_passed else f"{len(null_issues)} columns with null ratio diff")
    if not null_passed:
        results["passed"] = False

    card_passed = len(card_issues) == 0
    results["checks"]["4.4_cardinality"] = {"passed": card_passed, "issues": card_issues}
    log_check("4.4 Cardinality", card_passed,
              "all columns match" if card_passed else f"{len(card_issues)} columns with different unique counts")
    if not card_passed:
        for issue in card_issues:
            logger.warning(f"  {issue['column']:<20} Talend={issue['talend']} unique | SQL={issue['sql']} unique")
        results["passed"] = False

    pct_data = {col: s.get("percentiles", {}) for col, s in all_stats.items() if "percentiles" in s}
    results["checks"]["4.5_percentiles"] = {"data": pct_data}
    if pct_data:
        logger.info("4.5 Percentiles (informational):")
        for col, pcts in pct_data.items():
            for pname, vals in pcts.items():
                marker = "OK" if vals["talend"] == vals["sql"] else "!!"
                logger.info(f"  [{marker}] {col:<20} {pname}: Talend={vals['talend']} | SQL={vals['sql']}")

    results["all_col_stats"] = all_stats
    return results


def compute_final_decision(l1, l2, l3, l4):
    reasons = []
    if not l1["passed"]:
        reasons.append("L1: Structural mismatch (fatal)")
    if l2 and not l2["passed"]:
        reasons.append(f"L2: Data mismatch (match_rate={l2.get('match_rate',0):.2%})")
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
    logger.info(f"Layers run     : "
                f"L1={'OK' if l1['passed'] else 'FAIL'} | "
                f"L2={'OK' if l2 and l2['passed'] else ('FAIL' if l2 else 'SKIPPED')} | "
                f"L3={'OK' if l3 and l3['passed'] else ('FAIL' if l3 else 'SKIPPED')} | "
                f"L4={'OK' if l4 and l4['passed'] else ('FAIL' if l4 else 'SKIPPED')}")
    print(SEPARATOR + "\n")


def _build_output(decision, reasons, talend_df, sql_df, l1, l2, l3, l4):
    l2c = l2["checks"] if l2 else {}
    l3c = l3["checks"] if l3 else {}
    diff_data    = l2c.get("2.4_targeted_diff", {})
    match_rate   = l2.get("match_rate", 0.0) if l2 else 0.0
    return {
        "decision": decision, "match_rate": match_rate, "threshold": PASS_THRESHOLD,
        "talend_rows": len(talend_df), "sql_rows": len(sql_df),
        "run_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "talend_path": TALEND_REFERENCE_PATH, "sql_table": STAGING_TABLE,
        "fail_reasons": reasons,
        "structure": {
            "talend_row_count": len(talend_df), "sql_row_count": len(sql_df),
            "row_count_match": l2c.get("2.1_row_count", {}).get("passed", False) if l2 else False,
            "column_match": l1["checks"].get("1.1_column_naming", {}).get("passed", False),
            "columns_only_in_talend": l1["checks"].get("1.1_column_naming", {}).get("missing_in_sql", []),
            "columns_only_in_sql":    l1["checks"].get("1.1_column_naming", {}).get("extra_in_sql", []),
            "common_columns":         l1["checks"].get("1.1_column_naming", {}).get("common_columns", []),
        },
        "forbidden": {
            col: {"passed": r.get("passed",False), "reason": r.get("reason",""), "forbidden_count": r.get("count",0)}
            for col, r in l3c.get("3.1_forbidden_values", {}).items()
        } if l3 else {},
        "exact": l3c.get("3.2_exact_match", {}) if l3 else {},
        "rounding": {
            col: {"passed": r.get("passed",False), "mismatches": r.get("mismatches",0),
                  "reason": f"Relative tolerance {r.get('tolerance','N/A')}",
                  "talend_sample": [], "sql_sample": []}
            for col, r in l3c.get("3.3_relative_tolerance", {}).items()
        } if l3 else {},
        "row_level": {
            "total_rows": max(len(talend_df), len(sql_df)),
            "matched_rows": l2c.get("2.2_row_hash", {}).get("matched_hashes", 0) if l2 else 0,
            "diff_rows": l2c.get("2.2_row_hash", {}).get("only_in_talend", 0) if l2 else 0,
            "match_rate": match_rate,
            "sample_diffs": diff_data.get("sample_diffs", []),
            "full_diffs":   diff_data.get("full_diffs", []),
        },
        "layers": {"layer1": l1, "layer2": l2, "layer3": l3, "layer4": l4},
    }


def run_comparison():
    print("\n" + SEPARATOR)
    print("  VALIDATION AGENT - 4-LAYER COMPARATOR")
    print(SEPARATOR + "\n")

    logger.info("Loading datasets...")
    talend_df = pd.read_csv(TALEND_REFERENCE_PATH, encoding="utf-8")
    sql_df    = load_table_as_dataframe(STAGING_TABLE)
    logger.info(f"  Talend : {len(talend_df)} rows x {len(talend_df.columns)} cols")
    logger.info(f"  SQL    : {len(sql_df)} rows x {len(sql_df.columns)} cols")

    talend_norm = normalize(talend_df)
    sql_norm    = normalize(sql_df)

    l1 = layer1_structural(talend_norm, sql_norm)
    if l1.get("fatal"):
        decision = "FAIL"
        reasons  = ["L1: Structural mismatch -- pipeline stopped"]
        _print_final(decision, reasons, l1, None, None, None)
        return _build_output(decision, reasons, talend_df, sql_df, l1, None, None, None)

    l2 = layer2_data(talend_norm, sql_norm)
    l3 = layer3_business_rules(talend_norm, sql_norm)
    l4 = layer4_statistical(talend_norm, sql_norm)

    decision, reasons = compute_final_decision(l1, l2, l3, l4)
    _print_final(decision, reasons, l1, l2, l3, l4)
    return _build_output(decision, reasons, talend_df, sql_df, l1, l2, l3, l4)


if __name__ == "__main__":
    try:
        results = run_comparison()
        sys.exit(0 if results["decision"] == "PASS" else 1)
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        logger.error("Make sure run_executor.py ran successfully first.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise