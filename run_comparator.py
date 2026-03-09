"""
run_comparator.py
-----------------
4-layer validation pipeline for Talend-to-SQL migration.

FULLY GENERIC -- zero hardcoded column names, business rules, or domain logic.
All check targets (numeric cols, categorical cols, sort key, aggregation pairs)
are inferred automatically from the data itself.

LAYER 1 - Structural     (fail-fast: stops pipeline on any failure)
LAYER 2 - Data-Level     (row count, SHA-256 hashing, targeted diff)
LAYER 3 - Business Rules (inferred: aggregations, relative tolerance, referential integrity)
LAYER 4 - Statistical    (distributions, nulls, cardinality, percentiles)

Usage:
    python run_comparator.py
"""

import sys
import os
import hashlib
import pandas as pd
import numpy as np
from datetime import datetime
from loguru import logger
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from agent.tools.db import load_table_as_dataframe

logger.remove()
logger.add(sys.stdout,
    format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}",
    colorize=True, level="DEBUG")

# ------------------------------------------------------------------
# Configuration -- NO column names here, only thresholds
# ------------------------------------------------------------------
TALEND_REFERENCE_PATH    = "data/talend_reference.csv"
STAGING_TABLE            = "stg_output"
PASS_THRESHOLD           = 0.95
RELATIVE_TOLERANCE       = 0.01     # 1% default for all numeric columns
NULL_RATIO_THRESHOLD     = 0.05     # max 5% null ratio diff allowed
MAX_CATEGORICAL_RATIO    = 0.20     # col is categorical if unique/total <= 20%
MAX_AGG_PAIRS            = 9        # max (numeric x categorical) pairs for L3.4
PERCENTILES              = [0.50, 0.95, 0.99]

SEPARATOR = "=" * 60
STEP_LINE = "-" * 60


# ==================================================================
# SCHEMA INFERENCE -- called once, used by all layers
# ==================================================================

def infer_schema(df: pd.DataFrame) -> dict:
    """
    Auto-detect column roles from dtype and cardinality.
    Returns a schema dict used by all layers -- no human input needed.

    Rules:
      numeric_cols    : float or int dtype columns
      categorical_cols: string/object cols where unique_count/total <= MAX_CATEGORICAL_RATIO
      sort_key        : first column where all values are unique (candidate PK),
                        fallback to all columns if none found
    """
    n = len(df)
    numeric_cols     = [c for c in df.columns
                        if pd.api.types.is_numeric_dtype(df[c])]
    categorical_cols = [c for c in df.columns
                        if df[c].dtype == object
                        and n > 0
                        and df[c].nunique() / n <= MAX_CATEGORICAL_RATIO]

    # Pick sort key: prefer a column with all unique values (looks like a PK)
    sort_key = None
    for col in df.columns:
        if df[col].nunique() == n:
            sort_key = col
            break
    # Fallback: first column
    if sort_key is None and len(df.columns) > 0:
        sort_key = df.columns[0]

    return {
        "numeric_cols"     : numeric_cols,
        "categorical_cols" : categorical_cols,
        "sort_key"         : sort_key,
        "all_cols"         : list(df.columns),
        "n_rows"           : n,
    }


def normalize(df: pd.DataFrame, sort_key) -> pd.DataFrame:
    """
    Minimal normalization: strip whitespace and cast int to string
    to neutralize SQLite vs CSV type storage differences.
    Does NOT cast types to hide structural issues (Layer 1 catches those).
    """
    df = df.copy()
    df.columns = df.columns.str.lower().str.strip()
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip()
    for col in df.select_dtypes(include=["int64", "int32"]).columns:
        df[col] = df[col].astype(str).str.strip()
    if sort_key and sort_key in df.columns:
        df = df.sort_values(sort_key).reset_index(drop=True)
    return df


def print_section(title: str):
    print()
    print(STEP_LINE)
    print(f"  {title}")
    print(STEP_LINE)


def log_check(name: str, passed: bool, detail: str = ""):
    if passed: logger.success(f"[PASS] {name}  {detail}")
    else:      logger.error(f"[FAIL] {name}  {detail}")


# ==================================================================
# LAYER 1 -- STRUCTURAL  (fail-fast)
# ==================================================================

def layer1_structural(talend_df: pd.DataFrame, sql_df: pd.DataFrame) -> dict:
    print_section("LAYER 1 -- Structural Validation")
    results   = {"layer": 1, "passed": True, "checks": {}, "fatal": False}
    t_cols    = list(talend_df.columns)
    s_cols    = list(sql_df.columns)
    t_set     = set(t_cols)
    s_set     = set(s_cols)

    # 1.1 Column naming
    missing   = sorted(t_set - s_set)
    extra     = sorted(s_set - t_set)
    ok        = (t_set == s_set)
    results["checks"]["1.1_column_naming"] = {
        "passed": ok,
        "missing_in_sql": missing,
        "extra_in_sql": extra,
        "common_columns": sorted(t_set & s_set),
    }
    log_check("1.1 Column naming", ok,
              f"{len(t_set & s_set)} common | missing={missing or 'none'} | extra={extra or 'none'}")
    if not ok:
        results["passed"] = False; results["fatal"] = True
        logger.error("STRUCTURAL FAIL -- stopping pipeline. Fix column names first.")
        return results

    # 1.2 Column count
    ok = (len(t_cols) == len(s_cols))
    results["checks"]["1.2_column_count"] = {
        "passed": ok, "talend_count": len(t_cols), "sql_count": len(s_cols)
    }
    log_check("1.2 Column count", ok, f"Talend={len(t_cols)} | SQL={len(s_cols)}")
    if not ok:
        results["passed"] = False; results["fatal"] = True
        logger.error("STRUCTURAL FAIL -- stopping pipeline.")
        return results

    # 1.3 Data types
    type_issues = []
    for col in sorted(t_set & s_set):
        td = str(talend_df[col].dtype)
        sd = str(sql_df[col].dtype)
        compatible = (td == sd or {td, sd} <= {"object", "int64", "int32"})
        if not compatible:
            type_issues.append({"column": col, "talend_type": td, "sql_type": sd})
    ok = len(type_issues) == 0
    results["checks"]["1.3_data_types"] = {"passed": ok, "type_issues": type_issues}
    log_check("1.3 Data types", ok,
              "all types compatible" if ok else f"{len(type_issues)} incompatible columns")
    if not ok:
        for i in type_issues: logger.warning(f"  {i['column']:<22} Talend={i['talend_type']} | SQL={i['sql_type']}")
        results["passed"] = False; results["fatal"] = True
        logger.error("STRUCTURAL FAIL -- stopping pipeline.")
        return results

    # 1.4 Nullability
    null_issues = []
    for col in sorted(t_set & s_set):
        tn = talend_df[col].isnull().mean()
        sn = sql_df[col].isnull().mean()
        if abs(tn - sn) > NULL_RATIO_THRESHOLD:
            null_issues.append({"column": col,
                "talend_null_pct": round(tn*100,2),
                "sql_null_pct":    round(sn*100,2),
                "diff_pct":        round(abs(tn-sn)*100,2)})
    ok = len(null_issues) == 0
    results["checks"]["1.4_nullability"] = {"passed": ok, "null_issues": null_issues}
    log_check("1.4 Nullability", ok,
              "null ratios consistent" if ok
              else f"{len(null_issues)} columns with null diff > {NULL_RATIO_THRESHOLD:.0%}")
    if not ok:
        for i in null_issues:
            logger.warning(f"  {i['column']:<22} Talend={i['talend_null_pct']}% | SQL={i['sql_null_pct']}% | diff={i['diff_pct']}%")
        results["passed"] = False; results["fatal"] = True
        logger.error("STRUCTURAL FAIL -- stopping pipeline.")
        return results

    # 1.5 Column order (warning only)
    ok = (t_cols == s_cols)
    results["checks"]["1.5_column_order"] = {
        "passed": ok, "talend_order": t_cols, "sql_order": s_cols
    }
    log_check("1.5 Column order", ok,
              "same order" if ok else "different order (warning only -- comparison proceeds)")
    if not ok:
        logger.warning("  Column order differs but data comparison will proceed.")

    results["passed"] = True
    return results


# ==================================================================
# LAYER 2 -- DATA-LEVEL
# ==================================================================

def _row_hash(df, cols):
    return df[cols].apply(
        lambda r: hashlib.sha256("|".join(str(v) for v in r).encode()).hexdigest(), axis=1)

def _col_hash(series):
    return hashlib.sha256("|".join(sorted(series.astype(str).tolist())).encode()).hexdigest()


def layer2_data(talend_df: pd.DataFrame, sql_df: pd.DataFrame) -> dict:
    print_section("LAYER 2 -- Data-Level Validation")
    results     = {"layer": 2, "passed": True, "checks": {}}
    common_cols = sorted(set(talend_df.columns) & set(sql_df.columns))

    # 2.1 Row count
    ok = (len(talend_df) == len(sql_df))
    results["checks"]["2.1_row_count"] = {
        "passed": ok, "talend_rows": len(talend_df),
        "sql_rows": len(sql_df), "diff": abs(len(talend_df)-len(sql_df))
    }
    log_check("2.1 Row count", ok, f"Talend={len(talend_df)} | SQL={len(sql_df)}")
    if not ok: results["passed"] = False

    # 2.2 Full row hash
    ts = talend_df[common_cols].sort_values(by=common_cols).reset_index(drop=True)
    ss = sql_df[common_cols].sort_values(by=common_cols).reset_index(drop=True)
    th = set(_row_hash(ts, common_cols))
    sh = set(_row_hash(ss, common_cols))
    matched   = len(th & sh)
    total     = max(len(th), len(sh))
    match_rate = round(matched/total, 4) if total > 0 else 1.0
    ok = match_rate >= PASS_THRESHOLD
    results["checks"]["2.2_row_hash"] = {
        "passed": ok, "matched_hashes": matched,
        "only_in_talend": len(th-sh), "only_in_sql": len(sh-th),
        "match_rate": match_rate,
    }
    log_check("2.2 Full row hash", ok,
              f"match={match_rate:.2%} | matched={matched} | only_talend={len(th-sh)} | only_sql={len(sh-th)}")
    if not ok: results["passed"] = False

    # 2.3 Column-level hash
    diff_cols = [c for c in common_cols if _col_hash(ts[c]) != _col_hash(ss[c])]
    ok = len(diff_cols) == 0
    results["checks"]["2.3_column_hash"] = {
        "passed": ok,
        "differing_columns": diff_cols,
        "clean_columns": [c for c in common_cols if c not in diff_cols],
    }
    log_check("2.3 Column-level hash", ok,
              "all columns match" if ok else f"{len(diff_cols)} columns differ: {diff_cols}")
    if not ok: results["passed"] = False

    # 2.4 Targeted row diff
    sample_diffs = []
    full_diffs   = []
    if diff_cols and len(ts) == len(ss):
        mask = (ts != ss).any(axis=1)
        for idx in mask[mask].index:
            for col in diff_cols:
                tv = ts.loc[idx, col]; sv = ss.loc[idx, col]
                if str(tv) != str(sv):
                    e = {"row": int(idx), "column": col, "talend_value": str(tv), "sql_value": str(sv)}
                    full_diffs.append(e)
                    if len(sample_diffs) < 10: sample_diffs.append(e)
        if sample_diffs:
            logger.warning("  Sample differences (first 10):")
            for d in sample_diffs:
                logger.warning(f"  Row {d['row']:>3} | {d['column']:<22} | Talend: {d['talend_value']:<25} | SQL: {d['sql_value']}")
    results["checks"]["2.4_targeted_diff"] = {
        "passed": len(full_diffs) == 0, "total_diffs": len(full_diffs),
        "sample_diffs": sample_diffs, "full_diffs": full_diffs,
    }
    log_check("2.4 Targeted row diff", len(full_diffs) == 0,
              "no differences" if not full_diffs
              else f"{len(full_diffs)} cell-level diffs across {len(diff_cols)} columns")

    results["match_rate"] = match_rate
    return results


# ==================================================================
# LAYER 3 -- BUSINESS RULES  (fully inferred)
# ==================================================================

def layer3_business_rules(talend_df: pd.DataFrame, sql_df: pd.DataFrame,
                           schema: dict) -> dict:
    """
    All checks are derived from schema inference -- no column names are hardcoded.

    3.1  REMOVED -- forbidden values require human domain knowledge
    3.2  Exact match on inferred categorical columns (low cardinality strings)
    3.3  Relative tolerance on all inferred numeric columns
    3.4  Aggregation: every (numeric col x categorical col) pair, capped at MAX_AGG_PAIRS
    3.5  Referential integrity: SQL must not introduce values absent from Talend (all cols)
    """
    print_section("LAYER 3 -- Business Rule Validation  [fully inferred]")
    results      = {"layer": 3, "passed": True, "checks": {}}
    sort_key     = schema["sort_key"]
    numeric_cols = schema["numeric_cols"]
    cat_cols     = schema["categorical_cols"]

    t_sorted = talend_df.sort_values(sort_key).reset_index(drop=True) if sort_key else talend_df.copy()
    s_sorted = sql_df.sort_values(sort_key).reset_index(drop=True) if sort_key else sql_df.copy()

    logger.info(f"  Inferred numeric cols     : {numeric_cols}")
    logger.info(f"  Inferred categorical cols : {cat_cols}")
    logger.info(f"  Sort key                  : {sort_key}")

    # ------------------------------------------------------------------
    # 3.2 Exact match on categorical columns
    # ------------------------------------------------------------------
    exact_results = {}
    for col in cat_cols:
        if col not in talend_df.columns or col not in sql_df.columns: continue
        tv = t_sorted[col].reset_index(drop=True)
        sv = s_sorted[col].reset_index(drop=True)
        mn = min(len(tv), len(sv))
        mm = int((tv[:mn] != sv[:mn]).sum())
        exact_results[col] = {
            "passed": mm == 0, "mismatches": mm,
            "reason": "All values match" if mm == 0 else f"{mm} values differ",
        }
        log_check(f"3.2 Exact match '{col}'", mm == 0, exact_results[col]["reason"])
        if mm != 0: results["passed"] = False
    results["checks"]["3.2_exact_match"] = exact_results

    # ------------------------------------------------------------------
    # 3.3 Relative tolerance on numeric columns
    # ------------------------------------------------------------------
    tol_results = {}
    for col in numeric_cols:
        if col not in talend_df.columns or col not in sql_df.columns: continue
        diffs = []
        for i, (tv, sv) in enumerate(zip(t_sorted[col], s_sorted[col])):
            try:
                tvf = float(tv); svf = float(sv)
            except (ValueError, TypeError): continue
            rd = abs(tvf-svf)/abs(tvf) if tvf != 0 else abs(svf)
            if rd > RELATIVE_TOLERANCE:
                diffs.append({"row": i, "talend_value": round(tvf,6), "sql_value": round(svf,6),
                               "rel_diff_pct": round(rd*100,4), "tolerance_pct": round(RELATIVE_TOLERANCE*100,2)})
        ok = len(diffs) == 0
        tol_results[col] = {
            "passed": ok, "tolerance": f"{RELATIVE_TOLERANCE:.1%}",
            "mismatches": len(diffs), "sample": diffs[:5],
            "talend_sample": [], "sql_sample": [],
        }
        log_check(f"3.3 Relative tolerance '{col}' (tol={RELATIVE_TOLERANCE:.1%})", ok,
                  "all within tolerance" if ok else f"{len(diffs)} values exceed tolerance")
        if not ok:
            for d in diffs[:3]:
                logger.warning(f"  Row {d['row']:>3} | Talend={d['talend_value']} | SQL={d['sql_value']} | rel_diff={d['rel_diff_pct']}%")
            results["passed"] = False
    results["checks"]["3.3_relative_tolerance"] = tol_results

    # ------------------------------------------------------------------
    # 3.4 Aggregation validation -- all (numeric x categorical) pairs
    #     inferred automatically, capped at MAX_AGG_PAIRS
    # ------------------------------------------------------------------
    agg_results = []
    pairs_run   = 0
    for agg_col in numeric_cols:
        for grp_col in cat_cols:
            if agg_col not in talend_df.columns or grp_col not in talend_df.columns: continue
            if agg_col not in sql_df.columns   or grp_col not in sql_df.columns:    continue
            if pairs_run >= MAX_AGG_PAIRS: break

            def to_float(s):
                try: return s.astype(float)
                except: return s

            t_agg = (talend_df.assign(**{agg_col: to_float(talend_df[agg_col])})
                     .groupby(grp_col)[agg_col].agg(["sum","count","mean"]).round(4).sort_index())
            s_agg = (sql_df.assign(**{agg_col: to_float(sql_df[agg_col])})
                     .groupby(grp_col)[agg_col].agg(["sum","count","mean"]).round(4).sort_index())

            ok = True; issues = []
            for grp in t_agg.index:
                if grp not in s_agg.index:
                    issues.append(f"Group '{grp}' missing in SQL"); ok = False; continue
                for metric in ["sum","count","mean"]:
                    tv = t_agg.loc[grp, metric]; sv = s_agg.loc[grp, metric]
                    rd = abs(tv-sv)/abs(tv) if tv != 0 else abs(sv)
                    if rd > RELATIVE_TOLERANCE:
                        issues.append(f"Group='{grp}' {metric}: Talend={tv} | SQL={sv} | rel_diff={rd:.2%}")
                        ok = False

            label = f"{agg_col} by {grp_col}"
            agg_results.append({"label": label, "agg_col": agg_col, "group_col": grp_col,
                                 "passed": ok, "issues": issues})
            log_check(f"3.4 Aggregation: {label}", ok,
                      "all groups match" if ok else f"{len(issues)} issues")
            for issue in issues: logger.warning(f"  {issue}")
            if not ok: results["passed"] = False
            pairs_run += 1
        if pairs_run >= MAX_AGG_PAIRS: break

    if pairs_run == 0:
        logger.info("3.4 Aggregation: no (numeric x categorical) pairs found -- skipped")
    elif pairs_run >= MAX_AGG_PAIRS:
        logger.info(f"3.4 Aggregation: capped at {MAX_AGG_PAIRS} pairs")
    results["checks"]["3.4_aggregations"] = agg_results

    # ------------------------------------------------------------------
    # 3.5 Referential integrity -- for every column, SQL must not introduce
    #     values that don't exist in the Talend reference
    # ------------------------------------------------------------------
    ref_results = {}
    for col in sorted(set(talend_df.columns) & set(sql_df.columns)):
        t_vals  = set(talend_df[col].astype(str).unique())
        s_vals  = set(sql_df[col].astype(str).unique())
        orphans = s_vals - t_vals
        if orphans:
            ref_results[col] = {
                "passed": False,
                "orphan_in_sql":  sorted(orphans),
                "missing_in_sql": sorted(t_vals - s_vals),
            }
            log_check(f"3.5 Referential integrity '{col}'", False,
                      f"{len(orphans)} values in SQL not present in Talend: {sorted(orphans)[:5]}")
            results["passed"] = False
    if not ref_results:
        logger.success("[PASS] 3.5 Referential integrity  all columns -- no orphan values in SQL")
    results["checks"]["3.5_referential_integrity"] = ref_results
    return results


# ==================================================================
# LAYER 4 -- STATISTICAL
# ==================================================================

def layer4_statistical(talend_df: pd.DataFrame, sql_df: pd.DataFrame,
                        schema: dict) -> dict:
    print_section("LAYER 4 -- Statistical Validation")
    results      = {"layer": 4, "passed": True, "checks": {}}
    numeric_cols = schema["numeric_cols"]
    common_cols  = sorted(set(talend_df.columns) & set(sql_df.columns))
    stat_issues  = []; null_issues = []; card_issues = []
    all_stats    = {}

    for col in common_cols:
        tc = talend_df[col]; sc = sql_df[col]
        is_num  = col in numeric_cols
        cstats  = {"column": col, "numeric": is_num}

        # Null ratio
        tn = round(tc.isnull().mean()*100, 2)
        sn = round(sc.isnull().mean()*100, 2)
        cstats["null_ratio"] = {"talend": tn, "sql": sn, "diff": round(abs(tn-sn),2)}
        if abs(tn-sn) > NULL_RATIO_THRESHOLD*100:
            null_issues.append({"column": col, "talend_pct": tn, "sql_pct": sn, "diff_pct": round(abs(tn-sn),2)})

        # Cardinality
        tc_n = tc.nunique(); sc_n = sc.nunique()
        cstats["cardinality"] = {"talend": tc_n, "sql": sc_n, "diff": abs(tc_n-sc_n)}
        if tc_n != sc_n:
            card_issues.append({"column": col, "talend": tc_n, "sql": sc_n, "diff": abs(tc_n-sc_n)})

        if is_num:
            try:
                tn_num = pd.to_numeric(tc, errors="coerce").dropna()
                sn_num = pd.to_numeric(sc, errors="coerce").dropna()
                sv = {
                    "min":  (round(float(tn_num.min()),4),  round(float(sn_num.min()),4)),
                    "max":  (round(float(tn_num.max()),4),  round(float(sn_num.max()),4)),
                    "mean": (round(float(tn_num.mean()),4), round(float(sn_num.mean()),4)),
                    "std":  (round(float(tn_num.std()),4),  round(float(sn_num.std()),4)),
                }
                cstats["stats"] = {k: {"talend": v[0], "sql": v[1]} for k,v in sv.items()}
                cstats["percentiles"] = {
                    f"P{int(p*100)}": {"talend": round(float(tn_num.quantile(p)),4),
                                       "sql":    round(float(sn_num.quantile(p)),4)}
                    for p in PERCENTILES
                }
                for sname,(tv,sv2) in sv.items():
                    rd = abs(tv-sv2)/abs(tv) if tv != 0 else abs(sv2)
                    if rd > RELATIVE_TOLERANCE:
                        stat_issues.append({"column": col, "stat": sname, "talend": tv, "sql": sv2,
                                            "rel_diff": round(rd*100,4), "tol_pct": round(RELATIVE_TOLERANCE*100,2)})
            except Exception as e:
                cstats["stats_error"] = str(e)
        all_stats[col] = cstats

    stat_ok = len(stat_issues) == 0
    results["checks"]["4.1_4.2_distributions"] = {"passed": stat_ok, "issues": stat_issues, "all_stats": all_stats}
    log_check("4.1/4.2 Distributions (min/max/mean/std)", stat_ok,
              "all within tolerance" if stat_ok else f"{len(stat_issues)} distribution issues")
    if not stat_ok:
        for i in stat_issues[:5]:
            logger.warning(f"  {i['column']:<22} {i['stat']:<6} Talend={i['talend']} | SQL={i['sql']} | rel_diff={i['rel_diff']}%")
        results["passed"] = False

    null_ok = len(null_issues) == 0
    results["checks"]["4.3_null_ratio"] = {"passed": null_ok, "issues": null_issues}
    log_check("4.3 Null ratio", null_ok,
              "all consistent" if null_ok else f"{len(null_issues)} columns with null ratio diff")
    if not null_ok: results["passed"] = False

    card_ok = len(card_issues) == 0
    results["checks"]["4.4_cardinality"] = {"passed": card_ok, "issues": card_issues}
    log_check("4.4 Cardinality", card_ok,
              "all match" if card_ok else f"{len(card_issues)} columns with different unique counts")
    if not card_ok:
        for i in card_issues:
            logger.warning(f"  {i['column']:<22} Talend={i['talend']} unique | SQL={i['sql']} unique")
        results["passed"] = False

    pct_data = {col: s.get("percentiles",{}) for col,s in all_stats.items() if "percentiles" in s}
    results["checks"]["4.5_percentiles"] = {"data": pct_data}
    if pct_data:
        logger.info("4.5 Percentiles (informational):")
        for col,pcts in pct_data.items():
            for pname,vals in pcts.items():
                m = "OK" if vals["talend"] == vals["sql"] else "!!"
                logger.info(f"  [{m}] {col:<22} {pname}: Talend={vals['talend']} | SQL={vals['sql']}")

    results["all_col_stats"] = all_stats
    return results


# ==================================================================
# FINAL DECISION
# ==================================================================

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
    if decision == "PASS": logger.success("FINAL DECISION : PASS")
    else:
        logger.error("FINAL DECISION : FAIL")
        for r in reasons: logger.error(f"  Reason: {r}")
    if l2: logger.info(f"Match rate     : {l2.get('match_rate', 'N/A'):.2%}")
    logger.info(f"Layers run     : "
                f"L1={'OK' if l1['passed'] else 'FAIL'} | "
                f"L2={'OK' if l2 and l2['passed'] else ('FAIL' if l2 else 'SKIPPED')} | "
                f"L3={'OK' if l3 and l3['passed'] else ('FAIL' if l3 else 'SKIPPED')} | "
                f"L4={'OK' if l4 and l4['passed'] else ('FAIL' if l4 else 'SKIPPED')}")
    print(SEPARATOR + "\n")


def _build_output(decision, reasons, talend_df, sql_df, l1, l2, l3, l4):
    l2c = l2["checks"] if l2 else {}
    l3c = l3["checks"] if l3 else {}
    diff_data  = l2c.get("2.4_targeted_diff", {})
    match_rate = l2.get("match_rate", 0.0) if l2 else 0.0
    return {
        "decision": decision, "match_rate": match_rate, "threshold": PASS_THRESHOLD,
        "talend_rows": len(talend_df), "sql_rows": len(sql_df),
        "run_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "talend_path": TALEND_REFERENCE_PATH, "sql_table": STAGING_TABLE,
        "fail_reasons": reasons,
        "structure": {
            "talend_row_count": len(talend_df), "sql_row_count": len(sql_df),
            "row_count_match": l2c.get("2.1_row_count",{}).get("passed",False) if l2 else False,
            "column_match": l1["checks"].get("1.1_column_naming",{}).get("passed",False),
            "columns_only_in_talend": l1["checks"].get("1.1_column_naming",{}).get("missing_in_sql",[]),
            "columns_only_in_sql":    l1["checks"].get("1.1_column_naming",{}).get("extra_in_sql",[]),
            "common_columns":         l1["checks"].get("1.1_column_naming",{}).get("common_columns",[]),
        },
        "forbidden": {},   # removed -- requires human domain knowledge
        "exact":   l3c.get("3.2_exact_match", {}) if l3 else {},
        "rounding": {
            col: {"passed": r.get("passed",False), "mismatches": r.get("mismatches",0),
                  "reason": f"Relative tolerance {r.get('tolerance','N/A')}",
                  "talend_sample": [], "sql_sample": []}
            for col, r in l3c.get("3.3_relative_tolerance",{}).items()
        } if l3 else {},
        "row_level": {
            "total_rows": max(len(talend_df), len(sql_df)),
            "matched_rows": l2c.get("2.2_row_hash",{}).get("matched_hashes",0) if l2 else 0,
            "diff_rows":    l2c.get("2.2_row_hash",{}).get("only_in_talend",0) if l2 else 0,
            "match_rate": match_rate,
            "sample_diffs": diff_data.get("sample_diffs",[]),
            "full_diffs":   diff_data.get("full_diffs",[]),
        },
        "layers": {"layer1": l1, "layer2": l2, "layer3": l3, "layer4": l4},
    }


# ==================================================================
# MAIN
# ==================================================================

def run_comparison() -> dict:
    print("\n" + SEPARATOR)
    print("  VALIDATION AGENT - 4-LAYER COMPARATOR  [generic mode]")
    print(SEPARATOR + "\n")

    logger.info("Loading datasets...")
    talend_df = pd.read_csv(TALEND_REFERENCE_PATH, encoding="utf-8")
    sql_df    = load_table_as_dataframe(STAGING_TABLE)
    logger.info(f"  Talend : {len(talend_df)} rows x {len(talend_df.columns)} cols")
    logger.info(f"  SQL    : {len(sql_df)} rows x {len(sql_df.columns)} cols")

    # Infer schema from Talend reference (ground truth)
    logger.info("Inferring schema from Talend reference...")
    raw_schema = infer_schema(talend_df)
    logger.info(f"  Sort key         : {raw_schema['sort_key']}")
    logger.info(f"  Numeric cols     : {raw_schema['numeric_cols']}")
    logger.info(f"  Categorical cols : {raw_schema['categorical_cols']}")

    # Normalize both datasets
    talend_norm = normalize(talend_df, raw_schema["sort_key"])
    sql_norm    = normalize(sql_df,    raw_schema["sort_key"])

    # Re-infer on normalized data (types may change after normalization)
    schema = infer_schema(talend_norm)

    # Layer 1 -- fail-fast
    l1 = layer1_structural(talend_norm, sql_norm)
    if l1.get("fatal"):
        decision = "FAIL"
        reasons  = ["L1: Structural mismatch -- pipeline stopped"]
        _print_final(decision, reasons, l1, None, None, None)
        return _build_output(decision, reasons, talend_df, sql_df, l1, None, None, None)

    # Layers 2-4
    l2 = layer2_data(talend_norm, sql_norm)
    l3 = layer3_business_rules(talend_norm, sql_norm, schema)
    l4 = layer4_statistical(talend_norm, sql_norm, schema)

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