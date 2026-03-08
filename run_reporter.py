"""
run_reporter.py
---------------
Runs the full comparison internally then generates a Word (.docx) report.

This script combines run_comparator.py logic + report generation
into one single command.

Usage:
    python run_reporter.py

Output:
    outputs/validation_report_YYYYMMDD_HHMMSS.docx
"""

import sys
import os
import json
import pandas as pd
from datetime import datetime
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from agent.tools.db import load_table_as_dataframe

# ------------------------------------------------------------------
# Logger
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
# ------------------------------------------------------------------
TALEND_REFERENCE_PATH = "data/talend_reference.csv"
STAGING_TABLE         = "stg_output"
PASS_THRESHOLD        = 0.95
NUMERIC_TOLERANCE     = 2
OUTPUT_DIR            = "outputs"

COLS_NO_BLOCKED  = ["payment_status"]
COLS_EXACT_MATCH = ["payment_status", "invoice_id", "vendor_id", "currency"]
COLS_ROUNDED     = ["net_amount", "gross_amount", "tax_rate"]

SEPARATOR = "=" * 60
STEP_LINE = "-" * 60


# ==================================================================
# PART 1 - COMPARISON LOGIC (same as run_comparator.py)
# ==================================================================

def normalize(df):
    df = df.copy()
    df.columns = df.columns.str.lower().str.strip()
    for col in df.select_dtypes(include=["float64", "float32"]).columns:
        df[col] = df[col].round(NUMERIC_TOLERANCE)
    for col in df.select_dtypes(include=["int64", "int32"]).columns:
        df[col] = df[col].astype(str).str.strip()
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip()
    if "invoice_id" in df.columns:
        df = df.sort_values("invoice_id").reset_index(drop=True)
    return df


def check_structure(talend_df, sql_df):
    talend_cols = set(talend_df.columns)
    sql_cols    = set(sql_df.columns)
    return {
        "talend_row_count"       : len(talend_df),
        "sql_row_count"          : len(sql_df),
        "row_count_match"        : len(talend_df) == len(sql_df),
        "column_match"           : talend_cols == sql_cols,
        "columns_only_in_talend" : sorted(talend_cols - sql_cols),
        "columns_only_in_sql"    : sorted(sql_cols - talend_cols),
        "common_columns"         : sorted(talend_cols & sql_cols),
    }


def check_no_forbidden_statuses(sql_df):
    forbidden = ["BLOCKED", "CANCELLED"]
    results   = {}
    for col in COLS_NO_BLOCKED:
        if col not in sql_df.columns:
            results[col] = {"passed": False, "reason": f"Column '{col}' not found", "forbidden_count": 0}
            continue
        found = sql_df[sql_df[col].isin(forbidden)]
        results[col] = {
            "passed"          : len(found) == 0,
            "forbidden_count" : len(found),
            "reason"          : "No forbidden statuses found" if len(found) == 0
                                else f"Found {len(found)} forbidden rows",
        }
    return results


def check_exact_columns(talend_df, sql_df):
    results = {}
    for col in COLS_EXACT_MATCH:
        if col not in talend_df.columns or col not in sql_df.columns:
            results[col] = {"passed": False, "reason": f"Column '{col}' missing", "mismatches": 0}
            continue
        t = talend_df[col].sort_values().reset_index(drop=True)
        s = sql_df[col].sort_values().reset_index(drop=True)
        mismatches = int((t != s).sum())
        results[col] = {
            "passed"     : mismatches == 0,
            "mismatches" : mismatches,
            "reason"     : "All values match" if mismatches == 0
                           else f"{mismatches} values differ",
        }
    return results


def check_rounding(talend_df, sql_df):
    results = {}
    for col in COLS_ROUNDED:
        if col not in talend_df.columns or col not in sql_df.columns:
            results[col] = {"passed": False, "reason": f"Column '{col}' missing", "mismatches": 0}
            continue
        t = talend_df[col].round(NUMERIC_TOLERANCE).sort_values().reset_index(drop=True)
        s = sql_df[col].round(NUMERIC_TOLERANCE).sort_values().reset_index(drop=True)
        mismatches = int((t != s).sum())
        results[col] = {
            "passed"        : mismatches == 0,
            "mismatches"    : mismatches,
            "talend_sample" : t.head(3).tolist(),
            "sql_sample"    : s.head(3).tolist(),
            "reason"        : "All values match within tolerance" if mismatches == 0
                              else f"{mismatches} values differ after rounding",
        }
    return results


def check_row_level(talend_df, sql_df):
    common_cols   = sorted(set(talend_df.columns) & set(sql_df.columns))
    talend_sorted = talend_df[common_cols].sort_values(by=common_cols).reset_index(drop=True)
    sql_sorted    = sql_df[common_cols].sort_values(by=common_cols).reset_index(drop=True)

    sample_diffs = []
    if len(talend_sorted) == len(sql_sorted):
        diff_mask    = (talend_sorted != sql_sorted).any(axis=1)
        matched_rows = int((~diff_mask).sum())
        diff_indices = diff_mask[diff_mask].index[:10]
        for idx in diff_indices:
            for col in common_cols:
                tv = talend_sorted.loc[idx, col]
                sv = sql_sorted.loc[idx, col]
                if tv != sv:
                    sample_diffs.append({
                        "row"          : int(idx),
                        "column"       : col,
                        "talend_value" : str(tv),
                        "sql_value"    : str(sv),
                    })
    else:
        matched_rows = 0

    total_rows = max(len(talend_df), len(sql_df))
    match_rate = round(matched_rows / total_rows, 4) if total_rows > 0 else 1.0

    # Full diff for report (all mismatched rows, all columns)
    full_diffs = []
    if len(talend_sorted) == len(sql_sorted):
        diff_mask = (talend_sorted != sql_sorted).any(axis=1)
        for idx in diff_mask[diff_mask].index:
            for col in common_cols:
                tv = talend_sorted.loc[idx, col]
                sv = sql_sorted.loc[idx, col]
                if tv != sv:
                    full_diffs.append({
                        "row"          : int(idx),
                        "column"       : col,
                        "talend_value" : str(tv),
                        "sql_value"    : str(sv),
                    })

    return {
        "total_rows"   : total_rows,
        "matched_rows" : matched_rows,
        "diff_rows"    : total_rows - matched_rows,
        "match_rate"   : match_rate,
        "sample_diffs" : sample_diffs,
        "full_diffs"   : full_diffs,
    }


def run_comparison():
    """Run all comparison checks and return a results dict."""
    logger.info("Running comparison...")

    talend_df = pd.read_csv(TALEND_REFERENCE_PATH, encoding="utf-8")
    sql_df    = load_table_as_dataframe(STAGING_TABLE)

    talend_norm = normalize(talend_df)
    sql_norm    = normalize(sql_df)

    structure  = check_structure(talend_norm, sql_norm)
    forbidden  = check_no_forbidden_statuses(sql_norm)
    exact      = check_exact_columns(talend_norm, sql_norm)
    rounding   = check_rounding(talend_norm, sql_norm)
    row_level  = check_row_level(talend_norm, sql_norm)

    # Track whether ALL checks passed -- not just match rate
    all_checks_passed = (
        structure["row_count_match"]
        and structure["column_match"]
        and all(r["passed"] for r in forbidden.values())
        and all(r["passed"] for r in exact.values())
        and all(r["passed"] for r in rounding.values())
    )

    match_rate = row_level["match_rate"]
    # CRITICAL: match rate alone is not enough.
    # A missing/renamed column still gives 100% match on remaining cols.
    # All structural and column checks must also pass.
    decision   = "PASS" if (match_rate >= PASS_THRESHOLD and all_checks_passed) else "FAIL"

    logger.info(f"Match rate : {match_rate:.2%}")
    logger.info(f"Decision   : {decision}")

    return {
        "decision"   : decision,
        "match_rate" : match_rate,
        "threshold"  : PASS_THRESHOLD,
        "talend_rows": len(talend_df),
        "sql_rows"   : len(sql_df),
        "structure"  : structure,
        "forbidden"  : forbidden,
        "exact"      : exact,
        "rounding"   : rounding,
        "row_level"  : row_level,
        "run_date"   : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "talend_path": TALEND_REFERENCE_PATH,
        "sql_table"  : STAGING_TABLE,
    }


# ==================================================================
# PART 2 - WORD REPORT GENERATION
# ==================================================================

def build_report(results: dict) -> str:
    """
    Generate a Word (.docx) validation report from comparison results.
    Returns the path to the saved file.
    """
    from docx import Document
    from docx.shared import Pt, RGBColor, Inches
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    from docx.oxml.ns import qn
    from docx.oxml import OxmlElement

    # -- Color palette ------------------------------------------------
    COLOR_BLUE      = RGBColor(0x1A, 0x5C, 0x8A)   # headings
    COLOR_GREEN     = RGBColor(0x1E, 0x8B, 0x4C)   # PASS
    COLOR_RED       = RGBColor(0xC0, 0x39, 0x2B)   # FAIL
    COLOR_GREY      = RGBColor(0x66, 0x66, 0x66)   # metadata
    COLOR_WHITE     = RGBColor(0xFF, 0xFF, 0xFF)   # table headers

    decision        = results["decision"]
    is_pass         = decision == "PASS"
    decision_color  = COLOR_GREEN if is_pass else COLOR_RED

    doc = Document()

    # -- Page margins -------------------------------------------------
    for section in doc.sections:
        section.top_margin    = Inches(1)
        section.bottom_margin = Inches(1)
        section.left_margin   = Inches(1.2)
        section.right_margin  = Inches(1.2)

    # -- Helper: shade a table row ------------------------------------
    def shade_row(row, hex_color):
        for cell in row.cells:
            tc   = cell._tc
            tcPr = tc.get_or_add_tcPr()
            shd  = OxmlElement("w:shd")
            shd.set(qn("w:val"),   "clear")
            shd.set(qn("w:color"), "auto")
            shd.set(qn("w:fill"),  hex_color)
            tcPr.append(shd)

    # -- Helper: add a 2-col key-value table --------------------------
    def add_kv_table(pairs):
        table = doc.add_table(rows=len(pairs), cols=2)
        table.style = "Table Grid"
        for i, (key, val) in enumerate(pairs):
            row = table.rows[i]
            row.cells[0].text = key
            row.cells[1].text = str(val)
            row.cells[0].paragraphs[0].runs[0].bold = True
            row.cells[0].paragraphs[0].runs[0].font.color.rgb = COLOR_BLUE

    # -- Helper: section heading --------------------------------------
    def add_heading(text, level=1):
        h = doc.add_heading(text, level=level)
        for run in h.runs:
            run.font.color.rgb = COLOR_BLUE

    # -- Helper: PASS/FAIL badge paragraph ----------------------------
    def add_badge(text, color):
        p    = doc.add_paragraph()
        run  = p.add_run(f"  {text}  ")
        run.bold              = True
        run.font.size         = Pt(13)
        run.font.color.rgb    = color
        run.font.highlight_color = None
        return p

    # ==============================================================
    # COVER
    # ==============================================================
    title_para = doc.add_heading("Migration Validation Report", 0)
    title_para.alignment = WD_ALIGN_PARAGRAPH.CENTER
    for run in title_para.runs:
        run.font.color.rgb = COLOR_BLUE

    sub = doc.add_paragraph()
    sub.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = sub.add_run(f"Job: JOB_NXS_T26_Payment_Status_SAP_Coupa")
    r.font.color.rgb = COLOR_GREY
    r.font.size      = Pt(11)

    doc.add_paragraph()

    # Decision badge
    badge = doc.add_paragraph()
    badge.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = badge.add_run(f"  DECISION: {decision}  ")
    run.bold           = True
    run.font.size      = Pt(18)
    run.font.color.rgb = decision_color

    doc.add_paragraph()

    # ==============================================================
    # SECTION 1 - SUMMARY
    # ==============================================================
    add_heading("1. Summary", level=1)
    add_kv_table([
        ("Report Date",          results["run_date"]),
        ("Final Decision",       decision),
        ("Match Rate",           f"{results['match_rate']:.2%}"),
        ("Pass Threshold",       f"{results['threshold']:.0%}"),
        ("Talend Reference",     results["talend_path"]),
        ("SQL Staging Table",    results["sql_table"]),
        ("Talend Row Count",     str(results["talend_rows"])),
        ("SQL Row Count",        str(results["sql_rows"])),
        ("Matched Rows",         str(results["row_level"]["matched_rows"])),
        ("Mismatched Rows",      str(results["row_level"]["diff_rows"])),
    ])
    doc.add_paragraph()

    # ==============================================================
    # SECTION 2 - STRUCTURAL CHECK
    # ==============================================================
    add_heading("2. Structural Check", level=1)
    s = results["structure"]

    row_ok  = s["row_count_match"]
    col_ok  = s["column_match"]

    add_kv_table([
        ("Row Count Match",         "PASS" if row_ok else "FAIL"),
        ("Talend Rows",             str(s["talend_row_count"])),
        ("SQL Rows",                str(s["sql_row_count"])),
        ("Column Match",            "PASS" if col_ok else "FAIL"),
        ("Common Columns",          str(len(s["common_columns"]))),
        ("Columns Only in Talend",  ", ".join(s["columns_only_in_talend"]) or "None"),
        ("Columns Only in SQL",     ", ".join(s["columns_only_in_sql"])    or "None"),
    ])
    doc.add_paragraph()

    # ==============================================================
    # SECTION 3 - PER-COLUMN CHECK RESULTS
    # ==============================================================
    add_heading("3. Per-Column Check Results", level=1)

    # 3a - Forbidden status check
    add_heading("3.1 Forbidden Status Check (BLOCKED / CANCELLED)", level=2)
    forbidden_rows = []
    for col, res in results["forbidden"].items():
        forbidden_rows.append((
            col,
            "PASS" if res["passed"] else "FAIL",
            res["reason"],
            str(res.get("forbidden_count", 0)),
        ))

    tbl = doc.add_table(rows=1 + len(forbidden_rows), cols=4)
    tbl.style = "Table Grid"
    headers   = ["Column", "Status", "Detail", "Forbidden Count"]
    header_row = tbl.rows[0]
    for j, h in enumerate(headers):
        cell = header_row.cells[j]
        cell.text = h
        cell.paragraphs[0].runs[0].bold            = True
        cell.paragraphs[0].runs[0].font.color.rgb  = COLOR_WHITE
    shade_row(header_row, "1A5C8A")
    for i, (col, status, detail, count) in enumerate(forbidden_rows):
        row = tbl.rows[i + 1]
        row.cells[0].text = col
        row.cells[1].text = status
        row.cells[2].text = detail
        row.cells[3].text = count
        if status == "PASS":
            shade_row(row, "D5F5E3")
        else:
            shade_row(row, "FADBD8")
    doc.add_paragraph()

    # 3b - Exact match check
    add_heading("3.2 Exact Match on Critical Columns", level=2)
    exact_rows = []
    for col, res in results["exact"].items():
        exact_rows.append((col, "PASS" if res["passed"] else "FAIL", res["reason"], str(res["mismatches"])))

    tbl2 = doc.add_table(rows=1 + len(exact_rows), cols=4)
    tbl2.style = "Table Grid"
    headers2   = ["Column", "Status", "Detail", "Mismatches"]
    header_row2 = tbl2.rows[0]
    for j, h in enumerate(headers2):
        cell = header_row2.cells[j]
        cell.text = h
        cell.paragraphs[0].runs[0].bold           = True
        cell.paragraphs[0].runs[0].font.color.rgb = COLOR_WHITE
    shade_row(header_row2, "1A5C8A")
    for i, (col, status, detail, count) in enumerate(exact_rows):
        row = tbl2.rows[i + 1]
        row.cells[0].text = col
        row.cells[1].text = status
        row.cells[2].text = detail
        row.cells[3].text = count
        shade_row(row, "D5F5E3" if status == "PASS" else "FADBD8")
    doc.add_paragraph()

    # 3c - Rounding check
    add_heading("3.3 Numeric Rounding Check", level=2)
    round_rows = []
    for col, res in results["rounding"].items():
        talend_s = str(res.get("talend_sample", ""))
        sql_s    = str(res.get("sql_sample", ""))
        round_rows.append((col, "PASS" if res["passed"] else "FAIL", res["reason"], talend_s, sql_s))

    tbl3 = doc.add_table(rows=1 + len(round_rows), cols=5)
    tbl3.style = "Table Grid"
    headers3   = ["Column", "Status", "Detail", "Talend Sample", "SQL Sample"]
    header_row3 = tbl3.rows[0]
    for j, h in enumerate(headers3):
        cell = header_row3.cells[j]
        cell.text = h
        cell.paragraphs[0].runs[0].bold           = True
        cell.paragraphs[0].runs[0].font.color.rgb = COLOR_WHITE
    shade_row(header_row3, "1A5C8A")
    for i, (col, status, detail, ts, ss) in enumerate(round_rows):
        row = tbl3.rows[i + 1]
        row.cells[0].text = col
        row.cells[1].text = status
        row.cells[2].text = detail
        row.cells[3].text = ts
        row.cells[4].text = ss
        shade_row(row, "D5F5E3" if status == "PASS" else "FADBD8")
    doc.add_paragraph()

    # ==============================================================
    # SECTION 4 - SAMPLE OF MISMATCHED ROWS
    # ==============================================================
    add_heading("4. Sample of Mismatched Rows (first 10)", level=1)
    sample = results["row_level"]["sample_diffs"]

    if not sample:
        doc.add_paragraph("No mismatched rows found -- all rows match perfectly.")
    else:
        tbl4 = doc.add_table(rows=1 + len(sample), cols=4)
        tbl4.style = "Table Grid"
        headers4   = ["Row", "Column", "Talend Value", "SQL Value"]
        header_row4 = tbl4.rows[0]
        for j, h in enumerate(headers4):
            cell = header_row4.cells[j]
            cell.text = h
            cell.paragraphs[0].runs[0].bold           = True
            cell.paragraphs[0].runs[0].font.color.rgb = COLOR_WHITE
        shade_row(header_row4, "1A5C8A")
        for i, diff in enumerate(sample):
            row = tbl4.rows[i + 1]
            row.cells[0].text = str(diff["row"])
            row.cells[1].text = diff["column"]
            row.cells[2].text = diff["talend_value"]
            row.cells[3].text = diff["sql_value"]
            shade_row(row, "FADBD8")
    doc.add_paragraph()

    # ==============================================================
    # SECTION 5 - FULL LIST OF DIFFERENCES
    # ==============================================================
    add_heading("5. Full List of Differences", level=1)
    full = results["row_level"]["full_diffs"]

    if not full:
        doc.add_paragraph("No differences found -- datasets match completely.")
    else:
        doc.add_paragraph(f"Total differences found: {len(full)}")
        doc.add_paragraph()

        tbl5 = doc.add_table(rows=1 + len(full), cols=4)
        tbl5.style = "Table Grid"
        headers5   = ["Row", "Column", "Talend Value", "SQL Value"]
        header_row5 = tbl5.rows[0]
        for j, h in enumerate(headers5):
            cell = header_row5.cells[j]
            cell.text = h
            cell.paragraphs[0].runs[0].bold           = True
            cell.paragraphs[0].runs[0].font.color.rgb = COLOR_WHITE
        shade_row(header_row5, "1A5C8A")
        for i, diff in enumerate(full):
            row = tbl5.rows[i + 1]
            row.cells[0].text = str(diff["row"])
            row.cells[1].text = diff["column"]
            row.cells[2].text = diff["talend_value"]
            row.cells[3].text = diff["sql_value"]
            shade_row(row, "FADBD8")
    doc.add_paragraph()


    # ==============================================================
    # SECTION 6 - LLM REVIEW
    # ==============================================================
    add_heading("6. LLM Review", level=1)
    llm = results.get("llm_review")

    if not llm:
        doc.add_paragraph("LLM review not run. Execute run_llm_reviewer.py to add AI analysis.")
    else:
        # Mode badge
        mode_text = "Claude API (Live)" if llm["mode"] == "live" else "Mock Mode (no API key)"
        p = doc.add_paragraph()
        p.add_run(f"Mode: {mode_text}  |  Reviewed at: {llm.get('reviewed_at', 'N/A')}").italic = True
        doc.add_paragraph()

        # Final LLM decision banner
        llm_decision = llm.get("final_decision", "N/A")
        llm_color    = COLOR_GREEN if llm_decision == "PASS" else (
                       COLOR_GREY  if llm_decision == "PASS_WITH_WARNINGS" else COLOR_RED)
        banner = doc.add_paragraph()
        run    = banner.add_run(f"  LLM DECISION: {llm_decision}  ")
        run.bold           = True
        run.font.size      = Pt(13)
        run.font.color.rgb = llm_color
        doc.add_paragraph()

        # Risk level
        risk     = llm.get("risk_level", "N/A")
        risk_col = COLOR_GREEN if "LOW" in str(risk) else (
                   COLOR_GREY  if "MEDIUM" in str(risk) else COLOR_RED)
        rp  = doc.add_paragraph()
        rr  = rp.add_run(f"Risk Level: {risk}")
        rr.bold           = True
        rr.font.color.rgb = risk_col
        doc.add_paragraph()

        # Content sections
        sections = [
            ("Decision Reasoning",   "decision_reasoning"),
            ("Difference Analysis",  "difference_analysis"),
            ("SQL Fix",              "sql_fix"),
        ]
        for title, key in sections:
            val = llm.get(key)
            if val:
                h = doc.add_heading(title, level=2)
                for run in h.runs:
                    run.font.color.rgb = COLOR_BLUE
                for line in val.splitlines():
                    if line.strip():
                        doc.add_paragraph(line.strip())
                doc.add_paragraph()

    # ==============================================================
    # SAVE
    # ==============================================================
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path      = os.path.join(OUTPUT_DIR, f"validation_report_{timestamp}.docx")
    doc.save(path)
    return path


# ==================================================================
# MAIN
# ==================================================================

def main():
    print("\n" + SEPARATOR)
    print("  VALIDATION AGENT - REPORTER")
    print(SEPARATOR + "\n")

    # Step 1: Run comparison
    print("STEP 1: Running comparison")
    print(STEP_LINE)
    try:
        results = run_comparison()
    except Exception as e:
        logger.error(f"Comparison failed: {e}")
        logger.error("Make sure run_executor.py ran successfully first.")
        sys.exit(1)
    print()

    # Step 2: LLM Review
    print("STEP 2: LLM Review")
    print(STEP_LINE)
    try:
        import importlib.util, pathlib
        _root = pathlib.Path(__file__).parent
        spec  = importlib.util.spec_from_file_location(
            "run_llm_reviewer", str(_root / "run_llm_reviewer.py")
        )
        llm_mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(llm_mod)
        results = llm_mod.run_llm_review(results)
    except Exception as e:
        logger.warning(f"LLM review skipped: {e}")
        logger.warning("Report will be generated without LLM section.")
    print()

    # Step 3: Generate report
    print("STEP 3: Generating Word report")
    print(STEP_LINE)
    try:
        report_path = build_report(results)
        logger.success(f"Report saved: {report_path}")
    except Exception as e:
        logger.error(f"Report generation failed: {e}")
        sys.exit(1)
    print()

    # Final summary
    print(SEPARATOR)
    comparison_decision = results["decision"]
    llm_decision        = results.get("llm_review", {}).get("final_decision")
    risk_level          = results.get("llm_review", {}).get("risk_level", "N/A")
    final_decision      = llm_decision or comparison_decision

    logger.info(f"Comparison decision : {comparison_decision}")
    if llm_decision:
        if llm_decision == "PASS":
            logger.success(f"LLM decision        : {llm_decision}")
        elif llm_decision == "PASS_WITH_WARNINGS":
            logger.warning(f"LLM decision        : {llm_decision}")
        else:
            logger.error(f"LLM decision        : {llm_decision}")
    logger.info(f"Risk level          : {risk_level}")
    logger.info(f"Match rate          : {results['match_rate']:.2%}")
    logger.info(f"Report              : {report_path}")
    print(SEPARATOR + "\n")

    return final_decision in ["PASS", "PASS_WITH_WARNINGS"]


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)