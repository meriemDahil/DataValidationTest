"""
run_executor.py
---------------
Standalone script to test SQL execution only.
Run this first before touching any other part of the pipeline.

Usage:
    python run_executor.py                        # uses built-in sample SQL
    python run_executor.py scripts/my_script.sql  # uses your own SQL file
"""

import sys
import os
from loguru import logger

# Add project root to path so imports work
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from agent.tools.db import (
    reset_database,
    execute_sql_script,
    list_tables,
    preview_table,
    get_row_count,
)
from agent.tools.sql_adapter import adapt_sql_for_sqlite


# -- Configure logger for readable output -------------------------------------
logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}",
    colorize=True,
    level="DEBUG",
)


# -- Sample SQL ----------------------------------------------------------------
# This mimics what a migration agent would generate.
# Replace this with your real SQL or pass a file path as argument.

SAMPLE_SQL = "scripts/migration.sql"

with open(SAMPLE_SQL, "r") as f:
    SAMPLE_SQL = f.read()
def run(sql_script: str) -> bool:
    """
    Execute a SQL script and print a full diagnostic report.
    Returns True if successful, False if failed.
    """

    print("\n" + "=" * 60)
    print("  VALIDATION AGENT — SQL EXECUTION STEP")
    print("=" * 60 + "\n")

    # -- Step 1: Reset database ----------------------------------------
    print("-- STEP 1: Reset Database ----------------------------------")
    reset_database()
    print()

    # -- Step 2: Adapt SQL for SQLite if needed ------------------------
    print("-- STEP 2: SQL Adaptation ----------------------------------")
    adapted_sql = adapt_sql_for_sqlite(sql_script)
    print()

    # -- Step 3: Execute SQL -------------------------------------------
    print("-- STEP 3: Execute SQL Script ------------------------------")
    result = execute_sql_script(adapted_sql)
    print()

    # -- Step 4: Report execution result ------------------------------
    print("-- STEP 4: Execution Result --------------------------------")
    if result["status"] == "success":
        logger.success(
            f"OK Executed {result['statements_executed']} / "
            f"{result['total_statements']} statements successfully"
        )
    else:
        logger.error(f"FAILED Execution FAILED")
        logger.error(f"  Error: {result['error']}")
        logger.error(
            f"  Failed at statement "
            f"{result['statements_executed'] + 1} / "
            f"{result['total_statements']}"
        )
        print("\n" + "=" * 60)
        print("  RESULT: FAILED FAILED")
        print("=" * 60 + "\n")
        return False
    print()

    # -- Step 5: Verify tables were created ----------------------------
    print("-- STEP 5: Tables in Database ------------------------------")
    tables = list_tables()
    if tables:
        for t in tables:
            row_count = get_row_count(t)
            logger.info(f"  Table: '{t}' -> {row_count} rows")
    else:
        logger.warning("  No tables found in database after execution!")
    print()

    # -- Step 6: Preview staging table --------------------------------
    print("-- STEP 6: Data Preview (stg_output) ----------------------")
    if "stg_output" in tables:
        total_rows = get_row_count("stg_output")
        logger.info(f"Total rows in stg_output: {total_rows}")
        print()

        df = preview_table("stg_output", rows=10)
        print(df.to_string(index=False))
        print()
    else:
        logger.warning(
            "Table 'stg_output' not found. "
            "Make sure your SQL creates a table named 'stg_output'."
        )

    # -- Final result --------------------------------------------------
    print("=" * 60)
    print("  RESULT: SUCCESS OK")
    print(f"  Database: {os.getenv('DB_PATH', './data/validation_test.db')}")
    print("  Ready for comparison step.")
    print("=" * 60 + "\n")

    return True


if __name__ == "__main__":
    # Allow passing a SQL file path as argument
    if len(sys.argv) > 1:
        sql_file = sys.argv[1]
        if not os.path.exists(sql_file):
            print(f"Error: File not found: {sql_file}")
            sys.exit(1)
        with open(sql_file, encoding="utf-8") as f:
            sql = f.read()
        logger.info(f"Using SQL from file: {sql_file}")
    else:
        logger.info("No file provided — using built-in sample SQL")
        sql = SAMPLE_SQL

    success = run(sql)
    sys.exit(0 if success else 1)