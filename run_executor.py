"""
run_executor.py
---------------
1. Reads DDL from  ddl/schema.sql        → creates tables in SQLite (in-memory)
2. Loads CSVs from data/*.csv            → populates each table by matching filename to table name
3. Executes SQL from sql/transformation.sql → runs the transformation
4. Saves result to data/talend_reference.csv  (acts as the Talend reference output)
   AND      to data/stg_output.csv            (acts as the SQL staging output)

In a real migration both files would come from different systems.
Here we generate both from the same query so the comparator starts at PASS,
giving you a known-good baseline to break intentionally for testing.

Usage:
    python run_executor.py
"""

import os
import sqlite3
import pandas as pd
from pathlib import Path
from loguru import logger

# ------------------------------------------------------------------
# Paths
# ------------------------------------------------------------------
BASE_DIR        = Path(__file__).parent
DDL_PATH        = BASE_DIR / "ddl"  / "schema.sql"
SQL_PATH        = BASE_DIR / "scripts"  / "transformations.sql"
DATA_DIR        = BASE_DIR / "data"
TALEND_OUT      = DATA_DIR / "talend_reference.csv"
STG_OUT         = DATA_DIR / "stg_output.csv"

# Map CSV filename → table name (filename without .csv = table name)
# All CSVs in data/ that match a CREATE TABLE in the DDL are loaded automatically.


def load_ddl(conn: sqlite3.Connection, ddl_path: Path):
    """Execute the DDL script to create all tables."""
    logger.info(f"Loading DDL from {ddl_path}")
    ddl = ddl_path.read_text(encoding="utf-8")
    conn.executescript(ddl)
    conn.commit()
    logger.success("Tables created successfully")


def load_csvs(conn: sqlite3.Connection, data_dir: Path):
    """
    Load every CSV in data_dir into the SQLite connection.
    The table name is derived from the CSV filename (without extension).
    Skips files that don't match an existing table.
    """
    # Get existing table names from the DB
    cursor     = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    db_tables  = {row[0] for row in cursor.fetchall()}

    for csv_file in sorted(data_dir.glob("*.csv")):
        table_name = csv_file.stem  # e.g. dim_customer.csv → dim_customer
        if table_name not in db_tables:
            logger.warning(f"Skipping {csv_file.name} — no matching table '{table_name}' in DB")
            continue

        df = pd.read_csv(csv_file)
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        logger.success(f"Loaded {csv_file.name} → table '{table_name}'  ({len(df)} rows)")


def run_transformation(conn: sqlite3.Connection, sql_path: Path) -> pd.DataFrame:
    """Execute the transformation SQL and return the result as a DataFrame."""
    logger.info(f"Running transformation from {sql_path}")
    sql    = sql_path.read_text(encoding="utf-8")
    result = pd.read_sql_query(sql, conn)
    logger.success(f"Transformation complete — {len(result)} rows returned")
    return result


def save_outputs(result: pd.DataFrame):
    """
    Save the transformation result as both the Talend reference and SQL staging output.
    In a real scenario these would come from two different systems.
    Here both are identical to give the comparator a clean PASS baseline.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    result.to_csv(TALEND_OUT, index=False)
    result.to_csv(STG_OUT,    index=False)
    logger.success(f"Saved Talend reference → {TALEND_OUT}")
    logger.success(f"Saved SQL staging output → {STG_OUT}")


def preview(result: pd.DataFrame):
    logger.info("Transformation result preview:")
    print(result.to_string(index=False))


# ------------------------------------------------------------------
# DB helper used by the comparator (load_table_as_dataframe shim)
# ------------------------------------------------------------------

def get_connection() -> sqlite3.Connection:
    """
    Returns a fresh in-memory SQLite connection with all tables loaded.
    Used by the comparator's db.py if you point it here.
    """
    conn = sqlite3.connect(":memory:")
    load_ddl(conn, DDL_PATH)
    load_csvs(conn, DATA_DIR)
    return conn


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    logger.info("=" * 60)
    logger.info("  SQL EXECUTOR — mock pipeline")
    logger.info("=" * 60)

    conn = sqlite3.connect(":memory:")

    try:
        load_ddl(conn, DDL_PATH)
        load_csvs(conn, DATA_DIR)
        result = run_transformation(conn, SQL_PATH)
        preview(result)
        save_outputs(result)
    finally:
        conn.close()

    logger.info("=" * 60)
    logger.info("  Done. Run run_comparator.py to validate.")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()