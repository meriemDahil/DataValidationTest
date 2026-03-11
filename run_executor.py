"""
run_executor.py
---------------
1. Resets the database (deletes pipeline.db if exists)
2. Reads DDL from  ddl/schema.sql             → creates tables via db.execute_sql_script()
3. Loads CSVs from data/*.csv                 → populates each table via SQLAlchemy
4. Executes SQL from scripts/transformations.sql → runs the transformation
5. Saves result as table 'stg_output' in pipeline.db  (queried by the comparator)
6. Saves result to data/talend_reference.csv          (Talend baseline for comparator)

Usage:
    python run_executor.py
"""

import pandas as pd
from pathlib import Path
from loguru import logger

from agent.tools.db import (
    reset_database,
    execute_sql_script,
    list_tables,
    load_table_as_dataframe,
    _get_engine,
)

# ------------------------------------------------------------------
# Paths
# ------------------------------------------------------------------
BASE_DIR   = Path(__file__).parent
DDL_PATH   = BASE_DIR / "ddl"     / "schema.sql"
SQL_PATH   = BASE_DIR / "scripts" / "transformations.sql"
DATA_DIR   = BASE_DIR / "data"
TALEND_OUT = DATA_DIR / "talend_reference.csv"


# ------------------------------------------------------------------
# Pipeline steps
# ------------------------------------------------------------------

def load_ddl(ddl_path: Path):
    """Read and execute the DDL script to create all tables."""
    logger.info(f"Loading DDL from {ddl_path}")
    ddl    = ddl_path.read_text(encoding="utf-8")
    result = execute_sql_script(ddl)
    if result["status"] != "success":
        raise RuntimeError(f"DDL execution failed: {result['error']}")
    logger.success("Tables created successfully")


def load_csvs(data_dir: Path):
    """
    Load every CSV in data_dir into the database.
    Table name is derived from the CSV filename (without extension).
    Skips files that don't match an existing table in the DDL.
    """
    engine    = _get_engine()
    db_tables = set(list_tables())

    for csv_file in sorted(data_dir.glob("*.csv")):
        table_name = csv_file.stem
        if table_name not in db_tables:
            logger.warning(f"Skipping {csv_file.name} — no matching table '{table_name}' in DB")
            continue
        df = pd.read_csv(csv_file)
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        logger.success(f"Loaded {csv_file.name} → table '{table_name}'  ({len(df)} rows)")


def run_transformation(sql_path: Path) -> pd.DataFrame:
    """Execute the transformation SQL and return the result as a DataFrame."""
    logger.info(f"Running transformation from {sql_path}")
    sql    = sql_path.read_text(encoding="utf-8")
    engine = _get_engine()

    with engine.connect() as conn:
        result = pd.read_sql(sql, conn)

    logger.success(f"Transformation complete — {len(result)} rows returned")
    return result


def save_outputs(result: pd.DataFrame):
    """
    Persist the transformation result in two places:
      1. DB table 'stg_output'       → queried by the comparator via load_table_as_dataframe()
      2. data/talend_reference.csv   → CSV baseline read by the comparator
    """
    engine = _get_engine()

    # Write into the DB so the comparator can query it
    result.to_sql("stg_output", engine, if_exists="replace", index=False)
    logger.success("Saved result → DB table 'stg_output'")

    # Write Talend reference CSV (acts as the expected/baseline output)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    result.to_csv(TALEND_OUT, index=False)
    logger.success(f"Saved Talend reference → {TALEND_OUT}")


def preview(result: pd.DataFrame):
    """Display the transformation result in a human-readable table format."""
    logger.info("Transformation result preview:")
    print(result.to_string(index=False))


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    logger.info("=" * 60)
    logger.info("  SQL EXECUTOR — mock pipeline")
    logger.info("=" * 60)

    # Step 1: Wipe existing DB for a clean run
    reset_database()

    # Step 2: Create tables from DDL schema
    load_ddl(DDL_PATH)

    # Step 3: Populate tables from CSV files
    load_csvs(DATA_DIR)

    # Step 4: Run the SQL transformation
    result = run_transformation(SQL_PATH)
    preview(result)

    # Step 5: Save to DB (stg_output table) + talend_reference.csv
    save_outputs(result)

    logger.info("=" * 60)
    logger.info("  Done. Run run_comparator.py to validate.")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()