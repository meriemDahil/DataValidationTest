"""
agent/tools/db.py
-----------------
Database connection and SQL execution.
Uses SQLite — no installation needed on Windows.
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

# -- Connection setup ----------------------------------------------------------

DB_TYPE = os.getenv("DB_TYPE", "sqlite")
DB_PATH = os.getenv("DB_PATH", "./data/validation_test.db")


def _get_engine():
    """
    Build SQLAlchemy engine.
    Called fresh each time so reset_database() takes effect.
    """
    if DB_TYPE == "sqlite":
        # Make sure the data/ folder exists
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        url = f"sqlite:///{DB_PATH}"
        return create_engine(
            url,
            connect_args={"check_same_thread": False},
            echo=False
        )
    elif DB_TYPE == "postgres":
        url = (
            f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
            f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}"
            f"/{os.getenv('DB_NAME')}"
        )
        return create_engine(url, echo=False)
    else:
        raise ValueError(f"Unknown DB_TYPE: {DB_TYPE}")


# -- Public functions ----------------------------------------------------------

def reset_database() -> None:
    """
    Wipe the database before each run.
    Guarantees a clean state every time — replaces Docker.
    """
    if DB_TYPE == "sqlite":
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
            logger.info(f"SQLite file deleted: {DB_PATH}")
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        logger.info("Database reset — fresh start")
    else:
        engine = _get_engine()
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS stg_output CASCADE"))
            conn.commit()
        logger.info("Staging tables dropped")


def execute_sql_script(script: str) -> dict:
    """
    Execute a SQL script (can contain multiple statements).
    Splits on semicolons and runs each statement one by one.
    This is required for SQLite compatibility.

    Returns a result dict with status, row counts, and any error.
    """
    engine = _get_engine()

    # Split into individual statements, skip empty ones
    statements = [
        s.strip()
        for s in script.split(";")
        if s.strip()
    ]

    logger.info(f"Found {len(statements)} SQL statement(s) to execute")

    executed = 0
    with engine.connect() as conn:
        try:
            for i, stmt in enumerate(statements, 1):
                logger.info(f"Executing statement {i}/{len(statements)}...")
                logger.debug(f"SQL: {stmt[:120]}...")  # log first 120 chars
                conn.execute(text(stmt))
                executed += 1

            conn.commit()
            logger.success(f"All {executed} statements executed successfully")

            return {
                "status": "success",
                "statements_executed": executed,
                "total_statements": len(statements),
                "error": None,
            }

        except Exception as e:
            logger.error(f"Failed at statement {executed + 1}: {e}")
            logger.error(f"Failing SQL: {statements[executed][:300]}")
            return {
                "status": "failed",
                "statements_executed": executed,
                "total_statements": len(statements),
                "error": str(e),
            }


def list_tables() -> list[str]:
    """List all tables currently in the database."""
    engine = _get_engine()
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    return tables


def preview_table(table_name: str, rows: int = 5) -> pd.DataFrame:
    """
    Load the first N rows of a table into a DataFrame.
    Used to visually confirm the staging table was populated correctly.
    """
    engine = _get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(
            f"SELECT * FROM {table_name} LIMIT {rows}",
            conn
        )
    return df


def get_row_count(table_name: str) -> int:
    """Return the total row count of a table."""
    engine = _get_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT COUNT(*) FROM {table_name}")
        )
        return result.scalar()


def load_table_as_dataframe(table_name: str) -> pd.DataFrame:
    """
    Load a full table from the database into a pandas DataFrame.
    Used by the comparator to read stg_output after execution.
    """
    engine = _get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    logger.info(f"Loaded '{table_name}': {len(df)} rows x {len(df.columns)} columns")
    return df