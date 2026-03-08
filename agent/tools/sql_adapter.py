"""
agent/tools/sql_adapter.py
──────────────────────────
Rewrites PostgreSQL/Snowflake SQL to SQLite-compatible SQL.
Only active when DB_TYPE=sqlite in your .env.

When you switch to a real database later, this is bypassed automatically.
"""

import os
import re
from loguru import logger


# ── What gets replaced ────────────────────────────────────────────────────────
#
# PostgreSQL → SQLite
#
# Data types:
#   SERIAL          → INTEGER
#   VARCHAR(n)      → TEXT
#   BOOLEAN         → INTEGER  (SQLite has no bool, uses 0/1)
#   TIMESTAMP       → TEXT
#   NUMERIC(p,s)    → REAL
#
# Functions:
#   NOW()           → datetime('now')
#   TRUE / FALSE    → 1 / 0
#
# Syntax:
#   value::TEXT     → value     (PostgreSQL cast removed)
#   schema.table    → table     (SQLite has no schemas)

_REPLACEMENTS = [
    # Data types — order matters (more specific first)
    (r"\bBIGSERIAL\b",                          "INTEGER"),
    (r"\bSERIAL\b",                              "INTEGER"),
    (r"\bVARCHAR\s*\(\s*\d+\s*\)",              "TEXT"),
    (r"\bNVARCHAR\s*\(\s*\d+\s*\)",             "TEXT"),
    (r"\bVARCHAR\b",                             "TEXT"),
    (r"\bBOOLEAN\b",                             "INTEGER"),
    (r"\bTIMESTAMP\s+WITH\s+TIME\s+ZONE\b",     "TEXT"),
    (r"\bTIMESTAMP\b",                           "TEXT"),
    (r"\bDATETIME\b",                            "TEXT"),
    (r"\bDOUBLE\s+PRECISION\b",                  "REAL"),
    (r"\bFLOAT\b",                               "REAL"),
    (r"\bNUMERIC\s*\(\s*\d+\s*,\s*\d+\s*\)",   "REAL"),
    (r"\bNUMERIC\b",                             "REAL"),

    # Functions
    (r"\bNOW\s*\(\s*\)",                         "datetime('now')"),
    (r"\bGETDATE\s*\(\s*\)",                     "datetime('now')"),
    (r"\bCURRENT_TIMESTAMP\b",                   "datetime('now')"),
    (r"\bCURRENT_DATE\b",                        "date('now')"),
    (r"\bTRUE\b",                                "1"),
    (r"\bFALSE\b",                               "0"),
    (r"\bIFNULL\b",                              "COALESCE"),
]

# PostgreSQL cast: value::TYPE or value::TYPE(n)
_CAST_PATTERN = re.compile(
    r"::\s*\w+(\s*\(\s*\d+\s*(?:,\s*\d+\s*)?\))?",
    re.IGNORECASE
)

# Schema-qualified table names: schema.table → table
# Matches common staging prefixes
_SCHEMA_PATTERN = re.compile(
    r"\b\w+\.(stg_\w+|\w+_staging|\w+_raw|\w+_output)\b",
    re.IGNORECASE
)


def adapt_sql_for_sqlite(script: str) -> str:
    """
    Rewrite a SQL script so it runs on SQLite.
    Returns the script unchanged if DB_TYPE != sqlite.
    Logs a summary of what was changed.
    """
    if os.getenv("DB_TYPE", "sqlite") != "sqlite":
        return script  # passthrough in production

    original = script
    adapted = script

    # Remove PostgreSQL cast syntax
    adapted = _CAST_PATTERN.sub("", adapted)

    # Remove schema prefixes
    adapted = _SCHEMA_PATTERN.sub(r"\1", adapted)

    # Apply type and function replacements
    for pattern, replacement in _REPLACEMENTS:
        adapted = re.sub(pattern, replacement, adapted, flags=re.IGNORECASE)

    if adapted != original:
        logger.info("SQL adapted for SQLite (types, functions, casts rewritten)")
    else:
        logger.info("SQL needed no adaptation for SQLite")

    return adapted