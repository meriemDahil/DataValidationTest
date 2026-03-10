"""
comparator/config.py
--------------------
Shared thresholds and constants used across all layers.
"""

PASS_THRESHOLD        = 0.95
RELATIVE_TOLERANCE    = 0.01   # 1 %
NULL_RATIO_THRESHOLD  = 0.05   # 5 %
MAX_CATEGORICAL_RATIO = 0.20   # col is categorical if unique/total <= 20 %
MAX_AGG_PAIRS         = 9
PERCENTILES           = [0.50, 0.95, 0.99]

TALEND_REFERENCE_PATH = "data/talend_reference.csv"
STAGING_TABLE         = "stg_output"

SEPARATOR = "=" * 60
STEP_LINE = "-" * 60