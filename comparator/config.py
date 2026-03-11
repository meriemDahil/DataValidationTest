"""
comparator/config.py
--------------------
Shared thresholds and constants used across all layers.
"""

# ------------------------------------------------------------------
# Validation Thresholds — Control pass/fail criteria
# ------------------------------------------------------------------

# Minimum percentage of validations that must pass for overall success (95%)
PASS_THRESHOLD        = 0.95

# Maximum allowed relative difference for numeric value comparisons (1%)
# Used for floating-point comparisons with tolerance
RELATIVE_TOLERANCE    = 0.01

# Maximum proportion of NULL values acceptable in a column (5%)
# Columns exceeding this threshold are flagged as having significant missing data
NULL_RATIO_THRESHOLD  = 0.05

# Threshold for identifying categorical columns (20% unique values)
# If unique_values / total_rows <= 20%, the column is treated as categorical
MAX_CATEGORICAL_RATIO = 0.20

# Maximum number of aggregate function pairs to test
# Limits computational complexity when comparing aggregated data
MAX_AGG_PAIRS         = 9

# List of percentiles to compute during statistical analysis
# Used for distribution comparison between reference and staging data
PERCENTILES           = [0.50, 0.95, 0.99]

# ------------------------------------------------------------------
# File and Table Paths — Reference and staging data locations
# ------------------------------------------------------------------

# Path to the Talend reference file (expected/baseline data)
TALEND_REFERENCE_PATH = "data/talend_reference.csv"

# Table name for SQL staging output (actual transformation result)
STAGING_TABLE         = "stg_output"

# ------------------------------------------------------------------
# Output Formatting — Constants for visual report formatting
# ------------------------------------------------------------------

# Main section separator for reports (60 equal signs)
SEPARATOR = "=" * 60

# Sub-section separator for reports (60 hyphens)
STEP_LINE = "-" * 60