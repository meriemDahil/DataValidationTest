"""
run_comparator.py
-----------------
Entry point for the 4-layer Talend-to-SQL validation pipeline.

All logic lives in the ``comparator/`` package:

    comparator/
        config.py   – thresholds & constants
        schema.py   – SchemaInferrer + normalize()
        layer1.py   – Layer1Structural
        layer2.py   – Layer2Data
        layer3.py   – Layer3BusinessRules
        layer4.py   – Layer4Statistical
        pipeline.py – ComparatorPipeline  (orchestrator)
        __init__.py – public API

Usage:
    python run_comparator.py
"""

import sys
from loguru import logger
from comparator import ComparatorPipeline

logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}",
    colorize=True,
    level="DEBUG",
)

if __name__ == "__main__":
    try:
        results = ComparatorPipeline().run()
        sys.exit(0 if results["decision"] == "PASS" else 1)
    except FileNotFoundError as exc:
        logger.error(f"File not found: {exc}")
        logger.error("Make sure run_executor.py ran successfully first.")
        sys.exit(1)
    except Exception as exc:
        logger.error(f"Unexpected error: {exc}")
        raise