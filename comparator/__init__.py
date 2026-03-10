"""
comparator/
-----------
4-layer Talend-to-SQL validation pipeline.

Public API
----------
from comparator import ComparatorPipeline

results = ComparatorPipeline().run()
"""

from .pipline import ComparatorPipeline
from .schema   import infer_schema, normalize
from .layer1   import Layer1Structural
from .layer2   import Layer2Data
from .layer3   import Layer3BusinessRules
from .layer4   import Layer4Statistical

__all__ = [
    "ComparatorPipeline",
    "infer_schema",
    "normalize",
    "Layer1Structural",
    "Layer2Data",
    "Layer3BusinessRules",
    "Layer4Statistical",
]