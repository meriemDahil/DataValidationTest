from data_loader import DatasetLoader
from validators.base_validator import Canonicalizer
from validators.schema_validator import StructuralValidator

from validators.data_validation import DataValidator


df_a = DatasetLoader.load_dataset("test-2025v2(1).csv")
df_b = DatasetLoader.load_dataset("test-2025v2(1) copy.csv")


df_a = Canonicalizer.canonicalize(df_a)
df_b = Canonicalizer.canonicalize(df_b, reference_columns=df_a.columns)


structural_result = StructuralValidator.validate(df_a, df_b)
validation_result = DataValidator.validate(df_a, df_b)

print("Structural Validation Result:")
print(structural_result)
print("\nData Validation Result:")
print(validation_result)