# core/dataset_loader.py

from pathlib import Path
from typing import Dict, Any
import pandas as pd


class DatasetLoader:
    """
    Responsible for loading datasets from disk.
    Supported formats: CSV, Parquet.
    """

    SUPPORTED_FORMATS = {".csv", ".parquet"}

    @staticmethod
    def load_dataset(path: str) -> pd.DataFrame:
        file_path = Path(path)

        if not file_path.exists():
            raise FileNotFoundError(f"Dataset not found: {path}")

        if file_path.suffix not in DatasetLoader.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported file format: {file_path.suffix}")

        if file_path.suffix == ".csv":
            df = pd.read_csv(file_path)

        elif file_path.suffix == ".parquet":
            df = pd.read_parquet(file_path)

        return df

    @staticmethod
    def dataset_metadata(df: pd.DataFrame) -> Dict[str, Any]:
        """
        Extract metadata used in validation reports.
        """

        return {
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": list(df.columns),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
        }