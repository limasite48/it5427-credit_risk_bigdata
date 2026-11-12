"""
Data Loading & Memory Optimization Module
"""
from pathlib import Path
from typing import Optional, Union
import numpy as np
import pandas as pd


def optimize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Downcast numeric columns to reduce memory footprint."""
    for col in df.columns:
        if pd.api.types.is_float_dtype(df[col]):
            df[col] = df[col].astype(np.float32)
        elif pd.api.types.is_integer_dtype(df[col]):
            if col != "SK_ID_CURR" and col != "TARGET":
                c_min = df[col].min()
                c_max = df[col].max()
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
    return df


def load_application_data(
    file_path: Union[str, Path],
    nrows: Optional[int] = None,
    optimize_memory: bool = True
) -> pd.DataFrame:
    """Load application dataset with memory optimization."""
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Data file not found at {path}")

    df = pd.read_csv(path, nrows=nrows)
    if optimize_memory:
        df = optimize_dtypes(df)
    return df
