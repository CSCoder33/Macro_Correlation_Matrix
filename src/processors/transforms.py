from __future__ import annotations

import pandas as pd
import numpy as np


def resample_monthly(df: pd.DataFrame, how: str = "last") -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])  # ensure datetime
    df = df.set_index("date").sort_index()
    if how == "last":
        out = df.resample("ME").last()
    elif how == "mean":
        out = df.resample("ME").mean()
    else:
        raise ValueError("how must be 'last' or 'mean'")
    return out


def compute_returns(series: pd.Series) -> pd.Series:
    return series.pct_change() * 100.0


def compute_yoy(series: pd.Series) -> pd.Series:
    return series.pct_change(12) * 100.0


def apply_transforms(monthly_df: pd.DataFrame, transform: str) -> pd.Series:
    if transform == "level":
        return monthly_df.iloc[:, 0]
    elif transform == "return":
        return compute_returns(monthly_df.iloc[:, 0])
    elif transform == "yoy":
        return compute_yoy(monthly_df.iloc[:, 0])
    else:
        raise ValueError(f"Unknown transform: {transform}")
