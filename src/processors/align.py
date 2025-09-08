from __future__ import annotations

import glob
from typing import Dict, List, Tuple

import pandas as pd


def load_or_concat_raw(raw_dir: str, series_name: str) -> pd.DataFrame:
    """Load the most recent raw file for a series (pattern: series_YYYY-MM-DD.csv)."""
    pattern = f"{raw_dir}/{series_name}_*.csv"
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No raw files matching {pattern}")
    latest = files[-1]
    df = pd.read_csv(latest)
    if "date" not in df.columns or series_name not in df.columns:
        raise ValueError(f"Raw file {latest} missing expected columns: date, {series_name}")
    df["date"] = pd.to_datetime(df["date"])  # ensure datetime
    return df[["date", series_name]].dropna(subset=["date"]).sort_values("date")


def merge_series(series_frames: List[pd.DataFrame]) -> pd.DataFrame:
    """Outer-join on date across a list of single-column frames.

    Accepts frames that either have a 'date' column or a DatetimeIndex.
    Returns a DataFrame with a 'date' column for downstream steps.
    """
    if not series_frames:
        raise ValueError("No series provided to merge")
    # Normalize to have a 'date' column
    normed = []
    for df in series_frames:
        if "date" in df.columns:
            tmp = df.copy()
        else:
            # Assume DatetimeIndex
            idx = df.index
            if not isinstance(idx, pd.DatetimeIndex):
                raise ValueError("Series frame must have 'date' column or DatetimeIndex")
            tmp = df.reset_index().rename(columns={df.index.name or "index": "date"})
        normed.append(tmp)

    out = normed[0]
    for df in normed[1:]:
        out = out.merge(df, on="date", how="outer")
    out = out.sort_values("date")
    return out
