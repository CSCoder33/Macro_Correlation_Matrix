from __future__ import annotations

import io
import datetime as dt
from typing import Optional

import pandas as pd
import requests
from requests import HTTPError


FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"


def fetch_fred_series(series_id: str, timeout: int = 20) -> pd.DataFrame:
    """Fetch a FRED series via the fredgraph CSV endpoint.

    Returns a DataFrame with columns: [date, value]
    """
    series_id_used = series_id
    url = FRED_CSV_URL.format(series_id=series_id_used)
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        text = resp.text
    except HTTPError as e:
        # Fallback: gold AM series sometimes 404s; try PM code
        if getattr(e.response, "status_code", None) == 404 and series_id == "GOLDAMGBD228NLBM":
            series_id_used = "GOLDPMGBD228NLBM"
            url = FRED_CSV_URL.format(series_id=series_id_used)
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            text = resp.text
        else:
            raise
    df = pd.read_csv(io.StringIO(text))
    # Expected columns: DATE or observation_date, and <series_id>
    value_col = series_id_used
    # FRED can use 'DATE' or 'observation_date' depending on endpoint/version
    date_col = None
    for cand in ("DATE", "date", "observation_date"):
        if cand in df.columns:
            date_col = cand
            break
    if date_col is None or value_col not in df.columns:
        raise ValueError(f"Unexpected FRED CSV columns for {series_id}: {df.columns}")
    out = df[[date_col, value_col]].rename(columns={date_col: "date", value_col: "value"})
    out["date"] = pd.to_datetime(out["date"])
    # Coerce '.' or 'NA' to NaN
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    return out.dropna(subset=["date"]).sort_values("date")


def save_raw(df: pd.DataFrame, series_name: str, raw_dir: str) -> str:
    today = dt.date.today().isoformat()
    path = f"{raw_dir}/{series_name}_{today}.csv"
    out = df.rename(columns={"value": series_name})[["date", series_name]]
    out.to_csv(path, index=False)
    return path
