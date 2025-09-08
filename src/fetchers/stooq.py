from __future__ import annotations

import datetime as dt
from typing import Optional

import pandas as pd
from pandas_datareader import data as pdr
import requests
import io


def fetch_stooq_series(symbol: str) -> pd.DataFrame:
    """Fetch series from Stooq.

    Strategy:
    1) Try direct CSV endpoint (full history): https://stooq.com/q/d/l/?s=<symbol>&i=d
       Use lowercase and ensure '.us' suffix for US instruments when appropriate.
    2) Fallback to pandas-datareader('stooq') if direct fails.

    Returns columns: [date, value] with Close prices.
    """
    sym = symbol.strip()
    # Normalize common cases
    if sym.upper() in {"SPY", "SPY.US", "SPX", "^SPX"}:
        sym_csv = "spy.us"
    else:
        sym_csv = sym.lower()
    if not sym_csv.endswith(".us") and sym_csv.isalpha() and len(sym_csv) <= 5:
        # Try adding US market suffix for short US tickers
        sym_csv = f"{sym_csv}.us"

    url = f"https://stooq.com/q/d/l/?s={sym_csv}&i=d"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Accept": "text/csv,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    try:
        r = requests.get(url, headers=headers, timeout=20)
        r.raise_for_status()
        if r.text and r.text.lower().startswith("date,open,high,low,close"):
            dfd = pd.read_csv(io.StringIO(r.text))
            if dfd.empty:
                raise ValueError("empty csv")
            dfd["Date"] = pd.to_datetime(dfd["Date"])  # ensure datetime
            use_col = "Close" if "Close" in dfd.columns else ("Adj Close" if "Adj Close" in dfd.columns else None)
            if use_col is None:
                # fallback: first numeric
                numeric_cols = [c for c in dfd.columns if pd.api.types.is_numeric_dtype(dfd[c])]
                use_col = numeric_cols[0]
            out = dfd[["Date", use_col]].rename(columns={"Date": "date", use_col: "value"})
            out = out.dropna(subset=["date"]).sort_values("date")
            return out
    except Exception:
        pass

    # Fallback to pandas-datareader
    try_syms = [symbol, symbol.upper(), symbol.lower(), "SPY", "SPY.US", "spy", "spy.us"] if symbol.upper() in {"SPY", "SPX", "^SPX", "SPY.US"} else [symbol, symbol.upper(), symbol.lower()]
    best_df = pd.DataFrame()
    for cand in try_syms:
        try:
            df = pdr.DataReader(cand, "stooq")
        except Exception:
            df = pd.DataFrame()
        if df is None or df.empty:
            continue
        df_reset = df.reset_index()
        if "Date" not in df_reset.columns:
            continue
        df_reset["Date"] = pd.to_datetime(df_reset["Date"])
        if best_df.empty or df_reset["Date"].min() < best_df.reset_index()["Date"].min():
            best_df = df
    if best_df.empty:
        raise ValueError(f"No data from Stooq for {symbol}")
    df = best_df
    use_col = "Close" if "Close" in df.columns else ("Adj Close" if "Adj Close" in df.columns else None)
    if use_col is None:
        # fallback: first numeric column
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        if not numeric_cols:
            raise ValueError("No numeric columns in Stooq response")
        use_col = numeric_cols[0]
    out = df.reset_index()[["Date", use_col]].rename(columns={"Date": "date", use_col: "value"})
    out["date"] = pd.to_datetime(out["date"])  # ensure datetime
    out = out.dropna(subset=["date"]).sort_values("date")
    return out


def save_raw(df: pd.DataFrame, series_name: str, raw_dir: str) -> str:
    today = dt.date.today().isoformat()
    path = f"{raw_dir}/{series_name}_{today}.csv"
    out = df.rename(columns={"value": series_name})[["date", series_name]]
    out.to_csv(path, index=False)
    return path
