from __future__ import annotations

import datetime as dt
from typing import Optional, List

import pandas as pd
import yfinance as yf
from .fred import fetch_fred_series
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    })
    retry = Retry(total=3, backoff_factor=0.4, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def _try_yf(tick: str) -> pd.DataFrame:
    sess = _make_session()
    # Attempt 1: bulk download
    data = yf.download(tick, auto_adjust=False, progress=False, threads=False, session=sess)
    if data is None or data.empty:
        # Attempt 2: Ticker().history which sometimes bypasses tz failures
        t = yf.Ticker(tick, session=sess)
        hist = t.history(period="max", auto_adjust=False)
        if hist is None or hist.empty:
            raise ValueError("empty")
        data = hist
    cols = list(data.columns)
    # Some responses are multi-index if multiple tickers requested; ensure single
    if isinstance(data.columns, pd.MultiIndex):
        # pick Close column for the provided ticker
        close = data.loc[:, (slice(None), "Close")]
        # if only one level left, squeeze
        if close.shape[1] == 1:
            s = close.iloc[:, 0]
        else:
            # choose the first column
            s = close.iloc[:, 0]
        df = s.reset_index().rename(columns={s.name: "value", s.index.name or "index": "date"})
    else:
        use_col = "Close" if "Close" in data.columns else ("Adj Close" if "Adj Close" in data.columns else None)
        if use_col is None:
            raise ValueError("no close column")
        df = data.reset_index()[["Date", use_col]].rename(columns={"Date": "date", use_col: "value"})
    df["date"] = pd.to_datetime(df["date"])
    return df.dropna(subset=["date"]).sort_values("date")


def fetch_yahoo_series(ticker: str, timeout: int = 20) -> pd.DataFrame:
    """Fetch Yahoo Finance series (close) using yfinance.

    Returns a DataFrame with columns: [date, value]
    """
    # yfinance does not expose timeout directly; rely on default
    try:
        return _try_yf(ticker)
    except Exception:
        # Try Yahoo alternates first (some environments block certain tickers)
        alternates: dict[str, List[str]] = {
            "XAUUSD=X": ["GC=F", "GLD"],
            "GLD": ["GC=F", "XAUUSD=X"],
            "GC=F": ["XAUUSD=X", "GLD"],
            "^VIX": ["VIXY"],
        }
        for alt in alternates.get(ticker, []):
            try:
                return _try_yf(alt)
            except Exception:
                continue

        # Fallback to FRED equivalents for common tickers
        fred_map = {
            "^GSPC": "SP500",
            "^VIX": "VIXCLS",
            # Gold proxies to LBMA PM fix if Yahoo is unavailable
            "XAUUSD=X": "GOLDPMGBD228NLBM",
            "GC=F": "GOLDPMGBD228NLBM",
            "GLD": "GOLDPMGBD228NLBM",
        }
        fred_id = fred_map.get(ticker)
        if fred_id:
            df = fetch_fred_series(fred_id)
            return df  # already date/value
        raise ValueError(f"No data for {ticker}")


def save_raw(df: pd.DataFrame, series_name: str, raw_dir: str) -> str:
    today = dt.date.today().isoformat()
    path = f"{raw_dir}/{series_name}_{today}.csv"
    out = df.rename(columns={"value": series_name})[["date", series_name]]
    out.to_csv(path, index=False)
    return path
