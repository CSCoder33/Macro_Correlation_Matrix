from .fred import fetch_fred_series
from .yahoo import fetch_yahoo_series
from .stooq import fetch_stooq_series

__all__ = [
    "fetch_fred_series",
    "fetch_yahoo_series",
    "fetch_stooq_series",
]
