import yaml
from typing import Dict, Any


def load_series_config(path: str) -> Dict[str, Any]:
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)
    if not isinstance(cfg, dict) or "series" not in cfg:
        raise ValueError("series.yaml must contain a top-level 'series' mapping")
    return cfg["series"]


def load_viz_config(path: str) -> Dict[str, Any]:
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)
    required = [
        "lookback_months",
        "rolling_window_months",
        "mode",
        "cluster",
        "color_scale",
    ]
    for key in required:
        if key not in cfg:
            raise ValueError(f"viz.yaml missing required key: {key}")
    return cfg

