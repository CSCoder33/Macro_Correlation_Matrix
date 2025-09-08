from __future__ import annotations

import argparse
import datetime as dt
import os
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from src.config_loader import load_series_config, load_viz_config
from src.fetchers import (
    fetch_fred_series as fred_fetch,
    fetch_yahoo_series as yahoo_fetch,
    fetch_stooq_series as stooq_fetch,
)
from src.fetchers.fred import save_raw as save_raw_fred
from src.fetchers.yahoo import save_raw as save_raw_yahoo
from src.processors import resample_monthly, apply_transforms, merge_series, load_or_concat_raw
from src.visuals import plot_correlation_heatmap, build_rolling_correlation_animation


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
CONFIG_DIR = os.path.join(ROOT, "config")
DATA_DIR = os.path.join(ROOT, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
PROC_DIR = os.path.join(DATA_DIR, "processed")
FIG_DIR = os.path.join(ROOT, "reports", "figures")
ANIM_DIR = os.path.join(ROOT, "reports", "animations")


def ensure_dirs():
    for p in [DATA_DIR, RAW_DIR, PROC_DIR, FIG_DIR, ANIM_DIR]:
        os.makedirs(p, exist_ok=True)


def fetch_all(series_cfg: Dict[str, Dict[str, str]]) -> Dict[str, str]:
    paths = {}
    for name, meta in series_cfg.items():
        src = meta.get("source")
        sid = meta.get("id")
        try:
            if src == "fred":
                df = fred_fetch(sid)
                p = save_raw_fred(df, name, RAW_DIR)
            elif src == "yahoo":
                df = yahoo_fetch(sid)
                p = save_raw_yahoo(df, name, RAW_DIR)
            elif src == "stooq":
                df = stooq_fetch(sid)
                # reuse yahoo saver shape (date,value -> date,<name>)
                p = save_raw_yahoo(df, name, RAW_DIR)
            else:
                print(f"Unknown source for {name}: {src}")
                continue
            paths[name] = p
            print(f"Fetched {name} -> {p} ({len(df)} rows)")
        except Exception as e:
            print(f"WARN: Failed to fetch {name} ({src}:{sid}): {e}")
    return paths


def build_monthly_frames(series_cfg: Dict[str, Dict[str, str]], mode: str) -> pd.DataFrame:
    frames = []
    for name, meta in series_cfg.items():
        try:
            raw = load_or_concat_raw(RAW_DIR, name)
        except Exception as e:
            print(f"WARN: Missing raw for {name}, skipping: {e}")
            continue
        monthly = resample_monthly(raw[["date", name]].rename(columns={name: "value"}), how="last")
        transform = meta.get("transform", "level")
        # Decide per mode: in 'levels' keep levels for yields/spreads; price-like can be returns if desired
        if mode == "levels":
            series = monthly.iloc[:, 0]
        elif mode == "returns":
            if transform == "yoy":
                series = (monthly.iloc[:, 0].pct_change(12) * 100.0)
            elif transform == "return" or transform == "level":
                # Prices: return; yields/spreads: level (as configured)
                if transform == "return":
                    series = (monthly.iloc[:, 0].pct_change() * 100.0)
                else:
                    series = monthly.iloc[:, 0]
            else:
                series = monthly.iloc[:, 0]
        else:
            raise ValueError("mode must be 'levels' or 'returns'")
        frames.append(series.rename(name).to_frame())
    if not frames:
        raise ValueError("No frames to merge; check data availability")
    merged = merge_series(frames)
    # Forward-fill within monthly aggregation is already inherent; after merge, small FFill for single-step gaps
    # Fill small alignment gaps up to 3 months in either direction
    merged = merged.set_index("date").sort_index().ffill(limit=3).bfill(limit=3)
    # Keep USREC as indicator if present
    return merged


def compute_static_corr(df: pd.DataFrame, lookback_months: int) -> pd.DataFrame:
    tail = df.drop(columns=[c for c in df.columns if c.upper() == "USREC"], errors="ignore").tail(lookback_months)
    tail = tail.dropna(how="all", axis=1).dropna(how="all", axis=0)
    corr = tail.corr(method="pearson")
    return corr


def compute_rolling_corr(df: pd.DataFrame, window_months: int) -> Dict[pd.Timestamp, pd.DataFrame]:
    cols = [c for c in df.columns if c.upper() != "USREC"]
    X = df[cols].copy()
    out: Dict[pd.Timestamp, pd.DataFrame] = {}
    for i in range(window_months, len(X) + 1):
        window = X.iloc[i - window_months : i]
        # Require at least 2 non-empty columns and at least 2 rows with any data
        if window.dropna(how="all", axis=1).shape[1] < 2 or window.dropna(how="all").shape[0] < 2:
            continue
        # Pairwise correlations with very small min_periods so series appear earlier
        try:
            corr = window.corr(min_periods=2)
        except TypeError:
            # pandas < 1.5 fallback (no min_periods arg)
            corr = window.corr()
        out[X.index[i - 1]] = corr
    return out


def latest_stamp() -> str:
    return dt.date.today().isoformat()


def main():
    parser = argparse.ArgumentParser(description="Macro Correlation Matrix pipeline")
    parser.add_argument("--series", default=os.path.join(CONFIG_DIR, "series.yaml"))
    parser.add_argument("--viz", default=os.path.join(CONFIG_DIR, "viz.yaml"))
    parser.add_argument("--mode", choices=["levels", "returns"], default=None, help="Override viz.yaml mode")
    args = parser.parse_args()

    ensure_dirs()

    series_cfg = load_series_config(args.series)
    viz_cfg = load_viz_config(args.viz)
    mode = args.mode or viz_cfg.get("mode", "levels")
    lookback = int(viz_cfg["lookback_months"])
    roll_win = int(viz_cfg["rolling_window_months"])
    roll_lookback = int(viz_cfg.get("rolling_lookback_months", 300))
    color_scale = tuple(viz_cfg.get("color_scale", [-1.0, 1.0]))  # type: ignore
    cluster = bool(viz_cfg.get("cluster", True))
    min_series = int(viz_cfg.get("min_series_for_output", 5))

    # Attempt fetch; continue on failure (use existing raw or sample)
    fetch_all(series_cfg)

    # Build monthly dataset for selected mode
    try:
        df = build_monthly_frames(series_cfg, mode)
    except Exception as e:
        # Fallback to sample processed
        sample_levels = os.path.join(PROC_DIR, "sample_monthly_levels.csv")
        sample_returns = os.path.join(PROC_DIR, "sample_monthly_returns.csv")
        sample_path = sample_returns if mode == "returns" else sample_levels
        if os.path.exists(sample_path):
            print(f"FALLBACK: Using sample processed dataset: {sample_path}")
            df = pd.read_csv(sample_path, parse_dates=["date"]).set_index("date")
        else:
            raise

    # Sanity checks
    df = df.sort_index()
    if df.index.duplicated().any():
        df = df[~df.index.duplicated(keep="last")]

    available = [c for c in df.columns if c.upper() != "USREC"]
    if len(available) < min_series:
        raise SystemExit(f"Not enough series for output: have {len(available)}, need >= {min_series}")

    # Coverage log for debugging series inclusion in rolling windows
    try:
        print("Series coverage (non-NA) after monthly alignment:")
        for col in available:
            s = df[col].dropna()
            if not s.empty:
                print(f"  - {col}: {s.index.min().date()} -> {s.index.max().date()} ({len(s)} obs)")
            else:
                print(f"  - {col}: no data")
    except Exception:
        pass

    today = latest_stamp()

    # Build a full label map from config for all series (stable naming)
    label_map_all = {col: series_cfg.get(col, {}).get("label", col) for col in df.columns if col.upper() != "USREC"}

    # Static heatmap
    corr = compute_static_corr(df, lookback)
    # Apply display labels using the full map (restricted to present cols)
    label_map = {col: label_map_all.get(col, col) for col in corr.columns}
    corr = corr.rename(index=label_map, columns=label_map)
    order = list(corr.index)
    if cluster and corr.shape[0] > 2:
        try:
            # Get clustered order using the same helper in visuals
            from src.visuals.heatmap import cluster_order

            order = cluster_order(corr)
        except Exception:
            pass
    # Title without mode; end with date for clarity
    title = f"Correlation Heatmap — Last {lookback}m — {today}"
    out_png = os.path.join(FIG_DIR, f"corr_heatmap_{mode}_{lookback}_{today}.png")
    out_svg = os.path.join(FIG_DIR, f"corr_heatmap_{mode}_{lookback}_{today}.svg")
    plot_correlation_heatmap(corr, title, out_png, out_svg, color_scale=color_scale, cluster=cluster)
    # Also save latest copies
    plot_correlation_heatmap(corr, title, os.path.join(FIG_DIR, f"corr_heatmap_{mode}_latest.png"), os.path.join(FIG_DIR, f"corr_heatmap_{mode}_latest.svg"), color_scale=color_scale, cluster=cluster)

    # Rolling animation (limit to last N months for frames)
    df_roll = df.copy()
    try:
        last_date = df_roll.index.max()
        # Ensure earliest frame end-date is within last 'roll_lookback' months
        cutoff_end = last_date - pd.DateOffset(months=roll_lookback)
        # Include the preceding window so the first frame can be computed
        start_needed = cutoff_end - pd.DateOffset(months=roll_win - 1)
        df_roll = df_roll[df_roll.index >= start_needed]
    except Exception:
        pass
    rolling = compute_rolling_corr(df_roll, roll_win)
    if not rolling:
        print("WARN: No rolling frames after lookback trim; retrying with full history…")
        rolling = compute_rolling_corr(df, roll_win)
    if not rolling:
        print("WARN: Still no rolling frames; will fall back to a single frame from the static window.")
        # Build a single-frame rolling dict from the latest static correlation
        last_ts = df.index.max()
        rolling = {last_ts: corr.copy()}
    # Rename each rolling corr using the full label map so names are consistent even
    # if a series wasn't in the static lookback window
    rolling = {ts: m.rename(index=label_map_all, columns=label_map_all) for ts, m in rolling.items()}
    # Fix series order across frames: keep clustered order from static if requested.
    # Ensure any labels not present in static corr (due to coverage) are appended so they always appear.
    static_order = order if cluster else sorted(list(corr.index))
    all_labels = [label_map_all.get(c, c) for c in df.columns if c.upper() != "USREC"]
    # Deduplicate while preserving static clustering order first
    seen = set()
    series_order = []
    for lbl in static_order + all_labels:
        if lbl not in seen:
            series_order.append(lbl)
            seen.add(lbl)
    out_gif = os.path.join(ANIM_DIR, f"corr_heatmap_rolling_{mode}_{roll_win}_{today}.gif")
    try:
        build_rolling_correlation_animation(rolling, series_order, roll_win, mode, out_gif, color_scale=color_scale)
        # Latest copy
        latest_gif = os.path.join(ANIM_DIR, f"corr_heatmap_rolling_{mode}_latest.gif")
        latest_mp4 = os.path.join(ANIM_DIR, f"corr_heatmap_rolling_{mode}_latest.mp4")
        out_mp4 = out_gif.rsplit(".", 1)[0] + ".mp4"
        if os.path.exists(out_gif):
            if os.path.exists(latest_gif):
                try:
                    os.remove(latest_gif)
                except Exception:
                    pass
            try:
                import shutil

                shutil.copyfile(out_gif, latest_gif)
            except Exception:
                pass
        if os.path.exists(out_mp4):
            if os.path.exists(latest_mp4):
                try:
                    os.remove(latest_mp4)
                except Exception:
                    pass
            try:
                import shutil

                shutil.copyfile(out_mp4, latest_mp4)
            except Exception:
                pass
        else:
            print("WARN: Rolling animation not created (no frames). Skipping copy.")
    except ImportError as e:
        print(f"WARN: Skipping animation due to missing dependency: {e}")
    except Exception as e:
        print(f"WARN: Failed to build rolling animation: {e}")

    # Save processed snapshots for reproducibility
    out_proc = os.path.join(PROC_DIR, f"monthly_{mode}_{today}.csv")
    df.reset_index().to_csv(out_proc, index=False)

    # Update README last updated stamp (simple replace)
    readme_path = os.path.join(ROOT, "README.md")
    try:
        with open(readme_path, "r") as f:
            txt = f.read()
        import re

        txt = re.sub(r"Last updated:.*", f"Last updated: {today}", txt)
        with open(readme_path, "w") as f:
            f.write(txt)
    except Exception as e:
        print(f"WARN: Failed to update README stamp: {e}")

    print("Done. Artifacts written to reports/figures and reports/animations.")


if __name__ == "__main__":
    main()
