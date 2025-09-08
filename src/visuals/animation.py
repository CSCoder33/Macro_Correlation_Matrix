from __future__ import annotations

import datetime as dt
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import importlib
import io


def build_rolling_correlation_animation(
    rolling_corrs: Dict[pd.Timestamp, pd.DataFrame],
    series_order: List[str],
    window_months: int,
    mode: str,
    out_gif: str,
    color_scale: Tuple[float, float] = (-1.0, 1.0),
):
    # Lazy import imageio to avoid static import errors in IDEs
    try:
        imageio = importlib.import_module("imageio.v2")
    except Exception as e:
        raise ImportError(
            "imageio is required for GIF generation. Install with `pip install imageio`."
        ) from e
    frames = []
    sns.set(style="white")
    for end_date, corr in rolling_corrs.items():
        corr_ord = corr.reindex(index=series_order, columns=series_order)
        fig, ax = plt.subplots(figsize=(8, 7))
        sns.heatmap(
            corr_ord,
            vmin=color_scale[0],
            vmax=color_scale[1],
            cmap="coolwarm",
            square=True,
            cbar=True,
            cbar_kws={"ticks": [-1.0, -0.75, -0.5, -0.25, 0.0, 0.25, 0.5, 0.75, 1.0]},
            ax=ax,
            linewidths=0.4,
            linecolor="white",
        )
        # Show only year-month in the title; no mode label
        ym = pd.Timestamp(end_date).strftime("%Y-%m")
        title = f"Rolling {window_months}m Correlations — {ym}"
        ax.set_title(title, fontsize=12)
        ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right", fontsize=8)
        ax.set_yticklabels(ax.get_yticklabels(), rotation=0, fontsize=8)
        fig.tight_layout()
        # Render figure to a PNG buffer to avoid backend DPI/shape issues
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=150)
        buf.seek(0)
        img = imageio.imread(buf)
        frames.append(img)
        plt.close(fig)

    if not frames:
        # Gracefully no-op so pipeline can still produce static outputs
        return
    # Slow down a bit for easier viewing (0.5s per frame)
    imageio.mimsave(out_gif, frames, duration=0.5)

    # Also write an MP4 with controls (for README play/pause)
    try:
        fps = 2  # 0.5s per frame ⇒ 2 fps
        out_mp4 = out_gif.rsplit(".", 1)[0] + ".mp4"
        writer = imageio.get_writer(out_mp4, fps=fps, codec="libx264")
        for frm in frames:
            writer.append_data(frm)
        writer.close()
    except Exception as _:
        # MP4 is optional; keep pipeline green even if not available
        pass
