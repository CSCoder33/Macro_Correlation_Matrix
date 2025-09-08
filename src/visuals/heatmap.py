from __future__ import annotations

import datetime as dt
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.cluster.hierarchy import linkage, leaves_list
from scipy.spatial.distance import squareform


def cluster_order(corr: pd.DataFrame) -> List[str]:
    # Convert correlation to distance matrix (ensure finite values)
    c = corr.fillna(0.0).clip(-1, 1)
    d = 1 - c
    # Symmetrize and zero diagonal
    d = (d + d.T) / 2
    np.fill_diagonal(d.values, 0.0)
    # Condensed form for linkage
    condensed = squareform(d.values, checks=False)
    Z = linkage(condensed, method="average")
    order = leaves_list(Z)
    return list(corr.index[order])


def plot_correlation_heatmap(
    corr: pd.DataFrame,
    title: str,
    out_png: str,
    out_svg: Optional[str] = None,
    color_scale: Tuple[float, float] = (-1.0, 1.0),
    cluster: bool = False,
):
    sns.set(style="white", context="talk")
    order = list(corr.index)
    if cluster and corr.shape[0] > 2:
        try:
            order = cluster_order(corr)
        except Exception:
            order = list(corr.index)
    corr_ord = corr.loc[order, order]

    plt.figure(figsize=(10, 9))
    ax = sns.heatmap(
        corr_ord,
        vmin=color_scale[0],
        vmax=color_scale[1],
        cmap="coolwarm",
        square=True,
        cbar_kws={"ticks": [-1.0, -0.75, -0.5, -0.25, 0.0, 0.25, 0.5, 0.75, 1.0]},
        linewidths=0.5,
        linecolor="white",
    )
    ax.set_title(title, fontsize=14)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right")
    ax.set_yticklabels(ax.get_yticklabels(), rotation=0)
    plt.tight_layout()
    plt.savefig(out_png, dpi=200)
    if out_svg:
        plt.savefig(out_svg)
    plt.close()
