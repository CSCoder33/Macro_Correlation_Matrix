Macro Correlation Matrix — Static and Rolling Heatmaps

What this is
- Heatmaps of pairwise correlations across key macro and market series.
- Static view over a chosen lookback window and a rolling animation showing correlation regime shifts.

Why it matters
- Correlations drive diversification, risk, and regime detection. They shift materially in crises. Visualizing them over time makes shifts obvious and actionable.

Data sources
- FRED: DGS10 (US 10y yield), DCOILWTICO (WTI), GOLDAMGBD228NLBM (Gold), BAMLH0A0HYM2 (HY OAS), BAMLC0A0CM (IG OAS), CPIAUCSL (CPI), T10YIE (10y Breakeven), DTWEXBGS (Trade-weighted USD), INDPRO (Industrial Production), USREC (Recession indicator).
- Yahoo Finance: ^GSPC (S&P 500), ^VIX (VIX).

How to run locally
1) Create env and install deps
   - python -m venv .venv && source .venv/bin/activate
   - pip install -r requirements.txt
2) Run the pipeline (uses config in config/)
   - python -m src.pipeline_runner.main --mode levels
   - python -m src.pipeline_runner.main --mode returns
3) Open outputs
   - Static heatmaps: reports/figures/
   - Rolling animation: reports/animations/

Outputs
- Static heatmaps: corr_heatmap_<mode>_<lookback>_<YYYY-MM-DD>.png and .svg
- Rolling animation: corr_heatmap_rolling_<mode>_<window>_<YYYY-MM-DD>.gif
- Latest copies: *_latest.* for README linking

Glossary
- Pearson correlation: Linear dependence, range [-1, 1].
- Rolling window: Correlations computed over a moving monthly window.
- Clustering: Hierarchical ordering using distance = 1 − correlation to surface groups.
- Levels vs Returns: Price-like series (WTI, Gold, SPX, VIX, USD) show high autocorrelation; returns normalize. Macro levels (CPI, INDPRO) are often used as YoY %.

Automation (CI)
- A GitHub Actions workflow (update.yml) runs daily to refresh data, rebuild visuals, and update the README's last updated stamp.

Update Curves:
Update Correlation Heatmaps
- Latest rolling (click to play/pause; stops at last frame):
  
  <video src="reports/animations/corr_heatmap_rolling_levels_latest.mp4" controls playsinline muted style="max-width:100%; height:auto;"></video>
  
  If the video tag doesn’t render in your viewer, you can download the MP4 or view the GIF: [MP4](reports/animations/corr_heatmap_rolling_levels_latest.mp4) · ![GIF](reports/animations/corr_heatmap_rolling_levels_latest.gif)


Developer notes
- Config-driven: edit config/series.yaml and config/viz.yaml to change series, transforms, and windows. The pipeline reads these on each run.
- Offline sample: If fetching is unavailable, a small sample in data/processed is used so visuals can still be generated.

Last updated: 2025-09-08
