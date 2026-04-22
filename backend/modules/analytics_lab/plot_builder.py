"""
modules/analytics_lab/plot_builder.py
======================================
Solar Analytics Platform — Plotly Figure Builder

Builds synchronized, interactive Plotly figures for the Analytics Lab.
The UI calls these functions — it never touches Plotly directly.

Graph layout replicates SmartHelio Advanced Diagnostic Tool style:
  - Graph 1 (top)    : Current + Power + Irradiance (all in one axes set)
  - Graph 2 (bottom) : Voltage

Both graphs share a synchronized X-axis (time).
Hover shows unified tooltip across all traces at the same timestamp.
Zooming one graph zooms the other.
"""

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import List, Optional

# ── Colour Palette ─────────────────────────────────────────────────────────────
# A curated industrial palette — each equipment ID gets a unique colour.
_COLOUR_PALETTE = [
    "#00B4D8", "#0077B6", "#90E0EF", "#48CAE4",
    "#2EC4B6", "#CBEF43", "#FFB703", "#FB8500",
    "#E63946", "#A8DADC", "#457B9D", "#1D3557",
    "#52B788", "#74C69D", "#D8F3DC", "#95D5B2",
]

# Signal → display label and unit
SIGNAL_META = {
    "dc_current":  {"label": "DC Current",   "unit": "A",    "dash": "solid"},
    "dc_voltage":  {"label": "DC Voltage",   "unit": "V",    "dash": "solid"},
    "dc_power":    {"label": "DC Power",     "unit": "kW",   "dash": "dash"},
    "ac_power":    {"label": "AC Power",     "unit": "kW",   "dash": "dot"},
    "irradiance":  {"label": "Irradiance",   "unit": "W/m²", "dash": "longdash"},
    "temperature": {"label": "Temperature",  "unit": "°C",   "dash": "dashdot"},
    "wind_speed":  {"label": "Wind Speed",   "unit": "m/s",  "dash": "longdashdot"},
}

# Which signals go in which graph
_GRAPH1_SIGNALS = {"dc_current", "dc_power", "ac_power", "irradiance", "temperature"}
_GRAPH2_SIGNALS = {"dc_voltage"}


def _get_colour(equipment_id: str, equipment_ids: List[str]) -> str:
    """Assign a consistent colour to each equipment ID."""
    idx = equipment_ids.index(equipment_id) % len(_COLOUR_PALETTE)
    return _COLOUR_PALETTE[idx]


def build_dual_graph(
    df: pd.DataFrame,
    equipment_ids: List[str],
    selected_signals: List[str],
    title: str = "Solar Plant Analytics",
) -> Optional[go.Figure]:
    """
    Build the dual synchronized graph figure.

    Structure:
        Row 1 → Current, Power, Irradiance signals
        Row 2 → Voltage signals

    Args:
        df               : Long-format DataFrame (timestamp, equipment_id, signal, value)
        equipment_ids    : Ordered list of equipment IDs (for colour assignment)
        selected_signals : Signal names to include
        title            : Figure title

    Returns:
        Plotly Figure, or None if df is empty.
    """
    if df.empty:
        return None

    # Separate signals between the two graphs
    g1_sigs = [s for s in selected_signals if s in _GRAPH1_SIGNALS]
    g2_sigs = [s for s in selected_signals if s in _GRAPH2_SIGNALS]

    # Decide how many subplot rows we need
    has_g1 = len(g1_sigs) > 0
    has_g2 = len(g2_sigs) > 0

    if has_g1 and has_g2:
        row_heights = [0.6, 0.4]
        rows        = 2
    elif has_g1:
        row_heights = [1.0]
        rows        = 1
    else:
        row_heights = [1.0]
        rows        = 1

    fig = make_subplots(
        rows              = rows,
        cols              = 1,
        shared_xaxes      = True,
        vertical_spacing  = 0.06,
        row_heights       = row_heights,
        subplot_titles    = (
            ("Current / Power / Irradiance", "Voltage") if (has_g1 and has_g2)
            else ("Current / Power / Irradiance",) if has_g1
            else ("Voltage",)
        ),
    )

    # Detect multi-day for Day Overlay
    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    dates = sorted(df['timestamp'].dt.date.unique())
    is_multi_day = len(dates) > 1

    if is_multi_day:
        common_date = dates[0]
        df['orig_date'] = df['timestamp'].dt.date
        # Normalize to the first day's date but keep HH:MM:SS
        df['timestamp'] = df.apply(lambda r: r['timestamp'].replace(
            year=common_date.year, month=common_date.month, day=common_date.day
        ), axis=1)

    # ── Graph 1 Traces ────────────────────────────────────────────────────────
    if has_g1:
        for eq_id in equipment_ids:
            eq_df_all = df[df["equipment_id"] == eq_id]
            colour_base = _get_colour(eq_id, equipment_ids)

            for signal in g1_sigs:
                meta = SIGNAL_META.get(signal, {"label": signal, "unit": "", "dash": "solid"})
                
                # If multi-day, we create a trace per day
                if is_multi_day:
                    for i, d in enumerate(dates):
                        day_df = eq_df_all[(eq_df_all["signal"] == signal) & (eq_df_all["timestamp"].dt.date == common_date) & (eq_df_all["orig_date"] == d)].sort_values("timestamp")
                        if day_df.empty: continue
                        
                        # Slightly vary the color/opacity for different days if needed, 
                        # but usually just different names are enough.
                        name = f"{eq_id} ({d}) — {meta['label']}"
                        
                        fig.add_trace(
                            go.Scatter(
                                x            = day_df["timestamp"],
                                y            = day_df["value"],
                                name         = name,
                                mode         = "lines",
                                line         = dict(color=colour_base, dash=meta["dash"], width=1.5),
                                opacity      = 0.85 if i > 0 else 1.0,
                                hovertemplate= f"<b>{name}</b><br>Time: %{{x|%H:%M}}<br>Value: %{{y:.2f}} {meta['unit']}<extra></extra>",
                                showlegend   = True,
                                legendgroup  = eq_id,
                            ),
                            row=1, col=1
                        )
                else:
                    sig_df = eq_df_all[eq_df_all["signal"] == signal].sort_values("timestamp")
                    if sig_df.empty: continue

                    name = f"{eq_id} — {meta['label']}"
                    fig.add_trace(
                        go.Scatter(
                            x            = sig_df["timestamp"],
                            y            = sig_df["value"],
                            name         = name,
                            mode         = "lines",
                            line         = dict(color=colour_base, dash=meta["dash"], width=1.8),
                            hovertemplate= f"<b>{name}</b><br>Time: %{{x}}<br>Value: %{{y:.2f}} {meta['unit']}<extra></extra>",
                            showlegend   = True,
                        ),
                        row=1, col=1
                    )

    # ── Graph 2 Traces ────────────────────────────────────────────────────────
    if has_g2:
        g2_row = 2 if has_g1 else 1
        for eq_id in equipment_ids:
            eq_df_all = df[df["equipment_id"] == eq_id]
            colour_base = _get_colour(eq_id, equipment_ids)

            for signal in g2_sigs:
                meta = SIGNAL_META.get(signal, {"label": signal, "unit": "", "dash": "solid"})
                
                if is_multi_day:
                    for i, d in enumerate(dates):
                        day_df = eq_df_all[(eq_df_all["signal"] == signal) & (eq_df_all["timestamp"].dt.date == common_date) & (eq_df_all["orig_date"] == d)].sort_values("timestamp")
                        if day_df.empty: continue
                        
                        name = f"{eq_id} ({d}) — {meta['label']}"
                        
                        fig.add_trace(
                            go.Scatter(
                                x            = day_df["timestamp"],
                                y            = day_df["value"],
                                name         = name,
                                mode         = "lines",
                                line         = dict(color=colour_base, dash="solid", width=1.5),
                                opacity      = 0.85 if i > 0 else 1.0,
                                hovertemplate= f"<b>{name}</b><br>Time: %{{x|%H:%M}}<br>Value: %{{y:.2f}} {meta['unit']}<extra></extra>",
                                showlegend   = True,
                                legendgroup  = eq_id,
                            ),
                            row=g2_row, col=1
                        )
                else:
                    sig_df = eq_df_all[eq_df_all["signal"] == signal].sort_values("timestamp")
                    if sig_df.empty: continue

                    name = f"{eq_id} — {meta['label']}"
                    fig.add_trace(
                        go.Scatter(
                            x            = sig_df["timestamp"],
                            y            = sig_df["value"],
                            name         = name,
                            mode         = "lines",
                            line         = dict(color=colour_base, dash="solid", width=1.8),
                            hovertemplate= f"<b>{name}</b><br>Time: %{{x}}<br>Value: %{{y:.2f}} {meta['unit']}<extra></extra>",
                            showlegend   = True,
                        ),
                        row=g2_row, col=1
                    )

    # ── Layout Styling ─────────────────────────────────────────────────────────
    fig.update_layout(
        title              = dict(text=title, font=dict(size=18, color="#1A3A5C")),
        template           = "plotly_dark",
        paper_bgcolor      = "#0F1923",
        plot_bgcolor       = "#0D1E2E",
        legend             = dict(
            orientation    = "h",
            yanchor        = "bottom",
            y              = 1.02,
            xanchor        = "right",
            x              = 1,
            font           = dict(size=11),
        ),
        hovermode          = "x unified",
        height             = 700 if (has_g1 and has_g2) else 420,
        margin             = dict(l=60, r=30, t=80, b=40),
        font               = dict(family="Inter, sans-serif", color="#CCDDEE"),
    )

    # Axis labels
    if has_g1:
        fig.update_yaxes(
            title_text="Current (A) / Power (kW) / Irradiance (W/m²)",
            row=1, col=1,
            gridcolor="#1E3045",
            zerolinecolor="#2A4560",
        )
    if has_g2:
        g2_row = 2 if has_g1 else 1
        fig.update_yaxes(
            title_text="Voltage (V)",
            row=g2_row, col=1,
            gridcolor="#1E3045",
            zerolinecolor="#2A4560",
        )

    # X-axis styling (only bottom graph gets label)
    fig.update_xaxes(gridcolor="#1E3045", zerolinecolor="#2A4560")
    last_row = 2 if (has_g1 and has_g2) else 1
    fig.update_xaxes(title_text="Timestamp", row=last_row, col=1)

    return fig


def build_summary_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a summary statistics table from the time-series data.

    Columns: equipment_id | signal | min | max | mean | std

    Args:
        df : Long-format DataFrame from query_engine.

    Returns:
        Summary statistics DataFrame.
    """
    if df.empty:
        return pd.DataFrame()

    stats = (
        df.groupby(["equipment_id", "signal"])["value"]
        .agg(["min", "max", "mean", "std"])
        .reset_index()
    )
    stats.columns = ["Equipment ID", "Signal", "Min", "Max", "Mean", "Std Dev"]
    for col in ["Min", "Max", "Mean", "Std Dev"]:
        stats[col] = stats[col].round(3)

    return stats
