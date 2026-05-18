"""
Join plant / WMS irradiance to inverter (or plant) AC timestamps when clocks differ.

Raw pipelines often use the same nominal cadence (e.g. 15 min) but different offsets
(:00 vs :02). Exact ``timestamp`` equality drops most rows and tanks coverage / shutdown
detection. We use ``merge_asof`` on UTC-naive second-floored clocks with a configurable
tolerance (default 12 minutes — half a 15-minute bucket plus skew).
"""
from __future__ import annotations

import os
from typing import Optional

import numpy as np
import pandas as pd

IRR_ASOF_TOLERANCE_MIN = float(os.getenv("IRR_ASOF_TOLERANCE_MIN", "12"))


def align_utc_naive_seconds(s: pd.Series) -> pd.Series:
    """Match inverter_shutdown: UTC-normalize then drop sub-second noise."""
    t = pd.to_datetime(s, errors="coerce", utc=True)
    t = t.dt.tz_convert("UTC").dt.tz_localize(None)
    return t.dt.floor("s")


def build_irradiance_priority_frame(
    df_irr: pd.DataFrame,
    *,
    ts_col: str = "timestamp",
    signal_col: str = "signal",
    value_col: str,
    priority_mode: str,
) -> pd.DataFrame:
    """
    One row per irradiance timestamp after signal priority dedupe.

    priority_mode
      - ``clipping``: prefer gti, then irradiance, then ghi (matches clipping_derating).
      - ``standard``: prefer irradiance, then gti, then ghi (matches inverter_shutdown / PL / GB).
    """
    if df_irr is None or df_irr.empty:
        return pd.DataFrame(columns=["ts_irr", "irr_val"])

    df = df_irr.copy()
    df["__v"] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna(subset=["__v"])
    if df.empty:
        return pd.DataFrame(columns=["ts_irr", "irr_val"])

    sig = df[signal_col].astype(str).str.lower().str.strip()
    if priority_mode == "clipping":
        rank = sig.map({"gti": 0, "irradiance": 1, "ghi": 2}).fillna(9)
    elif priority_mode == "standard":
        rank = sig.map({"irradiance": 0, "gti": 1, "ghi": 2}).fillna(9)
    else:
        raise ValueError(f"unknown priority_mode: {priority_mode!r}")

    df["__sr"] = rank
    df["ts_irr"] = align_utc_naive_seconds(df[ts_col])
    df = df.sort_values(["ts_irr", "__sr"]).drop_duplicates("ts_irr", keep="first")
    return df[["ts_irr", "__v"]].rename(columns={"__v": "irr_val"})


def merge_irradiance_onto_ac(
    df_ac: pd.DataFrame,
    *,
    ts_col: str,
    df_irr: pd.DataFrame,
    irr_ts_col: str = "timestamp",
    signal_col: str = "signal",
    value_col: str,
    priority_mode: str,
    out_col: str,
    tolerance_min: Optional[float] = None,
) -> pd.DataFrame:
    """
    Left-keep all ``df_ac`` rows; set ``out_col`` from nearest irradiance within tolerance.
    Rows with no neighbour inside the window get NaN in ``out_col``.
    """
    tol = IRR_ASOF_TOLERANCE_MIN if tolerance_min is None else float(tolerance_min)
    if df_ac is None or df_ac.empty:
        return df_ac

    work = df_ac.copy()
    work["_idx"] = np.arange(len(work), dtype=np.int64)
    work["_ts_join"] = align_utc_naive_seconds(work[ts_col])

    if df_irr is None or df_irr.empty:
        out = work.drop(columns=["_ts_join"], errors="ignore")
        out[out_col] = np.nan
        return out.drop(columns=["_idx"], errors="ignore")

    irr_use = df_irr.rename(columns={irr_ts_col: "_irr_raw_ts"}) if irr_ts_col != "timestamp" else df_irr.copy()
    if irr_ts_col != "timestamp":
        irr_use["timestamp"] = irr_use["_irr_raw_ts"]

    right = build_irradiance_priority_frame(
        irr_use,
        ts_col="timestamp",
        signal_col=signal_col,
        value_col=value_col,
        priority_mode=priority_mode,
    )
    if right.empty:
        out = work.drop(columns=["_ts_join"], errors="ignore")
        out[out_col] = np.nan
        return out.drop(columns=["_idx"], errors="ignore")

    merged = pd.merge_asof(
        work.sort_values("_ts_join"),
        right.sort_values("ts_irr"),
        left_on="_ts_join",
        right_on="ts_irr",
        direction="nearest",
        tolerance=pd.Timedelta(minutes=tol),
    )
    merged = merged.sort_values("_idx")
    merged = merged.rename(columns={"irr_val": out_col})
    drop_cols = [c for c in ("_ts_join", "_idx", "ts_irr") if c in merged.columns]
    return merged.drop(columns=drop_cols).reset_index(drop=True)
