"""
Inverter Shutdown Detection
Rule:
  shutdown = (ac_power == 0) AND (irradiance > threshold W/m², default 10)

Energy loss (kWh): at each timestamp, expected AC (kW) = median of inverters in the
top quartile (>= 75th percentile) of AC among producing peers; if that is ~0 but peers
are producing, fall back to positive p75 then plant max AC at that timestamp. Timestamps
are aligned (UTC-naive, second resolution) so irradiance join and expected merge cannot
silently zero out. For shutdown samples, incremental loss = expected_kw × Δt (per-inverter).
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session


IS_IRRADIANCE_MIN = float(os.getenv("IS_IRRADIANCE_MIN", "10"))
IS_AC_ZERO_TOL = float(os.getenv("IS_AC_ZERO_TOL", "0.01"))


def _align_time_column(s: pd.Series) -> pd.Series:
    """
    UTC-naive timestamps floored to whole seconds so AC, irradiance, and merge keys
    line up across drivers (tz-aware vs naive) and str formatting differences.
    """
    t = pd.to_datetime(s, errors="coerce", utc=True)
    t = t.dt.tz_convert("UTC").dt.tz_localize(None)
    return t.dt.floor("s")


def _normalize_date_str(v: str) -> str:
    """
    Normalize common UI date formats to YYYY-MM-DD.
    Accepts:
      - YYYY-MM-DD
      - DD/MM/YYYY
      - MM/DD/YYYY (only if unambiguous after DD/MM attempt)
      - DD-MM-YYYY
      - YYYY/MM/DD
    """
    s = str(v or "").strip()
    if not s:
        return s
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    fmts = ("%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d", "%Y-%m-%d")
    for fmt in fmts:
        try:
            return datetime.strptime(s[:10], fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    return s[:10]


def _pick_irradiance(df_irr: pd.DataFrame) -> dict:
    # Priority: irradiance > gti > ghi
    prio = {"irradiance": 0, "gti": 1, "ghi": 2}
    df_irr["prio"] = df_irr["signal"].map(prio).fillna(99)
    df_irr = df_irr.sort_values(["timestamp", "prio"]).drop_duplicates(["timestamp"], keep="first")
    return dict(zip(df_irr["timestamp"], df_irr["irradiance"]))


def _expected_kw_top_quarter_median(vals: np.ndarray) -> float:
    """Median AC of inverters in the top quartile at one timestamp."""
    v = vals[np.isfinite(vals)]
    if v.size == 0:
        return 0.0
    thr = float(np.quantile(v, 0.75))
    top = v[v >= thr]
    if top.size == 0:
        return float(np.median(v))
    return float(np.median(top))


def run_inverter_shutdown(
    db: Session,
    plant_id: str,
    date_from: str,
    date_to: str,
    exclude_grid_breakdown: bool = True,
) -> Tuple[List[dict], List[dict]]:
    """
    Returns:
      (inverter_status_list, timeline_list)
    """
    d_from = _normalize_date_str(date_from)
    d_to = _normalize_date_str(date_to)
    f_ts = f"{d_from} 00:00:00"
    t_ts = f"{d_to} 23:59:59"

    sql_ac = text(
        """
        SELECT timestamp, equipment_id AS inverter_id, AVG(value) AS ac_kw
        FROM raw_data_generic
        WHERE plant_id = :p
          AND LOWER(TRIM(equipment_level::text)) = 'inverter'
          AND signal = 'ac_power'
          AND timestamp >= :f AND timestamp <= :t
        GROUP BY timestamp, equipment_id
        """
    )
    ac_rows = db.execute(sql_ac, {"p": plant_id, "f": f_ts, "t": t_ts}).fetchall()
    if not ac_rows:
        return [], []

    df = pd.DataFrame(ac_rows, columns=["timestamp", "inverter_id", "ac_kw"])
    df["timestamp"] = _align_time_column(df["timestamp"])
    df["ac_kw"] = pd.to_numeric(df["ac_kw"], errors="coerce")
    df = df.dropna(subset=["timestamp", "inverter_id", "ac_kw"]).copy()
    if df.empty:
        return [], []

    sql_irr = text(
        """
        SELECT timestamp, signal, AVG(value) AS irradiance
        FROM raw_data_generic
        WHERE plant_id = :p
          AND LOWER(TRIM(equipment_level::text)) IN ('plant','wms')
          AND signal IN ('irradiance','gti','ghi')
          AND timestamp >= :f AND timestamp <= :t
        GROUP BY timestamp, signal
        """
    )
    irr_rows = db.execute(sql_irr, {"p": plant_id, "f": f_ts, "t": t_ts}).fetchall()
    if not irr_rows:
        return [], []

    df_irr = pd.DataFrame(irr_rows, columns=["timestamp", "signal", "irradiance"])
    df_irr["timestamp"] = _align_time_column(df_irr["timestamp"])
    df_irr["irradiance"] = pd.to_numeric(df_irr["irradiance"], errors="coerce")
    df_irr = df_irr.dropna(subset=["timestamp", "irradiance"]).copy()
    if df_irr.empty:
        return [], []

    irr_map = _pick_irradiance(df_irr)
    df["irradiance"] = df["timestamp"].map(irr_map)
    df["irradiance"] = pd.to_numeric(df["irradiance"], errors="coerce")
    df = df.dropna(subset=["irradiance"]).copy()
    if df.empty:
        return [], []

    df["shutdown"] = (np.abs(df["ac_kw"]) <= IS_AC_ZERO_TOL) & (df["irradiance"] > IS_IRRADIANCE_MIN)

    # If every inverter is shutdown at the same timestamp, classify as Grid Breakdown,
    # not Inverter Shutdown.
    if exclude_grid_breakdown and not df.empty:
        ts_stats = (
            df.groupby("timestamp", as_index=False)
            .agg(
                inverter_count=("inverter_id", "nunique"),
                shutdown_count=("shutdown", "sum"),
            )
        )
        grid_ts = set(
            ts_stats[
                (ts_stats["inverter_count"] > 0)
                & (ts_stats["shutdown_count"] >= ts_stats["inverter_count"])
            ]["timestamp"].tolist()
        )
        if grid_ts:
            df["shutdown"] = df["shutdown"] & (~df["timestamp"].isin(grid_ts))
    df = df.sort_values(["inverter_id", "timestamp"]).reset_index(drop=True)

    # Expected plant AC (kW) at each timestamp: median of top-quartile inverters.
    ts_expected = (
        df.groupby("timestamp", sort=False)["ac_kw"]
        .apply(lambda s: _expected_kw_top_quarter_median(s.to_numpy(dtype=float)))
        .rename("expected_ac_kw")
    )
    df = df.merge(ts_expected.reset_index(), on="timestamp", how="left")
    df["expected_ac_kw"] = pd.to_numeric(df["expected_ac_kw"], errors="coerce").fillna(0.0)

    # If the timestamp-wide top-quartile median is ~0 but some inverters are still exporting,
    # use the 75th percentile of strictly positive AC at that timestamp so shutdown loss is not forced to 0.
    def _plant_pos_p75(s: pd.Series) -> float:
        x = pd.to_numeric(s, errors="coerce").to_numpy(dtype=float)
        x = x[np.isfinite(x) & (x > IS_AC_ZERO_TOL)]
        return float(np.percentile(x, 75)) if x.size else 0.0

    plant_pos_p75 = df.groupby("timestamp", sort=False)["ac_kw"].transform(_plant_pos_p75)
    df["expected_ac_kw"] = np.where(
        (df["expected_ac_kw"] <= IS_AC_ZERO_TOL) & (plant_pos_p75 > IS_AC_ZERO_TOL),
        plant_pos_p75,
        df["expected_ac_kw"],
    )

    # If peer median is still ~0 but any inverter is exporting at this timestamp, use plant max AC
    # (covers merge/join gaps and heavily skewed all-zero readings with a single producer).
    plant_max_kw = df.groupby("timestamp", sort=False)["ac_kw"].transform("max")
    df["expected_ac_kw"] = np.where(
        (df["expected_ac_kw"] <= IS_AC_ZERO_TOL) & (plant_max_kw > IS_AC_ZERO_TOL),
        plant_max_kw,
        df["expected_ac_kw"],
    )

    # Per-inverter Δt (hours) to next sample, capped (same spirit as CD engine).
    max_dt_h = float(os.getenv("IS_MAX_DT_HOURS", str(5.0 / 60.0)))
    df["dt_h"] = df.groupby("inverter_id", sort=False)["timestamp"].diff().dt.total_seconds() / 3600.0
    inv_median_dt = df.groupby("inverter_id", sort=False)["dt_h"].transform("median")
    df["dt_h"] = df["dt_h"].fillna(inv_median_dt).fillna(1.0 / 60.0)
    df["dt_h"] = df["dt_h"].clip(lower=1.0 / 3600.0, upper=max_dt_h)

    df["loss_kwh_step"] = np.where(
        df["shutdown"] & (df["expected_ac_kw"] > 0),
        df["expected_ac_kw"] * df["dt_h"],
        0.0,
    )

    inv_status: List[dict] = []
    timeline: List[dict] = []
    for inv_id, g in df.groupby("inverter_id"):
        g = g.sort_values("timestamp")
        sh = g[g["shutdown"]]
        shutdown_points = int(len(sh))
        shutdown_hours = round(float(sh["dt_h"].sum()), 3) if shutdown_points else 0.0
        total_loss = float(g["loss_kwh_step"].sum())
        last_seen = str(sh["timestamp"].max()) if shutdown_points else None
        window_start = str(sh["timestamp"].min()) if shutdown_points else None
        window_end = str(sh["timestamp"].max()) if shutdown_points else None
        inv_status.append(
            {
                "inverter_id": inv_id,
                "shutdown_points": shutdown_points,
                "shutdown_hours": shutdown_hours,
                "total_shutdown_energy_loss_kwh": round(total_loss, 2),
                "last_seen_shutdown": last_seen,
                "active_since_shutdown": window_start,
                "investigation_window_start": window_start,
                "investigation_window_end": window_end,
            }
        )
        for _, r in g.iterrows():
            row = {
                "timestamp": str(r["timestamp"]),
                "inverter_id": inv_id,
                "ac_power_kw": round(float(r["ac_kw"]), 3),
                "irradiance": round(float(r["irradiance"]), 3),
                "shutdown": bool(r["shutdown"]),
                "expected_ac_kw": round(float(r["expected_ac_kw"]), 3),
            }
            timeline.append(row)

    return inv_status, timeline
