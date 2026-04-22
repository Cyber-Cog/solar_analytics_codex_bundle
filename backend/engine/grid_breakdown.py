"""
Grid Breakdown Detection
Rule:
  grid_breakdown = (all inverters have ac_power == 0 at same timestamp) AND (irradiance > 5 W/m²)
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from engine.inverter_shutdown import IS_AC_ZERO_TOL, IS_IRRADIANCE_MIN, _normalize_date_str, _pick_irradiance


def run_grid_breakdown(
    db: Session,
    plant_id: str,
    date_from: str,
    date_to: str,
) -> Tuple[List[dict], List[dict]]:
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
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
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
    df_irr["timestamp"] = pd.to_datetime(df_irr["timestamp"], errors="coerce")
    df_irr["irradiance"] = pd.to_numeric(df_irr["irradiance"], errors="coerce")
    df_irr = df_irr.dropna(subset=["timestamp", "irradiance"]).copy()
    if df_irr.empty:
        return [], []

    irr_map = _pick_irradiance(df_irr)
    df["irradiance"] = df["timestamp"].astype(str).map(irr_map)
    df["irradiance"] = pd.to_numeric(df["irradiance"], errors="coerce")
    df = df.dropna(subset=["irradiance"]).copy()
    if df.empty:
        return [], []

    df["is_zero"] = np.abs(df["ac_kw"]) <= IS_AC_ZERO_TOL
    df["irr_ok"] = df["irradiance"] > IS_IRRADIANCE_MIN

    ts = (
        df.groupby("timestamp", as_index=False)
        .agg(
            inverter_count=("inverter_id", "nunique"),
            zero_count=("is_zero", "sum"),
            mean_irradiance=("irradiance", "mean"),
        )
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    if ts.empty:
        return [], []

    ts["grid_breakdown"] = (
        (ts["inverter_count"] > 0)
        & (ts["zero_count"] >= ts["inverter_count"])
        & (ts["mean_irradiance"] > IS_IRRADIANCE_MIN)
    )

    # Infer timestep duration
    diffs = ts["timestamp"].diff().dropna()
    dt_h = float(diffs.median().total_seconds() / 3600.0) if len(diffs) else (1.0 / 60.0)
    if not np.isfinite(dt_h) or dt_h <= 0:
        dt_h = 1.0 / 60.0

    gb = ts[ts["grid_breakdown"]].copy()
    events: List[dict] = []
    if not gb.empty:
        gb["run"] = (gb["grid_breakdown"].astype(int).diff().fillna(1) != 0).cumsum()
        for _, g in gb.groupby("run"):
            points = int(len(g))
            hours = round(points * dt_h, 3)
            start = g["timestamp"].min()
            end = g["timestamp"].max()
            events.append(
                {
                    "event_id": f"GB-{start.strftime('%Y%m%d-%H%M%S')}",
                    "breakdown_points": points,
                    "breakdown_hours": hours,
                    "last_seen_breakdown": str(end),
                    "investigation_window_start": str(start),
                    "investigation_window_end": str(end),
                }
            )

    timeline = []
    for _, r in ts.iterrows():
        timeline.append(
            {
                "timestamp": str(r["timestamp"]),
                "inverter_count": int(r["inverter_count"]),
                "zero_power_inverter_count": int(r["zero_count"]),
                "irradiance": round(float(r["mean_irradiance"]), 3),
                "grid_breakdown": bool(r["grid_breakdown"]),
            }
        )

    return events, timeline
