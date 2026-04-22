"""
Inverter Shutdown Detection
Rule:
  shutdown = (ac_power == 0) AND (irradiance > 5 W/m²)
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session


IS_IRRADIANCE_MIN = float(os.getenv("IS_IRRADIANCE_MIN", "5"))
IS_AC_ZERO_TOL = float(os.getenv("IS_AC_ZERO_TOL", "0.01"))


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
    return dict(zip(df_irr["timestamp"].astype(str), df_irr["irradiance"]))


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

    # Infer data interval
    diffs = df.groupby("inverter_id")["timestamp"].diff().dropna()
    dt_h = float(diffs.median().total_seconds() / 3600.0) if len(diffs) else (1.0 / 60.0)
    if not np.isfinite(dt_h) or dt_h <= 0:
        dt_h = 1.0 / 60.0

    inv_status: List[dict] = []
    timeline: List[dict] = []
    for inv_id, g in df.groupby("inverter_id"):
        g = g.sort_values("timestamp")
        sh = g[g["shutdown"]]
        shutdown_points = int(len(sh))
        shutdown_hours = round(shutdown_points * dt_h, 3)
        last_seen = str(sh["timestamp"].max()) if shutdown_points else None
        window_start = str(sh["timestamp"].min()) if shutdown_points else None
        window_end = str(sh["timestamp"].max()) if shutdown_points else None
        inv_status.append(
            {
                "inverter_id": inv_id,
                "shutdown_points": shutdown_points,
                "shutdown_hours": shutdown_hours,
                "last_seen_shutdown": last_seen,
                "investigation_window_start": window_start,
                "investigation_window_end": window_end,
            }
        )
        for _, r in g.iterrows():
            timeline.append(
                {
                    "timestamp": str(r["timestamp"]),
                    "inverter_id": inv_id,
                    "ac_power_kw": round(float(r["ac_kw"]), 3),
                    "irradiance": round(float(r["irradiance"]), 3),
                    "shutdown": bool(r["shutdown"]),
                }
            )

    return inv_status, timeline
