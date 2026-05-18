"""
Grid Breakdown Detection
Rule:
  grid_breakdown = (all inverters have ac_power == 0 at same timestamp) AND (irradiance > 10 W/m²)
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from engine.communication_issue import _load_architecture_meta
from engine.inverter_shutdown import IS_AC_ZERO_TOL, IS_IRRADIANCE_MIN, _normalize_date_str
from engine.irr_join import merge_irradiance_onto_ac


GB_PR_LOOKBACK_DAYS = int(os.getenv("GB_PR_LOOKBACK_DAYS", "30"))


def _dt_hours(timestamps: pd.Series) -> pd.Series:
    ts = pd.to_datetime(timestamps, errors="coerce")
    dt = ts.shift(-1).sub(ts).dt.total_seconds() / 3600.0
    valid = dt[(dt > 0) & (dt <= 6.0)]
    median = float(valid.median()) if not valid.empty else (1.0 / 60.0)
    if not np.isfinite(median) or median <= 0:
        median = 1.0 / 60.0
    return dt.fillna(median).clip(lower=1.0 / 3600.0, upper=max(8.0 * median, 1.0 / 60.0))


def _daily_pr_decimal(db: Session, plant_id: str, day: str, plant_dc_kwp: float) -> Optional[float]:
    if plant_dc_kwp <= 0:
        return None
    f_ts = f"{day} 00:00:00"
    t_ts = f"{day} 23:59:59"
    ac_rows = db.execute(
        text(
            """
            SELECT timestamp, SUM(value)::double precision AS plant_ac_kw
            FROM raw_data_generic
            WHERE plant_id = :p
              AND LOWER(TRIM(equipment_level::text)) = 'inverter'
              AND signal = 'ac_power'
              AND timestamp >= :f AND timestamp <= :t
            GROUP BY timestamp
            ORDER BY timestamp
            """
        ),
        {"p": plant_id, "f": f_ts, "t": t_ts},
    ).fetchall()
    irr_rows = db.execute(
        text(
            """
            SELECT timestamp, signal, AVG(value) AS irradiance
            FROM raw_data_generic
            WHERE plant_id = :p
              AND LOWER(TRIM(equipment_level::text)) IN ('plant','wms')
              AND signal IN ('irradiance','gti','ghi')
              AND timestamp >= :f AND timestamp <= :t
            GROUP BY timestamp, signal
            """
        ),
        {"p": plant_id, "f": f_ts, "t": t_ts},
    ).fetchall()
    if not ac_rows or not irr_rows:
        return None

    ac = pd.DataFrame(ac_rows, columns=["timestamp", "plant_ac_kw"])
    ac["timestamp"] = pd.to_datetime(ac["timestamp"], errors="coerce")
    ac["plant_ac_kw"] = pd.to_numeric(ac["plant_ac_kw"], errors="coerce")
    irr = pd.DataFrame(irr_rows, columns=["timestamp", "signal", "irradiance"])
    irr["timestamp"] = pd.to_datetime(irr["timestamp"], errors="coerce")
    irr["irradiance"] = pd.to_numeric(irr["irradiance"], errors="coerce")
    irr = irr.dropna(subset=["timestamp", "irradiance"]).copy()
    if ac.empty or irr.empty:
        return None

    ac = merge_irradiance_onto_ac(
        ac,
        ts_col="timestamp",
        df_irr=irr,
        signal_col="signal",
        value_col="irradiance",
        priority_mode="standard",
        out_col="irradiance",
    )
    ac["irradiance"] = pd.to_numeric(ac["irradiance"], errors="coerce")
    ac = ac.dropna(subset=["timestamp", "plant_ac_kw", "irradiance"]).sort_values("timestamp").reset_index(drop=True)
    ac = ac[(ac["irradiance"] > IS_IRRADIANCE_MIN) & (ac["plant_ac_kw"] > 0)].copy()
    if ac.empty:
        return None

    ac["dt_h"] = _dt_hours(ac["timestamp"])
    actual_kwh = float((ac["plant_ac_kw"] * ac["dt_h"]).sum())
    insolation_kwh_m2 = float((ac["irradiance"] * ac["dt_h"]).sum() / 1000.0)
    denom = insolation_kwh_m2 * plant_dc_kwp
    if actual_kwh <= 0 or denom <= 0:
        return None
    pr = actual_kwh / denom
    if not np.isfinite(pr) or pr <= 0:
        return None
    return float(min(pr, 1.25))


def _find_pr_decimal(db: Session, plant_id: str, day: str, plant_dc_kwp: float) -> Tuple[Optional[float], Optional[str]]:
    try:
        d = datetime.strptime(str(day)[:10], "%Y-%m-%d").date()
    except Exception:
        return None, None
    for offset in range(GB_PR_LOOKBACK_DAYS + 1):
        cur = (d - timedelta(days=offset)).isoformat()
        pr = _daily_pr_decimal(db, plant_id, cur, plant_dc_kwp)
        if pr is not None:
            return pr, cur
    return None, None


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

    df = merge_irradiance_onto_ac(
        df,
        ts_col="timestamp",
        df_irr=df_irr,
        signal_col="signal",
        value_col="irradiance",
        priority_mode="standard",
        out_col="irradiance",
    )
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

    inv_caps, _, _ = _load_architecture_meta(db, plant_id)
    plant_dc_kwp = float(sum(float(v or 0.0) for v in inv_caps.values()))
    ts["dt_h"] = _dt_hours(ts["timestamp"])
    ts["insolation_kwh_m2_step"] = (ts["mean_irradiance"] * ts["dt_h"]) / 1000.0
    ts["loss_kwh_step"] = 0.0

    gb = ts[ts["grid_breakdown"]].copy()
    events: List[dict] = []
    if not gb.empty:
        gb["run"] = (gb["grid_breakdown"].astype(int).diff().fillna(1) != 0).cumsum()
        for _, g in gb.groupby("run"):
            points = int(len(g))
            hours = round(float(g["dt_h"].sum()), 3)
            start = g["timestamp"].min()
            end = g["timestamp"].max()
            pr, pr_day = _find_pr_decimal(db, plant_id, str(start)[:10], plant_dc_kwp)
            shutdown_insolation = float(g["insolation_kwh_m2_step"].sum())
            step_loss = (pr or 0.0) * g["insolation_kwh_m2_step"] * plant_dc_kwp
            total_loss = float(step_loss.sum())
            ts.loc[g.index, "loss_kwh_step"] = step_loss.to_numpy()
            events.append(
                {
                    "event_id": f"GB-{start.strftime('%Y%m%d-%H%M%S')}",
                    "breakdown_points": points,
                    "breakdown_hours": hours,
                    "total_grid_breakdown_energy_loss_kwh": round(total_loss, 2),
                    "pr_used_pct": round(float(pr or 0.0) * 100.0, 2) if pr is not None else None,
                    "pr_source_date": pr_day,
                    "shutdown_insolation_kwh_m2": round(shutdown_insolation, 4),
                    "plant_dc_capacity_kwp": round(plant_dc_kwp, 3),
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
                "energy_loss_kwh_step": round(float(r.get("loss_kwh_step") or 0), 6),
                "grid_breakdown": bool(r["grid_breakdown"]),
            }
        )

    return events, timeline
