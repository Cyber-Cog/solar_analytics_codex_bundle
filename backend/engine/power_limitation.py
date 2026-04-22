"""
Power Limitation Detection (inverter-level only).
Between 10:00–15:00, if an inverter's AC power is more than X% below the peer
reference (median) while at least one inverter is normal, flag as limited.
Loss = area under curve (expected - actual) over the limitation window.
"""
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import List, Optional, Tuple
import pandas as pd
import numpy as np
from sqlalchemy import text
from sqlalchemy.orm import Session

from database import SessionLocal

# 10:00 AM to 03:00 PM (15:00) — configurable
PL_START_HOUR, PL_END_HOUR = 10, 15
_PL_START_MIN = PL_START_HOUR * 60
_PL_END_MIN = PL_END_HOUR * 60 + 59  # inclusive through 15:59
# Drop from reference: flag when actual < (1 - this) * reference.
# e.g. 0.30 => must be more than 30% below reference (actual < 70% of ref).
PL_DROP_FROM_REFERENCE = float(os.getenv("PL_DROP_FROM_REFERENCE", "0.30"))
# "Normal" inverter: at least 90% of reference
PL_NORMAL_RATIO = 0.9

_SQL_PL_TIME = (
    "AND (EXTRACT(HOUR FROM timestamp)::int * 60 + EXTRACT(MINUTE FROM timestamp)::int) "
    "BETWEEN :pm0 AND :pm1"
)

_SQL_INV = text(f"""
    SELECT timestamp, equipment_id AS inverter_id, AVG(value) AS ac_kw
    FROM raw_data_generic
    WHERE plant_id = :p
      AND equipment_level = 'inverter'
      AND signal = 'ac_power'
      AND timestamp >= :f AND timestamp <= :t
      {_SQL_PL_TIME}
    GROUP BY timestamp, equipment_id
""")

_SQL_IRR = text(f"""
    SELECT timestamp, signal, AVG(value) AS value
    FROM raw_data_generic
    WHERE plant_id = :p
      AND LOWER(TRIM(equipment_level::text)) IN ('plant', 'wms')
      AND signal IN ('irradiance', 'gti', 'ghi')
      AND timestamp >= :f AND timestamp <= :t
      {_SQL_PL_TIME}
    GROUP BY timestamp, signal
""")


def _fmt_ts(ts) -> Optional[str]:
    if ts is None:
        return None
    try:
        if isinstance(ts, (datetime, pd.Timestamp)):
            return ts.strftime("%Y-%m-%d %H:%M:%S")
        return str(ts)
    except Exception:
        return str(ts)


def _in_time_window(ts) -> bool:
    """True if timestamp is between 10:00 and 15:00 (inclusive). Kept for tests / parity docs."""
    try:
        if isinstance(ts, (datetime, pd.Timestamp)):
            h, m = int(ts.hour), int(ts.minute)
        else:
            ts_str = str(ts) if ts is not None else ""
            if len(ts_str) < 16:
                return False
            part = ts_str[11:16]
            h, m = int(part[:2]), int(part[3:5])
        minute = h * 60 + m
        return _PL_START_MIN <= minute <= _PL_END_MIN
    except Exception:
        return False


def _bind_pl_window(params: dict) -> dict:
    out = dict(params)
    out["pm0"] = _PL_START_MIN
    out["pm1"] = _PL_END_MIN
    return out


def _fetch_inv_rows(params: dict):
    session = SessionLocal()
    try:
        return session.execute(_SQL_INV, _bind_pl_window(params)).fetchall()
    finally:
        session.close()


def _fetch_irr_rows(params: dict):
    session = SessionLocal()
    try:
        return session.execute(_SQL_IRR, _bind_pl_window(params)).fetchall()
    finally:
        session.close()


def run_power_limitation(
    db: Session,
    plant_id: str,
    date_from: str,
    date_to: str,
) -> Tuple[List[dict], List[dict]]:
    """
    Compute power limitation from raw_data_generic (inverter ac_power).
    Returns (inverter_status_list, timeline_series).
    inverter_status_list: [{ inverter_id, total_energy_loss_kwh, last_seen_fault, investigation_window_start, investigation_window_end }, ...]
    timeline_series: [{ timestamp, inverter_id, expected_ac_kw, actual_ac_kw, limited, irradiance }, ...] for timeline API.
    """
    _ = db  # PL reads use thread-local SessionLocal; request session stays available for the caller
    f_ts = f"{date_from} 00:00:00"
    t_ts = f"{date_to} 23:59:59"
    base_params = {"p": plant_id, "f": f_ts, "t": t_ts}

    with ThreadPoolExecutor(max_workers=2) as pool:
        fut_inv = pool.submit(_fetch_inv_rows, base_params)
        fut_irr = pool.submit(_fetch_irr_rows, base_params)
        rows = fut_inv.result()
        irr_rows = fut_irr.result()

    if not rows:
        return [], []

    df = pd.DataFrame(rows, columns=["timestamp", "inverter_id", "ac_kw"])
    df["ac_kw"] = pd.to_numeric(df["ac_kw"], errors="coerce").fillna(0)

    df["ts"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["ts"]).sort_values(["inverter_id", "ts"]).reset_index(drop=True)
    if df.empty:
        return [], []

    irr_dict = {}
    if irr_rows:
        df_irr = pd.DataFrame(irr_rows, columns=["timestamp", "signal", "value"])
        df_irr["value"] = pd.to_numeric(df_irr["value"], errors="coerce")
        from pandas import Categorical
        df_irr["signal_cat"] = Categorical(df_irr["signal"], categories=["irradiance", "gti", "ghi"], ordered=True)
        df_irr = df_irr.sort_values("signal_cat").drop_duplicates("timestamp")
        irr_dict = dict(zip(df_irr["timestamp"], df_irr["value"]))

    # 1. Normalized Power
    cap = df.groupby("inverter_id")["ac_kw"].transform(lambda x: max(float(x.quantile(0.99)), 1.0))
    df["norm_pwr"] = df["ac_kw"] / cap

    # 2. Add Irradiance
    df["irradiance"] = df["timestamp"].map(irr_dict).fillna(0)

    # 3. Plant Median Normalized Power
    pl_med = df.groupby("timestamp")["norm_pwr"].median().rename("median_norm_pwr")
    df = df.merge(pl_med, on="timestamp", how="left")

    # 4. Previous Timestep Values (per inverter)
    df["prev_norm_pwr"] = df.groupby("inverter_id")["norm_pwr"].shift(1)
    df["prev_median_norm"] = df.groupby("inverter_id")["median_norm_pwr"].shift(1)
    df["prev_ts"] = df.groupby("inverter_id")["ts"].shift(1)

    # 5. Conditions for Robust Power Limitation Detection

    cond_irr = df["irradiance"] > 500

    # Keep a cadence-aware lookback (~5 minutes) so 5-min uploads are not over-penalized.
    step_sample = df.groupby("inverter_id")["ts"].diff().dropna()
    step_minutes = 1.0
    if len(step_sample) > 0:
        step_vals = step_sample.dt.total_seconds() / 60.0
        step_vals = step_vals[(step_vals > 0) & (step_vals <= 60)]
        if len(step_vals) > 0:
            step_minutes = float(step_vals.median())
    if not np.isfinite(step_minutes) or step_minutes <= 0:
        step_minutes = 1.0
    rolling_points = max(1, int(round(5.0 / step_minutes)))

    rolling_max = df.groupby("inverter_id")["norm_pwr"].transform(
        lambda x: x.rolling(rolling_points, min_periods=1).max()
    )
    cond_drop = (rolling_max - df["norm_pwr"]) > 0.30

    cond_dev = (df["median_norm_pwr"] - df["norm_pwr"]) > 0.25

    cond_power = df["norm_pwr"] > 0.10

    # 6. Apply logic
    df["state"] = cond_irr & cond_dev & cond_power
    df["trigger"] = cond_drop

    df["run_id"] = (df["state"] != df["state"].shift(1)).cumsum()

    limited_indices = set()
    for run_id, g in df[df["state"]].groupby("run_id"):
        if g.empty:
            continue

        start_ts = g["ts"].min()
        end_ts = g["ts"].max()
        duration_mins = (end_ts - start_ts).total_seconds() / 60.0
        row_count = len(g)

        valid_duration = (row_count >= 3 or duration_mins >= 3)

        start_idx = g.index[0]
        lookback = df.loc[max(0, start_idx - 5) : start_idx]
        lookback = lookback[lookback["inverter_id"] == g.iloc[0]["inverter_id"]]
        has_trigger = lookback["trigger"].any()

        if valid_duration and has_trigger:
            limited_indices.update(g.index.tolist())

    df["limited"] = False
    if limited_indices:
        df.loc[list(limited_indices), "limited"] = True

    df["reference_kw"] = df["median_norm_pwr"] * cap

    df["missing_kw"] = np.where(df["limited"], df["reference_kw"] - df["ac_kw"], 0.0)

    sample = df.groupby("inverter_id")["ts"].diff().dropna()
    dt_hours = float(sample.median().total_seconds() / 3600.0) if len(sample) > 0 else 1 / 60.0
    if dt_hours <= 0 or not np.isfinite(dt_hours):
        dt_hours = 1 / 60.0

    inv_status = []
    timeline_rows = []
    for inv_id, g in df.groupby("inverter_id"):
        g = g.sort_values("ts").reset_index(drop=True)
        g["limited_int"] = g["limited"].astype(int)
        g["run"] = (g["limited_int"].diff().fillna(0) != 0).cumsum()  # type: ignore
        windows = g[g["limited"]].groupby("run", as_index=False).agg(
            start=("timestamp", "first"),
            end=("timestamp", "last"),
            loss_kwh=("missing_kw", lambda x: (x * dt_hours).sum()),
        )
        total_loss = windows["loss_kwh"].sum()
        if total_loss <= 0 and not g["limited"].any():
            for _, r in g.iterrows():
                row = {
                    "timestamp": _fmt_ts(r["timestamp"]),
                    "inverter_id": inv_id,
                    "expected_ac_kw": round(float(r["reference_kw"]), 2),  # type: ignore
                    "actual_ac_kw": round(float(r["ac_kw"]), 2),  # type: ignore
                    "limited": False,
                }
                if pd.notnull(r["irradiance"]):
                    row["irradiance"] = round(float(r["irradiance"]), 2)  # type: ignore
                timeline_rows.append(row)
            continue

        last_seen = g.loc[g["limited"], "timestamp"].max() if g["limited"].any() else None
        win_starts = windows["start"].tolist()
        win_ends = windows["end"].tolist()
        inv_status.append({
            "inverter_id": inv_id,
            "total_energy_loss_kwh": round(float(total_loss), 2),
            "last_seen_fault": _fmt_ts(last_seen),
            "investigation_window_start": _fmt_ts(min(win_starts) if win_starts else None),
            "investigation_window_end": _fmt_ts(max(win_ends) if win_ends else None),
        })
        for _, r in g.iterrows():
            row = {
                "timestamp": _fmt_ts(r["timestamp"]),
                "inverter_id": inv_id,
                "expected_ac_kw": round(float(r["reference_kw"]), 2),  # type: ignore
                "actual_ac_kw": round(float(r["ac_kw"]), 2),  # type: ignore
                "limited": bool(r["limited"]),
            }
            if pd.notnull(r["irradiance"]):
                row["irradiance"] = round(float(r["irradiance"]), 2)  # type: ignore
            timeline_rows.append(row)

    return inv_status, timeline_rows
