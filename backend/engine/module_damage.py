"""
Bypass diode / module damage detection from SCB DC voltage telemetry.

Classification uses % deviation of each SCB voltage from the plant-level
reference (top-quartile inverter-median per timestamp):
  3–8%  → bypass diode (one bypass zone partially short-circuits the string)
  8–30% → module damage (1+ modules fully disconnected / shorted)

Energy loss proxy = dc_kw × pct_deviation × dt_h (MW·h captured as kWh).
"""
from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

# Classification uses % deviation of SCB voltage from plant reference
# (ref_v - scb_v) / ref_v as a decimal fraction
DM_BYPASS_PCT_LO  = float(os.getenv("DM_BYPASS_PCT_LO",  "0.03"))   # 3 %
DM_BYPASS_PCT_HI  = float(os.getenv("DM_BYPASS_PCT_HI",  "0.08"))   # 8 %
DM_DAMAGE_PCT_MIN = float(os.getenv("DM_DAMAGE_PCT_MIN", "0.08"))   # 8 %
DM_DAMAGE_PCT_MAX = float(os.getenv("DM_DAMAGE_PCT_MAX", "0.30"))   # 30 % cap (above → likely shutdown)
DM_MAX_DT_H = float(os.getenv("DM_MAX_DT_HOURS", "0"))              # 0 = auto cadence cap
DM_MIN_SAMPLES = int(os.getenv("DM_MIN_PERSIST_SAMPLES", "4"))

_SQL_VOLT = text("""
    SELECT timestamp, equipment_id AS scb_id, AVG(value::double precision) AS voltage_v
    FROM raw_data_generic
    WHERE plant_id = :p
      AND LOWER(TRIM(equipment_level::text)) = 'scb'
      AND LOWER(TRIM(signal::text)) = 'dc_voltage'
      AND timestamp >= :f AND timestamp <= :t
    GROUP BY timestamp, equipment_id
    ORDER BY timestamp, equipment_id
""")

_SQL_ARCH = text("""
    SELECT scb_id, inverter_id, strings_per_scb, modules_per_string
    FROM plant_architecture
    WHERE plant_id = :p AND scb_id IS NOT NULL
""")


def _fmt_ts(ts) -> Optional[str]:
    if ts is None:
        return None
    try:
        if isinstance(ts, (datetime, pd.Timestamp)):
            return ts.strftime("%Y-%m-%d %H:%M:%S")
        return str(ts)[:19]
    except Exception:
        return str(ts)


def _forward_dt_hours(ts: pd.Series) -> pd.Series:
    t = pd.to_datetime(ts, errors="coerce")
    dt = t.shift(-1).sub(t).dt.total_seconds() / 3600.0
    valid = dt[(dt > 0) & (dt <= 2.0)]
    median = float(valid.median()) if not valid.empty else (15.0 / 60.0)
    if not np.isfinite(median) or median <= 0:
        median = 15.0 / 60.0
    cap = DM_MAX_DT_H if DM_MAX_DT_H > 0 else max(2.0 * median, 1.0 / 60.0)
    return dt.fillna(median).clip(lower=1.0 / 3600.0, upper=cap)


def _load_arch(db: Session, plant_id: str) -> Dict[str, dict]:
    rows = db.execute(_SQL_ARCH, {"p": plant_id}).fetchall()
    out: Dict[str, dict] = {}
    for r in rows:
        sid = str(r[0])
        out[sid] = {
            "inverter_id": str(r[1]) if r[1] else None,
            "strings_per_scb": int(r[2] or 8),
            "modules_per_string": int(r[3] or 20),
        }
    return out


def run_module_damage(
    db: Session,
    plant_id: str,
    date_from: str,
    date_to: str,
) -> Tuple[List[dict], List[dict], dict]:
    """
    Returns (scb_status_list, timeline_rows, meta).
    scb_status: per-SCB summary with fault_kind, module_equiv, loss_kwh, windows.
    timeline: per-sample rows for investigate chart.
    """
    _ = db
    f_ts = f"{date_from[:10]} 00:00:00"
    t_ts = f"{date_to[:10]} 23:59:59"
    params = {"p": plant_id, "f": f_ts, "t": t_ts}

    arch = _load_arch(db, plant_id)
    rows = db.execute(_SQL_VOLT, params).fetchall()
    if not rows or not arch:
        return [], [], {"skipped": "no_voltage_or_architecture"}

    df = pd.DataFrame(rows, columns=["timestamp", "scb_id", "voltage_v"])
    df["voltage_v"] = pd.to_numeric(df["voltage_v"], errors="coerce")
    df = df.dropna(subset=["voltage_v"])
    if df.empty:
        return [], [], {"skipped": "empty_voltage"}

    df["inverter_id"] = df["scb_id"].map(lambda s: (arch.get(str(s)) or {}).get("inverter_id"))
    df = df.dropna(subset=["inverter_id"])
    df["ts"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.sort_values(["ts", "scb_id"]).reset_index(drop=True)

    inv_med = df.groupby(["ts", "inverter_id"], as_index=False)["voltage_v"].median()
    inv_med = inv_med.rename(columns={"voltage_v": "inv_median_v"})
    df = df.merge(inv_med, on=["ts", "inverter_id"], how="left")

    def _plant_ref_v(vals: pd.Series) -> float:
        x = vals.dropna().to_numpy(dtype=float)
        if x.size == 0:
            return float("nan")
        q75 = float(np.percentile(x, 75))
        top = x[x >= q75 - 1e-6]
        return float(np.median(top)) if top.size else float(np.median(x))

    ref_map = inv_med.groupby("ts")["inv_median_v"].apply(_plant_ref_v)
    df["ref_v"] = df["ts"].map(ref_map)

    def _modules_on_scb(scb_id: str) -> int:
        a = arch.get(str(scb_id)) or {}
        return max(1, int(a.get("strings_per_scb") or 8) * int(a.get("modules_per_string") or 20))

    df["modules_total"] = df["scb_id"].map(_modules_on_scb)

    # % deviation: positive means SCB voltage is BELOW reference
    df["pct_dev"] = np.where(
        df["ref_v"] > 1.0,
        ((df["ref_v"] - df["voltage_v"]) / df["ref_v"]).clip(lower=0.0),
        0.0,
    )

    def _classify(pct: float) -> str:
        if DM_BYPASS_PCT_LO <= pct < DM_BYPASS_PCT_HI:
            return "bypass_diode"
        if DM_DAMAGE_PCT_MIN <= pct <= DM_DAMAGE_PCT_MAX:
            return "module_damage"
        return "normal"

    df["fault_kind"] = df["pct_dev"].map(_classify)
    df["is_fault"] = df["fault_kind"] != "normal"
    # Keep module_equiv_short for backward compatibility (for chart tooltip)
    df["module_equiv_short"] = df["pct_dev"] * df["modules_total"]

    df["dt_h"] = df.groupby("scb_id", sort=False)["timestamp"].transform(_forward_dt_hours)

    # Fetch SCB current for energy proxy
    cur_rows = db.execute(
        text(
            """
            SELECT timestamp, equipment_id AS scb_id, AVG(value::double precision) AS i_a
            FROM raw_data_generic
            WHERE plant_id = :p
              AND equipment_level = 'scb' AND signal = 'dc_current'
              AND timestamp >= :f AND timestamp <= :t
            GROUP BY timestamp, equipment_id
            """
        ),
        params,
    ).fetchall()
    cur_df = pd.DataFrame(cur_rows, columns=["timestamp", "scb_id", "i_a"]) if cur_rows else pd.DataFrame()
    if not cur_df.empty:
        cur_df["i_a"] = pd.to_numeric(cur_df["i_a"], errors="coerce").fillna(0.0)
        df = df.merge(cur_df, on=["timestamp", "scb_id"], how="left")
    else:
        df["i_a"] = 0.0
    df["i_a"] = pd.to_numeric(df["i_a"], errors="coerce").fillna(0.0)

    from soiling_queries import scb_dc_map

    scb_dc = scb_dc_map(db, plant_id)
    df["dc_kw"] = df["scb_id"].map(lambda s: float(scb_dc.get(str(s)) or 33.6))
    # Loss = fraction of DC power affected (pct_dev) × rated DC capacity × interval
    df["loss_kwh_step"] = np.where(
        df["is_fault"],
        df["dc_kw"] * df["pct_dev"] * df["dt_h"],
        0.0,
    )

    status: List[dict] = []
    timeline: List[dict] = []

    for scb_id, g in df.groupby("scb_id"):
        g = g.sort_values("ts")
        fault_g = g[g["is_fault"]]
        if len(fault_g) < DM_MIN_SAMPLES:
            continue
        kind = fault_g["fault_kind"].mode().iloc[0] if not fault_g.empty else "normal"
        meq_med = float(fault_g["module_equiv_short"].median())
        pct_dev_med = float(fault_g["pct_dev"].median()) if "pct_dev" in fault_g else 0.0
        loss_kwh = float(g["loss_kwh_step"].sum())
        status.append(
            {
                "scb_id": scb_id,
                "inverter_id": (arch.get(str(scb_id)) or {}).get("inverter_id"),
                "fault_kind": kind,
                "module_equiv": round(meq_med, 2),
                "voltage_drop_pct": round(pct_dev_med * 100.0, 2),
                "impacted_modules_est": round(meq_med, 2),
                "total_energy_loss_kwh": round(loss_kwh, 3),
                "fault_points": int(len(fault_g)),
                "last_seen_fault": _fmt_ts(fault_g["timestamp"].max()),
                "investigation_window_start": _fmt_ts(fault_g["timestamp"].min()),
                "investigation_window_end": _fmt_ts(fault_g["timestamp"].max()),
            }
        )
        for _, r in g.iterrows():
            timeline.append(
                {
                    "timestamp": _fmt_ts(r["timestamp"]),
                    "scb_id": scb_id,
                    "inverter_id": r["inverter_id"],
                    "voltage_v": round(float(r["voltage_v"]), 2),
                    "reference_v": round(float(r["ref_v"]), 2) if pd.notnull(r["ref_v"]) else None,
                    "pct_dev": round(float(r["pct_dev"]) * 100.0, 2),
                    "module_equiv": round(float(r["module_equiv_short"]), 2),
                    "fault_kind": r["fault_kind"],
                    "current_a": round(float(r["i_a"]), 3),
                }
            )

    meta = {
        "bypass_count": sum(1 for s in status if s["fault_kind"] == "bypass_diode"),
        "damage_count": sum(1 for s in status if s["fault_kind"] == "module_damage"),
        "total_loss_kwh": round(sum(s["total_energy_loss_kwh"] for s in status), 3),
    }
    return status, timeline, meta


def summarise_module_damage(status: List[dict], meta: dict) -> dict:
    bypass = [s for s in status if s.get("fault_kind") == "bypass_diode"]
    damage = [s for s in status if s.get("fault_kind") == "module_damage"]
    loss_bypass = sum(float(s.get("total_energy_loss_kwh") or 0) for s in bypass)
    loss_damage = sum(float(s.get("total_energy_loss_kwh") or 0) for s in damage)
    return {
        "active_bypass_scbs": len(bypass),
        "active_damage_scbs": len(damage),
        "loss_bypass_kwh": round(loss_bypass, 3),
        "loss_damage_kwh": round(loss_damage, 3),
        "loss_total_kwh": round(loss_bypass + loss_damage, 3),
        "meta": meta or {},
    }
