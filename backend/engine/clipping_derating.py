"""
Clipping & Derating Detection (GTI-based virtual power curve).
===============================================================

Implements the workflow the user described:

Step 1 – Build a *virtual* power curve per inverter:
            P_virtual(t) = k × GTI(t)
        where k is the median of (P_actual / GTI) over *healthy* samples
        (no clipping, no shading, partial irradiance ~ morning/evening).

Step 2 – Gap(t) = max(0, P_virtual(t) − P_actual(t))

Step 3 – Classify each minute. A sample is eligible **only if the inverter is
actually producing** (excludes shutdown/offline minutes — those are a separate
fault). Classification then:
            POWER_CLIP      P_actual ≥ 97 % of rated AC capacity AND gap > noise
            STATIC_DERATE   active, below rated, gap > noise, local std < 1.5 % of rated
            DYNAMIC_DERATE  active, below rated, gap > noise, local std ≥ 1.5 % of rated

Step 4 – Energy loss per minute. We use the *per-sample* Δt (time to the next
observed sample) **capped at 5 minutes** so that a 30-minute data gap is not
logged as 30 minutes of loss.

Step 5 – Persistence filter: a fault must persist for ≥ 3 consecutive observed
samples to be counted (kills isolated noise flips).

Step 6 – Coverage guard: inverters with fewer than 40 % of expected samples
in the range are skipped (and listed in `summary.skipped`).

PERFORMANCE NOTES
-----------------
The engine is fully vectorised — no per-inverter Python loop during
classification. Rolling std, persistence filter, energy-loss integration and
timeline thinning are all groupby/transform operations on the full frame.
That keeps the worst case (96 inverters × 30 days of minute data ≈ 4 M rows)
well under a couple of seconds. The per-inverter timelines are cached with
the main tab payload, so the "Investigate" modal opens instantly.

Public API
----------
  run_clipping_derating(db, plant_id, date_from, date_to)
      -> (inverter_status, timelines_by_inv, meta)
  summarise_clipping_derating(inverter_status, meta) -> summary dict (KPIs)
"""
from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from database import SessionLocal
from models import EquipmentSpec


# ── Tunables (env-overridable) ────────────────────────────────────────────────
CD_RATED_HIT_RATIO        = float(os.getenv("CD_RATED_HIT_RATIO",        "0.97"))
CD_GTI_FLOOR_W_M2         = float(os.getenv("CD_GTI_FLOOR_W_M2",         "150"))
CD_HEALTHY_GTI_MIN        = float(os.getenv("CD_HEALTHY_GTI_MIN",        "200"))
CD_HEALTHY_GTI_MAX        = float(os.getenv("CD_HEALTHY_GTI_MAX",        "700"))
CD_HEALTHY_MAX_OF_RATED   = float(os.getenv("CD_HEALTHY_MAX_OF_RATED",   "0.85"))
CD_NOISE_FRAC_OF_RATED    = float(os.getenv("CD_NOISE_FRAC_OF_RATED",    "0.03"))
CD_STATIC_STD_FRAC_RATED  = float(os.getenv("CD_STATIC_STD_FRAC_RATED",  "0.015"))
CD_DERATE_RATED_MAX       = float(os.getenv("CD_DERATE_RATED_MAX",       "0.95"))
CD_ROLL_WINDOW_MIN        = int  (os.getenv("CD_ROLL_WINDOW_MIN",        "10"))

CD_MIN_ACTIVE_FRAC        = float(os.getenv("CD_MIN_ACTIVE_FRAC",        "0.03"))
CD_MIN_ACTIVE_ABS_KW      = float(os.getenv("CD_MIN_ACTIVE_ABS_KW",      "5.0"))
CD_MAX_DT_HOURS           = float(os.getenv("CD_MAX_DT_HOURS",           str(5.0 / 60.0)))
CD_PERSIST_MIN_SAMPLES    = int  (os.getenv("CD_PERSIST_MIN_SAMPLES",    "3"))
CD_HOUR_START             = int  (os.getenv("CD_HOUR_START",             "7"))
CD_HOUR_END               = int  (os.getenv("CD_HOUR_END",               "18"))
CD_MIN_COVERAGE_FRAC      = float(os.getenv("CD_MIN_COVERAGE_FRAC",      "0.40"))
CD_MIN_HEALTHY_SAMPLES    = int  (os.getenv("CD_MIN_HEALTHY_SAMPLES",    "20"))
CD_MAX_NORMAL_TL_PTS      = int  (os.getenv("CD_MAX_NORMAL_TL_PTS",      "600"))   # timeline cap per inverter


# ── SQL ──────────────────────────────────────────────────────────────────────
_SQL_INV_AC = text("""
    SELECT timestamp, equipment_id AS inverter_id, value AS ac_kw
    FROM raw_data_generic
    WHERE plant_id = :p
      AND LOWER(TRIM(equipment_level::text)) = 'inverter'
      AND signal = 'ac_power'
      AND timestamp >= :f AND timestamp <= :t
""")

_SQL_IRR = text("""
    SELECT timestamp, signal, value
    FROM raw_data_generic
    WHERE plant_id = :p
      AND LOWER(TRIM(equipment_level::text)) IN ('plant', 'wms')
      AND signal IN ('gti', 'irradiance', 'ghi')
      AND timestamp >= :f AND timestamp <= :t
""")


# ── Helpers ──────────────────────────────────────────────────────────────────
def _fetch_inv_rows(params: dict):
    s = SessionLocal()
    try:
        return s.execute(_SQL_INV_AC, params).fetchall()
    finally:
        s.close()


def _fetch_irr_rows(params: dict):
    s = SessionLocal()
    try:
        return s.execute(_SQL_IRR, params).fetchall()
    finally:
        s.close()


def _rated_kw_map(db: Session, plant_id: str) -> Dict[str, float]:
    """Prefer AC capacity from specs; fall back to rated_power or DC capacity."""
    out: Dict[str, float] = {}
    rows = db.query(EquipmentSpec).filter(
        EquipmentSpec.plant_id == plant_id,
        EquipmentSpec.equipment_type == "inverter",
    ).all()
    for r in rows:
        rated = r.ac_capacity_kw or r.rated_power or r.dc_capacity_kwp
        if r.equipment_id and rated and float(rated) > 0:
            out[str(r.equipment_id)] = float(rated)
    return out


def _grouped_persistence(mask: pd.Series, groups: pd.Series, min_run: int) -> pd.Series:
    """
    Vectorised persistence filter that respects inverter boundaries. A new run
    starts whenever (mask flips) OR (inverter_id changes). We then drop any run
    whose length is < min_run.
    """
    if min_run <= 1 or not mask.any():
        return mask
    boundary = (mask != mask.shift(1)) | (groups != groups.shift(1))
    seg = boundary.cumsum()
    run_sizes = mask.groupby(seg).transform("size")
    return mask & (run_sizes >= min_run)


# ── Main ─────────────────────────────────────────────────────────────────────
def run_clipping_derating(
    db: Session,
    plant_id: str,
    date_from: str,
    date_to: str,
) -> Tuple[List[dict], Dict[str, List[dict]], dict]:
    """
    Detect clipping / derating for every inverter in the date range.

    Returns
    -------
    (inverter_status, timelines_by_inv, meta)
        inverter_status  — one row per inverter that has an issue
        timelines_by_inv — dict mapping inverter_id → list of timeline rows
                           (all event rows plus a thinned normals baseline)
        meta             — per-inverter diagnostics (coverage %, healthy count,
                            k, skip flag + reason) for the advisory banner
    """
    _ = db
    f_ts = f"{date_from} 00:00:00"
    t_ts = f"{date_to} 23:59:59"
    params = {"p": plant_id, "f": f_ts, "t": t_ts}

    rated_map = _rated_kw_map(db, plant_id)

    with ThreadPoolExecutor(max_workers=2) as pool:
        fut_inv = pool.submit(_fetch_inv_rows, params)
        fut_irr = pool.submit(_fetch_irr_rows, params)
        inv_rows = fut_inv.result()
        irr_rows = fut_irr.result()

    meta: dict = {"inverters": {}, "skipped": [], "range": {"from": date_from, "to": date_to}}

    if not inv_rows or not irr_rows:
        meta["reason"] = "no inverter or irradiance data in range" if inv_rows else "no irradiance data in range"
        return [], {}, meta

    # ── Build irradiance map (prefer GTI → irradiance → GHI) ──
    df_irr = pd.DataFrame(irr_rows, columns=["timestamp", "signal", "value"])
    df_irr["value"] = pd.to_numeric(df_irr["value"], errors="coerce")
    df_irr = df_irr.dropna(subset=["value"])
    df_irr["signal_rank"] = df_irr["signal"].map({"gti": 0, "irradiance": 1, "ghi": 2}).fillna(9)
    df_irr = (
        df_irr.sort_values(["timestamp", "signal_rank"])
              .drop_duplicates("timestamp", keep="first")[["timestamp", "value"]]
              .rename(columns={"value": "gti"})
    )
    irr_map = dict(zip(df_irr["timestamp"], df_irr["gti"]))
    if not irr_map:
        meta["reason"] = "irradiance map empty after cleaning"
        return [], {}, meta

    # ── Inverter frame ──────────────────────────────────────────────────
    df = pd.DataFrame(inv_rows, columns=["timestamp", "inverter_id", "ac_kw"])
    df["ac_kw"] = pd.to_numeric(df["ac_kw"], errors="coerce")
    df["ts"]    = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["ts"])
    # Dedupe duplicates on (inverter, ts) — keep the last value
    df = df.sort_values(["inverter_id", "ts"]).drop_duplicates(["inverter_id", "ts"], keep="last").reset_index(drop=True)
    df["gti"] = df["timestamp"].map(irr_map)
    df["gti"] = pd.to_numeric(df["gti"], errors="coerce")
    df = df[df["gti"].notna()].reset_index(drop=True)
    if df.empty:
        meta["reason"] = "no inverter samples aligned with irradiance timestamps"
        return [], {}, meta

    # Per-sample Δt from the observed cadence of *this* inverter.
    df["dt_h"] = (
        df.groupby("inverter_id", sort=False)["ts"].diff().dt.total_seconds() / 3600.0
    )
    inv_median_dt = df.groupby("inverter_id", sort=False)["dt_h"].transform("median")
    df["dt_h"] = df["dt_h"].fillna(inv_median_dt).fillna(1.0 / 60.0)
    df["dt_h"] = df["dt_h"].clip(lower=1.0 / 3600.0, upper=CD_MAX_DT_HOURS)

    nominal_dt_min = float(np.nanmedian(df["dt_h"])) * 60.0 if len(df) else 1.0
    if not np.isfinite(nominal_dt_min) or nominal_dt_min <= 0:
        nominal_dt_min = 1.0
    roll_n = max(3, int(round(CD_ROLL_WINDOW_MIN / max(nominal_dt_min, 0.5))))

    # ── Rated kW per inverter (spec preferred, observed p99 fallback) ──
    df["rated_spec"] = df["inverter_id"].map(rated_map)
    # Observed fallback per inverter (p99 of non-null ac_kw)
    observed_p99 = (
        df.loc[df["ac_kw"].notna(), ["inverter_id", "ac_kw"]]
          .groupby("inverter_id")["ac_kw"].quantile(0.99)
    )
    df["rated"] = df["rated_spec"].fillna(df["inverter_id"].map(observed_p99)).fillna(1.0)
    df.loc[df["rated"] <= 0, "rated"] = 1.0

    # ── Productive-hour gate ──
    df["hr"] = df["ts"].dt.hour.astype("int16")
    in_hours = df["hr"].between(CD_HOUR_START, CD_HOUR_END - 1, inclusive="both")
    valid = df["ac_kw"].notna() & df["gti"].notna() & (df["gti"] > CD_GTI_FLOOR_W_M2) & in_hours

    # ── Coverage guard per inverter ──
    try:
        d0 = datetime.strptime(date_from[:10], "%Y-%m-%d")
        d1 = datetime.strptime(date_to[:10], "%Y-%m-%d")
        n_days = max(1, (d1 - d0).days + 1)
    except Exception:
        n_days = 1
    daylight_minutes = max(1, (CD_HOUR_END - CD_HOUR_START)) * 60 * n_days
    cadence_min = max(nominal_dt_min, 0.5)
    expected_samples = max(1.0, daylight_minutes / cadence_min)

    observed_counts = valid.groupby(df["inverter_id"]).sum().astype(int)
    coverage_pct = (observed_counts / expected_samples * 100.0).clip(upper=100.0).round(1)

    low_cov_invs = set(observed_counts.index[observed_counts / expected_samples < CD_MIN_COVERAGE_FRAC])

    # ── Calibration of k (per inverter) ──
    min_active_kw = np.maximum(CD_MIN_ACTIVE_FRAC * df["rated"], CD_MIN_ACTIVE_ABS_KW)
    df["min_active_kw"] = min_active_kw

    healthy_mask = (
        valid
        & (df["gti"] >= CD_HEALTHY_GTI_MIN)
        & (df["gti"] <= CD_HEALTHY_GTI_MAX)
        & (df["ac_kw"] >= min_active_kw)
        & (df["ac_kw"] <= CD_HEALTHY_MAX_OF_RATED * df["rated"])
    )
    healthy_counts = healthy_mask.groupby(df["inverter_id"]).sum().astype(int)

    ratio_frame = df.loc[healthy_mask, ["inverter_id", "ac_kw", "gti"]].copy()
    ratio_frame["r"] = ratio_frame["ac_kw"] / ratio_frame["gti"]
    ratio_frame = ratio_frame.replace([np.inf, -np.inf], np.nan).dropna(subset=["r"])
    ratio_frame = ratio_frame[ratio_frame["r"] > 0]
    k_per_inv = ratio_frame.groupby("inverter_id")["r"].median()

    # Pool of inverters that pass both guards
    all_invs = list(df["inverter_id"].unique())
    bad_calib_invs = {
        inv for inv in all_invs
        if (healthy_counts.get(inv, 0) < CD_MIN_HEALTHY_SAMPLES) or (k_per_inv.get(inv, 0) or 0) <= 0
    }
    skip_invs = low_cov_invs | bad_calib_invs
    keep_invs = [inv for inv in all_invs if inv not in skip_invs]

    # Record meta for every inverter
    for inv in all_invs:
        rated_val = float(df.loc[df["inverter_id"] == inv, "rated"].iat[0]) if (df["inverter_id"] == inv).any() else 1.0
        cov = float(coverage_pct.get(inv, 0.0))
        hc = int(healthy_counts.get(inv, 0))
        k_v = float(k_per_inv.get(inv, 0.0) or 0.0)
        skip_reason = None
        if inv in low_cov_invs:
            skip_reason = f"low data coverage ({cov:.0f} %)"
        elif inv in bad_calib_invs:
            skip_reason = f"not enough healthy calibration samples ({hc})"
        inv_meta = {
            "inverter_id":        inv,
            "rated_ac_kw":        round(rated_val, 2),
            "rated_source":       "spec" if rated_map.get(inv) else "observed_p99",
            "coverage_pct":       cov,
            "healthy_samples":    hc,
            "k_factor":           round(k_v, 4) if k_v else None,
            "skipped":            bool(skip_reason),
        }
        if skip_reason:
            inv_meta["skip_reason"] = skip_reason
            meta["skipped"].append({"inverter_id": inv, "reason": skip_reason, "coverage_pct": cov})
        meta["inverters"][inv] = inv_meta

    if not keep_invs:
        return [], {}, meta

    # ── Classify only on the kept subset (all vectorised) ──
    df = df[df["inverter_id"].isin(keep_invs)].reset_index(drop=True)
    in_hours = df["hr"].between(CD_HOUR_START, CD_HOUR_END - 1, inclusive="both")
    valid = df["ac_kw"].notna() & df["gti"].notna() & (df["gti"] > CD_GTI_FLOOR_W_M2) & in_hours

    df["k"]          = df["inverter_id"].map(k_per_inv)
    df["p_virtual"]  = df["k"] * df["gti"]
    df["gap"]        = (df["p_virtual"] - df["ac_kw"]).clip(lower=0.0)

    # Rolling std per inverter (single groupby-rolling call)
    roll = (
        df.groupby("inverter_id", sort=False)["ac_kw"]
          .rolling(roll_n, min_periods=max(3, roll_n // 2), center=True)
          .std()
    )
    df["ac_std"] = roll.reset_index(level=0, drop=True).fillna(0.0).values

    noise_abs       = CD_NOISE_FRAC_OF_RATED * df["rated"]
    static_std_abs  = CD_STATIC_STD_FRAC_RATED * df["rated"]
    min_active_kw_f = np.maximum(CD_MIN_ACTIVE_FRAC * df["rated"], CD_MIN_ACTIVE_ABS_KW)

    active     = valid & (df["ac_kw"] >= min_active_kw_f)
    at_cap     = active & (df["ac_kw"] >= CD_RATED_HIT_RATIO * df["rated"])
    below_rtd  = active & (df["ac_kw"] <  CD_DERATE_RATED_MAX * df["rated"])
    gap_real   = active & (df["gap"] > noise_abs)

    is_power_clip  = at_cap & gap_real
    is_static_der  = (~at_cap) & below_rtd & gap_real & (df["ac_std"] <  static_std_abs)
    is_dynamic_der = (~at_cap) & below_rtd & gap_real & (df["ac_std"] >= static_std_abs)

    # Persistence filter (respects inverter boundaries)
    is_power_clip  = _grouped_persistence(is_power_clip,  df["inverter_id"], CD_PERSIST_MIN_SAMPLES)
    is_static_der  = _grouped_persistence(is_static_der,  df["inverter_id"], CD_PERSIST_MIN_SAMPLES)
    is_dynamic_der = _grouped_persistence(is_dynamic_der, df["inverter_id"], CD_PERSIST_MIN_SAMPLES)

    # Per-kind energy loss (vector)
    gap_arr  = df["gap"].to_numpy(dtype=float, copy=False)
    dt_arr   = df["dt_h"].to_numpy(dtype=float, copy=False)
    loss_pc  = np.where(is_power_clip,  gap_arr * dt_arr, 0.0)
    loss_sd  = np.where(is_static_der,  gap_arr * dt_arr, 0.0)
    loss_dd  = np.where(is_dynamic_der, gap_arr * dt_arr, 0.0)

    df["_lpc"] = loss_pc
    df["_lsd"] = loss_sd
    df["_ldd"] = loss_dd
    df["_cpc"] = is_power_clip.astype(np.int32)
    df["_csd"] = is_static_der.astype(np.int32)
    df["_cdd"] = is_dynamic_der.astype(np.int32)

    # ── Per-inverter aggregates ──
    agg = df.groupby("inverter_id", sort=False).agg(
        loss_power_clip   =("_lpc", "sum"),
        loss_static_der   =("_lsd", "sum"),
        loss_dynamic_der  =("_ldd", "sum"),
        count_power_clip  =("_cpc", "sum"),
        count_static_der  =("_csd", "sum"),
        count_dynamic_der =("_cdd", "sum"),
    )

    # First / last event timestamps per inverter
    any_event_mask = is_power_clip | is_static_der | is_dynamic_der
    ev_df = df.loc[any_event_mask, ["inverter_id", "timestamp"]]
    first_seen = ev_df.groupby("inverter_id")["timestamp"].min()
    last_seen  = ev_df.groupby("inverter_id")["timestamp"].max()

    # ── State label column for the timeline ──
    df["state"] = np.select(
        [is_power_clip.values, is_static_der.values, is_dynamic_der.values],
        ["power_clip", "static_derate", "dynamic_derate"],
        default="normal",
    )

    # ── Build inverter_status rows + timelines ──
    inverter_status: List[dict] = []
    timelines_by_inv: Dict[str, List[dict]] = {}

    # Pre-format the timestamp column once as strings (cheap).
    df["ts_str"] = df["timestamp"].astype(str)

    tl_cols = {
        "ts_str":       "timestamp",
        "ac_kw":        "actual_ac_kw",
        "p_virtual":    "virtual_ac_kw",
        "gap":          "gap_kw",
        "gti":          "gti",
        "rated":        "rated_ac_kw",
        "state":        "state",
    }

    for inv_id, g in df.groupby("inverter_id", sort=False):
        a = agg.loc[inv_id]
        counts = {
            "power_clip":     int(a["count_power_clip"]),
            "static_derate":  int(a["count_static_der"]),
            "dynamic_derate": int(a["count_dynamic_der"]),
        }
        total_loss = float(a["loss_power_clip"] + a["loss_static_der"] + a["loss_dynamic_der"])
        any_issue = (counts["power_clip"] + counts["static_derate"] + counts["dynamic_derate"]) > 0

        # Timeline (always cached so the Investigate modal is instant even for
        # clean inverters the user might click on).
        ev_mask = g["state"].ne("normal")
        ev_df   = g.loc[ev_mask]
        norm_df = g.loc[~ev_mask]
        if len(norm_df) > CD_MAX_NORMAL_TL_PTS:
            step = max(1, int(round(len(norm_df) / CD_MAX_NORMAL_TL_PTS)))
            norm_df = norm_df.iloc[::step]
        tl = pd.concat([ev_df, norm_df]).sort_values("ts")
        tl = tl[["ts_str", "ac_kw", "p_virtual", "gap", "gti", "rated", "state"]].rename(columns=tl_cols)
        for c in ("actual_ac_kw", "virtual_ac_kw", "gap_kw", "gti", "rated_ac_kw"):
            tl[c] = tl[c].round(2)
        tl["inverter_id"] = inv_id
        # NaN-safe conversion: any NaN float becomes None in JSON
        tl_records = tl.where(pd.notna(tl), None).to_dict("records")
        timelines_by_inv[inv_id] = tl_records

        if not any_issue:
            # Still record meta (already done above); no status row needed.
            meta["inverters"][inv_id]["counts"] = counts
            meta["inverters"][inv_id]["dominant_kind"] = "normal"
            continue

        # Dominant kind / category
        if counts["power_clip"] >= (counts["static_derate"] + counts["dynamic_derate"]):
            dominant = "power_clip" if counts["power_clip"] > 0 else "normal"
        else:
            dominant = "static_derate" if counts["static_derate"] >= counts["dynamic_derate"] else "dynamic_derate"
        category = "clip" if dominant == "power_clip" else ("derate" if dominant in ("static_derate", "dynamic_derate") else "normal")

        inv_meta_entry = meta["inverters"][inv_id]
        inv_meta_entry["counts"] = counts
        inv_meta_entry["dominant_kind"] = dominant

        inverter_status.append({
            "inverter_id":                 inv_id,
            "rated_ac_kw":                 round(float(inv_meta_entry["rated_ac_kw"]), 2),
            "k_factor":                    inv_meta_entry["k_factor"],
            "coverage_pct":                inv_meta_entry["coverage_pct"],
            "dominant_kind":               dominant,
            "category":                    category,
            "total_energy_loss_kwh":       round(total_loss, 2),
            "loss_power_clipping_kwh":     round(float(a["loss_power_clip"]),  2),
            "loss_current_clipping_kwh":   0.0,  # reserved for DC-side
            "loss_static_derating_kwh":    round(float(a["loss_static_der"]),  2),
            "loss_dynamic_derating_kwh":   round(float(a["loss_dynamic_der"]), 2),
            "power_clip_points":           counts["power_clip"],
            "static_derate_points":        counts["static_derate"],
            "dynamic_derate_points":       counts["dynamic_derate"],
            "last_seen_fault":             str(last_seen.get(inv_id, "") or ""),
            "investigation_window_start":  str(first_seen.get(inv_id, "") or ""),
            "investigation_window_end":    str(last_seen.get(inv_id, "") or ""),
        })

    return inverter_status, timelines_by_inv, meta


def summarise_clipping_derating(inverter_status: List[dict], meta: Optional[dict] = None) -> dict:
    """KPIs for the two tabs (Clipping / Derating) and the overview tiles."""
    clip_inverters   = [r for r in inverter_status if r.get("category") == "clip"]
    derate_inverters = [r for r in inverter_status if r.get("category") == "derate"]

    loss_power_clip   = sum(float(r.get("loss_power_clipping_kwh")   or 0) for r in inverter_status)
    loss_current_clip = sum(float(r.get("loss_current_clipping_kwh") or 0) for r in inverter_status)
    loss_static_der   = sum(float(r.get("loss_static_derating_kwh")  or 0) for r in inverter_status)
    loss_dynamic_der  = sum(float(r.get("loss_dynamic_derating_kwh") or 0) for r in inverter_status)
    total_clip_loss   = loss_power_clip + loss_current_clip
    total_derate_loss = loss_static_der + loss_dynamic_der
    total_loss        = total_clip_loss + total_derate_loss

    inv_loss = sorted(
        [{
            "inverter_id":   r["inverter_id"],
            "loss_kwh":      float(r.get("total_energy_loss_kwh") or 0),
            "category":      r.get("category"),
            "dominant_kind": r.get("dominant_kind"),
         } for r in inverter_status if float(r.get("total_energy_loss_kwh") or 0) > 0],
        key=lambda x: x["loss_kwh"], reverse=True,
    )

    # Data-quality digest for the UI advisory banner.
    skipped = (meta or {}).get("skipped") or []
    inv_meta = (meta or {}).get("inverters") or {}
    total_inv = len(inv_meta)
    thin = [i for i in inv_meta.values() if i.get("coverage_pct", 0) < 80 and not i.get("skipped")]
    avg_cov = round(float(np.mean([i.get("coverage_pct", 0) for i in inv_meta.values()])), 1) if inv_meta else 0.0

    return {
        "total_energy_loss_kwh":       round(total_loss, 2),
        "active_clip_inverters":       len(clip_inverters),
        "loss_clipping_total_kwh":     round(total_clip_loss, 2),
        "loss_power_clipping_kwh":     round(loss_power_clip, 2),
        "loss_current_clipping_kwh":   round(loss_current_clip, 2),
        "active_derate_inverters":     len(derate_inverters),
        "loss_derating_total_kwh":     round(total_derate_loss, 2),
        "loss_static_derating_kwh":    round(loss_static_der, 2),
        "loss_dynamic_derating_kwh":   round(loss_dynamic_der, 2),
        "inverter_loss":               inv_loss,
        "data_quality": {
            "total_inverters":         total_inv,
            "skipped_count":           len(skipped),
            "skipped":                 skipped[:50],
            "avg_coverage_pct":        avg_cov,
            "thin_coverage_count":     len(thin),
        },
    }


def get_clipping_derating_timeline(
    db: Session,
    plant_id: str,
    date_from: str,
    date_to: str,
    inverter_id: Optional[str] = None,
) -> List[dict]:
    """
    Compatibility wrapper — when called directly (no cache), this still runs
    the full engine. The faults router prefers reading from the cached
    `timelines_by_inv` in the tab payload, so this is only a fallback.
    """
    _, timelines, _ = run_clipping_derating(db, plant_id, date_from, date_to)
    if inverter_id:
        return timelines.get(inverter_id, [])
    # Flatten
    out: List[dict] = []
    for rows in timelines.values():
        out.extend(rows)
    return out
