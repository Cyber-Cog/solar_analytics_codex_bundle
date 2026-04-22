"""
backend/engine/ds_detection.py
================================
Disconnected String (DS) Detection Engine.

Pipeline order:
  Step 0  — Load architecture (strings_per_scb, spare_flag, inverter mapping).
  Step 1  — Auto-detect data resolution → PERSISTENCE_POINTS = 30 min / resolution.
  Step 2  — Fetch irradiance for full date range (used in Steps 3 & 5).
  Step 3  — Leakage current detection on UNFILTERED data (must come before irradiance
             band filter, because leakage is defined at LOW irradiance < 200 W/m²
             which would be excluded by the band filter).
  Step 4  — Irradiance / time-of-day gate:
               • If WMS irradiance available : keep 600 ≤ irr ≤ 1000 W/m²
               • Else                        : keep 10:00–16:00
  Step 5  — Outlier removal (per TIMESTAMP, NOT per SCB-day):
               drop rows where scb_current < 0 OR > N_strings × Isc
  Step 6  — Near-constant value detection (per SCB-day):
               |current_t - current_t-1| < CONST_DELTA_TOL for > 30 consecutive
               pairs → remove that SCB for the entire day.
  Step 7  — Normalise: per_string_current = scb_current / N_strings
  Step 8  — Virtual reference string (per inverter + timestamp):
               top TOP_PERCENTILE SCBs → median(per_string_current) = ref_current
  Step 9  — DS candidate: missing_current = max(0, ref×N − actual), candidate if > ref
  Step 10 — Persistence window (≥ PERSISTENCE_POINTS consecutive): CONFIRMED_DS
  Step 11 — Window-min rule for missing_strings; resolution-aware energy loss.
  Step 12 — Bulk insert to fault_diagnostics.
"""

import os
import re
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import delete, text as sa_text
from sqlalchemy.orm import Session

from models import FaultDiagnostics


# ── Config (env-overridable) ──────────────────────────────────────────────────
IRRADIANCE_MIN = float(os.getenv("DS_IRRADIANCE_MIN", "100"))
IRRADIANCE_MAX = float(os.getenv("DS_IRRADIANCE_MAX", "1200"))

# Leakage detection thresholds
LEAKAGE_IRRADIANCE_MAX = float(os.getenv("DS_LEAKAGE_IRRADIANCE_MAX", "200"))  # W/m²
LEAKAGE_CURRENT_MIN_A  = float(os.getenv("DS_LEAKAGE_CURRENT_MIN_A",  "20"))   # A (SCB total)

DEFAULT_ISC_STC_A = float(os.getenv("DS_ISC_STC_A", "10"))      # A per string
TOP_PERCENTILE    = float(os.getenv("DS_TOP_PERCENTILE", "0.25"))  # top 25 %

# Constant-value tolerance: if abs difference < this between consecutive readings → "constant"
CONST_DELTA_TOL           = float(os.getenv("DS_CONST_DELTA_TOL", "0.01"))
CONST_CONSECUTIVE_THRESHOLD = int(os.getenv("DS_CONSTANT_CONSECUTIVE_THRESHOLD", "30"))
CONST_FLAT_RATIO_MIN = float(os.getenv("DS_CONSTANT_FLAT_RATIO_MIN", "0.995"))
CONST_UNIQUE_MAX = int(os.getenv("DS_CONSTANT_UNIQUE_MAX", "3"))

# Persistence: fault must last 30 MINUTES continuously (auto-converted to timestamps)
PERSISTENCE_MINUTES = int(os.getenv("DS_PERSISTENCE_MINUTES", "30"))


# ── Helpers ───────────────────────────────────────────────────────────────────

def _detect_resolution_minutes(df: pd.DataFrame) -> float:
    """
    Infer data resolution in minutes from the median of small consecutive gaps.
    Falls back to 1.0 minute if undetermined.
    """
    if df.empty or "timestamp" not in df.columns:
        return 1.0
    try:
        ts_sorted = df["timestamp"].drop_duplicates().sort_values()
        if len(ts_sorted) < 2:
            return 1.0
        diffs_min = ts_sorted.diff().dropna().dt.total_seconds() / 60.0
        valid = diffs_min[(diffs_min > 0) & (diffs_min <= 60)]
        return float(valid.median()) if not valid.empty else 1.0
    except Exception:
        return 1.0


def _virtual_reference_per_inverter(ts_inv_group: pd.DataFrame) -> float:
    """
    Virtual reference string current (per-string A) for one inverter+timestamp:
      - normalised per-string current for every SCB in that inverter
      - top TOP_PERCENTILE SCBs selected
      - return median of selected SCBs
    """
    vals = ts_inv_group["per_string_current"].to_numpy(dtype=float)
    vals = vals[np.isfinite(vals) & (vals > 0)]
    if vals.size == 0:
        return np.nan
    vals.sort()
    top_n = max(1, int(np.ceil(vals.size * TOP_PERCENTILE)))
    return float(np.median(vals[-top_n:]))


def _normalize_scb_id_for_arch(scb_id: str, inverter_id: Optional[str], arch_map: dict) -> Optional[str]:
    """
    Return an architecture-compatible SCB id.
    Supports both:
      - INV-01A-SCB-07 (canonical)
      - SCB-01A-07     (legacy NTPC/Apollo import shape)
    """
    sid = str(scb_id or "").strip()
    inv = str(inverter_id or "").strip()
    if not sid:
        return None
    if sid in arch_map:
        return sid

    m_short = re.match(r"^SCB-(\d{2}[A-Z])-(\d{1,2})$", sid, flags=re.IGNORECASE)
    if m_short:
        inv_tag = m_short.group(1).upper()
        scb_num = int(m_short.group(2))
        candidate = f"INV-{inv_tag}-SCB-{scb_num:02d}"
        if candidate in arch_map:
            return candidate

    m_full = re.match(r"^INV-(\d{2}[A-Z])-SCB-(\d{1,2})$", sid, flags=re.IGNORECASE)
    if m_full:
        inv_tag = m_full.group(1).upper()
        scb_num = int(m_full.group(2))
        candidate = f"SCB-{inv_tag}-{scb_num:02d}"
        if candidate in arch_map:
            return candidate

    if inv:
        m_tail = re.match(r"^SCB-(\d{1,2})$", sid, flags=re.IGNORECASE)
        if m_tail:
            scb_num = int(m_tail.group(1))
            candidate = f"{inv}-SCB-{scb_num:02d}"
            if candidate in arch_map:
                return candidate

    return None


def _fetch_irradiance_map(
    db: Session, plant_id: str, from_ts: str, to_ts: str
) -> tuple[Optional[str], dict]:
    """
    Returns (signal_name, {timestamp → irradiance_value}) for the best available
    plant/WMS irradiance signal. Returns (None, {}) if none found.
    Preference: irradiance > gti > ghi.
    """
    try:
        probe = db.execute(
            sa_text(
                "SELECT signal, COUNT(*) AS c FROM raw_data_generic "
                "WHERE plant_id=:p "
                "  AND LOWER(TRIM(equipment_level::text)) IN ('plant','wms') "
                "  AND signal IN ('irradiance','gti','ghi') "
                "  AND timestamp >= :f AND timestamp <= :t "
                "GROUP BY signal ORDER BY c DESC"
            ),
            {"p": plant_id, "f": from_ts, "t": to_ts},
        ).fetchall()
    except Exception:
        return None, {}

    if not probe:
        return None, {}

    signals = [r[0] for r in probe if r[0]]
    if not signals:
        return None, {}
    signal = "irradiance" if "irradiance" in signals else signals[0]

    try:
        rows = db.execute(
            sa_text(
                "SELECT timestamp, AVG(value) AS irr FROM raw_data_generic "
                "WHERE plant_id=:p "
                "  AND LOWER(TRIM(equipment_level::text)) IN ('plant','wms') "
                "  AND signal=:s "
                "  AND timestamp >= :f AND timestamp <= :t "
                "GROUP BY timestamp"
            ),
            {"p": plant_id, "s": signal, "f": from_ts, "t": to_ts},
        ).fetchall()
    except Exception:
        return signal, {}

    irr_map = {
        pd.to_datetime(r[0]): float(r[1])
        for r in rows
        if r[0] is not None and r[1] is not None
    }
    return signal, irr_map


# ── Main Entry Point ──────────────────────────────────────────────────────────

def run_ds_detection(plant_id: str, df: pd.DataFrame, db: Session):
    """
    Full DS detection pipeline.  Only touches fault_diagnostics — no UI changes.

    DB columns written:
      virtual_string_current  actual per-string current (A)
      expected_current        virtual reference per-string current (A)
      missing_current         SCB-level missing amps (≥ 0)
      missing_strings         disconnected string count (window-min rule)
      fault_status            NORMAL | CONFIRMED_DS
    """

    # ── Validate required columns ─────────────────────────────────────────────
    required = {"timestamp", "inverter_id", "scb_id", "scb_current"}
    if not required.issubset(df.columns):
        return

    df = df.dropna(subset=["timestamp", "inverter_id", "scb_id", "scb_current"]).copy()
    if df.empty:
        return

    df["timestamp"]   = pd.to_datetime(df["timestamp"])
    df["scb_current"] = pd.to_numeric(df["scb_current"], errors="coerce")
    df["dc_voltage"]  = (
        pd.to_numeric(df["dc_voltage"], errors="coerce")
        if "dc_voltage" in df.columns
        else np.nan
    )
    df.dropna(subset=["scb_current"], inplace=True)
    if df.empty:
        return

    # Fill missing DC voltage from raw_data_generic:
    # 1) SCB-level dc_voltage by (timestamp, scb_id)
    # 2) fallback inverter-level dc_voltage by (timestamp, inverter_id)
    try:
        need_fill = df["dc_voltage"].isna()
        if need_fill.any():
            from_ts_str = df["timestamp"].min().strftime("%Y-%m-%d %H:%M:%S")
            to_ts_str = df["timestamp"].max().strftime("%Y-%m-%d %H:%M:%S")

            scb_rows = db.execute(
                sa_text(
                    "SELECT timestamp, equipment_id, value "
                    "FROM raw_data_generic "
                    "WHERE plant_id = :p "
                    "  AND equipment_level = 'scb' "
                    "  AND signal = 'dc_voltage' "
                    "  AND timestamp >= :f AND timestamp <= :t"
                ),
                {"p": plant_id, "f": from_ts_str, "t": to_ts_str},
            ).fetchall()
            if scb_rows:
                scb_v = pd.DataFrame(scb_rows, columns=["timestamp", "scb_id", "_v_scb"])
                scb_v["timestamp"] = pd.to_datetime(scb_v["timestamp"], errors="coerce")
                scb_v["_v_scb"] = pd.to_numeric(scb_v["_v_scb"], errors="coerce")
                scb_v = scb_v.dropna(subset=["timestamp", "_v_scb"])
                if not scb_v.empty:
                    df = df.merge(scb_v, on=["timestamp", "scb_id"], how="left")
                    df["dc_voltage"] = df["dc_voltage"].fillna(df["_v_scb"])
                    df.drop(columns=["_v_scb"], inplace=True, errors="ignore")

            need_fill = df["dc_voltage"].isna()
            if need_fill.any():
                inv_rows = db.execute(
                    sa_text(
                        "SELECT timestamp, equipment_id, value "
                        "FROM raw_data_generic "
                        "WHERE plant_id = :p "
                        "  AND LOWER(TRIM(equipment_level::text)) = 'inverter' "
                        "  AND signal = 'dc_voltage' "
                        "  AND timestamp >= :f AND timestamp <= :t"
                    ),
                    {"p": plant_id, "f": from_ts_str, "t": to_ts_str},
                ).fetchall()
                if inv_rows:
                    inv_v = pd.DataFrame(inv_rows, columns=["timestamp", "inverter_id", "_v_inv"])
                    inv_v["timestamp"] = pd.to_datetime(inv_v["timestamp"], errors="coerce")
                    inv_v["_v_inv"] = pd.to_numeric(inv_v["_v_inv"], errors="coerce")
                    inv_v = inv_v.dropna(subset=["timestamp", "_v_inv"])
                    if not inv_v.empty:
                        df = df.merge(inv_v, on=["timestamp", "inverter_id"], how="left")
                        df["dc_voltage"] = df["dc_voltage"].fillna(df["_v_inv"])
                        df.drop(columns=["_v_inv"], inplace=True, errors="ignore")
    except Exception:
        # Keep DS detection resilient; if voltage enrichment fails, continue.
        pass

    # ── Step 0 — Architecture ─────────────────────────────────────────────────
    arch_rows = db.execute(
        sa_text(
            "SELECT DISTINCT scb_id, strings_per_scb, COALESCE(spare_flag, false) "
            "FROM plant_architecture "
            "WHERE plant_id = :p AND scb_id IS NOT NULL AND strings_per_scb IS NOT NULL"
        ),
        {"p": plant_id},
    ).fetchall()

    if not arch_rows:
        return

    arch_map  = {r[0]: int(r[1]) for r in arch_rows if r[1] and int(r[1]) > 0}
    spare_scbs = {r[0] for r in arch_rows if r[2] is True}
    if not arch_map:
        return

    df["scb_id"] = [
        _normalize_scb_id_for_arch(s, i, arch_map)
        for s, i in zip(df["scb_id"], df["inverter_id"])
    ]
    df = df[df["scb_id"].notna()].copy()
    df = df[df["scb_id"].isin(arch_map) & ~df["scb_id"].isin(spare_scbs)].copy()
    if df.empty:
        return

    df["string_count"] = df["scb_id"].map(arch_map)
    df = df[df["string_count"] > 0].copy()
    if df.empty:
        return

    # ── Step 1 — Auto-detect resolution → PERSISTENCE_POINTS ─────────────────
    resolution_minutes      = _detect_resolution_minutes(df)
    persistence_points      = max(2, int(round(PERSISTENCE_MINUTES / resolution_minutes)))
    # Tolerance for "consecutive": 1.5× the interval in seconds
    # (SUGGESTION A: auto-scaled so 5-min data with 90 s tolerance doesn't break windows)
    consecutive_tol_sec = max(90, int(resolution_minutes * 60 * 1.5))

    # ── Step 2 — Fetch irradiance for FULL date range ─────────────────────────
    from_ts_str = df["timestamp"].min().strftime("%Y-%m-%d %H:%M:%S")
    to_ts_str   = df["timestamp"].max().strftime("%Y-%m-%d %H:%M:%S")
    irr_signal, irr_map = _fetch_irradiance_map(db, plant_id, from_ts_str, to_ts_str)

    # ── Step 3 — Leakage current detection (on UNFILTERED data) ──────────────
    # MUST run BEFORE the irradiance band filter (Step 4), because leakage is
    # defined at LOW irradiance (<200 W/m²) — those timestamps are removed in Step 4.
    # Rule: if ALL irradiance values for an SCB-day are < 200 AND
    #       ALL scb_current values for that SCB-day are > 20 A → leakage, remove day.
    _leakage_removed: set = set()
    if irr_map:
        df_pre = df.copy()
        df_pre["irradiance"] = df_pre["timestamp"].map(irr_map)
        df_pre["irradiance"] = pd.to_numeric(df_pre["irradiance"], errors="coerce")
        df_pre["day_key"]    = df_pre["timestamp"].dt.date

        leakage_bad_keys: set = set()
        for (_scb, _day), grp in df_pre.groupby(["scb_id", "day_key"], sort=False):
            irr_vals  = grp["irradiance"].dropna()
            curr_vals = grp["scb_current"]
            if irr_vals.empty:
                continue
            if (irr_vals < LEAKAGE_IRRADIANCE_MAX).all() and (curr_vals > LEAKAGE_CURRENT_MIN_A).all():
                leakage_bad_keys.add((_scb, _day))

        if leakage_bad_keys:
            df["_day_key"] = df["timestamp"].dt.date
            leak_mask = pd.Series(
                list(zip(df["scb_id"], df["_day_key"]))
            ).isin(leakage_bad_keys).values
            df = df[~leak_mask].copy()
            df.drop(columns=["_day_key"], errors="ignore", inplace=True)

        _leakage_removed = {k[0] for k in leakage_bad_keys} - set(df["scb_id"].unique())

    if df.empty:
        return

    # ── Step 4 — Irradiance / time-of-day gate ───────────────────────────────
    # This is the first timestamp filter — only keep valid daytime irradiance window.
    _scbs_pre_filter = set(df["scb_id"].unique())

    if irr_map:
        df["irradiance"] = pd.to_numeric(df["timestamp"].map(irr_map), errors="coerce")
        df = df[
            np.isfinite(df["irradiance"])
            & (df["irradiance"] >= IRRADIANCE_MIN)
            & (df["irradiance"] <= IRRADIANCE_MAX)
        ].copy()
    else:
        t = df["timestamp"].dt.time
        df = df[
            (t >= pd.to_datetime("06:00:00").time())
            & (t <= pd.to_datetime("19:00:00").time())
        ].copy()

    if df.empty:
        return

    # ── Step 5 — Outlier removal (per TIMESTAMP, not per SCB-day) ────────────
    # Delta 6 fix: only remove the bad timestamps, not the whole SCB-day.
    isc_stc = DEFAULT_ISC_STC_A
    outlier_mask = (
        (~np.isfinite(df["scb_current"]))
        | (df["scb_current"] < 0)
        | (df["scb_current"] > isc_stc * df["string_count"])
    )
    df = df[~outlier_mask].copy()
    _scbs_after_outlier = set(df["scb_id"].unique())
    _outlier_removed    = _scbs_pre_filter - _scbs_after_outlier

    if df.empty:
        return

    # ── Step 6 — Near-constant value detection (per SCB-day) ─────────────────
    # Delta 3 fix: use abs(diff) < CONST_DELTA_TOL instead of exact equality.
    df = df.sort_values(["scb_id", "timestamp"]).reset_index(drop=True)
    df["day_key"] = df["timestamp"].dt.date

    bad_constant_keys: set = set()
    for (_scb, _day), grp in df.groupby(["scb_id", "day_key"], sort=False):
        vals = grp.sort_values("timestamp")["scb_current"].values
        n_v  = len(vals)
        if n_v < 2:
            continue
        # Guardrail: avoid dropping legitimate stable production days.
        # Drop only if the full SCB-day looks effectively frozen.
        diffs = np.abs(np.diff(vals))
        flat_ratio = float(np.mean(diffs < CONST_DELTA_TOL)) if diffs.size else 0.0
        uniq = int(pd.Series(vals).nunique(dropna=True))
        if flat_ratio >= CONST_FLAT_RATIO_MIN and uniq <= CONST_UNIQUE_MAX:
            bad_constant_keys.add((_scb, _day))
            continue

        # Keep legacy run-length check as a secondary condition.
        run = 1
        for idx in range(1, n_v):
            if abs(vals[idx] - vals[idx - 1]) < CONST_DELTA_TOL:
                run += 1
                if run > CONST_CONSECUTIVE_THRESHOLD and flat_ratio >= 0.95:
                    bad_constant_keys.add((_scb, _day))
                    break
            else:
                run = 1

    if bad_constant_keys:
        flat_mask = pd.Series(
            list(zip(df["scb_id"], df["day_key"]))
        ).isin(bad_constant_keys).values
        df = df[~flat_mask].copy()

    _scbs_after_constant = set(df["scb_id"].unique())
    _constant_removed    = _scbs_after_outlier - _scbs_after_constant

    # Drop helper columns
    for col in ["day_key", "irradiance"]:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

    if df.empty:
        return

    # ── Save filter summary to fault_cache ────────────────────────────────────
    try:
        _run_date = df["timestamp"].min().strftime("%Y-%m-%d")
        from fault_cache import set_cached as _fc_set
        _fc_set(
            db,
            f"filter_summary:{plant_id}:{_run_date}",
            {
                "outlier":  sorted(_outlier_removed),
                "constant": sorted(_constant_removed),
                "leakage":  sorted(_leakage_removed),
            },
            ttl_minutes=876000,
        )
    except Exception:
        pass

    # ── Clear existing fault_diagnostics for this time range ─────────────────
    min_ts = df["timestamp"].min().strftime("%Y-%m-%d %H:%M:%S")
    max_ts = df["timestamp"].max().strftime("%Y-%m-%d %H:%M:%S")
    db.execute(
        delete(FaultDiagnostics)
        .where(FaultDiagnostics.plant_id == plant_id)
        .where(FaultDiagnostics.timestamp >= min_ts)
        .where(FaultDiagnostics.timestamp <= max_ts)
    )
    db.commit()

    # ── Step 7 — Normalise ────────────────────────────────────────────────────
    df["per_string_current"] = df["scb_current"] / df["string_count"]

    # ── Step 8 — Virtual reference string ────────────────────────────────────
    ref_series = df.groupby(
        ["timestamp", "inverter_id"], group_keys=False
    ).apply(_virtual_reference_per_inverter)

    ref_df = ref_series.reset_index()
    ref_df.columns = ["timestamp", "inverter_id", "ref_current"]
    df = df.merge(ref_df, on=["timestamp", "inverter_id"], how="left")
    df = df[np.isfinite(df["ref_current"]) & (df["ref_current"] > 0)].copy()
    if df.empty:
        return

    # ── Step 9 — DS candidate detection ──────────────────────────────────────
    df["expected_scb_current"] = df["ref_current"] * df["string_count"]
    df["missing_current"]      = np.maximum(0.0, df["expected_scb_current"] - df["scb_current"])
    df["candidate"]            = df["missing_current"] > df["ref_current"]
    df["ds_count"]             = 0

    cand_mask = df["candidate"] & (df["ref_current"] > 0)
    df.loc[cand_mask, "ds_count"] = np.floor(
        df.loc[cand_mask, "missing_current"] / df.loc[cand_mask, "ref_current"]
    ).astype(int)
    df.loc[df["ds_count"] < 0, "ds_count"] = 0

    # ── Step 10+11 — Persistence window + window-min + energy loss ────────────
    df = df.sort_values(["scb_id", "timestamp"]).reset_index(drop=True)
    n = len(df)
    if n == 0:
        return

    ts_epoch      = df["timestamp"].values.astype("datetime64[s]").astype(np.int64)
    scb_arr       = df["scb_id"].values
    candidate_arr = df["candidate"].values.astype(bool)
    ds_arr        = df["ds_count"].values.astype(np.int32)
    mc_arr        = df["missing_current"].values.astype(np.float64)
    dv_arr        = df["dc_voltage"].values.astype(np.float64)
    ref_arr       = df["ref_current"].values.astype(np.float64)

    fault_flag = np.zeros(n, dtype=np.int8)
    power_loss = np.zeros(n, dtype=np.float64)
    energy_loss = np.zeros(n, dtype=np.float64)
    ms_out      = np.zeros(n, dtype=np.int32)

    # SCB segment boundaries (data is sorted by scb_id, timestamp)
    scb_change = np.concatenate([[True], scb_arr[1:] != scb_arr[:-1]])
    starts = np.where(scb_change)[0]
    ends   = np.concatenate([starts[1:], [n]])

    # Energy divisor: kWh per timestamp = kW × (resolution_minutes / 60)
    # SUGGESTION B: resolution-aware energy loss (was hardcoded to /60 → only valid for 1-min data)
    energy_interval_h = resolution_minutes / 60.0

    for si, ei in zip(starts, ends):
        i = si
        while i < ei:
            if not candidate_arr[i]:
                i += 1
                continue

            # Identify consecutive candidate run
            run_start = i
            run_end   = i + 1
            while run_end < ei:
                if not candidate_arr[run_end]:
                    break
                dt = int(ts_epoch[run_end]) - int(ts_epoch[run_end - 1])
                if not (0 < dt <= consecutive_tol_sec):
                    break
                run_end += 1

            run_len = run_end - run_start
            if run_len >= persistence_points:
                win_min = max(0, int(np.min(ds_arr[run_start:run_end])))
                for j in range(run_start, run_end):
                    fault_flag[j] = 1
                    ms_out[j]     = win_min
                    if (
                        np.isfinite(dv_arr[j]) and dv_arr[j] > 0
                        and np.isfinite(mc_arr[j]) and mc_arr[j] > 0
                    ):
                        pl = dv_arr[j] * mc_arr[j] / 1000.0
                        power_loss[j]  = pl
                        energy_loss[j] = pl * energy_interval_h

            i = run_end

    # ── Step 12 — Bulk insert ─────────────────────────────────────────────────
    ts_strs  = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S").values
    inv_vals = df["inverter_id"].values
    psc_vals = df["per_string_current"].values

    BATCH = 10000
    batch: list = []
    for i in range(n):
        batch.append({
            "timestamp":             ts_strs[i],
            "plant_id":              plant_id,
            "inverter_id":           inv_vals[i],
            "scb_id":                scb_arr[i],
            "virtual_string_current": round(float(psc_vals[i]), 4) if np.isfinite(psc_vals[i]) else 0.0,
            "expected_current":      round(float(ref_arr[i]),  4) if np.isfinite(ref_arr[i])  else 0.0,
            "missing_current":       round(float(mc_arr[i]),   4),
            "missing_strings":       int(ms_out[i]),
            "power_loss_kw":         round(float(power_loss[i]),  4),
            "energy_loss_kwh":       round(float(energy_loss[i]), 6),
            "fault_status":          "CONFIRMED_DS" if fault_flag[i] == 1 else "NORMAL",
        })
        if len(batch) >= BATCH:
            db.bulk_insert_mappings(FaultDiagnostics, batch)
            db.flush()
            batch = []

    if batch:
        db.bulk_insert_mappings(FaultDiagnostics, batch)
    db.commit()

    # Sidecar recurrence metadata (episodes/day) for fast "active since" queries.
    try:
        from engine.fault_episodes import rebuild_fault_episodes_for_scbs
        rebuild_fault_episodes_for_scbs(db, plant_id, set(scb_arr.tolist()))
    except Exception:
        pass

    try:
        from fault_cache import invalidate_plant
        invalidate_plant(db, plant_id)
        from dashboard_cache import invalidate_plant as _inv_dash
        _inv_dash(plant_id)
    except Exception:
        pass


# ── String-level DS Detection (for plants with per-string current data) ────────

STRING_IRRADIANCE_MIN = float(os.getenv("DS_STRING_IRRADIANCE_MIN", "50"))   # W/m²


def run_ds_detection_string_level(plant_id: str, db: Session,
                                   date_from: str, date_to: str) -> None:
    """
    String-level DS detection pipeline for plants (e.g. PDCL) that have
    individual string-current data stored at equipment_level='string'.

    Logic:
        - Fetch irradiance (same helper as SCB pipeline).
        - For each string: string_current == 0 AND irradiance > 50 W/m²
          for >= PERSISTENCE_MINUTES consecutively → CONFIRMED_DS.
        - Results are aggregated per SCB and written to fault_diagnostics
          (same schema as run_ds_detection, dashboard needs no changes).

    Completely isolated from run_ds_detection — does NOT touch Nokhra or
    any other plant.
    """

    # ── Fetch per-string current ───────────────────────────────────────────────
    sql_str = sa_text("""
        SELECT s.timestamp,
               pa.inverter_id,
               pa.scb_id,
               s.equipment_id AS string_id,
               s.value        AS string_current
        FROM   raw_data_generic s
        JOIN   plant_architecture pa
               ON  pa.plant_id       = s.plant_id
               AND pa.string_id      = s.equipment_id
        WHERE  s.plant_id            = :plant_id
          AND  s.equipment_level     = 'string'
          AND  s.signal              = 'string_current'
          AND  s.timestamp          >= :df
          AND  s.timestamp          <= :dt
        ORDER  BY s.equipment_id, s.timestamp
    """)
    rows = db.execute(sql_str, {"plant_id": plant_id,
                                "df": date_from, "dt": date_to}).fetchall()
    if not rows:
        return

    df = pd.DataFrame(rows, columns=[
        "timestamp", "inverter_id", "scb_id", "string_id", "string_current"
    ])
    df["timestamp"]      = pd.to_datetime(df["timestamp"])
    df["string_current"] = pd.to_numeric(df["string_current"], errors="coerce")
    df.dropna(subset=["string_current"], inplace=True)
    if df.empty:
        return

    # ── Irradiance gate ────────────────────────────────────────────────────────
    from_ts = df["timestamp"].min().strftime("%Y-%m-%d %H:%M:%S")
    to_ts   = df["timestamp"].max().strftime("%Y-%m-%d %H:%M:%S")
    _, irr_map = _fetch_irradiance_map(db, plant_id, from_ts, to_ts)

    if irr_map:
        df["irradiance"] = pd.to_numeric(df["timestamp"].map(irr_map), errors="coerce")
        df = df[df["irradiance"].notna() & (df["irradiance"] > STRING_IRRADIANCE_MIN)].copy()
    else:
        t = df["timestamp"].dt.hour
        df = df[(t >= 7) & (t <= 18)].copy()

    if df.empty:
        return

    # ── Resolution / persistence ───────────────────────────────────────────────
    resolution_minutes  = _detect_resolution_minutes(df)
    persistence_points  = max(2, int(round(PERSISTENCE_MINUTES / resolution_minutes)))
    consecutive_tol_sec = max(90, int(resolution_minutes * 60 * 1.5))
    energy_interval_h   = resolution_minutes / 60.0

    # ── Per-string persistence window ─────────────────────────────────────────
    df = df.sort_values(["string_id", "timestamp"]).reset_index(drop=True)
    n            = len(df)
    ts_epoch     = df["timestamp"].values.astype("datetime64[s]").astype(np.int64)
    str_arr      = df["string_id"].values
    curr_arr     = df["string_current"].values.astype(np.float64)
    fault_flag   = np.zeros(n, dtype=np.int8)

    str_change = np.concatenate([[True], str_arr[1:] != str_arr[:-1]])
    starts     = np.where(str_change)[0]
    ends       = np.concatenate([starts[1:], [n]])

    for si, ei in zip(starts, ends):
        i = si
        while i < ei:
            if curr_arr[i] != 0.0:
                i += 1
                continue
            run_start = i
            run_end   = i + 1
            while run_end < ei:
                if curr_arr[run_end] != 0.0:
                    break
                dt_s = int(ts_epoch[run_end]) - int(ts_epoch[run_end - 1])
                if not (0 < dt_s <= consecutive_tol_sec):
                    break
                run_end += 1
            if (run_end - run_start) >= persistence_points:
                fault_flag[run_start:run_end] = 1
            i = run_end

    df["fault_flag"] = fault_flag
    df["ts_str"]     = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # ── Load supporting data ───────────────────────────────────────────────────
    sql_v = sa_text("""
        SELECT timestamp, equipment_id, value FROM raw_data_generic
        WHERE  plant_id = :p AND equipment_level = 'scb'
          AND  signal = 'dc_voltage'
          AND  timestamp >= :df AND timestamp <= :dt
    """)
    v_rows = db.execute(sql_v, {"p": plant_id, "df": from_ts, "dt": to_ts}).fetchall()
    v_map  = {(str(r[0]), r[1]): float(r[2]) for r in v_rows if r[2] is not None}

    arch_rows = db.execute(
        sa_text("SELECT DISTINCT scb_id, strings_per_scb FROM plant_architecture "
                "WHERE plant_id = :p AND strings_per_scb IS NOT NULL"),
        {"p": plant_id}
    ).fetchall()
    strings_per_scb_map = {r[0]: int(r[1]) for r in arch_rows if r[1]}

    # ── Build inverter-level reference current (top 25 % SCBs' avg) ───────────
    ref_lookup: dict = {}
    for (scb_id, ts_str, inv_id), grp in df.groupby(
            ["scb_id", "ts_str", "inverter_id"], sort=False):
        nz = grp.loc[grp["string_current"] > 0, "string_current"]
        if not nz.empty:
            ref_lookup.setdefault((inv_id, ts_str), []).append(float(nz.mean()))

    ref_final: dict = {}
    for (inv_id, ts_str), vals in ref_lookup.items():
        sorted_v = sorted(vals)
        top_n    = max(1, int(np.ceil(len(sorted_v) * TOP_PERCENTILE)))
        ref_final[(inv_id, ts_str)] = float(np.median(sorted_v[-top_n:]))

    # ── Clear and insert fault_diagnostics ────────────────────────────────────
    db.execute(
        delete(FaultDiagnostics)
        .where(FaultDiagnostics.plant_id == plant_id)
        .where(FaultDiagnostics.timestamp >= from_ts)
        .where(FaultDiagnostics.timestamp <= to_ts)
    )
    db.commit()

    BATCH = 10000
    batch: list = []
    for (scb_id, ts_str, inv_id), grp in df.groupby(
            ["scb_id", "ts_str", "inverter_id"], sort=False):
        n_total   = strings_per_scb_map.get(scb_id, len(grp))
        n_ds      = int(grp["fault_flag"].sum())
        non_zero  = grp.loc[grp["string_current"] > 0, "string_current"]
        avg_curr  = float(non_zero.mean()) if not non_zero.empty else 0.0
        ref_curr  = ref_final.get((inv_id, ts_str), avg_curr)
        miss_I    = max(0.0, ref_curr - avg_curr) * max(n_ds, 1) if n_ds > 0 else 0.0
        v_val     = v_map.get((ts_str, scb_id), 0.0)
        pwr_loss  = (v_val * miss_I / 1000.0) if (v_val > 0 and miss_I > 0) else 0.0

        batch.append({
            "timestamp":              ts_str,
            "plant_id":               plant_id,
            "inverter_id":            inv_id,
            "scb_id":                 scb_id,
            "virtual_string_current": round(avg_curr, 4),
            "expected_current":       round(ref_curr, 4),
            "missing_current":        round(miss_I,   4),
            "missing_strings":        n_ds,
            "power_loss_kw":          round(pwr_loss, 4),
            "energy_loss_kwh":        round(pwr_loss * energy_interval_h, 6),
            "fault_status":           "CONFIRMED_DS" if n_ds > 0 else "NORMAL",
        })
        if len(batch) >= BATCH:
            db.bulk_insert_mappings(FaultDiagnostics, batch)
            db.flush()
            batch = []

    if batch:
        db.bulk_insert_mappings(FaultDiagnostics, batch)
    db.commit()

    # Sidecar recurrence metadata (episodes/day) for fast "active since" queries.
    try:
        from engine.fault_episodes import rebuild_fault_episodes_for_scbs
        rebuild_fault_episodes_for_scbs(db, plant_id, set(df["scb_id"].astype(str).tolist()))
    except Exception:
        pass

    try:
        from fault_cache import invalidate_plant
        invalidate_plant(db, plant_id)
        from dashboard_cache import invalidate_plant as _inv_dash
        _inv_dash(plant_id)
    except Exception:
        pass

