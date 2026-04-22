"""
Hierarchical communication issue detection against WMS timestamps.

Ownership per daylight timestamp:
  - Plant-level when all inverters are missing.
  - Inverter-level when inverter AC is missing.
  - Inverter-level ingestion gap when all SCBs under a present inverter are missing.
  - SCB-level only for remaining missing SCBs under a present inverter.

Loss model:
  - Expected power is irradiance-driven: dc_kWp * (irradiance / 1000) * performance_factor.
  - performance_factor comes from healthy inverter AC rows at the same timestamp, with a
    plant-wide daylight median fallback.
  - Loss is assigned only to plant-level and inverter-level communication events.
  - SCB-only missing data while inverter AC is present is treated as ingestion/missing-source
    context and does not contribute communication loss.
"""

from __future__ import annotations

import os
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from engine.inverter_shutdown import _normalize_date_str, _pick_irradiance


COMM_IRRADIANCE_MIN = float(os.getenv("COMM_IRRADIANCE_MIN", "5"))
COMM_DEFAULT_STEP_H = 1.0 / 60.0
COMM_PERF_FACTOR_MAX = float(os.getenv("COMM_PERF_FACTOR_MAX", "1.25"))
COMM_PERF_FACTOR_DEFAULT = float(os.getenv("COMM_PERF_FACTOR_DEFAULT", "1.0"))

ISSUE_PLANT = "plant_communication"
ISSUE_INV = "inverter_communication"
ISSUE_INV_ALL_SCB = "all_scbs_missing_inverter_present"
ISSUE_SCB = "scb_data_missing"

ISSUE_STATUS = {
    ISSUE_PLANT: "Plant communication fault",
    ISSUE_INV: "Inverter communication fault",
    ISSUE_INV_ALL_SCB: "All SCBs missing (inverter data present)",
    ISSUE_SCB: "SCB data missing",
}


def _empty_summary() -> dict:
    return {
        "total_communication_issues": 0,
        "total_loss_kwh": 0.0,
        "total_communication_hours": 0.0,
        "plant_issue_count": 0,
        "inverter_issue_count": 0,
        "scb_issue_count": 0,
        "ingestion_gap_issue_count": 0,
        "irradiance_threshold_w_m2": COMM_IRRADIANCE_MIN,
    }


def _bucket_to_minute(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.floor("min")


def _median_step_hours(ts_values: pd.Series) -> float:
    vals = pd.Series(ts_values).dropna().drop_duplicates().sort_values()
    diffs = vals.diff().dropna()
    if len(diffs):
        step_h = float(diffs.median().total_seconds() / 3600.0)
        if np.isfinite(step_h) and step_h > 0:
            return step_h
    return COMM_DEFAULT_STEP_H


def _load_irradiance_frame(db: Session, plant_id: str, f_ts: str, t_ts: str) -> pd.DataFrame:
    sql_irr = text(
        """
        SELECT timestamp, signal, AVG(value) AS irradiance
        FROM raw_data_generic
        WHERE plant_id = :p
          AND LOWER(TRIM(equipment_level::text)) IN ('plant', 'wms')
          AND signal IN ('irradiance', 'gti', 'ghi')
          AND timestamp >= :f AND timestamp <= :t
        GROUP BY timestamp, signal
        """
    )
    irr_rows = db.execute(sql_irr, {"p": plant_id, "f": f_ts, "t": t_ts}).fetchall()
    if not irr_rows:
        return pd.DataFrame(columns=["timestamp", "irradiance"])

    df_irr = pd.DataFrame(irr_rows, columns=["timestamp", "signal", "irradiance"])
    df_irr["timestamp"] = pd.to_datetime(df_irr["timestamp"], errors="coerce")
    df_irr["irradiance"] = pd.to_numeric(df_irr["irradiance"], errors="coerce")
    df_irr = df_irr.dropna(subset=["timestamp", "irradiance"]).copy()
    if df_irr.empty:
        return pd.DataFrame(columns=["timestamp", "irradiance"])

    irr_map = _pick_irradiance(df_irr)
    out = pd.DataFrame({"timestamp": list(irr_map.keys()), "irradiance": list(irr_map.values())})
    out["timestamp"] = _bucket_to_minute(out["timestamp"])
    out["irradiance"] = pd.to_numeric(out["irradiance"], errors="coerce")
    out = (
        out.dropna(subset=["timestamp", "irradiance"])
        .groupby("timestamp", as_index=False)["irradiance"]
        .mean()
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    return out


def _load_architecture_meta(db: Session, plant_id: str) -> Tuple[Dict[str, float], Dict[str, Set[str]], Dict[str, str]]:
    sql_arch = text(
        """
        SELECT inverter_id, scb_id, COALESCE(spare_flag, false) AS spare_flag, SUM(dc_capacity_kw) AS dc_capacity_kw
        FROM plant_architecture
        WHERE plant_id = :p
          AND inverter_id IS NOT NULL
        GROUP BY inverter_id, scb_id, COALESCE(spare_flag, false)
        """
    )
    rows = db.execute(sql_arch, {"p": plant_id}).fetchall()
    if not rows:
        return {}, {}, {}

    arch = pd.DataFrame(rows, columns=["inverter_id", "scb_id", "spare_flag", "dc_capacity_kw"])
    arch["dc_capacity_kw"] = pd.to_numeric(arch["dc_capacity_kw"], errors="coerce").fillna(0.0)
    arch["spare_flag"] = arch["spare_flag"].fillna(False).astype(bool)

    inv_caps = (
        arch.groupby("inverter_id", as_index=False)["dc_capacity_kw"]
        .sum()
        .set_index("inverter_id")["dc_capacity_kw"]
        .to_dict()
    )
    inv_to_scbs: Dict[str, Set[str]] = defaultdict(set)
    scb_to_inv: Dict[str, str] = {}
    for _, row in arch.iterrows():
        inv = str(row["inverter_id"])
        scb = row["scb_id"]
        if pd.isna(scb) or bool(row["spare_flag"]):
            continue
        scb_s = str(scb)
        inv_to_scbs[inv].add(scb_s)
        scb_to_inv[scb_s] = inv
    return {str(k): float(v) for k, v in inv_caps.items()}, dict(inv_to_scbs), scb_to_inv


def _load_equipment_frame(
    db: Session,
    plant_id: str,
    f_ts: str,
    t_ts: str,
    equipment_level: str,
    signal: str,
    id_label: str,
    value_label: str,
) -> pd.DataFrame:
    sql = text(
        f"""
        SELECT timestamp, equipment_id AS {id_label}, AVG(value) AS {value_label}
        FROM raw_data_generic
        WHERE plant_id = :p
          AND LOWER(TRIM(equipment_level::text)) = :lvl
          AND signal = :sig
          AND timestamp >= :f AND timestamp <= :t
        GROUP BY timestamp, equipment_id
        """
    )
    rows = db.execute(sql, {"p": plant_id, "lvl": equipment_level, "sig": signal, "f": f_ts, "t": t_ts}).fetchall()
    if not rows:
        return pd.DataFrame(columns=["timestamp", id_label, value_label])

    df = pd.DataFrame(rows, columns=["timestamp", id_label, value_label])
    df["timestamp"] = _bucket_to_minute(df["timestamp"])
    df[value_label] = pd.to_numeric(df[value_label], errors="coerce")
    df = df.dropna(subset=["timestamp", id_label]).copy()
    if df.empty:
        return pd.DataFrame(columns=["timestamp", id_label, value_label])

    return (
        df.groupby(["timestamp", id_label], as_index=False)[value_label]
        .mean()
        .sort_values(["timestamp", id_label])
        .reset_index(drop=True)
    )


def _count_windows(ts_list: List[pd.Timestamp], dt_h: float) -> int:
    if not ts_list:
        return 0
    window_count = 1
    gap_s = max(dt_h * 3600.0 * 1.5, 1.0)
    for prev, cur in zip(ts_list, ts_list[1:]):
        if (cur - prev).total_seconds() > gap_s:
            window_count += 1
    return window_count


def _safe_perf_factor(raw: Optional[float], fallback: float) -> float:
    val = float(raw if raw is not None else fallback)
    if not np.isfinite(val) or val < 0:
        val = fallback
    return max(0.0, min(val, COMM_PERF_FACTOR_MAX))


def _build_expected_power_maps(
    daylight: pd.DataFrame,
    ac_df: pd.DataFrame,
    inv_caps: Dict[str, float],
) -> Tuple[Dict[pd.Timestamp, float], Dict[Tuple[pd.Timestamp, str], float], float]:
    if daylight.empty:
        return {}, {}, COMM_PERF_FACTOR_DEFAULT

    daylight_irr = daylight.set_index("timestamp")["irradiance"].to_dict()
    ratio_samples: List[Tuple[pd.Timestamp, float]] = []
    if not ac_df.empty:
        work = ac_df.copy()
        work["inverter_id"] = work["inverter_id"].astype(str)
        work["cap_kw"] = work["inverter_id"].map(lambda inv: float(inv_caps.get(inv) or 0.0))
        work["irradiance"] = work["timestamp"].map(daylight_irr)
        work = work[(work["cap_kw"] > 0) & work["irradiance"].notna() & (work["irradiance"] > COMM_IRRADIANCE_MIN)].copy()
        if not work.empty:
            work["stc_kw"] = work["cap_kw"] * (work["irradiance"] / 1000.0)
            work["perf_factor"] = np.where(work["stc_kw"] > 0, work["ac_kw"] / work["stc_kw"], np.nan)
            work["perf_factor"] = work["perf_factor"].clip(lower=0.0, upper=COMM_PERF_FACTOR_MAX)
            ratio_samples = [
                (row["timestamp"], float(row["perf_factor"]))
                for _, row in work.dropna(subset=["perf_factor"]).iterrows()
            ]

    ratio_by_ts: Dict[pd.Timestamp, float] = {}
    if ratio_samples:
        ratio_df = pd.DataFrame(ratio_samples, columns=["timestamp", "perf_factor"])
        ratio_df = ratio_df.groupby("timestamp", as_index=False)["perf_factor"].median()
        ratio_by_ts = ratio_df.set_index("timestamp")["perf_factor"].to_dict()

    global_perf = COMM_PERF_FACTOR_DEFAULT
    if ratio_samples:
        raw_vals = [x[1] for x in ratio_samples if np.isfinite(x[1])]
        if raw_vals:
            global_perf = float(np.median(raw_vals))
    global_perf = _safe_perf_factor(global_perf, COMM_PERF_FACTOR_DEFAULT)

    expected_plant_kw: Dict[pd.Timestamp, float] = {}
    expected_inv_kw: Dict[Tuple[pd.Timestamp, str], float] = {}
    plant_dc = sum(float(v or 0.0) for v in inv_caps.values())
    for _, row in daylight.iterrows():
        ts = row["timestamp"]
        irr = float(row["irradiance"] or 0.0)
        perf = _safe_perf_factor(ratio_by_ts.get(ts), global_perf)
        stc_factor = irr / 1000.0
        expected_plant_kw[ts] = max(0.0, plant_dc * stc_factor * perf)
        for inv, cap in inv_caps.items():
            expected_inv_kw[(ts, str(inv))] = max(0.0, float(cap or 0.0) * stc_factor * perf)
    return expected_plant_kw, expected_inv_kw, global_perf


def _event_record(
    event_map: Dict[Tuple[str, str, str], dict],
    equipment_level: str,
    equipment_id: str,
    issue_kind: str,
    inverter_id: Optional[str],
    timestamp: pd.Timestamp,
    dt_h: float,
    loss_kwh: float,
) -> None:
    key = (equipment_level, equipment_id, issue_kind)
    rec = event_map.get(key)
    if rec is None:
        rec = {
            "equipment_level": equipment_level,
            "equipment_id": equipment_id,
            "inverter_id": inverter_id or "",
            "issue_kind": issue_kind,
            "status": ISSUE_STATUS.get(issue_kind, issue_kind.replace("_", " ").title()),
            "timestamps": [],
            "estimated_loss_kwh": 0.0,
            "loss_enabled": issue_kind in {ISSUE_PLANT, ISSUE_INV},
        }
        event_map[key] = rec
    rec["timestamps"].append(timestamp)
    rec["estimated_loss_kwh"] += float(loss_kwh or 0.0)


def _build_events(event_map: Dict[Tuple[str, str, str], dict], dt_h: float) -> List[dict]:
    events: List[dict] = []
    for rec in event_map.values():
        ts_list = sorted(set(rec["timestamps"]))
        if not ts_list:
            continue
        points = len(ts_list)
        events.append(
            {
                "equipment_level": rec["equipment_level"],
                "equipment_id": rec["equipment_id"],
                "inverter_id": rec["inverter_id"],
                "issue_kind": rec["issue_kind"],
                "status": rec["status"],
                "loss_enabled": bool(rec["loss_enabled"]),
                "communication_points": points,
                "communication_hours": round(points * dt_h, 3),
                "communication_windows": _count_windows(ts_list, dt_h),
                "estimated_loss_kwh": round(float(rec["estimated_loss_kwh"] or 0.0), 2),
                "last_seen_communication": str(max(ts_list)),
                "investigation_window_start": str(min(ts_list)),
                "investigation_window_end": str(max(ts_list)),
            }
        )
    events.sort(
        key=lambda r: (
            float(r.get("estimated_loss_kwh") or 0.0),
            float(r.get("communication_hours") or 0.0),
            str(r.get("equipment_level") or ""),
            str(r.get("equipment_id") or ""),
            str(r.get("issue_kind") or ""),
        ),
        reverse=True,
    )
    return events


def _build_comm_state(db: Session, plant_id: str, date_from: str, date_to: str) -> dict:
    d_from = _normalize_date_str(date_from)
    d_to = _normalize_date_str(date_to)
    f_ts = f"{d_from} 00:00:00"
    t_ts = f"{d_to} 23:59:59"

    irr = _load_irradiance_frame(db, plant_id, f_ts, t_ts)
    if irr.empty:
        return {
            "summary": _empty_summary(),
            "events": [],
            "loss_bars": [],
            "plant_issue_ts": set(),
            "inverter_issue_ts": {},
            "scb_issue_ts": {},
            "plant_power_by_ts": {},
            "inv_metric_by_ts": {},
            "scb_metric_by_ts": {},
            "daylight": pd.DataFrame(columns=["timestamp", "irradiance"]),
        }

    irr = irr.drop_duplicates("timestamp").sort_values("timestamp").reset_index(drop=True)
    dt_h = _median_step_hours(irr["timestamp"])
    daylight = irr[irr["irradiance"] > COMM_IRRADIANCE_MIN].copy()
    if daylight.empty:
        return {
            "summary": _empty_summary(),
            "events": [],
            "loss_bars": [],
            "plant_issue_ts": set(),
            "inverter_issue_ts": {},
            "scb_issue_ts": {},
            "plant_power_by_ts": {},
            "inv_metric_by_ts": {},
            "scb_metric_by_ts": {},
            "daylight": daylight,
        }

    inv_caps, inv_to_scbs, scb_to_inv = _load_architecture_meta(db, plant_id)
    ac_df = _load_equipment_frame(db, plant_id, f_ts, t_ts, "inverter", "ac_power", "inverter_id", "ac_kw")
    scb_df = _load_equipment_frame(db, plant_id, f_ts, t_ts, "scb", "dc_current", "scb_id", "dc_current_a")

    expected_inverters = sorted(set(inv_caps.keys()) or set(ac_df["inverter_id"].dropna().astype(str).tolist()))
    if not expected_inverters:
        return {
            "summary": _empty_summary(),
            "events": [],
            "loss_bars": [],
            "plant_issue_ts": set(),
            "inverter_issue_ts": {},
            "scb_issue_ts": {},
            "plant_power_by_ts": {},
            "inv_metric_by_ts": {},
            "scb_metric_by_ts": {},
            "daylight": daylight,
        }

    plant_power_by_ts: Dict[pd.Timestamp, float] = {}
    inv_metric_by_ts: Dict[Tuple[pd.Timestamp, str], float] = {}
    scb_metric_by_ts: Dict[Tuple[pd.Timestamp, str], float] = {}

    present_inverters_by_ts: Dict[pd.Timestamp, Set[str]] = defaultdict(set)
    present_scbs_by_ts: Dict[pd.Timestamp, Set[str]] = defaultdict(set)

    if not ac_df.empty:
        ac_df["inverter_id"] = ac_df["inverter_id"].astype(str)
        plant_power_by_ts = (
            ac_df.groupby("timestamp", as_index=False)["ac_kw"]
            .sum()
            .set_index("timestamp")["ac_kw"]
            .to_dict()
        )
        for _, row in ac_df.iterrows():
            ts = row["timestamp"]
            inv = str(row["inverter_id"])
            present_inverters_by_ts[ts].add(inv)
            inv_metric_by_ts[(ts, inv)] = float(row["ac_kw"])

    if not scb_df.empty:
        scb_df["scb_id"] = scb_df["scb_id"].astype(str)
        for _, row in scb_df.iterrows():
            ts = row["timestamp"]
            scb = str(row["scb_id"])
            present_scbs_by_ts[ts].add(scb)
            scb_metric_by_ts[(ts, scb)] = float(row["dc_current_a"])

    expected_plant_kw_by_ts, expected_inv_kw_by_ts, _ = _build_expected_power_maps(daylight, ac_df, inv_caps)
    event_map: Dict[Tuple[str, str, str], dict] = {}
    per_inverter_loss: Dict[str, float] = {inv: 0.0 for inv in expected_inverters}
    plant_issue_ts: Set[pd.Timestamp] = set()
    inverter_issue_ts: Dict[Tuple[str, str], Set[pd.Timestamp]] = defaultdict(set)
    scb_issue_ts: Dict[str, Set[pd.Timestamp]] = defaultdict(set)

    for ts in daylight["timestamp"].drop_duplicates().sort_values().tolist():
        present_invs = present_inverters_by_ts.get(ts, set())
        missing_invs = [inv for inv in expected_inverters if inv not in present_invs]
        if missing_invs and len(missing_invs) == len(expected_inverters):
            loss_kwh = float(expected_plant_kw_by_ts.get(ts) or 0.0) * dt_h
            _event_record(event_map, "plant", plant_id, ISSUE_PLANT, None, ts, dt_h, loss_kwh)
            plant_issue_ts.add(ts)
            for inv in expected_inverters:
                inv_loss = float(expected_inv_kw_by_ts.get((ts, inv)) or 0.0) * dt_h
                per_inverter_loss[inv] = round(per_inverter_loss.get(inv, 0.0) + inv_loss, 6)
            continue

        present_scbs = present_scbs_by_ts.get(ts, set())
        for inv in expected_inverters:
            scbs = inv_to_scbs.get(inv, set())
            if inv not in present_invs:
                loss_kwh = float(expected_inv_kw_by_ts.get((ts, inv)) or 0.0) * dt_h
                _event_record(event_map, "inverter", inv, ISSUE_INV, inv, ts, dt_h, loss_kwh)
                inverter_issue_ts[(inv, ISSUE_INV)].add(ts)
                per_inverter_loss[inv] = round(per_inverter_loss.get(inv, 0.0) + loss_kwh, 6)
                continue

            if scbs:
                missing_scbs = sorted(scb for scb in scbs if scb not in present_scbs)
                if missing_scbs and len(missing_scbs) == len(scbs):
                    _event_record(event_map, "inverter", inv, ISSUE_INV_ALL_SCB, inv, ts, dt_h, 0.0)
                    inverter_issue_ts[(inv, ISSUE_INV_ALL_SCB)].add(ts)
                    continue
                for scb in missing_scbs:
                    _event_record(event_map, "scb", scb, ISSUE_SCB, inv, ts, dt_h, 0.0)
                    scb_issue_ts[scb].add(ts)

    events = _build_events(event_map, dt_h)
    loss_bars = [
        {"inverter_id": inv, "estimated_loss_kwh": round(float(loss or 0.0), 2)}
        for inv, loss in per_inverter_loss.items()
        if float(loss or 0.0) > 0
    ]
    loss_bars.sort(key=lambda r: (float(r["estimated_loss_kwh"]), str(r["inverter_id"])), reverse=True)

    plant_rows = [r for r in events if r.get("equipment_level") == "plant"]
    inverter_rows = [r for r in events if r.get("equipment_level") == "inverter"]
    scb_rows = [r for r in events if r.get("equipment_level") == "scb"]
    summary = {
        "total_communication_issues": len(events),
        "total_loss_kwh": round(sum(float(r.get("estimated_loss_kwh") or 0.0) for r in events), 2),
        "total_communication_hours": round(sum(float(r.get("communication_hours") or 0.0) for r in events), 3),
        "plant_issue_count": len(plant_rows),
        "inverter_issue_count": len(inverter_rows),
        "scb_issue_count": len(scb_rows),
        "ingestion_gap_issue_count": len([r for r in events if r.get("issue_kind") in {ISSUE_INV_ALL_SCB, ISSUE_SCB}]),
        "irradiance_threshold_w_m2": COMM_IRRADIANCE_MIN,
    }
    return {
        "summary": summary,
        "events": events,
        "loss_bars": loss_bars,
        "plant_issue_ts": plant_issue_ts,
        "inverter_issue_ts": inverter_issue_ts,
        "scb_issue_ts": scb_issue_ts,
        "plant_power_by_ts": plant_power_by_ts,
        "inv_metric_by_ts": inv_metric_by_ts,
        "scb_metric_by_ts": scb_metric_by_ts,
        "daylight": daylight,
    }


def run_communication_issue(
    db: Session,
    plant_id: str,
    date_from: str,
    date_to: str,
) -> Tuple[dict, List[dict], List[dict]]:
    state = _build_comm_state(db, plant_id, date_from, date_to)
    return state["summary"], state["events"], state["loss_bars"]


def get_communication_timeline(
    db: Session,
    plant_id: str,
    equipment_level: str,
    equipment_id: str,
    date_from: str,
    date_to: str,
    issue_kind: Optional[str] = None,
) -> List[dict]:
    state = _build_comm_state(db, plant_id, date_from, date_to)
    daylight = state["daylight"]
    if daylight is None or getattr(daylight, "empty", True):
        return []

    eq_level = str(equipment_level or "").strip().lower()
    eq_id = str(equipment_id or "").strip()
    issue_k = str(issue_kind or "").strip().lower() or None

    if eq_level == "plant":
        metric_label = "active_power_kw"
        metric_map = state["plant_power_by_ts"]
        issue_ts = state["plant_issue_ts"]
    elif eq_level == "inverter":
        metric_label = "active_power_kw"
        metric_map = {
            ts: val
            for (ts, inv), val in state["inv_metric_by_ts"].items()
            if inv == eq_id
        }
        if issue_k:
            issue_ts = state["inverter_issue_ts"].get((eq_id, issue_k), set())
        else:
            issue_ts = {
                ts
                for (inv, kind), vals in state["inverter_issue_ts"].items()
                if inv == eq_id
                for ts in vals
            }
    elif eq_level == "scb":
        metric_label = "dc_current_a"
        metric_map = {
            ts: val
            for (ts, scb), val in state["scb_metric_by_ts"].items()
            if scb == eq_id
        }
        issue_ts = state["scb_issue_ts"].get(eq_id, set())
    else:
        return []

    out: List[dict] = []
    for _, row in daylight.sort_values("timestamp").iterrows():
        ts = row["timestamp"]
        metric_value = metric_map.get(ts)
        out.append(
            {
                "timestamp": str(ts),
                "irradiance": round(float(row["irradiance"]), 3) if row["irradiance"] is not None else None,
                metric_label: round(float(metric_value), 3) if metric_value is not None and np.isfinite(metric_value) else None,
                "communication_issue": bool(ts in issue_ts),
                "equipment_level": eq_level,
                "equipment_id": eq_id,
                "issue_kind": issue_k,
            }
        )
    return out
