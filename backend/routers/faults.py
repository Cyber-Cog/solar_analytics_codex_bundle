import os
from datetime import date as _date

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy import func
from typing import List, Optional, Tuple
from auth.routes import get_current_user
from database import get_db, get_read_db
from db_perf import choose_data_table
from soiling_queries import (
    build_plant_soiling_payload,
    build_scb_soiling_trend_payload,
    build_soiling_rankings_payload,
    fetch_hourly_plant_irradiance_w_m2,
    scb_dc_map,
)
from models import User, FaultDiagnostics, FaultEpisode, EquipmentSpec, ScbFaultReview
from fault_cache import get_cached
from dashboard_cache import get as _dc_get, set as _dc_set
from fault_runtime_snapshot import (
    KIND_PL_PAGE,
    KIND_IS_TAB,
    KIND_GB_TAB,
    try_snapshot_payload,
    save_snapshot_payload,
)
from engine.communication_issue import get_communication_timeline, run_communication_issue

router = APIRouter(prefix="/api/faults", tags=["Faults"])

_MEM_PL_PAGE = "faults_pl_page_v2"
_MEM_IS_TAB = "faults_is_tab_v2"
_MEM_GB_TAB = "faults_gb_tab_v2"
_MEM_COMM_TAB = "faults_comm_tab_v2"
_MEM_INV_EFF_AGG = "faults_inv_eff_agg_v1"
_MEM_CD_TAB = "faults_cd_tab_v3_fast"
_MEM_RUNTIME_TABS_BUNDLE = "faults_runtime_tabs_bundle_v7"


def _compute_inv_eff_aggregate(db: Session, plant_id: str, _from: str, _to: str) -> Tuple[float, int]:
    """Lightweight SQL-side aggregation of inverter DC→AC conversion loss for unified-feed tiles.

    Mirrors the core formula of `get_inverter_efficiency_analysis` (Σ (dc_kw − ac_kw) × dt_h)
    but in a single window-function query so the overview tile matches the tab value without
    re-reading ~1M rows into Python. Returns (total_loss_mwh, inverters_with_active_loss).
    """
    sql = text(
        """
        WITH paired AS (
          SELECT timestamp, equipment_id,
                 MAX(CASE WHEN signal='dc_power' THEN value END) AS dc_kw,
                 MAX(CASE WHEN signal='ac_power' THEN value END) AS ac_kw
            FROM raw_data_generic
           WHERE plant_id = :p
             AND equipment_level = 'inverter'
             AND timestamp BETWEEN :f AND :t
           GROUP BY timestamp, equipment_id
        ),
        stepped AS (
          SELECT equipment_id,
                 dc_kw, ac_kw,
                 (timestamp::timestamp - LAG(timestamp::timestamp) OVER (
                     PARTITION BY equipment_id ORDER BY timestamp
                 )) AS step
            FROM paired
           WHERE dc_kw IS NOT NULL AND ac_kw IS NOT NULL
        )
        SELECT equipment_id,
               SUM(
                 CASE WHEN dc_kw > ac_kw THEN
                   (dc_kw - ac_kw) *
                   CASE WHEN step IS NULL THEN (1.0/60.0)
                        ELSE LEAST(GREATEST(EXTRACT(EPOCH FROM step)/3600.0, 1.0/3600.0), 1.0)
                   END
                 ELSE 0 END
               ) AS loss_kwh
          FROM stepped
         GROUP BY equipment_id
        """
    )
    try:
        rows = db.execute(
            sql,
            {"p": plant_id, "f": f"{_from} 00:00:00", "t": f"{_to} 23:59:59"},
        ).fetchall()
    except Exception:
        return 0.0, 0
    total_mwh = sum(float(r.loss_kwh or 0) for r in rows) / 1000.0
    active = sum(1 for r in rows if (r.loss_kwh or 0) > 1.0)
    return round(total_mwh, 4), active


def _inv_eff_aggregate_with_cache(db: Session, plant_id: str, _from: str, _to: str) -> Tuple[float, int]:
    hit = _dc_get(_MEM_INV_EFF_AGG, plant_id, _from, _to)
    if hit is not None:
        try:
            return float(hit.get("loss_mwh") or 0), int(hit.get("active") or 0)
        except Exception:
            pass
    total_mwh, active = _compute_inv_eff_aggregate(db, plant_id, _from, _to)
    _dc_set(_MEM_INV_EFF_AGG, plant_id, _from, _to, {"loss_mwh": total_mwh, "active": active})
    return total_mwh, active


def _fault_date_range(date_from: Optional[str], date_to: Optional[str]) -> Tuple[str, str]:
    from datetime import date, timedelta

    today = date.today()
    _from = date_from or str(today - timedelta(days=7))
    _to = date_to or str(today)
    return _from, _to


def _build_pl_page_payload(inv_status: list) -> dict:
    active = [s for s in inv_status if (s.get("total_energy_loss_kwh") or 0) > 0]
    total_loss = sum(s.get("total_energy_loss_kwh") or 0 for s in active)
    summary = {
        "active_pl_inverters": len(active),
        "total_energy_loss_kwh": round(total_loss, 2),
        "inverters": [{"inverter_id": s["inverter_id"], "energy_loss_kwh": s["total_energy_loss_kwh"]} for s in active],
    }
    data = [s for s in inv_status if (s.get("total_energy_loss_kwh") or 0) > 0]
    return {"summary": summary, "inverter_status": {"data": data}}


def _compute_pl_page(db: Session, plant_id: str, _from: str, _to: str) -> dict:
    from engine.power_limitation import run_power_limitation

    inv_status, _ = run_power_limitation(db, plant_id, _from, _to)
    return _build_pl_page_payload(inv_status)


def _pl_page_with_cache(db: Session, plant_id: str, _from: str, _to: str) -> dict:
    hit = _dc_get(_MEM_PL_PAGE, plant_id, _from, _to)
    if hit is not None:
        return hit
    snap = try_snapshot_payload(db, plant_id, _from, _to, KIND_PL_PAGE)
    if snap is not None:
        _dc_set(_MEM_PL_PAGE, plant_id, _from, _to, snap)
        return snap
    out = _compute_pl_page(db, plant_id, _from, _to)
    _dc_set(_MEM_PL_PAGE, plant_id, _from, _to, out)
    save_snapshot_payload(db, plant_id, _from, _to, KIND_PL_PAGE, out)
    return out


def _compute_is_tab(db: Session, plant_id: str, _from: str, _to: str) -> dict:
    from engine.inverter_shutdown import run_inverter_shutdown

    inv_status, _ = run_inverter_shutdown(db, plant_id, _from, _to)
    active = [s for s in inv_status if (s.get("shutdown_points") or 0) > 0]
    total_hours = sum(float(s.get("shutdown_hours") or 0) for s in active)
    summary = {
        "active_shutdown_inverters": len(active),
        "total_shutdown_hours": round(total_hours, 3),
        "inverters": [
            {
                "inverter_id": s["inverter_id"],
                "shutdown_points": s.get("shutdown_points", 0),
                "shutdown_hours": s.get("shutdown_hours", 0),
            }
            for s in active
        ],
    }
    data = [s for s in inv_status if (s.get("shutdown_points") or 0) > 0]
    return {"summary": summary, "inverter_status": {"data": data}}


def _is_tab_with_cache(db: Session, plant_id: str, _from: str, _to: str) -> dict:
    hit = _dc_get(_MEM_IS_TAB, plant_id, _from, _to)
    if hit is not None:
        return hit
    snap = try_snapshot_payload(db, plant_id, _from, _to, KIND_IS_TAB)
    if snap is not None:
        _dc_set(_MEM_IS_TAB, plant_id, _from, _to, snap)
        return snap
    out = _compute_is_tab(db, plant_id, _from, _to)
    _dc_set(_MEM_IS_TAB, plant_id, _from, _to, out)
    save_snapshot_payload(db, plant_id, _from, _to, KIND_IS_TAB, out)
    return out


def _compute_gb_tab(db: Session, plant_id: str, _from: str, _to: str) -> dict:
    from engine.grid_breakdown import run_grid_breakdown

    events, _ = run_grid_breakdown(db, plant_id, _from, _to)
    active = [e for e in events if (e.get("breakdown_points") or 0) > 0]
    total_hours = sum(float(e.get("breakdown_hours") or 0) for e in active)
    summary = {
        "active_grid_events": len(active),
        "total_grid_breakdown_hours": round(total_hours, 3),
        "events": [
            {
                "event_id": e["event_id"],
                "breakdown_points": e.get("breakdown_points", 0),
                "breakdown_hours": e.get("breakdown_hours", 0),
            }
            for e in active
        ],
    }
    data = [e for e in events if (e.get("breakdown_points") or 0) > 0]
    return {"summary": summary, "events": {"data": data}}


def _gb_tab_with_cache(db: Session, plant_id: str, _from: str, _to: str) -> dict:
    hit = _dc_get(_MEM_GB_TAB, plant_id, _from, _to)
    if hit is not None:
        return hit
    snap = try_snapshot_payload(db, plant_id, _from, _to, KIND_GB_TAB)
    if snap is not None:
        _dc_set(_MEM_GB_TAB, plant_id, _from, _to, snap)
        return snap
    out = _compute_gb_tab(db, plant_id, _from, _to)
    _dc_set(_MEM_GB_TAB, plant_id, _from, _to, out)
    save_snapshot_payload(db, plant_id, _from, _to, KIND_GB_TAB, out)
    return out


def _compute_comm_tab(db: Session, plant_id: str, _from: str, _to: str) -> dict:
    summary, events, inverter_loss = run_communication_issue(db, plant_id, _from, _to)
    return {
        "summary": summary,
        "events": {"data": events},
        "inverter_loss": {"data": inverter_loss},
    }


def _comm_tab_with_cache(db: Session, plant_id: str, _from: str, _to: str) -> dict:
    hit = _dc_get(_MEM_COMM_TAB, plant_id, _from, _to)
    if hit is not None:
        return hit
    out = _compute_comm_tab(db, plant_id, _from, _to)
    _dc_set(_MEM_COMM_TAB, plant_id, _from, _to, out)
    return out


# ── Clipping & Derating tab compute/cache ─────────────────────────────────────
def _compute_cd_tab(db: Session, plant_id: str, _from: str, _to: str) -> dict:
    """Run GTI-based clipping/derating detection and assemble the tab payload.

    The per-inverter timelines are stored inside the cached payload
    (`_timelines_by_inv`). The /cd-timeline endpoint reads from this dict so
    the Investigate modal opens instantly without re-running the engine.
    """
    from engine.clipping_derating import (
        run_clipping_derating,
        summarise_clipping_derating,
    )

    inv_status, timelines_by_inv, eng_meta = run_clipping_derating(db, plant_id, _from, _to)
    summary = summarise_clipping_derating(inv_status, eng_meta)
    return {
        "summary": summary,
        "inverter_status": inv_status,
        "engine_meta": eng_meta,
        "_timelines_by_inv": timelines_by_inv,
        "_has_timeline": bool(timelines_by_inv),
    }


def _cd_tab_with_cache(db: Session, plant_id: str, _from: str, _to: str) -> dict:
    hit = _dc_get(_MEM_CD_TAB, plant_id, _from, _to)
    if hit is not None:
        return hit
    out = _compute_cd_tab(db, plant_id, _from, _to)
    _dc_set(_MEM_CD_TAB, plant_id, _from, _to, out)
    return out


def _fault_latest_ts(session: Session, plant_id: str, date_from: str = None, date_to: str = None):
    q = session.query(func.max(FaultDiagnostics.timestamp)).filter(FaultDiagnostics.plant_id == plant_id)
    if date_from:
        q = q.filter(FaultDiagnostics.timestamp >= f"{date_from} 00:00:00")
    if date_to:
        q = q.filter(FaultDiagnostics.timestamp <= f"{date_to} 23:59:59")
    return q.scalar()


def _fault_timeline_query(session: Session, plant_id: str, scb_id: Optional[str], date_from: Optional[str], date_to: Optional[str]):
    query = session.query(FaultDiagnostics).filter(FaultDiagnostics.plant_id == plant_id)
    if scb_id:
        query = query.filter(FaultDiagnostics.scb_id == scb_id)
    if date_from:
        query = query.filter(FaultDiagnostics.timestamp >= f"{date_from} 00:00:00")
    if date_to:
        query = query.filter(FaultDiagnostics.timestamp <= f"{date_to} 23:59:59")
    return query


def _get_arch_spare_and_total(db: Session, plant_id: str):
    """Return (spare_scbs set, total_non_spare count). Uses main DB. Tolerates missing spare_flag column."""
    try:
        rows = db.execute(
            text(
                "SELECT DISTINCT scb_id, COALESCE(spare_flag, false) FROM plant_architecture "
                "WHERE plant_id = :p AND scb_id IS NOT NULL"
            ),
            {"p": plant_id},
        ).fetchall()
        spare = {r[0] for r in rows if r[1] is True}
        total_non_spare = len([r for r in rows if r[1] is not True])
        return spare, total_non_spare
    except Exception:
        rows = db.execute(
            text("SELECT DISTINCT scb_id FROM plant_architecture WHERE plant_id = :p AND scb_id IS NOT NULL"),
            {"p": plant_id},
        ).fetchall()
        return set(), len(rows)


def _min_disconnected_strings(session: Session, plant_id: str, date_from: Optional[str], date_to: Optional[str]) -> dict:
    """
    Return {scb_id: min_missing_strings} for CONFIRMED_DS rows in the range.

    Why no 'missing_strings > 0' filter:
      CONFIRMED_DS rows can legitimately have missing_strings = 0 during recovery
      grace-period windows.  If the minimum over all confirmed-fault timestamps is 0
      it means the fault was intermittent / self-resolved, so we keep MIN = 0 so the
      heatmap shows green (no persistent fault) — consistent with what the chart shows.

    Only SCBs where the MIN is actually > 0 are returned (HAVING clause), so callers
    get a non-empty entry only when a genuine persistent disconnection was detected.
    """
    f_ts = f"{date_from} 00:00:00" if date_from else "1970-01-01 00:00:00"
    t_ts = f"{date_to} 23:59:59" if date_to else "2099-12-31 23:59:59"
    sql = text("""
        SELECT scb_id, MIN(COALESCE(missing_strings, 0)) AS min_ms
        FROM fault_diagnostics
        WHERE plant_id = :p
          AND fault_status = 'CONFIRMED_DS'
          AND timestamp >= :f
          AND timestamp <= :t
        GROUP BY scb_id
        HAVING MIN(COALESCE(missing_strings, 0)) > 0
    """)
    try:
        rows = session.execute(sql, {"p": plant_id, "f": f_ts, "t": t_ts}).fetchall()
        return {r[0]: int(r[1]) for r in rows if r[1] is not None}
    except Exception:
        return {}


def _range_min_disconnected_strings(session: Session, plant_id: str, date_from: Optional[str], date_to: Optional[str]) -> dict:
    """
    Return {scb_id: min_missing_strings} across ALL timestamps in the selected range.

    This is used by the heatmap because the user wants the displayed disconnected
    string count to be the minimum over the full selected window. If any timestamp
    in the range has missing_strings = 0, the heatmap must show 0 for that SCB.

    Cast to integer so MIN is numeric; if the column is TEXT, MIN would otherwise
    be lexicographic (e.g. "1216" < "22") and the table would not match the chart.
    Cap each row at 2000 so a single bad row (e.g. from negative current) does not
    become the displayed minimum; the real minimum (e.g. 22) is then used.
    """
    f_ts = f"{date_from} 00:00:00" if date_from else "1970-01-01 00:00:00"
    t_ts = f"{date_to} 23:59:59" if date_to else "2099-12-31 23:59:59"
    sql = text("""
        SELECT scb_id, MIN(CASE
            WHEN CAST(COALESCE(missing_strings, 0) AS INTEGER) > 2000 THEN 2000
            ELSE CAST(COALESCE(missing_strings, 0) AS INTEGER)
        END) AS min_ms
        FROM fault_diagnostics
        WHERE plant_id = :p
          AND timestamp >= :f
          AND timestamp <= :t
        GROUP BY scb_id
    """)
    try:
        rows = session.execute(sql, {"p": plant_id, "f": f_ts, "t": t_ts}).fetchall()
        return {r[0]: int(r[1]) for r in rows if r[1] is not None}
    except Exception:
        return {}


def _voltage_meta(session: Session, plant_id: str, date_from: Optional[str] = None, date_to: Optional[str] = None, scb_id: Optional[str] = None):
    sql = (
        "SELECT COUNT(*) "
        "FROM raw_data_generic "
        "WHERE plant_id = :p "
        "  AND signal = 'dc_voltage' "
        "  AND equipment_level IN ('scb', 'inverter') "
    )
    params = {"p": plant_id}
    if scb_id:
        # For a specific SCB: check SCB voltage first, then its parent inverter voltage.
        parent_inv = session.execute(
            text(
                "SELECT inverter_id FROM plant_architecture "
                "WHERE plant_id = :p AND scb_id = :s "
                "LIMIT 1"
            ),
            {"p": plant_id, "s": scb_id},
        ).scalar()
        if parent_inv:
            sql += "AND (equipment_id = :scb_id OR equipment_id = :inv_id) "
            params["scb_id"] = scb_id
            params["inv_id"] = parent_inv
        else:
            sql += "AND equipment_id = :scb_id "
            params["scb_id"] = scb_id
    # Always bound the count so it never scans full history. Caller date
    # filters are respected; if absent, default to the last 90 days which is
    # enough to decide whether DC voltage is being ingested right now.
    if date_from:
        sql += "AND timestamp >= :f "
        params["f"] = f"{date_from} 00:00:00"
    else:
        from datetime import date as _date, timedelta as _timedelta
        sql += "AND timestamp >= :f "
        params["f"] = (_date.today() - _timedelta(days=90)).isoformat() + " 00:00:00"
    if date_to:
        sql += "AND timestamp <= :t "
        params["t"] = f"{date_to} 23:59:59"
    has_voltage = (session.execute(text(sql), params).scalar() or 0) > 0
    note = None if has_voltage else "Energy cannot be calculated because DC voltage data is unavailable for the selected period."
    return has_voltage, note


def _episode_map_for_days(session: Session, plant_id: str, scb_day_map: dict) -> dict:
    """
    Return {(scb_id, day): FaultEpisode} including both open and closed episodes.
    """
    if not scb_day_map:
        return {}
    scb_ids = sorted({s for s in scb_day_map.keys() if s})
    all_days = [d for d in scb_day_map.values() if d]
    if not scb_ids or not all_days:
        return {}
    min_day = min(all_days)
    max_day = max(all_days)
    rows = session.query(FaultEpisode).filter(
        FaultEpisode.plant_id == plant_id,
        FaultEpisode.fault_type == "DS",
        FaultEpisode.scb_id.in_(scb_ids),
        FaultEpisode.start_date <= max_day,
        FaultEpisode.last_seen_date >= min_day,
    ).all()
    out = {}
    for ep in rows:
        day = scb_day_map.get(ep.scb_id)
        if not day:
            continue
        if ep.start_date <= day <= ep.last_seen_date:
            out[(ep.scb_id, day)] = ep
    return out


def _fallback_recurrence_map(session: Session, plant_id: str, scb_ids: list[str]) -> dict:
    """
    Fallback recurrence derived directly from fault_diagnostics:
    {scb_id: {"active_since": YYYY-MM-DD, "recurring_days": N}}
    """
    ids = [s for s in scb_ids if s]
    if not ids:
        return {}
    rows = session.execute(
        text(
            """
            SELECT scb_id,
                   MIN(SUBSTR(timestamp, 1, 10)) AS first_day,
                   COUNT(DISTINCT SUBSTR(timestamp, 1, 10)) AS active_days
            FROM fault_diagnostics
            WHERE plant_id = :p
              AND scb_id = ANY(:ids)
              AND fault_status = 'CONFIRMED_DS'
              AND COALESCE(missing_strings, 0) > 0
            GROUP BY scb_id
            """
        ),
        {"p": plant_id, "ids": ids},
    ).fetchall()
    return {
        str(r[0]): {"active_since": str(r[1])[:10], "recurring_days": int(r[2] or 0)}
        for r in rows if r[0] and r[1]
    }

@router.get("/ds-summary")
def get_ds_summary(
    plant_id: str = Query(...),
    date_from: str = Query(default=None),
    date_to: str = Query(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Returns high-level insights for the Dashboard:
    - active_ds_faults (Current number of CONFIRMED_DS scbs in latest timestamp)
    - total_disconnected_strings (Sum of missing strings for those faults)
    - daily_energy_loss_kwh (Total energy loss for the current day/latest available day)
    - top_affected_scbs (List of SCBs with highest energy loss)
    Cached for faster repeat fetches; cache invalidated when new fault data is saved.
    """
    spare_scbs, total_scbs = _get_arch_spare_and_total(db, plant_id)
    latest_ts = _fault_latest_ts(db, plant_id, date_from, date_to)

    if not latest_ts:
        return {
            "active_ds_faults": 0,
            "total_disconnected_strings": 0,
            "daily_energy_loss_kwh": 0,
            "top_affected_scbs": [],
            "latest_date": date_to or "No Data",
            "total_scbs": total_scbs,
            "communicating_scbs": 0,
        }

    latest_date = latest_ts.split(" ")[0]
    f_ts = f"{date_from} 00:00:00" if date_from else None
    t_ts = f"{date_to} 23:59:59" if date_to else None
    communicating_scbs = 0
    if f_ts and t_ts:
        comm_q = db.query(FaultDiagnostics.scb_id).filter(
                FaultDiagnostics.plant_id == plant_id,
            FaultDiagnostics.timestamp >= f_ts,
            FaultDiagnostics.timestamp <= t_ts,
        ).distinct()
        comm_ids = {r[0] for r in comm_q.all()}
        communicating_scbs = len(comm_ids - spare_scbs)

    # Min(missing_strings) over range; exclude filter-summary SCBs so card matches table.
    range_min_map = _range_min_disconnected_strings(db, plant_id, date_from, date_to)
    excluded_by_filter = set()
    if date_from and date_to:
        try:
            from datetime import date as _date, timedelta
            start = _date.fromisoformat(date_from)
            end = _date.fromisoformat(date_to)
            d = start
            while d <= end:
                key = f"filter_summary:{plant_id}:{d.isoformat()}"
                day_data = get_cached(db, key, 876000)
                if day_data and isinstance(day_data, dict):
                    excluded_by_filter.update(day_data.get("outlier", []))
                    excluded_by_filter.update(day_data.get("constant", []))
                    excluded_by_filter.update(day_data.get("leakage", []))
                d += timedelta(days=1)
        except Exception:
            pass
    active_scbs = [
        scb_id for scb_id in range_min_map
        if range_min_map[scb_id] > 0 and scb_id not in spare_scbs and scb_id not in excluded_by_filter
    ]
    active_count = len(active_scbs)
    total_strings = sum(range_min_map.get(scb_id, 0) for scb_id in active_scbs)

    energy_available, energy_note = _voltage_meta(db, plant_id, date_from, date_to)
    daily_loss = None
    top_scbs = []
    daily_loss_mwh = None
    if energy_available:
        daily_loss = db.query(func.sum(FaultDiagnostics.energy_loss_kwh)).filter(
            FaultDiagnostics.plant_id == plant_id,
            FaultDiagnostics.timestamp.like(f"{latest_date}%")
        ).scalar() or 0.0
        top_scbs_query = db.query(
            FaultDiagnostics.scb_id,
            func.sum(FaultDiagnostics.energy_loss_kwh).label("total_loss")
        ).filter(
            FaultDiagnostics.plant_id == plant_id,
            FaultDiagnostics.timestamp.like(f"{latest_date}%")
        ).group_by(FaultDiagnostics.scb_id).order_by(func.sum(FaultDiagnostics.energy_loss_kwh).desc()).limit(5).all()
        top_scbs = [{"scb_id": r[0], "loss_kwh": round(r[1], 2), "loss_mwh": round((r[1] or 0) / 1000, 3)} for r in top_scbs_query if r[1] > 0 and r[0] not in spare_scbs]
        daily_loss_mwh = round(daily_loss / 1000, 3) if daily_loss else 0

    f_ts = f"{date_from} 00:00:00" if date_from else None
    t_ts = f"{date_to} 23:59:59" if date_to else None
    es_where = "WHERE plant_id = :p"
    es_params: dict = {"p": plant_id}
    if f_ts:
        es_where += " AND timestamp >= :f"
        es_params["f"] = f_ts
    if t_ts:
        es_where += " AND timestamp <= :t"
        es_params["t"] = t_ts
    daily_energy_series = []
    if energy_available:
        energy_sql = text(f"""
            SELECT SUBSTR(timestamp, 1, 10) AS date, SUM(energy_loss_kwh) AS loss
            FROM fault_diagnostics
            {es_where}
            GROUP BY SUBSTR(timestamp, 1, 10)
            ORDER BY date
        """)
        energy_rows = db.execute(energy_sql, es_params).fetchall()
        daily_energy_series = [
            {"date": r[0], "energy_loss_kwh": round(r[1], 2) if r[1] else 0}
            for r in energy_rows if r[1] and r[1] > 0
        ]

    return {
        "active_ds_faults": active_count,
        "total_disconnected_strings": total_strings,
        "daily_energy_loss_kwh": round(daily_loss, 2) if daily_loss is not None else None,
        "daily_energy_loss_mwh": daily_loss_mwh,
        "top_affected_scbs": top_scbs,
        "latest_date": latest_date,
        "daily_energy_series": daily_energy_series,
        "energy_available": energy_available,
        "energy_note": energy_note,
        "total_scbs": total_scbs,
        "communicating_scbs": communicating_scbs,
    }

def _serialize_fault_row(r):
    expected = r.expected_current or 0
    actual   = r.virtual_string_current or 0
    # Deviation % = (actual_per_string - reference_per_string) / reference_per_string × 100
    # Both columns are stored as per-string (A), so N_strings cancels out.
    deviation_pct = round((actual - expected) / expected * 100, 2) if expected else None
    return {
        "timestamp": r.timestamp,
        "inverter_id": r.inverter_id,
        "scb_id": r.scb_id,
        "expected_current": round(expected, 2),
        "virtual_string_current": round(actual, 2),
        "missing_strings": r.missing_strings or 0,
        "power_loss_kw": round(r.power_loss_kw, 2) if r.power_loss_kw else 0,
        "energy_loss_kwh": round(r.energy_loss_kwh, 2) if r.energy_loss_kwh else 0,
        "fault_status": r.fault_status,
        "missing_current": round(r.missing_current, 2) if r.missing_current else 0,
        "deviation_pct": deviation_pct,
        "active_since": None,
        "recurring_days": 0,
        "episode_id": None,
    }


@router.get("/ds-scb-status")
def get_ds_scb_status(
    plant_id: str = Query(...),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Returns the latest fault state per SCB for the given date range.
    Used by the main Fault Diagnostics page for the active-faults table,
    heatmap, and bar chart. Reads directly from fault_diagnostics.
    """
    from sqlalchemy import text as _text

    spare_scbs, _ = _get_arch_spare_and_total(db, plant_id)

    f_ts = f"{date_from} 00:00:00" if date_from else None
    t_ts = f"{date_to} 23:59:59" if date_to else None
    where = "WHERE f.plant_id = :p"
    params: dict = {"p": plant_id}
    if f_ts:
        where += " AND f.timestamp >= :f"
        params["f"] = f_ts
    if t_ts:
        where += " AND f.timestamp <= :t"
        params["t"] = t_ts

    sql = _text(f"""
        SELECT f.* FROM fault_diagnostics f
        INNER JOIN (
            SELECT scb_id, MAX(timestamp) AS max_ts
            FROM fault_diagnostics
            {where.replace('f.', '')}
            GROUP BY scb_id
        ) g ON f.scb_id = g.scb_id AND f.timestamp = g.max_ts
        {where}
        ORDER BY f.scb_id
    """)
    rows = db.execute(sql, params).fetchall()
    cols = [c.key for c in FaultDiagnostics.__table__.columns]

    range_min_map = _range_min_disconnected_strings(db, plant_id, date_from, date_to)
    scb_day_map = {}
    idx_scb = cols.index("scb_id")
    idx_ts = cols.index("timestamp")
    for r in rows:
        scb = str(r[idx_scb]) if r[idx_scb] else ""
        day = str(r[idx_ts])[:10] if r[idx_ts] else ""
        if scb and day:
            scb_day_map[scb] = day
    episode_map = _episode_map_for_days(db, plant_id, scb_day_map)
    if rows and len(episode_map) < len(scb_day_map):
        try:
            from engine.fault_episodes import rebuild_fault_episodes_for_scbs
            rebuild_fault_episodes_for_scbs(db, plant_id, set(scb_day_map.keys()))
            episode_map = _episode_map_for_days(db, plant_id, scb_day_map)
        except Exception:
            pass
    fallback_map = _fallback_recurrence_map(db, plant_id, list(scb_day_map.keys()))
    data = []
    for r in rows:
        obj = type("R", (), dict(zip(cols, r)))()
        row_dict = _serialize_fault_row(obj)
        # Show all SCBs with fault rows; do not require plant_architecture to list them
        # (incomplete uploads would otherwise hide real diagnostics).
        if row_dict["scb_id"] in spare_scbs:
            continue
        scb = row_dict["scb_id"]
        final_min = range_min_map.get(scb, 0)
        row_dict["missing_strings"] = final_min
        row_dict["range_min_missing_strings"] = final_min
        if final_min > 0:
            row_day = str(row_dict["timestamp"])[:10]
            ep = episode_map.get((scb, row_day))
            if ep and ep.start_date:
                try:
                    days = (_date.fromisoformat(row_day) - _date.fromisoformat(ep.start_date)).days + 1
                    row_dict["active_since"] = ep.start_date
                    row_dict["recurring_days"] = max(1, days)
                    row_dict["episode_id"] = ep.episode_id
                except Exception:
                    row_dict["active_since"] = ep.start_date
                    row_dict["recurring_days"] = max(1, int(ep.days_active or 1))
                    row_dict["episode_id"] = ep.episode_id
            elif scb in fallback_map:
                row_dict["active_since"] = fallback_map[scb]["active_since"]
                row_dict["recurring_days"] = max(1, int(fallback_map[scb]["recurring_days"]))
        data.append(row_dict)

    energy_available, energy_note = _voltage_meta(db, plant_id, date_from, date_to)
    return {"data": data, "energy_available": energy_available, "energy_note": energy_note}


@router.get("/ds-timeline")
def get_ds_timeline(
    plant_id: str = Query(...),
    scb_id: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Returns chronological time-series for a specific SCB (for the investigate
    modal). When called without scb_id, falls back to ds-scb-status style.
    Reads directly from fault_diagnostics.
    Augments each row with plant-level irradiance (null if unavailable).
    """
    if not scb_id:
        return get_ds_scb_status(plant_id=plant_id, date_from=date_from,
                               date_to=date_to, db=db, current_user=current_user)

    spare_scbs, _ = _get_arch_spare_and_total(db, plant_id)
    if scb_id in spare_scbs:
        return {
            "data": [],
            "energy_available": False,
            "irradiance_available": False,
            "energy_note": f"SCB '{scb_id}' is marked as spare in the plant architecture. DS detection is not applicable.",
            "rows": 0,
        }

    query = _fault_timeline_query(db, plant_id, scb_id, date_from, date_to)
    # 20k is >200 days of 15-min samples — plenty for any UI timeline without
    # dragging 10+ MB of JSON onto the wire.
    results = query.order_by(FaultDiagnostics.timestamp.asc()).limit(20000).all()
    data = [_serialize_fault_row(r) for r in results]
    energy_available, energy_note = _voltage_meta(db, plant_id, date_from, date_to, scb_id)

    # ── Fetch plant-level irradiance and attach per timestamp ─────────────────
    irradiance_available = False
    irr_map: dict = {}
    if data:
        f_ts = str(data[0]["timestamp"])
        t_ts = str(data[-1]["timestamp"])
        try:
            probe = db.execute(
                text(
                    "SELECT signal FROM raw_data_generic "
                    "WHERE plant_id = :p "
                    "AND LOWER(TRIM(equipment_level::text)) IN ('plant','wms') "
                    "AND signal IN ('irradiance','gti','ghi') "
                    "AND timestamp >= :f AND timestamp <= :t "
                    "GROUP BY signal ORDER BY COUNT(*) DESC LIMIT 1"
                ),
                {"p": plant_id, "f": f_ts, "t": t_ts},
            ).fetchone()
            if probe:
                irr_signal = probe[0]
                irr_rows = db.execute(
                    text(
                        "SELECT timestamp, AVG(value) FROM raw_data_generic "
                        "WHERE plant_id = :p "
                        "AND LOWER(TRIM(equipment_level::text)) IN ('plant','wms') "
                        "AND signal = :s "
                        "AND timestamp >= :f AND timestamp <= :t "
                        "GROUP BY timestamp"
                    ),
                    {"p": plant_id, "s": irr_signal, "f": f_ts, "t": t_ts},
                ).fetchall()
                irr_map = {str(r[0]): round(float(r[1]), 2) for r in irr_rows if r[0] and r[1] is not None}
                irradiance_available = bool(irr_map)
        except Exception:
            irr_map = {}

    # Attach irradiance to each row (nearest-timestamp match via string key)
    for row in data:
        ts_key = str(row["timestamp"])
        row["irradiance"] = irr_map.get(ts_key, None)

    return {
        "data": data,
        "energy_available": energy_available,
        "energy_note": energy_note,
        "irradiance_available": irradiance_available,
    }


# ─── Power Limitation (inverter-level, 10:00–15:00, 40% drop vs reference) ───
@router.get("/pl-summary")
def get_pl_summary(
    plant_id: str = Query(...),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Summary for Power Limitation: active inverter count, total loss, per-inverter loss (for bar chart)."""
    _from, _to = _fault_date_range(date_from, date_to)
    return _pl_page_with_cache(db, plant_id, _from, _to)["summary"]


@router.get("/pl-inverter-status")
def get_pl_inverter_status(
    plant_id: str = Query(...),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Table data: one row per inverter with loss, last seen, investigation window. Reads from raw_data_generic."""
    _from, _to = _fault_date_range(date_from, date_to)
    return _pl_page_with_cache(db, plant_id, _from, _to)["inverter_status"]


@router.get("/pl-page")
def get_pl_page(
    plant_id: str = Query(...),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Summary KPIs + inverter table in one response — runs run_power_limitation once (avoids duplicate work from two parallel calls)."""
    _from, _to = _fault_date_range(date_from, date_to)
    return _pl_page_with_cache(db, plant_id, _from, _to)


@router.get("/pl-timeline")
def get_pl_timeline(
    plant_id: str = Query(...),
    inverter_id: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Timeline for one inverter (investigate modal): expected vs actual AC power, limited flag."""
    from datetime import date, timedelta
    from engine.power_limitation import run_power_limitation

    today = date.today()
    _from = date_from or str(today - timedelta(days=7))
    _to = date_to or str(today)
    _, timeline_rows = run_power_limitation(db, plant_id, _from, _to)
    if inverter_id:
        timeline_rows = [r for r in timeline_rows if r.get("inverter_id") == inverter_id]
    return {"data": timeline_rows}


@router.get("/is-summary")
def get_is_summary(
    plant_id: str = Query(...),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Summary for inverter shutdown: ac_power == 0 and irradiance > 5 W/m²."""
    _from, _to = _fault_date_range(date_from, date_to)
    return _is_tab_with_cache(db, plant_id, _from, _to)["summary"]


@router.get("/is-inverter-status")
def get_is_inverter_status(
    plant_id: str = Query(...),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Table rows for inverter shutdown status."""
    _from, _to = _fault_date_range(date_from, date_to)
    return _is_tab_with_cache(db, plant_id, _from, _to)["inverter_status"]


@router.get("/is-timeline")
def get_is_timeline(
    plant_id: str = Query(...),
    inverter_id: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Timeline for inverter shutdown detection."""
    from datetime import date, timedelta
    from engine.inverter_shutdown import run_inverter_shutdown

    today = date.today()
    _from = date_from or str(today - timedelta(days=7))
    _to = date_to or str(today)
    _, timeline_rows = run_inverter_shutdown(db, plant_id, _from, _to)
    if inverter_id:
        timeline_rows = [r for r in timeline_rows if r.get("inverter_id") == inverter_id]
    return {"data": timeline_rows}


@router.get("/gb-summary")
def get_gb_summary(
    plant_id: str = Query(...),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Summary for grid breakdown: all inverters AC=0 and irradiance > 5 W/m²."""
    _from, _to = _fault_date_range(date_from, date_to)
    return _gb_tab_with_cache(db, plant_id, _from, _to)["summary"]


@router.get("/gb-events")
def get_gb_events(
    plant_id: str = Query(...),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Table rows for grid breakdown events."""
    _from, _to = _fault_date_range(date_from, date_to)
    return _gb_tab_with_cache(db, plant_id, _from, _to)["events"]


@router.get("/comm-summary")
def get_comm_summary(
    plant_id: str = Query(...),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Summary for communication issues detected against WMS timestamps."""
    _from, _to = _fault_date_range(date_from, date_to)
    return _comm_tab_with_cache(db, plant_id, _from, _to)["summary"]


@router.get("/comm-events")
def get_comm_events(
    plant_id: str = Query(...),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Table rows for inverter/SCB communication issues."""
    _from, _to = _fault_date_range(date_from, date_to)
    return _comm_tab_with_cache(db, plant_id, _from, _to)["events"]


@router.get("/comm-inverter-loss")
def get_comm_inverter_loss(
    plant_id: str = Query(...),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Per-inverter estimated communication loss (kWh)."""
    _from, _to = _fault_date_range(date_from, date_to)
    return _comm_tab_with_cache(db, plant_id, _from, _to)["inverter_loss"]


@router.get("/runtime-tabs-bundle")
def get_runtime_tabs_bundle(
    plant_id: str = Query(...),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    current_user: User = Depends(get_current_user),
):
    """
    One response for PL + IS + GB tab payloads. Computes the three analyses in parallel
    (separate DB sessions) and warms per-tab memory + DB snapshots.
    """
    from concurrent.futures import ThreadPoolExecutor

    from database import SessionLocal

    _from, _to = _fault_date_range(date_from, date_to)
    bhit = _dc_get(_MEM_RUNTIME_TABS_BUNDLE, plant_id, _from, _to)
    if bhit is not None:
        return bhit

    def _pl():
        s = SessionLocal()
        try:
            return _pl_page_with_cache(s, plant_id, _from, _to)
        finally:
            s.close()

    def _is():
        s = SessionLocal()
        try:
            return _is_tab_with_cache(s, plant_id, _from, _to)
        finally:
            s.close()

    def _gb():
        s = SessionLocal()
        try:
            return _gb_tab_with_cache(s, plant_id, _from, _to)
        finally:
            s.close()

    def _comm():
        s = SessionLocal()
        try:
            return _comm_tab_with_cache(s, plant_id, _from, _to)
        finally:
            s.close()

    with ThreadPoolExecutor(max_workers=4) as pool:
        f_pl = pool.submit(_pl)
        f_is = pool.submit(_is)
        f_gb = pool.submit(_gb)
        f_comm = pool.submit(_comm)
        pl_out = f_pl.result()
        is_out = f_is.result()
        gb_out = f_gb.result()
        comm_out = f_comm.result()

    out = {
        "power_limitation": pl_out,
        "inverter_shutdown": is_out,
        "grid_breakdown": gb_out,
        "communication_issue": comm_out,
    }
    _dc_set(_MEM_RUNTIME_TABS_BUNDLE, plant_id, _from, _to, out)
    return out


def _unified_fault_categories_core(
    db: Session,
    plant_id: str,
    _from: str,
    _to: str,
    current_user: User,
) -> dict:
    """
    Shared aggregates for Fault Diagnostics unified feed (category tiles + totals).
    Does not call get_ds_scb_status (heavy); row expansion uses pl/is/gb/soiling only.
    """
    ds_summary = get_ds_summary(plant_id=plant_id, date_from=_from, date_to=_to, db=db, current_user=current_user)
    pl_page = _pl_page_with_cache(db, plant_id, _from, _to)
    is_tab = _is_tab_with_cache(db, plant_id, _from, _to)
    gb_tab = _gb_tab_with_cache(db, plant_id, _from, _to)
    comm_tab = _comm_tab_with_cache(db, plant_id, _from, _to)
    cd_tab = _cd_tab_with_cache(db, plant_id, _from, _to)

    soiling_plant: dict = {}
    soiling_rank: dict = {"rows": []}
    try:
        soiling_plant = build_plant_soiling_payload(db, plant_id, _from, _to) or {}
        soiling_rank = build_soiling_rankings_payload(db, plant_id, _from, _to, "scb") or {}
    except Exception:
        pass

    series = ds_summary.get("daily_energy_series") or []
    ds_energy_ok = bool(ds_summary.get("energy_available"))
    ds_loss_mwh = (
        round(sum(float(d.get("energy_loss_kwh") or 0) for d in series) / 1000.0, 4) if ds_energy_ok else 0.0
    )
    ds_count = int(ds_summary.get("active_ds_faults") or 0)

    pl_sum = pl_page.get("summary") or {}
    pl_loss_mwh = round(float(pl_sum.get("total_energy_loss_kwh") or 0) / 1000.0, 4)
    pl_count = int(pl_sum.get("active_pl_inverters") or 0)

    is_sum = is_tab.get("summary") or {}
    is_count = int(is_sum.get("active_shutdown_inverters") or 0)
    is_hours = float(is_sum.get("total_shutdown_hours") or 0)

    gb_sum = gb_tab.get("summary") or {}
    gb_count = int(gb_sum.get("active_grid_events") or 0)
    gb_hours = float(gb_sum.get("total_grid_breakdown_hours") or 0)

    comm_sum = comm_tab.get("summary") or {}
    comm_count = int(comm_sum.get("total_communication_issues") or 0)
    comm_loss_mwh = round(float(comm_sum.get("total_loss_kwh") or 0) / 1000.0, 4)

    cd_sum = cd_tab.get("summary") or {}
    clip_count     = int(cd_sum.get("active_clip_inverters") or 0)
    clip_loss_mwh  = round(float(cd_sum.get("loss_clipping_total_kwh") or 0) / 1000.0, 4)
    derate_count   = int(cd_sum.get("active_derate_inverters") or 0)
    derate_loss_mwh = round(float(cd_sum.get("loss_derating_total_kwh") or 0) / 1000.0, 4)

    sol_loss = soiling_plant.get("soiling_loss_mwh")
    sol_loss_mwh = round(float(sol_loss), 4) if sol_loss is not None else 0.0
    sol_rows_raw = soiling_rank.get("rows") or []
    sol_count = len([r for r in sol_rows_raw if float(r.get("loss_mwh") or 0) > 1e-6])

    # Inverter DC→AC conversion loss — keeps the overview tile in sync with the
    # Inverter Efficiency sub-tab value (previously hard-coded to 0).
    inv_eff_loss_mwh, inv_eff_active = _inv_eff_aggregate_with_cache(db, plant_id, _from, _to)

    categories = [
        {
            "id": "ds",
            "label": "Disconnected Strings",
            "loss_mwh": ds_loss_mwh,
            "fault_count": ds_count,
            "metric_note": "Energy loss summed over range" if ds_energy_ok else (ds_summary.get("energy_note") or "Energy N/A"),
        },
        {
            "id": "pl",
            "label": "Power Limitation",
            "loss_mwh": pl_loss_mwh,
            "fault_count": pl_count,
            "metric_note": "10:00–15:00 window; energy loss (kWh) / 1000",
        },
        {
            "id": "is",
            "label": "Inverter Shutdown",
            "loss_mwh": 0.0,
            "fault_count": is_count,
            "metric_note": f"Shutdown hours (plant total): {is_hours:.2f} h; MWh not modeled in feed",
        },
        {
            "id": "gb",
            "label": "Grid Breakdown",
            "loss_mwh": 0.0,
            "fault_count": gb_count,
            "metric_note": f"Breakdown hours (plant total): {gb_hours:.2f} h; MWh not modeled in feed",
        },
        {
            "id": "comm",
            "label": "Communication Issue",
            "loss_mwh": comm_loss_mwh,
            "fault_count": comm_count,
            "metric_note": "Hierarchical communication ownership with expected-power loss and no SCB duplicate loss",
        },
        {
            "id": "scb_perf",
            "label": "Soiling",
            "loss_mwh": sol_loss_mwh,
            "fault_count": sol_count,
            "metric_note": "Plant PR-regression loss + top SCB peer losses (estimated)",
        },
        {
            "id": "inv_eff",
            "label": "Inverter Efficiency",
            "loss_mwh": inv_eff_loss_mwh,
            "fault_count": inv_eff_active,
            "metric_note": "DC→AC conversion loss Σ (Pdc−Pac)·dt across inverters (same basis as the Inverter Efficiency tab)",
        },
        {
            "id": "clip",
            "label": "Clipping",
            "loss_mwh": clip_loss_mwh,
            "fault_count": clip_count,
            "metric_note": "GTI virtual-power model: inverter at rated AC with residual gap (power / current clipping)",
        },
        {
            "id": "derate",
            "label": "Derating",
            "loss_mwh": derate_loss_mwh,
            "fault_count": derate_count,
            "metric_note": "GTI virtual-power model: inverter producing but below virtual curve (static or dynamic derating)",
        },
        {
            "id": "damage",
            "label": "ByPass Diode/Module Damage",
            "loss_mwh": 0.0,
            "fault_count": 0,
            "metric_note": "No unified rows yet — use category tab",
        },
    ]

    total_loss_mwh = round(sum(c["loss_mwh"] for c in categories), 4)
    total_fault_count = sum(c["fault_count"] for c in categories)

    return {
        "categories": categories,
        "totals": {"loss_mwh": total_loss_mwh, "fault_count": total_fault_count},
        "ds_summary": ds_summary,
        "pl_page": pl_page,
        "is_tab": is_tab,
        "gb_tab": gb_tab,
        "comm_tab": comm_tab,
        "cd_tab": cd_tab,
        "soiling_rank_rows": sol_rows_raw,
    }


def _unified_feed_categories_only(
    db: Session,
    plant_id: str,
    _from: str,
    _to: str,
    current_user: User,
) -> dict:
    """Fault Diagnostics category MWh totals only — skips per-SCB DS row build (faster for Loss Analysis)."""
    core = _unified_fault_categories_core(db, plant_id, _from, _to, current_user)
    ds_summary = core["ds_summary"]
    return {
        "date_from": _from,
        "date_to": _to,
        "plant_id": plant_id,
        "categories": core["categories"],
        "totals": core["totals"],
        "ds_energy_note": ds_summary.get("energy_note"),
    }


def _unified_feed_rows_and_categories(
    db: Session,
    plant_id: str,
    _from: str,
    _to: str,
    current_user: User,
) -> dict:
    """
    Normalized cross-category fault rows + per-category aggregates for the Fault Diagnostics overview.
    Rows are capped for performance; investigate payloads match frontend modal routing.
    """
    MAX_ROWS = 500

    core = _unified_fault_categories_core(db, plant_id, _from, _to, current_user)
    categories = core["categories"]
    total_loss_mwh = core["totals"]["loss_mwh"]
    total_fault_count = core["totals"]["fault_count"]
    ds_summary = core["ds_summary"]
    pl_page = core["pl_page"]
    is_tab = core["is_tab"]
    gb_tab = core["gb_tab"]
    comm_tab = core["comm_tab"]
    sol_rows_raw = core["soiling_rank_rows"]

    ds_pack = get_ds_scb_status(plant_id=plant_id, date_from=_from, date_to=_to, db=db, current_user=current_user)

    rows: List[dict] = []

    for drow in ds_pack.get("data") or []:
        ms = int(drow.get("missing_strings") or drow.get("range_min_missing_strings") or 0)
        if ms <= 0:
            continue
        scb = drow.get("scb_id")
        if not scb:
            continue
        ekwh = float(drow.get("energy_loss_kwh") or 0)
        ts = drow.get("timestamp")
        rows.append(
            {
                "id": f"ds:{scb}",
                "category": "ds",
                "category_label": "Disconnected Strings",
                "occurred_at": str(ts) if ts else f"{_to} 00:00:00",
                "equipment_id": scb,
                "equipment_level": "scb",
                "severity_energy_kwh": round(ekwh, 4),
                "severity_hours": None,
                "duration_note": f"{drow.get('recurring_days') or 0} recurring days" if drow.get("recurring_days") else None,
                "status": str(drow.get("fault_status") or "DS"),
                "investigate": {"kind": "ds", "scb_id": scb},
                "_sort_loss_kwh": ekwh,
            }
        )

    for prow in (pl_page.get("inverter_status") or {}).get("data") or []:
        inv = prow.get("inverter_id")
        if not inv:
            continue
        ekwh = float(prow.get("total_energy_loss_kwh") or 0)
        if ekwh <= 0:
            continue
        rows.append(
            {
                "id": f"pl:{inv}",
                "category": "pl",
                "category_label": "Power Limitation",
                "occurred_at": str(prow.get("last_seen_fault") or prow.get("investigation_window_end") or f"{_to} 23:59:59"),
                "equipment_id": inv,
                "equipment_level": "inverter",
                "severity_energy_kwh": round(ekwh, 4),
                "severity_hours": None,
                "duration_note": None,
                "status": "Power limitation",
                "investigate": {"kind": "pl", "inverter_id": inv},
                "_sort_loss_kwh": ekwh,
            }
        )

    for irow in (is_tab.get("inverter_status") or {}).get("data") or []:
        inv = irow.get("inverter_id")
        if not inv:
            continue
        hrs = float(irow.get("shutdown_hours") or 0)
        pts = int(irow.get("shutdown_points") or 0)
        if pts <= 0 and hrs <= 0:
            continue
        rows.append(
            {
                "id": f"is:{inv}",
                "category": "is",
                "category_label": "Inverter Shutdown",
                "occurred_at": str(irow.get("last_seen_shutdown") or irow.get("investigation_window_end") or f"{_to} 23:59:59"),
                "equipment_id": inv,
                "equipment_level": "inverter",
                "severity_energy_kwh": 0.0,
                "severity_hours": round(hrs, 4),
                "duration_note": f"{pts} points",
                "status": "Inverter shutdown",
                "investigate": {"kind": "is", "inverter_id": inv},
                "_sort_loss_kwh": hrs * 50.0,
            }
        )

    for erow in (gb_tab.get("events") or {}).get("data") or []:
        eid = erow.get("event_id")
        if not eid:
            continue
        hrs = float(erow.get("breakdown_hours") or 0)
        pts = int(erow.get("breakdown_points") or 0)
        if pts <= 0 and hrs <= 0:
            continue
        rows.append(
            {
                "id": f"gb:{eid}",
                "category": "gb",
                "category_label": "Grid Breakdown",
                "occurred_at": str(erow.get("last_seen_breakdown") or erow.get("investigation_window_end") or f"{_to} 23:59:59"),
                "equipment_id": str(eid),
                "equipment_level": "plant_event",
                "severity_energy_kwh": 0.0,
                "severity_hours": round(hrs, 4),
                "duration_note": f"{pts} points",
                "status": "Grid breakdown",
                "investigate": {"kind": "gb", "event_id": str(eid)},
                "_sort_loss_kwh": hrs * 100.0,
            }
        )

    for crow in (comm_tab.get("events") or {}).get("data") or []:
        eq_id = crow.get("equipment_id")
        eq_level = crow.get("equipment_level")
        if not eq_id or not eq_level:
            continue
        hrs = float(crow.get("communication_hours") or 0)
        pts = int(crow.get("communication_points") or 0)
        ekwh = float(crow.get("estimated_loss_kwh") or 0)
        if pts <= 0 and hrs <= 0 and ekwh <= 0:
            continue
        inv = crow.get("inverter_id")
        issue_kind = str(crow.get("issue_kind") or "")
        status = str(crow.get("status") or "Communication issue")
        inv_payload = {"kind": "comm", "equipment_level": str(eq_level), "equipment_id": str(eq_id)}
        if inv:
            inv_payload["inverter_id"] = str(inv)
        if issue_kind:
            inv_payload["issue_kind"] = issue_kind
        rows.append(
            {
                "id": f"comm:{eq_level}:{eq_id}:{issue_kind or 'event'}",
                "category": "comm",
                "category_label": "Communication Issue",
                "occurred_at": str(crow.get("last_seen_communication") or crow.get("investigation_window_end") or f"{_to} 23:59:59"),
                "equipment_id": str(eq_id),
                "equipment_level": str(eq_level),
                "severity_energy_kwh": round(ekwh, 4),
                "severity_hours": round(hrs, 4),
                "duration_note": f"{int(crow.get('communication_windows') or 0)} windows / {pts} points",
                "status": status,
                "investigate": inv_payload,
                "_sort_loss_kwh": ekwh if ekwh > 0 else hrs * 25.0,
            }
        )

    for srow in sol_rows_raw:
        sid = srow.get("id")
        lm = float(srow.get("loss_mwh") or 0)
        if not sid or lm <= 1e-6:
            continue
        rows.append(
            {
                "id": f"scb_perf:{sid}",
                "category": "scb_perf",
                "category_label": "Soiling",
                "occurred_at": f"{_to} 12:00:00",
                "equipment_id": sid,
                "equipment_level": "scb",
                "severity_energy_kwh": round(lm * 1000.0, 4),
                "severity_hours": None,
                "duration_note": "Peer-based estimate (range)",
                "status": "Soiling (est.)",
                "investigate": {"kind": "scb_perf", "scb_id": sid},
                "_sort_loss_kwh": lm * 1000.0,
            }
        )

    rows.sort(key=lambda r: float(r.get("_sort_loss_kwh") or 0), reverse=True)
    for r in rows:
        r.pop("_sort_loss_kwh", None)
    rows = rows[:MAX_ROWS]

    return {
        "date_from": _from,
        "date_to": _to,
        "plant_id": plant_id,
        "categories": categories,
        "totals": {
            "loss_mwh": total_loss_mwh,
            "fault_count": total_fault_count,
        },
        "rows": rows,
        "row_limit": MAX_ROWS,
        "ds_energy_note": ds_summary.get("energy_note"),
    }


@router.get("/unified-feed")
def get_unified_fault_feed(
    plant_id: str = Query(...),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: Session = Depends(get_read_db),
    current_user: User = Depends(get_current_user),
):
    """Single round-trip overview: aggregate tiles + normalized fault rows for the unified table."""
    _from, _to = _fault_date_range(date_from, date_to)
    return _unified_feed_rows_and_categories(db, plant_id, _from, _to, current_user)


@router.get("/comm-timeline")
def get_comm_timeline(
    plant_id: str = Query(...),
    equipment_level: str = Query(...),
    equipment_id: str = Query(...),
    issue_kind: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Timeline for inverter/SCB communication issue investigation."""
    _from, _to = _fault_date_range(date_from, date_to)
    return {
        "data": get_communication_timeline(
            db,
            plant_id=plant_id,
            equipment_level=equipment_level,
            equipment_id=equipment_id,
            date_from=_from,
            date_to=_to,
            issue_kind=issue_kind,
        )
    }


# ── Clipping & Derating endpoints ─────────────────────────────────────────────
@router.get("/cd-summary")
def get_cd_summary(
    plant_id: str = Query(...),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Clipping & Derating summary: loss split (power/current/static/dynamic) + per-inverter bar data."""
    _from, _to = _fault_date_range(date_from, date_to)
    return _cd_tab_with_cache(db, plant_id, _from, _to)["summary"]


@router.get("/cd-inverter-status")
def get_cd_inverter_status(
    plant_id: str = Query(...),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Per-inverter clipping/derating detail rows for the table (loss-by-kind, dominant kind, investigate window)."""
    _from, _to = _fault_date_range(date_from, date_to)
    return {"data": _cd_tab_with_cache(db, plant_id, _from, _to)["inverter_status"]}


@router.get("/cd-page")
def get_cd_page(
    plant_id: str = Query(...),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Bundled payload for the Clipping & Derating tab — summary + table in one response.

    The cached payload also holds per-inverter timelines; we strip them from
    the wire response (they can be multi-MB across 96 inverters) and serve
    them individually via /cd-timeline so the UI stays fast.
    """
    _from, _to = _fault_date_range(date_from, date_to)
    cached = _cd_tab_with_cache(db, plant_id, _from, _to)
    return {
        "summary":         cached.get("summary"),
        "inverter_status": cached.get("inverter_status"),
        "engine_meta":     cached.get("engine_meta"),
        "_has_timeline":   cached.get("_has_timeline"),
    }


@router.get("/cd-timeline")
def get_cd_timeline(
    plant_id: str = Query(...),
    inverter_id: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Per-minute actual-vs-virtual timeline for the investigate modal.

    Reads from the cached tab payload (populated by _compute_cd_tab) so that
    clicking Investigate does *not* trigger a full engine re-run. Falls back
    to a targeted single-inverter compute only if the cache is absent.
    """
    _from, _to = _fault_date_range(date_from, date_to)
    cached = _cd_tab_with_cache(db, plant_id, _from, _to)
    tl_map = cached.get("_timelines_by_inv") or {}
    if inverter_id:
        return {"data": tl_map.get(inverter_id, [])}
    out: list = []
    for rows in tl_map.values():
        out.extend(rows)
    return {"data": out}


@router.get("/gb-timeline")
def get_gb_timeline(
    plant_id: str = Query(...),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Timeline for grid breakdown detection."""
    from datetime import date, timedelta
    from engine.grid_breakdown import run_grid_breakdown

    today = date.today()
    _from = date_from or str(today - timedelta(days=7))
    _to = date_to or str(today)
    _, timeline_rows = run_grid_breakdown(db, plant_id, _from, _to)
    return {"data": timeline_rows}


@router.get("/inverter-efficiency-analysis")
def get_inverter_efficiency_analysis(
    plant_id: str = Query(...),
    date_from: str = Query(default=None),
    date_to: str = Query(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Comprehensive Inverter Efficiency Loss Analysis.
    Calculates:
    - KPIs (Total DC/AC Energy, Loss, Avg Eff)
    - Per-Inverter Stats (for Bar Chart & Box Plot)
    - Time-series Trend (Actual vs Target)
    """
    from sqlalchemy import text
    import pandas as pd
    import numpy as np
    from datetime import datetime, timedelta

    today = datetime.now().date()
    if date_from and date_to:
        _from, _to = date_from, date_to
    else:
        _from = str(today - timedelta(days=7))
        _to = str(today)

    # 1. Fetch Pdc and Pac joined by timestamp and equipment_id
    sql = text("""
        SELECT timestamp, equipment_id,
               MAX(CASE WHEN signal='dc_power' THEN value END) as dc_kw,
               MAX(CASE WHEN signal='ac_power' THEN value END) as ac_kw
        FROM raw_data_generic
        WHERE plant_id = :plant_id
          AND equipment_level = 'inverter'
          AND timestamp BETWEEN :f AND :t
        GROUP BY timestamp, equipment_id
    """)
    res = db.execute(sql, {"plant_id": plant_id, "f": f"{_from} 00:00:00", "t": f"{_to} 23:59:59"}).fetchall()
    
    df = pd.DataFrame(res, columns=['timestamp', 'inverter_id', 'dc_kw', 'ac_kw'])
    
    # 1b. Fetch SCB fallback data (Current/Voltage)
    scb_sql = text("""
        SELECT r.timestamp, p.inverter_id,
               SUM(CASE WHEN r.signal='dc_current' THEN r.value END) as total_scb_curr,
               AVG(CASE WHEN r.signal='dc_voltage' THEN r.value END) as avg_scb_volt
        FROM raw_data_generic r
        JOIN plant_architecture p ON r.equipment_id = p.scb_id
        WHERE p.plant_id = :plant_id
          AND r.signal IN ('dc_current', 'dc_voltage')
          AND r.timestamp BETWEEN :f AND :t
        GROUP BY r.timestamp, p.inverter_id
    """)
    scb_res = db.execute(scb_sql, {"plant_id": plant_id, "f": f"{_from} 00:00:00", "t": f"{_to} 23:59:59"}).fetchall()
    if scb_res:
        scb_df = pd.DataFrame(scb_res, columns=['timestamp', 'inverter_id', 'total_scb_curr', 'avg_scb_volt'])
        df = pd.merge(df, scb_df, on=['timestamp', 'inverter_id'], how='outer')

    df['dc_kw'] = pd.to_numeric(df.get('dc_kw', 0), errors='coerce').fillna(0)
    df['ac_kw'] = pd.to_numeric(df.get('ac_kw', 0), errors='coerce').fillna(0)
    
    # Apply Fallback: Pdc = I_scb * V / 1000
    if 'total_scb_curr' in df.columns:
        df['total_scb_curr'] = pd.to_numeric(df['total_scb_curr'], errors='coerce').fillna(0)
        df['avg_scb_volt'] = pd.to_numeric(df['avg_scb_volt'], errors='coerce').fillna(1000.0) # Dummy 1000V if missing
        
        df['dc_kw'] = df.apply(lambda r: 
            (r['total_scb_curr'] * r['avg_scb_volt'] / 1000.0) if (r['dc_kw'] == 0 and r['total_scb_curr'] > 0)
            else r['dc_kw'], axis=1)
    
    # Validation: Only use points where DC > AC
    df_valid = df[df['dc_kw'] > df['ac_kw']].copy()
    
    if df_valid.empty:
        return {"metrics": {}, "inverters": [], "trend": [], "inverter_box_stats": []}

    # Per-timestamp efficiency for box plot: Efficiency (%) = (AC Power / DC Power) × 100
    df_valid["eff_pct"] = np.where(
        df_valid["dc_kw"] > 0,
        (df_valid["ac_kw"] / df_valid["dc_kw"]) * 100.0,
        np.nan,
    )
    # Box stats per inverter (min, Q1, median, Q3, max) from all efficiency values in the period
    def _box_stats(series):
        arr = series.dropna()
        arr = arr[(arr >= 0) & (arr <= 200)]  # clamp to plausible range
        if arr.size == 0:
            return {"min": None, "q1": None, "median": None, "q3": None, "max": None}
        return {
            "min": float(round(np.min(arr), 2)),
            "q1": float(round(np.nanpercentile(arr, 25), 2)),
            "median": float(round(np.median(arr), 2)),
            "q3": float(round(np.nanpercentile(arr, 75), 2)),
            "max": float(round(np.max(arr), 2)),
        }

    box_by_inv = df_valid.groupby("inverter_id")["eff_pct"].apply(
        lambda s: _box_stats(s)
    ).to_dict()

    # Energy calculations with cadence-aware interval (supports 1-min/5-min/15-min uploads).
    df_valid["timestamp_dt"] = pd.to_datetime(df_valid["timestamp"], errors="coerce")
    step_sample = (
        df_valid.sort_values(["inverter_id", "timestamp_dt"])
        .groupby("inverter_id")["timestamp_dt"]
        .diff()
        .dropna()
        .dt.total_seconds()
        / 3600.0
    )
    step_sample = step_sample[(step_sample > 0) & (step_sample <= 6.0)]
    dt_h = float(step_sample.median()) if len(step_sample) > 0 else (1.0 / 60.0)
    if not np.isfinite(dt_h) or dt_h <= 0:
        dt_h = 1.0 / 60.0

    df_valid['loss_kwh'] = (df_valid['dc_kw'] - df_valid['ac_kw']) * dt_h
    df_valid['dc_kwh'] = df_valid['dc_kw'] * dt_h
    df_valid['ac_kwh'] = df_valid['ac_kw'] * dt_h
    
    # 2. Per-Inverter Aggregation
    inv_grp = df_valid.groupby('inverter_id').agg({
        'dc_kwh': 'sum',
        'ac_kwh': 'sum',
        'loss_kwh': 'sum'
    }).reset_index()
    
    inv_grp['efficiency_pct'] = (inv_grp['ac_kwh'] / inv_grp['dc_kwh']) * 100
    inv_grp = inv_grp.replace([np.inf, -np.inf], np.nan).fillna(0)
    inv_grp = inv_grp.sort_values(by='loss_kwh', ascending=False)
    
    # Get target/rated efficiency from EquipmentSpec (prefer rated_efficiency when set)
    specs = db.query(EquipmentSpec).filter(EquipmentSpec.plant_id == plant_id, EquipmentSpec.equipment_type == 'inverter').all()
    def _eff(s):
        return s.rated_efficiency if getattr(s, 'rated_efficiency', None) is not None else (s.target_efficiency or 98.5)
    spec_map = {s.equipment_id: _eff(s) for s in specs}
    global_target_eff = np.mean([_eff(s) for s in specs]) if specs else 98.5

    inv_list = []
    inverter_box_stats = []
    for _, r in inv_grp.iterrows():
        inv_id = r["inverter_id"]
        target = spec_map.get(inv_id, 98.5)
        box = box_by_inv.get(inv_id, {})
        inv_list.append({
            "inverter_id": inv_id,
            "dc_energy_mwh": round(r["dc_kwh"] / 1000, 3),
            "ac_energy_mwh": round(r["ac_kwh"] / 1000, 3),
            "loss_energy_mwh": round(r["loss_kwh"] / 1000, 3),
            "efficiency_pct": round(r["efficiency_pct"], 2),
            "target_efficiency": target,
            "box_min": box.get("min"),
            "box_q1": box.get("q1"),
            "box_median": box.get("median"),
            "box_q3": box.get("q3"),
            "box_max": box.get("max"),
        })
        if box.get("median") is not None:
            inverter_box_stats.append({
                "inverter_id": inv_id,
                "min": box["min"],
                "q1": box["q1"],
                "median": box["median"],
                "q3": box["q3"],
                "max": box["max"],
                "iqrLength": round((box["q3"] or 0) - (box["q1"] or 0), 2),
                "errorMin": round((box["median"] or 0) - (box["min"] or 0), 2),
                "errorMax": round((box["max"] or 0) - (box["median"] or 0), 2),
            })

    # 3. Global Metrics
    total_dc_kwh = inv_grp['dc_kwh'].sum()
    total_ac_kwh = inv_grp['ac_kwh'].sum()
    total_loss_kwh = inv_grp['loss_kwh'].sum()
    avg_eff = (total_ac_kwh / total_dc_kwh) * 100 if total_dc_kwh > 0 else 0
    
    metrics = {
        "total_dc_mwh": round(total_dc_kwh / 1000, 2),
        "total_ac_mwh": round(total_ac_kwh / 1000, 2),
        "total_loss_mwh": round(total_loss_kwh / 1000, 2),
        "avg_efficiency_pct": round(avg_eff, 2),
        "target_efficiency_pct": round(global_target_eff, 1)
    }

    # 4. Trend Calculation (Hourly resolution)
    df_valid['timestamp'] = df_valid['timestamp'].astype(str)
    df_valid['hour'] = df_valid['timestamp'].str.slice(0, 13)
    trend_grp = df_valid.groupby('hour').agg({
        'dc_kwh': 'sum',
        'ac_kwh': 'sum'
    }).reset_index()
    
    trend_grp['efficiency_pct'] = (trend_grp['ac_kwh'] / trend_grp['dc_kwh']) * 100
    trend_grp = trend_grp.replace([np.inf, -np.inf], np.nan).fillna(0)
    
    trend_list = []
    for _, r in trend_grp.iterrows():
        trend_list.append({
            "timestamp": r['hour'] + ":00",
            "efficiency_pct": float(round(r['efficiency_pct'], 2)),
            "target_efficiency": float(round(global_target_eff, 1))
        })

    result = {
        "metrics": {k: float(v) for k, v in metrics.items()},
        "inverters": [
            {k: (float(v) if isinstance(v, (int, float, np.number)) and not isinstance(v, bool) else v)
             for k, v in row_dict.items()}
            for row_dict in inv_list
        ],
        "trend": trend_list,
        "inverter_box_stats": inverter_box_stats,
    }
    return result


@router.get("/scb-performance-heatmap")
def get_scb_performance_heatmap(
    plant_id: str = Query(...),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    SCB performance heatmap: per-SCB DC current by hour, within-inverter percentile (legacy),
    and `ratio` = current / (scb_dc_kw * max(irr_W/m2/1000, eps)) for continuous Soiling coloring.
    """
    import pandas as pd
    import numpy as np

    _from, _to = _fault_date_range(date_from, date_to)

    params = {"plant_id": plant_id, "f": f"{_from[:10]} 00:00:00", "t": f"{_to[:10]} 23:59:59"}

    # Architecture first (needed for inverter grouping)
    arch_sql = text("""
        SELECT scb_id, inverter_id FROM plant_architecture
        WHERE plant_id = :p AND scb_id IS NOT NULL
    """)
    arch = db.execute(arch_sql, {"p": plant_id}).fetchall()
    inv_by_scb = {r[0]: r[1] for r in arch}
    if not inv_by_scb:
        return {"time_buckets": [], "scbs": [], "cells": []}

    # Aggregate in DB by hour + scb to avoid fetching millions of rows (hourly heatmap).
    # Postgres only: raw_data_15m was a legacy SQLite-only cache and has been removed.
    sql = text("""
        SELECT date_trunc('hour', CAST(r.timestamp AS TIMESTAMP)) AS bucket,
               r.equipment_id AS scb_id,
               AVG(r.value) AS current_val
          FROM raw_data_generic r
         WHERE r.plant_id = :plant_id
           AND r.equipment_level = 'scb'
           AND r.signal = 'dc_current'
           AND r.timestamp >= :f
           AND r.timestamp <= :t
         GROUP BY date_trunc('hour', CAST(r.timestamp AS TIMESTAMP)), r.equipment_id
    """)
    try:
        rows = db.execute(sql, params).fetchall()
    except Exception:
        rows = []

    if not rows:
        return {"time_buckets": [], "scbs": [], "cells": []}

    df = pd.DataFrame(rows, columns=["bucket", "scb_id", "current_val"])
    df["current_val"] = pd.to_numeric(df["current_val"], errors="coerce")
    df = df.dropna(subset=["current_val"])
    df["inverter_id"] = df["scb_id"].map(inv_by_scb)
    df = df[df["inverter_id"].notna()]

    # Keep only buckets inside the selected inclusive date range (avoids TZ/extra-day columns).
    lo, hi = _from[:10], _to[:10]
    df["_day"] = pd.to_datetime(df["bucket"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df[(df["_day"] >= lo) & (df["_day"] <= hi)].drop(columns=["_day"])

    if df.empty:
        return {"time_buckets": [], "scbs": [], "cells": []}
    buckets_sorted = sorted(df["bucket"].unique())
    # Limit to 7 days * 24 = 168 points max; if more, sample
    if len(buckets_sorted) > 168:
        step = len(buckets_sorted) // 168
        buckets_sorted = buckets_sorted[:: max(1, step)][:168]
    bucket_to_idx = {b: i for i, b in enumerate(buckets_sorted)}

    irr_by_bucket = {}
    raw_irr = fetch_hourly_plant_irradiance_w_m2(db, used_table, plant_id, params["f"], params["t"])
    for k, v in raw_irr.items():
        irr_by_bucket[pd.Timestamp(k)] = float(v or 0.0)
    scb_dc_by = scb_dc_map(db, plant_id)
    eps = 0.01

    # For each (bucket, inverter) compute percentile (legacy) and irradiance-normalized ratio (Soiling gradient)
    cells = []
    for (bucket, inv_id), grp in df.groupby(["bucket", "inverter_id"]):
        if bucket not in bucket_to_idx:
            continue
        idx = bucket_to_idx[bucket]
        vals = grp.set_index("scb_id")["current_val"]
        vmin, vmax = vals.min(), vals.max()
        if vmax > vmin:
            pct = (vals - vmin) / (vmax - vmin) * 100
        else:
            pct = pd.Series(50.0, index=vals.index)
        b_ts = pd.Timestamp(bucket)
        irr_w = float(irr_by_bucket.get(b_ts, 0.0))
        for scb_id, current in vals.items():
            sdc = float(scb_dc_by.get(str(scb_id), 1.0) or 1.0)
            denom = max(sdc * max(irr_w / 1000.0, eps), eps)
            ratio = float(current) / denom
            cells.append({
                "scb_id": scb_id,
                "bucket_idx": idx,
                "current": round(float(current), 2),
                "percentile": round(float(pct[scb_id]), 1),
                "ratio": round(float(ratio), 6),
            })

    scb_order = sorted(df["scb_id"].unique(), key=lambda s: (inv_by_scb.get(s, ""), s))
    scbs = [{"scb_id": s, "inverter_id": inv_by_scb.get(s)} for s in scb_order]
    time_buckets = [{"index": i, "label": str(b)[:16]} for i, b in enumerate(buckets_sorted)]

    return {
        "time_buckets": time_buckets,
        "scbs": scbs,
        "cells": cells,
    }


@router.get("/scb-trend")
def get_scb_trend(
    plant_id: str = Query(...),
    scb_id: str = Query(...),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Time-series of DC current for one SCB over the selected date range.
    Returns series and optional slope (linear regression) for trend.
    """
    from datetime import datetime, timedelta

    today = datetime.now().date()
    _from = date_from or str(today - timedelta(days=1))
    _to = date_to or str(today)

    params = {"plant_id": plant_id, "scb_id": scb_id, "f": f"{_from} 00:00:00", "t": f"{_to} 23:59:59"}
    sql = text("""
        SELECT timestamp, value AS current_val
          FROM raw_data_generic
         WHERE plant_id = :plant_id
           AND equipment_level = 'scb'
           AND equipment_id = :scb_id
           AND signal = 'dc_current'
           AND timestamp >= :f
           AND timestamp <= :t
         ORDER BY timestamp
    """)
    try:
        rows = db.execute(sql, params).fetchall()
    except Exception:
        rows = []

    if not rows:
        return {"scb_id": scb_id, "series": [], "slope": None, "slope_unit": "A/min"}

    import numpy as np
    series = []
    for r in rows:
        ts = r[0]
        val = float(r[1]) if r[1] is not None else None
        if val is not None:
            series.append({"timestamp": str(ts), "current": round(val, 2)})

    if len(series) < 2:
        return {"scb_id": scb_id, "series": series, "slope": None, "slope_unit": "A/min"}

    import pandas as pd
    # Simple linear regression for slope (current vs time as minutes from start)
    t0 = pd.Timestamp(series[0]["timestamp"])
    x = np.array([(pd.Timestamp(s["timestamp"]) - t0).total_seconds() / 60.0 for s in series])
    y = np.array([s["current"] for s in series])
    n = len(x)
    sx, sy = x.sum(), y.sum()
    sxy = (x * y).sum()
    sxx = (x * x).sum()
    denom = n * sxx - sx * sx
    if abs(denom) > 1e-10:
        slope = (n * sxy - sx * sy) / denom
    else:
        slope = 0.0
    return {
        "scb_id": scb_id,
        "series": series,
        "slope": round(float(slope), 4),
        "slope_unit": "A/min",
    }


@router.get("/soiling-plant-pr")
def get_soiling_plant_pr(
    plant_id: str = Query(...),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Daily plant PR, soiling KPIs, loss and revenue estimates for the Soiling tab."""
    _from, _to = _fault_date_range(date_from, date_to)
    return build_plant_soiling_payload(db, plant_id, _from, _to)


@router.get("/soiling-rankings")
def get_soiling_rankings(
    plant_id: str = Query(...),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    group_by: str = Query("inverter", description="inverter or scb"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _from, _to = _fault_date_range(date_from, date_to)
    gb = (group_by or "inverter").lower().strip()
    if gb not in ("inverter", "scb"):
        gb = "inverter"
    return build_soiling_rankings_payload(db, plant_id, _from, _to, gb)


@router.get("/scb-soiling-trend")
def get_scb_soiling_trend(
    plant_id: str = Query(...),
    scb_id: str = Query(...),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Daily normalized current vs irradiance for Soiling SCB modal."""
    _from, _to = _fault_date_range(date_from, date_to)
    return build_scb_soiling_trend_payload(db, plant_id, scb_id, _from, _to)


@router.get("/ds-filter-summary")
def get_ds_filter_summary(
    plant_id: str = Query(...),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Returns how many SCBs had data quality issues (outlier / constant / leakage)
    filtered out during DS detection for the given date range.
    Reads stored filter_summary per day from fault_cache (written by ds_detection at run time).
    No response caching — computed from DB/cache storage each request.
    """
    from datetime import date as _date, timedelta

    outlier_set: set = set()
    constant_set: set = set()
    leakage_set: set = set()

    if date_from and date_to:
        try:
            start = _date.fromisoformat(date_from)
            end = _date.fromisoformat(date_to)
            d = start
            while d <= end:
                key = f"filter_summary:{plant_id}:{d.isoformat()}"
                day_data = get_cached(db, key, 876000)
                if day_data and isinstance(day_data, dict):
                    outlier_set.update(day_data.get("outlier", []))
                    constant_set.update(day_data.get("constant", []))
                    leakage_set.update(day_data.get("leakage", []))
                d += timedelta(days=1)
        except Exception:
            pass

    return {
        "outlier_count": len(outlier_set),
        "constant_count": len(constant_set),
        "leakage_count": len(leakage_set),
        "total_filtered": len(outlier_set | constant_set | leakage_set),
        "outlier_scbs": sorted(outlier_set),
        "constant_scbs": sorted(constant_set),
        "leakage_scbs": sorted(leakage_set),
    }


# ── SCB Fault Review endpoints ─────────────────────────────────────────────────

@router.get("/ds-review")
def get_ds_reviews(
    plant_id: str = Query(...),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Returns all saved reviews for the given plant + date range.
    Returns a dict keyed by scb_id for easy lookup.
    """
    q = db.query(ScbFaultReview).filter(ScbFaultReview.plant_id == plant_id)
    if date_from:
        q = q.filter(ScbFaultReview.date_from == date_from)
    if date_to:
        q = q.filter(ScbFaultReview.date_to == date_to)
    rows = q.all()
    return {
        r.scb_id: {
            "review_status": r.review_status,
            "remarks": r.remarks or "",
            "reviewed_by": r.reviewed_by or "",
            "reviewed_at": str(r.reviewed_at) if r.reviewed_at else "",
        }
        for r in rows
    }


from pydantic import BaseModel as _BM

class _ReviewIn(_BM):
    plant_id: str
    scb_id: str
    date_from: str
    date_to: str
    review_status: str   # valid_fault | other_fault | no_fault
    remarks: Optional[str] = ""


@router.post("/ds-review")
def save_ds_review(
    body: _ReviewIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Upsert a review for one SCB in the given date range.
    If a review already exists for (plant_id, scb_id, date_from, date_to) it's updated.
    """
    VALID_STATUSES = {"valid_fault", "other_fault", "no_fault"}
    if body.review_status not in VALID_STATUSES:
        from fastapi import HTTPException
        raise HTTPException(status_code=422, detail=f"review_status must be one of {VALID_STATUSES}")

    existing = db.query(ScbFaultReview).filter(
        ScbFaultReview.plant_id  == body.plant_id,
        ScbFaultReview.scb_id    == body.scb_id,
        ScbFaultReview.date_from == body.date_from,
        ScbFaultReview.date_to   == body.date_to,
    ).first()

    if existing:
        existing.review_status = body.review_status
        existing.remarks       = body.remarks or ""
        existing.reviewed_by   = current_user.email
    else:
        db.add(ScbFaultReview(
            plant_id      = body.plant_id,
            scb_id        = body.scb_id,
            date_from     = body.date_from,
            date_to       = body.date_to,
            review_status = body.review_status,
            remarks       = body.remarks or "",
            reviewed_by   = current_user.email,
        ))

    db.commit()
    return {"ok": True, "scb_id": body.scb_id, "review_status": body.review_status}
