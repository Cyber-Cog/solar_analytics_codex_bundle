"""
DB queries and assembly for Soiling analytics (Faults tab).
"""

from __future__ import annotations

import statistics
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from ac_power_energy_sql import (
    sql_inverter_ac_daily_energy,
    sql_plant_ac_daily_energy,
    sql_wms_irradiance_daily_sums,
)
from dashboard_helpers import gti_insolation_kwh_m2_from_sums
from db_perf import choose_data_table
from models import EquipmentSpec, Plant
from soiling_helpers import (
    linreg_slope_per_step,
    median_consecutive_delta,
    moving_median,
    ratio_trend_stats,
    soiling_loss_kwh_from_pr_steps,
)


def _is_pg(db: Session) -> bool:
    try:
        return getattr(db.get_bind().dialect, "name", "") == "postgresql"
    except Exception:
        return False


def _day_key(d: Any) -> str:
    if d is None:
        return ""
    if hasattr(d, "isoformat"):
        return str(d.isoformat())[:10]
    s = str(d)
    return s[:10] if len(s) >= 10 else s


def _inverter_dc_maps(db: Session, plant_id: str) -> Tuple[Dict[str, float], Dict[str, float]]:
    spec_dc: Dict[str, float] = {}
    for s in (
        db.query(EquipmentSpec)
        .filter(EquipmentSpec.plant_id == plant_id, EquipmentSpec.equipment_type == "inverter")
        .all()
    ):
        if s.equipment_id and s.dc_capacity_kwp is not None:
            spec_dc[s.equipment_id] = float(s.dc_capacity_kwp)
    arch_dc: Dict[str, float] = {}
    rows = db.execute(
        text(
            """
            SELECT inverter_id, SUM(dc_capacity_kw) AS s
            FROM plant_architecture
            WHERE plant_id = :p AND inverter_id IS NOT NULL
            GROUP BY inverter_id
            """
        ),
        {"p": plant_id},
    ).fetchall()
    for r in rows:
        if r[0] is not None and r[1] is not None:
            arch_dc[str(r[0])] = float(r[1])
    return spec_dc, arch_dc


def _plant_dc_kwp(spec_dc: Dict[str, float], arch_dc: Dict[str, float]) -> float:
    keys = set(spec_dc.keys()) | set(arch_dc.keys())
    total = 0.0
    for k in keys:
        v = arch_dc.get(k) or spec_dc.get(k) or 0.0
        total += float(v)
    return total


def scb_dc_map(db: Session, plant_id: str) -> Dict[str, float]:
    out: Dict[str, float] = {}
    rows = db.execute(
        text(
            """
            SELECT DISTINCT scb_id, dc_capacity_kw
            FROM plant_architecture
            WHERE plant_id = :p AND scb_id IS NOT NULL AND dc_capacity_kw IS NOT NULL
            """
        ),
        {"p": plant_id},
    ).fetchall()
    for r in rows:
        if r[0] is not None and r[1] is not None:
            out[str(r[0])] = float(r[1])
    return out


def _fetch_daily_inv_energy(
    db: Session, table: str, plant_id: str, f_ts: str, t_ts: str
) -> List[Tuple[Any, str, float]]:
    if not _is_pg(db):
        return []
    try:
        sql = text(sql_inverter_ac_daily_energy(table))
        rows = db.execute(
            sql, {"plant_id": plant_id, "f": f_ts, "t": t_ts}
        ).fetchall()
        return [(r[0], str(r[1]), float(r[2] or 0)) for r in rows]
    except Exception:
        return []


def _fetch_daily_irradiance(
    db: Session, table: str, plant_id: str, f_ts: str, t_ts: str
) -> List[Tuple[str, float, float]]:
    if not _is_pg(db):
        return []
    try:
        sql = text(sql_wms_irradiance_daily_sums(table))
        rows = db.execute(sql, {"plant_id": plant_id, "f": f_ts, "t": t_ts}).fetchall()
        return [( _day_key(r[0]), float(r[1] or 0), float(r[2] or 0)) for r in rows]
    except Exception:
        return []


def _fetch_plant_daily_gen(
    db: Session, table: str, plant_id: str, f_ts: str, t_ts: str
) -> Dict[str, float]:
    if not _is_pg(db):
        return {}
    try:
        sql = text(sql_plant_ac_daily_energy(table))
        rows = db.execute(
            sql, {"plant_id": plant_id, "from_ts": f_ts, "to_ts": t_ts}
        ).fetchall()
        return {_day_key(r[0]): float(r[1] or 0) for r in rows}
    except Exception:
        return {}


def _fetch_daily_plant_irr_avg_w_m2(
    db: Session, table: str, plant_id: str, f_ts: str, t_ts: str
) -> Dict[str, float]:
    """Daily average irradiance (W/m²) from plant/WMS gti+irradiance rows."""
    if not _is_pg(db):
        return {}
    try:
        sql = text(
            f"""
            SELECT DATE(timestamp)::text AS day, AVG(value::double precision) AS irr_avg
            FROM {table}
            WHERE plant_id = :plant_id
              AND LOWER(TRIM(equipment_level::text)) IN ('plant', 'wms')
              AND LOWER(TRIM(signal::text)) IN ('gti', 'irradiance')
              AND timestamp BETWEEN :f AND :t
            GROUP BY DATE(timestamp)
            ORDER BY day
            """
        )
        rows = db.execute(sql, {"plant_id": plant_id, "f": f_ts, "t": t_ts}).fetchall()
        return {str(r[0])[:10]: float(r[1] or 0) for r in rows}
    except Exception:
        return {}


def _fetch_daily_scb_current_avg(
    db: Session, table: str, plant_id: str, f_ts: str, t_ts: str
) -> List[Tuple[str, str, float]]:
    if not _is_pg(db):
        return []
    try:
        sql = text(
            f"""
            SELECT DATE(timestamp)::text AS day, equipment_id AS scb_id,
                   AVG(value::double precision) AS iavg
            FROM {table}
            WHERE plant_id = :plant_id
              AND equipment_level = 'scb'
              AND signal = 'dc_current'
              AND timestamp BETWEEN :f AND :t
            GROUP BY DATE(timestamp), equipment_id
            ORDER BY day, scb_id
            """
        )
        rows = db.execute(sql, {"plant_id": plant_id, "f": f_ts, "t": t_ts}).fetchall()
        return [(str(r[0])[:10], str(r[1]), float(r[2] or 0)) for r in rows]
    except Exception:
        return []


def _build_plant_pr_by_day(
    inv_energy_rows: List[Tuple[Any, str, float]],
    irr_rows: List[Tuple[str, float, float]],
    spec_dc: Dict[str, float],
    arch_dc: Dict[str, float],
) -> Tuple[List[str], List[Optional[float]], Dict[str, float]]:
    energy_by_day: Dict[str, Dict[str, float]] = defaultdict(dict)
    for day, inv, kwh in inv_energy_rows:
        dk = _day_key(day)
        energy_by_day[dk][inv] = float(kwh)

    irr_by_day: Dict[str, Tuple[float, float]] = {}
    for day, gti_s, irr_s in irr_rows:
        irr_by_day[day] = (gti_s, irr_s)

    all_days = sorted(set(energy_by_day.keys()) | set(irr_by_day.keys()))
    plant_pr: List[Optional[float]] = []
    h_by_day: Dict[str, float] = {}
    for day in all_days:
        gti_irr = irr_by_day.get(day, (0.0, 0.0))
        hd = gti_insolation_kwh_m2_from_sums(gti_irr[0], gti_irr[1])
        h_by_day[day] = hd
        prs: List[float] = []
        for inv, e_kwh in energy_by_day.get(day, {}).items():
            dc = arch_dc.get(inv) or spec_dc.get(inv)
            if not dc or float(dc) <= 0 or hd <= 0:
                continue
            yld = float(e_kwh) / float(dc)
            prs.append((yld / hd) * 100.0)
        plant_pr.append(sum(prs) / len(prs) if prs else None)
    return all_days, plant_pr, h_by_day


def build_plant_soiling_payload(db: Session, plant_id: str, date_from: str, date_to: str) -> Dict[str, Any]:
    f_ts = f"{date_from[:10]} 00:00:00"
    t_ts = f"{date_to[:10]} 23:59:59"
    table = choose_data_table(db, plant_id, date_from, date_to)
    spec_dc, arch_dc = _inverter_dc_maps(db, plant_id)
    plant_dc_kwp = _plant_dc_kwp(spec_dc, arch_dc)

    inv_rows = _fetch_daily_inv_energy(db, table, plant_id, f_ts, t_ts)
    irr_rows = _fetch_daily_irradiance(db, table, plant_id, f_ts, t_ts)
    gen_by_day = _fetch_plant_daily_gen(db, table, plant_id, f_ts, t_ts)

    plant = db.query(Plant).filter(Plant.plant_id == plant_id).first()
    ppa = float(plant.ppa_tariff) if plant and plant.ppa_tariff is not None else None

    all_days, plant_pr, h_by_day = _build_plant_pr_by_day(inv_rows, irr_rows, spec_dc, arch_dc)

    # Clamp to requested calendar range (SQL DATE()/timezone can add an adjacent day).
    lo, hi = date_from[:10], date_to[:10]
    _fd, _fp = [], []
    for d, p in zip(all_days, plant_pr):
        dk = str(d)[:10]
        if lo <= dk <= hi:
            _fd.append(d)
            _fp.append(p)
    all_days, plant_pr = _fd, _fp
    h_by_day = {k: v for k, v in h_by_day.items() if lo <= str(k)[:10] <= hi}

    series_chart = [{"date": d, "pr_pct": (round(p, 2) if p is not None else None)} for d, p in zip(all_days, plant_pr)]

    valid = [(d, p) for d, p in zip(all_days, plant_pr) if p is not None]
    soiling_loss_mwh = None
    soiling_rate_regression_pp_per_day = None
    soiling_rate_median_delta_pp = None
    generation_mwh = round(sum(gen_by_day.values()) / 1000.0, 4) if gen_by_day else 0.0
    revenue_loss_inr = None

    if len(valid) >= 2:
        days_v, pr_v = zip(*valid)
        pr_smooth = moving_median([float(x) for x in pr_v], 3)
        e_ref = [plant_dc_kwp * h_by_day.get(d, 0.0) for d in days_v]
        loss_kwh = soiling_loss_kwh_from_pr_steps(pr_smooth, e_ref)
        soiling_loss_mwh = round(loss_kwh / 1000.0, 4)
        soiling_rate_regression_pp_per_day = (
            round(float(linreg_slope_per_step(pr_smooth)), 6) if linreg_slope_per_step(pr_smooth) is not None else None
        )
        md = median_consecutive_delta(pr_smooth)
        soiling_rate_median_delta_pp = round(float(md), 6) if md is not None else None
        if ppa is not None and soiling_loss_mwh is not None:
            revenue_loss_inr = round(soiling_loss_mwh * 1000.0 * ppa, 2)

    top_scb_id = None
    top_scb_loss = None
    try:
        scb_rows = _fetch_daily_scb_current_avg(db, table, plant_id, f_ts, t_ts)
        _scb_dc = scb_dc_map(db, plant_id)
        scb_day_current: Dict[str, Dict[str, float]] = defaultdict(dict)
        for _day, _scb, _iavg in scb_rows:
            scb_day_current[_day][_scb] = _iavg
        _scb_ids = sorted(set(s for _, s, _ in scb_rows))
        scored_scbs: List[Tuple[str, float]] = []
        for _sid in _scb_ids:
            _lm = _scb_peer_loss_mwh(_sid, scb_day_current, h_by_day, _scb_dc)
            if _lm is not None:
                scored_scbs.append((_sid, _lm))
        scored_scbs.sort(key=lambda x: -x[1])
        if scored_scbs:
            top_scb_id = scored_scbs[0][0]
            top_scb_loss = scored_scbs[0][1]
    except Exception:
        pass

    return {
        "series": series_chart,
        "plant_dc_kwp": round(plant_dc_kwp, 3) if plant_dc_kwp else None,
        "soiling_rate_regression_pp_per_day": soiling_rate_regression_pp_per_day,
        "soiling_rate_median_delta_pp": soiling_rate_median_delta_pp,
        "soiling_loss_mwh": soiling_loss_mwh,
        "generation_mwh": generation_mwh,
        "ppa_tariff": ppa,
        "revenue_loss_inr": revenue_loss_inr,
        "top_soiling_scb_id": top_scb_id,
        "top_soiling_scb_loss_mwh": top_scb_loss,
        "data_hints": {
            "inverter_energy_rows": len(inv_rows),
            "irradiance_rows": len(irr_rows),
            "pr_days_computed": len(all_days),
            "pr_days_with_pr": len(valid),
        },
    }


def _inverter_soiling_loss_mwh(
    inverter_id: str,
    inv_energy_rows: List[Tuple[Any, str, float]],
    irr_rows: List[Tuple[str, float, float]],
    spec_dc: Dict[str, float],
    arch_dc: Dict[str, float],
) -> Optional[float]:
    dc = arch_dc.get(inverter_id) or spec_dc.get(inverter_id)
    if not dc or float(dc) <= 0:
        return None
    irr_by_day: Dict[str, Tuple[float, float]] = {}
    for day, gti_s, irr_s in irr_rows:
        irr_by_day[day] = (gti_s, irr_s)
    energy_by_day: Dict[str, float] = {}
    for day, inv, kwh in inv_energy_rows:
        if inv != inverter_id:
            continue
        energy_by_day[_day_key(day)] = float(kwh)
    all_days = sorted(set(energy_by_day.keys()) | set(irr_by_day.keys()))
    prs: List[Optional[float]] = []
    e_ref: List[float] = []
    for day in all_days:
        hd = gti_insolation_kwh_m2_from_sums(*irr_by_day.get(day, (0.0, 0.0)))
        ek = energy_by_day.get(day)
        if ek is None or hd <= 0:
            prs.append(None)
            e_ref.append(float(dc) * hd)
            continue
        yld = ek / float(dc)
        prs.append((yld / hd) * 100.0)
        e_ref.append(float(dc) * hd)
    valid = [(p, e) for p, e in zip(prs, e_ref) if p is not None]
    if len(valid) < 2:
        return None
    pr_v = [float(x[0]) for x in valid]
    e_v = [float(x[1]) for x in valid]
    pr_smooth = moving_median(pr_v, 3)
    loss_kwh = soiling_loss_kwh_from_pr_steps(pr_smooth, e_v)
    return round(loss_kwh / 1000.0, 4)


def _scb_peer_loss_mwh(
    scb_id: str,
    scb_day_current: Dict[str, Dict[str, float]],
    h_by_day: Dict[str, float],
    scb_dc: Dict[str, float],
) -> Optional[float]:
    dc = scb_dc.get(scb_id)
    if not dc or dc <= 0:
        return None
    days = sorted(scb_day_current.keys())
    loss_kwh = 0.0
    any_data = False
    for day in days:
        per = scb_day_current.get(day) or {}
        if scb_id not in per:
            continue
        vals = [per[s] for s in per if per[s] is not None]
        if not vals:
            continue
        med = float(statistics.median(vals))
        r_d = float(per[scb_id])
        hd = h_by_day.get(day, 0.0)
        if hd <= 0:
            continue
        any_data = True
        if med <= 0:
            continue
        short = max(0.0, (med - r_d) / med)
        loss_kwh += short * float(dc) * hd
    if not any_data:
        return None
    return round(loss_kwh / 1000.0, 4)


def build_soiling_rankings_payload(
    db: Session, plant_id: str, date_from: str, date_to: str, group_by: str
) -> Dict[str, Any]:
    f_ts = f"{date_from[:10]} 00:00:00"
    t_ts = f"{date_to[:10]} 23:59:59"
    table = choose_data_table(db, plant_id, date_from, date_to)
    spec_dc, arch_dc = _inverter_dc_maps(db, plant_id)
    inv_rows = _fetch_daily_inv_energy(db, table, plant_id, f_ts, t_ts)
    irr_rows = _fetch_daily_irradiance(db, table, plant_id, f_ts, t_ts)

    if group_by == "inverter":
        inv_ids = sorted(set(r[1] for r in inv_rows))
        scored: List[Tuple[str, float]] = []
        for inv in inv_ids:
            lm = _inverter_soiling_loss_mwh(inv, inv_rows, irr_rows, spec_dc, arch_dc)
            if lm is not None:
                scored.append((inv, lm))
        scored.sort(key=lambda x: -x[1])
        top = scored[:15]
        return {
            "group_by": "inverter",
            "rows": [{"id": i, "label": i, "loss_mwh": v} for i, v in top],
        }

    # SCB: peer-based loss
    scb_rows = _fetch_daily_scb_current_avg(db, table, plant_id, f_ts, t_ts)
    scb_dc = scb_dc_map(db, plant_id)
    _, _, h_by_day = _build_plant_pr_by_day(inv_rows, irr_rows, spec_dc, arch_dc)

    scb_day_current: Dict[str, Dict[str, float]] = defaultdict(dict)
    for day, scb, iavg in scb_rows:
        scb_day_current[day][scb] = iavg

    scb_ids = sorted(set(s for _, s, _ in scb_rows))
    scored2: List[Tuple[str, float]] = []
    for scb in scb_ids:
        lm = _scb_peer_loss_mwh(scb, scb_day_current, h_by_day, scb_dc)
        if lm is not None:
            scored2.append((scb, lm))
    scored2.sort(key=lambda x: -x[1])
    top2 = scored2[:15]
    return {
        "group_by": "scb",
        "rows": [{"id": i, "label": i, "loss_mwh": v} for i, v in top2],
    }


def build_scb_soiling_trend_payload(
    db: Session, plant_id: str, scb_id: str, date_from: str, date_to: str
) -> Dict[str, Any]:
    f_ts = f"{date_from[:10]} 00:00:00"
    t_ts = f"{date_to[:10]} 23:59:59"
    table = choose_data_table(db, plant_id, date_from, date_to)
    scb_dc_map_res = scb_dc_map(db, plant_id)
    dc_kw = scb_dc_map_res.get(scb_id)

    irr_daily = _fetch_daily_plant_irr_avg_w_m2(db, table, plant_id, f_ts, t_ts)
    scb_avgs = _fetch_daily_scb_current_avg(db, table, plant_id, f_ts, t_ts)
    by_day: Dict[str, float] = {}
    for day, sid, iavg in scb_avgs:
        if sid == scb_id:
            by_day[day] = iavg

    eps = 0.01
    ratios: List[float] = []
    series: List[Dict[str, Any]] = []
    for d in sorted(by_day.keys()):
        i_mean = by_day[d]
        irr_w = irr_daily.get(d) or 0.0
        denom = max((dc_kw or 1.0) * max(irr_w / 1000.0, eps), eps)
        ratio = float(i_mean) / denom
        ratios.append(ratio)
        series.append(
            {
                "date": d,
                "ratio": round(ratio, 6),
                "current_avg": round(i_mean, 4),
                "irr_avg_wm2": round(irr_w, 2),
                "scb_dc_kw": dc_kw,
            }
        )

    med_d, slope, _smooth = ratio_trend_stats(ratios, 3)
    return {
        "scb_id": scb_id,
        "scb_dc_kw": dc_kw,
        "series": series,
        "median_daily_slope_ratio": round(float(med_d), 8) if med_d is not None else None,
        "regression_slope_ratio_per_day": round(float(slope), 8) if slope is not None else None,
    }


def fetch_hourly_plant_irradiance_w_m2(
    db: Session, table: str, plant_id: str, f_ts: str, t_ts: str
) -> Dict[Any, float]:
    """Bucket (datetime) -> average W/m² (PostgreSQL only; app DB is PG-only)."""
    if not _is_pg(db):
        return {}
    try:
        sql = text(
            f"""
            SELECT date_trunc('hour', timestamp) AS bucket,
                   AVG(value::double precision) AS irr_avg
            FROM {table}
            WHERE plant_id = :plant_id
              AND LOWER(TRIM(equipment_level::text)) IN ('plant', 'wms')
              AND LOWER(TRIM(signal::text)) IN ('gti', 'irradiance')
              AND timestamp BETWEEN :f AND :t
            GROUP BY date_trunc('hour', timestamp)
            ORDER BY bucket
            """
        )
        rows = db.execute(sql, {"plant_id": plant_id, "f": f_ts, "t": t_ts}).fetchall()
        return {r[0]: float(r[1] or 0) for r in rows}
    except Exception:
        return {}
