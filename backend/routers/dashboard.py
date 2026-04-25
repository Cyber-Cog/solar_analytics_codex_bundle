"""
backend/routers/dashboard.py
=============================
Dashboard API endpoints — station details, energy, weather, KPIs,
inverter performance table, active power vs GTI, and loss waterfall.

All queries run against raw_data_generic and plant tables.
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Any, Dict, List, Optional
from datetime import datetime, date
import math
import logging

logger = logging.getLogger(__name__)

from database import get_db, get_read_db, SessionLocal, ReadSessionLocal
from db_perf import choose_data_table
from models import Plant, User, EquipmentSpec
from schemas import (
    StationDetails, EnergyDataPoint, WeatherDataPoint,
    KPIData, WMSKPIData, InverterRow, PowerVsGTIPoint,
    LossWaterfallInput, LossWaterfallPoint,
)
from auth.routes import get_current_user
from dashboard_cache import get as cache_get, set as cache_set
from fault_cache import (
    get_cached as fault_cache_get,
    set_cached as fault_cache_set,
    cache_key_loss_gen_snapshot,
    TTL_LOSS_GEN_SNAPSHOT_MIN,
)
from ac_power_energy_sql import (
    sql_plant_ac_totals,
    sql_plant_ac_daily_energy,
    sql_inverter_performance_with_energy,
)
from dashboard_helpers import (
    resolve_dashboard_date_range,
)
from dashboard_mv_sql import (
    sql_mv_inverter_performance,
    sql_mv_weather_timeline,
    sql_mv_power_vs_gti,
    sql_mv_plant_ac_daily_energy,
)
import time

router = APIRouter(prefix="/api/dashboard", tags=["Dashboard"])

# WMS insolation from summed irradiance (W/m²): assumes **1-minute** samples.
# kWh/m² = Σ P_W/m² × (1/60 h) / 1000 = Σ P / 60000
def _wms_float(x) -> Optional[float]:
    if x is None:
        return None
    try:
        f = float(x)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (TypeError, ValueError):
        return None


def _wms_kpis_payload(db: Session, table: str, plant_id: str, f_ts: str, t_ts: str) -> dict:
    """
    WMS KPIs (1‑minute samples for insolation divisor 60000):
    - Insolation GHI (kWh/m²) = SUM(GHI W/m²) / 60000
    - Insolation GTI (kWh/m²) = SUM(GTI W/m²) / 60000; if no `gti` rows, uses SUM(`irradiance`) (legacy ingest)
    - Irradiance horizontal (W/m²) = AVG(all GHI samples)
    - Irradiance tilt (W/m²) = AVG(all GTI samples); if none, AVG(`irradiance`)
    """
    wms_where = (
        "r.plant_id = :plant_id "
        "AND LOWER(TRIM(r.equipment_level::text)) IN ('plant', 'wms') "
        "AND r.timestamp BETWEEN :f AND :t"
    )
    agg_sql = text(f"""
        SELECT
            (
                SELECT COALESCE(SUM(r.value), 0)::double precision
                FROM {table} r
                WHERE {wms_where}
                  AND LOWER(TRIM(r.signal::text)) IN ('ghi')
            ) AS ghi_sum,
            (
                SELECT COALESCE(SUM(r.value), 0)::double precision
                FROM {table} r
                WHERE {wms_where}
                  AND LOWER(TRIM(r.signal::text)) IN ('gti')
            ) AS gti_sum,
            (
                SELECT AVG(r.value)
                FROM {table} r
                WHERE {wms_where}
                  AND LOWER(TRIM(r.signal::text)) IN ('ghi')
                  AND r.value IS NOT NULL
            ) AS ghi_avg,
            (
                SELECT AVG(r.value)
                FROM {table} r
                WHERE {wms_where}
                  AND LOWER(TRIM(r.signal::text)) IN ('gti')
                  AND r.value IS NOT NULL
            ) AS gti_avg,
            (
                SELECT AVG(r.value)
                FROM {table} r
                WHERE {wms_where}
                  AND LOWER(TRIM(r.signal::text)) IN ('irradiance')
                  AND r.value IS NOT NULL
            ) AS irr_avg
    """)
    agg = db.execute(agg_sql, {"plant_id": plant_id, "f": f_ts, "t": t_ts}).fetchone()

    wms_sql = text(f"""
        SELECT signal,
               AVG(value) as avg_val,
               SUM(value) as total_val
        FROM {table}
        WHERE plant_id = :plant_id
          AND LOWER(TRIM(equipment_level::text)) IN ('plant', 'wms')
          AND timestamp BETWEEN :f AND :t
        GROUP BY signal
    """)
    res = db.execute(wms_sql, {"plant_id": plant_id, "f": f_ts, "t": t_ts}).fetchall()
    avgs_ci: Dict[str, Any] = {}
    for r in res:
        if r.signal is None:
            continue
        key = str(r.signal).strip().lower()
        # last wins; include 0.0 averages (valid temperature / wind)
        avgs_ci[key] = r.avg_val

    def _wms_pick_avg(*names: str) -> Optional[float]:
        for n in names:
            v = avgs_ci.get(n.lower())
            if v is not None:
                return _wms_float(v)
        return None

    ins = _wms_integrated_insolation_kwh_m2(db, table, plant_id, f_ts, t_ts)
    ghi_kwh_m2 = float(ins.get("ghi_kwh_m2") or 0.0)
    gti_kwh_m2 = float(ins.get("gti_kwh_m2") or 0.0)
    irr_kwh_m2 = float(ins.get("irr_kwh_m2") or 0.0)
    tilt_kwh_m2 = gti_kwh_m2 if gti_kwh_m2 > 0 else irr_kwh_m2

    ghi_avg = _wms_float(agg.ghi_avg) if agg else None
    gti_avg = _wms_float(agg.gti_avg) if agg else None
    irr_avg = _wms_float(agg.irr_avg) if agg else None
    # Tilt average: strict GTI; fallback to `irradiance` when no gti rows (same as insolation fallback)
    tilt_avg = gti_avg if gti_avg is not None else irr_avg

    return {
        "ghi": round(ghi_kwh_m2, 2) if ghi_kwh_m2 else 0.0,
        "gti": round(tilt_kwh_m2, 2) if tilt_kwh_m2 else 0.0,
        "irradiance_horizontal": round(ghi_avg, 2) if ghi_avg is not None else 0.0,
        "irradiance_tilt": round(tilt_avg, 2) if tilt_avg is not None else 0.0,
        "ambient_temp": round(x, 1)
        if (x := _wms_pick_avg(
            "ambient_temp",
            "temperature",
            "amb_temp",
            "ambienttemperature",
            "air_temp",
            "temp_ambient",
            "oat",
        ))
        is not None
        else 0.0,
        "module_temp": round(x, 1)
        if (x := _wms_pick_avg("module_temp", "mod_temp", "moduletemperature", "cell_temp", "backsheet_temp"))
        is not None
        else (round(y, 1) if (y := _wms_pick_avg("ambient_temp", "temperature", "amb_temp")) is not None else 0.0),
        "wind_speed": round(x, 1)
        if (x := _wms_pick_avg("wind_speed", "windspeed", "wind", "wind_speed_ms", "ws"))
        is not None
        else 0.0,
        "rainfall_mm": round(
            _wms_float(rain_row.s) or 0.0,
            2,
        )
        if (
            rain_row := db.execute(
                text(
                    f"""
                    SELECT COALESCE(SUM(r.value), 0)::double precision AS s
                    FROM {table} r
                    WHERE r.plant_id = :plant_id
                      AND LOWER(TRIM(r.equipment_level::text)) IN ('plant', 'wms')
                      AND r.timestamp BETWEEN :f AND :t
                      AND LOWER(TRIM(r.signal::text)) IN (
                        'rainfall', 'rain', 'precipitation', 'rain_mm', 'precipitation_mm',
                        'rain_gauge', 'rainfall_mm', 'rainfall_accum', 'precip', 'prcp'
                      )
                    """
                ),
                {"plant_id": plant_id, "f": f_ts, "t": t_ts},
            ).fetchone()
        )
        is not None
        else 0.0,
    }


def _wms_integrated_insolation_kwh_m2(
    db: Session, table: str, plant_id: str, f_ts: str, t_ts: str
) -> Dict[str, float]:
    """
    Cadence-aware insolation integration from WMS/plant irradiance signals.
    Works for 1-min, 5-min, 15-min, and irregular data by integrating value * dt.
    """
    row = db.execute(
        text(
            f"""
            WITH samples AS (
                SELECT
                    CAST(r.timestamp AS TIMESTAMP) AS ts,
                    LOWER(TRIM(r.signal::text)) AS sig,
                    AVG(r.value)::double precision AS v
                FROM {table} r
                WHERE r.plant_id = :plant_id
                  AND LOWER(TRIM(r.equipment_level::text)) IN ('plant', 'wms')
                  AND LOWER(TRIM(r.signal::text)) IN ('ghi', 'gti', 'irradiance')
                  AND r.timestamp BETWEEN :f AND :t
                GROUP BY CAST(r.timestamp AS TIMESTAMP), LOWER(TRIM(r.signal::text))
            ),
            gapped AS (
                SELECT
                    sig,
                    v,
                    EXTRACT(EPOCH FROM (
                        LEAD(ts) OVER (PARTITION BY sig ORDER BY ts) - ts
                    )) / 3600.0 AS dt_h
                FROM samples
            ),
            step_median AS (
                SELECT
                    sig,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dt_h) AS m
                FROM gapped
                WHERE dt_h IS NOT NULL AND dt_h > 0 AND dt_h <= 6.0
                GROUP BY sig
            ),
            integrated AS (
                SELECT
                    g.sig,
                    SUM(
                        g.v * (
                            CASE
                                WHEN g.dt_h IS NULL OR g.dt_h <= 0 THEN COALESCE(sm.m, 1.0 / 60.0)
                                ELSE LEAST(
                                    g.dt_h,
                                    GREATEST(8.0 * COALESCE(sm.m, 1.0 / 60.0), 1.0 / 60.0)
                                )
                            END
                        ) / 1000.0
                    ) AS kwh_m2
                FROM gapped g
                LEFT JOIN step_median sm ON sm.sig = g.sig
                GROUP BY g.sig
            )
            SELECT
                COALESCE(MAX(CASE WHEN sig = 'ghi' THEN kwh_m2 END), 0)::double precision AS ghi_kwh_m2,
                COALESCE(MAX(CASE WHEN sig = 'gti' THEN kwh_m2 END), 0)::double precision AS gti_kwh_m2,
                COALESCE(MAX(CASE WHEN sig = 'irradiance' THEN kwh_m2 END), 0)::double precision AS irr_kwh_m2
            FROM integrated
            """
        ),
        {"plant_id": plant_id, "f": f_ts, "t": t_ts},
    ).fetchone()
    if not row:
        return {"ghi_kwh_m2": 0.0, "gti_kwh_m2": 0.0, "irr_kwh_m2": 0.0}
    return {
        "ghi_kwh_m2": float(row.ghi_kwh_m2 or 0.0),
        "gti_kwh_m2": float(row.gti_kwh_m2 or 0.0),
        "irr_kwh_m2": float(row.irr_kwh_m2 or 0.0),
    }


def _wms_tilt_insolation_kwh_m2(
    db: Session, table: str, plant_id: str, f_ts: str, t_ts: str
) -> float:
    """Tilt-plane insolation using GTI when present, else legacy irradiance."""
    ins = _wms_integrated_insolation_kwh_m2(db, table, plant_id, f_ts, t_ts)
    gti_kwh_m2 = float(ins.get("gti_kwh_m2") or 0.0)
    irr_kwh_m2 = float(ins.get("irr_kwh_m2") or 0.0)
    return gti_kwh_m2 if gti_kwh_m2 > 0 else irr_kwh_m2


def _wms_gti_irradiance_sums(
    db: Session, table: str, plant_id: str, f_ts: str, t_ts: str
) -> tuple[float, float]:
    """Case-insensitive sums of GTI and legacy irradiance (WMS / plant level) for insolation math."""
    row = db.execute(
        text(
            f"""
            SELECT
                COALESCE(SUM(CASE WHEN LOWER(TRIM(r.signal::text)) IN ('gti') THEN r.value ELSE 0 END), 0)::double precision AS gti_sum,
                COALESCE(SUM(CASE WHEN LOWER(TRIM(r.signal::text)) IN ('irradiance') THEN r.value ELSE 0 END), 0)::double precision AS irr_sum
            FROM {table} r
            WHERE r.plant_id = :plant_id
              AND LOWER(TRIM(r.equipment_level::text)) IN ('plant', 'wms')
              AND r.timestamp BETWEEN :f AND :t
            """
        ),
        {"plant_id": plant_id, "f": f_ts, "t": t_ts},
    ).fetchone()
    if not row:
        return 0.0, 0.0
    return float(row.gti_sum or 0), float(row.irr_sum or 0)


def _range_days_inclusive(date_from: str, date_to: str) -> int:
    try:
        d0 = datetime.strptime(date_from[:10], "%Y-%m-%d").date()
        d1 = datetime.strptime(date_to[:10], "%Y-%m-%d").date()
        return max(1, (d1 - d0).days + 1)
    except Exception:
        return 1


def _inverter_dc_maps(db: Session, plant_id: str) -> tuple[Dict[str, float], Dict[str, float]]:
    spec_dc: Dict[str, float] = {}
    inv_specs = db.query(EquipmentSpec).filter(
        EquipmentSpec.plant_id == plant_id,
        EquipmentSpec.equipment_type == "inverter",
    ).all()
    for spec in inv_specs:
        if spec.equipment_id and spec.dc_capacity_kwp is not None:
            spec_dc[str(spec.equipment_id)] = float(spec.dc_capacity_kwp)

    arch_rows = db.execute(
        text(
            """
            SELECT inverter_id, SUM(dc_capacity_kw) AS dc_capacity_kw
            FROM plant_architecture
            WHERE plant_id = :p
              AND inverter_id IS NOT NULL
            GROUP BY inverter_id
            """
        ),
        {"p": plant_id},
    ).fetchall()
    arch_dc: Dict[str, float] = {}
    for row in arch_rows:
        if row[0] is not None and row[1] is not None:
            arch_dc[str(row[0])] = float(row[1])
    return spec_dc, arch_dc


def _plant_dc_kwp_from_inverters(spec_dc: Dict[str, float], arch_dc: Dict[str, float]) -> Optional[float]:
    keys = set(spec_dc.keys()) | set(arch_dc.keys())
    if not keys:
        return None
    total = 0.0
    for key in keys:
        total += float(arch_dc.get(key) or spec_dc.get(key) or 0.0)
    return total if total > 0 else None


def _plant_pr_pct(total_kwh: Optional[float], plant_dc_kwp: Optional[float], insolation_kwh_m2_raw: float) -> Optional[float]:
    if total_kwh is None or not plant_dc_kwp or plant_dc_kwp <= 0:
        return None
    if not insolation_kwh_m2_raw or insolation_kwh_m2_raw <= 0:
        return None
    return round((float(total_kwh) / float(plant_dc_kwp) / float(insolation_kwh_m2_raw)) * 100, 1)


def _inverter_performance_table(
    db: Session,
    table: str,
    plant_id: str,
    f_ts: str,
    t_ts: str,
    date_from: str,
    date_to: str,
) -> List[Dict[str, Any]]:
    """
    Per-inverter yield, PR, PLF.
    PR uses plant tilt insolation (GTI + legacy irradiance fallback), ΣW/m²/60000 (1‑minute sampling model).
    PLF = inverter energy / (dc_kWp × 24 h × days) × 100.
    """
    days = _range_days_inclusive(date_from, date_to)
    insolation_kwh_m2 = _wms_tilt_insolation_kwh_m2(db, table, plant_id, f_ts, t_ts)

    sql = text(sql_inverter_performance_with_energy(table))
    rows = db.execute(sql, {"plant_id": plant_id, "f": f_ts, "t": t_ts}).fetchall()

    spec_dc_cap, _ = _inverter_dc_maps(db, plant_id)

    out: List[Dict[str, Any]] = []
    for r in rows:
        eff = None
        if r.dc_power and r.ac_power and r.dc_power > 0:
            eff = round((float(r.ac_power) / float(r.dc_power)) * 100, 1)

        dc_cap = r.dc_cap_kw or spec_dc_cap.get(r.equipment_id)
        yld, pr_pct, plf_pct = None, None, None
        if r.energy_kwh and dc_cap and float(dc_cap) > 0:
            e = float(r.energy_kwh)
            c = float(dc_cap)
            raw_yld = e / c
            yld = round(raw_yld, 2)
            if insolation_kwh_m2 and insolation_kwh_m2 > 0:
                pr_pct = round((raw_yld / insolation_kwh_m2) * 100, 1)
            denom = c * 24 * days
            if denom > 0:
                plf_pct = round((e / denom) * 100, 1)

        out.append(
            {
                "inverter_id": r.equipment_id,
                "dc_power_kw": round(float(r.dc_power), 2) if r.dc_power is not None else None,
                "ac_power_kw": round(float(r.ac_power), 2) if r.ac_power is not None else None,
                "generation_kwh": round(float(r.energy_kwh), 2) if r.energy_kwh is not None else None,
                "dc_capacity_kwp": round(float(dc_cap), 2) if dc_cap is not None else None,
                "efficiency_pct": eff,
                "yield_kwh_kwp": yld,
                "pr_pct": pr_pct,
                "plf_pct": plf_pct,
            }
        )
    return out


def finalize_inverter_rows(
    db: Session,
    plant_id: str,
    raw_rows: List[Dict[str, Any]],
    date_from: str,
    date_to: str,
) -> List[Dict[str, Any]]:
    """
    Map MV `sql_mv_inverter_performance` row dicts to the same structure as
    _inverter_performance_table (used by /inverter-performance and /bundle).
    """
    table = choose_data_table(db, plant_id, date_from, date_to)
    f_lo = f"{date_from} 00:00:00"
    t_hi = f"{date_to} 23:59:59"
    days = _range_days_inclusive(date_from, date_to)
    insolation_kwh_m2 = _wms_tilt_insolation_kwh_m2(db, table, plant_id, f_lo, t_hi)
    spec_dc_cap, _ = _inverter_dc_maps(db, plant_id)
    out: List[Dict[str, Any]] = []
    for r in raw_rows:
        dcp = r.get("dc_power")
        acp = r.get("ac_power")
        e_kwh = r.get("energy_kwh")
        eid = r.get("inverter_id")
        eff = None
        if dcp is not None and acp is not None and float(dcp) > 0:
            eff = round((float(acp) / float(dcp)) * 100, 1)
        dc_cap = r.get("dc_capacity_kw")
        if dc_cap is None and eid is not None:
            dc_cap = spec_dc_cap.get(eid)
        yld, pr_pct, plf_pct = None, None, None
        if e_kwh is not None and dc_cap is not None and float(dc_cap) > 0:
            e = float(e_kwh)
            c = float(dc_cap)
            raw_yld = e / c
            yld = round(raw_yld, 2)
            if insolation_kwh_m2 and insolation_kwh_m2 > 0:
                pr_pct = round((raw_yld / insolation_kwh_m2) * 100, 1)
            denom = c * 24 * days
            if denom > 0:
                plf_pct = round((e / denom) * 100, 1)
        out.append(
            {
                "inverter_id": eid,
                "dc_power_kw": round(float(dcp), 2) if dcp is not None else None,
                "ac_power_kw": round(float(acp), 2) if acp is not None else None,
                "generation_kwh": round(float(e_kwh), 2) if e_kwh is not None else None,
                "dc_capacity_kwp": round(float(dc_cap), 2) if dc_cap is not None else None,
                "efficiency_pct": eff,
                "yield_kwh_kwp": yld,
                "pr_pct": pr_pct,
                "plf_pct": plf_pct,
            }
        )
    return out


def _average_pr_from_inverter_table(inv: List[Dict[str, Any]]) -> Optional[float]:
    vals = [r["pr_pct"] for r in inv if r.get("pr_pct") is not None]
    if not vals:
        return None
    return round(sum(vals) / len(vals), 1)


# ── Station Details ───────────────────────────────────────────────────────────
@router.get("/station-details", response_model=StationDetails)
def station_details(
    plant_id: str = Query(...),
    db: Session = Depends(get_read_db),
    current_user: User = Depends(get_current_user),
):
    """Return static plant metadata for the Station Details card."""
    cached = cache_get("station_v1", plant_id, "", "")
    if cached is not None:
        return StationDetails(**cached)

    plant = db.query(Plant).filter(Plant.plant_id == plant_id).first()
    if not plant:
        payload = {
            "plant_id": plant_id,
            "name": plant_id,
            "technology": "Solar PV",
            "status": "Unknown",
            "capacity_mwp": None,
            "cod_date": None,
            "ppa_tariff": None,
            "plant_age_years": None,
            "location": None,
        }
        cache_set("station_v1", plant_id, "", "", payload)
        return StationDetails(**payload)

    age = None
    if plant.cod_date:
        try:
            cod = datetime.strptime(plant.cod_date[:10], "%Y-%m-%d").date()
            age = round((date.today() - cod).days / 365.25, 1)
        except Exception:
            pass

    payload = {
        "plant_id": plant.plant_id,
        "name": plant.name,
        "technology": plant.technology or "Solar PV",
        "status": plant.status or "Active",
        "capacity_mwp": plant.capacity_mwp,
        "cod_date": plant.cod_date,
        "ppa_tariff": plant.ppa_tariff,
        "plant_age_years": age,
        "location": plant.location,
    }
    cache_set("station_v1", plant_id, "", "", payload)
    return StationDetails(**payload)


# ── Energy Generation (Bar Chart) ────────────────────────────────────────────
@router.get("/energy", response_model=List[EnergyDataPoint])
def energy_generation(
    plant_id: str = Query(...),
    date_from: str = Query(default=None),
    date_to: str   = Query(default=None),
    db: Session    = Depends(get_read_db),
    current_user: User = Depends(get_current_user),
):
    """Daily actual energy generation compared to a simple capacity-based target."""
    _from, _to = _default_range(date_from, date_to)
    cached = cache_get("energy_v1", plant_id, _from, _to)
    if cached is not None:
        return cached

    start_time = time.time()
    f_ts, t_ts = f"{_from} 00:00:00", f"{_to} 23:59:59"
    
    # Try Materialized View first
    view_exists = db.execute(text("SELECT 1 FROM pg_matviews WHERE matviewname = 'mv_inverter_power_1min'")).fetchone()
    if view_exists:
        sql = text(sql_mv_plant_ac_daily_energy())
        rows = db.execute(sql, {"plant_id": plant_id, "from_ts": f_ts, "to_ts": t_ts}).fetchall()
        logger.info(f"Dashboard /energy hit Materialized View in {time.time() - start_time:.4f}s")
    else:
        table = choose_data_table(db, plant_id, _from, _to)
        sql = text(sql_plant_ac_daily_energy(table))
        rows = db.execute(sql, {"plant_id": plant_id, "from_ts": f_ts, "to_ts": t_ts}).fetchall()
        logger.info(f"Dashboard /energy hit {table} in {time.time() - start_time:.4f}s")

    plant = db.query(Plant).filter(Plant.plant_id == plant_id).first()
    cap   = (plant.capacity_mwp * 1000) if (plant and plant.capacity_mwp) else None

    result = []
    for r in rows:
        target_kwh = round(cap * 4.5, 1) if cap else None   # simple CUF-based target per day (kWh)
        actual_kwh = round(float(r.actual_kwh), 1) if r.actual_kwh is not None else None
        result.append(EnergyDataPoint(
            date       = _as_json_str(r.day),
            actual_kwh = actual_kwh,
            target_kwh = target_kwh,
            actual_mwh = round(actual_kwh / 1000, 3) if actual_kwh is not None else None,
            target_mwh = round(target_kwh / 1000, 3) if target_kwh is not None else None,
        ))
    
    # Store plain dictionaries in the cache
    payload = [r.dict() for r in result]
    cache_set("energy_v1", plant_id, _from, _to, payload)
    return result


# ── Weather Data ──────────────────────────────────────────────────────────────
@router.get("/weather", response_model=List[WeatherDataPoint])
def weather_data(
    plant_id: str  = Query(...),
    date_from: str = Query(default=None),
    date_to: str   = Query(default=None),
    db: Session    = Depends(get_read_db),
    current_user: User = Depends(get_current_user),
):
    """Return WMS weather time-series for the selected plant."""
    _from, _to = _default_range(date_from, date_to)
    cached = cache_get("weather_v1", plant_id, _from, _to)
    if cached is not None:
        return cached

    start_time = time.time()
    
    view_exists = db.execute(text("SELECT 1 FROM pg_matviews WHERE matviewname = 'mv_weather_1min'")).fetchone()
    if view_exists:
        sql = text(sql_mv_weather_timeline())
        rows = db.execute(sql, {"plant_id": plant_id, "f": f"{_from} 00:00:00", "t": f"{_to} 23:59:59"}).fetchall()
        logger.info(f"Dashboard /weather hit Materialized View in {time.time() - start_time:.4f}s")
        return [
            WeatherDataPoint(
                timestamp    = _as_json_str(r.timestamp),
                ghi          = r.ghi,
                gti          = r.gti,
                ambient_temp = r.ambient_temp,
                module_temp  = r.module_temp,
                wind_speed   = r.wind_speed,
            )
            for r in rows
        ]

    table = choose_data_table(db, plant_id, _from, _to)
    logger.info(f"Dashboard /weather fallback to {table}")

    sql = text(f"""
        SELECT timestamp,
               MAX(CASE WHEN LOWER(TRIM(signal::text)) = 'ghi'          THEN value END) as ghi,
               MAX(CASE WHEN LOWER(TRIM(signal::text)) = 'gti'          THEN value END) as gti,
               MAX(CASE WHEN LOWER(TRIM(signal::text)) = 'irradiance'   THEN value END) as irradiance,
               MAX(CASE WHEN LOWER(TRIM(signal::text)) = 'ambient_temp' THEN value END) as ambient_temp,
               MAX(CASE WHEN LOWER(TRIM(signal::text)) = 'module_temp'  THEN value END) as module_temp,
               MAX(CASE WHEN LOWER(TRIM(signal::text)) = 'wind_speed'   THEN value END) as wind_speed
        FROM {table}
        WHERE plant_id = :plant_id
          AND LOWER(TRIM(equipment_level::text)) IN ('plant', 'wms')
          AND timestamp BETWEEN :f AND :t
        GROUP BY timestamp
        ORDER BY timestamp
        LIMIT 500
    """)
    rows = db.execute(sql, {"plant_id": plant_id, "f": f"{_from} 00:00:00", "t": f"{_to} 23:59:59"}).fetchall()
    return [
        WeatherDataPoint(
            timestamp    = _as_json_str(r.timestamp),
            ghi          = r.ghi,
            gti          = r.gti if r.gti is not None else getattr(r, "irradiance", None),
            ambient_temp = r.ambient_temp,
            module_temp  = r.module_temp,
            wind_speed   = r.wind_speed,
        )
        for r in rows
    ]


# ── KPIs ──────────────────────────────────────────────────────────────────────
@router.get("/kpis", response_model=KPIData)
def kpis(
    plant_id: str  = Query(...),
    date_from: str = Query(default=None),
    date_to: str   = Query(default=None),
    db: Session    = Depends(get_read_db),
    current_user: User = Depends(get_current_user),
):
    """Return aggregate KPI values for the selected date range."""
    _from, _to  = _default_range(date_from, date_to)
    cached = cache_get("kpis_v1", plant_id, _from, _to)
    if cached is not None:
        return cached

    f_ts, t_ts = f"{_from} 00:00:00", f"{_to} 23:59:59"
    table = choose_data_table(db, plant_id, _from, _to)

    ac_sql = text(sql_plant_ac_totals(table))
    ac = db.execute(ac_sql, {"plant_id": plant_id, "f": f_ts, "t": t_ts}).fetchone()

    plant   = db.query(Plant).filter(Plant.plant_id == plant_id).first()
    cap_kw  = (plant.capacity_mwp * 1000) if (plant and plant.capacity_mwp) else None
    spec_dc_map, arch_dc_map = _inverter_dc_maps(db, plant_id)
    plant_dc_kwp = _plant_dc_kwp_from_inverters(spec_dc_map, arch_dc_map)

    total_kwh = round(ac.total_kwh, 1) if ac and ac.total_kwh else None
    peak_kw   = round(ac.peak_kw, 1)  if ac and ac.peak_kw  else None
    avg_kw    = round(ac.avg_kw, 2)   if ac and ac.avg_kw   else None

    days = max(1, (datetime.strptime(_to[:10], "%Y-%m-%d") - datetime.strptime(_from[:10], "%Y-%m-%d")).days + 1)

    # Insolation (GTI) kWh/m² — same model as WMS + inverter PR (Σ W/m² / 60000, irradiance fallback)
    _ins_raw = _wms_tilt_insolation_kwh_m2(db, table, plant_id, f_ts, t_ts)
    insolation_kwh_m2 = round(_ins_raw, 2) if _ins_raw > 0 else None

    # Plant KPI PR must use plant-level generation / plant DC / insolation.
    pr = _plant_pr_pct(total_kwh, plant_dc_kwp, _ins_raw)

    # Plant PLF = plant generation / (plant DC kWp × 24 hours × days) × 100.
    plf = None
    plf_cap_kw = plant_dc_kwp or cap_kw
    if total_kwh and plf_cap_kw and days and (plf_cap_kw * 24 * days) > 0:
        plf = round((total_kwh / (plf_cap_kw * 24 * days)) * 100, 1)

    total_mwh = round(total_kwh / 1000, 2) if total_kwh else None

    result = KPIData(
        energy_export_kwh              = total_kwh,
        net_generation_kwh             = total_kwh,
        energy_export_mwh               = total_mwh,
        net_generation_mwh             = total_mwh,
        total_inverter_generation_mwh  = total_mwh,
        active_power_kw                = avg_kw,
        peak_power_kw                  = peak_kw,
        performance_ratio              = pr,
        plant_load_factor              = plf,
        total_inverter_generation_kwh  = total_kwh,
        insolation_kwh_m2              = insolation_kwh_m2,
    )
    cache_set("kpis_v1", plant_id, _from, _to, result.dict())
    return result


# ── WMS KPIs ──────────────────────────────────────────────────────────────────
@router.get("/wms-kpis", response_model=WMSKPIData)
def wms_kpis(
    plant_id: str  = Query(...),
    date_from: str = Query(default=None),
    date_to: str   = Query(default=None),
    db: Session    = Depends(get_read_db),
    current_user: User = Depends(get_current_user),
):
    """WMS KPIs: insolation kWh/m² (Σ W/m² / 60000 for 1-min data), mean irradiance W/m², temps, wind."""
    _from, _to = _default_range(date_from, date_to)
    cached = cache_get("wmskpis_v1", plant_id, _from, _to)
    if cached is not None:
        return cached

    table = choose_data_table(db, plant_id, _from, _to)
    f_ts, t_ts = f"{_from} 00:00:00", f"{_to} 23:59:59"
    payload = _wms_kpis_payload(db, table, plant_id, f_ts, t_ts)
    cache_set("wmskpis_v1", plant_id, _from, _to, payload)
    return WMSKPIData(**payload)


# ── Inverter Performance Table ────────────────────────────────────────────────
@router.get("/inverter-performance", response_model=List[InverterRow])
def inverter_performance(
    plant_id: str  = Query(...),
    date_from: str = Query(default=None),
    date_to: str   = Query(default=None),
    db: Session    = Depends(get_read_db),
    current_user: User = Depends(get_current_user),
):
    """Return average DC/AC power per inverter for the date range (+ PLF)."""
    _from, _to = _default_range(date_from, date_to)
    cached = cache_get("invperf_v1", plant_id, _from, _to)
    if cached is not None:
        return cached

    start_time = time.time()
    f_ts, t_ts = f"{_from} 00:00:00", f"{_to} 23:59:59"

    view_exists = db.execute(text("SELECT 1 FROM pg_matviews WHERE matviewname = 'mv_inverter_power_1min'")).fetchone()
    if view_exists:
        sql = text(sql_mv_inverter_performance(plant_id))
        rows_raw = db.execute(sql, {"plant_id": plant_id, "f": f_ts, "t": t_ts}).fetchall()
        # Convert Row objects to dicts for helper
        rows = []
        for r in rows_raw:
            rows.append({
                "inverter_id": r.equipment_id,
                "dc_power": r.dc_power,
                "ac_power": r.ac_power,
                "energy_kwh": r.energy_kwh,
                "dc_capacity_kw": r.dc_cap_kw
            })
        rows = finalize_inverter_rows(db, plant_id, rows, _from, _to)
        logger.info(f"Dashboard /inverter-performance hit Materialized View in {time.time() - start_time:.4f}s")
    else:
        table = choose_data_table(db, plant_id, _from, _to)
        rows = _inverter_performance_table(db, table, plant_id, f_ts, t_ts, _from, _to)
        logger.info(f"Dashboard /inverter-performance hit {table} in {time.time() - start_time:.4f}s")
    result = [
        InverterRow(
            inverter_id=r["inverter_id"],
            dc_power_kw=r.get("dc_power_kw"),
            ac_power_kw=r.get("ac_power_kw"),
            generation_kwh=r.get("generation_kwh"),
            dc_capacity_kwp=r.get("dc_capacity_kwp"),
            efficiency_pct=r.get("efficiency_pct"),
            yield_kwh_kwp=r.get("yield_kwh_kwp"),
            pr_pct=r.get("pr_pct"),
            plf_pct=r.get("plf_pct"),
        )
        for r in rows
    ]
    payload = [r.dict() for r in result]
    cache_set("invperf_v1", plant_id, _from, _to, payload)
    return result


# ── Active Power vs GTI ───────────────────────────────────────────────────────
@router.get("/power-vs-gti", response_model=List[PowerVsGTIPoint])
def power_vs_gti(
    plant_id: str  = Query(...),
    date_from: str = Query(default=None),
    date_to: str   = Query(default=None),
    db: Session    = Depends(get_read_db),
    current_user: User = Depends(get_current_user),
):
    """Return time-series of total active power and GTI for charting (full selected date span)."""
    _from, _to = _default_range(date_from, date_to)
    start_time = time.time()
    lim = _power_vs_gti_row_limit(_from, _to)

    view_exists = db.execute(text("SELECT 1 FROM pg_matviews WHERE matviewname = 'mv_inverter_power_1min'")).fetchone()
    if view_exists:
        sql = text(sql_mv_power_vs_gti())
        rows = db.execute(sql, {"plant_id": plant_id, "f": f"{_from} 00:00:00", "t": f"{_to} 23:59:59", "limit": lim}).fetchall()
        logger.info(f"Dashboard /power-vs-gti hit Materialized View in {time.time() - start_time:.4f}s")
    else:
        table = choose_data_table(db, plant_id, _from, _to)
        sql = text(_sql_power_vs_gti(table, lim))
        rows = db.execute(sql, {"plant_id": plant_id, "f": f"{_from} 00:00:00", "t": f"{_to} 23:59:59"}).fetchall()
        logger.info(f"Dashboard /power-vs-gti hit {table} in {time.time() - start_time:.4f}s")
    return [
        PowerVsGTIPoint(
            timestamp      = _as_json_str(r.timestamp),
            active_power_kw= round(float(r.active_power), 2) if r.active_power is not None else None,
            gti            = round(float(r.gti), 1) if r.gti is not None else None,
        )
        for r in rows
    ]


# ── Loss Waterfall Builder ─────────────────────────────────────────────────────
@router.post("/loss-waterfall", response_model=List[LossWaterfallPoint])
def loss_waterfall(
    payload: LossWaterfallInput,
    current_user: User = Depends(get_current_user),
):
    """Calculate a loss waterfall chart given user-entered loss percentages."""
    cap    = payload.plant_capacity_kwp
    steps  = [
        ("Nameplate Capacity",  cap),
        ("Irradiance Loss",     -cap * payload.irradiance_loss_pct / 100),
        ("Soiling Loss",        -cap * payload.soiling_loss_pct / 100),
        ("Inverter Loss",       -cap * payload.inverter_loss_pct / 100),
        ("Curtailment",         -cap * payload.curtailment_pct / 100),
        ("Grid Loss",           -cap * payload.grid_loss_pct / 100),
    ]

    result    = []
    cumulative = 0.0
    for label, val in steps:
        cumulative += val
        result.append(LossWaterfallPoint(
            category   = label,
            value      = round(val, 2),
            cumulative = round(cumulative, 2),
        ))
    result.append(LossWaterfallPoint(
        category   = "Net Generation",
        value      = round(cumulative, 2),
        cumulative = round(cumulative, 2),
    ))
    return result


def _fetch_target_generation_payload(plant_id: str, date_from: str, date_to: str) -> Any:
    """Load expected/actual snapshot; uses write pool and fault_cache (same as bundle worker)."""
    s = None
    try:
        s = SessionLocal()
        lg_key = cache_key_loss_gen_snapshot(plant_id, date_from, date_to)
        tg = fault_cache_get(s, lg_key, TTL_LOSS_GEN_SNAPSHOT_MIN)
        # Stale error snapshots were cached before we skipped caching — treat as miss.
        if tg and tg.get("compute_error"):
            tg = None
        if tg is None:
            from routers.loss_analysis import compute_plant_expected_actual_mwh_for_range

            tg = compute_plant_expected_actual_mwh_for_range(s, plant_id, date_from, date_to)
            if not (isinstance(tg, dict) and tg.get("compute_error")):
                fault_cache_set(s, lg_key, tg)
        return tg
    except Exception as exc:  # noqa: BLE001
        logger.exception("_fetch_target_generation_payload plant_id=%s", plant_id)
        msg = str(exc)[:240] if str(exc) else "Could not load expected vs actual for this range."
        return {
            "expected_mwh": None,
            "actual_mwh": None,
            "insolation_kwh_m2": None,
            "plant_dc_kwp": None,
            "compute_error": msg,
        }
    finally:
        if s is not None:
            s.close()


# ── Bundle (single request + cache) ───────────────────────────────────────────
@router.get("/target-generation")
def dashboard_target_generation(
    plant_id: str = Query(...),
    date_from: str = Query(default=None),
    date_to: str = Query(default=None),
    current_user: User = Depends(get_current_user),
):
    """
    Expected vs actual (target gen) for dashboard only — use after /bundle?include_target_generation=0
    for faster first paint. Same payload the full bundle would embed under `target_generation`.
    """
    _from, _to = _default_range(date_from, date_to)
    return _fetch_target_generation_payload(plant_id, _from, _to)


@router.get("/bundle")
def dashboard_bundle(
    plant_id: str  = Query(...),
    date_from: str = Query(default=None),
    date_to: str   = Query(default=None),
    include_target_generation: bool = Query(
        default=True,
        description="If false, omits target_generation in response (faster; call /target-generation separately).",
    ),
    db: Session    = Depends(get_read_db),
    current_user: User = Depends(get_current_user),
):
    """
    Return station, kpis, wms, energy, inverter_performance, power_vs_gti in one response.
    Cached per (plant_id, date range). Lite variant (no target gen) uses a separate cache key.
    """
    _from, _to = _default_range(date_from, date_to)
    cache_prefix = "bundle_v9" if include_target_generation else "bundle_v9_lite"
    cached = cache_get(cache_prefix, plant_id, _from, _to)
    if cached is not None:
        logger.info(
            "dashboard bundle plant=%s range=%s..%s cache=hit key=%s itg=%s",
            plant_id,
            _from,
            _to,
            cache_prefix,
            include_target_generation,
        )
        return cached

    _bundle_t0 = time.perf_counter()
    f_ts, t_ts = f"{_from} 00:00:00", f"{_to} 23:59:59"
    table = choose_data_table(db, plant_id, _from, _to)
    mv_inv = (
        db.execute(
            text("SELECT 1 FROM pg_matviews WHERE matviewname = 'mv_inverter_power_1min'")
        ).fetchone()
        is not None
    )
    plant = db.query(Plant).filter(Plant.plant_id == plant_id).first()
    cap_kw = (plant.capacity_mwp * 1000) if (plant and plant.capacity_mwp) else None
    spec_dc_map, arch_dc_map = _inverter_dc_maps(db, plant_id)
    plant_dc_kwp = _plant_dc_kwp_from_inverters(spec_dc_map, arch_dc_map)

    # Station
    age = None
    if plant and plant.cod_date:
        try:
            cod = datetime.strptime(plant.cod_date[:10], "%Y-%m-%d").date()
            age = round((date.today() - cod).days / 365.25, 1)
        except Exception:
            pass
    station = {
        "plant_id": plant_id, "name": plant.name if plant else plant_id, "technology": (plant.technology or "Solar PV") if plant else "Solar PV",
        "status": (plant.status or "Active") if plant else "Unknown", "capacity_mwp": plant.capacity_mwp if plant else None,
        "cod_date": plant.cod_date if plant else None, "ppa_tariff": plant.ppa_tariff if plant else None,
        "plant_age_years": age, "location": plant.location if plant else None,
    }

    # ── Parallelize heavy queries ────────────────────────────────────────────
    # Each task gets its own DB session so queries run truly concurrently.
    # Read-only work uses ReadSessionLocal (read pool); target_generation may
    # write fault_cache rows and uses SessionLocal in _fetch_target_generation_payload.
    from concurrent.futures import ThreadPoolExecutor

    params = {"plant_id": plant_id, "f": f_ts, "t": t_ts}

    def _q_inv_table():
        s = ReadSessionLocal()
        try:
            if mv_inv:
                isql = text(sql_mv_inverter_performance(plant_id))
                rows_raw = s.execute(
                    isql, {"plant_id": plant_id, "f": f_ts, "t": t_ts}
                ).fetchall()
                raw_dicts: List[Dict[str, Any]] = []
                for r in rows_raw:
                    raw_dicts.append(
                        {
                            "inverter_id": r.equipment_id,
                            "dc_power": r.dc_power,
                            "ac_power": r.ac_power,
                            "energy_kwh": r.energy_kwh,
                            "dc_capacity_kw": r.dc_cap_kw,
                        }
                    )
                return finalize_inverter_rows(s, plant_id, raw_dicts, _from, _to)
            return _inverter_performance_table(s, table, plant_id, f_ts, t_ts, _from, _to)
        finally:
            s.close()

    def _q_ac_totals():
        s = ReadSessionLocal()
        try:
            return s.execute(text(sql_plant_ac_totals(table)), params).fetchone()
        finally:
            s.close()

    def _q_insolation():
        s = ReadSessionLocal()
        try:
            return _wms_tilt_insolation_kwh_m2(s, table, plant_id, f_ts, t_ts)
        finally:
            s.close()

    def _q_wms():
        s = ReadSessionLocal()
        try:
            return _wms_kpis_payload(s, table, plant_id, f_ts, t_ts)
        finally:
            s.close()

    def _q_energy():
        s = ReadSessionLocal()
        try:
            if mv_inv:
                esql = text(sql_mv_plant_ac_daily_energy())
                return s.execute(
                    esql, {"plant_id": plant_id, "from_ts": f_ts, "to_ts": t_ts}
                ).fetchall()
            return s.execute(
                text(sql_plant_ac_daily_energy(table)),
                {"plant_id": plant_id, "from_ts": f_ts, "to_ts": t_ts},
            ).fetchall()
        finally:
            s.close()

    def _q_pvg():
        s = ReadSessionLocal()
        try:
            pvg_lim = _power_vs_gti_row_limit(_from, _to)
            if mv_inv:
                psql = text(sql_mv_power_vs_gti())
                return s.execute(
                    psql,
                    {
                        "plant_id": plant_id,
                        "f": f_ts,
                        "t": t_ts,
                        "limit": pvg_lim,
                    },
                ).fetchall()
            return s.execute(text(_sql_power_vs_gti(table, pvg_lim)), params).fetchall()
        finally:
            s.close()

    def _q_target_gen():
        return _fetch_target_generation_payload(plant_id, _from, _to)

    # Six read pools + optional target_gen (7) — pool size 6 matches prior behavior when itg on.
    _workers = 6 if include_target_generation else 5
    with ThreadPoolExecutor(max_workers=_workers) as pool:
        fut_inv = pool.submit(_q_inv_table)
        fut_ac  = pool.submit(_q_ac_totals)
        fut_ins = pool.submit(_q_insolation)
        fut_wms = pool.submit(_q_wms)
        fut_en  = pool.submit(_q_energy)
        fut_pvg = pool.submit(_q_pvg)
        fut_tg = pool.submit(_q_target_gen) if include_target_generation else None

        inv_table = fut_inv.result()
        ac        = fut_ac.result()
        _ins_b    = fut_ins.result()
        wms       = fut_wms.result()
        en_rows   = fut_en.result()
        pvg_rows  = fut_pvg.result()
        target_generation = fut_tg.result() if fut_tg is not None else None
    _parallel_ms = (time.perf_counter() - _bundle_t0) * 1000.0

    # ── Assemble KPIs (same logic, now using parallel results) ─────────────
    insolation_kwh_m2 = round(_ins_b, 2) if _ins_b > 0 else None
    days = max(1, (datetime.strptime(_to[:10], "%Y-%m-%d") - datetime.strptime(_from[:10], "%Y-%m-%d")).days + 1)
    total_kwh = round(ac.total_kwh, 1) if ac and ac.total_kwh else None
    total_mwh = round(total_kwh / 1000, 2) if total_kwh else None
    plf_cap_kw = plant_dc_kwp or cap_kw
    plf = round((total_kwh / (plf_cap_kw * 24 * days)) * 100, 1) if (total_kwh and plf_cap_kw and days and (plf_cap_kw * 24 * days) > 0) else None
    plant_pr = _plant_pr_pct(total_kwh, plant_dc_kwp, _ins_b)
    kpis = {
        "energy_export_kwh": total_kwh, "net_generation_kwh": total_kwh,
        "energy_export_mwh": total_mwh, "net_generation_mwh": total_mwh, "total_inverter_generation_mwh": total_mwh,
        "active_power_kw": round(ac.avg_kw, 2) if ac and ac.avg_kw else None,
        "peak_power_kw": round(ac.peak_kw, 1) if ac and ac.peak_kw else None,
        "performance_ratio": plant_pr, "plant_load_factor": plf, "total_inverter_generation_kwh": total_kwh,
        "insolation_kwh_m2": insolation_kwh_m2,
    }

    # Energy (daily)
    target_kwh = round(cap_kw * 4.5, 1) if cap_kw else None
    energy = [
        {
            "date": _as_json_str(r.day),
            "actual_kwh": round(float(r.actual_kwh), 1) if r.actual_kwh is not None else None,
            "target_kwh": target_kwh,
            "actual_mwh": round(float(r.actual_kwh) / 1000, 3) if r.actual_kwh is not None else None,
            "target_mwh": round(target_kwh / 1000, 3) if target_kwh is not None else None,
        }
        for r in en_rows
    ]

    # Power vs GTI
    power_gti = [
        {
            "timestamp": _as_json_str(r.timestamp),
            "active_power_kw": round(float(r.active_power), 2) if r.active_power is not None else None,
            "gti": round(float(r.gti), 1) if r.gti is not None else None,
        }
        for r in pvg_rows
    ]

    out = {
        "station": station,
        "kpis": kpis,
        "wms": wms,
        "energy": energy,
        "inverter_performance": inv_table,
        "power_vs_gti": power_gti,
        "target_generation": target_generation,
    }
    cache_set(cache_prefix, plant_id, _from, _to, out)
    _total_ms = (time.perf_counter() - _bundle_t0) * 1000.0
    logger.info(
        "dashboard bundle plant=%s range=%s..%s total_ms=%.1f parallel_ms=%.1f cache=miss itg=%s pvg_rows=%d",
        plant_id,
        _from,
        _to,
        _total_ms,
        _parallel_ms,
        include_target_generation,
        len(power_gti),
    )
    return out


# ── Helper ────────────────────────────────────────────────────────────────────
def _default_range(date_from: Optional[str], date_to: Optional[str]):
    """Delegates to dashboard_helpers.resolve_dashboard_date_range (unit-tested)."""
    return resolve_dashboard_date_range(date_from, date_to)


def _power_vs_gti_row_limit(date_from: str, date_to: str) -> int:
    """Row budget tuned for chart responsiveness and query speed."""
    try:
        d0 = datetime.strptime(date_from[:10], "%Y-%m-%d").date()
        d1 = datetime.strptime(date_to[:10], "%Y-%m-%d").date()
        days = max(1, (d1 - d0).days + 1)
    except Exception:
        days = 7
    # Tuned for UI chart density + faster DB/JSON; long ranges cap lower than v1
    return min(50_000, max(2_000, days * 1_200))


def _as_json_str(v: Any) -> Optional[str]:
    """Normalize date/datetime-like DB values into JSON-safe strings."""
    if v is None:
        return None
    try:
        return v.isoformat()
    except Exception:
        return str(v)


def _sql_power_vs_gti(table: str, limit: int) -> str:
    """Plant AC power (kW) vs tilt irradiance; GTI falls back to `irradiance` signal."""
    return f"""
        WITH inverter_power AS (
            SELECT timestamp, SUM(value) AS active_power
            FROM {table}
            WHERE plant_id = :plant_id
              AND LOWER(TRIM(equipment_level::text)) = 'inverter'
              AND LOWER(TRIM(signal::text)) = 'ac_power'
              AND timestamp BETWEEN :f AND :t
            GROUP BY timestamp
        ),
        plant_gti AS (
            SELECT timestamp, MAX(value) AS gti
            FROM {table}
            WHERE plant_id = :plant_id
              AND LOWER(TRIM(equipment_level::text)) IN ('plant', 'wms')
              AND LOWER(TRIM(signal::text)) IN ('gti', 'irradiance')
              AND timestamp BETWEEN :f AND :t
            GROUP BY timestamp
        )
        SELECT ip.timestamp, ip.active_power, pg.gti
        FROM inverter_power ip
        LEFT JOIN plant_gti pg ON pg.timestamp = ip.timestamp
        ORDER BY ip.timestamp
        LIMIT {int(limit)}
    """
