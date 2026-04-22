"""
backend/routers/analytics.py
==============================
Analytics Lab API — equipment list, timeseries data, availability.
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text, func
from typing import List, Optional
from datetime import date, timedelta

from database import get_read_db
from db_perf import choose_data_table
from models import User, PlantEquipment, PlantArchitecture, EquipmentSpec
from schemas import EquipmentListResponse, TimeseriesPoint, TimeseriesResponse
from auth.routes import get_current_user

router = APIRouter(prefix="/api/analytics", tags=["Analytics Lab"])
VALID_LEVELS = {"inverter", "scb", "string", "wms"}

# Inverter-level dc_current / dc_power can appear from three UNION branches (see SQL below):
#   precedence 1 = raw row on equipment_id (inverter tag in raw_data_generic)
#   precedence 2 = dc_hierarchy_derived
#   precedence 3 = SUM(SCB values) rolled up to inverter_id via plant_architecture
# Default dc_source=raw prefers (1) then (2) then (3) so the chart matches inverter telemetry.
# Use dc_source=scb_aggregate to prefer (3) when you explicitly want summed SCB currents.
_DC_INVERTER_DEDUPE_SIGNALS = frozenset({"dc_current", "dc_power"})
_NORMALIZE_MIN_IRR_W_M2 = 50.0


@router.get("/equipment", response_model=EquipmentListResponse)
def get_equipment(
    level: str     = Query(..., description="inverter | scb | string"),
    plant_id: str  = Query(...),
    db: Session    = Depends(get_read_db),
    current_user: User = Depends(get_current_user),
):
    """
    Return unique equipment IDs. Inverter/SCB/string: plant_equipment first, then
    plant_architecture. WMS: always unions plant_equipment with a raw_data_generic
    DISTINCT scan so weather rows are not hidden by stale materialized rows.
    """
    from dashboard_cache import get_any, set_any
    level = _validate_level(level)
    cache_key = f"equipment:v5:{plant_id}:{level}"

    # ── 1. In-memory cache ────────────────────────────────────────────────────
    cached = get_any(cache_key, 300)
    if cached is not None:
        if isinstance(cached, dict):
            return EquipmentListResponse(**cached)
        return cached

    # WMS / plant-level meteo: uploads may use equipment_level 'plant' OR 'wms'
    lookup_level = "plant" if level == "wms" else level

    # ── 2. WMS: always UNION plant_equipment + raw_data_generic ─────────────────
    #    Bug we fixed: non-empty but wrong/stale plant_equipment rows caused an early
    #    return that skipped raw_data, so Excel `wms` rows never appeared.
    if level == "wms":
        mat_rows = db.query(PlantEquipment.equipment_id).filter(
            PlantEquipment.plant_id == plant_id,
            func.lower(PlantEquipment.equipment_level).in_(("plant", "wms")),
        ).all()
        mat_ids = [r[0] for r in mat_rows if r and r[0]]
        raw_rows = db.execute(
            text(
                "SELECT DISTINCT equipment_id FROM raw_data_generic "
                "WHERE plant_id=:p AND LOWER(TRIM(equipment_level::text)) IN ('plant', 'wms') "
                "AND equipment_id IS NOT NULL LIMIT 200"
            ),
            {"p": plant_id},
        ).fetchall()
        raw_ids = [r[0] for r in raw_rows if r[0]]
        ids = sorted(set(mat_ids) | set(raw_ids), key=lambda x: str(x))
        if plant_id and str(plant_id).strip() and plant_id not in ids:
            ids = [plant_id] + ids
        out = {"equipment_ids": ids, "total": len(ids)}
        set_any(cache_key, out, 300)
        return EquipmentListResponse(**out)

    # ── 3. Other levels: materialized plant_equipment first ─────────────────────
    mat_rows = db.query(PlantEquipment.equipment_id).filter(
        PlantEquipment.plant_id == plant_id,
        PlantEquipment.equipment_level == lookup_level,
    ).order_by(PlantEquipment.equipment_id).all()

    if mat_rows:
        ids = [r[0] for r in mat_rows if r and r[0]]
        out = {"equipment_ids": ids, "total": len(ids)}
        set_any(cache_key, out, 300)
        return EquipmentListResponse(**out)

    # ── 4. Fast fallback: use plant_architecture (44K rows) not raw_data_generic ─
    #    plant_architecture is tiny; raw_data_generic is 16M+ rows (avoid it here)
    if level in ("inverter", "scb", "string"):
        arch_col = "inverter_id" if level == "inverter" else "scb_id" if level == "scb" else "string_id"
        arch_rows = db.execute(
            text(f"SELECT DISTINCT {arch_col} FROM plant_architecture "
                 f"WHERE plant_id=:p AND {arch_col} IS NOT NULL ORDER BY {arch_col}"),
            {"p": plant_id}
        ).fetchall()
        ids = [r[0] for r in arch_rows if r[0]]
    else:
        ids = []

    out = {"equipment_ids": ids, "total": len(ids)}
    set_any(cache_key, out, 300)
    return EquipmentListResponse(**out)


@router.get("/timeseries", response_model=TimeseriesResponse)
def get_timeseries(
    equipment_ids: str = Query(..., description="Comma-separated IDs"),
    signals: str       = Query(..., description="Comma-separated signal names"),
    plant_id: str      = Query(...),
    date_from: str     = Query(default=None),
    date_to: str       = Query(default=None),
    normalize: bool    = Query(default=False),
    dc_source: str     = Query(
        "raw",
        description="raw | scb_aggregate — for inverter dc_current/dc_power dedupe (see API docs).",
    ),
    level: str         = Query(default="inverter", description="equipment level: inverter | scb | string | wms"),
    db: Session        = Depends(get_read_db),
    current_user: User = Depends(get_current_user),
):
    """
    Fetch time-series for selected equipment and signals.
    Optionally normalize electrical signals by equipment DC capacity.

    **Inverter DC current/power:** The query unions raw inverter rows with a roll-up that
    **SUMs every SCB** under that inverter at each timestamp. Summed SCB currents are often
    tens–hundreds of kA and are *not* the same as a single inverter DC bus reading.
    By default ``dc_source=raw`` keeps the inverter-reported point when present.
    Set ``dc_source=scb_aggregate`` to prefer the summed-SCB series.
    """
    from dashboard_cache import get_any, set_any

    ids  = [i.strip() for i in equipment_ids.split(",") if i.strip()]
    sigs = [s.strip() for s in signals.split(",") if s.strip()]

    capacity_map: dict[str, float] = {}
    if normalize and ids:
        capacity_map = _load_dc_capacity_map(db, plant_id, ids)

    _from, _to = _default_range(date_from, date_to)

    cache_key = (
        f"analytics:timeseries:v2:{plant_id}:{level}:{_from}:{_to}:"
        f"{','.join(ids)}:{','.join(sigs)}:{int(normalize)}:{dc_source}"
    )
    cached = get_any(cache_key, 180)
    if cached is not None:
        if isinstance(cached, dict):
            return TimeseriesResponse(**cached)
        return cached

    if not ids or not sigs:
        return TimeseriesResponse(data=[], availability_pct=0.0, date_range={"from": _from, "to": _to})

    table = choose_data_table(db, plant_id, _from, _to)
    requested_signals = list(dict.fromkeys(sigs))
    query_signals = []
    for sig in requested_signals:
        if sig == "gti":
            query_signals.extend(["gti", "irradiance"])
        else:
            query_signals.append(sig)
    query_signals = list(dict.fromkeys(query_signals))

    id_placeholders  = ",".join(f"'{i}'" for i in ids)
    sig_placeholders = ",".join(f"'{s}'" for s in query_signals)

    # The SCB roll-up UNION branch must ONLY run when the user selected INVERTER level.
    # When SCB/string level is selected the ids are already SCB/string IDs — running the
    # JOIN would incorrectly use them as inverter_ids and produce wrong data or errors.
    is_inverter_level = (level or "inverter").strip().lower() == "inverter"

    scb_rollup_union = ""
    if is_inverter_level:
        scb_rollup_union = f"""
        UNION ALL
        SELECT DATE_TRUNC('minute', CAST(r.timestamp AS TIMESTAMP)) AS timestamp,
               map.inverter_id AS equipment_id,
               r.signal,
               CASE WHEN r.signal LIKE '%voltage%' THEN AVG(r.value) ELSE SUM(r.value) END AS value,
               3 AS precedence
        FROM {table} r
        JOIN (
            SELECT DISTINCT plant_id, inverter_id, scb_id
            FROM plant_architecture
            WHERE plant_id = :plant_id
              AND inverter_id IN ({id_placeholders})
              AND scb_id IS NOT NULL
        ) map ON r.equipment_id = map.scb_id
        WHERE r.plant_id = :plant_id
          AND r.signal IN ({sig_placeholders})
          AND r.signal = 'dc_current'
          AND r.timestamp BETWEEN :from_ts AND :to_ts
        GROUP BY 1, 2, 3"""

    sql = text(f"""
        SELECT DATE_TRUNC('minute', CAST(timestamp AS TIMESTAMP)) AS timestamp,
               equipment_id, signal, AVG(value) AS value, 1 AS precedence
        FROM {table}
        WHERE plant_id = :plant_id
          AND equipment_id IN ({id_placeholders})
          AND signal       IN ({sig_placeholders})
          AND timestamp BETWEEN :from_ts AND :to_ts
        GROUP BY 1, 2, 3
        UNION ALL
        SELECT DATE_TRUNC('minute', CAST(timestamp AS TIMESTAMP)) AS timestamp,
               equipment_id, signal, AVG(value) AS value, 2 AS precedence
        FROM dc_hierarchy_derived
        WHERE plant_id     = :plant_id
          AND equipment_id IN ({id_placeholders})
          AND signal       IN ({sig_placeholders})
          AND timestamp BETWEEN :from_ts AND :to_ts
        GROUP BY 1, 2, 3
        {scb_rollup_union}
        ORDER BY timestamp, equipment_id, precedence
        LIMIT 100000
    """)
    rows = db.execute(sql, {"plant_id": plant_id, "from_ts": f"{_from} 00:00:00", "to_ts": f"{_to} 23:59:59"}).fetchall()

    def _canonical_signal(sig: str) -> str:
        return "gti" if sig == "irradiance" else sig

    # De-dupe (timestamp, equipment_id, signal): for dc_current/dc_power prefer higher SQL precedence
    # (3 = SUM/Aggregate from SCBs via plant_architecture, 2 = dc_hierarchy_derived, 1 = raw row).
    # Previously precedence-1 raw inverter current won and hid the correct SCB sum.
    def _row_prec(r) -> int:
        try:
            return int(r.precedence)
        except Exception:
            try:
                return int(r[4])
            except Exception:
                return 0

    prefer_scb_aggregate = (dc_source or "raw").strip().lower() == "scb_aggregate"

    def _dedupe_sort_prec(r, canon_sig: str):
        p = _row_prec(r)
        if canon_sig in _DC_INVERTER_DEDUPE_SIGNALS:
            if prefer_scb_aggregate:
                # Explicit SCB aggregate mode:
                # 3 (sum of mapped SCBs) -> 1 (raw inverter) -> 2 (derived)
                rank = {3: 0, 1: 1, 2: 2}.get(p, 99)
            else:
                # Default raw mode:
                # 1 (raw inverter) -> 3 (sum of mapped SCBs) -> 2 (derived)
                # This enforces: if inverter current not uploaded, fallback to mapped SCB sum.
                rank = {1: 0, 3: 1, 2: 2}.get(p, 99)
            return (rank, p)
        return (0, -p)

    rows = sorted(
        rows,
        key=lambda r: (
            r.timestamp,
            r.equipment_id,
            _canonical_signal(r.signal),
            _dedupe_sort_prec(r, _canonical_signal(r.signal)),
        ),
    )

    data = []
    seen = set()
    for r in rows:
        canonical_signal = _canonical_signal(r.signal)
        key = (r.timestamp, r.equipment_id, canonical_signal)
        if key in seen:
            continue
        seen.add(key)
        
        val = r.value
        if normalize and val is not None:
            cap = capacity_map.get(r.equipment_id)
            if canonical_signal == "dc_current":
                # Normalize current by DC capacity -> A per kWp.
                if cap and cap > 0:
                    val = round(val / cap, 4)
                else:
                    val = None
            elif canonical_signal in {"dc_power", "ac_power"}:
                # Normalize power by DC capacity -> % of DC capacity.
                if cap and cap > 0:
                    val = round((val / cap) * 100, 2)
                else:
                    val = None

        data.append(TimeseriesPoint(
            timestamp    = r.timestamp.isoformat() if hasattr(r.timestamp, 'isoformat') else str(r.timestamp),
            equipment_id = r.equipment_id,
            signal       = canonical_signal,
            value        = round(val, 2) if val is not None else None,
        ))

    # Data availability
    avail = _calc_availability(data, ids, requested_signals, _from, _to)

    payload = {
        "data": [p.model_dump() for p in data],
        "availability_pct": avail,
        "date_range": {"from": _from, "to": _to},
    }
    set_any(cache_key, payload, 180)
    return TimeseriesResponse(**payload)


@router.get("/signals")
def get_available_signals(
    level: str     = Query(...),
    plant_id: str  = Query(...),
    db: Session    = Depends(get_read_db),
    current_user: User = Depends(get_current_user),
):
    """Return all signals present in the DB for the given equipment level and plant."""
    from dashboard_cache import get_any, set_any

    level = _validate_level(level)
    cache_key = f"analytics:signals:v2:{plant_id}:{level}"
    cached = get_any(cache_key, 300)
    if cached is not None:
        return cached

    if level == "wms":
        # Scope to the last year so the DISTINCT scan uses recent partitions
        # / index ranges rather than full table history.
        sql = text("""
            SELECT DISTINCT signal FROM raw_data_generic
            WHERE plant_id = :plant_id
              AND LOWER(TRIM(equipment_level::text)) IN ('plant', 'wms')
              AND timestamp >= :since
            ORDER BY signal
        """)
        since = (date.today() - timedelta(days=365)).isoformat()
        rows = db.execute(sql, {"plant_id": plant_id, "since": since}).fetchall()
        signals = [r[0] for r in rows if r[0]]
        out = {"signals": signals}
        set_any(cache_key, out, 300)
        return out

    if level == "inverter":
        # Constrain lookback so the DISTINCT scan can use the BRIN / composite
        # indexes and never has to touch partitions older than a year.
        sql = text("""
            SELECT DISTINCT signal
            FROM raw_data_generic
            WHERE plant_id = :plant_id
              AND LOWER(TRIM(equipment_level::text)) = 'inverter'
              AND timestamp >= :since
            UNION
            SELECT DISTINCT signal
            FROM raw_data_generic
            WHERE plant_id = :plant_id
              AND LOWER(TRIM(equipment_level::text)) = 'scb'
              AND timestamp >= :since
            UNION
            SELECT DISTINCT signal
            FROM dc_hierarchy_derived
            WHERE plant_id = :plant_id
              AND LOWER(TRIM(equipment_level::text)) = 'inverter'
              AND timestamp >= :since
            ORDER BY signal
        """)
        since = (date.today() - timedelta(days=365)).isoformat()
        rows = db.execute(sql, {"plant_id": plant_id, "since": since}).fetchall()
        out = {"signals": [r[0] for r in rows]}
        set_any(cache_key, out, 300)
        return out

    # SCB / string: case-insensitive equipment_level (some plants use "String", "SCB", etc.).
    # For string, also UNION signals for rows keyed by plant_architecture.string_id — PDCL-style
    # uploads often tag per-string series as equipment_level scb (or other) while equipment_id is str01-01-22.
    if level == "scb":
        sql = text("""
            SELECT DISTINCT signal FROM raw_data_generic
            WHERE plant_id = :plant_id
              AND LOWER(TRIM(equipment_level::text)) = 'scb'
              AND timestamp >= :since
            UNION
            SELECT DISTINCT signal FROM dc_hierarchy_derived
            WHERE plant_id = :plant_id
              AND LOWER(TRIM(equipment_level::text)) = 'scb'
              AND timestamp >= :since
            ORDER BY signal
        """)
        since = (date.today() - timedelta(days=365)).isoformat()
        rows = db.execute(sql, {"plant_id": plant_id, "since": since}).fetchall()
        out = {"signals": [r[0] for r in rows if r[0]]}
        set_any(cache_key, out, 300)
        return out

    if level == "string":
        sql = text("""
            SELECT DISTINCT signal FROM raw_data_generic
            WHERE plant_id = :plant_id
              AND LOWER(TRIM(equipment_level::text)) = 'string'
              AND CAST(timestamp AS TIMESTAMP) >= :since
            UNION
            SELECT DISTINCT r.signal FROM raw_data_generic r
            WHERE r.plant_id = :plant_id
              AND r.timestamp >= :since
              AND r.equipment_id IN (
                SELECT DISTINCT string_id FROM plant_architecture
                WHERE plant_id = :plant_id AND string_id IS NOT NULL
              )
            UNION
            SELECT DISTINCT signal FROM dc_hierarchy_derived
            WHERE plant_id = :plant_id
              AND LOWER(TRIM(equipment_level::text)) = 'string'
              AND CAST(timestamp AS TIMESTAMP) >= :since
            UNION
            SELECT DISTINCT d.signal FROM dc_hierarchy_derived d
            WHERE d.plant_id = :plant_id
              AND d.timestamp >= :since
              AND d.equipment_id IN (
                SELECT DISTINCT string_id FROM plant_architecture
                WHERE plant_id = :plant_id AND string_id IS NOT NULL
              )
            ORDER BY signal
        """)
        since = (date.today() - timedelta(days=365)).isoformat()
        rows = db.execute(sql, {"plant_id": plant_id, "since": since}).fetchall()
        out = {"signals": [r[0] for r in rows if r[0]]}
        set_any(cache_key, out, 300)
        return out

    out = {"signals": []}
    set_any(cache_key, out, 300)
    return out


# ── Helpers ───────────────────────────────────────────────────────────────────
def _default_range(date_from, date_to):
    if date_from and date_to:
        return date_from, date_to
    today = date.today()
    return str(today - timedelta(days=7)), str(today)


def _validate_level(level: str) -> str:
    level = (level or "").strip().lower()
    if level not in VALID_LEVELS:
        raise HTTPException(status_code=400, detail=f"Unsupported hierarchy level: {level}")
    return level


def _load_dc_capacity_map(db: Session, plant_id: str, equipment_ids: List[str]) -> dict[str, float]:
    """
    Build a DC-capacity lookup for the selected equipment IDs.

    Preference order:
    1. Plant architecture rollups for inverter / SCB / string IDs.
    2. equipment_specs.dc_capacity_kwp fallback for IDs that have explicit specs.
    """
    out: dict[str, float] = {}
    ids = [str(i).strip() for i in (equipment_ids or []) if str(i).strip()]
    if not ids:
        return out

    inv_rows = (
        db.query(PlantArchitecture.inverter_id, func.sum(PlantArchitecture.dc_capacity_kw))
        .filter(
            PlantArchitecture.plant_id == plant_id,
            PlantArchitecture.inverter_id.in_(ids),
            PlantArchitecture.dc_capacity_kw.isnot(None),
        )
        .group_by(PlantArchitecture.inverter_id)
        .all()
    )
    for eq_id, cap in inv_rows:
        if eq_id and cap is not None and float(cap) > 0:
            out[str(eq_id)] = float(cap)

    scb_rows = (
        db.query(PlantArchitecture.scb_id, func.sum(PlantArchitecture.dc_capacity_kw))
        .filter(
            PlantArchitecture.plant_id == plant_id,
            PlantArchitecture.scb_id.in_(ids),
            PlantArchitecture.dc_capacity_kw.isnot(None),
        )
        .group_by(PlantArchitecture.scb_id)
        .all()
    )
    for eq_id, cap in scb_rows:
        if eq_id and cap is not None and float(cap) > 0:
            out[str(eq_id)] = float(cap)

    string_rows = (
        db.query(PlantArchitecture.string_id, func.sum(PlantArchitecture.dc_capacity_kw))
        .filter(
            PlantArchitecture.plant_id == plant_id,
            PlantArchitecture.string_id.in_(ids),
            PlantArchitecture.dc_capacity_kw.isnot(None),
        )
        .group_by(PlantArchitecture.string_id)
        .all()
    )
    for eq_id, cap in string_rows:
        if eq_id and cap is not None and float(cap) > 0:
            out[str(eq_id)] = float(cap)

    spec_rows = (
        db.query(EquipmentSpec.equipment_id, EquipmentSpec.dc_capacity_kwp)
        .filter(
            EquipmentSpec.plant_id == plant_id,
            EquipmentSpec.equipment_id.in_(ids),
            EquipmentSpec.dc_capacity_kwp.isnot(None),
            EquipmentSpec.dc_capacity_kwp > 0,
        )
        .all()
    )
    for eq_id, cap in spec_rows:
        if eq_id and cap is not None and float(cap) > 0 and str(eq_id) not in out:
            out[str(eq_id)] = float(cap)

    return out


def _calc_availability(rows, ids, sigs, from_ts, to_ts) -> float:
    """
    Availability based on the observed plotting timestamp grid, not a fixed 15-minute day.

    This keeps 1-minute uploads from being misclassified and lets missing equipment points
    show up as reduced availability when other selected equipment/WMS timestamps exist.
    """
    try:
        timestamps = sorted(
            {
                getattr(r, "timestamp", None)
                for r in (rows or [])
                if getattr(r, "timestamp", None) is not None
            }
        )
        actual = len(rows or [])
        if actual <= 0 or not ids or not sigs:
            return 0.0
        if not timestamps:
            return 0.0

        expected = len(timestamps) * len(ids) * len(sigs)
        return min(round((actual / expected) * 100, 1), 100.0) if expected > 0 else 0.0
    except Exception:
        return 0.0
