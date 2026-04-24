"""
Loss Analysis — energy bridge vs Fault Diagnostics categories.
Expected = DC kWp × tilt insolation (kWh/m²) / 1000 → MWh; then degradation, temperature,
per-category fault losses (same list as unified feed), unknown gap vs metered actual.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, text
from sqlalchemy.orm import Session

from auth.routes import get_current_user
from database import get_db, SessionLocal
from module_snapshots import get_loss_analysis_snapshot, save_loss_analysis_snapshot
from snap_perf import record_compute_ms, record_snapshot
from db_perf import choose_data_table
from models import EquipmentSpec, PlantArchitecture, PlantEquipment, User
from routers.dashboard import _wms_kpis_payload, _wms_tilt_insolation_kwh_m2
from routers.faults import _fault_date_range, _unified_feed_categories_only
from ac_power_energy_sql import sql_inverter_performance_with_energy

# Shared handlers mounted at two URL prefixes: some deployments / proxies only expose `/api/dashboard/*`.
_loss_routes = APIRouter(tags=["Loss Analysis"])

DEFAULT_TEMP_COEFF = 0.004  # ~0.4% energy per °C above 25 when no module gamma


def _filter_inverter_spec(q):
    """Match inverter rows regardless of equipment_type casing (metadata / Excel)."""
    return q.filter(func.lower(EquipmentSpec.equipment_type) == "inverter")


def _inverter_ids_for_plant(db: Session, plant_id: str) -> List[str]:
    """
    Union of inverter IDs from equipment specs, materialized plant_equipment, and
    plant_architecture — same sources as Analytics Lab, so Loss Analysis lists match metadata.
    """
    ids: set[str] = set()
    for (eid,) in _filter_inverter_spec(
        db.query(EquipmentSpec.equipment_id).filter(EquipmentSpec.plant_id == plant_id)
    ).all():
        if eid and str(eid).strip():
            ids.add(str(eid).strip())
    for (eid,) in (
        db.query(PlantEquipment.equipment_id)
        .filter(
            PlantEquipment.plant_id == plant_id,
            func.lower(PlantEquipment.equipment_level) == "inverter",
        )
        .all()
    ):
        if eid and str(eid).strip():
            ids.add(str(eid).strip())
    for (iid,) in (
        db.query(PlantArchitecture.inverter_id)
        .filter(PlantArchitecture.plant_id == plant_id, PlantArchitecture.inverter_id.isnot(None))
        .distinct()
        .all()
    ):
        if iid and str(iid).strip():
            ids.add(str(iid).strip())
    # Telemetry-only inverters (no row in equipment_specs / architecture yet)
    try:
        raw_rows = db.execute(
            text(
                "SELECT DISTINCT equipment_id FROM raw_data_generic "
                "WHERE plant_id = :p AND LOWER(TRIM(equipment_level::text)) = 'inverter' "
                "AND equipment_id IS NOT NULL LIMIT 400"
            ),
            {"p": plant_id},
        ).fetchall()
        for r in raw_rows:
            if r[0] and str(r[0]).strip():
                ids.add(str(r[0]).strip())
    except Exception:
        pass
    return sorted(ids)


def _inverter_spec_row(db: Session, plant_id: str, inverter_id: str):
    return (
        _filter_inverter_spec(
            db.query(EquipmentSpec).filter(
                EquipmentSpec.plant_id == plant_id,
                EquipmentSpec.equipment_id == inverter_id,
            )
        ).first()
    )


def _dc_kwp_for_inverter(db: Session, plant_id: str, inverter_id: str) -> float:
    q = (
        db.query(func.coalesce(func.sum(PlantArchitecture.dc_capacity_kw), 0))
        .filter(PlantArchitecture.plant_id == plant_id, PlantArchitecture.inverter_id == inverter_id)
        .scalar()
    )
    dc = float(q or 0.0)
    if dc > 0:
        return dc
    spec = _inverter_spec_row(db, plant_id, inverter_id)
    return float(spec.dc_capacity_kwp or 0.0) if spec else 0.0


def _insolation_kwh_m2(db: Session, table: str, plant_id: str, f_ts: str, t_ts: str) -> float:
    return float(_wms_tilt_insolation_kwh_m2(db, table, plant_id, f_ts, t_ts) or 0.0)


def _module_temp_coeff(db: Session, plant_id: str) -> float:
    mod = (
        db.query(EquipmentSpec)
        .filter(EquipmentSpec.plant_id == plant_id, func.lower(EquipmentSpec.equipment_type) == "module")
        .first()
    )
    if not mod or mod.gamma_stc is None:
        return DEFAULT_TEMP_COEFF
    g = abs(float(mod.gamma_stc))
    # Heuristic: if stored as %/°C (e.g. 0.36), convert to fraction
    return g / 100.0 if g > 0.05 else g


def _plant_dc_kwp(db: Session, plant_id: str) -> Tuple[float, Dict[str, float]]:
    """Return plant total DC kWp and per-inverter DC kWp.

    Collapses what used to be an N+1 (one SQL call per inverter to read DC kWp
    from architecture and another to read from equipment_specs) into two batch
    queries: one GROUP BY against plant_architecture and one bulk pull of any
    remaining inverters' specs.
    """
    ids = _inverter_ids_for_plant(db, plant_id)
    if not ids:
        return 0.0, {}

    # 1) DC kWp from plant_architecture, all inverters in one query.
    arch_rows = (
        db.query(
            PlantArchitecture.inverter_id,
            func.coalesce(func.sum(PlantArchitecture.dc_capacity_kw), 0.0).label("dc"),
        )
        .filter(
            PlantArchitecture.plant_id == plant_id,
            PlantArchitecture.inverter_id.in_(ids),
        )
        .group_by(PlantArchitecture.inverter_id)
        .all()
    )
    arch_map: Dict[str, float] = {str(r.inverter_id): float(r.dc or 0.0) for r in arch_rows}

    # 2) For inverters with no architecture DC (or 0), fall back to equipment_specs in one bulk query.
    missing = [inv for inv in ids if arch_map.get(inv, 0.0) <= 0]
    spec_map: Dict[str, float] = {}
    if missing:
        spec_rows = _filter_inverter_spec(
            db.query(EquipmentSpec.equipment_id, EquipmentSpec.dc_capacity_kwp).filter(
                EquipmentSpec.plant_id == plant_id,
                EquipmentSpec.equipment_id.in_(missing),
            )
        ).all()
        for r in spec_rows:
            if r[0]:
                spec_map[str(r[0])] = float(r[1] or 0.0)

    m: Dict[str, float] = {}
    for inv in ids:
        dc = arch_map.get(inv, 0.0)
        if dc <= 0:
            dc = spec_map.get(inv, 0.0)
        m[inv] = dc
    total = sum(m.values())
    return total, m


def _scb_to_inverter_map(db: Session, plant_id: str) -> Dict[str, str]:
    rows = db.query(PlantArchitecture.scb_id, PlantArchitecture.inverter_id).filter(PlantArchitecture.plant_id == plant_id).distinct().all()
    out: Dict[str, str] = {}
    for scb_id, inv_id in rows:
        if scb_id and inv_id:
            out[str(scb_id)] = str(inv_id)
    return out


def _scb_dc_kwp(db: Session, plant_id: str, scb_id: str) -> float:
    q = (
        db.query(func.sum(PlantArchitecture.dc_capacity_kw))
        .filter(PlantArchitecture.plant_id == plant_id, PlantArchitecture.scb_id == scb_id)
        .scalar()
    )
    return float(q or 0.0)


def _string_dc_kw(db: Session, plant_id: str, inverter_id: str, scb_id: str, string_id: str) -> float:
    row = (
        db.query(PlantArchitecture.dc_capacity_kw)
        .filter(
            PlantArchitecture.plant_id == plant_id,
            PlantArchitecture.inverter_id == inverter_id,
            PlantArchitecture.scb_id == scb_id,
            PlantArchitecture.string_id == string_id,
        )
        .first()
    )
    return float(row[0] or 0.0) if row else 0.0


def _inverter_actual_kwh(db: Session, table: str, plant_id: str, f_ts: str, t_ts: str) -> Dict[str, float]:
    sql = text(sql_inverter_performance_with_energy(table))
    rows = db.execute(sql, {"plant_id": plant_id, "f": f_ts, "t": t_ts}).fetchall()
    out: Dict[str, float] = {}
    for r in rows:
        eid = r.equipment_id
        ek = float(r.energy_kwh or 0.0)
        if eid:
            out[str(eid)] = ek
    return out


def _entity_row(
    label: str,
    entity_type: str,
    entity_id: Optional[str],
    dc_kwp: float,
    insolation_kwh_m2: float,
    degradation_pct: float,
    temp_coeff: float,
    module_temp_c: float,
    category_losses_mwh: List[Tuple[str, str, float]],
    actual_kwh: float,
) -> Dict[str, Any]:
    expected_mwh = (dc_kwp * insolation_kwh_m2) / 1000.0 if dc_kwp > 0 and insolation_kwh_m2 > 0 else 0.0
    deg_mwh = expected_mwh * (degradation_pct / 100.0)
    after_deg = max(0.0, expected_mwh - deg_mwh)
    dtemp = max(0.0, module_temp_c - 25.0)
    temp_mwh = after_deg * dtemp * temp_coeff
    diagnostics_mwh = sum(x[2] for x in category_losses_mwh)
    actual_mwh = actual_kwh / 1000.0
    model_mwh = deg_mwh + temp_mwh
    all_losses_mwh = model_mwh + diagnostics_mwh
    unknown_mwh = expected_mwh - all_losses_mwh - actual_mwh
    cat_list = [{"id": c[0], "label": c[1], "mwh": round(c[2], 4)} for c in category_losses_mwh]
    return {
        "label": label,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "dc_kwp": round(dc_kwp, 4),
        "expected_mwh": round(expected_mwh, 4),
        "actual_mwh": round(actual_mwh, 4),
        "degradation_mwh": round(deg_mwh, 4),
        "temperature_loss_mwh": round(temp_mwh, 4),
        "category_losses_mwh": cat_list,
        "model_loss_mwh": round(model_mwh, 4),
        "diagnostics_loss_mwh": round(diagnostics_mwh, 4),
        "all_losses_mwh": round(all_losses_mwh, 4),
        "unknown_mwh": round(unknown_mwh, 4),
    }


def _waterfall_bridge_segments(primary: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Stacked-bar waterfall: each row has invisible_mwh (spacer from 0) + visible_mwh (colored segment).
    Steps down from Expected through each loss to Actual (reference bridge chart style).
    """
    E = float(primary.get("expected_mwh") or 0)
    deg = float(primary.get("degradation_mwh") or 0)
    temp = float(primary.get("temperature_loss_mwh") or 0)
    cats = primary.get("category_losses_mwh") or []
    unk = float(primary.get("unknown_mwh") or 0)
    act = float(primary.get("actual_mwh") or 0)

    segs: List[Dict[str, Any]] = []
    segs.append(
        {
            "key": "expected",
            "label": "Expected energy",
            "invisible_mwh": 0.0,
            "visible_mwh": round(E, 4),
            "kind": "total",
        }
    )
    cum = E
    cum -= deg
    segs.append(
        {
            "key": "degradation",
            "label": "Degradation loss",
            "invisible_mwh": round(cum, 4),
            "visible_mwh": round(deg, 4),
            "kind": "loss",
        }
    )
    cum -= temp
    segs.append(
        {
            "key": "temperature",
            "label": "Temperature loss",
            "invisible_mwh": round(cum, 4),
            "visible_mwh": round(temp, 4),
            "kind": "loss",
        }
    )
    for c in cats:
        lm = float(c.get("mwh") or 0)
        if lm <= 1e-9:
            continue
        cum -= lm
        segs.append(
            {
                "key": f"diag_{c.get('id')}",
                "label": str(c.get("label") or c.get("id") or "Fault"),
                "invisible_mwh": round(cum, 4),
                "visible_mwh": round(lm, 4),
                "kind": "loss",
                "category_id": c.get("id"),
            }
        )
    segs.append(
        {
            "key": "unknown",
            "label": "Unknown loss",
            "invisible_mwh": round(act, 4),
            "visible_mwh": round(unk, 4),
            "kind": "unknown",
        }
    )
    segs.append(
        {
            "key": "actual",
            "label": "Actual energy (metered)",
            "invisible_mwh": 0.0,
            "visible_mwh": round(act, 4),
            "kind": "total",
        }
    )
    return segs


def compute_plant_expected_actual_mwh_for_range(
    db: Session, plant_id: str, date_from: str, date_to: str
) -> Dict[str, Any]:
    """
    Plant-level expected (DC kWp × tilt insolation / 1000) and metered actual (sum inverter kWh / 1000).
    Same basis as Loss Analysis `primary` for scope=plant, but skips unified-feed categories and
    per-inverter table rows — suitable for dashboard KPIs and caching.
    """
    _from, _to = date_from, date_to
    f_ts, t_ts = f"{_from} 00:00:00", f"{_to} 23:59:59"
    table = choose_data_table(db, plant_id, _from, _to)
    insolation = _insolation_kwh_m2(db, table, plant_id, f_ts, t_ts)
    plant_dc, inv_dc_map = _plant_dc_kwp(db, plant_id)
    actual_by_inv = _inverter_actual_kwh(db, table, plant_id, f_ts, t_ts)
    for inv_id, _ek in actual_by_inv.items():
        k = str(inv_id).strip() if inv_id is not None else ""
        if k and k not in inv_dc_map:
            inv_dc_map[k] = 0.0
    plant_dc = sum(inv_dc_map.values())
    plant_actual_kwh = sum(actual_by_inv.values())
    expected_mwh = (plant_dc * insolation) / 1000.0 if plant_dc > 0 and insolation > 0 else 0.0
    actual_mwh = plant_actual_kwh / 1000.0
    return {
        "expected_mwh": round(expected_mwh, 4),
        "actual_mwh": round(actual_mwh, 4),
        "insolation_kwh_m2": round(insolation, 4),
        "plant_dc_kwp": round(plant_dc, 4),
    }


@_loss_routes.get("/options")
def loss_options(
    plant_id: str = Query(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Hierarchy pick lists for Loss Analysis UI."""
    inverters = _inverter_ids_for_plant(db, plant_id)
    arch = db.query(PlantArchitecture).filter(PlantArchitecture.plant_id == plant_id).all()
    scbs = sorted({a.scb_id for a in arch if a.scb_id})
    strings = []
    for a in arch:
        if a.inverter_id and a.scb_id and a.string_id:
            strings.append(
                {
                    "inverter_id": a.inverter_id,
                    "scb_id": a.scb_id,
                    "string_id": a.string_id,
                    "label": f"{a.inverter_id} / {a.scb_id} / {a.string_id}",
                }
            )
    strings.sort(key=lambda x: x["label"])
    return {"inverters": inverters, "scbs": scbs, "strings": strings}


def build_loss_bridge_payload(
    db: Session,
    plant_id: str,
    date_from: Optional[str],
    date_to: Optional[str],
    scope: str,
    equipment_id: Optional[str],
    current_user: User,
) -> dict:
    """
    Full `/bridge` response dict (including error keys). Used by the route and post-ingest precompute.
    """
    _from, _to = _fault_date_range(date_from, date_to)
    f_ts, t_ts = f"{_from} 00:00:00", f"{_to} 23:59:59"
    table = choose_data_table(db, plant_id, _from, _to)

    pack = _unified_feed_categories_only(db, plant_id, _from, _to, current_user)
    categories: List[dict] = pack.get("categories") or []

    insolation = _insolation_kwh_m2(db, table, plant_id, f_ts, t_ts)
    wms = _wms_kpis_payload(db, table, plant_id, f_ts, t_ts)
    module_temp_c = float(wms.get("module_temp") or 25.0)
    temp_coeff = DEFAULT_TEMP_COEFF
    tc_mod = _module_temp_coeff(db, plant_id)
    if tc_mod:
        temp_coeff = tc_mod

    plant_dc, inv_dc_map = _plant_dc_kwp(db, plant_id)
    actual_by_inv = _inverter_actual_kwh(db, table, plant_id, f_ts, t_ts)
    for inv_id, ek in actual_by_inv.items():
        k = str(inv_id).strip() if inv_id is not None else ""
        if k and k not in inv_dc_map:
            inv_dc_map[k] = 0.0
    plant_dc = sum(inv_dc_map.values())
    scb_to_inv = _scb_to_inverter_map(db, plant_id)
    inv_list = sorted(inv_dc_map.keys())
    plant_actual_kwh = sum(actual_by_inv.values())

    def cat_tuples_for_entity(alloc_factor: float) -> List[Tuple[str, str, float]]:
        return [(c["id"], c.get("label") or c["id"], float(c.get("loss_mwh") or 0.0) * alloc_factor) for c in categories]

    scope_l = (scope or "plant").strip().lower()
    table_rows: List[Dict[str, Any]] = []
    primary: Optional[Dict[str, Any]] = None

    if scope_l == "plant":
        deg_weighted = 0.0
        if plant_dc > 0:
            for inv, dc in inv_dc_map.items():
                spec = _inverter_spec_row(db, plant_id, inv)
                pct = float(spec.degradation_loss_pct or 0.0) if spec else 0.0
                deg_weighted += (dc / plant_dc) * pct
        else:
            deg_weighted = 0.0
        cat_tuples = cat_tuples_for_entity(1.0)
        primary = _entity_row(
            "Whole plant",
            "plant",
            None,
            plant_dc,
            insolation,
            deg_weighted,
            temp_coeff,
            module_temp_c,
            cat_tuples,
            plant_actual_kwh,
        )
        for inv in inv_list:
            dc = inv_dc_map.get(inv, 0.0)
            act_kwh = actual_by_inv.get(inv, 0.0)
            if dc <= 0 and act_kwh <= 0:
                continue
            fac = dc / plant_dc if plant_dc > 0 else 0.0
            spec = _inverter_spec_row(db, plant_id, inv)
            d_pct = float(spec.degradation_loss_pct or 0.0) if spec else 0.0
            t_coeff = (float(spec.temp_coefficient_per_deg or 0.0) or temp_coeff) if spec else temp_coeff
            row = _entity_row(
                inv,
                "inverter",
                inv,
                dc,
                insolation,
                d_pct,
                t_coeff,
                module_temp_c,
                cat_tuples_for_entity(fac),
                actual_by_inv.get(inv, 0.0),
            )
            table_rows.append(row)
    elif scope_l == "inverter":
        inv = (equipment_id or "").strip()
        if not inv or inv not in inv_dc_map:
            return {"error": "invalid_inverter", "message": "Select a valid inverter.", "scope": scope_l}
        dc = inv_dc_map.get(inv, 0.0)
        fac = dc / plant_dc if plant_dc > 0 else 1.0
        spec = _inverter_spec_row(db, plant_id, inv)
        d_pct = float(spec.degradation_loss_pct or 0.0) if spec else 0.0
        t_coeff = (float(spec.temp_coefficient_per_deg or 0.0) or temp_coeff) if spec else temp_coeff
        primary = _entity_row(
            inv,
            "inverter",
            inv,
            dc,
            insolation,
            d_pct,
            t_coeff,
            module_temp_c,
            cat_tuples_for_entity(fac),
            actual_by_inv.get(inv, 0.0),
        )
        table_rows = [primary]
    elif scope_l == "scb":
        scb = (equipment_id or "").strip()
        if not scb:
            return {"error": "invalid_scb", "message": "Select an SCB.", "scope": scope_l}
        dc = _scb_dc_kwp(db, plant_id, scb)
        parent_inv = scb_to_inv.get(scb)
        inv_dc = inv_dc_map.get(parent_inv, 0.0) if parent_inv else plant_dc
        fac = (dc / plant_dc) if plant_dc > 0 else 0.0
        spec = _inverter_spec_row(db, plant_id, parent_inv) if parent_inv else None
        d_pct = float(spec.degradation_loss_pct or 0.0) if spec else 0.0
        t_coeff = (float(spec.temp_coefficient_per_deg or 0.0) or temp_coeff) if spec else temp_coeff
        inv_actual = actual_by_inv.get(parent_inv, 0.0) if parent_inv else plant_actual_kwh
        prorate_actual = inv_actual * (dc / inv_dc) if inv_dc > 0 else 0.0
        primary = _entity_row(
            scb,
            "scb",
            scb,
            dc,
            insolation,
            d_pct,
            t_coeff,
            module_temp_c,
            cat_tuples_for_entity(fac),
            prorate_actual,
        )
        table_rows = [primary]
    elif scope_l == "string":
        key = (equipment_id or "").strip()
        parts = key.split("::")
        if len(parts) != 3:
            return {"error": "invalid_string", "message": "String key must be inverter_id::scb_id::string_id", "scope": scope_l}
        inv_id, scb_id, str_id = parts[0], parts[1], parts[2]
        dc = _string_dc_kw(db, plant_id, inv_id, scb_id, str_id)
        inv_dc = inv_dc_map.get(inv_id, 0.0)
        fac = (dc / plant_dc) if plant_dc > 0 else 0.0
        spec = _inverter_spec_row(db, plant_id, inv_id)
        d_pct = float(spec.degradation_loss_pct or 0.0) if spec else 0.0
        t_coeff = (float(spec.temp_coefficient_per_deg or 0.0) or temp_coeff) if spec else temp_coeff
        inv_actual = actual_by_inv.get(inv_id, 0.0)
        prorate_actual = inv_actual * (dc / inv_dc) if inv_dc > 0 else 0.0
        primary = _entity_row(
            key,
            "string",
            key,
            dc,
            insolation,
            d_pct,
            t_coeff,
            module_temp_c,
            cat_tuples_for_entity(fac),
            prorate_actual,
        )
        table_rows = [primary]
    else:
        return {"error": "invalid_scope", "message": "scope must be plant, inverter, scb, or string"}

    assert primary is not None
    wf_bridge = _waterfall_bridge_segments(primary)

    worst = sorted(table_rows, key=lambda r: abs(float(r.get("unknown_mwh") or 0.0)), reverse=True)[:10]
    worst_unknown_chart = [{"id": r["entity_id"] or r["label"], "label": r["label"], "unknown_mwh": r["unknown_mwh"]} for r in worst]

    return {
        "date_from": _from,
        "date_to": _to,
        "plant_id": plant_id,
        "scope": scope_l,
        "insolation_kwh_m2": round(insolation, 4),
        "module_temp_c": round(module_temp_c, 2),
        "temp_coefficient_used": round(temp_coeff, 6),
        "fault_categories_source": "unified_feed",
        "primary": primary,
        "waterfall_bridge": wf_bridge,
        "table": table_rows if scope_l == "plant" else table_rows,
        "worst_unknown": worst_unknown_chart,
        "notes": [
            "Expected (MWh) = DC kWp × insolation (kWh/m²) / 1000.",
            "Waterfall: each loss steps down from Expected; Unknown bridges to Actual.",
            "Fault Diagnostics bars use the same category MWh as the Faults overview (new categories appear automatically).",
            "Table keeps aggregated All losses; chart shows individual steps.",
        ],
    }


@_loss_routes.get("/bridge")
def loss_bridge(
    plant_id: str = Query(...),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    scope: str = Query("plant", description="plant | inverter | scb | string"),
    equipment_id: Optional[str] = Query(None, description="inverter id, scb id, or string key inv::scb::str"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Build waterfall + table rows. Uses `loss_analysis_snapshot` when fresh vs raw_data_stats.
    """
    t0 = time.perf_counter()
    _from, _to = _fault_date_range(date_from, date_to)
    scope_l = (scope or "plant").strip().lower()
    snap = get_loss_analysis_snapshot(db, plant_id, _from, _to, scope_l, equipment_id or "")
    if snap is not None:
        record_snapshot("loss_bridge", True)
        record_compute_ms("loss_bridge_http", (time.perf_counter() - t0) * 1000.0, f"hit plant={plant_id}")
        return snap
    record_snapshot("loss_bridge", False)
    out = build_loss_bridge_payload(db, plant_id, date_from, date_to, scope, equipment_id, current_user)
    if isinstance(out, dict) and not out.get("error"):
        wdb = SessionLocal()
        try:
            save_loss_analysis_snapshot(wdb, plant_id, _from, _to, scope_l, equipment_id or "", out)
        finally:
            wdb.close()
    record_compute_ms("loss_bridge_http", (time.perf_counter() - t0) * 1000.0, f"miss plant={plant_id}")
    return out


router = APIRouter(prefix="/api/loss-analysis")
router.include_router(_loss_routes)

# Alias under dashboard prefix (reverse proxies often whitelist `/api/dashboard/*` first).
router_dashboard_alias = APIRouter(prefix="/api/dashboard/loss-analysis", tags=["Dashboard"])
router_dashboard_alias.include_router(_loss_routes)
