"""
backend/routers/metadata.py
=============================
Metadata API — plant architecture and equipment specs CRUD + Excel upload.
"""

import io
import json
import logging
import os
import re
import traceback
import pandas as pd
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, UploadFile, File, Query, Form
from fastapi.responses import StreamingResponse, FileResponse, RedirectResponse
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List, Optional, Sequence

from database import get_db, SessionLocal
from db_perf import refresh_15m_cache
from dashboard_cache import invalidate_plant as invalidate_dashboard_cache
from models import PlantArchitecture, EquipmentSpec, RawDataGeneric, RawDataStats, PlantEquipment, User
from schemas import ArchitectureRow, EquipmentSpecRow
from auth.routes import get_current_user

router = APIRouter(prefix="/api/metadata", tags=["Metadata"])

# Spec sheet uploads: stored under backend/uploads/spec_sheets
_BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _db_levels_for_ui_equipment_filter(equipment_level: str) -> Sequence[str]:
    """
    Map Metadata / UI hierarchy filter to raw_data_generic.equipment_level values.

    WMS weather rows may be stored as 'plant' (legacy ingest) or 'wms' (Excel / SCADA).
    The Raw Data preview labeled "WMS" must match both. Matching is case-insensitive.
    """
    el = (equipment_level or "").strip().lower()
    if not el:
        return ()
    if el in ("wms", "plant", "weather", "meteo"):
        return ("plant", "wms")
    return (el,)


def _refresh_equipment_mat(db: Session, plant_id: str) -> None:
    """Update the plant_equipment materialized table for all levels after an upload.

    Uses a single set-based INSERT ... ON CONFLICT DO NOTHING per level so we
    do not ORM-merge thousands of rows one at a time.
    """
    from sqlalchemy import text as _text
    from dashboard_cache import invalidate_plant
    try:
        for lvl in ("inverter", "scb", "string"):
            db.execute(
                _text(
                    """
                    INSERT INTO plant_equipment (plant_id, equipment_level, equipment_id)
                    SELECT DISTINCT :p, :l, equipment_id
                      FROM raw_data_generic
                     WHERE plant_id = :p
                       AND LOWER(TRIM(equipment_level::text)) = :l
                       AND equipment_id IS NOT NULL
                    ON CONFLICT (plant_id, equipment_level, equipment_id) DO NOTHING
                    """
                ),
                {"p": plant_id, "l": lvl},
            )
        # Plant-level meteo: preserve the original equipment_level casing
        # ("plant" vs "wms") because later queries OR between the two.
        db.execute(
            _text(
                """
                INSERT INTO plant_equipment (plant_id, equipment_level, equipment_id)
                SELECT DISTINCT :p,
                       LOWER(TRIM(equipment_level::text)),
                       equipment_id
                  FROM raw_data_generic
                 WHERE plant_id = :p
                   AND LOWER(TRIM(equipment_level::text)) IN ('plant', 'wms')
                   AND equipment_id IS NOT NULL
                ON CONFLICT (plant_id, equipment_level, equipment_id) DO NOTHING
                """
            ),
            {"p": plant_id},
        )
        db.commit()
        invalidate_plant(plant_id)
        from fault_cache import invalidate_loss_gen_snapshots
        invalidate_loss_gen_snapshots(db, plant_id)
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass


def _post_upload_refresh(plant_id: str, min_ts: Optional[str], max_ts: Optional[str]) -> None:
    """
    Run after the HTTP response has been sent. Opens its own DB session so we
    never reuse the request-scoped one (which FastAPI closes after the yield).

    Batches everything that used to run serially inside the upload endpoint:
      * invalidate dashboard + fault caches
      * refresh raw_data_stats
      * refresh plant_equipment materialized table
      * clear fault runtime snapshots for this plant
    """
    db = SessionLocal()
    try:
        try:
            if min_ts and max_ts:
                refresh_15m_cache(db, plant_id=plant_id, min_ts=min_ts, max_ts=max_ts)
            else:
                invalidate_dashboard_cache(plant_id)
        except Exception:
            pass

        try:
            from fault_runtime_snapshot import clear_snapshots_for_plant
            clear_snapshots_for_plant(db, plant_id)
        except Exception:
            try:
                db.rollback()
            except Exception:
                pass

        try:
            _refresh_plant_stats(db, plant_id)
        except Exception:
            try:
                db.rollback()
            except Exception:
                pass

        try:
            _refresh_equipment_mat(db, plant_id)
        except Exception:
            try:
                db.rollback()
            except Exception:
                pass
    finally:
        db.close()


def _refresh_plant_stats(db: Session, plant_id: str) -> None:
    """Recompute and upsert the raw_data_stats row for one plant. Fast future lookups."""
    from sqlalchemy import func as sqlfunc
    try:
        agg = db.query(
            sqlfunc.count(RawDataGeneric.id).label("total"),
            sqlfunc.min(RawDataGeneric.timestamp).label("min_ts"),
            sqlfunc.max(RawDataGeneric.timestamp).label("max_ts"),
        ).filter(RawDataGeneric.plant_id == plant_id).first()

        total = int(agg.total or 0) if agg else 0
        min_ts = str(agg.min_ts) if agg and agg.min_ts else None
        max_ts = str(agg.max_ts) if agg and agg.max_ts else None

        levels_q = db.query(
            RawDataGeneric.equipment_level,
            sqlfunc.count(sqlfunc.distinct(RawDataGeneric.equipment_id))
        ).filter(RawDataGeneric.plant_id == plant_id).group_by(RawDataGeneric.equipment_level).all()
        levels = {r[0]: r[1] for r in levels_q}

        existing = db.query(RawDataStats).filter(RawDataStats.plant_id == plant_id).first()
        if existing:
            existing.total_rows = total
            existing.min_ts = min_ts
            existing.max_ts = max_ts
            existing.levels_json = json.dumps(levels)
        else:
            db.add(RawDataStats(
                plant_id=plant_id, total_rows=total,
                min_ts=min_ts, max_ts=max_ts,
                levels_json=json.dumps(levels)
            ))
        db.commit()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
_UPLOADS_DIR = os.path.join(_BACKEND_DIR, "uploads", "spec_sheets")


# ── Plant Architecture ────────────────────────────────────────────────────────
# Paginated for speed: default limit 10k so first load is ~1–2 MB instead of 8+ MB
@router.get("/architecture")
def get_architecture(
    plant_id: str = Query(default=None),
    limit: int = Query(default=10000, ge=1, le=100000),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    from dashboard_cache import get_any, set_any
    ck = f"arch_full:{plant_id or ''}:{limit}:{offset}"
    cached = get_any(ck, 300)
    if cached is not None:
        return cached
    q = db.query(PlantArchitecture)
    if plant_id:
        q = q.filter(PlantArchitecture.plant_id == plant_id)
    total = q.count()
    items = q.order_by(PlantArchitecture.inverter_id, PlantArchitecture.scb_id).offset(offset).limit(limit).all()
    out = {"items": items, "total": total}
    set_any(ck, out, 300)
    return out


@router.get("/architecture/compact")
def get_architecture_compact(
    plant_id: str = Query(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Lightweight endpoint for the Fault page heatmap and hover metadata. Cached 5 min."""
    from dashboard_cache import get_any, set_any
    from sqlalchemy import text as _text
    ck = f"arch_compact:{plant_id}"
    cached = get_any(ck, 300)
    if cached is not None:
        return cached
    try:
        rows = db.execute(
            _text("SELECT DISTINCT scb_id, strings_per_scb, modules_per_string, dc_capacity_kw, COALESCE(spare_flag, false) AS spare_flag, inverter_id "
                  "FROM plant_architecture WHERE plant_id = :p AND scb_id IS NOT NULL ORDER BY inverter_id, scb_id"),
            {"p": plant_id}
        ).fetchall()
        out = [{
            "scb_id": r[0],
            "strings_per_scb": r[1],
            "modules_per_string": r[2],
            "dc_capacity_kw": r[3],
            "spare_flag": bool(r[4]) if r[4] is not None else False,
            "inverter_id": r[5],
        } for r in rows]
    except Exception:
        rows = db.execute(
            _text("SELECT DISTINCT scb_id, strings_per_scb, modules_per_string, dc_capacity_kw, inverter_id FROM plant_architecture "
                  "WHERE plant_id = :p AND scb_id IS NOT NULL ORDER BY inverter_id, scb_id"),
            {"p": plant_id}
        ).fetchall()
        out = [{
            "scb_id": r[0],
            "strings_per_scb": r[1],
            "modules_per_string": r[2] if len(r) > 2 else None,
            "dc_capacity_kw": r[3] if len(r) > 3 else None,
            "spare_flag": False,
            "inverter_id": r[4] if len(r) > 4 else None,
        } for r in rows]
    set_any(ck, out, 300)
    return out


@router.post("/architecture", response_model=ArchitectureRow, status_code=201)
def add_architecture(
    payload: ArchitectureRow,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    row = PlantArchitecture(**payload.model_dump(exclude={"id"}))
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


@router.delete("/architecture/{row_id}")
def delete_architecture(
    row_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    row = db.query(PlantArchitecture).filter(PlantArchitecture.id == row_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="Row not found")
    db.delete(row)
    db.commit()
    return {"success": True}


# ── Equipment Specs ───────────────────────────────────────────────────────────
@router.get("/equipment", response_model=List[EquipmentSpecRow])
def get_equipment_specs(
    plant_id: str = Query(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    q = db.query(EquipmentSpec)
    if plant_id:
        q = q.filter(EquipmentSpec.plant_id == plant_id)
    return q.order_by(EquipmentSpec.equipment_type, EquipmentSpec.equipment_id).all()


@router.post("/equipment", response_model=EquipmentSpecRow, status_code=201)
def add_equipment_spec(
    payload: EquipmentSpecRow,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    data = payload.model_dump(exclude={"id"})
    existing = db.query(EquipmentSpec).filter(EquipmentSpec.equipment_id == payload.equipment_id).filter(EquipmentSpec.plant_id == payload.plant_id).first()
    if existing:
        for k, v in data.items():
            if hasattr(EquipmentSpec, k):
                setattr(existing, k, v)
        db.commit()
        db.refresh(existing)
        return existing
    row = EquipmentSpec(**{k: v for k, v in data.items() if hasattr(EquipmentSpec, k)})
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


@router.delete("/equipment/{spec_id}")
def delete_equipment_spec(
    spec_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    spec = db.query(EquipmentSpec).filter(EquipmentSpec.id == spec_id).first()
    if not spec:
        raise HTTPException(status_code=404, detail="Equipment spec not found")
    db.delete(spec)
    db.commit()
    return {"success": True}


@router.get("/equipment/{spec_id}/spec-sheet")
def download_spec_sheet(
    spec_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Download the attached spec sheet for an equipment spec."""
    spec = db.query(EquipmentSpec).filter(EquipmentSpec.id == spec_id).first()
    if not spec or not spec.spec_sheet_path:
        raise HTTPException(status_code=404, detail="Spec sheet not found")
    if spec.spec_sheet_path.startswith(("http://", "https://")):
        return RedirectResponse(spec.spec_sheet_path)
    path = os.path.join(_BACKEND_DIR, spec.spec_sheet_path)
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path, filename=os.path.basename(path), media_type="application/octet-stream")


@router.post("/equipment/{spec_id}/spec-sheet")
def upload_spec_sheet(
    spec_id: int,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Upload a spec sheet PDF/document for an equipment spec. Replaces any existing."""
    spec = db.query(EquipmentSpec).filter(EquipmentSpec.id == spec_id).first()
    if not spec:
        raise HTTPException(status_code=404, detail="Equipment spec not found")
    safe_name = re.sub(r"[^\w\-.]", "_", file.filename or "spec")[:80]
    if not safe_name:
        safe_name = "spec"
    if not safe_name.lower().endswith((".pdf", ".xlsx", ".xls", ".doc", ".docx", ".png", ".jpg", ".jpeg")):
        safe_name += ".pdf"
    dir_path = os.path.join(_UPLOADS_DIR, spec.plant_id or "default")
    rel_path = os.path.join("uploads", "spec_sheets", spec.plant_id or "default", f"{spec.equipment_id}_{safe_name}")
    abs_path = os.path.join(_BACKEND_DIR, rel_path)
    try:
        content = file.file.read()
        is_serverless = os.environ.get("SOLAR_SERVERLESS", "").lower() in ("1", "true", "yes") or os.environ.get("VERCEL") == "1"
        if is_serverless:
            from blob_storage import blob_uploads_enabled, upload_bytes
            if not blob_uploads_enabled():
                raise HTTPException(
                    status_code=503,
                    detail="Spec-sheet uploads on Vercel require ENABLE_BLOB_UPLOADS=1 and BLOB_READ_WRITE_TOKEN.",
                )
            blob_path = f"spec-sheets/{spec.plant_id or 'default'}/{spec.equipment_id}_{safe_name}"
            spec.spec_sheet_path = upload_bytes(blob_path, content, file.content_type)
        else:
            os.makedirs(dir_path, exist_ok=True)
            with open(abs_path, "wb") as f:
                f.write(content)
            spec.spec_sheet_path = rel_path
        db.commit()
        return {"success": True, "spec_sheet_path": spec.spec_sheet_path}
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))


# ── Excel Upload ──────────────────────────────────────────────────────────────
@router.post("/upload-architecture")
async def upload_architecture_excel(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Bulk upload plant architecture from Excel."""
    content = await file.read()
    df = pd.read_excel(io.BytesIO(content), dtype=str)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    required = {"plant_id", "inverter_id", "scb_id", "string_id"}
    if not required.issubset(df.columns):
        raise HTTPException(status_code=400, detail=f"Missing columns: {required - set(df.columns)}")

    count = 0
    for _, row in df.iterrows():
        arch = PlantArchitecture(
            plant_id           = str(row.get("plant_id", "")).strip(),
            inverter_id        = str(row.get("inverter_id", "")).strip(),
            scb_id             = str(row.get("scb_id", "")).strip(),
            string_id          = str(row.get("string_id", "")).strip(),
            modules_per_string = _safe_int(row.get("modules_per_string")),
            strings_per_scb    = _safe_int(row.get("strings_per_scb")),
            scbs_per_inverter  = _safe_int(row.get("scbs_per_inverter")),
            dc_capacity_kw     = _safe_float(row.get("dc_capacity_kw")),
        )
        db.merge(arch)
        count += 1

    db.commit()
    return {"success": True, "rows_imported": count}


@router.post("/upload-equipment")
async def upload_equipment_excel(
    plant_id: str = Query(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Bulk upload equipment specs from Excel."""
    content = await file.read()
    df = pd.read_excel(io.BytesIO(content), dtype=str)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    required = {"equipment_id", "equipment_type"}
    if not required.issubset(df.columns):
        raise HTTPException(status_code=400, detail=f"Missing columns: {required - set(df.columns)}")

    def _set_spec_from_row(spec, row):
        spec.equipment_type = str(row.get("equipment_type", "")).strip()
        spec.manufacturer = str(row.get("manufacturer", "")).strip() or None
        spec.model = str(row.get("model", "")).strip() or None
        spec.rated_power = _safe_float(row.get("rated_power"))
        spec.imp = _safe_float(row.get("imp"))
        spec.vmp = _safe_float(row.get("vmp"))
        spec.isc = _safe_float(row.get("isc"))
        spec.voc = _safe_float(row.get("voc"))
        spec.target_efficiency = _safe_float(row.get("target_efficiency")) or 98.5
        spec.ac_capacity_kw = _safe_float(row.get("ac_capacity_kw"))
        spec.dc_capacity_kwp = _safe_float(row.get("dc_capacity_kwp"))
        spec.rated_efficiency = _safe_float(row.get("rated_efficiency"))
        spec.mppt_voltage_min = _safe_float(row.get("mppt_voltage_min"))
        spec.mppt_voltage_max = _safe_float(row.get("mppt_voltage_max"))
        spec.voltage_limit = _safe_float(row.get("voltage_limit"))
        spec.current_set_point = _safe_float(row.get("current_set_point"))
        spec.impp = _safe_float(row.get("impp"))
        spec.vmpp = _safe_float(row.get("vmpp"))
        spec.pmax = _safe_float(row.get("pmax"))
        spec.degradation_loss_pct = _safe_float(row.get("degradation_loss_pct"))
        spec.temp_coefficient_per_deg = _safe_float(row.get("temp_coefficient_per_deg"))
        spec.degradation_year1_pct = _safe_float(row.get("degradation_year1_pct"))
        spec.degradation_year2_pct = _safe_float(row.get("degradation_year2_pct"))
        spec.degradation_annual_pct = _safe_float(row.get("degradation_annual_pct"))
        spec.module_efficiency_pct = _safe_float(row.get("module_efficiency_pct"))
        spec.alpha_stc = _safe_float(row.get("alpha_stc"))
        spec.beta_stc = _safe_float(row.get("beta_stc"))
        spec.gamma_stc = _safe_float(row.get("gamma_stc"))
        spec.alpha_noct = _safe_float(row.get("alpha_noct"))
        spec.beta_noct = _safe_float(row.get("beta_noct"))
        spec.gamma_noct = _safe_float(row.get("gamma_noct"))

    count = 0
    for _, row in df.iterrows():
        eq_id = str(row.get("equipment_id", "")).strip()
        if not eq_id or eq_id == "nan":
            continue
        existing = db.query(EquipmentSpec).filter(EquipmentSpec.equipment_id == eq_id).filter(EquipmentSpec.plant_id == plant_id).first()
        if existing:
            _set_spec_from_row(existing, row)
        else:
            spec = EquipmentSpec(plant_id=plant_id, equipment_id=eq_id)
            _set_spec_from_row(spec, row)
            db.add(spec)
        count += 1

    db.commit()
    return {"success": True, "rows_imported": count}


# ── Upload Raw Data (Dynamic Mapping) ─────────────────────────────────────────
@router.post("/upload-raw-data-analyze")
async def analyze_raw_data(
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_user),
):
    """Read the first few rows of an Excel file to extract column headers.
       Detects if it's the specific NTPC report format."""
    content = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(content), nrows=15, header=None)
        
        # Check for NTPC format signature (e.g. REPORT in cell A1 or something similar)
        # Using a safer heuristic: ICRxx in row 6, INV in row 7
        is_ntpc = False
        if len(df) > 8:
            row6_vals = str(df.iloc[6].values).lower()
            row7_vals = str(df.iloc[7].values).lower()
            if 'icr' in row6_vals and 'inv' in row7_vals:
                is_ntpc = True
                
        if is_ntpc:
            # Check if it's the SCB current report or Inverter Power report
            # Inverter Power has AC_ACTIVE_POWER_kW in row 10 (index 9)
            is_inv_pwr = False
            if len(df) > 9:
                row9_vals = str(df.iloc[8].values).lower()
                if 'ac_active_power' in row9_vals or 'dc_power' in row9_vals:
                    is_inv_pwr = True
            
            return {"is_ntpc": True, "ntpc_type": "inv_pwr" if is_inv_pwr else "scb_curr", "columns": []}

        generic = pd.read_excel(io.BytesIO(content), nrows=5)
        columns = [str(c).strip() for c in generic.columns if str(c).strip() and str(c).strip().lower() != "nan"]
        return {"is_ntpc": False, "columns": columns}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read Excel file: {str(e)}")


@router.post("/upload-raw-data-ntpc")
async def upload_raw_data_ntpc(
    background_tasks: BackgroundTasks,
    plant_id: str = Form(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Ingest SCADA data explicitly matching the specific NTPC report format 
       and run DS Detection."""
    content = await file.read()
    try:
        result = import_ntpc_scb_content(plant_id, content, db)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"NTPC SCB import failed: {str(exc)}",
        )
    # Defer cache invalidation + stats/equipment refresh until after the
    # response is sent — it takes several seconds on large plants and the UI
    # does not need to wait for it.
    background_tasks.add_task(
        _post_upload_refresh, plant_id, result.get("min_ts"), result.get("max_ts"),
    )
    return {"success": True, **result}
@router.post("/upload-raw-data-inv-pwr")
async def upload_raw_data_inv_pwr(
    background_tasks: BackgroundTasks,
    plant_id: str = Form(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Ingest Inverter AC/DC Power data explicitly matching the Wide NTPC report format."""
    content = await file.read()
    try:
        result = import_ntpc_inv_pwr_content(plant_id, content, db)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"NTPC inverter power import failed: {str(exc)}",
        )
    background_tasks.add_task(
        _post_upload_refresh, plant_id, result.get("min_ts"), result.get("max_ts"),
    )
    return {"success": True, **result}

@router.post("/upload-raw-data-mapped")
async def upload_raw_data_mapped(
    background_tasks: BackgroundTasks,
    plant_id: str = Form(...),
    mapping: str = Form(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Ingest SCADA data based on user mapping and trigger DS detection."""
    try:
        map_dict = json.loads(mapping)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON mapping")

    content = await file.read()
    df = pd.read_excel(io.BytesIO(content))

    # reverse_map: Excel Column Name -> Required Internal Field
    reverse_map = {v: k for k, v in map_dict.items() if v}
    df = df.rename(columns=reverse_map)

    required_fields = {"timestamp", "inverter_id", "scb_id", "scb_current", "dc_voltage"}
    missing = required_fields - set(df.columns)
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing mapped columns for required fields: {missing}")

    count = 0
    batch = []
    for _, row in df.iterrows():
        ts = str(row.get("timestamp", "")).strip()
        inv_id = str(row.get("inverter_id", "")).strip()
        scb_id = str(row.get("scb_id", "")).strip()
        
        if not ts or not scb_id or ts == "nan":
            continue

        if "scb_current" in row and pd.notna(row["scb_current"]):
            batch.append(RawDataGeneric(plant_id=plant_id, timestamp=ts, equipment_level="scb", equipment_id=scb_id, signal="dc_current", value=_safe_float(row["scb_current"])))
        
        if "dc_voltage" in row and pd.notna(row["dc_voltage"]):
            batch.append(RawDataGeneric(plant_id=plant_id, timestamp=ts, equipment_level="scb", equipment_id=scb_id, signal="dc_voltage", value=_safe_float(row["dc_voltage"])))
            
        if "inverter_status" in row and pd.notna(row["inverter_status"]):
            batch.append(RawDataGeneric(plant_id=plant_id, timestamp=ts, equipment_level="inverter", equipment_id=inv_id, signal="status", value=_safe_float(row["inverter_status"])))
            
        if "string_count" in row and pd.notna(row["string_count"]):
            batch.append(RawDataGeneric(plant_id=plant_id, timestamp=ts, equipment_level="scb", equipment_id=scb_id, signal="string_count", value=_safe_float(row["string_count"])))

        count += 1
        if len(batch) >= 5000:
            db.bulk_save_objects(batch)
            db.commit()
            batch = []

    if batch:
        db.bulk_save_objects(batch)
        db.commit()

    # Trigger DS Engine here (inline — UI needs fault rows visible on success)
    from engine.ds_detection import run_ds_detection
    run_ds_detection(plant_id, df, db)

    # Defer cache invalidation + stats refresh to background
    _min_ts = _max_ts = None
    try:
        ts_series = pd.to_datetime(df["timestamp"], errors="coerce").dropna()
        if not ts_series.empty:
            _min_ts = ts_series.min().strftime("%Y-%m-%d %H:%M:%S")
            _max_ts = ts_series.max().strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass
    background_tasks.add_task(_post_upload_refresh, plant_id, _min_ts, _max_ts)

    return {"success": True, "rows_imported": count}


# ── Excel Template Downloads ──────────────────────────────────────────────────
@router.get("/template/architecture")
def download_architecture_template(current_user: User = Depends(get_current_user)):
    """Download Excel template for plant architecture bulk upload."""
    df = pd.DataFrame([{
        "plant_id": "PLANT-WMS-01", "inverter_id": "INV-01",
        "scb_id": "INV-01-SCB-01", "string_id": "INV-01-SCB-01-STR-01",
        "modules_per_string": 20, "strings_per_scb": 10,
        "scbs_per_inverter": 5, "dc_capacity_kw": 4.2,
    }])
    return _df_to_excel_response(df, "architecture_template.xlsx")


@router.get("/template/equipment")
def download_equipment_template(current_user: User = Depends(get_current_user)):
    """Download Excel template for equipment specs bulk upload."""
    df = pd.DataFrame([{
        "equipment_id": "INV-01", "equipment_type": "inverter",
        "manufacturer": "SMA", "model": "Sunny Tripower 50",
        "rated_power": 50.0, "imp": 9.5, "vmp": 555.0, "isc": 9.8, "voc": 650.0,
    }])
    return _df_to_excel_response(df, "equipment_template.xlsx")


@router.get("/template/raw-data")
def download_raw_data_template(current_user: User = Depends(get_current_user)):
    """Download an exact replica of the NTPC format report as the template."""
    # Build a small dummy NTPC report format matrix
    # Shape needs to match the parsing logic exactly
    rows = [
        ["", "", " NTPC NOKHRA,Rajasthan", "", "", "", "", ""],
        [""] * 8,
        ["REPORT"] + [""] * 7,
        [""] * 8,
        ["Date and Time : 3/11/2026", "", "", "", "To : 3/11/2026", "", "", ""],
        [""] * 8,
        ["DATE AND TIME", "", "", "ICR01", "", "ICR01", "", "WMS"],
        ["", "", "", "INV1", "", "INV2", "", "WMS"],
        ["", "", "", "DC_INPUT_CURRENT01", "DC_INPUT_CURRENT02", "DC_INPUT_CURRENT01", "DC_INPUT_CURRENT02", "GHI Main (W/m2)"],
        ["2026-03-11 08:00:00", "", "", 99.3, 98.1, 97.4, 98.6, 650.0],
        ["2026-03-11 09:00:00", "", "", 150.1, 148.5, 149.2, 147.9, 850.0]
    ]
    df = pd.DataFrame(rows)
    # We use header=False when saving because we built headers as rows
    
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, header=False)
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers    = {"Content-Disposition": "attachment; filename=raw_data_ntpc_template.xlsx"},
    )


@router.get("/raw-data-summary")
def raw_data_summary(
    plant_id: str = Query(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Read from pre-computed raw_data_stats table — instant, no full table scan."""
    stats = db.query(RawDataStats).filter(RawDataStats.plant_id == plant_id).first()
    if not stats or not stats.total_rows:
        return {"total_rows": 0, "date_range": None, "levels": {}}
    try:
        levels = json.loads(stats.levels_json) if stats.levels_json else {}
    except Exception:
        levels = {}
    return {
        "total_rows": stats.total_rows,
        "date_range": {"from": stats.min_ts, "to": stats.max_ts} if stats.min_ts else None,
        "levels": levels,
    }


@router.post("/reindex-raw-equipment")
def reindex_raw_equipment(
    plant_id: str = Query(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Rebuild plant_equipment from raw_data_generic (inverter, scb, string, plant, wms).
    Call after changing raw data outside the app or deploying mapping fixes.
    """
    _refresh_equipment_mat(db, plant_id)
    return {"ok": True, "plant_id": plant_id}


@router.get("/raw-data-preview")
def raw_data_preview(
    plant_id: str = Query(...),
    equipment_level: str = Query(default=None),
    equipment_id: str = Query(default=None),
    signal: str = Query(default=None),
    date_from: str = Query(default=None),
    date_to: str = Query(default=None),
    limit: int = Query(default=2000, ge=1, le=10000),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    query = db.query(RawDataGeneric).filter(RawDataGeneric.plant_id == plant_id)
    if equipment_level and str(equipment_level).strip():
        levels = _db_levels_for_ui_equipment_filter(equipment_level)
        if len(levels) > 1:
            query = query.filter(func.lower(RawDataGeneric.equipment_level).in_(levels))
        else:
            query = query.filter(func.lower(RawDataGeneric.equipment_level) == levels[0])
    if equipment_id:
        query = query.filter(RawDataGeneric.equipment_id == equipment_id)
    if signal and str(signal).strip():
        query = query.filter(func.lower(RawDataGeneric.signal) == str(signal).strip().lower())
    if date_from:
        query = query.filter(RawDataGeneric.timestamp >= f"{date_from} 00:00:00")
    if date_to:
        query = query.filter(RawDataGeneric.timestamp <= f"{date_to} 23:59:59")

    rows = (
        query.order_by(RawDataGeneric.timestamp.desc(), RawDataGeneric.id.desc())
        .limit(limit)
        .all()
    )

    return {
        "rows": [
            {
                "timestamp": r.timestamp,
                "equipment_level": r.equipment_level,
                "equipment_id": r.equipment_id,
                "signal": r.signal,
                "value": r.value,
                "source": r.source,
            }
            for r in rows
        ],
        "count": len(rows),
    }


@router.get("/scb-metadata")
def get_scb_metadata(
    plant_id: str = Query(...),
    scb_id: str = Query(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Return plant architecture + module spec for one SCB (Analytics Lab / Fault Compare)."""
    from sqlalchemy import text as _text
    row = db.execute(
        _text(
            "SELECT DISTINCT scb_id, inverter_id, strings_per_scb, modules_per_string, dc_capacity_kw "
            "FROM plant_architecture WHERE plant_id = :p AND scb_id = :s LIMIT 1"
        ),
        {"p": plant_id, "s": scb_id},
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="SCB not found in architecture")
    arch = {
        "scb_id": row[0],
        "inverter_id": row[1],
        "number_of_strings": row[2],
        "modules_per_string": row[3],
        "dc_capacity_kw": round(float(row[4]), 2) if row[4] is not None else None,
    }
    if arch["number_of_strings"] and arch["modules_per_string"] and arch["dc_capacity_kw"]:
        module_wp = (arch["dc_capacity_kw"] * 1000) / (arch["number_of_strings"] * arch["modules_per_string"])
        arch["module_wp"] = round(module_wp, 1)
    else:
        arch["module_wp"] = None
    spec = db.query(EquipmentSpec).filter(
        EquipmentSpec.plant_id == plant_id,
        EquipmentSpec.equipment_type == "module",
    ).first()
    if spec:
        arch["impp"] = getattr(spec, "impp", None) or getattr(spec, "imp", None)
        arch["vmpp"] = getattr(spec, "vmpp", None) or getattr(spec, "vmp", None)
        arch["pmax"] = getattr(spec, "pmax", None) or getattr(spec, "rated_power", None)
    else:
        arch["impp"] = arch["vmpp"] = arch["pmax"] = None
    return arch


# ── Helpers ───────────────────────────────────────────────────────────────────
def import_ntpc_scb_content(plant_id: str, content: bytes, db: Session):
    from sqlalchemy import text as _text
    report = _parse_ntpc_report(content)
    raw_rows = []
    ds_rows = []
    plant_signals = set()
    arch_rows = db.execute(
        _text(
            "SELECT scb_id, strings_per_scb "
            "FROM plant_architecture "
            "WHERE plant_id = :p AND scb_id IS NOT NULL AND strings_per_scb IS NOT NULL"
        ),
        {"p": plant_id},
    ).fetchall()
    scb_strings_map = {r[0]: int(r[1]) for r in arch_rows if r[1]}

    for _, row in report["data"].iterrows():
        ts = _format_timestamp(row["_ts"])
        for c in range(1, report["column_count"]):
            sig_raw = report["sigs"].iloc[c]
            if pd.isna(sig_raw):
                continue

            value = _normalize_ntpc_value(row.iloc[c])
            if value is None:
                continue

            signal_name = str(sig_raw).strip()
            inv = str(report["invs"].iloc[c]).strip()
            icr = str(report["icrs"].iloc[c]).strip()

            if signal_name.startswith("DC_INPUT_CURRENT"):
                inverter_id = _build_inv_id(icr, inv)
                scb_suffix = "".join(ch for ch in signal_name if ch.isdigit()).zfill(2)
                scb_id = f"{inverter_id}-SCB-{scb_suffix}"
                legacy_scb_id = f"SCB-{inverter_id.replace('INV-', '')}-{scb_suffix}"
                string_count = scb_strings_map.get(scb_id)
                if string_count is None:
                    string_count = scb_strings_map.get(legacy_scb_id)
                raw_rows.append(_raw_row(plant_id, ts, "scb", scb_id, "dc_current", value))
                ds_rows.append({
                    "timestamp": ts,
                    "inverter_id": inverter_id,
                    "scb_id": scb_id,
                    "scb_current": value,
                    "dc_voltage": None,
                    "string_count": string_count,
                })
                continue

            for plant_signal in _map_wms_signals(signal_name):
                raw_rows.append(_raw_row(plant_id, ts, "plant", plant_id, plant_signal, value))
                plant_signals.add(plant_signal)

    if not raw_rows:
        raise ValueError("No supported SCB or WMS rows were detected in this report.")

    _warn_if_unusual_ts_span(report["min_ts"], report["max_ts"], "import_ntpc_scb")

    # Exact set of SCB ids and signals this upload will re-insert — we only
    # want to delete rows that will actually be replaced, not every SCB row in
    # the date range.
    scb_ids_in_file = sorted({
        r["equipment_id"] for r in raw_rows if r["equipment_level"] == "scb"
    })
    scb_signals_in_file = sorted({
        r["signal"] for r in raw_rows if r["equipment_level"] == "scb"
    }) or ["dc_current"]

    # De-duplicate within this upload payload (same as inv_pwr branch already does)
    # so a malformed Excel row can't insert two rows with the same key.
    dedup = {}
    for r in raw_rows:
        key = (r["timestamp"], r["equipment_level"], r["equipment_id"], r["signal"])
        dedup[key] = r
    raw_rows = list(dedup.values())

    try:
        if scb_ids_in_file:
            _replace_raw_rows(
                db,
                plant_id,
                report["min_ts"],
                report["max_ts"],
                equipment_levels=["scb"],
                signals=scb_signals_in_file,
                equipment_ids=scb_ids_in_file,
            )
        if plant_signals:
            _replace_raw_rows(
                db,
                plant_id,
                report["min_ts"],
                report["max_ts"],
                equipment_levels=["plant"],
                signals=sorted(plant_signals),
            )
        _bulk_insert_raw_rows(db, raw_rows)
        db.commit()
    except Exception:
        db.rollback()
        raise

    # DS detection is part of the upload semantics (the user expects fault
    # rows to be visible immediately), so it runs inline. Cache / stats
    # refresh is deferred to BackgroundTasks — see upload_raw_data_ntpc.
    from engine.ds_detection import run_ds_detection
    run_ds_detection(plant_id, pd.DataFrame(ds_rows), db)

    return {
        "rows_imported": len(raw_rows),
        "date_range": {"from": report["min_ts"], "to": report["max_ts"]},
        "min_ts": report["min_ts"],
        "max_ts": report["max_ts"],
    }


def import_ntpc_inv_pwr_content(plant_id: str, content: bytes, db: Session):
    report = _parse_ntpc_report(content)
    raw_rows = []
    plant_signals = set()
    inverter_signals = set()

    for _, row in report["data"].iterrows():
        ts = _format_timestamp(row["_ts"])
        for c in range(1, report["column_count"]):
            sig_raw = report["sigs"].iloc[c]
            if pd.isna(sig_raw):
                continue

            value = _normalize_ntpc_value(row.iloc[c])
            if value is None:
                continue

            signal_name = str(sig_raw).strip()
            signal_lower = signal_name.lower()
            inv = str(report["invs"].iloc[c]).strip()
            icr = str(report["icrs"].iloc[c]).strip()

            if inv.upper().startswith("INV"):
                inverter_id = _build_inv_id(icr, inv)
                internal_signal = None
                if "ac_active_power" in signal_lower:
                    internal_signal = "ac_power"
                elif "dc_power" in signal_lower:
                    internal_signal = "dc_power"
                elif "dc_current" in signal_lower:
                    internal_signal = "dc_current"
                elif "dc_voltage" in signal_lower:
                    internal_signal = "dc_voltage"
                elif "ac_active_energy" in signal_lower or "daily_energy" in signal_lower:
                    internal_signal = "daily_energy_kwh"

                if internal_signal:
                    raw_rows.append(_raw_row(plant_id, ts, "inverter", inverter_id, internal_signal, value))
                    inverter_signals.add(internal_signal)
                    continue

            for plant_signal in _map_wms_signals(signal_name):
                raw_rows.append(_raw_row(plant_id, ts, "plant", plant_id, plant_signal, value))
                plant_signals.add(plant_signal)

    if not raw_rows:
        raise ValueError("No inverter AC/DC or WMS rows were detected in this report.")

    _warn_if_unusual_ts_span(report["min_ts"], report["max_ts"], "import_ntpc_inv_pwr")

    # De-duplicate within the same upload payload before DB insert.
    dedup = {}
    for r in raw_rows:
        key = (r["timestamp"], r["equipment_level"], r["equipment_id"], r["signal"])
        dedup[key] = r
    raw_rows = list(dedup.values())

    inverter_ids_in_file = sorted({
        r["equipment_id"] for r in raw_rows if r["equipment_level"] == "inverter"
    })

    try:
        # Re-upload safe: replace only the inverter signals present in this file,
        # scoped to the actual inverter ids we are about to re-insert.
        if inverter_signals and inverter_ids_in_file:
            _replace_raw_rows(
                db,
                plant_id,
                report["min_ts"],
                report["max_ts"],
                equipment_levels=["inverter"],
                signals=sorted(inverter_signals),
                equipment_ids=inverter_ids_in_file,
            )
        if plant_signals:
            _replace_raw_rows(
                db,
                plant_id,
                report["min_ts"],
                report["max_ts"],
                equipment_levels=["plant"],
                signals=sorted(plant_signals),
            )
        _bulk_insert_raw_rows(db, raw_rows)
        db.commit()
    except Exception:
        db.rollback()
        raise

    return {
        "rows_imported": len(raw_rows),
        "date_range": {"from": report["min_ts"], "to": report["max_ts"]},
        "min_ts": report["min_ts"],
        "max_ts": report["max_ts"],
    }


def _parse_ntpc_report(content: bytes):
    df = pd.read_excel(io.BytesIO(content), header=None)
    if len(df.index) < 10:
        raise ValueError("The uploaded NTPC report is shorter than expected.")

    icrs = df.iloc[6].ffill()
    invs = df.iloc[7].ffill()
    sigs = df.iloc[8]

    data = df.iloc[9:].copy()
    data = data.dropna(subset=[0])
    ts = pd.to_datetime(data.iloc[:, 0], errors="coerce")
    data = data.loc[ts.notna()].copy()
    data["_ts"] = ts.loc[ts.notna()].values

    if data.empty:
        raise ValueError("No timestamped data rows were found in the uploaded report.")

    return {
        "icrs": icrs,
        "invs": invs,
        "sigs": sigs,
        "data": data,
        "column_count": len(df.columns),
        "min_ts": _format_timestamp(data["_ts"].min()),
        "max_ts": _format_timestamp(data["_ts"].max()),
    }


def _format_timestamp(value):
    return pd.Timestamp(value).strftime("%Y-%m-%d %H:%M:%S")


def _build_inv_id(icr, inv):
    icr_num = "".join(ch for ch in str(icr) if ch.isdigit()).zfill(2)
    inv_num = "".join(ch for ch in str(inv) if ch.isdigit())
    inv_letter = {"1": "A", "2": "B", "3": "C", "4": "D", "5": "E"}.get(inv_num, "")
    return f"INV-{icr_num}{inv_letter}"


def _map_wms_signals(signal_name: str):
    sig = (signal_name or "").strip().lower()
    if not sig or sig == "nan" or "kwhr" in sig:
        return []
    if "ghi" in sig and "w/m2" in sig:
        return ["ghi"]
    if "gti" in sig and "w/m2" in sig:
        return ["irradiance", "gti"]
    if "ambient temperature" in sig:
        return ["ambient_temp", "temperature"]
    if "module temperature" in sig:
        return ["module_temp"]
    if "wind speed" in sig:
        return ["wind_speed"]
    return []


def _normalize_ntpc_value(val):
    num = _safe_float(val)
    if num is None:
        return None
    if abs(num) >= 1e7:
        return None
    return num


def _raw_row(plant_id, timestamp, equipment_level, equipment_id, signal, value):
    return {
        "plant_id": plant_id,
        "timestamp": timestamp,
        "equipment_level": equipment_level,
        "equipment_id": equipment_id,
        "signal": signal,
        "value": value,
        "source": "excel_upload",
    }


def _replace_raw_rows(
    db: Session,
    plant_id: str,
    min_ts: str,
    max_ts: str,
    equipment_levels,
    signals=None,
    equipment_ids=None,
):
    """
    Delete rows that the next insert will replace.

    Every filter that the caller supplies narrows the delete. Callers MUST pass
    the tightest scope they know: signals that are about to be re-inserted,
    equipment_ids that actually appear in the new file, etc. A too-broad delete
    here is how uploads silently wipe previously ingested data.
    """
    query = db.query(RawDataGeneric).filter(
        RawDataGeneric.plant_id == plant_id,
        RawDataGeneric.equipment_level.in_(equipment_levels),
        RawDataGeneric.timestamp >= min_ts,
        RawDataGeneric.timestamp <= max_ts,
    )
    if signals:
        query = query.filter(RawDataGeneric.signal.in_(signals))
    if equipment_ids:
        query = query.filter(RawDataGeneric.equipment_id.in_(equipment_ids))
    query.delete(synchronize_session=False)
    # Caller is responsible for the commit so delete+insert can share one txn.


def _bulk_insert_raw_rows(db: Session, rows, batch_size: int = 5000):
    for start in range(0, len(rows), batch_size):
        db.bulk_insert_mappings(RawDataGeneric, rows[start:start + batch_size])
    # Caller commits once the full batch is written.


def _warn_if_unusual_ts_span(min_ts: str, max_ts: str, context: str) -> None:
    """Log a warning when an upload's timestamp span is wider than expected.

    NTPC SCADA reports are daily; anything beyond 26 h strongly suggests a
    stray/bad row in the Excel file that would otherwise cause _replace_raw_rows
    to delete data for days the user did not intend to touch.
    """
    try:
        mn = pd.Timestamp(min_ts)
        mx = pd.Timestamp(max_ts)
        span_h = (mx - mn).total_seconds() / 3600.0
    except Exception:
        return
    if span_h > 26:
        logging.getLogger(__name__).warning(
            "upload(%s): timestamp span %.1fh (%s -> %s) exceeds one day; "
            "the replace-range delete may touch unrelated dates — verify the source file.",
            context, span_h, min_ts, max_ts,
        )


def _safe_int(val):
    try:
        if val is None:
            return None
        sval = str(val).strip()
        if sval == "" or sval.lower() == "nan":
            return None
        return int(float(val))
    except Exception:
        return None


def _safe_float(val):
    try:
        if val is None:
            return None
        sval = str(val).strip()
        if sval == "" or sval.lower() == "nan":
            return None
        return float(val)
    except Exception:
        return None


def _df_to_excel_response(df: pd.DataFrame, filename: str) -> StreamingResponse:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False)
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers    = {"Content-Disposition": f"attachment; filename={filename}"},
    )
