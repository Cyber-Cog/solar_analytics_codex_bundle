"""
backend/routers/admin.py
========================
Admin-only routes for user management and plant access control.
"""

import os
import shutil
import json
from datetime import datetime, timezone

from pydantic import BaseModel, Field

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional

from database import get_db
from models import (
    User, Plant, RawDataGeneric, DCHierarchyDerived, PlantArchitecture,
    EquipmentSpec, SupportTicket, FaultDiagnostics, FaultEpisode,
    FaultEpisodeDay, PlantEquipment, RawDataStats, ScbFaultReview,
    FaultRuntimeSnapshot, FaultCache, PrecomputeJob, UnifiedFeedCategoryTotal,
    FaultEvent,
)
from schemas import UserCreate, UserUpdate, UserResponse, MessageResponse
from auth.routes import get_current_user
from auth.jwt import hash_password

router = APIRouter(prefix="/api/admin", tags=["Admin"])


class PlantAdminUpdate(BaseModel):
    plant_type: Optional[str] = Field(default=None, description="SCB | MPPT")

def check_admin(user: User = Depends(get_current_user)):
    if not user.is_admin:
        raise HTTPException(status_code=403, detail="Admin access required")
    return user

@router.get("/users", response_model=List[UserResponse])
def list_users(db: Session = Depends(get_db), admin: User = Depends(check_admin)):
    return db.query(User).all()

@router.post("/users", response_model=UserResponse)
def create_user(payload: UserCreate, db: Session = Depends(get_db), admin: User = Depends(check_admin)):
    existing = db.query(User).filter(User.email == payload.email).first()
    if existing:
        raise HTTPException(status_code=400, detail="User already exists")
    
    user = User(
        email = payload.email,
        full_name = payload.full_name,
        hashed_password = hash_password(payload.password),
        is_admin = payload.is_admin or False,
        allowed_plants = payload.allowed_plants
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user

@router.put("/users/{user_id}", response_model=UserResponse)
def update_user(
    user_id: int,
    payload: UserUpdate,
    db: Session = Depends(get_db),
    admin: User = Depends(check_admin),
):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if payload.email is not None:
        email = payload.email.strip()
        if not email:
            raise HTTPException(status_code=400, detail="Email cannot be empty")
        exists = db.query(User).filter(User.email == email, User.id != user_id).first()
        if exists:
            raise HTTPException(status_code=400, detail="Email already in use")
        user.email = email

    if payload.full_name is not None:
        user.full_name = payload.full_name.strip() or None

    if payload.password is not None:
        password = payload.password.strip()
        if password:
            user.hashed_password = hash_password(password)

    if payload.is_active is not None:
        user.is_active = bool(payload.is_active)

    if payload.is_admin is not None:
        user.is_admin = bool(payload.is_admin)

    if payload.allowed_plants is not None:
        user.allowed_plants = payload.allowed_plants.strip() or None
    elif payload.is_admin is True:
        user.allowed_plants = None

    db.commit()
    db.refresh(user)
    return user

@router.delete("/users/{user_id}", response_model=MessageResponse)
def delete_user(user_id: int, db: Session = Depends(get_db), admin: User = Depends(check_admin)):
    if admin.id == user_id:
        raise HTTPException(status_code=400, detail="Cannot delete self")
        
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    db.delete(user)
    db.commit()
    return MessageResponse(message="User deleted successfully")


@router.delete("/plants/{plant_id}", response_model=MessageResponse)
def delete_plant_forever(
    plant_id: str,
    db: Session = Depends(get_db),
    admin: User = Depends(check_admin),
):
    plant_id = str(plant_id or "").strip()
    if not plant_id:
        raise HTTPException(status_code=400, detail="Plant ID is required")

    plant = db.query(Plant).filter(Plant.plant_id == plant_id).first()
    if not plant:
        raise HTTPException(status_code=404, detail=f"Plant '{plant_id}' not found")

    try:
        # Remove plant from per-user access lists before deleting the plant row.
        users = db.query(User).all()
        for user in users:
            allowed = str(user.allowed_plants or "").strip()
            if not allowed:
                continue
            if allowed == "*":
                continue
            plants = [p.strip() for p in allowed.split(",") if p.strip()]
            if plant_id not in plants:
                continue
            next_plants = [p for p in plants if p != plant_id]
            user.allowed_plants = ",".join(next_plants) if next_plants else None

        # Delete all plant-scoped rows.
        for model in (
            RawDataGeneric,
            DCHierarchyDerived,
            PlantArchitecture,
            EquipmentSpec,
            SupportTicket,
            FaultDiagnostics,
            FaultEpisode,
            FaultEpisodeDay,
            FaultEvent,
            PlantEquipment,
            RawDataStats,
            ScbFaultReview,
            FaultRuntimeSnapshot,
        ):
            db.query(model).filter(model.plant_id == plant_id).delete(synchronize_session=False)

        db.query(FaultCache).filter(
            FaultCache.cache_key.like(f"ds_summary:{plant_id}%")
        ).delete(synchronize_session=False)
        db.query(FaultCache).filter(
            FaultCache.cache_key.like(f"inv_eff:%{plant_id}%")
        ).delete(synchronize_session=False)
        db.query(FaultCache).filter(
            FaultCache.cache_key.like(f"inv_eff_v2:{plant_id}:%")
        ).delete(synchronize_session=False)
        db.query(FaultCache).filter(
            FaultCache.cache_key.like(f"loss_gen_snapshot:{plant_id}:%")
        ).delete(synchronize_session=False)

        db.delete(plant)
        db.commit()
    except Exception:
        db.rollback()
        raise

    try:
        from dashboard_cache import invalidate_plant as invalidate_dashboard_cache_plant
        invalidate_dashboard_cache_plant(plant_id)
    except Exception:
        pass

    # Best-effort cleanup of uploaded spec sheets for the plant.
    try:
        backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        spec_dir = os.path.join(backend_dir, "uploads", "spec_sheets", plant_id)
        if os.path.isdir(spec_dir):
            shutil.rmtree(spec_dir, ignore_errors=True)
    except Exception:
        pass

    return MessageResponse(message=f"Plant '{plant_id}' and all related data were deleted permanently")


@router.put("/plants/{plant_id}", response_model=MessageResponse)
def update_plant_admin(
    plant_id: str,
    payload: PlantAdminUpdate,
    db: Session = Depends(get_db),
    admin: User = Depends(check_admin),
):
    plant_id = str(plant_id or "").strip()
    plant = db.query(Plant).filter(Plant.plant_id == plant_id).first()
    if not plant:
        raise HTTPException(status_code=404, detail=f"Plant '{plant_id}' not found")
    if payload.plant_type is not None:
        plant_type = str(payload.plant_type or "").strip().upper()
        if plant_type not in {"SCB", "MPPT"}:
            raise HTTPException(status_code=400, detail="plant_type must be SCB or MPPT")
        plant.plant_type = plant_type
    db.commit()
    return MessageResponse(message=f"Plant '{plant_id}' updated successfully")

# -- Site appearance (org default theme; stored in fault_cache, no new tables) --
from routers.site import SITE_APPEARANCE_KEY, _normalize_theme_id, ALLOWED_ORG_THEMES


class SiteAppearanceUpdate(BaseModel):
    org_default_theme: str = Field(..., min_length=2, max_length=32)


@router.put("/site-appearance")
def update_site_appearance(
    payload: SiteAppearanceUpdate,
    db: Session = Depends(get_db),
    admin: User = Depends(check_admin),
):
    raw = (payload.org_default_theme or "").strip()
    if raw not in ALLOWED_ORG_THEMES:
        raise HTTPException(status_code=400, detail="Invalid org_default_theme")
    canon = _normalize_theme_id(raw)
    body = {
        "org_default_theme": canon,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    row = db.query(FaultCache).filter(FaultCache.cache_key == SITE_APPEARANCE_KEY).first()
    if row:
        row.payload = json.dumps(body)
    else:
        db.add(FaultCache(cache_key=SITE_APPEARANCE_KEY, payload=json.dumps(body)))
    db.commit()
    return {"ok": True, "org_default_theme": canon, "updated_at": body["updated_at"]}


# -- Analytics precompute (durable queue; worker: `python -m jobs.precompute_runner --once`) --


class PrecomputeEnqueueBody(BaseModel):
    """Queue DS summary + unified fault + loss + fault-tab snapshots for a date range."""

    plant_id: Optional[str] = Field(
        default=None,
        description="If omitted, all plants (uses each plant's raw_data_stats bounds when dates omitted).",
    )
    date_from: Optional[str] = Field(default=None, description="YYYY-MM-DD; optional with date_to")
    date_to: Optional[str] = Field(default=None, description="YYYY-MM-DD; optional with date_from")
    chunk_days: int = Field(
        default=62,
        ge=0,
        le=366,
        description="Split range into N-day jobs (0 = one job per plant for full range).",
    )


@router.get("/precompute/queue")
def get_precompute_queue(
    db: Session = Depends(get_db),
    admin: User = Depends(check_admin),
    limit: int = Query(30, ge=1, le=200),
):
    """Counts and recent precompute_jobs rows (pending / running / done / failed)."""
    pending = db.query(PrecomputeJob).filter(PrecomputeJob.status == "pending").count()
    running = db.query(PrecomputeJob).filter(PrecomputeJob.status == "running").count()
    recent = (
        db.query(PrecomputeJob)
        .order_by(PrecomputeJob.id.desc())
        .limit(limit)
        .all()
    )
    return {
        "pending": pending,
        "running": running,
        "worker_hint": "From backend/: python -m jobs.precompute_runner --once --max-jobs 20  (re-run or schedule until pending=0)",
        "alerts_hint": "Alert if pending grows unbounded or jobs stay running > SOLAR_PRECOMPUTE_STALE_LOCK_MINUTES; see backend/docs/PRECOMPUTE_OPERATIONS.md",
        "recent_jobs": [
            {
                "id": j.id,
                "plant_id": j.plant_id,
                "date_from": j.date_from,
                "date_to": j.date_to,
                "status": j.status,
                "attempts": j.attempts,
                "error_message": (j.error_message or "")[:500] or None,
                "created_at": j.created_at.isoformat() if j.created_at else None,
                "updated_at": j.updated_at.isoformat() if j.updated_at else None,
            }
            for j in recent
        ],
    }


@router.get("/precompute/unified-category-totals")
def get_unified_category_totals(
    plant_id: str = Query(..., description="Plant id"),
    date_from: str = Query(..., description="YYYY-MM-DD (must match precompute key)"),
    date_to: str = Query(..., description="YYYY-MM-DD"),
    db: Session = Depends(get_db),
    admin: User = Depends(check_admin),
):
    """
    Narrow SQL-backed KPI rows written alongside `unified_fault_snapshot` JSON during precompute
    (loss_mwh, fault_count per category_id). For BI/reporting without parsing payload_json.
    """
    d0, d1 = (date_from or "")[:10], (date_to or "")[:10]
    rows = (
        db.query(UnifiedFeedCategoryTotal)
        .filter(
            UnifiedFeedCategoryTotal.plant_id == plant_id,
            UnifiedFeedCategoryTotal.date_from == d0,
            UnifiedFeedCategoryTotal.date_to == d1,
        )
        .order_by(UnifiedFeedCategoryTotal.category_id)
        .all()
    )
    return {
        "plant_id": plant_id,
        "date_from": d0,
        "date_to": d1,
        "rows": [
            {
                "category_id": r.category_id,
                "loss_mwh": r.loss_mwh,
                "fault_count": r.fault_count,
                "computed_at": r.computed_at.isoformat() if r.computed_at else None,
            }
            for r in rows
        ],
    }


@router.post("/precompute/enqueue")
def enqueue_precompute_historical(
    payload: PrecomputeEnqueueBody,
    db: Session = Depends(get_db),
    admin: User = Depends(check_admin),
):
    """
    Enqueue one or more snapshot jobs in `precompute_jobs` (no merge — supports historical chunks).

    Processes DS summary, unified feed, loss bridge, and fault tab caches
    (via `module_precompute.compute_snapshots_for_range` + underlying engines).
    """
    if os.environ.get("SOLAR_MODULE_PRECOMPUTE", "1").strip().lower() in ("0", "false", "no"):
        raise HTTPException(
            status_code=400,
            detail="Module precompute is disabled (SOLAR_MODULE_PRECOMPUTE=0). Enable it to enqueue jobs.",
        )
    if bool(payload.date_from) != bool(payload.date_to):
        raise HTTPException(
            status_code=400,
            detail="Provide both date_from and date_to, or leave both empty to use each plant's raw data bounds.",
        )
    from jobs.enqueue import enqueue_historical_backfill

    try:
        out = enqueue_historical_backfill(
            db,
            plant_id=(payload.plant_id or None),
            date_from=payload.date_from,
            date_to=payload.date_to,
            chunk_days=payload.chunk_days,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    if out.get("jobs_enqueued", 0) == 0:
        raise HTTPException(
            status_code=400,
            detail="No jobs were enqueued. Check that plants exist and raw_data_stats has min_ts/max_ts for the selected plant(s), or pass explicit date_from and date_to.",
        )
    return {**out, "worker_hint": "Run: python -m jobs.precompute_runner --once --max-jobs 20 (from the backend/ folder)"}
