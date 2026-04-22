"""
backend/routers/admin.py
========================
Admin-only routes for user management and plant access control.
"""

import os
import shutil

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from database import get_db
from models import (
    User, Plant, RawDataGeneric, DCHierarchyDerived, PlantArchitecture,
    EquipmentSpec, SupportTicket, FaultDiagnostics, FaultEpisode,
    FaultEpisodeDay, PlantEquipment, RawDataStats, ScbFaultReview,
    FaultRuntimeSnapshot, FaultCache,
)
from schemas import UserCreate, UserUpdate, UserResponse, MessageResponse
from auth.routes import get_current_user
from auth.jwt import hash_password

router = APIRouter(prefix="/api/admin", tags=["Admin"])

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
