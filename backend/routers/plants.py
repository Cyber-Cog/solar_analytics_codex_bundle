"""
backend/routers/plants.py
===========================
Plant management API — list, create, and get plant details.
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from database import get_db
from models import Plant, User
from schemas import PlantCreate, PlantResponse
from auth.routes import get_current_user

router = APIRouter(prefix="/api/plants", tags=["Plants"])


@router.get("", response_model=List[PlantResponse])
def list_plants(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Return all plants, sorted ascending by name."""
    query = db.query(Plant)
    if not current_user.is_admin:
        if current_user.allowed_plants:
            allowed = [p.strip() for p in current_user.allowed_plants.split(",")]
            query = query.filter(Plant.plant_id.in_(allowed))
        else:
            # Fallback to owner_id if no explicit allowed_plants string
            query = query.filter(Plant.owner_id == current_user.id)
            
    return query.order_by(Plant.name.asc()).all()


@router.get("/{plant_id}", response_model=PlantResponse)
def get_plant(
    plant_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Return a single plant by plant_id."""
    plant = db.query(Plant).filter(Plant.plant_id == plant_id).first()
    if not plant:
        raise HTTPException(status_code=404, detail=f"Plant '{plant_id}' not found")
    return plant


@router.post("", response_model=PlantResponse, status_code=201)
def create_plant(
    payload: PlantCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Create a new plant entry."""
    existing = db.query(Plant).filter(Plant.plant_id == payload.plant_id).first()
    if existing:
        raise HTTPException(status_code=400, detail="Plant ID already exists")

    plant = Plant(**payload.model_dump(), owner_id=current_user.id)
    db.add(plant)
    db.commit()
    db.refresh(plant)
    return plant
