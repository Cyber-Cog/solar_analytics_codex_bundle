"""
In-memory cache for fault/analytics API results so repeated requests are fast.
Uses DB table fault_cache for persistence across restarts.
TTL: 10 minutes for DS summary, 15 minutes for inverter efficiency.
Cache is invalidated when new fault diagnostics are written for a plant.
"""

import json
from datetime import datetime, timedelta
from sqlalchemy.orm import Session

from models import FaultCache

# Minutes after which cached entry is considered stale
TTL_DS_SUMMARY_MIN = 10
TTL_INV_EFF_MIN = 15  # exported for use in routers
TTL_LOSS_GEN_SNAPSHOT_MIN = 15  # dashboard expected vs actual (same table as other analytics caches)


def _cache_key_ds(plant_id: str, date_from: str = None, date_to: str = None) -> str:
    return f"ds_summary:{plant_id}:{date_from or ''}:{date_to or ''}"


def _cache_key_inv_eff(plant_id: str, date_from: str, date_to: str) -> str:
    return f"inv_eff_v2:{plant_id}:{date_from}:{date_to}"


def get_cached(db: Session, key: str, ttl_minutes: int):
    row = db.query(FaultCache).filter(FaultCache.cache_key == key).first()
    if not row or not row.payload:
        return None
    try:
        created = row.created_at
        if isinstance(created, str):
            created = datetime.fromisoformat(created.replace("Z", "+00:00").replace("+00:00", ""))
        if created.tzinfo:
            created = created.replace(tzinfo=None)
        if (datetime.utcnow() - created).total_seconds() > ttl_minutes * 60:
            db.delete(row)
            db.commit()
            return None
        return json.loads(row.payload)
    except Exception:
        return None


def set_cached(db: Session, key: str, payload: dict) -> None:
    try:
        existing = db.query(FaultCache).filter(FaultCache.cache_key == key).first()
        payload_str = json.dumps(payload)
        if existing:
            existing.payload = payload_str
            existing.created_at = datetime.utcnow()
        else:
            db.add(FaultCache(cache_key=key, payload=payload_str))
        db.commit()
    except Exception:
        db.rollback()


def cache_key_loss_gen_snapshot(plant_id: str, date_from: str, date_to: str) -> str:
    return f"loss_gen_snapshot:{plant_id}:{date_from}:{date_to}"


def invalidate_loss_gen_snapshots(db: Session, plant_id: str) -> None:
    """Clear persisted expected/actual snapshot rows for a plant (metadata or raw data changed)."""
    try:
        db.query(FaultCache).filter(
            FaultCache.cache_key.like(f"loss_gen_snapshot:{plant_id}:%")
        ).delete(synchronize_session=False)
        db.commit()
    except Exception:
        db.rollback()


def invalidate_plant(db: Session, plant_id: str) -> None:
    """Remove all cached fault/analytics entries for this plant (call after writing new fault_diagnostics)."""
    try:
        db.query(FaultCache).filter(
            FaultCache.cache_key.like(f"ds_summary:{plant_id}%")
        ).delete(synchronize_session=False)
        db.query(FaultCache).filter(
            FaultCache.cache_key.like(f"inv_eff:{plant_id}%")
        ).delete(synchronize_session=False)
        db.query(FaultCache).filter(
            FaultCache.cache_key.like(f"loss_gen_snapshot:{plant_id}:%")
        ).delete(synchronize_session=False)
        db.commit()
    except Exception:
        db.rollback()
