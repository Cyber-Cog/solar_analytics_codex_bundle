"""
DB-backed cache for expensive raw-data fault tab payloads.
Complements in-memory dashboard_cache (180s TTL); rows persist across process restarts
until raw upload calls clear_snapshots_for_plant.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from models import FaultRuntimeSnapshot, RawDataStats

KIND_PL_PAGE = "pl_page"
KIND_IS_TAB = "is_tab"
KIND_GB_TAB = "gb_tab"
KIND_COMM_TAB = "comm_tab"
KIND_CD_TAB = "cd_tab"


def _fresh_vs_raw_stats(db: Session, plant_id: str, updated_at: Optional[datetime]) -> bool:
    if updated_at is None:
        return False
    stats = db.query(RawDataStats).filter(RawDataStats.plant_id == plant_id).first()
    if not stats or not stats.updated_at:
        return False
    snap_ts = updated_at
    anchor = stats.updated_at
    if snap_ts.tzinfo is not None:
        snap_ts = snap_ts.replace(tzinfo=None)
    if anchor.tzinfo is not None:
        anchor = anchor.replace(tzinfo=None)
    return snap_ts >= anchor


def try_snapshot_payload(
    db: Session, plant_id: str, date_from: str, date_to: str, kind: str
) -> Optional[Dict[str, Any]]:
    row = (
        db.query(FaultRuntimeSnapshot)
        .filter(
            FaultRuntimeSnapshot.plant_id == plant_id,
            FaultRuntimeSnapshot.date_from == date_from,
            FaultRuntimeSnapshot.date_to == date_to,
            FaultRuntimeSnapshot.kind == kind,
        )
        .first()
    )
    if not row or not _fresh_vs_raw_stats(db, plant_id, row.updated_at):
        return None
    try:
        return json.loads(row.payload_json)
    except Exception:
        return None


def save_snapshot_payload(
    db: Session, plant_id: str, date_from: str, date_to: str, kind: str, payload: Dict[str, Any]
) -> None:
    raw = json.dumps(payload)
    row = (
        db.query(FaultRuntimeSnapshot)
        .filter(
            FaultRuntimeSnapshot.plant_id == plant_id,
            FaultRuntimeSnapshot.date_from == date_from,
            FaultRuntimeSnapshot.date_to == date_to,
            FaultRuntimeSnapshot.kind == kind,
        )
        .first()
    )
    if row:
        row.payload_json = raw
        row.updated_at = datetime.now(timezone.utc)
    else:
        db.add(
            FaultRuntimeSnapshot(
                plant_id=plant_id,
                date_from=date_from,
                date_to=date_to,
                kind=kind,
                payload_json=raw,
            )
        )
    db.commit()


def clear_snapshots_for_plant(db: Session, plant_id: str) -> None:
    db.query(FaultRuntimeSnapshot).filter(FaultRuntimeSnapshot.plant_id == plant_id).delete()
    db.commit()
