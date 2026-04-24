"""
Helpers for DB-backed module snapshots (faults DS summary, unified feed, loss bridge).

Freshness: snapshot is used only when computed_at >= raw_data_stats.updated_at for the plant.

Writes use PostgreSQL ON CONFLICT DO UPDATE (UPSERT) to avoid duplicate rows.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import delete
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from models import (
    DsSummarySnapshot,
    LossAnalysisSnapshot,
    RawDataStats,
    UnifiedFaultSnapshot,
)


def stats_anchor_time(db: Session, plant_id: str) -> Optional[datetime]:
    row = db.query(RawDataStats).filter(RawDataStats.plant_id == plant_id).first()
    if not row or not row.updated_at:
        return None
    return row.updated_at


def is_snapshot_fresh(db: Session, plant_id: str, computed_at: Optional[datetime]) -> bool:
    if computed_at is None:
        return False
    anchor = stats_anchor_time(db, plant_id)
    if anchor is None:
        return False
    return computed_at >= anchor


def get_ds_summary_snapshot(db: Session, plant_id: str, date_from: str, date_to: str) -> Optional[dict]:
    row = (
        db.query(DsSummarySnapshot)
        .filter(
            DsSummarySnapshot.plant_id == plant_id,
            DsSummarySnapshot.date_from == (date_from or ""),
            DsSummarySnapshot.date_to == (date_to or ""),
        )
        .first()
    )
    if not row or not row.payload_json:
        return None
    if not is_snapshot_fresh(db, plant_id, row.computed_at):
        return None
    try:
        return json.loads(row.payload_json)
    except Exception:
        return None


def save_ds_summary_snapshot(db: Session, plant_id: str, date_from: str, date_to: str, payload: dict) -> None:
    try:
        key_from = date_from or ""
        key_to = date_to or ""
        body = json.dumps(payload)
        now = datetime.utcnow()
        stmt = insert(DsSummarySnapshot).values(
            plant_id=plant_id,
            date_from=key_from,
            date_to=key_to,
            payload_json=body,
            computed_at=now,
        ).on_conflict_do_update(
            constraint="uq_ds_summary_snapshot",
            set_={"payload_json": body, "computed_at": now},
        )
        db.execute(stmt)
        db.commit()
    except Exception:
        db.rollback()


def get_unified_fault_snapshot(db: Session, plant_id: str, date_from: str, date_to: str) -> Optional[dict]:
    row = (
        db.query(UnifiedFaultSnapshot)
        .filter(
            UnifiedFaultSnapshot.plant_id == plant_id,
            UnifiedFaultSnapshot.date_from == date_from,
            UnifiedFaultSnapshot.date_to == date_to,
        )
        .first()
    )
    if not row or not row.payload_json:
        return None
    if not is_snapshot_fresh(db, plant_id, row.computed_at):
        return None
    try:
        return json.loads(row.payload_json)
    except Exception:
        return None


def save_unified_fault_snapshot(db: Session, plant_id: str, date_from: str, date_to: str, payload: dict) -> None:
    try:
        body = json.dumps(payload)
        now = datetime.utcnow()
        stmt = insert(UnifiedFaultSnapshot).values(
            plant_id=plant_id,
            date_from=date_from,
            date_to=date_to,
            payload_json=body,
            computed_at=now,
        ).on_conflict_do_update(
            constraint="uq_unified_fault_snapshot",
            set_={"payload_json": body, "computed_at": now},
        )
        db.execute(stmt)
        db.commit()
    except Exception:
        db.rollback()


def get_loss_analysis_snapshot(
    db: Session,
    plant_id: str,
    date_from: str,
    date_to: str,
    scope: str,
    equipment_id: str,
) -> Optional[dict]:
    eq = (equipment_id or "").strip()
    row = (
        db.query(LossAnalysisSnapshot)
        .filter(
            LossAnalysisSnapshot.plant_id == plant_id,
            LossAnalysisSnapshot.date_from == date_from,
            LossAnalysisSnapshot.date_to == date_to,
            LossAnalysisSnapshot.scope == (scope or "plant").strip().lower(),
            LossAnalysisSnapshot.equipment_id == eq,
        )
        .first()
    )
    if not row or not row.payload_json:
        return None
    if not is_snapshot_fresh(db, plant_id, row.computed_at):
        return None
    try:
        return json.loads(row.payload_json)
    except Exception:
        return None


def save_loss_analysis_snapshot(
    db: Session,
    plant_id: str,
    date_from: str,
    date_to: str,
    scope: str,
    equipment_id: str,
    payload: dict,
) -> None:
    try:
        sc = (scope or "plant").strip().lower()
        eq = (equipment_id or "").strip()
        body = json.dumps(payload)
        now = datetime.utcnow()
        stmt = insert(LossAnalysisSnapshot).values(
            plant_id=plant_id,
            date_from=date_from,
            date_to=date_to,
            scope=sc,
            equipment_id=eq,
            payload_json=body,
            computed_at=now,
        ).on_conflict_do_update(
            constraint="uq_loss_analysis_snapshot",
            set_={"payload_json": body, "computed_at": now},
        )
        db.execute(stmt)
        db.commit()
    except Exception:
        db.rollback()


def apply_snapshot_retention(db: Session, plant_id: Optional[str] = None) -> int:
    """
    Delete snapshot rows older than SNAP_RETENTION_DAYS (default 120).
    If plant_id is set, only that plant is pruned.
    """
    days = int(os.environ.get("SNAPSHOT_RETENTION_DAYS", "120"))
    cutoff = datetime.utcnow() - timedelta(days=max(days, 7))
    deleted = 0
    for model in (DsSummarySnapshot, UnifiedFaultSnapshot, LossAnalysisSnapshot):
        q = delete(model).where(model.computed_at < cutoff)
        if plant_id:
            q = q.where(model.plant_id == plant_id)
        r = db.execute(q)
        deleted += r.rowcount or 0
    db.commit()
    return deleted
