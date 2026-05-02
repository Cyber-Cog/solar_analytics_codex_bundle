"""
Helpers for DB-backed module snapshots (faults DS summary, unified feed, loss bridge).

Freshness: snapshot is used only when computed_at >= raw_data_stats.updated_at for the plant.

Writes use PostgreSQL ON CONFLICT DO UPDATE (UPSERT) to avoid duplicate rows.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Optional, Tuple

from sqlalchemy import delete
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from models import (
    DsSummarySnapshot,
    DsStatusSnapshot,
    LossAnalysisSnapshot,
    RawDataStats,
    UnifiedFaultSnapshot,
    UnifiedFeedCategoryTotal,
)

# HTTP 503 bodies for SOLAR_SNAPSHOT_READ_ONLY routes (shared by faults + loss_analysis).
SNAPSHOT_READ_ONLY_HTTP_DETAIL = {
    "error": "snapshot_unavailable",
    "message": (
        "No precomputed snapshot for this plant and date range. "
        "Run `python -m jobs.precompute_runner` (or wait for the ingest enqueue), "
        "or set SOLAR_SNAPSHOT_READ_ONLY=0 for request-time compute."
    ),
}
SNAPSHOT_STALE_HTTP_DETAIL = {
    "error": "snapshot_stale",
    "message": (
        "Snapshot exists but is older than the latest raw data refresh. "
        "Run the precompute worker, or set SOLAR_SNAPSHOT_ALLOW_STALE=1 to return stale payloads."
    ),
}


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


def get_ds_status_snapshot(db: Session, plant_id: str, date_from: str, date_to: str) -> Optional[dict]:
    row = (
        db.query(DsStatusSnapshot)
        .filter(
            DsStatusSnapshot.plant_id == plant_id,
            DsStatusSnapshot.date_from == (date_from or ""),
            DsStatusSnapshot.date_to == (date_to or ""),
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


def save_ds_status_snapshot(db: Session, plant_id: str, date_from: str, date_to: str, payload: dict) -> None:
    try:
        key_from = date_from or ""
        key_to = date_to or ""
        body = json.dumps(payload)
        now = datetime.utcnow()
        stmt = insert(DsStatusSnapshot).values(
            plant_id=plant_id,
            date_from=key_from,
            date_to=key_to,
            payload_json=body,
            computed_at=now,
        ).on_conflict_do_update(
            constraint="uq_ds_status_snapshot",
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
    for model in (DsSummarySnapshot, DsStatusSnapshot, UnifiedFaultSnapshot, LossAnalysisSnapshot):
        q = delete(model).where(model.computed_at < cutoff)
        if plant_id:
            q = q.where(model.plant_id == plant_id)
        r = db.execute(q)
        deleted += r.rowcount or 0
    q2 = delete(UnifiedFeedCategoryTotal).where(UnifiedFeedCategoryTotal.computed_at < cutoff)
    if plant_id:
        q2 = q2.where(UnifiedFeedCategoryTotal.plant_id == plant_id)
    r2 = db.execute(q2)
    deleted += r2.rowcount or 0
    db.commit()
    return deleted


# ── Read-only / serverless: serve snapshots only (no heavy request-time compute) ─────────────


def snapshot_read_only_enabled() -> bool:
    """If true, fault/loss routes that support it must not run heavy compute; use DB snapshots only."""
    # Hardcoded to False so users can select custom date ranges without getting 503 snapshot errors.
    return False


def snapshot_allow_stale() -> bool:
    """When read-only: if true, return last snapshot with _snapshot_meta.stale when anchor is newer than computed_at."""
    v = (os.environ.get("SOLAR_SNAPSHOT_ALLOW_STALE") or "1").strip().lower()
    return v in ("1", "true", "yes", "on")


def attach_snapshot_stale_meta(payload: dict, *, computed_at_iso: Optional[str] = None) -> dict:
    out = {**payload, "_snapshot_meta": {"stale": True}}
    if computed_at_iso:
        out["_snapshot_meta"]["computed_at"] = computed_at_iso
    out["_snapshot_meta"]["message"] = (
        "Snapshot predates latest raw data refresh; run the precompute worker to refresh."
    )
    return out


def _load_row_json_payload(row, plant_id: str, db: Session) -> Optional[Tuple[dict, bool]]:
    if not row or not getattr(row, "payload_json", None):
        return None
    try:
        body = json.loads(row.payload_json)
    except Exception:
        return None
    fresh = is_snapshot_fresh(db, plant_id, row.computed_at)
    return (body, fresh)


def load_unified_fault_snapshot_any(
    db: Session, plant_id: str, date_from: str, date_to: str
) -> Optional[Tuple[dict, bool]]:
    """Return (payload, is_fresh) if a row exists; else None."""
    row = (
        db.query(UnifiedFaultSnapshot)
        .filter(
            UnifiedFaultSnapshot.plant_id == plant_id,
            UnifiedFaultSnapshot.date_from == date_from,
            UnifiedFaultSnapshot.date_to == date_to,
        )
        .first()
    )
    return _load_row_json_payload(row, plant_id, db)


def load_ds_summary_snapshot_any(
    db: Session, plant_id: str, date_from: str, date_to: str
) -> Optional[Tuple[dict, bool]]:
    row = (
        db.query(DsSummarySnapshot)
        .filter(
            DsSummarySnapshot.plant_id == plant_id,
            DsSummarySnapshot.date_from == (date_from or ""),
            DsSummarySnapshot.date_to == (date_to or ""),
        )
        .first()
    )
    return _load_row_json_payload(row, plant_id, db)


def load_ds_status_snapshot_any(
    db: Session, plant_id: str, date_from: str, date_to: str
) -> Optional[Tuple[dict, bool]]:
    row = (
        db.query(DsStatusSnapshot)
        .filter(
            DsStatusSnapshot.plant_id == plant_id,
            DsStatusSnapshot.date_from == (date_from or ""),
            DsStatusSnapshot.date_to == (date_to or ""),
        )
        .first()
    )
    return _load_row_json_payload(row, plant_id, db)


def load_loss_analysis_snapshot_any(
    db: Session,
    plant_id: str,
    date_from: str,
    date_to: str,
    scope: str,
    equipment_id: str,
) -> Optional[Tuple[dict, bool]]:
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
    return _load_row_json_payload(row, plant_id, db)


def upsert_unified_feed_category_totals_from_payload(
    db: Session, plant_id: str, date_from: str, date_to: str, unified_payload: dict
) -> None:
    """
    Write narrow rows for BI/reporting (same loss_mwh / fault_count as unified_feed JSON).
    Replaces all category rows for this plant+range.
    """
    categories = unified_payload.get("categories")
    if not isinstance(categories, list) or not categories:
        return
    df, dt = (date_from or "")[:10], (date_to or "")[:10]
    now = datetime.utcnow()
    try:
        db.query(UnifiedFeedCategoryTotal).filter(
            UnifiedFeedCategoryTotal.plant_id == plant_id,
            UnifiedFeedCategoryTotal.date_from == df,
            UnifiedFeedCategoryTotal.date_to == dt,
        ).delete(synchronize_session=False)
        for c in categories:
            cid = str(c.get("id") or "").strip()
            if not cid:
                continue
            db.add(
                UnifiedFeedCategoryTotal(
                    plant_id=plant_id,
                    date_from=df,
                    date_to=dt,
                    category_id=cid,
                    loss_mwh=float(c.get("loss_mwh") or 0.0),
                    fault_count=int(c.get("fault_count") or 0),
                    computed_at=now,
                )
            )
        db.commit()
    except Exception:
        db.rollback()
