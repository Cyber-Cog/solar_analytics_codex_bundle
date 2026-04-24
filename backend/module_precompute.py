"""
Incremental module snapshot recompute (faults DS summary, unified feed, loss bridge).

Uses ingest min/max timestamps (or raw_data_stats) — not a fixed 7-day window.
Invoked by the DB-backed job runner (`python -m jobs.precompute_runner`).
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, Optional, Tuple

from sqlalchemy.orm import Session

from models import PlantComputeStatus, RawDataStats, User
from module_snapshots import (
    apply_snapshot_retention,
    save_ds_summary_snapshot,
    save_loss_analysis_snapshot,
    save_unified_fault_snapshot,
)
from snap_perf import Timer

log = logging.getLogger(__name__)


def resolve_recompute_day_range(
    db: Session,
    plant_id: str,
    min_ts: Optional[str],
    max_ts: Optional[str],
) -> Tuple[str, str]:
    """
    Derive inclusive YYYY-MM-DD bounds from ingest timestamps, else raw_data_stats,
    else today (single day fallback — avoids unbounded history).
    """
    d0 = (min_ts or "").strip()[:10] if min_ts else None
    d1 = (max_ts or "").strip()[:10] if max_ts else None
    stats = db.query(RawDataStats).filter(RawDataStats.plant_id == plant_id).first()
    if stats and stats.min_ts and stats.max_ts:
        smin, smax = str(stats.min_ts)[:10], str(stats.max_ts)[:10]
        d0 = d0 or smin
        d1 = d1 or smax
    if not d0 or not d1:
        t = date.today()
        return str(t), str(t)
    if d0 > d1:
        d0, d1 = d1, d0
    max_span = int(os.environ.get("SOLAR_PRECOMPUTE_MAX_SPAN_DAYS", "366"))
    try:
        a = date.fromisoformat(d0)
        b = date.fromisoformat(d1)
        if (b - a).days > max_span:
            b = a + timedelta(days=max_span)
            d1 = b.isoformat()
            log.warning(
                "precompute span capped plant=%s from=%s to=%s max_days=%s",
                plant_id, d0, d1, max_span,
            )
    except ValueError:
        pass
    return d0, d1


def compute_snapshots_for_range(
    db: Session,
    plant_id: str,
    date_from: str,
    date_to: str,
    user: User,
) -> Dict[str, Any]:
    """
    Recompute and UPSERT snapshots for one plant + inclusive date range.
    Returns simple metrics for logging/monitoring.
    """
    from routers.faults import build_ds_summary_dict, _unified_feed_rows_and_categories
    from routers.loss_analysis import build_loss_bridge_payload

    t0 = time.perf_counter()
    rows_hint = 0
    try:
        st = db.query(RawDataStats).filter(RawDataStats.plant_id == plant_id).first()
        rows_hint = int(st.total_rows or 0) if st else 0
    except Exception:
        pass

    with Timer("ds_summary_snapshot", f"plant={plant_id}"):
        ds_payload = build_ds_summary_dict(db, plant_id, date_from, date_to)
        save_ds_summary_snapshot(db, plant_id, date_from, date_to, ds_payload)

    with Timer("unified_fault_snapshot", f"plant={plant_id}"):
        unified_payload = _unified_feed_rows_and_categories(db, plant_id, date_from, date_to, user)
        save_unified_fault_snapshot(db, plant_id, date_from, date_to, unified_payload)

    with Timer("loss_bridge_snapshot", f"plant={plant_id}"):
        loss_payload = build_loss_bridge_payload(
            db, plant_id, date_from, date_to, "plant", None, user
        )
        if isinstance(loss_payload, dict) and not loss_payload.get("error"):
            save_loss_analysis_snapshot(db, plant_id, date_from, date_to, "plant", "", loss_payload)

    every = int(os.environ.get("SNAPSHOT_RETENTION_EVERY_N_JOBS", "1"))
    if every > 0 and (hash(plant_id) % max(every, 1)) == 0:
        try:
            deleted = apply_snapshot_retention(db, plant_id=None)
            if deleted:
                log.info("snapshot_retention_deleted_rows=%s", deleted)
        except Exception:
            log.exception("snapshot_retention_failed")

    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    log.info(
        "module_precompute_done plant=%s range=%s..%s total_ms=%.1f raw_rows_hint=%s",
        plant_id, date_from, date_to, elapsed_ms, rows_hint,
    )
    return {
        "plant_id": plant_id,
        "date_from": date_from,
        "date_to": date_to,
        "total_ms": round(elapsed_ms, 1),
        "raw_rows_hint": rows_hint,
    }


def update_plant_compute_status(
    db: Session,
    plant_id: str,
    *,
    status: str,
    date_from: str,
    date_to: str,
    duration_seconds: Optional[int] = None,
    error_message: Optional[str] = None,
) -> None:
    row = db.query(PlantComputeStatus).filter(PlantComputeStatus.plant_id == plant_id).first()
    if not row:
        row = PlantComputeStatus(plant_id=plant_id)
        db.add(row)
    row.status = status
    row.last_range_json = json.dumps({"date_from": date_from, "date_to": date_to})
    if status == "running":
        row.started_at = datetime.utcnow()
        row.finished_at = None
        row.error_message = None
        row.duration_seconds = None
    else:
        row.finished_at = datetime.utcnow()
        row.duration_seconds = duration_seconds
        row.error_message = (error_message or None)[:4000] if error_message else None
    db.commit()
