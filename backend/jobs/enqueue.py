"""
Enqueue precompute jobs after raw-data ingest (DB row — no in-process threads).

Workers: `python -m jobs.precompute_runner --once` (cron / systemd / ECS scheduled task).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from models import PrecomputeJob
from module_precompute import resolve_recompute_day_range

log = logging.getLogger(__name__)


def enqueue_precompute_after_ingest(
    db: Session,
    plant_id: str,
    min_ts: Optional[str],
    max_ts: Optional[str],
) -> None:
    if os.environ.get("SOLAR_MODULE_PRECOMPUTE", "1").strip().lower() in ("0", "false", "no"):
        return

    df, dt = resolve_recompute_day_range(db, plant_id, min_ts, max_ts)
    pending = (
        db.query(PrecomputeJob)
        .filter(PrecomputeJob.plant_id == plant_id, PrecomputeJob.status == "pending")
        .order_by(PrecomputeJob.id.asc())
        .first()
    )
    if pending:
        pending.date_from = min(pending.date_from, df)
        pending.date_to = max(pending.date_to, dt)
        pending.updated_at = datetime.utcnow()
        log.info("precompute_job_merged plant=%s range=%s..%s", plant_id, pending.date_from, pending.date_to)
    else:
        db.add(
            PrecomputeJob(
                plant_id=plant_id,
                date_from=df,
                date_to=dt,
                status="pending",
                attempts=0,
                max_attempts=int(os.environ.get("SOLAR_PRECOMPUTE_MAX_ATTEMPTS", "5")),
            )
        )
        log.info("precompute_job_enqueued plant=%s range=%s..%s", plant_id, df, dt)
    db.commit()


def add_isolated_precompute_job(
    db: Session,
    plant_id: str,
    date_from: str,
    date_to: str,
) -> None:
    """
    Insert a new pending job without merging into existing pending rows.

    Use for chunked historical backfill so each date window is processed
    separately by the worker (avoids one multi-year job timing out).
    """
    d0, d1 = (date_from or "")[:10], (date_to or "")[:10]
    if d0 and d1 and d0 > d1:
        d0, d1 = d1, d0
    db.add(
        PrecomputeJob(
            plant_id=plant_id,
            date_from=d0,
            date_to=d1,
            status="pending",
            attempts=0,
            max_attempts=int(os.environ.get("SOLAR_PRECOMPUTE_MAX_ATTEMPTS", "5")),
        )
    )


def enqueue_historical_backfill(
    db: Session,
    *,
    plant_id: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    chunk_days: int = 62,
) -> dict:
    """
    Enqueue precompute for existing raw data (one or all plants).

    Date bounds default to raw_data_stats min/max per plant when omitted.
    When chunk_days > 0, split into consecutive windows; chunk_days=0 means
    one job per plant covering the full range.
    """
    from datetime import date, timedelta
    from models import Plant, RawDataStats

    if chunk_days < 0:
        chunk_days = 62
    if plant_id:
        p = db.query(Plant).filter(Plant.plant_id == str(plant_id).strip()).first()
        if not p:
            raise ValueError(f"Plant not found: {plant_id}")
        plant_list = [p]
    else:
        plant_list = db.query(Plant).all()

    total_jobs = 0
    touched: list[str] = []
    for p in plant_list:
        pid = p.plant_id
        d0, d1 = None, None
        if date_from and date_to:
            d0, d1 = str(date_from).strip()[:10], str(date_to).strip()[:10]
        else:
            st = db.query(RawDataStats).filter(RawDataStats.plant_id == pid).first()
            if not st or not st.min_ts or not st.max_ts:
                log.info("precompute_historical_skip_no_stats plant=%s", pid)
                continue
            d0, d1 = str(st.min_ts)[:10], str(st.max_ts)[:10]
        if not d0 or not d1:
            continue
        if d0 > d1:
            d0, d1 = d1, d0
        touched.append(pid)
        a = date.fromisoformat(d0)
        b = date.fromisoformat(d1)
        if chunk_days == 0:
            add_isolated_precompute_job(db, pid, d0, d1)
            total_jobs += 1
        else:
            cur = a
            while cur <= b:
                end = min(cur + timedelta(days=chunk_days - 1), b)
                add_isolated_precompute_job(
                    db, pid, cur.isoformat(), end.isoformat()
                )
                total_jobs += 1
                cur = end + timedelta(days=1)
    db.commit()
    return {
        "ok": True,
        "jobs_enqueued": total_jobs,
        "plants_touched": touched,
    }
