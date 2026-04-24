"""
Legacy hook: enqueue a durable precompute job (no in-process compute).

Prefer `jobs.enqueue.enqueue_precompute_after_ingest` from API paths.
Worker: `python -m jobs.precompute_runner --once` (cron / scheduler).
"""

from __future__ import annotations

import logging
import os
from typing import Optional

log = logging.getLogger(__name__)


def run_post_ingest_precompute(
    plant_id: str,
    min_ts: Optional[str] = None,
    max_ts: Optional[str] = None,
) -> None:
    if os.environ.get("SOLAR_MODULE_PRECOMPUTE", "1").strip().lower() in ("0", "false", "no"):
        return
    from database import SessionLocal
    from jobs.enqueue import enqueue_precompute_after_ingest

    db = SessionLocal()
    try:
        enqueue_precompute_after_ingest(db, plant_id, min_ts, max_ts)
    finally:
        db.close()
