"""
CLI worker: claims one `precompute_jobs` row (SKIP LOCKED), runs incremental snapshot recompute.

From the `backend/` directory:
  python -m jobs.precompute_runner --once
  python -m jobs.precompute_runner --once --max-jobs 5

Schedule with cron, systemd timer, or a cloud scheduler. Survives API restarts;
multiple workers coordinate via PostgreSQL row locks.
"""

from __future__ import annotations

import argparse
import logging
import os
import socket
import sys
import time
from datetime import datetime

_BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
log = logging.getLogger("jobs.precompute_runner")


def _load_env() -> None:
    env_path = os.path.join(_BACKEND, ".env")
    if not os.path.isfile(env_path):
        return
    for line in open(env_path, encoding="utf-8", errors="ignore"):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            if k.strip() not in os.environ:
                os.environ[k.strip()] = v.strip()


def reset_stale_running_jobs(conn) -> int:
    from sqlalchemy import text

    mins = int(os.environ.get("SOLAR_PRECOMPUTE_STALE_LOCK_MINUTES", "45"))
    r = conn.execute(
        text(
            """
            UPDATE precompute_jobs
               SET status = 'pending',
                   locked_at = NULL,
                   worker_id = NULL,
                   updated_at = now(),
                   error_message = COALESCE(error_message,'') || ' [stale lock reset]'
             WHERE status = 'running'
               AND locked_at < (now() - ((:m)::int * interval '1 minute'))
            """
        ),
        {"m": mins},
    )
    return r.rowcount or 0


def claim_next_job(conn, worker_id: str):
    from sqlalchemy import text

    return conn.execute(
        text(
            """
            WITH c AS (
              SELECT id FROM precompute_jobs
              WHERE status = 'pending'
              ORDER BY created_at ASC
              FOR UPDATE SKIP LOCKED
              LIMIT 1
            )
            UPDATE precompute_jobs j
               SET status = 'running',
                   locked_at = now(),
                   updated_at = now(),
                   worker_id = :wid
              FROM c
             WHERE j.id = c.id
         RETURNING j.id, j.plant_id, j.date_from, j.date_to, j.attempts, j.max_attempts
            """
        ),
        {"wid": worker_id},
    ).fetchone()


def run_job(row) -> None:
    from database import SessionLocal
    from models import PrecomputeJob, User
    from module_precompute import compute_snapshots_for_range, update_plant_compute_status

    job_id = int(row[0])
    plant_id = str(row[1])
    date_from = str(row[2])
    date_to = str(row[3])
    max_attempts = int(row[5] or 5)

    db = SessionLocal()
    t0 = time.monotonic()
    try:
        update_plant_compute_status(db, plant_id, status="running", date_from=date_from, date_to=date_to)
        user = db.query(User).first()
        if user is None:
            raise RuntimeError("No users in database; cannot run precompute")
        compute_snapshots_for_range(db, plant_id, date_from, date_to, user)
        elapsed = int(time.monotonic() - t0)
        update_plant_compute_status(
            db,
            plant_id,
            status="done",
            date_from=date_from,
            date_to=date_to,
            duration_seconds=elapsed,
            error_message=None,
        )
        db2 = SessionLocal()
        try:
            job = db2.query(PrecomputeJob).filter(PrecomputeJob.id == job_id).first()
            if job:
                job.status = "done"
                job.locked_at = None
                job.worker_id = None
                job.error_message = None
                job.updated_at = datetime.utcnow()
                db2.commit()
        finally:
            db2.close()
        log.info(
            "precompute_job_done id=%s plant=%s range=%s..%s s=%s",
            job_id, plant_id, date_from, date_to, elapsed,
        )
    except Exception as exc:
        log.exception("precompute_job_fail id=%s plant=%s", job_id, plant_id)
        try:
            elapsed = int(time.monotonic() - t0)
            update_plant_compute_status(
                db,
                plant_id,
                status="error",
                date_from=date_from,
                date_to=date_to,
                duration_seconds=elapsed,
                error_message=str(exc),
            )
        except Exception:
            db.rollback()
        db3 = SessionLocal()
        try:
            job = db3.query(PrecomputeJob).filter(PrecomputeJob.id == job_id).first()
            if job:
                job.attempts = int(job.attempts or 0) + 1
                job.error_message = (str(exc) or "")[:4000]
                job.status = "failed" if job.attempts >= int(job.max_attempts or max_attempts) else "pending"
                job.locked_at = None
                job.worker_id = None
                job.updated_at = datetime.utcnow()
                db3.commit()
        finally:
            db3.close()
    finally:
        db.close()


def run_once(engine, max_jobs: int) -> int:
    worker_id = os.environ.get("SOLAR_WORKER_ID") or socket.gethostname()
    done = 0
    with engine.begin() as conn:
        reset_stale_running_jobs(conn)
    for _ in range(max(1, max_jobs)):
        with engine.begin() as conn:
            row = claim_next_job(conn, worker_id)
        if not row:
            break
        run_job(row)
        done += 1
    return done


def main() -> int:
    _load_env()
    parser = argparse.ArgumentParser(description="Solar module precompute worker")
    parser.add_argument("--once", action="store_true", help="Process pending jobs then exit")
    parser.add_argument("--max-jobs", type=int, default=1, dest="max_jobs")
    args = parser.parse_args()

    from database import engine

    if args.once:
        n = run_once(engine, args.max_jobs)
        log.info("precompute_runner_finished jobs=%s", n)
        return 0
    parser.error("Specify --once (use cron to invoke periodically)")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
