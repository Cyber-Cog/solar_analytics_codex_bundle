"""
backend/routers/perf_monitor.py
================================
Admin-only performance monitoring endpoints + middleware.

Provides:
  - Request timing middleware that records per-endpoint latency
  - GET /api/admin/perf/overview       — aggregate perf stats
  - GET /api/admin/perf/slow-queries   — recent slow queries
  - GET /api/admin/perf/endpoint-stats — per-endpoint timing
  - GET /api/admin/perf/db-health      — DB connection + table stats
  - GET /api/admin/perf/timescale-status — Timescale extension + CAGG readiness
  - POST /api/admin/perf/run-precompute — trigger fault/loss precompute
  - GET /api/admin/perf/precompute-status — status of precompute jobs
"""

import os, time, logging, threading, json
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text

from database import get_db
from models import User, FaultCache, Plant, RawDataStats
from auth.routes import get_current_user

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin/perf", tags=["Performance"])

def _check_admin(user: User = Depends(get_current_user)):
    if not user.is_admin:
        raise HTTPException(status_code=403, detail="Admin access required")
    return user

# ── Request Timing Store (in-process, ring buffer) ───────────────────────────
_MAX_ENTRIES = 2000
_timing_lock = threading.Lock()
_request_timings: list = []  # [{path, method, status, duration_ms, timestamp}]
_slow_queries: list = []     # [{sql_preview, duration_ms, timestamp, params_preview}]

def record_request_timing(path: str, method: str, status_code: int, duration_ms: float):
    """Called by the middleware on every response."""
    entry = {
        "path": path,
        "method": method,
        "status": status_code,
        "duration_ms": round(duration_ms, 1),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    with _timing_lock:
        _request_timings.append(entry)
        if len(_request_timings) > _MAX_ENTRIES:
            _request_timings[:] = _request_timings[-_MAX_ENTRIES:]

def record_slow_query(sql_preview: str, duration_ms: float, params_preview: str = ""):
    """Called when a query exceeds the slow threshold."""
    entry = {
        "sql_preview": sql_preview[:300],
        "duration_ms": round(duration_ms, 1),
        "params_preview": params_preview[:200],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    with _timing_lock:
        _slow_queries.append(entry)
        if len(_slow_queries) > 500:
            _slow_queries[:] = _slow_queries[-500:]


# ── Full fault pipeline (admin UI) — module snapshots + fault runtime tab engines ──
_MAX_EVENT_LOG = 200
_precompute_lock = threading.Lock()
_precompute_status: dict = {
    "running": False,
    "mode": "idle",  # idle | full_fault_pipeline
    "last_run": None,
    "last_duration_s": None,
    "last_error": None,
    "plants_done": 0,
    "plants_total": 0,
    "current_plant": None,
    "step": None,
    "percent": 0.0,
    "eta_seconds": None,
    "elapsed_seconds": None,
    "started_at": None,
    "event_log": [],
}


def _precompute_log(msg: str) -> None:
    global _precompute_status
    entry = {
        "t": datetime.now(timezone.utc).isoformat(),
        "message": str(msg)[:2000],
    }
    with _precompute_lock:
        ev: list = _precompute_status.setdefault("event_log", [])
        ev.append(entry)
        if len(ev) > _MAX_EVENT_LOG:
            del ev[: len(ev) - _MAX_EVENT_LOG]


def _set_precompute_state(**kwargs) -> None:
    with _precompute_lock:
        for k, v in kwargs.items():
            _precompute_status[k] = v


def _update_eta_plant_wise(plant_index_0based: int, plants_total: int, t0_wall: float) -> None:
    """ETA = avg seconds per completed plant * remaining plants."""
    if plant_index_0based < 0 or plants_total <= 0:
        return
    elapsed = max(0.001, time.time() - t0_wall)
    done = plant_index_0based + 1
    if done <= 0:
        return
    sp = elapsed / done
    remain = max(0, plants_total - done)
    eta = int(sp * remain)
    pct = min(100.0, (done / plants_total) * 100.0)
    _set_precompute_state(eta_seconds=eta, percent=round(pct, 1), elapsed_seconds=round(elapsed, 1))


def _run_full_fault_pipeline_background():
    """
    Background: for each plant, run
      1) module_precompute.compute_snapshots_for_range (DS summary, unified feed JSON, loss bridge, category total rows);
      2) parallel warm of fault tab engines: PL, IS, GB, comm, CD (same as Fault Diagnostics sub-tabs).

    Note: this does not re-run the disconnected-string *detection* (run_ds_detection on raw SCB
    time series); that runs on data ingest. This warms *aggregates and tab analyses* for the UI.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from database import SessionLocal

    global _precompute_status

    with _precompute_lock:
        if _precompute_status.get("running"):
            return
        _precompute_status["running"] = True
        _precompute_status["last_error"] = None
        _precompute_status["plants_done"] = 0
        _precompute_status["plants_total"] = 0
        _precompute_status["current_plant"] = None
        _precompute_status["step"] = "starting"
        _precompute_status["percent"] = 0.0
        _precompute_status["eta_seconds"] = None
        _precompute_status["elapsed_seconds"] = None
        _precompute_status["event_log"] = []
        _precompute_status["mode"] = "full_fault_pipeline"
        _precompute_status["started_at"] = datetime.now(timezone.utc).isoformat()

    t0_wall = time.time()
    _precompute_log("Full fault pipeline started (module snapshots + fault tab engines per plant)")

    try:
        s0 = SessionLocal()
        try:
            plants = s0.query(Plant).all()
            plant_ids = [p.plant_id for p in plants if p.plant_id]
            u = s0.query(User).first()
        finally:
            s0.close()

        if u is None:
            raise RuntimeError("No user row in database; cannot run module precompute")

        n = len(plant_ids)
        _set_precompute_state(plants_total=n, plants_done=0)
        _precompute_log("Plants to process: %d" % n)
        if n == 0:
            _set_precompute_state(
                step="complete",
                percent=100.0,
                eta_seconds=0,
                running=False,
            )
            _precompute_status["last_duration_s"] = 0.0
            _precompute_status["elapsed_seconds"] = 0.0
            _precompute_status["last_run"] = datetime.now(timezone.utc).isoformat()
            _precompute_log("No plants in database — nothing to do")

        for i, pid in enumerate(plant_ids):
            _set_precompute_state(
                current_plant=pid,
                plants_done=i,
                step="module_snapshots",
            )
            frac = (i + 0.1) / max(n, 1)
            _set_precompute_state(percent=round(100.0 * frac, 1))
            _precompute_log(
                "[%s] Step 1/2: module snapshots (ds_summary, unified_feed, loss_bridge, category_totals)…" % pid
            )

            df, dt = None, None
            s1 = SessionLocal()
            try:
                from module_precompute import (
                    compute_snapshots_for_range,
                    resolve_recompute_day_range,
                    validate_or_refresh_raw_data_stats,
                )

                st = validate_or_refresh_raw_data_stats(s1, pid)
                mnts = st.min_ts if st else None
                mxxt = st.max_ts if st else None
                df, dt = resolve_recompute_day_range(s1, pid, mnts, mxxt)
                compute_snapshots_for_range(s1, pid, df, dt, u)
            except Exception as exc:
                log.warning("module precompute failed for %s: %s", pid, exc)
                _precompute_log("[%s] ERROR module snapshots: %s" % (pid, exc))
            finally:
                s1.close()

            _set_precompute_state(
                step="fault_tab_engines", percent=round(100.0 * (i + 0.55) / max(n, 1), 1)
            )
            _precompute_log("[%s] Step 2/2: fault tab engines (PL, IS, GB, comm, CD in parallel)…" % pid)

            try:
                from routers.faults import (
                    _pl_page_with_cache,
                    _is_tab_with_cache,
                    _gb_tab_with_cache,
                    _comm_tab_with_cache,
                    _cd_tab_with_cache,
                )

                def _run_pl():
                    s = SessionLocal()
                    try:
                        return _pl_page_with_cache(s, pid, df, dt)
                    finally:
                        s.close()

                def _run_is():
                    s = SessionLocal()
                    try:
                        return _is_tab_with_cache(s, pid, df, dt)
                    finally:
                        s.close()

                def _run_gb():
                    s = SessionLocal()
                    try:
                        return _gb_tab_with_cache(s, pid, df, dt)
                    finally:
                        s.close()

                def _run_comm():
                    s = SessionLocal()
                    try:
                        return _comm_tab_with_cache(s, pid, df, dt)
                    finally:
                        s.close()

                def _run_cd():
                    s = SessionLocal()
                    try:
                        return _cd_tab_with_cache(s, pid, df, dt)
                    finally:
                        s.close()

                with ThreadPoolExecutor(max_workers=5) as pool:
                    futs = {
                        pool.submit(_run_pl): "power_limitation",
                        pool.submit(_run_is): "inverter_shutdown",
                        pool.submit(_run_gb): "grid_breakdown",
                        pool.submit(_run_comm): "communication_issue",
                        pool.submit(_run_cd): "clipping_derating",
                    }
                    for fut in as_completed(futs):
                        name = futs[fut]
                        try:
                            fut.result()
                            _precompute_log("[%s]   OK %s" % (pid, name))
                        except Exception as ex:
                            _precompute_log("[%s]   FAIL %s: %s" % (pid, name, ex))
            except Exception as exc:
                log.warning("fault tab warm failed for %s: %s", pid, exc)
                _precompute_log("[%s] ERROR fault tabs: %s" % (pid, exc))

            _set_precompute_state(
                plants_done=i + 1,
                step="plant_done",
                percent=round(100.0 * (i + 1) / max(n, 1), 1),
            )
            _update_eta_plant_wise(i, n, t0_wall)
            _precompute_log("[%s] finished" % pid)

        if n > 0:
            _set_precompute_state(
                current_plant=None,
                step="complete",
                percent=100.0,
                eta_seconds=0,
                running=False,
            )
            _precompute_status["last_duration_s"] = round(time.time() - t0_wall, 1)
            _precompute_status["elapsed_seconds"] = _precompute_status["last_duration_s"]
            _precompute_status["last_run"] = datetime.now(timezone.utc).isoformat()
            _precompute_log("All plants complete in %ss" % _precompute_status["last_duration_s"])
    except Exception as exc:
        _set_precompute_state(last_error=str(exc), running=False, step="error", percent=0.0)
        _precompute_log("FATAL: %s" % exc)
        log.error("Full fault pipeline error: %s", exc, exc_info=True)
    finally:
        with _precompute_lock:
            if _precompute_status.get("running"):
                _precompute_status["running"] = False
            err = _precompute_status.get("last_error")
            _precompute_status["mode"] = "error" if err else "idle"


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/overview")
def perf_overview(admin: User = Depends(_check_admin)):
    """Aggregate performance stats for the admin dashboard."""
    from dashboard_cache import get_cache_stats

    with _timing_lock:
        recent = list(_request_timings[-200:])
        slow_count = sum(1 for t in _request_timings if t["duration_ms"] > 3000)

    # Compute per-endpoint aggregate stats
    endpoint_map = defaultdict(list)
    for t in recent:
        endpoint_map[t["path"]].append(t["duration_ms"])

    endpoint_stats = []
    for path, durations in sorted(endpoint_map.items()):
        durations.sort()
        n = len(durations)
        endpoint_stats.append({
            "path": path,
            "count": n,
            "avg_ms": round(sum(durations) / n, 1),
            "p50_ms": round(durations[n // 2], 1),
            "p95_ms": round(durations[int(n * 0.95)], 1) if n > 1 else round(durations[0], 1),
            "max_ms": round(max(durations), 1),
        })

    endpoint_stats.sort(key=lambda x: x["avg_ms"], reverse=True)

    cache_stats = get_cache_stats()

    return {
        "total_requests_tracked": len(recent),
        "slow_requests_gt3s": slow_count,
        "cache": cache_stats,
        "top_endpoints": endpoint_stats[:20],
        "precompute": dict(_precompute_status),
        "server_time": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/slow-queries")
def perf_slow_queries(
    limit: int = Query(default=50, le=200),
    admin: User = Depends(_check_admin),
):
    """Return recent slow queries (>1s)."""
    with _timing_lock:
        queries = list(_slow_queries[-limit:])
    queries.reverse()
    return {"queries": queries, "total": len(queries)}


@router.get("/endpoint-stats")
def perf_endpoint_stats(
    minutes: int = Query(default=60, le=1440),
    admin: User = Depends(_check_admin),
):
    """Per-endpoint timing stats for the last N minutes."""
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()

    with _timing_lock:
        recent = [t for t in _request_timings if t["timestamp"] >= cutoff]

    endpoint_map = defaultdict(list)
    for t in recent:
        endpoint_map[t["path"]].append(t["duration_ms"])

    rows = []
    for path, durations in sorted(endpoint_map.items()):
        durations.sort()
        n = len(durations)
        rows.append({
            "path": path,
            "count": n,
            "avg_ms": round(sum(durations) / n, 1),
            "p50_ms": round(durations[n // 2], 1),
            "p95_ms": round(durations[int(n * 0.95)], 1) if n > 1 else round(durations[0], 1),
            "p99_ms": round(durations[int(n * 0.99)], 1) if n > 1 else round(durations[0], 1),
            "max_ms": round(max(durations), 1),
            "min_ms": round(min(durations), 1),
        })

    rows.sort(key=lambda x: x["avg_ms"], reverse=True)
    return {"minutes": minutes, "endpoints": rows}


@router.get("/db-health")
def perf_db_health(
    db: Session = Depends(get_db),
    admin: User = Depends(_check_admin),
):
    """Database health: table sizes, index stats, active connections."""
    result = {}

    # Table sizes
    try:
        rows = db.execute(text("""
            SELECT relname AS table_name,
                   pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
                   pg_total_relation_size(c.oid) AS size_bytes,
                   n_live_tup AS row_estimate
            FROM pg_class c
            JOIN pg_stat_user_tables s ON s.relname = c.relname
            WHERE s.schemaname = 'public'
            ORDER BY pg_total_relation_size(c.oid) DESC
            LIMIT 20
        """)).fetchall()
        result["tables"] = [
            {"name": r.table_name, "size": r.total_size, "size_bytes": r.size_bytes, "rows": r.row_estimate}
            for r in rows
        ]
    except Exception as exc:
        result["tables"] = {"error": str(exc)}

    # Index usage stats
    try:
        rows = db.execute(text("""
            SELECT relname, indexrelname, idx_scan, idx_tup_read,
                   pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
            FROM pg_stat_user_indexes
            WHERE schemaname = 'public'
            ORDER BY idx_scan DESC
            LIMIT 30
        """)).fetchall()
        result["indexes"] = [
            {"table": r.relname, "index": r.indexrelname, "scans": r.idx_scan,
             "rows_read": r.idx_tup_read, "size": r.index_size}
            for r in rows
        ]
    except Exception as exc:
        result["indexes"] = {"error": str(exc)}

    # Active connections
    try:
        row = db.execute(text("""
            SELECT count(*) AS total,
                   count(*) FILTER (WHERE state = 'active') AS active,
                   count(*) FILTER (WHERE state = 'idle') AS idle
            FROM pg_stat_activity
            WHERE datname = current_database()
        """)).fetchone()
        result["connections"] = {"total": row.total, "active": row.active, "idle": row.idle}
    except Exception as exc:
        result["connections"] = {"error": str(exc)}

    return result


@router.get("/timescale-status")
def perf_timescale_status(
    db: Session = Depends(get_db),
    admin: User = Depends(_check_admin),
):
    """TimescaleDB extension, continuous aggregate presence, and timestamp_ts backfill status."""
    from routers import analytics_timescale

    return analytics_timescale.timescale_status_payload(db)


@router.post("/run-precompute")
def trigger_precompute(admin: User = Depends(_check_admin)):
    """
    Trigger the full fault & analytics cache pipeline for all plants (background thread):
    module snapshots (DS summary, unified feed, loss bridge, category KPI rows) plus
    fault tab engines (power limitation, inverter shutdown, grid breakdown, communication, clipping/derating).
    """
    with _precompute_lock:
        if _precompute_status.get("running"):
            return {"ok": False, "message": "Precompute is already running", "status": dict(_precompute_status)}

    thread = threading.Thread(target=_run_full_fault_pipeline_background, daemon=True)
    thread.start()
    return {
        "ok": True,
        "message": "Full fault pipeline started in background. Poll GET /api/admin/perf/precompute-status for progress.",
        "status": dict(_precompute_status),
    }


@router.get("/precompute-status")
def precompute_status(admin: User = Depends(_check_admin)):
    """Return current precompute job status."""
    return dict(_precompute_status)


@router.get("/request-log")
def perf_request_log(
    limit: int = Query(default=100, le=500),
    path_filter: Optional[str] = Query(default=None),
    admin: User = Depends(_check_admin),
):
    """Return recent request timings (optionally filtered by path)."""
    with _timing_lock:
        data = list(_request_timings[-limit:])
    if path_filter:
        data = [d for d in data if path_filter in d["path"]]
    data.reverse()
    return {"entries": data, "total": len(data)}
