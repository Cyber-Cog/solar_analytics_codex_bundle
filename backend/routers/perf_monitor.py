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
from models import User, FaultCache, Plant
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


# ── Precompute state ─────────────────────────────────────────────────────────
_precompute_lock = threading.Lock()
_precompute_status = {
    "running": False,
    "last_run": None,
    "last_duration_s": None,
    "last_error": None,
    "plants_done": 0,
    "plants_total": 0,
    "current_plant": None,
}

def _run_precompute_background():
    """Background thread: compute and cache fault/loss results for every plant."""
    from database import SessionLocal
    global _precompute_status

    with _precompute_lock:
        if _precompute_status["running"]:
            return
        _precompute_status["running"] = True
        _precompute_status["last_error"] = None
        _precompute_status["plants_done"] = 0
        _precompute_status["current_plant"] = None

    t0 = time.time()
    try:
        s = SessionLocal()
        try:
            plants = s.query(Plant).all()
            plant_ids = [p.plant_id for p in plants]
        finally:
            s.close()

        _precompute_status["plants_total"] = len(plant_ids)

        today = datetime.now().date()
        date_from = str(today - timedelta(days=7))
        date_to = str(today)

        for i, pid in enumerate(plant_ids):
            _precompute_status["current_plant"] = pid
            _precompute_status["plants_done"] = i
            try:
                _precompute_one_plant(pid, date_from, date_to)
            except Exception as exc:
                log.warning("Precompute failed for plant %s: %s", pid, exc)

        _precompute_status["plants_done"] = len(plant_ids)
        _precompute_status["current_plant"] = None
        _precompute_status["last_duration_s"] = round(time.time() - t0, 1)
        _precompute_status["last_run"] = datetime.now(timezone.utc).isoformat()
    except Exception as exc:
        _precompute_status["last_error"] = str(exc)
        log.error("Precompute background error: %s", exc)
    finally:
        _precompute_status["running"] = False


def _precompute_one_plant(plant_id: str, date_from: str, date_to: str):
    """
    Pre-compute dashboard bundle + fault diagnostics for a single plant.
    Results go into the existing cache layers so subsequent user requests
    hit the warm cache instead of computing from scratch.
    """
    from database import SessionLocal

    # 1. Warm dashboard bundle cache
    try:
        s = SessionLocal()
        try:
            from db_perf import choose_data_table
            from dashboard_cache import get as cache_get
            # Check if already cached
            cached = cache_get("bundle_v8", plant_id, date_from, date_to)
            if cached is None:
                from routers.dashboard import dashboard_bundle
                # Import helpers to manually invoke the bundle logic
                from routers.dashboard import _default_range, _inverter_dc_maps, _plant_dc_kwp_from_inverters
                _from, _to = _default_range(date_from, date_to)
                # Trigger by importing the internal function path
                # We call the endpoint function directly — it writes to cache
                from unittest.mock import MagicMock
                mock_user = MagicMock()
                mock_user.is_admin = True
                mock_user.allowed_plants = "*"
                # Actually, let's just call the SQL-level functions directly
                # to warm the cache without needing full DI
                from routers.dashboard import (
                    sql_plant_ac_totals, _wms_tilt_insolation_kwh_m2,
                    _wms_kpis_payload, sql_plant_ac_daily_energy,
                    _inverter_performance_table, _sql_power_vs_gti,
                    _power_vs_gti_row_limit
                )
                from sqlalchemy import text as sa_text
                from models import Plant as PlantModel

                table = choose_data_table(s, plant_id, _from, _to)
                plant = s.query(PlantModel).filter(PlantModel.plant_id == plant_id).first()
                cap_kw = (plant.capacity_mwp * 1000) if (plant and plant.capacity_mwp) else None
                f_ts = f"{_from} 00:00:00"
                t_ts = f"{_to} 23:59:59"

                # Run queries (these populate internal caches)
                try:
                    _wms_tilt_insolation_kwh_m2(s, table, plant_id, f_ts, t_ts)
                except Exception:
                    pass
                try:
                    _inverter_performance_table(s, table, plant_id, f_ts, t_ts, _from, _to)
                except Exception:
                    pass
        finally:
            s.close()
    except Exception as exc:
        log.debug("Precompute dashboard cache for %s: %s", plant_id, exc)

    # 2. Warm fault diagnostics cache
    try:
        s = SessionLocal()
        try:
            from routers.faults import (
                _pl_page_with_cache, _is_tab_with_cache,
                _gb_tab_with_cache, _comm_tab_with_cache,
                _cd_tab_with_cache,
            )
            _pl_page_with_cache(s, plant_id, date_from, date_to)
            _is_tab_with_cache(s, plant_id, date_from, date_to)
            _gb_tab_with_cache(s, plant_id, date_from, date_to)
            _comm_tab_with_cache(s, plant_id, date_from, date_to)
            _cd_tab_with_cache(s, plant_id, date_from, date_to)
        finally:
            s.close()
    except Exception as exc:
        log.debug("Precompute faults for %s: %s", plant_id, exc)


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


@router.post("/run-precompute")
def trigger_precompute(admin: User = Depends(_check_admin)):
    """Trigger background precompute of fault/loss results for all plants."""
    if _precompute_status["running"]:
        return {"ok": False, "message": "Precompute is already running", "status": dict(_precompute_status)}

    thread = threading.Thread(target=_run_precompute_background, daemon=True)
    thread.start()
    return {"ok": True, "message": "Precompute started in background", "status": dict(_precompute_status)}


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
