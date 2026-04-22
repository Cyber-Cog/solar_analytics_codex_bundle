"""
backend/main.py
================
Solar Analytics Platform — FastAPI Application Entry Point

Run with:
    uvicorn main:app --reload --port 8000

All routers registered here. CORS configured for the React dev server.
"""

import os
import sys
import mimetypes
import threading

# ── Load .env file if present (DATABASE_URL, etc.) ───────────────────────────
_env_path = os.path.join(os.path.dirname(__file__), ".env")
if os.path.isfile(_env_path):
    for _line in open(_env_path):
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            if _k.strip() not in os.environ:
                os.environ[_k.strip()] = _v.strip()

# ── Fix Windows MIME types (critical: without this .js files get wrong Content-Type
#    and browsers silently refuse to execute them, causing blank page) ──────────
mimetypes.add_type("application/javascript", ".js")
mimetypes.add_type("text/css", ".css")
mimetypes.add_type("text/html", ".html")

# ── Path Setup ────────────────────────────────────────────────────────────────
# Add backend directory to path so all local imports work from any cwd
BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, Response

FRONTEND_DIR = os.path.join(os.path.dirname(BACKEND_DIR), "frontend")
IS_SERVERLESS = os.environ.get("SOLAR_SERVERLESS", "").lower() in ("1", "true", "yes") or os.environ.get("VERCEL") == "1"

from database import engine, Base, SessionLocal
from db_perf import ensure_performance_objects, ensure_performance_objects_bg
from models import (
    User, Plant, RawDataGeneric, DCHierarchyDerived,
    PlantArchitecture, EquipmentSpec, SupportTicket, RawDataStats, PlantEquipment,
)

# ── Router Imports ─────────────────────────────────────────────────────────────
from auth.routes      import router as auth_router
from routers.plants   import router as plants_router
from routers.dashboard import router as dashboard_router
from routers.analytics import router as analytics_router
from routers.metadata  import router as metadata_router
from routers.tickets   import router as tickets_router
from routers.faults    import router as faults_router
from routers.admin      import router as admin_router
from routers.loss_analysis import router as loss_analysis_router, router_dashboard_alias as loss_analysis_dashboard_router
from routers.reports    import router as reports_router


def _ensure_equipment_spec_loss_columns():
    """PostgreSQL: add Loss Analysis inverter fields on existing DBs."""
    try:
        from sqlalchemy import text
        with engine.begin() as conn:
            for stmt in (
                "ALTER TABLE equipment_specs ADD COLUMN IF NOT EXISTS degradation_loss_pct DOUBLE PRECISION",
                "ALTER TABLE equipment_specs ADD COLUMN IF NOT EXISTS temp_coefficient_per_deg DOUBLE PRECISION",
            ):
                conn.execute(text(stmt))
    except Exception:
        pass


# ── Create all tables on startup (safe: only creates if not exists) ───────────
# Skip schema writes in serverless mode. Vercel Functions should boot fast and
# should not depend on running DDL successfully during module import.
if not IS_SERVERLESS:
    Base.metadata.create_all(bind=engine)
    _ensure_equipment_spec_loss_columns()

# ── Apply pending SAFE migrations (migrations/sql/*.sql) ──────────────────────
# Risky migrations (type changes, partitioning) live under migrations/manual/
# and must be triggered explicitly via: python -m migrations.runner manual
#
# IMPORTANT: even "safe" additive migrations can take a LONG time on a large
# table (e.g. 001_raw_data_dedupe_and_unique on a 40M-row raw_data_generic
# easily runs 20+ minutes). Blocking module import on them means uvicorn never
# binds its port and the whole app appears dead.
#
# Default behaviour (SOLAR_MIGRATIONS_ON_BOOT=background, the default):
#   - Fire the migration runner in a daemon thread.
#   - Let uvicorn bind its port immediately.
#   - Log progress; the app will simply serve cached responses / degraded
#     performance until the migration completes.
#
# Override with SOLAR_MIGRATIONS_ON_BOOT=blocking to reproduce the old
# behaviour (useful in Docker/CI where you *want* startup to wait).
#
# Set SOLAR_MIGRATIONS_ON_BOOT=skip to disable entirely (run
# `python -m migrations.runner auto` yourself when you can afford the window).
_MIGRATIONS_STRICT = os.environ.get("SOLAR_MIGRATIONS_STRICT", "0") == "1"
_MIGRATIONS_MODE = os.environ.get("SOLAR_MIGRATIONS_ON_BOOT", "background").strip().lower()
if _MIGRATIONS_MODE not in {"background", "blocking", "skip"}:
    _MIGRATIONS_MODE = "background"


def _run_boot_migrations() -> None:
    try:
        from migrations.runner import run_pending_safe
        summary = run_pending_safe(engine)
        if summary["applied"]:
            print(f"[migrations] applied: {summary['applied']}")
        if summary["failed"]:
            print(
                f"[migrations] FAILED: {summary['failed']} "
                f"(will retry on next boot)"
            )
            if _MIGRATIONS_STRICT and _MIGRATIONS_MODE == "blocking":
                raise RuntimeError(
                    f"Refusing to start: safe migrations failed "
                    f"({summary['failed']}) and SOLAR_MIGRATIONS_STRICT=1"
                )
    except RuntimeError:
        raise
    except Exception as exc:
        print(f"[migrations] runner error: {exc}")
        if _MIGRATIONS_STRICT and _MIGRATIONS_MODE == "blocking":
            raise


if IS_SERVERLESS:
    print("[migrations] skipped at boot in serverless mode")
elif _MIGRATIONS_MODE == "skip":
    print("[migrations] skipped at boot (SOLAR_MIGRATIONS_ON_BOOT=skip)")
elif _MIGRATIONS_MODE == "blocking":
    _run_boot_migrations()
else:
    import threading
    _mig_thread = threading.Thread(
        target=_run_boot_migrations,
        name="solar-migrations",
        daemon=True,
    )
    _mig_thread.start()
    print("[migrations] started in background; uvicorn continuing startup")

if not IS_SERVERLESS:
    ensure_performance_objects(engine)


def _background_warmup():
    """
    Runs after server startup (~5 s delay):
    1. Heavy DB indexes + ANALYZE (so they don't block startup)
    2. Pre-compute raw_data_stats for every plant (backfill for existing DBs)
    3. Pre-warm equipment list cache for Analytics Lab
    All errors are silently ignored so this thread never crashes the server.
    """
    import time, json
    time.sleep(5)

    # ── Step 1: heavy indexes + ANALYZE ──────────────────────────────────────
    try:
        ensure_performance_objects_bg(engine)
    except Exception:
        pass

    # ── Step 2 & 3: per-plant cache warm-up ──────────────────────────────────
    try:
        from sqlalchemy import text
        from dashboard_cache import set_any

        db = SessionLocal()
        try:
            plant_ids = [r[0] for r in db.query(Plant.plant_id).all()]
        except Exception:
            db.close()
            return

        for pid in plant_ids:
            try:
                # Backfill raw_data_stats if missing (existing DB without the table populated)
                stats = db.query(RawDataStats).filter(RawDataStats.plant_id == pid).first()
                if not stats or not stats.total_rows:
                    from routers.metadata import _refresh_plant_stats
                    _refresh_plant_stats(db, pid)

                # Pre-warm equipment list (Analytics Lab) via materialized table.
                # Set-based upsert — no more per-row db.merge.
                for lvl in ("inverter", "scb"):
                    try:
                        existing = db.query(PlantEquipment).filter(
                            PlantEquipment.plant_id == pid,
                            PlantEquipment.equipment_level == lvl
                        ).first()
                        if not existing:
                            db.execute(
                                text(
                                    """
                                    INSERT INTO plant_equipment (plant_id, equipment_level, equipment_id)
                                    SELECT DISTINCT :p, :l, equipment_id
                                      FROM raw_data_generic
                                     WHERE plant_id = :p
                                       AND equipment_level = :l
                                       AND equipment_id IS NOT NULL
                                    ON CONFLICT (plant_id, equipment_level, equipment_id) DO NOTHING
                                    """
                                ),
                                {"p": pid, "l": lvl},
                            )
                            db.commit()
                        rows2 = db.query(PlantEquipment.equipment_id).filter(
                            PlantEquipment.plant_id == pid, PlantEquipment.equipment_level == lvl
                        ).order_by(PlantEquipment.equipment_id).all()
                        ids = [r[0] for r in rows2]
                        set_any(f"equipment:{pid}:{lvl}", {"equipment_ids": ids, "total": len(ids)}, 300)
                    except Exception:
                        try:
                            db.rollback()
                        except Exception:
                            pass

                # Pre-warm ds_timeline (fault page first open)
                try:
                    from dashboard_cache import get_any
                    ck = f"ds_timeline:{pid}:::"
                    if get_any(ck, 120) is None:
                        from models import FaultDiagnostics
                        rows = db.query(FaultDiagnostics).filter(
                            FaultDiagnostics.plant_id == pid
                        ).order_by(FaultDiagnostics.timestamp.asc()).limit(10000).all()
                        data = [{"timestamp": r.timestamp, "inverter_id": r.inverter_id,
                                 "scb_id": r.scb_id,
                                 "expected_current": round(r.expected_current or 0, 2),
                                 "virtual_string_current": round(r.virtual_string_current or 0, 2),
                                 "missing_strings": r.missing_strings or 0,
                                 "power_loss_kw": round(r.power_loss_kw or 0, 2),
                                 "energy_loss_kwh": round(r.energy_loss_kwh or 0, 2),
                                 "fault_status": r.fault_status,
                                 "missing_current": round(r.missing_current or 0, 2)} for r in rows]
                        set_any(ck, {"data": data}, 300)
                except Exception:
                    pass
            except Exception:
                pass

        db.close()
    except Exception:
        pass


if not IS_SERVERLESS:
    threading.Thread(target=_background_warmup, daemon=True, name="cache-warmup").start()

# ── FastAPI App ───────────────────────────────────────────────────────────────
app = FastAPI(
    title       = "Solar Analytics Platform API",
    description = "Backend for Solar Analytics Platform v2",
    version     = "2.0.0",
    docs_url    = "/docs",
    redoc_url   = "/redoc",
)

# ── GZip: compress all JSON responses > 1KB (massive win for large payloads) ──
app.add_middleware(GZipMiddleware, minimum_size=1024)

# ── CORS ──────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins     = ["http://localhost:5173", "http://localhost:3000", "*"],
    allow_credentials = True,
    allow_methods     = ["*"],
    allow_headers     = ["*"],
)

# ── Register Routers ──────────────────────────────────────────────────────────
app.include_router(auth_router)
app.include_router(plants_router)
app.include_router(dashboard_router)
app.include_router(analytics_router)
app.include_router(metadata_router)
app.include_router(tickets_router)
app.include_router(faults_router)
app.include_router(loss_analysis_router)
app.include_router(loss_analysis_dashboard_router)
app.include_router(admin_router)
app.include_router(reports_router)


# ── Health Check ──────────────────────────────────────────────────────────────
@app.get("/health", tags=["Health"])
def health():
    return {"status": "ok", "version": "2.0.0"}


@app.get("/app", tags=["Frontend"], include_in_schema=False)
def serve_frontend():
    """Serve the React frontend index.html."""
    index = os.path.join(FRONTEND_DIR, "index.html")
    return FileResponse(index)


# ── Static JS/CSS: aggressive caching when ?v= query param is present ────────
# Files fetched with ?v=xxx (version-stamped) are safe to cache for 1 h.
# index.html and files without ?v= get no-cache so they're always fresh.
@app.middleware("http")
async def static_cache_headers(request: Request, call_next):
    response = await call_next(request)
    path = request.url.path
    has_version = bool(request.query_params.get("v"))
    if path.endswith((".js", ".css")) and has_version:
        response.headers["Cache-Control"] = "public, max-age=3600, immutable"
    elif path in ("/", "/index.html"):
        response.headers["Cache-Control"] = "no-cache"
    return response


# Mount static files AFTER all API routes so /docs and /api/* still work.
# We keep this enabled in serverless too, because Vercel Services mode can ship
# the frontend files alongside the FastAPI service.
if os.path.isdir(FRONTEND_DIR):
    app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")
