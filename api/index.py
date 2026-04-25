"""
Vercel Python function entrypoint for the FastAPI app.
"""

import os
import sys
import traceback
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import JSONResponse


ROOT_DIR = Path(__file__).resolve().parent.parent
BACKEND_DIR = ROOT_DIR / "backend"

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

os.environ.setdefault("SOLAR_SERVERLESS", "1")
os.environ.setdefault("SOLAR_SNAPSHOT_READ_ONLY", "1")
os.environ.setdefault("SOLAR_SNAPSHOT_ALLOW_STALE", "1")
os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")

# Match app.py: expose a top-level FastAPI object before any conditional import
# so Vercel's Python runtime can detect it consistently.
app = FastAPI(title="Solar Analytics Bootstrap")

try:
    from backend.main import app as backend_app  # noqa: E402
    app = backend_app
except Exception as exc:  # pragma: no cover - deploy-time diagnostic wrapper
    _trace = traceback.format_exc()
    app = FastAPI(title="Solar Analytics Startup Error")

    @app.get("/favicon.ico", include_in_schema=False)
    @app.get("/favicon.png", include_in_schema=False)
    def startup_error_favicon():
        return JSONResponse(status_code=204, content=None)

    @app.get("/", include_in_schema=False)
    def startup_error_root():
        return JSONResponse(
            status_code=500,
            content={
                "status": "startup_error",
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
        )

    @app.get("/health", include_in_schema=False)
    def startup_error_health():
        return JSONResponse(
            status_code=500,
            content={
                "status": "startup_error",
                "error_type": type(exc).__name__,
                "error": str(exc),
                "traceback": _trace.splitlines()[-20:],
            },
        )
