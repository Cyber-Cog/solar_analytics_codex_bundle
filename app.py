"""
Root Vercel FastAPI entrypoint.
"""

import os
import sys
import traceback
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import JSONResponse


ROOT_DIR = Path(__file__).resolve().parent
BACKEND_DIR = ROOT_DIR / "backend"

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

os.environ.setdefault("SOLAR_SERVERLESS", "1")
os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")

try:
    from backend.main import app  # noqa: E402
except Exception as exc:  # pragma: no cover - deploy-time diagnostic wrapper
    _trace = traceback.format_exc()
    app = FastAPI(title="Solar Analytics Startup Error")

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
