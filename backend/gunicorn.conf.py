"""
Gunicorn configuration for the Solar Analytics FastAPI app.

This keeps production server settings in one place instead of burying them in
the container command. All key values can still be overridden via env vars.
"""

import multiprocessing
import os


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


bind = f"0.0.0.0:{os.getenv('PORT', '8000')}"
worker_class = "uvicorn.workers.UvicornWorker"

# Default worker count: one per core plus one extra, capped to avoid
# accidentally oversubscribing small VMs with very high reported CPU counts.
default_workers = min(max(multiprocessing.cpu_count() + 1, 2), 9)
workers = _int_env("GUNICORN_WORKERS", default_workers)

timeout = _int_env("GUNICORN_TIMEOUT", 180)
graceful_timeout = _int_env("GUNICORN_GRACEFUL_TIMEOUT", 30)
keepalive = _int_env("GUNICORN_KEEPALIVE", 5)

accesslog = "-"
errorlog = "-"
loglevel = os.getenv("GUNICORN_LOG_LEVEL", "info")

capture_output = True
worker_tmp_dir = "/dev/shm"
preload_app = False

