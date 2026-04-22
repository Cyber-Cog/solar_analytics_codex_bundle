#!/usr/bin/env python3
"""
UAT: ensure Loss Analysis routes exist on the FastAPI app.
Run from repo root or backend/:  python scripts/verify_loss_analysis_routes.py
Requires DATABASE_URL (e.g. in backend/.env).
"""
from __future__ import annotations

import os
import sys

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BACKEND not in sys.path:
    sys.path.insert(0, BACKEND)
os.chdir(BACKEND)

_env = os.path.join(BACKEND, ".env")
if os.path.isfile(_env):
    for line in open(_env, encoding="utf-8"):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            k, v = k.strip(), v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v

from main import app  # noqa: E402

EXPECTED = {
    "/api/loss-analysis/options",
    "/api/loss-analysis/bridge",
    "/api/dashboard/loss-analysis/options",
    "/api/dashboard/loss-analysis/bridge",
}

paths = set()
for r in app.routes:
    p = getattr(r, "path", None)
    if p:
        paths.add(p)

missing = [e for e in sorted(EXPECTED) if e not in paths]
if missing:
    print("FAIL: missing routes:", missing)
    print("Registered (sample):", sorted(p for p in paths if "loss" in p))
    sys.exit(1)

print("OK: Loss Analysis routes registered:", " ".join(sorted(EXPECTED)))
sys.exit(0)
