"""
Apply performance DDL from db_perf.py (indexes + ANALYZE).

  cd backend && python -m scripts.ensure_db_perf

Uses DATABASE_URL from backend/.env or environment.
"""

from __future__ import annotations

import os
import sys

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BACKEND not in sys.path:
    sys.path.insert(0, BACKEND)

_env = os.path.join(BACKEND, ".env")
if os.path.isfile(_env):
    for line in open(_env, encoding="utf-8", errors="ignore"):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            if k.strip() not in os.environ:
                os.environ[k.strip()] = v.strip()


def main() -> None:
    from database import engine
    from db_perf import ensure_performance_objects, ensure_performance_objects_bg

    print("[ensure_db_perf] fast indexes…")
    ensure_performance_objects(engine)
    print("[ensure_db_perf] background indexes + ANALYZE (may take a long time)…")
    ensure_performance_objects_bg(engine)
    print("[ensure_db_perf] done.")


if __name__ == "__main__":
    main()
