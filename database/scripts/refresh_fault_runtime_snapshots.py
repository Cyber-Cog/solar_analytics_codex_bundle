"""
Pre-populate fault_runtime_snapshot rows for Power Limitation / IS / GB tabs.

Usage (from backend directory, DATABASE_URL set):
  python scripts/refresh_fault_runtime_snapshots.py --plant NTPCNOKHRA --from 2026-03-01 --to 2026-03-07

Intended for cron or post-ingest jobs so first UI hit after deploy is fast.
"""

from __future__ import annotations

import argparse
import os
import sys

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BACKEND not in sys.path:
    sys.path.insert(0, BACKEND)

from database import SessionLocal  # noqa: E402
from routers.faults import (  # noqa: E402
    _pl_page_with_cache,
    _is_tab_with_cache,
    _gb_tab_with_cache,
)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--plant", required=True, help="plant_id")
    p.add_argument("--from", dest="date_from", required=True)
    p.add_argument("--to", dest="date_to", required=True)
    args = p.parse_args()

    db = SessionLocal()
    try:
        _pl_page_with_cache(db, args.plant, args.date_from, args.date_to)
        _is_tab_with_cache(db, args.plant, args.date_from, args.date_to)
        _gb_tab_with_cache(db, args.plant, args.date_from, args.date_to)
        print("OK: refreshed pl_page, is_tab, gb_tab snapshots")
    finally:
        db.close()


if __name__ == "__main__":
    main()
