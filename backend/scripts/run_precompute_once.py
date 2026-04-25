"""
One-shot snapshot recompute for all active plants (or one plant_id arg).
Run from backend/:  python scripts/run_precompute_once.py [PLANT_ID]

Loads backend/.env with override so shell DATABASE_URL does not win.
Uses DB_STATEMENT_TIMEOUT_MS from env or 300000 for this process only if unset
(note: engine is created on first import of database — set env BEFORE import).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

_BACKEND = Path(__file__).resolve().parents[1]


def _load_env() -> None:
    p = _BACKEND / ".env"
    if not p.is_file():
        return
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ[k.strip()] = v.strip()


def main() -> int:
    _load_env()
    if not os.environ.get("DB_STATEMENT_TIMEOUT_MS"):
        os.environ["DB_STATEMENT_TIMEOUT_MS"] = "300000"

    sys.path.insert(0, str(_BACKEND))
    os.chdir(_BACKEND)

    from database import SessionLocal  # noqa: E402
    from models import Plant, User  # noqa: E402
    from module_precompute import (  # noqa: E402
        compute_snapshots_for_range,
        resolve_recompute_day_range,
        update_plant_compute_status,
        validate_or_refresh_raw_data_stats,
    )

    db = SessionLocal()
    try:
        user = db.query(User).first()
        if not user:
            print("No users in DB — cannot run precompute.")
            return 2

        arg = (sys.argv[1] or "").strip() if len(sys.argv) > 1 else ""
        if arg:
            plants = db.query(Plant).filter(Plant.plant_id == arg).all()
        else:
            plants = db.query(Plant).filter(Plant.status == "Active").all()
            if not plants:
                plants = db.query(Plant).all()

        if not plants:
            print("No plants found.")
            return 3

        for p in plants:
            pid = p.plant_id
            st = validate_or_refresh_raw_data_stats(db, pid)
            d0, d1 = resolve_recompute_day_range(
                db,
                pid,
                st.min_ts if st else None,
                st.max_ts if st else None,
            )
            print(f"--- {pid} ({p.name})  {d0} .. {d1} ---")
            update_plant_compute_status(db, pid, status="running", date_from=d0, date_to=d1)
            try:
                out = compute_snapshots_for_range(db, pid, d0, d1, user)
                warm_fault_runtime_snapshots(pid, d0, d1)
                sec = max(1, int((out.get("total_ms") or 0) / 1000))
                update_plant_compute_status(
                    db, pid, status="done", date_from=d0, date_to=d1, duration_seconds=sec
                )
                print("OK", out)
            except Exception as exc:
                db.rollback()
                update_plant_compute_status(
                    db,
                    pid,
                    status="failed",
                    date_from=d0,
                    date_to=d1,
                    error_message=str(exc)[:4000],
                )
                print("FAILED", pid, exc)
                return 1
        return 0
    finally:
        db.close()


def warm_fault_runtime_snapshots(plant_id: str, date_from: str, date_to: str) -> None:
    """Warm durable PL/IS/GB/comm/CD tab payloads for the same range."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    from database import SessionLocal
    from routers.faults import (
        _cd_tab_with_cache,
        _comm_tab_with_cache,
        _gb_tab_with_cache,
        _is_tab_with_cache,
        _pl_page_with_cache,
    )

    funcs = {
        "pl_page": _pl_page_with_cache,
        "is_tab": _is_tab_with_cache,
        "gb_tab": _gb_tab_with_cache,
        "comm_tab": _comm_tab_with_cache,
        "cd_tab": _cd_tab_with_cache,
    }

    def _run(name, fn):
        s = SessionLocal()
        try:
            fn(s, plant_id, date_from, date_to)
            return name
        finally:
            s.close()

    with ThreadPoolExecutor(max_workers=5) as pool:
        futs = [pool.submit(_run, name, fn) for name, fn in funcs.items()]
        for fut in as_completed(futs):
            print("runtime snapshot OK", fut.result())


if __name__ == "__main__":
    raise SystemExit(main())
