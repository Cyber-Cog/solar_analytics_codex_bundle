"""
Forward-only SQL migration runner.

Design goals:
  - Plain .sql files on disk. No ORM, no ALEMBIC, no surprise autogen.
  - Idempotent: a migration that has already run is skipped.
  - Transactional: every migration runs inside a single BEGIN/COMMIT.
    If any statement in the file fails, the whole migration is rolled back and
    startup continues (the failure is logged but not raised by default).
  - Safe to include in app startup: we call ``run_pending_safe`` which runs the
    numbered files under ``migrations/sql/``. Risky changes live under
    ``migrations/manual/`` and must be triggered explicitly via ``run_manual``.

File naming convention:
    sql/NNN_short_description.sql       -- auto-applied at startup
    manual/NNN_short_description.sql    -- explicit opt-in (after backup!)

Where NNN is a zero-padded integer. Files are applied in lexicographic order.
"""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Iterable

from sqlalchemy import text
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)

_MIGRATIONS_DIR = Path(__file__).resolve().parent
_AUTO_DIR = _MIGRATIONS_DIR / "sql"
_MANUAL_DIR = _MIGRATIONS_DIR / "manual"

_TRACK_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version      TEXT PRIMARY KEY,
    applied_at   TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
    runtime_ms   INTEGER NOT NULL DEFAULT 0,
    source       TEXT NOT NULL DEFAULT 'auto'
);
"""

_VERSION_RE = re.compile(r"^(\d+)_.+\.sql$")


def _ensure_tracking(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(_TRACK_TABLE_DDL))


def _applied_versions(engine: Engine) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT version FROM schema_migrations")).fetchall()
    return {r[0] for r in rows}


def _iter_sql_files(folder: Path) -> Iterable[Path]:
    if not folder.is_dir():
        return []
    return sorted(
        [p for p in folder.iterdir() if p.suffix == ".sql" and _VERSION_RE.match(p.name)],
        key=lambda p: p.name,
    )


def _apply_file(engine: Engine, path: Path, source: str = "auto") -> bool:
    """Apply one migration file. Returns True on success, False on failure.

    Sets per-session lock_timeout + statement_timeout so a migration that can't
    get its locks (e.g. another session is already mid-DDL on the same table)
    fails fast rather than sitting for hours. Tune via env vars:

      SOLAR_MIGRATION_LOCK_TIMEOUT_MS       (default 10000, 10 s)
      SOLAR_MIGRATION_STATEMENT_TIMEOUT_MS  (default 3600000, 60 min)
    """
    sql = path.read_text(encoding="utf-8")
    version = path.name
    lock_ms = int(os.environ.get("SOLAR_MIGRATION_LOCK_TIMEOUT_MS", "10000"))
    stmt_ms = int(os.environ.get("SOLAR_MIGRATION_STATEMENT_TIMEOUT_MS", "3600000"))
    log.info(
        "migration: applying %s (source=%s, lock_timeout=%dms, statement_timeout=%dms)",
        version, source, lock_ms, stmt_ms,
    )
    import time
    t0 = time.monotonic()
    try:
        with engine.begin() as conn:
            conn.execute(text(f"SET LOCAL lock_timeout = {lock_ms}"))
            conn.execute(text(f"SET LOCAL statement_timeout = {stmt_ms}"))
            conn.exec_driver_sql(sql)
            runtime_ms = int((time.monotonic() - t0) * 1000)
            conn.execute(
                text(
                    "INSERT INTO schema_migrations (version, runtime_ms, source) "
                    "VALUES (:v, :r, :s) ON CONFLICT (version) DO NOTHING"
                ),
                {"v": version, "r": runtime_ms, "s": source},
            )
        log.info("migration: applied %s in %d ms", version, runtime_ms)
        return True
    except Exception as exc:
        log.error("migration: %s FAILED: %s", version, exc)
        return False


def run_pending_safe(engine: Engine) -> dict:
    """Apply all pending migrations from migrations/sql/ at startup.

    Never raises — returns a summary instead. The app should continue to start
    even if a migration fails (the failing statement is logged and will be
    retried on next startup).
    """
    summary = {"applied": [], "skipped": [], "failed": []}
    try:
        _ensure_tracking(engine)
    except Exception as exc:
        log.error("migration: tracking table setup failed: %s", exc)
        summary["failed"].append("schema_migrations setup")
        return summary

    try:
        applied = _applied_versions(engine)
    except Exception as exc:
        log.error("migration: could not read schema_migrations: %s", exc)
        return summary

    for path in _iter_sql_files(_AUTO_DIR):
        if path.name in applied:
            summary["skipped"].append(path.name)
            continue
        if _apply_file(engine, path, source="auto"):
            summary["applied"].append(path.name)
        else:
            summary["failed"].append(path.name)
    return summary


def run_manual(engine: Engine, filename: str | None = None) -> dict:
    """Apply a specific manual migration (or all pending manual ones).

    Meant to be invoked from a CLI after the user has confirmed they have a
    recent backup. Raises if the file does not exist or fails.
    """
    _ensure_tracking(engine)
    applied = _applied_versions(engine)

    if not _MANUAL_DIR.is_dir():
        raise RuntimeError(f"manual migration folder not found: {_MANUAL_DIR}")

    targets = list(_iter_sql_files(_MANUAL_DIR))
    if filename:
        targets = [p for p in targets if p.name == filename]
        if not targets:
            raise RuntimeError(f"manual migration not found: {filename}")

    summary = {"applied": [], "skipped": [], "failed": []}
    for path in targets:
        if path.name in applied:
            summary["skipped"].append(path.name)
            continue
        ok = _apply_file(engine, path, source="manual")
        if ok:
            summary["applied"].append(path.name)
        else:
            summary["failed"].append(path.name)
            raise RuntimeError(f"manual migration {path.name} failed; see logs")
    return summary


# ── CLI entry point ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    import sys

    sys.path.insert(0, str(_MIGRATIONS_DIR.parent))
    # Load env the same way main.py does
    _env_path = _MIGRATIONS_DIR.parent / ".env"
    if _env_path.is_file():
        for line in _env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from database import engine as _engine  # type: ignore

    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("status", help="List applied and pending migrations.")
    sub.add_parser("auto", help="Apply all pending migrations under migrations/sql/.")

    mp = sub.add_parser("manual", help="Apply a manual migration.")
    mp.add_argument("--file", help="Specific file name, e.g. 010_timestamps_to_native.sql. Omit to run all pending manual migrations.")

    args = parser.parse_args()

    if args.cmd == "status":
        _ensure_tracking(_engine)
        applied = _applied_versions(_engine)
        for folder, label in ((_AUTO_DIR, "auto"), (_MANUAL_DIR, "manual")):
            print(f"\n[{label}]  {folder}")
            for p in _iter_sql_files(folder):
                mark = "APPLIED" if p.name in applied else "PENDING"
                print(f"  {mark:8s} {p.name}")
    elif args.cmd == "auto":
        summary = run_pending_safe(_engine)
        print("auto migration result:", summary)
    elif args.cmd == "manual":
        summary = run_manual(_engine, filename=args.file)
        print("manual migration result:", summary)
