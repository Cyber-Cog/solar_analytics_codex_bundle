"""
common/database.py
==================
Database helpers for modules/ scripts (PostgreSQL only).

Loads DATABASE_URL from environment or backend/.env.
SQL uses SQLite-style ? placeholders; they are converted to PostgreSQL %s.
"""

from __future__ import annotations

import os
import re
import pandas as pd
from typing import Any, List, Optional

# ── Load backend/.env (same keys as FastAPI) ─────────────────────────────────
_COMMON_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT_DIR = os.path.dirname(_COMMON_DIR)
_BACKEND_ENV = os.path.join(_ROOT_DIR, "backend", ".env")
if os.path.isfile(_BACKEND_ENV):
    for _line in open(_BACKEND_ENV, encoding="utf-8", errors="ignore"):
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            if _k.strip() not in os.environ:
                os.environ[_k.strip()] = _v.strip()


def _require_pg_url() -> str:
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url or not (url.startswith("postgresql") or url.startswith("postgres://")):
        raise RuntimeError(
            "DATABASE_URL must be set to a PostgreSQL URL (e.g. in backend/.env). "
            "SQLite (solar.db) is no longer supported."
        )
    return url


def _sql_qmarks_to_percent_s(sql: str) -> str:
    """Convert positional ? placeholders to PostgreSQL %s (do not use ? inside string literals)."""
    return re.sub(r"\?(?=(?:[^']*'[^']*')*[^']*$)", "%s", sql)


def get_connection():
    """Return a new psycopg2 connection (caller must close)."""
    import psycopg2

    return psycopg2.connect(_require_pg_url())


def execute_query(sql: str, params: tuple = ()) -> List[Any]:
    """Execute SELECT; returns list of dict-like rows (RealDictRow)."""
    from psycopg2.extras import RealDictCursor

    sql_pg = _sql_qmarks_to_percent_s(sql)
    conn = get_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql_pg, params)
        return list(cur.fetchall())
    finally:
        conn.close()


def execute_query_df(sql: str, params: tuple = ()) -> pd.DataFrame:
    conn = get_connection()
    try:
        sql_pg = _sql_qmarks_to_percent_s(sql)
        if params:
            return pd.read_sql_query(sql_pg, conn, params=list(params))
        return pd.read_sql_query(sql_pg, conn)
    finally:
        conn.close()


def execute_write(sql: str, params: tuple = ()) -> int:
    sql_pg = _sql_qmarks_to_percent_s(sql)
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql_pg, params)
        conn.commit()
        return cur.rowcount if cur.rowcount is not None else 0
    finally:
        conn.close()


def execute_many(sql: str, records: list[tuple]) -> int:
    if not records:
        return 0
    sql_pg = _sql_qmarks_to_percent_s(sql)
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.executemany(sql_pg, records)
        conn.commit()
        return cur.rowcount if cur.rowcount is not None else len(records)
    finally:
        conn.close()


def table_exists(table_name: str) -> bool:
    rows = execute_query(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ? LIMIT 1",
        (table_name,),
    )
    return len(rows) > 0


def db_is_initialized() -> bool:
    """True if PostgreSQL is reachable and core tables exist."""
    try:
        _require_pg_url()
    except RuntimeError:
        return False
    required = {
        "raw_data_generic",
        "dc_hierarchy_derived",
        "plant_architecture",
        "equipment_specs",
    }
    try:
        for t in required:
            if not table_exists(t):
                return False
        return True
    except Exception:
        return False
