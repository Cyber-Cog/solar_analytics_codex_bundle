"""
backend/database.py
====================
SQLAlchemy engine + session factory.
All routers import `get_db` to obtain a session.

PostgreSQL only: set DATABASE_URL in backend/.env (loaded from main.py before import).
Install: pip install psycopg2-binary
"""

import os
from sqlalchemy import create_engine, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool, QueuePool

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL is not set. Add to backend/.env, e.g.\n"
        "  DATABASE_URL=postgresql://solar:solar@localhost:5432/solar\n"
        "PostgreSQL is required; SQLite is no longer supported as the app database."
    )
if not (DATABASE_URL.startswith("postgresql") or DATABASE_URL.startswith("postgres://")):
    raise RuntimeError(
        f"DATABASE_URL must be a PostgreSQL URL (postgresql://...). Got: {DATABASE_URL[:40]}..."
    )

_ECHO = os.environ.get("SQL_ECHO", "").lower() in ("1", "true")
_SERVERLESS = os.environ.get("SOLAR_SERVERLESS", "").lower() in ("1", "true", "yes") or os.environ.get("VERCEL") == "1"

# Statement timeout (ms) — safety net so runaway queries don't consume the
# entire Vercel function budget (default 60 s). Applies per-session.
_STATEMENT_TIMEOUT_MS = int(os.environ.get("DB_STATEMENT_TIMEOUT_MS", "25000"))


def _engine_kwargs(pool_size: int, max_overflow: int) -> dict:
    kwargs = {
        "echo": _ECHO,
        "pool_pre_ping": True,
        # TCP keepalive so NAT gateways / EC2 don't drop idle connections
        "connect_args": {
            "options": f"-c statement_timeout={_STATEMENT_TIMEOUT_MS}",
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 5,
        },
    }
    if _SERVERLESS:
        # Use a tiny QueuePool instead of NullPool.  Warm Vercel invocations
        # reuse the existing TCP+SSL connection (~0 ms) instead of opening a
        # fresh one every call (~300-800 ms to EC2). pool_size=1 means at most
        # 1 idle connection is kept; max_overflow=2 allows brief bursts.
        kwargs["poolclass"] = QueuePool
        kwargs["pool_size"] = 1
        kwargs["max_overflow"] = 2
        kwargs["pool_recycle"] = 270  # recycle before Vercel's ~5 min freeze
        kwargs["pool_timeout"] = 10
    else:
        kwargs["pool_size"] = pool_size
        kwargs["max_overflow"] = max_overflow
    return kwargs

# ── Write pool (ingests, schema changes, admin mutations) ────────────────────
engine = create_engine(
    DATABASE_URL,
    **_engine_kwargs(pool_size=10, max_overflow=20),
)

# ── Read pool (dashboard / analytics / fault pages) ──────────────────────────
# Two distinct pools mean a heavy upload cannot starve interactive reads: each
# pool has its own set of DB connections. If DATABASE_URL_READ is set it will
# be used (handy when pointing reads at a read-replica); otherwise we reuse
# the same URL with a larger pool.
_READ_URL = os.environ.get("DATABASE_URL_READ", "").strip() or DATABASE_URL
_READ_POOL_SIZE = int(os.environ.get("DB_READ_POOL_SIZE", "20"))
_READ_MAX_OVERFLOW = int(os.environ.get("DB_READ_MAX_OVERFLOW", "20"))

read_engine = create_engine(
    _READ_URL,
    **_engine_kwargs(pool_size=_READ_POOL_SIZE, max_overflow=_READ_MAX_OVERFLOW),
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
ReadSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=read_engine)
Base = declarative_base()


def get_db():
    """FastAPI dependency — yields a write-capable DB session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_read_db():
    """FastAPI dependency — session bound to the read pool.

    Use this in GET endpoints that only read. Falls back to the same physical
    DB when no read-replica is configured, but always uses a separate pool so
    heavy writes (uploads, migrations) can't starve dashboards.
    """
    db = ReadSessionLocal()
    try:
        yield db
    finally:
        db.close()
