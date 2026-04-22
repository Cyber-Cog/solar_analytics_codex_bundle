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
from sqlalchemy.pool import NullPool

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


def _engine_kwargs(pool_size: int, max_overflow: int) -> dict:
    kwargs = {
        "echo": _ECHO,
        "pool_pre_ping": True,
    }
    if _SERVERLESS:
        kwargs["poolclass"] = NullPool
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
