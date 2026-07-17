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

def _normalize_pg_url(url: str) -> str:
    """
    Accept provider-style URLs and make them SQLAlchemy-2.x safe.
    SQLAlchemy removed the legacy 'postgres://' scheme; Neon / Supabase /
    Heroku / Render still hand out URLs in that form, so rewrite it.
    """
    url = (url or "").strip()
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    return url


def _ensure_sslmode(url: str, *, serverless: bool) -> str:
    """
    Managed Postgres (RDS / Neon / Supabase) often requires SSL from Vercel.
    If the URL has no sslmode and the host is not local, default to require
    when running serverless. Override with ?sslmode=disable if needed.
    """
    if not url or not serverless:
        return url
    lower = url.lower()
    if "sslmode=" in lower:
        return url
    # Local / docker hosts do not need forced SSL.
    try:
        from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        if host in ("", "localhost", "127.0.0.1", "::1", "db", "postgres"):
            return url
        q = dict(parse_qsl(parsed.query, keep_blank_values=True))
        q["sslmode"] = "require"
        return urlunparse(parsed._replace(query=urlencode(q)))
    except Exception:
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}sslmode=require"


DATABASE_URL = _normalize_pg_url(os.environ.get("DATABASE_URL", ""))
if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL is not set. Add to backend/.env, e.g.\n"
        "  DATABASE_URL=postgresql://solar:solar@localhost:5432/solar\n"
        "PostgreSQL is required; SQLite is no longer supported as the app database."
    )
if not DATABASE_URL.startswith("postgresql"):
    raise RuntimeError(
        f"DATABASE_URL must be a PostgreSQL URL (postgresql://...). Got: {DATABASE_URL[:40]}..."
    )

_ECHO = os.environ.get("SQL_ECHO", "").lower() in ("1", "true")
_SERVERLESS = os.environ.get("SOLAR_SERVERLESS", "").lower() in ("1", "true", "yes") or os.environ.get("VERCEL") == "1"
DATABASE_URL = _ensure_sslmode(DATABASE_URL, serverless=_SERVERLESS)

# Statement timeout (ms) — safety net so runaway queries don't consume the
# entire Vercel function budget. Applies per-session on every DB connection.
# Large on-prem / RDS plants (wide date ranges) often need 2–5+ minutes for first
# fault/loss/snapshot compute before caches exist. Vercel stays at 25s; self-hosted
# default is 5 minutes unless DB_STATEMENT_TIMEOUT_MS is set in .env.
_DEFAULT_STMT_MS = "25000" if _SERVERLESS else "300000"
_STATEMENT_TIMEOUT_MS = int(os.environ.get("DB_STATEMENT_TIMEOUT_MS", _DEFAULT_STMT_MS))


def _engine_kwargs(pool_size: int, max_overflow: int) -> dict:
    connect_args = {
        "connect_timeout": int(os.environ.get("DB_CONNECT_TIMEOUT_SEC", "15")),
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
    }
    # Some managed Postgres endpoints reject startup GUC options from serverless.
    _use_stmt = os.environ.get(
        "DB_USE_STATEMENT_TIMEOUT",
        "0" if _SERVERLESS else "1",
    ).strip().lower() in ("1", "true", "yes")
    if _use_stmt:
        connect_args["options"] = f"-c statement_timeout={_STATEMENT_TIMEOUT_MS}"

    kwargs = {
        "echo": _ECHO,
        "pool_pre_ping": True,
        "connect_args": connect_args,
    }
    if _SERVERLESS:
        # Use a tiny QueuePool instead of NullPool.  Warm Vercel invocations
        # reuse the existing TCP+SSL connection (~0 ms) instead of opening a
        # fresh one every call (~300-800 ms to EC2). pool_size=1 means at most
        # 1 idle connection is kept; max_overflow allows brief bursts (tune if
        # parallel bundle workers wait on the pool — watch Postgres max_connections).
        kwargs["poolclass"] = QueuePool
        kwargs["pool_size"] = int(os.environ.get("DB_SERVERLESS_POOL_SIZE", "1"))
        kwargs["max_overflow"] = int(os.environ.get("DB_SERVERLESS_MAX_OVERFLOW", "2"))
        kwargs["pool_recycle"] = 270  # recycle before Vercel's ~5 min freeze
        kwargs["pool_timeout"] = int(os.environ.get("DB_SERVERLESS_POOL_TIMEOUT", "10"))
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
_READ_URL = _ensure_sslmode(
    _normalize_pg_url(os.environ.get("DATABASE_URL_READ", "")) or DATABASE_URL,
    serverless=_SERVERLESS,
)
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
