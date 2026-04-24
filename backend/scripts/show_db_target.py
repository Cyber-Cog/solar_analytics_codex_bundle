"""
Print which Postgres host/database the app will use (from backend/.env + env override).
Does not print username or password.

Usage (from backend/):  python scripts/show_db_target.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from urllib.parse import urlparse, unquote

_EXPECT_HOST = "database-1.c92kau6ymg43.eu-north-1.rds.amazonaws.com"
_EXPECT_PORT = 5432
_EXPECT_DB = "postgres"


def _load_env(backend_dir: Path) -> None:
    p = backend_dir / ".env"
    if not p.is_file():
        return
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ[k.strip()] = v.strip()


def main() -> int:
    backend_dir = Path(__file__).resolve().parents[1]
    _load_env(backend_dir)

    raw = (os.environ.get("DATABASE_URL") or "").strip()
    if not raw:
        print("DATABASE_URL is not set (check backend/.env).")
        return 1

    # sqlalchemy style postgresql+psycopg2:// — normalize for urlparse
    if raw.startswith("postgresql+") and "://" in raw:
        raw = "postgresql://" + raw.split("://", 1)[1]

    u = urlparse(raw)
    host = (u.hostname or "").strip()
    port = u.port or 5432
    db = (u.path or "").lstrip("/").split("?", 1)[0] or "(none)"
    user = u.username or ""

    print("Effective DATABASE_URL (redacted):")
    print(f"  host     : {host}")
    print(f"  port     : {port}")
    print(f"  database : {db}")
    print(f"  user     : {user[:20] + '…' if user and len(user) > 20 else (user or '(none)')}")
    print()
    match = (
        host.lower() == _EXPECT_HOST.lower()
        and int(port) == int(_EXPECT_PORT)
        and db.lower() == _EXPECT_DB.lower()
    )
    if match:
        print("MATCH: This matches your JDBC target (eu-north-1 RDS, database postgres).")
    else:
        print("MISMATCH vs JDBC: postgresql://database-1.c92kau6ymg43.eu-north-1.rds.amazonaws.com:5432/postgres")
        print("  Fix DATABASE_URL in backend/.env and restart the API.")
    read_url = (os.environ.get("DATABASE_URL_READ") or "").strip()
    if read_url:
        print()
        print("DATABASE_URL_READ is also set (read pool uses a different URL).")
        if read_url.startswith("postgresql+") and "://" in read_url:
            read_url = "postgresql://" + read_url.split("://", 1)[1]
        r = urlparse(read_url)
        print(f"  read host: {r.hostname}  port: {r.port or 5432}  db: {(r.path or '').lstrip('/').split('?',1)[0]}")
    return 0 if match else 2


if __name__ == "__main__":
    raise SystemExit(main())
