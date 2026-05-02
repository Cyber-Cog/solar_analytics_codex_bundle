"""
Reset ALL precompute jobs to pending so the server will re-run them.
Run from project root:  python reset_all_precompute.py

Requires DATABASE_URL to be set in the environment or in backend/.env
"""
import sys, os
sys.path.insert(0, "backend")

# Load .env if present (never hard-code credentials)
_env_path = os.path.join("backend", ".env")
if os.path.isfile(_env_path):
    for _line in open(_env_path, encoding="utf-8"):
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            if _k.strip() not in os.environ:
                os.environ[_k.strip()] = _v.strip()

if not os.environ.get("DATABASE_URL"):
    raise SystemExit("ERROR: DATABASE_URL is not set. Export it or add it to backend/.env")

from database import SessionLocal
from sqlalchemy import text

db = SessionLocal()
try:
    result = db.execute(text("UPDATE precompute_jobs SET status='pending', attempts=0"))
    db.commit()
    print(f"Reset {result.rowcount} precompute jobs to pending for all plants.")
    plants = db.execute(text("SELECT plant_id, status FROM precompute_jobs")).fetchall()
    for p in plants:
        print(f"  {p[0]}: {p[1]}")
finally:
    db.close()
