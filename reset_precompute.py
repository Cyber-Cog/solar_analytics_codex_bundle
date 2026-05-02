"""
Reset the Tiger precompute job to pending.
Run from project root:  python reset_precompute.py

Requires DATABASE_URL in the environment or in backend/.env
"""
import sys, os
sys.path.insert(0, "backend")

_env_path = os.path.join("backend", ".env")
if os.path.isfile(_env_path):
    for _line in open(_env_path, encoding="utf-8"):
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            if _k.strip() not in os.environ:
                os.environ[_k.strip()] = _v.strip()

if not os.environ.get("DATABASE_URL"):
    raise SystemExit("ERROR: DATABASE_URL is not set.")

from database import SessionLocal
from sqlalchemy import text

db = SessionLocal()
db.execute(text("UPDATE precompute_jobs SET status='pending', attempts=0 WHERE plant_id='Tiger'"))
db.commit()
print("Precompute job reset to pending for Tiger.")
