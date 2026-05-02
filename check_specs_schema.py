"""
Check columns and constraints on the equipment_specs table.
Run from project root:  python check_specs_schema.py

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

from database import engine
from sqlalchemy import text, inspect

insp = inspect(engine)
cols = [c["name"] for c in insp.get_columns("equipment_specs")]
print("Columns:", cols)

with engine.connect() as conn:
    constraints = conn.execute(text(
        "SELECT constraint_name, constraint_type FROM information_schema.table_constraints WHERE table_name='equipment_specs'"
    )).fetchall()
    print("Constraints:", constraints)

    sample = conn.execute(text("SELECT * FROM equipment_specs LIMIT 1")).fetchone()
    print("Sample row:", sample)
