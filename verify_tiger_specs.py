"""
Verify Tiger inverter + module specs are correctly ingested.
Run from project root:  python verify_tiger_specs.py

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
from sqlalchemy import text

with engine.connect() as conn:
    print("=== MODULE ROW ===")
    r = conn.execute(text(
        "SELECT * FROM equipment_specs WHERE plant_id='Tiger' AND equipment_type='module'"
    )).fetchone()
    if r:
        keys = r._fields
        for k, v in zip(keys, r):
            if v is not None:
                print(f"  {k}: {v}")

    print()
    print("=== INVERTER SUMMARY ===")
    rows = conn.execute(text(
        "SELECT equipment_id, ac_capacity_kw, dc_capacity_kwp, rated_efficiency, mppt_voltage_min, mppt_voltage_max "
        "FROM equipment_specs WHERE plant_id='Tiger' AND equipment_type='inverter' ORDER BY equipment_id"
    )).fetchall()
    for r in rows[:5]:
        print(f"  {r[0]}: AC={r[1]}kW DC={r[2]}kWp eff={r[3]}% MPPT={r[4]}-{r[5]}V")
    print(f"  ... total {len(rows)} inverters")
