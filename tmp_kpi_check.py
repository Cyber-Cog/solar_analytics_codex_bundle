import sys, os
sys.path.append('backend')
env_path = os.path.join('backend', '.env')
if os.path.isfile(env_path):
    with open(env_path) as f:
        for line in f:
            if '=' in line and not line.startswith('#'):
                k, v = line.strip().split('=', 1)
                os.environ[k] = v

from backend.database import SessionLocal
from backend.ac_power_energy_sql import sql_plant_ac_daily_energy
from backend.routers.dashboard import _wms_tilt_insolation_kwh_m2
from sqlalchemy import text

db = SessionLocal()
try:
    print("Testing sql_plant_ac_daily_energy...")
    sql = text(sql_plant_ac_daily_energy("raw_data_generic"))
    rows = db.execute(sql, {"plant_id": "NTPCNOKHRA", "from_ts": "2026-03-01 00:00:00", "to_ts": "2026-03-07 23:59:59"}).fetchall()
    print("Energy rows:", rows)

    print("Testing _wms_tilt_insolation_kwh_m2...")
    ins = _wms_tilt_insolation_kwh_m2(db, "raw_data_generic", "NTPCNOKHRA", "2026-03-01 00:00:00", "2026-03-07 23:59:59")
    print("Insolation result:", ins)
except Exception as e:
    print("EXCEPTION:", e)
finally:
    db.close()
