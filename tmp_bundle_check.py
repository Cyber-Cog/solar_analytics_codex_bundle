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
from backend.routers.dashboard import dashboard_bundle

db = SessionLocal()
class FakeUser:
    id = 1
    email = "admin@example.com"
    allowed_plants = None
    is_admin = True

try:
    print("Testing dashboard_bundle API...")
    out = dashboard_bundle(plant_id="NTPCNOKHRA", date_from="2026-03-01", date_to="2026-03-07", db=db, current_user=FakeUser())
    print("Bundle success! Keys:", out.keys())
    #print("Bundle response:", out)
except Exception as e:
    import traceback
    traceback.print_exc()
finally:
    db.close()
