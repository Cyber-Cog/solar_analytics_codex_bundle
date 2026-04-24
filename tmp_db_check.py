import sys, os
sys.path.append('backend')

# Load env variables for database config
env_path = os.path.join('backend', '.env')
if os.path.isfile(env_path):
    with open(env_path) as f:
        for line in f:
            if '=' in line and not line.startswith('#'):
                k, v = line.strip().split('=', 1)
                os.environ[k] = v

from backend.database import engine
from sqlalchemy import text

try:
    with engine.connect() as conn:
        res = conn.execute(text("SELECT MIN(timestamp), MAX(timestamp), COUNT(*) FROM raw_data_generic")).fetchone()
        print(f'Raw Data Stats -> Min: {res[0]}, Max: {res[1]}, Count: {res[2]}')

        # Check plant
        res2 = conn.execute(text("SELECT DISTINCT plant_id FROM raw_data_generic")).fetchall()
        print(f'Plants in raw data: {[r[0] for r in res2]}')
        
        # Check specific dataset range (March 2026)
        res_date = conn.execute(text("SELECT count(*) FROM raw_data_generic WHERE timestamp LIKE '2026-03%'")).fetchone()
        print(f'Rows in March 2026: {res_date[0]}')

except Exception as e:
    print(f'Error connecting or querying: {e}')
