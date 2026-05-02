"""diagnose_tiger.py - Check DB state and run missing post-ingest jobs."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "backend"))

from database import SessionLocal, engine
from sqlalchemy import text

db = SessionLocal()

print("=== 1. RAW DATA IN DB ===")
r = db.execute(text("""
    SELECT equipment_level, signal, COUNT(*) as cnt,
           MIN(timestamp) as min_ts, MAX(timestamp) as max_ts
    FROM raw_data_generic
    WHERE plant_id = 'Tiger'
    GROUP BY equipment_level, signal
    ORDER BY equipment_level, signal
""")).fetchall()
for row in r:
    print(f"  {row[0]:12s} | {row[1]:20s} | {row[2]:>8,} rows | {row[3]} -> {row[4]}")

total = sum(row[2] for row in r)
print(f"  TOTAL: {total:,} rows")

print()
print("=== 2. RAW DATA STATS TABLE ===")
s = db.execute(text("SELECT * FROM raw_data_stats WHERE plant_id='Tiger'")).fetchone()
print(f"  {s}")

print()
print("=== 3. PLANT COMPUTE STATUS ===")
pc = db.execute(text("SELECT * FROM plant_compute_status WHERE plant_id='Tiger'")).fetchone()
print(f"  {pc}")

print()
print("=== 4. DS DETECTION RESULTS ===")
ds = db.execute(text("""
    SELECT COUNT(*) FROM scb_daily_status WHERE plant_id='Tiger'
""")).fetchone()
print(f"  scb_daily_status rows: {ds[0]:,}")

ds2 = db.execute(text("""
    SELECT status, COUNT(*) FROM scb_daily_status WHERE plant_id='Tiger' GROUP BY status
""")).fetchall()
for row in ds2:
    print(f"    status={row[0]}: {row[1]:,}")

print()
print("=== 5. SNAPSHOT TABLES ===")
for tbl in ["daily_energy_snapshot", "plant_hourly_snapshot", "inverter_daily_snapshot"]:
    try:
        c = db.execute(text(f"SELECT COUNT(*) FROM {tbl} WHERE plant_id='Tiger'")).fetchone()
        print(f"  {tbl}: {c[0]:,} rows")
    except Exception as e:
        print(f"  {tbl}: ERROR - {e}")

print()
print("=== 6. UNIFIED FAULT FEED TEST ===")
try:
    faults = db.execute(text("""
        SELECT COUNT(*) FROM scb_daily_status 
        WHERE plant_id='Tiger' AND timestamp >= '2026-02-25' AND timestamp <= '2026-03-25'
    """)).fetchone()
    print(f"  SCB faults in date range: {faults[0]:,}")
except Exception as e:
    print(f"  Fault query error: {e}")

db.close()
