import sys
sys.path.insert(0, "backend")
from database import engine
from sqlalchemy import text

with engine.connect() as conn:
    tables = conn.execute(text(
        "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename"
    )).fetchall()
    print("ALL TABLES:")
    for t in tables:
        print(" ", t[0])
    print()

    # Check what DS detection actually writes to
    ds_tables = [t[0] for t in tables if "ds" in t[0].lower() or "scb" in t[0].lower() or "fault" in t[0].lower() or "snapshot" in t[0].lower() or "compute" in t[0].lower() or "stat" in t[0].lower()]
    print("Relevant tables:", ds_tables)

    # Check counts in key tables for Tiger
    for tbl in ds_tables:
        try:
            if "plant_id" in str(conn.execute(text(f"SELECT column_name FROM information_schema.columns WHERE table_name='{tbl}' AND column_name='plant_id'")).fetchall()):
                c = conn.execute(text(f"SELECT COUNT(*) FROM {tbl} WHERE plant_id='Tiger'")).fetchone()
                print(f"  {tbl}: {c[0]:,} Tiger rows")
            else:
                c = conn.execute(text(f"SELECT COUNT(*) FROM {tbl}")).fetchone()
                print(f"  {tbl}: {c[0]:,} rows (no plant_id col)")
        except Exception as e:
            print(f"  {tbl}: {e}")
