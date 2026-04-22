"""
Recompute all Disconnected String (DS) fault data using the corrected algorithm.
Delegates to engine/ds_detection.py so the logic is never duplicated.

Usage (from repo root):
    cd solar_analytics_codex_bundle/backend
    python scripts/recompute_ds_faults.py [--plant NTPCNOKHRA]

Windows PowerShell:
    cd backend; python scripts/recompute_ds_faults.py --plant NTPCNOKHRA
"""

import sys
import os

BACKEND_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND_ROOT)

import script_env
script_env.load_backend_env()

import argparse
from datetime import timedelta

import pandas as pd
import numpy as np

from database import engine, SessionLocal
from engine.ds_detection import run_ds_detection


def fetch_plant_ids(cur):
    cur.execute("SELECT plant_id FROM plants ORDER BY plant_id")
    return [r[0] for r in cur.fetchall()]


def fetch_plant_dates(cur, plant_id, date_from, date_to):
    query = "SELECT DISTINCT DATE(timestamp) FROM raw_data_generic WHERE plant_id=%s AND LOWER(TRIM(equipment_level::text))='scb' AND signal='dc_current'"
    params = [plant_id]
    if date_from:
        query += " AND timestamp >= %s"
        params.append(f"{date_from} 00:00:00")
    if date_to:
        query += " AND timestamp <= %s"
        params.append(f"{date_to} 23:59:59")
    query += " ORDER BY 1"
    cur.execute(query, tuple(params))
    return [r[0] for r in cur.fetchall()]


def fetch_raw_for_plant_day(cur, plant_id, target_date):
    """
    Pull all raw SCB dc_current + dc_voltage data for ONE day into a DataFrame
    that matches what run_ds_detection() expects.
    """
    cur.execute(
        "SELECT DISTINCT scb_id, inverter_id FROM plant_architecture "
        "WHERE plant_id=%s AND scb_id IS NOT NULL",
        (plant_id,),
    )
    arch = cur.fetchall()
    scb_inverter = {str(r[0]).strip(): str(r[1]).strip() for r in arch if r[1]}

    date_str = target_date.strftime('%Y-%m-%d')
    start_ts = f"{date_str} 00:00:00"
    end_ts = f"{date_str} 23:59:59"

    # DC current
    cur.execute(
        "SELECT timestamp, equipment_id, value FROM raw_data_generic "
        "WHERE plant_id=%s AND LOWER(TRIM(equipment_level::text))='scb' "
        "AND signal='dc_current' AND timestamp >= %s AND timestamp <= %s ORDER BY timestamp",
        (plant_id, start_ts, end_ts),
    )
    curr_rows = cur.fetchall()
    if not curr_rows:
        return pd.DataFrame()

    # DC voltage
    cur.execute(
        "SELECT timestamp, equipment_id, value FROM raw_data_generic "
        "WHERE plant_id=%s AND LOWER(TRIM(equipment_level::text))='scb' "
        "AND signal='dc_voltage' AND timestamp >= %s AND timestamp <= %s",
        (plant_id, start_ts, end_ts),
    )
    volt_rows = cur.fetchall()
    volt_map = {(pd.to_datetime(r[0]), str(r[1]).strip()): float(r[2]) for r in volt_rows if r[2] is not None}

    df = pd.DataFrame(curr_rows, columns=["timestamp", "scb_id", "scb_current"])
    df["scb_id"] = df["scb_id"].astype(str).str.strip()
    df["inverter_id"] = df["scb_id"].map(scb_inverter)
    df.dropna(subset=["inverter_id"], inplace=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    if volt_map:
        # volt_map keys are (string_ts, equipment_id). We ensure matching by converting df items to str.
        # But wait, Step 4 in run_ds_detection uses pd.to_datetime(r[0]) for irr_map.
        # Let's just make volt_map keys datetime objects to be consistent.
        pass
    
    # Actually, let's fix the volt_map creation to use pd.to_datetime as well.
    # See ReplacementChunk 3.
    df["dc_voltage"] = df.set_index(["timestamp", "scb_id"]).index.map(volt_map.get).astype(float)
    return df


def main():
    parser = argparse.ArgumentParser(description="Recompute DS faults using the updated algorithm.")
    parser.add_argument("--plant", default=None, help="Plant ID to recompute. Omit to recompute ALL plants.")
    parser.add_argument("--from", dest="date_from", default=None, help="Start date YYYY-MM-DD")
    parser.add_argument("--to", dest="date_to", default=None, help="End date YYYY-MM-DD")
    args = parser.parse_args()

    try:
        conn = engine.raw_connection()
    except Exception as e:
        print(
            f"Could not connect to PostgreSQL. Check backend/.env DATABASE_URL.\n  Error: {e}",
            file=sys.stderr, flush=True,
        )
        sys.exit(1)

    conn.autocommit = False
    cur = conn.cursor()

    # Determine plant list
    if args.plant:
        plant_ids = [args.plant]
    else:
        plant_ids = fetch_plant_ids(cur)
        if not plant_ids:
            print("No plants found in the database.", flush=True)
            sys.exit(0)
        print(f"Recomputing for ALL plants: {plant_ids}", flush=True)

    cur.close()
    conn.close()

    # One SQLAlchemy session per run (run_ds_detection uses ORM session)
    db = SessionLocal()
    try:
        for plant_id in plant_ids:
            print(f"\n{'='*60}", flush=True)
            print(f"--- Plant: {plant_id} ---", flush=True)

            # --- CLEAR RANGE FIRST ---
            if args.date_from and args.date_to:
                print(f"  Clearing existing faults for {args.date_from} -> {args.date_to}...", end=" ", flush=True)
                db.execute(
                    __import__("sqlalchemy").text(
                        "DELETE FROM fault_diagnostics WHERE plant_id = :p AND timestamp >= :f AND timestamp <= :t"
                    ),
                    {"p": plant_id, "f": f"{args.date_from} 00:00:00", "t": f"{args.date_to} 23:59:59"},
                )
                db.commit()
                print("Done.", flush=True)

            print(f"{'='*60}", flush=True)

            # Use a raw psycopg2 connection just for the fast bulk fetch
            raw_conn = engine.raw_connection()
            raw_conn.autocommit = False
            raw_cur = raw_conn.cursor()

            try:
                dates = fetch_plant_dates(raw_cur, plant_id, args.date_from, args.date_to)
            finally:
                raw_cur.close()
                raw_conn.close()

            if not dates:
                print("  No raw SCB dc_current dates found.", flush=True)
                continue

            print(f"  Found {len(dates)} days of data to process.", flush=True)
            for d in dates:
                print(f"  [{d}] Fetching data...", end=" ", flush=True)
                
                raw_conn = engine.raw_connection()
                raw_cur = raw_conn.cursor()
                df = fetch_raw_for_plant_day(raw_cur, plant_id, d)
                raw_cur.close()
                raw_conn.close()
                
                if df.empty:
                    print("Empty.")
                    continue
                
                print(f"Processing {len(df):,} rows...", end=" ", flush=True)
                run_ds_detection(plant_id, df, db)
                print("Done.", flush=True)

            # Count results
            result = db.execute(
                __import__("sqlalchemy").text(
                    "SELECT COUNT(*), SUM(CASE WHEN fault_status='CONFIRMED_DS' THEN 1 ELSE 0 END) "
                    "FROM fault_diagnostics WHERE plant_id=:p"
                ),
                {"p": plant_id},
            ).fetchone()
            total_rows  = result[0] or 0
            ds_rows     = result[1] or 0
            print(f"  Done. Written: {total_rows:,} rows | CONFIRMED_DS: {ds_rows:,}", flush=True)

    finally:
        db.close()

    print("\n✅ Recompute complete.", flush=True)


if __name__ == "__main__":
    main()
