import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import text
from dotenv import load_dotenv

# Load environment and database
load_dotenv('backend/.env')
sys.path.insert(0, 'backend')
from database import SessionLocal
from models import RawDataGeneric

PLANT_ID = "NTPCNOKHRA"
SOURCE_DIR = r"C:\Users\Asus\Downloads\NTPCNOKHRA\SCBrawdata"

def parse_ts(val):
    try:
        ts = pd.to_datetime(val)
        return ts.replace(second=0, microsecond=0)
    except:
        return None

def build_inv_id(icr, inv):
    icr_num = "".join(ch for ch in str(icr) if ch.isdigit()).zfill(2)
    inv_num = "".join(ch for ch in str(inv) if ch.isdigit())
    inv_letter = {"1": "A", "2": "B", "3": "C", "4": "D", "5": "E"}.get(inv_num, "")
    return f"INV-{icr_num}{inv_letter}"

def ingest_file(path, db):
    print(f"Processing {os.path.basename(path)}...")
    df_raw = pd.read_excel(path, header=None)
    
    icrs = df_raw.iloc[6].ffill()
    invs = df_raw.iloc[7].ffill()
    sigs = df_raw.iloc[8]
    
    data = df_raw.iloc[9:].copy()
    data[0] = data[0].apply(parse_ts)
    data = data.dropna(subset=[0])
    
    # Aggregating by minute (mean)
    for col in data.columns[1:]:
        data[col] = pd.to_numeric(data[col], errors='coerce')
    data_agg = data.groupby(0).mean().reset_index()
    
    # Re-import safety: clear previous ingest rows for the file day so reruns are idempotent.
    if not data_agg.empty:
        day = data_agg.iloc[0, 0].date()
        day_start = pd.Timestamp(day).strftime("%Y-%m-%d 00:00:00")
        day_end = pd.Timestamp(day).strftime("%Y-%m-%d 23:59:59")
        db.execute(
            text(
                """
                DELETE FROM raw_data_generic
                WHERE plant_id = :plant_id
                  AND source = 'nokhra_scb_ingest'
                  AND equipment_level = 'scb'
                  AND signal = 'dc_current'
                  AND timestamp BETWEEN :day_start AND :day_end
                """
            ),
            {"plant_id": PLANT_ID, "day_start": day_start, "day_end": day_end},
        )
        db.commit()

    batch = []
    count = 0
    
    for _, row in data_agg.iterrows():
        ts = row[0]
        for c in range(1, len(data_agg.columns)):
            signal_name = str(sigs[c]).strip()
            if not signal_name.startswith("DC_INPUT_CURRENT"):
                continue
            
            icr = icrs[c]
            inv = invs[c]
            if pd.isna(icr) or pd.isna(inv):
                continue
                
            inverter_id = build_inv_id(icr, inv)
            scb_suffix = "".join(ch for ch in signal_name if ch.isdigit()).zfill(2)
            scb_id = f"SCB-{inverter_id.replace('INV-', '')}-{scb_suffix}"
            
            val = row[c]
            if pd.notnull(val):
                batch.append({
                    "plant_id": PLANT_ID,
                    "timestamp": ts,
                    "equipment_level": "scb",
                    "equipment_id": scb_id,
                    "signal": "dc_current",
                    "value": float(val),
                    "source": "nokhra_scb_ingest"
                })
                count += 1
                
            if len(batch) >= 5000:
                db.bulk_insert_mappings(RawDataGeneric, batch)
                db.commit()
                batch = []
                
    if batch:
        db.bulk_insert_mappings(RawDataGeneric, batch)
        db.commit()
    
    print(f"  Imported {count} points from {os.path.basename(path)}")
    return count

def main():
    db = SessionLocal()
    files = [f for f in os.listdir(SOURCE_DIR) if f.endswith('.xlsx')]
    print(f"Found {len(files)} files: {files}")
    total = 0
    
    for f in files:
        path = os.path.join(SOURCE_DIR, f)
        c = ingest_file(path, db)
        print(f"File {f} imported {c} points.")
        total += c
        
    print(f"\nFinished. Total points imported: {total}")
    db.close()

if __name__ == "__main__":
    main()
