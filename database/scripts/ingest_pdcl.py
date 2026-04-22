import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime

# Path setup
BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

# Load .env
_env_path = os.path.join(BACKEND_DIR, ".env")
if os.path.isfile(_env_path):
    print(f"Loading .env from {_env_path}")
    for _line in open(_env_path):
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            if _k.strip() not in os.environ:
                os.environ[_k.strip()] = _v.strip()

from database import SessionLocal
from models import RawDataGeneric
from sqlalchemy import text

DATA_DIR = r"C:\Users\Asus\Downloads\PDCL"
PLANT_ID = "PDCL"

# Global store for fallbacks
inv_voltages = {} # {timestamp: {inv_id: voltage}}

def get_db():
    db = SessionLocal()
    try:
        return db
    except:
        db.close()
        raise

def parse_ts(ts_str, date_format="%Y-%m-%d %H:%M:%S", truncate_to_minute=False):
    try:
        if isinstance(ts_str, datetime):
            dt = ts_str
        else:
            dt = None
            for fmt in [date_format, "%d/%m/%y %H:%M:%S", "%d/%m/%Y %H:%M:%S"]:
                try:
                    dt = datetime.strptime(str(ts_str), fmt)
                    break
                except:
                    continue
        if dt is None:
            return str(ts_str)
        # Ensure year is 2026 if it comes as 26
        if dt.year < 2000:
            dt = dt.replace(year=dt.year + 2000)
        # Truncate to minute so :15 second SMB timestamps align with :00 inverter timestamps
        if truncate_to_minute:
            dt = dt.replace(second=0, microsecond=0)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return str(ts_str)

def ingest_inverters(db):
    print("--- Ingesting Inverters ---")
    inv_dir = os.path.join(DATA_DIR, "INV_RAW DATA")
    files = [f for f in os.listdir(inv_dir) if f.endswith(".csv")]
    
    for f in files:
        inv_part = f.split("-")[1]
        inv_num = "".join(filter(str.isdigit, inv_part))
        inv_id = f"INV{inv_num.zfill(2)}"
        
        path = os.path.join(inv_dir, f)
        df = pd.read_csv(path)
        
        mappings = {
            'DC voltage(V)': 'dc_voltage',
            'DC current(A)': 'dc_current',
            'DC power(Kw)':  'dc_power',
            'Active Power(kw)': 'ac_power',
            'Daily Cumu.energy(kWh)': 'ac_energy'
        }
        
        # Standard signals
        df['ts'] = df['Time'].apply(lambda x: parse_ts(x, truncate_to_minute=True))
        
        # Keep only used columns for aggregation
        used_cols = ['ts'] + [c for c in mappings.keys() if c in df.columns]
        df_num = df[used_cols].copy()
        for col in mappings.keys():
            if col in df_num.columns:
                df_num[col] = pd.to_numeric(df_num[col], errors='coerce')
        
        df_agg = df_num.groupby('ts').mean().reset_index()
        
        objs = []
        for _, row in df_agg.iterrows():
            ts = row['ts']
            
            # Save voltage for SCB fallback
            if ts not in inv_voltages: inv_voltages[ts] = {}
            v_val = row.get('DC voltage(V)')
            if pd.notnull(v_val):
                inv_voltages[ts][inv_id] = float(v_val)
            
            # Standard signals
            for col, signal in mappings.items():
                if col in row and pd.notnull(row[col]):
                    objs.append({
                        'plant_id': PLANT_ID, 'timestamp': ts, 'equipment_level': 'inverter',
                        'equipment_id': inv_id, 'signal': signal, 'value': float(row[col]), 'source': 'pdcl_ingest'
                    })
            
            # Calculation logic: dc_power = dc_current * dc_voltage if missing
            if pd.isnull(row.get('DC power(Kw)')) and pd.notnull(row.get('DC voltage(V)')) and pd.notnull(row.get('DC current(A)')):
                calc_pow = (float(row['DC voltage(V)']) * float(row['DC current(A)'])) / 1000.0
                objs.append({
                    'plant_id': PLANT_ID, 'timestamp': ts, 'equipment_level': 'inverter',
                    'equipment_id': inv_id, 'signal': 'dc_power', 'value': calc_pow, 'source': 'pdcl_ingest'
                })
        
        if objs:
            db.bulk_insert_mappings(RawDataGeneric, objs)
            db.commit()
            print(f"  Processed {f} -> {inv_id}: {len(objs)} points")

def ingest_smbs(db):
    print("--- Ingesting SMBs ---")
    smb_dir = os.path.join(DATA_DIR, "String data")
    if not os.path.exists(smb_dir):
        print(f"  Dir not found: {smb_dir}")
        return
    files = [f for f in os.listdir(smb_dir) if f.endswith(".xlsx")]
    
    all_objs = []
    for f in files:
        path = os.path.join(smb_dir, f)
        print(f"  Reading {f}...")
        meta_df = pd.read_excel(path, header=None, nrows=10, engine='openpyxl')
        row2 = meta_df.iloc[2].tolist()
        row3 = meta_df.iloc[3].tolist()
        
        smb_blocks = []
        needed_cols = [0]
        for i, val in enumerate(row2):
            if pd.notnull(val) and "SMB" in str(val):
                raw_id = str(val)
                parts = raw_id.split("_")
                inv_num = parts[1].replace("INV", "").zfill(2) if len(parts) > 1 else "01"
                smb_num = parts[2].replace("SMB", "").zfill(2) if len(parts) > 2 else "01"
                scb_id = f"SCB{inv_num}-{smb_num}"
                inv_id = f"INV{inv_num}"
                smb_blocks.append((i, scb_id, inv_id, inv_num, smb_num))
                # Identify columns to read for this SMB
                for idx in range(i, i + 35):
                    if idx < len(row3): needed_cols.append(idx)
        
        needed_cols = sorted(list(set(needed_cols)))
        print(f"    Loading {len(needed_cols)} columns from data rows...")
        data_df = pd.read_excel(path, header=None, skiprows=5, usecols=needed_cols, engine='openpyxl')
        
        # Aggregate per minute
        data_df[0] = data_df[0].apply(lambda x: parse_ts(x, truncate_to_minute=True))
        for col in data_df.columns:
            if col == 0: continue
            data_df[col] = pd.to_numeric(data_df[col], errors='coerce')
        
        print("    Aggregating by minute...")
        data_df_agg = data_df.groupby(0).mean().reset_index()

        file_count = 0
        for i, scb_id, inv_id, inv_num, smb_num in smb_blocks:
            for idx in range(i, i + 35):
                if idx >= len(row3) or idx not in data_df_agg.columns: continue
                signal_raw = str(row3[idx]).strip()
                if not signal_raw or signal_raw == "nan": continue
                
                db_signal = None; level = 'scb'; equip_id = scb_id
                if 'INSTANT VOLTAGE (V)' in signal_raw: db_signal = 'dc_voltage'
                elif 'POWER (kW)' in signal_raw: db_signal = 'dc_power'
                elif 'SUM ALL CURRENT (A)' in signal_raw: db_signal = 'dc_current'
                elif 'STRING CURR' in signal_raw:
                    db_signal = 'string_current'; level = 'string'
                    num_raw = "".join(filter(str.isdigit, signal_raw))
                    equip_id = f"str{inv_num}-{smb_num}-{num_raw.zfill(2)}"
                
                if db_signal:
                    col_data = data_df_agg[idx]
                    dates = data_df_agg[0]
                    for r_idx, v in col_data.items():
                        if pd.notnull(v):
                            all_objs.append({
                                'plant_id': PLANT_ID, 'timestamp': dates[r_idx], 'equipment_level': level,
                                'equipment_id': equip_id, 'signal': db_signal, 'value': float(v), 'source': 'pdcl_ingest'
                            })
                            file_count += 1
        print(f"    Added {file_count} points from {f}")
        if len(all_objs) > 100000:
            print(f"    Flushing {len(all_objs)} points to DB...")
            db.bulk_insert_mappings(RawDataGeneric, all_objs)
            db.commit()
            all_objs = []
    
    if all_objs:
        db.bulk_insert_mappings(RawDataGeneric, all_objs); db.commit()

def apply_fallbacks(db):
    print("--- Applying Fallback & Calculation Logic ---")
    existing_v = set()
    rows = db.execute(text("SELECT timestamp, equipment_id FROM raw_data_generic WHERE plant_id=:p AND equipment_level='scb' AND signal='dc_voltage'"), {"p": PLANT_ID}).all()
    for ts, scb_id in rows:
        existing_v.add((ts, scb_id))
    
    current_rows = db.execute(text("SELECT timestamp, equipment_id FROM raw_data_generic WHERE plant_id=:p AND equipment_level='scb' AND signal='dc_current'"), {"p": PLANT_ID}).all()
    
    new_v = []
    for ts, scb_id in current_rows:
        inv_id = "INV" + scb_id[3:5]
        if (ts, scb_id) not in existing_v:
            if ts in inv_voltages and inv_id in inv_voltages[ts]:
                new_v.append({
                    'plant_id': PLANT_ID, 'timestamp': ts, 'equipment_level': 'scb',
                    'equipment_id': scb_id, 'signal': 'dc_voltage', 'value': inv_voltages[ts][inv_id],
                    'source': 'pdcl_fallback'
                })
    
    if new_v:
        db.bulk_insert_mappings(RawDataGeneric, new_v)
        db.commit()
    print(f"  Fallback complete. Added {len(new_v)} voltages.")

def ingest_wms(db):
    print("--- Ingesting WMS ---")
    WMS_FILES = [f for f in os.listdir(DATA_DIR) if f.startswith("WMS data") and f.endswith(".xlsx")]
    for f in WMS_FILES:
        path = os.path.join(DATA_DIR, f)
        print(f"  Reading {f}...")
        headers = pd.read_excel(path, header=None, nrows=2).iloc[1].tolist()
        data_df = pd.read_excel(path, header=None, skiprows=3)
        mapping = {
            'AMBIENT TEMP (DegC)': 'ambient_temp',
            'GHI RADIATION (w/m^2)': 'ghi',
            'GII RADIATION (w/m^2)': 'gti',
            'MODULE TEMP1 (DegC)': 'module_temp',
            'WIND SPEED (M/s)': 'wind_speed'
        }
        
        data_df[0] = data_df[0].apply(lambda x: parse_ts(x, truncate_to_minute=True))
        for col_idx in range(1, data_df.shape[1]):
            data_df[col_idx] = pd.to_numeric(data_df[col_idx], errors='coerce')
        data_df_agg = data_df.groupby(0).mean().reset_index()

        objs = []
        for col_idx, h in enumerate(headers):
            h_str = str(h).strip()
            if h_str in mapping:
                db_signal = mapping[h_str]
                if col_idx not in data_df_agg.columns: continue
                col_data = data_df_agg[col_idx]
                dates = data_df_agg[0]
                for i, val in col_data.items():
                    if pd.notnull(val):
                        objs.append({
                            'plant_id': PLANT_ID, 'timestamp': dates[i], 'equipment_level': 'wms',
                            'equipment_id': 'WMS-01', 'signal': db_signal, 'value': float(val), 'source': 'pdcl_ingest'
                        })
        if objs:
            db.bulk_insert_mappings(RawDataGeneric, objs)
            db.commit()
            print(f"  Processed {f}: {len(objs)} points")

if __name__ == "__main__":
    db = get_db()
    try:
        # Clear existing PDCL data
        print("Cleaning old data...")
        db.execute(text("DELETE FROM raw_data_generic WHERE plant_id = :p"), {"p": PLANT_ID})
        db.commit()
        
        ingest_inverters(db)
        ingest_smbs(db)
        ingest_wms(db)
        apply_fallbacks(db) # This might be slow for millions of rows, I'll optimize if needed.
        
        print("\n--- Ingestion Complete ---")
    finally:
        db.close()
