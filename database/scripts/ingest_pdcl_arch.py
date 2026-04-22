import os
import sys
import pandas as pd

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
from sqlalchemy import text

DATA_DIR = r"C:\Users\Asus\Downloads\PDCL"
ARCH_FILE = os.path.join(DATA_DIR, "Plant Architecture.xlsx")
PLANT_ID = "PDCL"

def ingest_arch():
    df = pd.read_excel(ARCH_FILE)
    print(f"Read {len(df)} rows from {ARCH_FILE}")
    
    db = SessionLocal()
    try:
        # Clear existing PDCL architecture
        db.execute(text("DELETE FROM plant_architecture WHERE plant_id = :p"), {"p": PLANT_ID})
        
        for _, row in df.iterrows():
            # Columns: plant_id, inverter_id, scb_id, strings_per_scb, modules_per_string, Module Wp, dc_capacity_kw, scbs_per_inverter, string_id
            try:
                # inv_id: 1.0 -> 01 -> INV01
                inv_raw = int(float(row['inverter_id']))
                inv_num = str(inv_raw).zfill(2)
                inv_id = f"INV{inv_num}"
                
                # scb_id: 1.1 -> parts [1, 1] -> 01-01 -> SCB01-01
                # Format is usually Inverter.SCB
                scb_raw = str(row['scb_id'])
                if "." in scb_raw:
                    scb_parts = scb_raw.split(".")
                    # Handle cases like 1.1, 1.2 ... 1.10
                    # if scb_raw is "1.1", parts are ["1", "1"]
                    # if scb_raw is "1.1", it might be intended as SCB 01 or SCB 10?
                    # Looking at the head check in Step 597: 1.1, 1.2, 1.3, 1.4, 1.5.
                    # These are likely sequential SCB 01, 02, etc.
                    scb_val = scb_parts[1].zfill(2)
                else:
                    scb_val = scb_raw.zfill(2)
                
                scb_id = f"SCB{inv_num}-{scb_val}"
                
                strings_count = int(row['strings_per_scb'])
                mods = int(row['modules_per_string'])
                # dc_capacity_kw in file is for the WHOLE SCB?
                # row: strings_per_scb=24, dc_capacity_kw=369.6
                # (24 strings * 28 modules/string * 550 W/module) / 1000 = 369.6. Correct.
                # Capacity per string = 369.6 / 24 = 15.4 kW.
                cap_per_string = float(row['dc_capacity_kw']) / strings_count
                
                for s_idx in range(1, strings_count + 1):
                    str_num = str(s_idx).zfill(2)
                    string_id = f"str{inv_num}-{scb_val}-{str_num}"
                    
                    db.execute(text("""
                        INSERT INTO plant_architecture 
                        (plant_id, inverter_id, scb_id, string_id, modules_per_string, dc_capacity_kw, strings_per_scb, scbs_per_inverter)
                        VALUES (:p, :inv, :scb, :s, :m, :dc, :sps, :spi)
                    """), {
                        "p": PLANT_ID,
                        "inv": inv_id,
                        "scb": scb_id,
                        "s": string_id,
                        "m": mods,
                        "dc": cap_per_string,
                        "sps": strings_count,
                        "spi": int(row['scbs_per_inverter'])
                    })
            except Exception as row_e:
                print(f"Skipping row due to error: {row_e}")
                continue
        
        db.commit()
        print("Architecture ingestion complete.")
    except Exception as e:
        db.rollback()
        print(f"Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    ingest_arch()
