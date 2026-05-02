"""
ingest_tiger.py
===============
One-shot ingestion script for the Tiger plant.

Transformations applied:
  Architecture:
    - inverter_id:  "I 1"  → "INV-01"      (zero-padded, no spaces)
    - scb_id:       "SCB1" → "INV-01-SCB-01"  (composite, zero-padded)
    - string_id:    "1"    → "INV-01-SCB-01-STR-01"
    - spare_flag:   "N"    → False

  Raw Data:
    - equipment_level: "Inverter" → "inverter", weather → "plant"
    - equipment_id (inverter): "Inv 1" → "INV-01"
    - DC Current Input N → equipment_level=scb, equipment_id="INV-01-SCB-0N", signal=dc_current
    - DC Voltage Input N  → equipment_level=scb, equipment_id="INV-01-SCB-0N", signal=dc_voltage
    - Total AC Active Power → signal=ac_power  (inverter level)
    - DC Power (total)     → signal=dc_power   (inverter level)
    - Irradiance           → signal=irradiance, equipment_level=plant, equipment_id=Tiger
    - Ambient Temperature  → convert °F→°C, signal=ambient_temp, equipment_level=plant
    - Null values dropped (155k rows)

Run:
    cd backend
    python ../ingest_tiger.py
"""

import sys, os, re, math
sys.path.insert(0, os.path.dirname(__file__) + "/backend")

import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import text as sa_text
from database import SessionLocal
from models import PlantArchitecture, RawDataGeneric

PLANT_ID = "Tiger"

ARCH_FILE = os.path.join(os.path.dirname(__file__), "FINAL_ARCHITECTURE.xlsx")
RAW_FILE  = os.path.join(os.path.dirname(__file__), "FINAL_RAW_DATA.csv")


# ── ID normalisation helpers ───────────────────────────────────────────────────

def norm_inv(raw_inv: str) -> str:
    """'I 1' / 'Inv 1' / 'I1' → 'INV-01'"""
    s = str(raw_inv).strip()
    m = re.search(r'(\d+)', s)
    if not m:
        raise ValueError(f"Cannot parse inverter number from: {raw_inv!r}")
    return f"INV-{int(m.group(1)):02d}"

def norm_scb(inv_norm: str, raw_scb: str) -> str:
    """'INV-01', 'SCB1' / 'SCB-1' → 'INV-01-SCB-01'"""
    m = re.search(r'(\d+)', str(raw_scb).strip())
    if not m:
        raise ValueError(f"Cannot parse SCB number from: {raw_scb!r}")
    return f"{inv_norm}-SCB-{int(m.group(1)):02d}"

def norm_str(scb_norm: str, raw_str: str) -> str:
    """'INV-01-SCB-01', '1' → 'INV-01-SCB-01-STR-01'"""
    m = re.search(r'(\d+)', str(raw_str).strip())
    if not m:
        raise ValueError(f"Cannot parse string number from: {raw_str!r}")
    return f"{scb_norm}-STR-{int(m.group(1)):02d}"

def scb_num_from_signal(signal_name: str):
    """'DC Current Input 3 (Idc3) Amps' → 3"""
    m = re.search(r'Input\s+(\d+)', signal_name, re.IGNORECASE)
    return int(m.group(1)) if m else None


# ── Step 1: Transform & ingest architecture ───────────────────────────────────

def ingest_architecture(db: Session):
    print("\n[1/3] Loading architecture...")
    df = pd.read_excel(ARCH_FILE, dtype=str)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    records = []
    errors = []
    for _, row in df.iterrows():
        try:
            inv_norm = norm_inv(row["inverter_id"])
            scb_norm = norm_scb(inv_norm, row["scb_id"])
            str_norm = norm_str(scb_norm, row["string_id"])
            spare = str(row.get("spare_flag", "N")).strip().upper() in ("Y", "YES", "TRUE", "1", "X")
            records.append({
                "plant_id":           PLANT_ID,
                "inverter_id":        inv_norm,
                "scb_id":             scb_norm,
                "string_id":          str_norm,
                "modules_per_string": int(float(row["modules_per_string"])) if row.get("modules_per_string") else None,
                "strings_per_scb":    int(float(row["strings_per_scb"])) if row.get("strings_per_scb") else None,
                "scbs_per_inverter":  int(float(row["scbs_per_inverter"])) if row.get("scbs_per_inverter") else None,
                "dc_capacity_kw":     float(row["dc_capacity_kw"]) if row.get("dc_capacity_kw") else None,
                "spare_flag":         spare,
            })
        except Exception as e:
            errors.append(f"Row {_}: {e}")

    if records:
        db.execute(sa_text("""
            INSERT INTO plant_architecture
                (plant_id, inverter_id, scb_id, string_id,
                 modules_per_string, strings_per_scb, scbs_per_inverter,
                 dc_capacity_kw, spare_flag)
            VALUES
                (:plant_id, :inverter_id, :scb_id, :string_id,
                 :modules_per_string, :strings_per_scb, :scbs_per_inverter,
                 :dc_capacity_kw, :spare_flag)
            ON CONFLICT (plant_id, inverter_id, scb_id, string_id)
            DO UPDATE SET
                modules_per_string = EXCLUDED.modules_per_string,
                strings_per_scb    = EXCLUDED.strings_per_scb,
                scbs_per_inverter  = EXCLUDED.scbs_per_inverter,
                dc_capacity_kw     = EXCLUDED.dc_capacity_kw,
                spare_flag         = EXCLUDED.spare_flag
        """), records)
        db.commit()

    print(f"  [OK] Architecture: {len(records)} rows ingested, {len(errors)} errors")
    if errors:
        for e in errors[:5]:
            print("   ", e)
    return len(records)


# ── Step 2: Transform & ingest raw data ───────────────────────────────────────

# Signal name → (canonical_signal, None means use as-is for inverter/SCB level)
INVERTER_SIGNAL_MAP = {
    "Total AC Active Power (KwAC) Kilowatts": "ac_power",
    "DC Power (total) (KwDC) Kilowatts":      "dc_power",
}

FAHRENHEIT_SIGNALS = {
    "Ambient Temperature (Ambient) Degrees Fahrenheit": "ambient_temp",
}

def ingest_raw_data(db: Session):
    print("\n[2/3] Loading raw data CSV (1.2M rows)…")
    raw = pd.read_csv(RAW_FILE)

    # Drop rows with null values
    before = len(raw)
    raw.dropna(subset=["value"], inplace=True)
    print(f"  Dropped {before - len(raw)} null-value rows. Remaining: {len(raw)}")

    # Parse timestamps
    raw["timestamp"] = pd.to_datetime(raw["timestamp"], errors="coerce")
    fail_ts = raw["timestamp"].isna().sum()
    if fail_ts:
        print(f"  WARNING: {fail_ts} rows have unparseable timestamps — dropping them")
        raw.dropna(subset=["timestamp"], inplace=True)

    # Normalise equipment_level
    raw["equipment_level"] = raw["equipment_level"].str.strip().str.lower()

    out_rows = []  # list of dicts for bulk insert

    for _, row in raw.iterrows():
        eq_id  = str(row["equipment_id"]).strip()
        sig    = str(row["signal"]).strip()
        val    = float(row["value"])
        ts     = row["timestamp"]
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
        eq_lv  = str(row["equipment_level"]).strip().lower()

        # ── Weather station rows ──────────────────────────────────────────
        if "weather" in eq_id.lower() or "solcast" in eq_id.lower():
            if "irradiance" in sig.lower() or "sun" in sig.lower():
                out_rows.append({
                    "plant_id": PLANT_ID, "timestamp": ts_str,
                    "equipment_level": "plant", "equipment_id": PLANT_ID,
                    "signal": "irradiance", "value": val,
                })
            elif "temperature" in sig.lower() or "ambient" in sig.lower():
                # Convert °F → °C
                val_c = (val - 32) * 5 / 9
                out_rows.append({
                    "plant_id": PLANT_ID, "timestamp": ts_str,
                    "equipment_level": "plant", "equipment_id": PLANT_ID,
                    "signal": "ambient_temp", "value": round(val_c, 2),
                })
            continue

        # ── Inverter rows ─────────────────────────────────────────────────
        try:
            inv_norm = norm_inv(eq_id)
        except Exception:
            # Skip unknowable IDs
            continue

        # DC Current Input N → SCB level
        if re.search(r'DC Current Input\s+(\d+)', sig, re.IGNORECASE):
            scb_num = scb_num_from_signal(sig)
            if scb_num is None:
                continue
            scb_norm = f"{inv_norm}-SCB-{scb_num:02d}"
            out_rows.append({
                "plant_id": PLANT_ID, "timestamp": ts_str,
                "equipment_level": "scb", "equipment_id": scb_norm,
                "signal": "dc_current", "value": val,
            })
            continue

        # DC Voltage Input N → SCB level
        if re.search(r'DC Voltage Input\s+(\d+)', sig, re.IGNORECASE):
            scb_num = scb_num_from_signal(sig)
            if scb_num is None:
                continue
            scb_norm = f"{inv_norm}-SCB-{scb_num:02d}"
            out_rows.append({
                "plant_id": PLANT_ID, "timestamp": ts_str,
                "equipment_level": "scb", "equipment_id": scb_norm,
                "signal": "dc_voltage", "value": val,
            })
            continue

        # AC power → inverter level
        if sig in INVERTER_SIGNAL_MAP:
            out_rows.append({
                "plant_id": PLANT_ID, "timestamp": ts_str,
                "equipment_level": "inverter", "equipment_id": inv_norm,
                "signal": INVERTER_SIGNAL_MAP[sig], "value": val,
            })
            continue

        # Fahrenheit temps → inverter-level ambient
        if sig in FAHRENHEIT_SIGNALS:
            val_c = (val - 32) * 5 / 9
            out_rows.append({
                "plant_id": PLANT_ID, "timestamp": ts_str,
                "equipment_level": "inverter", "equipment_id": inv_norm,
                "signal": FAHRENHEIT_SIGNALS[sig], "value": round(val_c, 2),
            })
            continue

        # All other inverter signals → store with lowercased signal name
        out_rows.append({
            "plant_id": PLANT_ID, "timestamp": ts_str,
            "equipment_level": "inverter", "equipment_id": inv_norm,
            "signal": sig.lower()[:100], "value": val,
        })

    print(f"  Transformed {len(raw)} CSV rows → {len(out_rows)} DB rows")

    # Clear existing Tiger raw data first to avoid duplicates on re-run
    print("  Clearing existing raw data for Tiger plant...")
    db.execute(sa_text("DELETE FROM raw_data_generic WHERE plant_id = :p"), {"p": PLANT_ID})
    db.commit()
    print("  Cleared. Inserting new rows...")

    # Bulk insert in batches of 5000
    BATCH = 5000
    total = len(out_rows)
    inserted = 0
    for i in range(0, total, BATCH):
        chunk = out_rows[i:i+BATCH]
        db.execute(
            sa_text("""
                INSERT INTO raw_data_generic
                    (plant_id, timestamp, equipment_level, equipment_id, signal, value)
                VALUES
                    (:plant_id, :timestamp, :equipment_level, :equipment_id, :signal, :value)
            """),
            chunk,
        )
        db.commit()
        inserted += len(chunk)
        pct = int(inserted / total * 100)
        print(f"  [{pct:3d}%] {inserted:,}/{total:,} rows committed...", end="\r")

    print(f"  [OK] Raw data: {inserted:,} rows ingested")
    return inserted



# ── Step 3: Run DS fault detection ────────────────────────────────────────────

def run_fault_detection(db: Session):
    print("\n[3/3] Running DS fault detection…")
    try:
        from engine.ds_detection import run_ds_detection

        # Fetch SCB dc_current data from DB for this plant
        rows = db.execute(sa_text("""
            SELECT r.timestamp, pa.inverter_id, r.equipment_id AS scb_id, r.value AS scb_current
            FROM raw_data_generic r
            JOIN plant_architecture pa
              ON pa.plant_id = r.plant_id AND pa.scb_id = r.equipment_id
            WHERE r.plant_id = :p
              AND r.equipment_level = 'scb'
              AND r.signal = 'dc_current'
            LIMIT 2000000
        """), {"p": PLANT_ID}).fetchall()

        if not rows:
            print("  ⚠ No SCB dc_current data found after join with architecture.")
            print("    This means SCB IDs in raw_data do not match architecture scb_ids.")
            print("    Check: architecture scb_ids vs raw_data equipment_ids")
            # Debug: show what's in each table
            arch_scbs = db.execute(sa_text("SELECT DISTINCT scb_id FROM plant_architecture WHERE plant_id=:p LIMIT 10"), {"p": PLANT_ID}).fetchall()
            raw_scbs  = db.execute(sa_text("SELECT DISTINCT equipment_id FROM raw_data_generic WHERE plant_id=:p AND equipment_level='scb' LIMIT 10"), {"p": PLANT_ID}).fetchall()
            print("    Architecture SCB IDs:", [r[0] for r in arch_scbs])
            print("    Raw data SCB IDs:    ", [r[0] for r in raw_scbs])
            return

        import pandas as pd
        df = pd.DataFrame(rows, columns=["timestamp","inverter_id","scb_id","scb_current"])
        df["timestamp"]   = pd.to_datetime(df["timestamp"])
        df["scb_current"] = pd.to_numeric(df["scb_current"], errors="coerce")
        print(f"  Found {len(df):,} SCB rows for DS detection")

        run_ds_detection(PLANT_ID, df, db)
        print("  [OK] DS fault detection complete. Check Fault Diagnostics in the UI.")

    except Exception as e:
        print(f"  ✗ DS detection failed: {e}")
        import traceback; traceback.print_exc()


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"=== Tiger Plant Ingestion ===")
    print(f"Plant ID: {PLANT_ID}")
    print(f"Architecture: {ARCH_FILE}")
    print(f"Raw Data:     {RAW_FILE}")

    db: Session = SessionLocal()
    try:
        arch_rows = ingest_architecture(db)
        raw_rows  = ingest_raw_data(db)
        run_fault_detection(db)
        print(f"[DONE] Architecture: {arch_rows} rows | Raw: {raw_rows:,} rows")
        print("  Open the UI -> select plant 'Tiger' -> Fault Diagnostics")
    finally:
        db.close()
