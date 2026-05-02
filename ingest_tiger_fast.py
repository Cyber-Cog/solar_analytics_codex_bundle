"""
ingest_tiger_fast.py
====================
High-speed ingestion for Tiger plant using PostgreSQL COPY command.
~20-50x faster than row-by-row INSERT over WAN.

Strategy:
  1. Transform entire dataset in-memory with pandas (vectorised, fast)
  2. Stream to DB via psycopg2 copy_expert (COPY FROM STDIN CSV)
     - Single network round-trip for each table
  3. Run DS fault detection

Run from the `backend` directory:
    $env:DATABASE_URL='postgresql://...'
    $env:PYTHONIOENCODING='utf-8'
    python ../ingest_tiger_fast.py
"""

import sys, os, re, io, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "backend"))

import pandas as pd
import numpy as np

PLANT_ID  = "Tiger"
ARCH_FILE = os.path.join(os.path.dirname(__file__), "FINAL_ARCHITECTURE.xlsx")
RAW_FILE  = os.path.join(os.path.dirname(__file__), "FINAL_RAW_DATA.csv")

# ── helpers ────────────────────────────────────────────────────────────────────

def norm_inv(s: str) -> str:
    m = re.search(r'(\d+)', str(s).strip())
    return f"INV-{int(m.group(1)):02d}" if m else None

def norm_scb(inv: str, scb: str) -> str:
    m = re.search(r'(\d+)', str(scb).strip())
    return f"{inv}-SCB-{int(m.group(1)):02d}" if m else None

def norm_str(scb: str, sid: str) -> str:
    m = re.search(r'(\d+)', str(sid).strip())
    return f"{scb}-STR-{int(m.group(1)):02d}" if m else None

def get_conn():
    """Return a raw psycopg2 connection (bypasses SQLAlchemy for COPY speed)."""
    import psycopg2
    db_url = os.environ["DATABASE_URL"]
    # parse  postgresql://user:pass@host:port/dbname?sslmode=require
    import re as re2
    m = re2.match(
        r'postgresql://([^:]+):([^@]+)@([^:/]+):?(\d*)/(.*?)(\?.*)?$',
        db_url.strip()
    )
    if not m:
        raise ValueError(f"Cannot parse DATABASE_URL: {db_url}")
    user, password, host, port, dbname, qs = m.groups()
    port = int(port) if port else 5432
    sslmode = "require" if "sslmode=require" in (qs or "") else "prefer"
    return psycopg2.connect(
        host=host, port=port, dbname=dbname.split("?")[0],
        user=user, password=password, sslmode=sslmode,
        connect_timeout=30,
        options="-c statement_timeout=0",          # no query timeout
        keepalives=1, keepalives_idle=60,           # keep WAN connection alive
    )

def copy_df_to_table(conn, df: pd.DataFrame, table: str, columns: list):
    """Stream DataFrame to Postgres via COPY FROM STDIN (fastest possible method)."""
    buf = io.StringIO()
    df[columns].to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    with conn.cursor() as cur:
        col_list = ", ".join(columns)
        cur.copy_expert(
            f"COPY {table} ({col_list}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
            buf,
        )
    conn.commit()

def progress(done, total, label="", start_t=None):
    pct = int(done / total * 100) if total else 0
    bar_len = 35
    filled = int(bar_len * done / total) if total else 0
    bar = "#" * filled + "-" * (bar_len - filled)
    elapsed = time.time() - start_t if start_t else 0
    eta = (elapsed / done * (total - done)) if done > 0 else 0
    print(f"\r  [{bar}] {pct:3d}%  {done:,}/{total:,}  ETA {eta:.0f}s  {label}   ", end="", flush=True)


# ── Step 1: Architecture ───────────────────────────────────────────────────────

def transform_architecture() -> pd.DataFrame:
    print("[1/3] Transforming architecture...")
    df = pd.read_excel(ARCH_FILE, dtype=str)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    df["plant_id"]    = PLANT_ID
    df["inverter_id"] = df["inverter_id"].apply(norm_inv)
    df["scb_id"]      = df.apply(lambda r: norm_scb(r["inverter_id"], r["scb_id"]), axis=1)
    df["string_id"]   = df.apply(lambda r: norm_str(r["scb_id"], r["string_id"]), axis=1)
    df["spare_flag"]  = df["spare_flag"].str.strip().str.upper().isin(["Y","YES","TRUE","1","X"])
    df["modules_per_string"] = pd.to_numeric(df["modules_per_string"], errors="coerce").astype("Int64")
    df["strings_per_scb"]    = pd.to_numeric(df["strings_per_scb"],    errors="coerce").astype("Int64")
    df["scbs_per_inverter"]  = pd.to_numeric(df["scbs_per_inverter"],  errors="coerce").astype("Int64")
    df["dc_capacity_kw"]     = pd.to_numeric(df["dc_capacity_kw"],     errors="coerce")

    df.dropna(subset=["inverter_id","scb_id","string_id"], inplace=True)
    print(f"  -> {len(df)} rows ready")
    print(f"     Inverters: {sorted(df['inverter_id'].unique())[:5]}...")
    print(f"     SCBs sample: {sorted(df['scb_id'].unique())[:4]}...")
    return df


def ingest_architecture(conn, df: pd.DataFrame):
    cur = conn.cursor()
    # Delete existing Tiger architecture (safe re-run)
    cur.execute("DELETE FROM plant_architecture WHERE plant_id = %s", (PLANT_ID,))
    conn.commit()

    cols = ["plant_id","inverter_id","scb_id","string_id",
            "modules_per_string","strings_per_scb","scbs_per_inverter",
            "dc_capacity_kw","spare_flag"]

    # Build INSERT via copy
    buf = io.StringIO()
    df[cols].to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    col_str = ", ".join(cols)
    cur.copy_expert(
        f"COPY plant_architecture ({col_str}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
        buf,
    )
    conn.commit()
    print(f"  [OK] Architecture: {len(df)} rows ingested via COPY")


# ── Step 2: Raw Data ───────────────────────────────────────────────────────────

INVERTER_SIG_MAP = {
    "total ac active power (kwac) kilowatts": "ac_power",
    "dc power (total) (kwdc) kilowatts":      "dc_power",
}

def transform_raw_data() -> pd.DataFrame:
    print("\n[2/3] Loading + transforming 1.2M rows (vectorised)...")
    t0 = time.time()

    raw = pd.read_csv(RAW_FILE)
    print(f"  Loaded {len(raw):,} rows in {time.time()-t0:.1f}s")

    # Drop nulls
    before = len(raw)
    raw.dropna(subset=["value"], inplace=True)
    print(f"  Dropped {before-len(raw):,} null-value rows. Remaining: {len(raw):,}")

    raw["timestamp"] = pd.to_datetime(raw["timestamp"], errors="coerce")
    raw.dropna(subset=["timestamp"], inplace=True)

    raw["equipment_level"] = raw["equipment_level"].str.strip().str.lower()
    raw["equipment_id"]    = raw["equipment_id"].str.strip()
    raw["signal_raw"]      = raw["signal"].str.strip()
    raw["signal_lower"]    = raw["signal_raw"].str.lower()

    # ── Split into groups ──────────────────────────────────────────────────────

    out_frames = []

    # 1. Weather rows → plant level
    wx_mask = raw["equipment_id"].str.contains("weather|solcast", case=False, na=False)
    wx = raw[wx_mask].copy()
    if len(wx):
        irr = wx[wx["signal_lower"].str.contains("irradiance|sun", na=False)].copy()
        irr["plant_id"]        = PLANT_ID
        irr["equipment_level"] = "plant"
        irr["equipment_id"]    = PLANT_ID
        irr["signal"]          = "irradiance"
        out_frames.append(irr[["plant_id","timestamp","equipment_level","equipment_id","signal","value"]])

        amb = wx[wx["signal_lower"].str.contains("temperature|ambient", na=False)].copy()
        amb["plant_id"]        = PLANT_ID
        amb["equipment_level"] = "plant"
        amb["equipment_id"]    = PLANT_ID
        amb["signal"]          = "ambient_temp"
        amb["value"]           = (amb["value"] - 32) * 5 / 9  # F -> C
        out_frames.append(amb[["plant_id","timestamp","equipment_level","equipment_id","signal","value"]])

    # All non-weather rows
    inv_rows = raw[~wx_mask].copy()

    # Normalise inverter IDs: "Inv 1" -> "INV-01"
    # Vectorised: extract number, zero-pad
    inv_rows["inv_num"] = inv_rows["equipment_id"].str.extract(r'(\d+)').astype(float).astype("Int64")
    inv_rows["inv_norm"] = "INV-" + inv_rows["inv_num"].astype(str).str.zfill(2)

    # 2. DC Current Input N -> SCB rows, signal=dc_current
    cur_mask = inv_rows["signal_raw"].str.contains(r"DC Current Input\s+\d+", case=False, na=False, regex=True)
    cur_df = inv_rows[cur_mask].copy()
    if len(cur_df):
        cur_df["scb_num"] = cur_df["signal_raw"].str.extract(r"Input\s+(\d+)", expand=False).astype(float).astype("Int64")
        cur_df["plant_id"]        = PLANT_ID
        cur_df["equipment_level"] = "scb"
        cur_df["equipment_id"]    = cur_df["inv_norm"] + "-SCB-" + cur_df["scb_num"].astype(str).str.zfill(2)
        cur_df["signal"]          = "dc_current"
        out_frames.append(cur_df[["plant_id","timestamp","equipment_level","equipment_id","signal","value"]])
        print(f"  SCB dc_current rows: {len(cur_df):,}")

    # 3. DC Voltage Input N -> SCB rows, signal=dc_voltage
    volt_mask = inv_rows["signal_raw"].str.contains(r"DC Voltage Input\s+\d+", case=False, na=False, regex=True)
    volt_df = inv_rows[volt_mask].copy()
    if len(volt_df):
        volt_df["scb_num"] = volt_df["signal_raw"].str.extract(r"Input\s+(\d+)", expand=False).astype(float).astype("Int64")
        volt_df["plant_id"]        = PLANT_ID
        volt_df["equipment_level"] = "scb"
        volt_df["equipment_id"]    = volt_df["inv_norm"] + "-SCB-" + volt_df["scb_num"].astype(str).str.zfill(2)
        volt_df["signal"]          = "dc_voltage"
        out_frames.append(volt_df[["plant_id","timestamp","equipment_level","equipment_id","signal","value"]])
        print(f"  SCB dc_voltage rows: {len(volt_df):,}")

    # 4. Inverter-level signals
    other_mask = ~cur_mask & ~volt_mask
    other_df = inv_rows[other_mask].copy()
    if len(other_df):
        other_df["plant_id"] = PLANT_ID
        other_df["equipment_level"] = "inverter"
        other_df["equipment_id"]    = other_df["inv_norm"]
        # Map signal names to canonical names
        other_df["signal"] = other_df["signal_lower"].map(INVERTER_SIG_MAP).fillna(other_df["signal_lower"].str[:100])
        out_frames.append(other_df[["plant_id","timestamp","equipment_level","equipment_id","signal","value"]])
        print(f"  Inverter-level rows: {len(other_df):,}")

    result = pd.concat(out_frames, ignore_index=True)
    result["timestamp"] = result["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    result["plant_id"]  = PLANT_ID

    print(f"  -> Total transformed rows: {len(result):,}  (in {time.time()-t0:.1f}s total)")
    return result


def ingest_raw_data(conn, df: pd.DataFrame):
    cur = conn.cursor()

    print(f"  Clearing existing Tiger raw data...")
    cur.execute("DELETE FROM raw_data_generic WHERE plant_id = %s", (PLANT_ID,))
    conn.commit()
    deleted = cur.rowcount
    print(f"  Cleared {deleted:,} old rows.")

    total = len(df)
    print(f"  Streaming {total:,} rows via COPY (single round-trip)...")
    t0 = time.time()

    cols = ["plant_id","timestamp","equipment_level","equipment_id","signal","value"]
    buf = io.StringIO()
    df[cols].to_csv(buf, index=False, header=False, na_rep="\\N")
    buf_size_mb = buf.tell() / 1024 / 1024
    buf.seek(0)

    print(f"  CSV buffer size: {buf_size_mb:.1f} MB — uploading...")

    cur.copy_expert(
        "COPY raw_data_generic (plant_id, timestamp, equipment_level, equipment_id, signal, value) "
        "FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
        buf,
    )
    conn.commit()
    elapsed = time.time() - t0
    print(f"  [OK] Raw data: {total:,} rows ingested via COPY in {elapsed:.1f}s  ({total/elapsed:,.0f} rows/sec)")


# ── Step 3: DS fault detection ─────────────────────────────────────────────────

def run_fault_detection(conn):
    print("\n[3/3] Running DS fault detection...")
    try:
        from database import SessionLocal
        from engine.ds_detection import run_ds_detection
        from sqlalchemy import text as sa_text

        db = SessionLocal()
        try:
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
                # Debug
                arch_scbs = db.execute(sa_text("SELECT DISTINCT scb_id FROM plant_architecture WHERE plant_id=:p LIMIT 5"), {"p": PLANT_ID}).fetchall()
                raw_scbs  = db.execute(sa_text("SELECT DISTINCT equipment_id FROM raw_data_generic WHERE plant_id=:p AND equipment_level='scb' LIMIT 5"), {"p": PLANT_ID}).fetchall()
                print("  WARNING: No SCB rows matched architecture!")
                print("  Architecture SCB sample:", [r[0] for r in arch_scbs])
                print("  Raw data SCB sample:    ", [r[0] for r in raw_scbs])
                return

            import pandas as pd
            df = pd.DataFrame(rows, columns=["timestamp","inverter_id","scb_id","scb_current"])
            df["timestamp"]   = pd.to_datetime(df["timestamp"])
            df["scb_current"] = pd.to_numeric(df["scb_current"], errors="coerce")
            print(f"  Found {len(df):,} SCB rows for DS detection. Running...")
            run_ds_detection(PLANT_ID, df, db)
            print("  [OK] DS fault detection complete.")
        finally:
            db.close()

    except Exception as e:
        print(f"  [WARN] DS detection failed: {e}")
        import traceback; traceback.print_exc()
        print("  You can re-trigger fault detection from the UI: Metadata -> Raw Data -> Compute Faults Now")


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import os
    if "DATABASE_URL" not in os.environ:
        # Try loading from backend/.env
        env_path = os.path.join(os.path.dirname(__file__), "backend", ".env")
        if os.path.exists(env_path):
            for line in open(env_path):
                line = line.strip()
                if line.startswith("DATABASE_URL="):
                    os.environ["DATABASE_URL"] = line[len("DATABASE_URL="):].strip()
                    break

    print("=" * 55)
    print(f"  Tiger Plant Fast Ingestion")
    print(f"  Architecture: {os.path.basename(ARCH_FILE)}")
    print(f"  Raw Data:     {os.path.basename(RAW_FILE)}")
    print("=" * 55)

    t_total = time.time()

    # Transform
    arch_df = transform_architecture()
    raw_df  = transform_raw_data()

    # Connect once
    print("\n  Connecting to database...")
    conn = get_conn()
    conn.autocommit = False
    print("  Connected.")

    try:
        ingest_architecture(conn, arch_df)
        ingest_raw_data(conn, raw_df)
        run_fault_detection(conn)
    finally:
        conn.close()

    print(f"\n{'='*55}")
    print(f"  DONE in {(time.time()-t_total)/60:.1f} minutes")
    print(f"  Architecture: {len(arch_df)} rows")
    print(f"  Raw data:     {len(raw_df):,} rows")
    print(f"  Open UI -> select plant 'Tiger' -> Fault Diagnostics")
    print("=" * 55)
