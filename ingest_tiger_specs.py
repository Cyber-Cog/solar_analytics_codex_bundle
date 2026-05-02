"""
Ingest Tiger SMA Inverter + Hanwha Module specs into equipment_specs table.
Run from project root:
  python ingest_tiger_specs.py
"""
import os, sys
sys.path.insert(0, "backend")

os.environ.setdefault(
    "DATABASE_URL",
    ""  # Set DATABASE_URL in backend/.env or export it — never hard-code credentials
)

if not os.environ.get("DATABASE_URL"):
    # Fall back to backend/.env
    _env_path = os.path.join("backend", ".env")
    if os.path.isfile(_env_path):
        for _line in open(_env_path, encoding="utf-8"):
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _, _v = _line.partition("=")
                if _k.strip() not in os.environ:
                    os.environ[_k.strip()] = _v.strip()

if not os.environ.get("DATABASE_URL"):
    raise SystemExit("ERROR: DATABASE_URL is not set. Add it to backend/.env")

from database import engine
from sqlalchemy import text

PLANT_ID = "Tiger"

# ── Columns that actually exist in equipment_specs ──────────────────────────
# ['id', 'plant_id', 'equipment_id', 'equipment_type', 'manufacturer', 'model',
#  'rated_power', 'imp', 'vmp', 'isc', 'voc', 'target_efficiency',
#  'ac_capacity_kw', 'dc_capacity_kwp', 'rated_efficiency',
#  'mppt_voltage_min', 'mppt_voltage_max', 'voltage_limit', 'current_set_point',
#  'spec_sheet_path', 'impp', 'vmpp', 'pmax', 'degradation_year1_pct',
#  'degradation_year2_pct', 'degradation_annual_pct', 'module_efficiency_pct',
#  'alpha_stc', 'beta_stc', 'gamma_stc', 'alpha_noct', 'beta_noct', 'gamma_noct',
#  'degradation_loss_pct', 'temp_coefficient_per_deg']

def build_inverter_rows():
    """31 SMA Sunny TriPower Core1 50-US inverters."""
    rows = []
    for i in range(1, 32):
        inv_id = f"INV-{i:02d}"
        rows.append({
            "plant_id": PLANT_ID,
            "equipment_id": inv_id,
            "equipment_type": "inverter",
            "manufacturer": "SMA Solar Technology AG",
            "model": "SMA STPCORE1 50-US",
            # AC capacity
            "ac_capacity_kw": 50.0,           # Rated AC Output = 50,000 W
            # DC capacity (max DC input)
            "dc_capacity_kwp": 62.5,           # Max DC Input Power = 62,500 W
            # Rated efficiency — SMA STPCORE1 CEC weighted ~98.5%
            "rated_efficiency": 98.5,
            # MPPT voltage window
            "mppt_voltage_min": 500.0,         # MPP Voltage Range Min
            "mppt_voltage_max": 800.0,         # MPP Voltage Range Max
            # Voltage limit (max DC input voltage)
            "voltage_limit": 1000.0,
            # Current set point (max input current per MPPT)
            "current_set_point": 75.0,
            # Inverters do not degrade like modules; all degradation cols = 0
            "degradation_year1_pct": 0.0,
            "degradation_year2_pct": 0.0,
            "degradation_annual_pct": 0.0,
            "degradation_loss_pct": 0.0,
            "temp_coefficient_per_deg": 0.0,
            # No module-level electrical params for inverters
            "imp": None, "vmp": None, "isc": None, "voc": None,
            "impp": None, "vmpp": None, "pmax": None,
            "rated_power": 50000.0,            # W — same as AC rating
        })
    return rows


def build_module_row():
    """Hanwha Q CELLS Q.PEAK DUO L-G5.3 395W — one row for the plant module type."""
    return {
        "plant_id": PLANT_ID,
        "equipment_id": "MODULE-TIGER",
        "equipment_type": "module",
        "manufacturer": "Hanwha Q CELLS",
        "model": "Q.PEAK DUO L-G5.3 395W",
        # STC electrical parameters
        "rated_power": 395.0,              # Pmax at STC (W)
        "pmax": 395.0,
        "isc": 10.19,                      # Short-circuit current (A)
        "voc": 48.74,                      # Open-circuit voltage (V)
        "imp": 9.7,                        # Max power current Imp (A)
        "impp": 9.7,
        "vmp": 40.71,                      # Max power voltage Vmp (V)
        "vmpp": 40.71,
        # Temperature coefficients (STC)
        # beta_stc = Voc temperature coefficient = -0.27 %/°C (from sheet)
        "beta_stc": -0.27,
        # gamma_stc = Pmax temperature coefficient — typical for Q CELLS G5.3 = -0.34 %/°C
        "gamma_stc": 0.34,                 # algorithms check >0.05 → treat as %/°C
        # alpha_stc = Isc temperature coefficient — typical for silicon = +0.05 %/°C
        "alpha_stc": 0.05,
        # temp_coefficient_per_deg stored as fraction for loss_analysis engine
        "temp_coefficient_per_deg": 0.0034,  # 0.34%/°C → fraction
        # DC capacity of ONE module
        "dc_capacity_kwp": 0.395,          # 395 W = 0.395 kWp
        # Degradation: Q CELLS warranty = 2% year-1, 0.45%/yr thereafter
        "degradation_year1_pct": 2.0,
        "degradation_year2_pct": 0.45,
        "degradation_annual_pct": 0.45,
        "degradation_loss_pct": 0.5,       # used by loss_analysis engine
        # Module efficiency ≈ Pmax / (area) — 395 W / (2.015 m² for 79.33×39.37 in) ≈ 19.6%
        "module_efficiency_pct": 19.6,
        # No inverter-level fields
        "ac_capacity_kw": None,
        "rated_efficiency": None,
        "mppt_voltage_min": None,
        "mppt_voltage_max": None,
        "voltage_limit": None,
        "current_set_point": None,
    }


def ingest():
    inv_rows = build_inverter_rows()
    mod_row = build_module_row()
    all_rows = inv_rows + [mod_row]

    COLS = [
        "plant_id", "equipment_id", "equipment_type", "manufacturer", "model",
        "rated_power", "imp", "vmp", "isc", "voc",
        "ac_capacity_kw", "dc_capacity_kwp", "rated_efficiency",
        "mppt_voltage_min", "mppt_voltage_max", "voltage_limit", "current_set_point",
        "impp", "vmpp", "pmax",
        "degradation_year1_pct", "degradation_year2_pct", "degradation_annual_pct",
        "module_efficiency_pct", "alpha_stc", "beta_stc", "gamma_stc",
        "degradation_loss_pct", "temp_coefficient_per_deg",
    ]

    with engine.begin() as conn:
        # Delete existing Tiger specs to start clean
        deleted = conn.execute(
            text("DELETE FROM equipment_specs WHERE plant_id=:p"),
            {"p": PLANT_ID}
        ).rowcount
        print(f"Deleted {deleted} existing Tiger rows.")

        inserted = 0
        for row in all_rows:
            filtered = {c: row.get(c) for c in COLS}
            col_list = ", ".join(COLS)
            placeholders = ", ".join(f":{c}" for c in COLS)
            sql = text(f"INSERT INTO equipment_specs ({col_list}) VALUES ({placeholders})")
            conn.execute(sql, filtered)
            inserted += 1

    print(f"Inserted {inserted} rows for plant '{PLANT_ID}'.")
    return inserted


def verify():
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT equipment_type, equipment_id, manufacturer, model, dc_capacity_kwp, gamma_stc, temp_coefficient_per_deg "
            "FROM equipment_specs WHERE plant_id=:p ORDER BY equipment_type, equipment_id"
        ), {"p": PLANT_ID}).fetchall()
    print(f"\nVerification — {len(rows)} rows in equipment_specs for Tiger:")
    for r in rows[:5]:
        print(f"  [{r[0]}] {r[1]} | {r[2]} {r[3]} | dc_kwp={r[4]} | gamma={r[5]} | tc={r[6]}")
    if len(rows) > 5:
        print(f"  ... and {len(rows)-5} more rows")


if __name__ == "__main__":
    print("=== Tiger Equipment Specs Ingestion ===")
    ingest()
    verify()
    print("\nDone.")
