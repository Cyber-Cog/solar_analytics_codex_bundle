"""
Ingest the four NTPC-Nokhra spec sheets as reusable EquipmentSpec *variant* rows.

Background
----------
This plant has two inverter variants and two PV-module variants running side by
side. The existing `equipment_specs` schema keys rows by `equipment_id`, so we
store each variant as a template row with a sentinel id prefixed `VARIANT-`.
A follow-up UI ticket will let operators pick which physical inverter/module
maps to which variant; until that ships, these rows are the single source of
truth for spec-sheet values (used by Clipping & Derating's `rated_kw_map`,
Loss Analysis, and the Guidebook).

Source PDFs (located in the user's Downloads folder):
  • 5759-004-P2-PVE-Y-006A-00.pdf  →  Vikram Solar VSP.72.335.05 module (335 Wp)
  • 5759-004-P2-PVE-Y-006B-00.pdf  →  Adani / Mundra 72-cell multi module (335 Wp mid-bin)
  • 5759-004-P2-PVE-Y-007-02.pdf   →  Sineng EP-3125-HA-UD central inverter (3125 kW)
  • 5759-004-P2-PVE-Y-007A-02.pdf  →  TBEA TC3125KF central inverter (3125 kW)

Values were extracted directly from the datasheets (see transcript of
`tmp_pdf_probe.py` run during development).

Usage
-----
    python database/scripts/ingest_ntpc_nokhra_specs.py           # upsert + copy PDFs
    python database/scripts/ingest_ntpc_nokhra_specs.py --dry-run # print plan only
"""
from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path
from typing import Dict, List

from sqlalchemy import text


ROOT_DIR = Path(__file__).resolve().parents[2]
BACKEND_DIR = ROOT_DIR / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))


def _load_env() -> None:
    env_path = BACKEND_DIR / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


_load_env()

from database import SessionLocal  # noqa: E402
from models import EquipmentSpec  # noqa: E402


PLANT_ID = "NTPCNOKHRA"

SOURCE_PDF_DIR = Path(r"C:\Users\ayush.r\Downloads")
UPLOADS_DIR = BACKEND_DIR / "uploads" / "spec_sheets" / PLANT_ID


# ── Variant specs (pulled straight from the datasheets) ──────────────────────
# Keep the `equipment_id` prefixed with `VARIANT-` so list endpoints that filter
# by EquipmentSpec.equipment_type still see them, and the Metadata page can
# show them as a "Variant" row type.
INVERTER_VARIANTS: List[dict] = [
    {
        "equipment_id":     "VARIANT-INV-TBEA-TC3125KF",
        "equipment_type":   "inverter",
        "manufacturer":     "TBEA",
        "model":            "TC3125KF",
        "rated_power":      3125.0,      # legacy column — matches AC capacity in kW
        "ac_capacity_kw":   3125.0,      # Rated AC @ 50°C, unity PF
        "dc_capacity_kwp":  5000.0,      # Max. allowed PV field power (DC:AC 1.6:1)
        "rated_efficiency": 98.7,        # Euro efficiency (%); max eff is 99 %
        "mppt_voltage_min": 900.0,
        "mppt_voltage_max": 1300.0,
        "voltage_limit":    1500.0,
        "current_set_point": 4073.0,     # Max. input DC current (A)
        "target_efficiency": 98.5,
        "spec_sheet_src":   "5759-004-P2-PVE-Y-007A-02.pdf",
    },
    {
        "equipment_id":     "VARIANT-INV-SINENG-EP3125HAUD",
        "equipment_type":   "inverter",
        "manufacturer":     "Sineng",
        "model":            "EP-3125-HA-UD",
        "rated_power":      3125.0,
        "ac_capacity_kw":   3125.0,
        "dc_capacity_kwp":  5625.0,      # Max. DC input power @ 50 °C (180 % overload)
        "rated_efficiency": 98.7,
        "mppt_voltage_min": 900.0,
        "mppt_voltage_max": 1300.0,
        "voltage_limit":    1500.0,
        "current_set_point": 4075.0,
        "target_efficiency": 98.5,
        "spec_sheet_src":   "5759-004-P2-PVE-Y-007-02.pdf",
    },
]


MODULE_VARIANTS: List[dict] = [
    {
        "equipment_id":        "VARIANT-MOD-VIKRAM-VSP72-335",
        "equipment_type":      "module",
        "manufacturer":        "Vikram Solar Ltd.",
        "model":               "VSP.72.335.05",
        "rated_power":         335.66,   # Pmpp (W)
        "pmax":                335.66,
        "vmp":                 38.13,
        "imp":                 8.803,
        "voc":                 46.66,
        "isc":                 9.35,
        "impp":                8.803,
        "vmpp":                38.13,
        "module_efficiency_pct": 17.1,
        "spec_sheet_src":      "5759-004-P2-PVE-Y-006A-00.pdf",
    },
    {
        "equipment_id":        "VARIANT-MOD-ADANI-MUNDRA-335",
        "equipment_type":      "module",
        "manufacturer":        "Mundra Solar PV / Adani Solar",
        "model":               "72-cell multi (STC mid-bin 335 Wp)",
        "rated_power":         335.6,    # mid-bin of 325/330/335/340/345 family
        "pmax":                335.6,
        "vmp":                 37.67,
        "imp":                 8.91,
        "voc":                 45.65,
        "isc":                 9.58,
        "impp":                8.91,
        "vmpp":                37.67,
        "module_efficiency_pct": 16.6,
        "spec_sheet_src":      "5759-004-P2-PVE-Y-006B-00.pdf",
    },
]


def _copy_spec_pdf(src_name: str) -> str | None:
    """Copy the source PDF into uploads/ and return the relative path for the DB column."""
    src = SOURCE_PDF_DIR / src_name
    if not src.exists():
        print(f"  [warn] source PDF missing: {src}")
        return None
    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    dst = UPLOADS_DIR / src_name
    shutil.copy2(src, dst)
    rel = f"uploads/spec_sheets/{PLANT_ID}/{src_name}"
    return rel


def _upsert_variant(session, variant: dict, dry_run: bool = False) -> None:
    """Insert-or-update a single EquipmentSpec row for this plant + equipment_id."""
    eq_id = variant["equipment_id"]
    existing = session.query(EquipmentSpec).filter(
        EquipmentSpec.plant_id == PLANT_ID,
        EquipmentSpec.equipment_id == eq_id,
    ).first()

    spec_src = variant.pop("spec_sheet_src", None)
    spec_path = _copy_spec_pdf(spec_src) if (spec_src and not dry_run) else None

    payload = {k: v for k, v in variant.items() if v is not None}
    payload["plant_id"] = PLANT_ID
    if spec_path:
        payload["spec_sheet_path"] = spec_path

    if existing:
        for k, v in payload.items():
            setattr(existing, k, v)
        action = "UPDATE"
    else:
        obj = EquipmentSpec(**payload)
        session.add(obj)
        action = "INSERT"

    print(f"  [{action}] {eq_id}  "
          f"({payload.get('manufacturer', '?')} / {payload.get('model', '?')})")


def _summarise_existing(session) -> Dict[str, int]:
    """Useful context for the operator running the script."""
    rows = session.execute(
        text("SELECT equipment_type, COUNT(*) FROM equipment_specs "
             "WHERE plant_id = :p GROUP BY equipment_type ORDER BY equipment_type"),
        {"p": PLANT_ID},
    ).fetchall()
    return {r[0]: int(r[1]) for r in rows}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="print plan without writing")
    args = parser.parse_args()

    session = SessionLocal()
    try:
        print(f"── NTPC Nokhra spec-sheet ingestion ({PLANT_ID}) ──")
        before = _summarise_existing(session)
        print(f"  before: {before or '(no specs)'}")

        print("\nInverter variants:")
        for v in INVERTER_VARIANTS:
            _upsert_variant(session, dict(v), dry_run=args.dry_run)

        print("\nModule variants:")
        for v in MODULE_VARIANTS:
            _upsert_variant(session, dict(v), dry_run=args.dry_run)

        if args.dry_run:
            print("\n[dry-run] rolling back.")
            session.rollback()
        else:
            session.commit()
            print("\n[committed]")

        after = _summarise_existing(session)
        print(f"  after: {after or '(no specs)'}")
        return 0
    except Exception as e:
        session.rollback()
        print(f"[error] {e}")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
