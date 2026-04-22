import json
import os
import re
import shutil
import sys
from pathlib import Path

import pandas as pd
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
from models import (
    EquipmentSpec,
    FaultCache,
    FaultRuntimeSnapshot,
    Plant,
    PlantArchitecture,
    PlantEquipment,
    RawDataGeneric,
    RawDataStats,
)  # noqa: E402


PLANT_ID = "BDL"
PLANT_NAME = "BDL"
PLANT_CAPACITY_MWP = 5.59608

SOURCE_BASE = Path(
    r"D:\WorkingFolder\OneDrive - vikramsolar.com\Desktop\ANALYSIS\BDL\Re_ Requirement of RAW Data-Asansol and BDL"
)
LOCAL_BASE = ROOT_DIR / "tmp_bdl"
STRINGS_FILE = LOCAL_BASE / "Strings data.xlsx"
INVERTER_REPORTS_DIR = ROOT_DIR / "tmp_bdl" / "inverter_reports"
WMS_REPORTS_DIR = ROOT_DIR / "tmp_bdl" / "wms_reports"
INVERTER_SPEC_FILE = LOCAL_BASE / "FIMER_PVS800-57B-from1645to1732_EN_RevB.pdf"
MODULE_265_SPEC_FILE = LOCAL_BASE / "datasheet-vikram-solar-eldora-prime-250-265-Wp-India.pdf"
MODULE_270_SPEC_FILE = LOCAL_BASE / "DS-60-ELD-Prime-1000V(270Wp).pdf"

UPLOADS_DIR = BACKEND_DIR / "uploads" / "spec_sheets" / PLANT_ID


def _safe_int(value):
    if pd.isna(value):
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def _safe_float(value):
    if pd.isna(value):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _ensure_inputs() -> None:
    missing = [str(p) for p in [STRINGS_FILE, INVERTER_SPEC_FILE, MODULE_265_SPEC_FILE, MODULE_270_SPEC_FILE] if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Missing required input(s): {missing}")
    if not INVERTER_REPORTS_DIR.exists():
        raise FileNotFoundError(
            f"Local inverter report copy not found: {INVERTER_REPORTS_DIR}. Copy the source reports into tmp_bdl\\inverter_reports first."
        )
    if not WMS_REPORTS_DIR.exists():
        raise FileNotFoundError(
            f"Local WMS report copy not found: {WMS_REPORTS_DIR}. Copy the source reports into tmp_bdl\\wms_reports first."
        )


def _copy_spec_sheet(src: Path, equipment_id: str) -> str:
    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    dest_name = f"{equipment_id}_{src.name}"
    dest = UPLOADS_DIR / dest_name
    shutil.copy2(src, dest)
    return str(Path("uploads") / "spec_sheets" / PLANT_ID / dest_name)


def build_architecture() -> tuple[list[dict], dict[int, int]]:
    df = pd.read_excel(STRINGS_FILE)
    df = df.rename(columns=lambda c: str(c).strip())
    df["Inverter no."] = df["Inverter no."].ffill()
    df = df[df["Inverter no."].notna()].copy()
    df = df[df["Inverter no."].astype(str).str.upper() != "TOTAL"].copy()

    df["inverter_num"] = df["Inverter no."].astype(str).str.extract(r"(\d+)").astype(int)
    df["scb_seq"] = df.groupby("inverter_num").cumcount() + 1
    scbs_per_inv = df.groupby("inverter_num").size().to_dict()

    arch_rows: list[dict] = []
    for _, row in df.iterrows():
        inv_num = int(row["inverter_num"])
        scb_seq = int(row["scb_seq"])
        inverter_id = f"INV-{inv_num:02d}"
        scb_id = f"{inverter_id}-SCB-{scb_seq:02d}"
        modules_per_string = _safe_int(row["No. of modules per string"])
        strings_per_scb = _safe_int(row["No. of strings connected to SCB"]) or 0
        scbs_per_inverter = int(scbs_per_inv[inv_num])
        dc_capacity_kw = _safe_float(row["Load per string(KW)"])

        for string_idx in range(1, strings_per_scb + 1):
            arch_rows.append(
                {
                    "plant_id": PLANT_ID,
                    "inverter_id": inverter_id,
                    "scb_id": scb_id,
                    "string_id": f"{scb_id}-STR-{string_idx:02d}",
                    "modules_per_string": modules_per_string,
                    "strings_per_scb": strings_per_scb,
                    "scbs_per_inverter": scbs_per_inverter,
                    "dc_capacity_kw": dc_capacity_kw,
                }
            )

    return arch_rows, scbs_per_inv


def build_equipment_specs() -> list[dict]:
    inverter_spec_path = _copy_spec_sheet(INVERTER_SPEC_FILE, "INV-SPEC")
    module_265_spec_path = _copy_spec_sheet(MODULE_265_SPEC_FILE, "MODULE-265WP")
    module_270_spec_path = _copy_spec_sheet(MODULE_270_SPEC_FILE, "MODULE-270WP")

    specs = []
    for inv_num in range(1, 4):
        specs.append(
            {
                "plant_id": PLANT_ID,
                "equipment_id": f"INV-{inv_num:02d}",
                "equipment_type": "inverter",
                "manufacturer": "ABB / FIMER",
                "model": "PVS800-57B-1732kW-C",
                "rated_power": 1732.0,
                "target_efficiency": 98.5,
                "ac_capacity_kw": 1732.0,
                # Use plant architecture for actual connected DC capacity per inverter.
                # The inverter datasheet max DC input is not the same as site-connected kWp.
                "dc_capacity_kwp": None,
                "rated_efficiency": 98.4,
                "mppt_voltage_min": 580.0,
                "mppt_voltage_max": 850.0,
                "voltage_limit": 1000.0,
                "current_set_point": 3700.0,
                "spec_sheet_path": inverter_spec_path,
            }
        )

    specs.extend(
        [
            {
                "plant_id": PLANT_ID,
                "equipment_id": "MODULE-265WP",
                "equipment_type": "module",
                "manufacturer": "Vikram Solar",
                "model": "ELDORA VSP.60.AAA.03",
                "rated_power": 265.0,
                "imp": 8.50,
                "vmp": 31.2,
                "isc": 9.00,
                "voc": 38.0,
                "target_efficiency": 16.3,
                "impp": 8.50,
                "vmpp": 31.2,
                "pmax": 265.0,
                "module_efficiency_pct": 16.3,
                "degradation_year1_pct": 2.5,
                "degradation_year2_pct": 0.67,
                "degradation_annual_pct": 0.67,
                "alpha_stc": 0.058,
                "beta_stc": -0.31,
                "gamma_stc": -0.41,
                "spec_sheet_path": module_265_spec_path,
            },
            {
                "plant_id": PLANT_ID,
                "equipment_id": "MODULE-270WP",
                "equipment_type": "module",
                "manufacturer": "Vikram Solar",
                "model": "ELDORA VSP.60.AAA.03.04",
                "rated_power": 270.0,
                "imp": 8.70,
                "vmp": 31.0,
                "isc": 9.12,
                "voc": 38.3,
                "target_efficiency": 16.6,
                "impp": 8.70,
                "vmpp": 31.0,
                "pmax": 270.0,
                "module_efficiency_pct": 16.6,
                "degradation_year1_pct": 2.5,
                "degradation_year2_pct": 0.67,
                "degradation_annual_pct": 0.67,
                "alpha_stc": 0.058,
                "beta_stc": -0.31,
                "gamma_stc": -0.41,
                "spec_sheet_path": module_270_spec_path,
            },
        ]
    )
    return specs


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    flattened = []
    for col in df.columns:
        if isinstance(col, tuple):
            parts = [str(part).strip() for part in col if str(part).strip() and not str(part).startswith("Unnamed:")]
            flattened.append(" | ".join(parts))
        else:
            flattened.append(str(col).strip())
    out = df.copy()
    out.columns = flattened
    return out


def build_raw_records() -> list[dict]:
    records: list[dict] = []
    report_files = sorted(INVERTER_REPORTS_DIR.glob("*.xlsx"))
    if not report_files:
        raise FileNotFoundError(f"No inverter reports found in {INVERTER_REPORTS_DIR}")

    for path in report_files:
        df = pd.read_excel(path, header=[2, 3, 4])
        df = _flatten_columns(df)
        ts_col = df.columns[0]
        df["timestamp"] = pd.to_datetime(df[ts_col], format="%Y-%m-%d %I:%M:%S %p", errors="coerce")
        df = df[df["timestamp"].notna()].copy()
        if df.empty:
            continue

        for inv_num in range(1, 4):
            prefix = f"INVERTER {inv_num} | "
            voltage_col = prefix + "DC INPUT | VOLTAGE (V)"
            current_col = prefix + "DC INPUT | CURRENT (A)"
            ac_power_col = prefix + "AC OUTPUT | POWER (KW)"
            energy_col = prefix + "ENERGY Today | (KWH)"
            cumulative_col = prefix + "ENERGY Today | Cumm. (KWH)"

            if voltage_col not in df.columns or current_col not in df.columns:
                continue

            voltage = pd.to_numeric(df[voltage_col], errors="coerce")
            current = pd.to_numeric(df[current_col], errors="coerce")
            ac_power = pd.to_numeric(df[ac_power_col], errors="coerce") if ac_power_col in df.columns else None
            energy = pd.to_numeric(df[energy_col], errors="coerce") if energy_col in df.columns else None
            cumulative = pd.to_numeric(df[cumulative_col], errors="coerce") if cumulative_col in df.columns else None

            equipment_id = f"INV-{inv_num:02d}"
            for idx, ts in df["timestamp"].items():
                ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
                v_val = _safe_float(voltage.loc[idx])
                c_val = _safe_float(current.loc[idx])

                if c_val is not None:
                    records.append(
                        {
                            "plant_id": PLANT_ID,
                            "timestamp": ts_str,
                            "equipment_level": "inverter",
                            "equipment_id": equipment_id,
                            "signal": "dc_current",
                            "value": c_val,
                            "source": path.stem,
                        }
                    )
                if v_val is not None:
                    records.append(
                        {
                            "plant_id": PLANT_ID,
                            "timestamp": ts_str,
                            "equipment_level": "inverter",
                            "equipment_id": equipment_id,
                            "signal": "dc_voltage",
                            "value": v_val,
                            "source": path.stem,
                        }
                    )
                if v_val is not None and c_val is not None:
                    dc_power_kw = max(v_val * c_val / 1000.0, 0.0)
                    records.append(
                        {
                            "plant_id": PLANT_ID,
                            "timestamp": ts_str,
                            "equipment_level": "inverter",
                            "equipment_id": equipment_id,
                            "signal": "dc_power",
                            "value": dc_power_kw,
                            "source": path.stem,
                        }
                    )

                ac_val = _safe_float(ac_power.loc[idx]) if ac_power is not None else None
                if ac_val is not None:
                    records.append(
                        {
                            "plant_id": PLANT_ID,
                            "timestamp": ts_str,
                            "equipment_level": "inverter",
                            "equipment_id": equipment_id,
                            "signal": "ac_power",
                            "value": max(ac_val, 0.0),
                            "source": path.stem,
                        }
                    )

                energy_val = None
                if energy is not None:
                    energy_val = _safe_float(energy.loc[idx])
                if energy_val is None and cumulative is not None:
                    energy_val = _safe_float(cumulative.loc[idx])
                if energy_val is not None:
                    records.append(
                        {
                            "plant_id": PLANT_ID,
                            "timestamp": ts_str,
                            "equipment_level": "inverter",
                            "equipment_id": equipment_id,
                            "signal": "energy_export_kwh",
                            "value": energy_val,
                            "source": path.stem,
                        }
                    )

    return records


def build_wms_records() -> list[dict]:
    records: list[dict] = []
    report_files = sorted(WMS_REPORTS_DIR.glob("*.csv"))
    if not report_files:
        return records

    deduped: dict[str, Path] = {}
    for path in report_files:
        match = re.search(r"(\d{4}-\d{2}-\d{2})", path.name)
        day_key = match.group(1) if match else path.stem
        deduped.setdefault(day_key, path)

    column_map = {
        "GTI(W/m2)_Avg": "gti",
        "GHI(W/m2)_Avg": "ghi",
        "WindSpeed(m/s)_Avg": "wind_speed",
        "ModuleTemp(DegC)_AvgDate": "module_temp",
    }

    for path in sorted(deduped.values(), key=lambda p: p.name):
        df = pd.read_csv(path)
        if "time" not in df.columns:
            continue
        df["timestamp"] = pd.to_datetime(df["time"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
        df = df[df["timestamp"].notna()].copy()
        if df.empty:
            continue

        numeric_cols = [col for col in column_map if col in df.columns]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        for _, row in df.iterrows():
            ts_str = row["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
            for src_col, signal in column_map.items():
                if src_col not in df.columns:
                    continue
                value = _safe_float(row.get(src_col))
                if value is None:
                    continue
                records.append(
                    {
                        "plant_id": PLANT_ID,
                        "timestamp": ts_str,
                        "equipment_level": "wms",
                        "equipment_id": PLANT_ID,
                        "signal": signal,
                        "value": value,
                        "source": path.stem,
                    }
                )

    return records


def _chunked(items: list[dict], size: int):
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def ingest() -> None:
    _ensure_inputs()
    arch_rows, scbs_per_inv = build_architecture()
    equipment_specs = build_equipment_specs()
    raw_records = build_raw_records()
    wms_records = build_wms_records()
    all_raw_records = raw_records + wms_records

    session = SessionLocal()
    try:
        plant = session.query(Plant).filter(Plant.plant_id == PLANT_ID).first()
        if plant is None:
            plant = Plant(
                plant_id=PLANT_ID,
                name=PLANT_NAME,
                technology="Solar PV",
                capacity_mwp=PLANT_CAPACITY_MWP,
                status="Active",
            )
            session.add(plant)
        else:
            plant.name = PLANT_NAME
            plant.technology = "Solar PV"
            plant.capacity_mwp = PLANT_CAPACITY_MWP
            plant.status = "Active"

        for model in (RawDataGeneric, PlantArchitecture, EquipmentSpec, PlantEquipment, RawDataStats, FaultRuntimeSnapshot):
            session.query(model).filter(getattr(model, "plant_id") == PLANT_ID).delete(synchronize_session=False)
        session.query(FaultCache).filter(
            FaultCache.cache_key.like(f"loss_gen_snapshot:{PLANT_ID}:%")
        ).delete(synchronize_session=False)
        session.query(FaultCache).filter(
            FaultCache.cache_key.like(f"ds_summary:{PLANT_ID}%")
        ).delete(synchronize_session=False)
        session.query(FaultCache).filter(
            FaultCache.cache_key.like(f"inv_eff_v2:{PLANT_ID}:%")
        ).delete(synchronize_session=False)
        session.commit()

        session.bulk_insert_mappings(PlantArchitecture, arch_rows)
        session.bulk_insert_mappings(EquipmentSpec, equipment_specs)
        for batch in _chunked(all_raw_records, 10000):
            session.bulk_insert_mappings(RawDataGeneric, batch)
            session.commit()

        equipment_rows = []
        for inv_num in sorted(scbs_per_inv):
            inverter_id = f"INV-{inv_num:02d}"
            equipment_rows.append({"plant_id": PLANT_ID, "equipment_level": "inverter", "equipment_id": inverter_id})
            for scb_seq in range(1, scbs_per_inv[inv_num] + 1):
                equipment_rows.append(
                    {
                        "plant_id": PLANT_ID,
                        "equipment_level": "scb",
                        "equipment_id": f"{inverter_id}-SCB-{scb_seq:02d}",
                    }
                )
        equipment_rows.extend(
            {
                "plant_id": PLANT_ID,
                "equipment_level": "string",
                "equipment_id": row["string_id"],
            }
            for row in arch_rows
        )
        equipment_rows.append({"plant_id": PLANT_ID, "equipment_level": "wms", "equipment_id": PLANT_ID})
        session.bulk_insert_mappings(PlantEquipment, equipment_rows)

        stats = session.execute(
            text(
                """
                SELECT COUNT(*) AS total_rows, MIN(timestamp) AS min_ts, MAX(timestamp) AS max_ts
                FROM raw_data_generic
                WHERE plant_id = :plant_id
                """
            ),
            {"plant_id": PLANT_ID},
        ).mappings().one()
        levels = session.execute(
            text(
                """
                SELECT equipment_level, COUNT(DISTINCT equipment_id) AS equipment_count
                FROM raw_data_generic
                WHERE plant_id = :plant_id
                GROUP BY equipment_level
                """
            ),
            {"plant_id": PLANT_ID},
        ).mappings().all()
        session.add(
            RawDataStats(
                plant_id=PLANT_ID,
                total_rows=int(stats["total_rows"] or 0),
                min_ts=stats["min_ts"],
                max_ts=stats["max_ts"],
                levels_json=json.dumps({row["equipment_level"]: int(row["equipment_count"]) for row in levels}),
            )
        )
        session.commit()

        try:
            from dashboard_cache import invalidate_plant as invalidate_dashboard_cache_plant
            invalidate_dashboard_cache_plant(PLANT_ID)
        except Exception:
            pass

        print(json.dumps(
            {
                "plant_id": PLANT_ID,
                "architecture_rows": len(arch_rows),
                "equipment_specs": len(equipment_specs),
                "raw_records": len(all_raw_records),
                "wms_records": len(wms_records),
                "equipment_index_rows": len(equipment_rows),
                "date_range": {"from": str(stats["min_ts"]) if stats["min_ts"] else None, "to": str(stats["max_ts"]) if stats["max_ts"] else None},
            },
            indent=2,
        ))
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    ingest()
