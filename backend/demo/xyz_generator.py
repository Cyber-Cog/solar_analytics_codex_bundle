"""
Deterministic demo telemetry for plant XYZ (CEO demo).

Shapes values only — all fault classification uses existing engines.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterator, List, Optional, Tuple

import numpy as np
import pandas as pd

PLANT_ID = "XYZ"
PLANT_NAME = "XYZ"
DEFAULT_DATE_FROM = "2026-03-01"
DEFAULT_DATE_TO = "2026-03-14"
RNG_SEED = 42
DEMO_PPA_TARIFF = 4.5  # ₹/kWh for revenue-loss KPIs

NUM_INVERTERS = 4
SCBS_PER_INVERTER = 2
STRINGS_PER_SCB = 8
MODULES_PER_STRING = 20
DC_KW_PER_STRING = 4.2
AC_RATED_KW = 50.0
CADENCE_MINUTES = 15

ISC_PER_STRING_A = 8.0
REF_GTI = 800.0
DC_VOLTAGE_V = 550.0
VMP_PER_MODULE = 41.0

DS_DAY = "2026-03-09"
IS_DAY = "2026-03-10"
PL_DAY = "2026-03-11"
COMM_DAY = "2026-03-12"
CD_DAY = "2026-03-13"
DERATE_DAY = "2026-03-08"
SOILING_CLEAN_DAY = "2026-03-07"
SOILING_RAIN_DAY = "2026-03-10"
BYPASS_DAY = "2026-03-06"
MODULE_DAMAGE_DAY = "2026-03-05"

BYPASS_SCB = "INV-01-SCB-02"
MODULE_DAMAGE_SCB = "INV-02-SCB-01"
SOILING_TARGET_SCB = "INV-01-SCB-01"


@dataclass(frozen=True)
class DemoConfig:
    plant_id: str = PLANT_ID
    plant_name: str = PLANT_NAME
    date_from: str = DEFAULT_DATE_FROM
    date_to: str = DEFAULT_DATE_TO
    cadence_minutes: int = CADENCE_MINUTES


def demo_date_range(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> Tuple[str, str]:
    d0 = (date_from or DEFAULT_DATE_FROM)[:10]
    d1 = (date_to or DEFAULT_DATE_TO)[:10]
    return d0, d1


def _inverter_ids() -> List[str]:
    return [f"INV-{i:02d}" for i in range(1, NUM_INVERTERS + 1)]


def _scb_id(inv_id: str, scb_idx: int) -> str:
    return f"{inv_id}-SCB-{scb_idx:02d}"


def _string_id(scb_id: str, str_idx: int) -> str:
    return f"{scb_id}-STR-{str_idx:02d}"


def build_architecture_rows(cfg: Optional[DemoConfig] = None) -> List[dict]:
    cfg = cfg or DemoConfig()
    rows: List[dict] = []
    for inv in _inverter_ids():
        for scb_n in range(1, SCBS_PER_INVERTER + 1):
            scb = _scb_id(inv, scb_n)
            spare = inv == "INV-04" and scb_n == 2
            for s in range(1, STRINGS_PER_SCB + 1):
                rows.append(
                    {
                        "plant_id": cfg.plant_id,
                        "inverter_id": inv,
                        "scb_id": scb,
                        "string_id": _string_id(scb, s),
                        "modules_per_string": MODULES_PER_STRING,
                        "strings_per_scb": STRINGS_PER_SCB,
                        "scbs_per_inverter": SCBS_PER_INVERTER,
                        "dc_capacity_kw": DC_KW_PER_STRING,
                        "spare_flag": spare,
                    }
                )
    return rows


def build_equipment_specs(cfg: Optional[DemoConfig] = None) -> Tuple[List[dict], float]:
    cfg = cfg or DemoConfig()
    arch = build_architecture_rows(cfg)
    inv_dc: Dict[str, float] = {}
    for row in arch:
        if row.get("spare_flag"):
            continue
        inv = row["inverter_id"]
        inv_dc[inv] = inv_dc.get(inv, 0.0) + float(row["dc_capacity_kw"] or 0.0)

    specs: List[dict] = [
        {
            "plant_id": cfg.plant_id,
            "equipment_id": "MOD-DEMO",
            "equipment_type": "module",
            "manufacturer": "Demo",
            "model": "Demo-550W",
            "isc": 10.0,
            "imp": 10.5,
            "vmp": VMP_PER_MODULE,
            "pmax": 0.55,
            "target_efficiency": 98.5,
        }
    ]
    for inv, dc_kwp in sorted(inv_dc.items()):
        specs.append(
            {
                "plant_id": cfg.plant_id,
                "equipment_id": inv,
                "equipment_type": "inverter",
                "manufacturer": "Demo",
                "model": "Demo-50kW",
                "rated_power": AC_RATED_KW,
                "ac_capacity_kw": AC_RATED_KW,
                "dc_capacity_kwp": round(dc_kwp, 3),
                "rated_efficiency": 98.0,
                "target_efficiency": 98.5,
            }
        )
    plant_mwp = sum(inv_dc.values()) / 1000.0
    return specs, plant_mwp


def _iter_timestamps(d0: datetime, d1: datetime, step_min: int) -> Iterator[datetime]:
    cur = d0
    end = d1 + timedelta(days=1)
    delta = timedelta(minutes=step_min)
    while cur < end:
        yield cur
        cur += delta


def _solar_gti(ts: datetime, rng: np.random.Generator) -> float:
    h = ts.hour + ts.minute / 60.0
    if h < 6.0 or h >= 19.0:
        return 0.0
    x = (h - 6.0) / 13.0
    base = 850.0 * np.sin(np.pi * x) ** 1.15
    noise = rng.normal(0.0, 12.0)
    return float(max(0.0, base + noise))


def _healthy_scb_current(gti: float, rng: np.random.Generator) -> float:
    if gti < 150.0:
        return 0.0
    scale = gti / REF_GTI
    per_string = ISC_PER_STRING_A * scale
    total = per_string * STRINGS_PER_SCB
    return float(max(0.0, total + rng.normal(0.0, 1.5)))


def _healthy_ac_kw(gti: float, inv_index: int, rng: np.random.Generator) -> float:
    if gti < 150.0:
        return 0.0
    eff = 0.96 if inv_index == 0 else 0.92 + 0.02 * (inv_index % 3)
    nominal = AC_RATED_KW * eff * (gti / REF_GTI)
    return float(max(0.0, min(AC_RATED_KW * 0.99, nominal + rng.normal(0.0, 0.8))))


def _pl_limit_factor(hm: int) -> float:
    """Smooth U-shaped AC suppression for power-limitation demo (10:15–16:00)."""
    start = 10 * 60 + 15
    end = 16 * 60
    if hm < start or hm > end:
        return 1.0
    mid = (start + end) / 2.0
    half = (end - start) / 2.0
    x = (hm - mid) / half
    u = 0.42 + 0.18 * (x * x)
    if hm < start + 30:
        ramp = (hm - start) / 30.0
        return 1.0 - (1.0 - u) * max(0.0, min(1.0, ramp))
    return u


def _derate_factor(hm: int) -> float:
    """Shallow static derate bowl — not shutdown-like."""
    if hm < 10 * 60 or hm > 15 * 60:
        return 1.0
    mid = 12 * 60 + 30
    half = 2.5 * 60
    x = (hm - mid) / half
    return 0.90 + 0.04 * (x * x)


def _soiling_cpr_factor(day: str, scb: str, day_index: int, d0s: str) -> float:
    """Deterministic CPR multiplier: slow decline, one SCB cleaning jump, one plant rain recovery."""
    base = 1.0 - 0.004 * day_index
    if scb == SOILING_TARGET_SCB:
        base -= 0.006 * day_index
    if day == SOILING_CLEAN_DAY and scb == SOILING_TARGET_SCB:
        base += 0.12
    if day == SOILING_RAIN_DAY:
        base += 0.08
    return max(0.78, min(1.05, base))


def _scb_voltage_v(scb: str, day: str, hm: int, rng: np.random.Generator) -> float:
    v = DC_VOLTAGE_V + rng.normal(0.0, 0.45)
    modules_total = STRINGS_PER_SCB * MODULES_PER_STRING
    v_per_module = DC_VOLTAGE_V / modules_total
    if scb == BYPASS_SCB and day == BYPASS_DAY and 9 * 60 <= hm <= 16 * 60:
        v -= 0.33 * v_per_module
    if scb == MODULE_DAMAGE_SCB and day == MODULE_DAMAGE_DAY and 9 * 60 <= hm <= 16 * 60:
        v -= 1.25 * v_per_module
    return float(v)


def _apply_faults(
    ts: datetime,
    inv: str,
    scb: str,
    gti: float,
    ac_kw: float,
    scb_a: float,
    day_index: int,
    d0s: str,
) -> Tuple[float, float]:
    day = ts.strftime("%Y-%m-%d")
    hm = ts.hour * 60 + ts.minute

    if day == DS_DAY and inv == "INV-01" and scb == "INV-01-SCB-02":
        if 7 * 60 + 30 <= hm <= 16 * 60 and gti >= 200.0:
            scb_a = scb_a * 0.55

    if day == IS_DAY and inv == "INV-02":
        if 10 * 60 <= hm < 14 * 60 and gti > 10.0:
            ac_kw = 0.0

    if day == PL_DAY and inv == "INV-03" and gti > 500.0:
        if hm < 10 * 60 + 15:
            pass
        elif hm <= 15 * 60 + 59:
            ac_kw = ac_kw * _pl_limit_factor(hm)

    if day == CD_DAY and inv == "INV-01":
        if 10 * 60 <= hm <= 14 * 60 and gti >= 650.0:
            ac_kw = AC_RATED_KW * 0.99
        elif gti >= 200.0 and gti <= 680.0 and hm < 10 * 60:
            ac_kw = min(AC_RATED_KW * 0.84, AC_RATED_KW * 0.96 * (gti / REF_GTI))

    if day == DERATE_DAY and inv == "INV-02" and gti >= 400.0:
        ac_kw = ac_kw * _derate_factor(hm)

    if scb:
        scb_a = scb_a * _soiling_cpr_factor(day, scb, day_index, d0s)

    return ac_kw, scb_a


def build_raw_dataframe(cfg: Optional[DemoConfig] = None) -> pd.DataFrame:
    cfg = cfg or DemoConfig()
    d0s, d1s = demo_date_range(cfg.date_from, cfg.date_to)
    d0 = datetime.strptime(d0s, "%Y-%m-%d")
    d1 = datetime.strptime(d1s, "%Y-%m-%d")

    rng = np.random.default_rng(RNG_SEED)
    arch = build_architecture_rows(cfg)
    scb_by_inv: Dict[str, List[str]] = {}
    spare_scbs = set()
    for row in arch:
        if row.get("spare_flag"):
            spare_scbs.add(row["scb_id"])
            continue
        scb_by_inv.setdefault(row["inverter_id"], [])
        if row["scb_id"] not in scb_by_inv[row["inverter_id"]]:
            scb_by_inv[row["inverter_id"]].append(row["scb_id"])

    records: List[dict] = []
    day_index = 0
    prev_day: Optional[str] = None

    for ts in _iter_timestamps(d0, d1, cfg.cadence_minutes):
        gti = _solar_gti(ts, rng)
        if gti <= 0.0:
            continue
        day = ts.strftime("%Y-%m-%d")
        if day != prev_day:
            if prev_day is not None:
                day_index += 1
            prev_day = day
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
        irr = float(gti)
        hm = ts.hour * 60 + ts.minute

        records.append(
            {
                "timestamp": ts_str,
                "equipment_level": "plant",
                "equipment_id": cfg.plant_id,
                "signal": "gti",
                "value": gti,
            }
        )
        records.append(
            {
                "timestamp": ts_str,
                "equipment_level": "plant",
                "equipment_id": cfg.plant_id,
                "signal": "irradiance",
                "value": irr,
            }
        )

        skip_inv04_ac = day == COMM_DAY and 11 * 60 <= hm < 13 * 60

        for inv_i, inv in enumerate(_inverter_ids()):
            ac = _healthy_ac_kw(gti, inv_i, rng)
            if skip_inv04_ac and inv == "INV-04":
                pass
            else:
                ac, _ = _apply_faults(ts, inv, "", gti, ac, 0.0, day_index, d0s)
                records.append(
                    {
                        "timestamp": ts_str,
                        "equipment_level": "inverter",
                        "equipment_id": inv,
                        "signal": "ac_power",
                        "value": ac,
                    }
                )
                records.append(
                    {
                        "timestamp": ts_str,
                        "equipment_level": "inverter",
                        "equipment_id": inv,
                        "signal": "dc_power",
                        "value": ac * 1.05,
                    }
                )

            for scb in scb_by_inv.get(inv, []):
                if scb in spare_scbs:
                    continue
                scb_a = _healthy_scb_current(gti, rng)
                _, scb_a = _apply_faults(ts, inv, scb, gti, ac, scb_a, day_index, d0s)
                records.append(
                    {
                        "timestamp": ts_str,
                        "equipment_level": "scb",
                        "equipment_id": scb,
                        "signal": "dc_current",
                        "value": scb_a,
                    }
                )
                records.append(
                    {
                        "timestamp": ts_str,
                        "equipment_level": "scb",
                        "equipment_id": scb,
                        "signal": "dc_voltage",
                        "value": _scb_voltage_v(scb, day, hm, rng),
                    }
                )

    return pd.DataFrame(records)


def build_plant_row(cfg: Optional[DemoConfig] = None, capacity_mwp: float = 0.0) -> dict:
    cfg = cfg or DemoConfig()
    _, plant_mwp = build_equipment_specs(cfg)
    cap = capacity_mwp if capacity_mwp > 0 else plant_mwp
    return {
        "plant_id": cfg.plant_id,
        "name": cfg.plant_name,
        "technology": "Solar PV",
        "location": "Demo Site",
        "capacity_mwp": round(cap, 4),
        "status": "Active",
        "plant_type": "SCB",
        "ppa_tariff": DEMO_PPA_TARIFF,
    }


def export_demo_workbooks(
    out_dir: str,
    cfg: Optional[DemoConfig] = None,
) -> Dict[str, str]:
    import os

    cfg = cfg or DemoConfig()
    os.makedirs(out_dir, exist_ok=True)
    paths: Dict[str, str] = {}

    arch_df = pd.DataFrame(build_architecture_rows(cfg))
    spec_rows, _ = build_equipment_specs(cfg)
    spec_df = pd.DataFrame(spec_rows)
    raw_df = build_raw_dataframe(cfg)

    p_arch = os.path.join(out_dir, "architecture.xlsx")
    p_spec = os.path.join(out_dir, "equipment_specs.xlsx")
    p_raw = os.path.join(out_dir, "raw_data_generic.xlsx")
    arch_df.to_excel(p_arch, index=False)
    spec_df.to_excel(p_spec, index=False)
    raw_df.to_excel(p_raw, index=False)
    paths["architecture"] = p_arch
    paths["equipment_specs"] = p_spec
    paths["raw_data_generic"] = p_raw
    return paths


def build_ds_detection_dataframe(
    raw_df: pd.DataFrame,
    cfg: Optional[DemoConfig] = None,
) -> pd.DataFrame:
    cfg = cfg or DemoConfig()
    arch = build_architecture_rows(cfg)
    inv_map = {r["scb_id"]: r["inverter_id"] for r in arch if r.get("scb_id")}
    scb = raw_df[
        (raw_df["equipment_level"] == "scb") & (raw_df["signal"] == "dc_current")
    ].copy()
    if scb.empty:
        return pd.DataFrame(columns=["timestamp", "inverter_id", "scb_id", "scb_current"])
    out = scb.rename(columns={"equipment_id": "scb_id", "value": "scb_current"})
    out["inverter_id"] = out["scb_id"].map(inv_map)
    out = out.dropna(subset=["inverter_id", "scb_current"])
    return out[["timestamp", "inverter_id", "scb_id", "scb_current"]]


def expected_counts(cfg: Optional[DemoConfig] = None) -> dict:
    cfg = cfg or DemoConfig()
    raw_df = build_raw_dataframe(cfg)
    n_ts = raw_df[raw_df["signal"] == "gti"]["timestamp"].nunique()
    n_inv = NUM_INVERTERS
    n_scb = NUM_INVERTERS * SCBS_PER_INVERTER - 1
    arch_rows = len(build_architecture_rows(cfg))
    return {
        "architecture_rows": arch_rows,
        "daylight_timestamps": int(n_ts),
        "raw_rows": len(raw_df),
        "equipment_spec_rows": len(build_equipment_specs(cfg)[0]),
        "inverters": n_inv,
        "active_scbs": n_scb,
    }
