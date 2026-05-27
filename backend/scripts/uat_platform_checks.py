"""
Platform UAT — demo plant XYZ and cross-module consistency checks.

Run from backend/:
  python scripts/uat_platform_checks.py
  python scripts/uat_platform_checks.py --reload-demo
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from script_env import load_backend_env

load_backend_env()

from database import SessionLocal
from models import EquipmentSpec, User
from sqlalchemy import func, text


PLANT = "XYZ"
DATE_FROM = "2026-03-01"
DATE_TO = "2026-03-14"


def _check(name: str, ok: bool, detail: str = "") -> bool:
    tag = "PASS" if ok else "FAIL"
    line = f"  [{tag}] {name}"
    if detail:
        line += f" — {detail}"
    print(line)
    return ok


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reload-demo", action="store_true", help="Run load_demo_plant_xyz --reset first")
    args = parser.parse_args()

    if args.reload_demo:
        print("Reloading demo plant XYZ…")
        import subprocess
        r = subprocess.run(
            [sys.executable, "scripts/load_demo_plant_xyz.py", "--reset"],
            cwd=str(Path(__file__).resolve().parents[1]),
        )
        if r.returncode != 0:
            print("Demo reload failed.")
            return 1

    db = SessionLocal()
    failures = 0
    try:
        user = db.query(User).first()
        if not user:
            failures += 1
            _check("auth user exists", False)
            return 1

        print(f"\n=== Platform UAT — {PLANT} {DATE_FROM} .. {DATE_TO} ===\n")

        # ── Raw telemetry ──
        wms = db.execute(
            text(
                """
                SELECT signal, COUNT(*)::int AS n, ROUND(AVG(value)::numeric, 2) AS avg_v
                FROM raw_data_generic
                WHERE plant_id = :p
                  AND LOWER(TRIM(equipment_level::text)) = 'plant'
                  AND signal IN ('module_temp', 'ambient_temp', 'gti')
                  AND timestamp >= :f AND timestamp <= :t
                GROUP BY signal
                """
            ),
            {"p": PLANT, "f": f"{DATE_FROM} 00:00:00", "t": f"{DATE_TO} 23:59:59"},
        ).fetchall()
        wms_map = {r[0]: (r[1], float(r[2] or 0)) for r in wms}
        mt_n, mt_avg = wms_map.get("module_temp", (0, 0))
        failures += 0 if _check("WMS module_temp rows", mt_n > 100, f"count={mt_n} avg={mt_avg}°C") else 1
        failures += 0 if _check("WMS module_temp > 25°C (daylight avg)", mt_avg > 25.0, f"avg={mt_avg}") else 1

        # ── Equipment specs ──
        inv_specs = (
            db.query(EquipmentSpec)
            .filter(EquipmentSpec.plant_id == PLANT, func.lower(EquipmentSpec.equipment_type) == "inverter")
            .all()
        )
        deg_vals = [float(s.degradation_loss_pct or 0) for s in inv_specs]
        tc_vals = [float(s.temp_coefficient_per_deg or 0) for s in inv_specs]
        failures += 0 if _check("inverter specs exist", len(inv_specs) >= 4, f"n={len(inv_specs)}") else 1
        failures += 0 if _check("inverter degradation_loss_pct set", all(v > 0 for v in deg_vals), str(deg_vals)) else 1
        failures += 0 if _check("inverter temp_coefficient set", all(v > 0 for v in tc_vals), str(tc_vals)) else 1

        mod = (
            db.query(EquipmentSpec)
            .filter(EquipmentSpec.plant_id == PLANT, func.lower(EquipmentSpec.equipment_type) == "module")
            .first()
        )
        failures += 0 if _check("module gamma_stc set", mod and float(mod.gamma_stc or 0) > 0, f"gamma={getattr(mod, 'gamma_stc', None)}") else 1

        # ── Loss Analysis bridge ──
        from routers.loss_analysis import build_loss_bridge_payload

        bridge = build_loss_bridge_payload(db, PLANT, DATE_FROM, DATE_TO, "plant", None, user)
        if bridge.get("error"):
            failures += 1
            _check("loss bridge compute", False, bridge.get("message"))
        else:
            pri = bridge.get("primary") or {}
            deg_m = float(pri.get("degradation_mwh") or 0)
            temp_m = float(pri.get("temperature_loss_mwh") or 0)
            exp_m = float(pri.get("expected_mwh") or 0)
            wf = bridge.get("waterfall_bridge") or []
            wf_keys = [s.get("key") for s in wf]
            failures += 0 if _check("loss bridge expected_mwh > 0", exp_m > 0, f"{exp_m:.3f}") else 1
            failures += 0 if _check("loss bridge degradation_mwh > 0", deg_m > 0.01, f"{deg_m:.3f}") else 1
            failures += 0 if _check("loss bridge temperature_mwh > 0", temp_m > 0.01, f"{temp_m:.3f}") else 1
            failures += 0 if _check("waterfall has degradation step", "degradation" in wf_keys) else 1
            failures += 0 if _check("waterfall has temperature step", "temperature" in wf_keys) else 1
            failures += 0 if _check("module_temp in bridge meta", float(bridge.get("module_temp_c") or 0) > 25, str(bridge.get("module_temp_c"))) else 1
            # UI contract: first segment expected, last actual, unknown before actual
            wf_ok = (
                len(wf_keys) >= 3
                and wf_keys[0] == "expected"
                and wf_keys[-1] == "actual"
                and "unknown" in wf_keys
                and wf_keys.index("unknown") == len(wf_keys) - 2
            )
            failures += 0 if _check("waterfall segment order (expected…unknown,actual)", wf_ok, str(wf_keys)) else 1
            failures += 0 if _check("bridge_payload_version >= 2", int(bridge.get("bridge_payload_version") or 0) >= 2, str(bridge.get("bridge_payload_version"))) else 1
            ice = bridge.get("iceberg_faults") or []
            failures += 0 if _check("iceberg_faults present", len(ice) >= 8, f"n={len(ice)}") else 1
            ice_fc = sum(int(x.get("fault_count") or 0) for x in ice if isinstance(x, dict))
            failures += 0 if _check("iceberg fault_count sum > 0", ice_fc > 0, f"sum={ice_fc}") else 1
            from routers.faults import _unified_feed_categories_only

            uf = _unified_feed_categories_only(db, PLANT, DATE_FROM, DATE_TO, user)
            uf_fc = sum(int(c.get("fault_count") or 0) for c in (uf.get("categories") or []))
            failures += 0 if _check("iceberg counts match unified feed", ice_fc == uf_fc, f"ice={ice_fc} uf={uf_fc}") else 1
            diag_keys = [k for k in wf_keys if str(k).startswith("diag_")]
            failures += 0 if _check("waterfall has per-category diag_* steps", len(diag_keys) >= 1, str(diag_keys[:5])) else 1

        # ── Metadata save round-trip ──
        from routers.metadata import add_equipment_spec
        from schemas import EquipmentSpecRow
        from module_snapshots import get_loss_analysis_snapshot

        test_inv = inv_specs[0].equipment_id if inv_specs else "INV-01"
        base = inv_specs[0]
        payload = EquipmentSpecRow(
            id=base.id,
            plant_id=PLANT,
            equipment_id=test_inv,
            equipment_type="inverter",
            manufacturer=base.manufacturer,
            model=base.model,
            rated_power=base.rated_power,
            imp=base.imp,
            vmp=base.vmp,
            isc=base.isc,
            voc=base.voc,
            target_efficiency=base.target_efficiency,
            degradation_loss_pct=2.5,
            temp_coefficient_per_deg=0.004,
            ac_capacity_kw=base.ac_capacity_kw,
            dc_capacity_kwp=base.dc_capacity_kwp,
        )
        from module_snapshots import invalidate_loss_analysis_snapshots

        snap_before = get_loss_analysis_snapshot(db, PLANT, DATE_FROM, DATE_TO, "plant", "")
        add_equipment_spec(payload, db=db, current_user=user)
        row = (
            db.query(EquipmentSpec)
            .filter(EquipmentSpec.plant_id == PLANT, EquipmentSpec.equipment_id == test_inv)
            .first()
        )
        saved_deg = float(row.degradation_loss_pct or 0) if row else 0
        snap_after = get_loss_analysis_snapshot(db, PLANT, DATE_FROM, DATE_TO, "plant", "")
        failures += 0 if _check("metadata save degradation_loss_pct", abs(saved_deg - 2.5) < 0.01, f"got {saved_deg}") else 1
        failures += 0 if _check("loss snapshot cleared after spec save", snap_after is None, "re-open Loss Analysis to recompute") else 1
        if snap_after is None:
            bridge2 = build_loss_bridge_payload(db, PLANT, DATE_FROM, DATE_TO, "plant", None, user)
            from module_snapshots import save_loss_analysis_snapshot
            save_loss_analysis_snapshot(db, PLANT, DATE_FROM, DATE_TO, "plant", "", bridge2)
            deg2 = float((bridge2.get("primary") or {}).get("degradation_mwh") or 0)
            failures += 0 if _check("recomputed bridge uses new degradation %", deg2 > 0.05, f"deg_mwh={deg2:.3f}") else 1
        # restore demo default for repeatable runs
        payload.degradation_loss_pct = 1.2
        add_equipment_spec(payload, db=db, current_user=user)
        invalidate_loss_analysis_snapshots(db, PLANT)

        # ── Fault engines (summary) ──
        from scripts.validate_demo_plant_xyz import main as validate_main

        print("\n--- validate_demo_plant_xyz ---")
        vcode = validate_main()
        if vcode != 0:
            failures += 1
            _check("validate_demo_plant_xyz", False)
        else:
            _check("validate_demo_plant_xyz", True)

        print(f"\n=== UAT {'OK' if failures == 0 else 'FAILED'} ({failures} failed groups) ===\n")
        return 1 if failures else 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
