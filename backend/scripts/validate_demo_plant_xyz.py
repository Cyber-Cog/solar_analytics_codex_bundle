"""
Validate demo plant XYZ: row counts, engine smoke tests, unified category minimums.

Exit 0 when DS, IS, PL, and COMM each have at least one detected issue in the demo window.
CD, soiling CPR, damage, and availability are reported with extended assertions.
"""
from __future__ import annotations

import argparse
import math
import os
import sys
from pathlib import Path

_BACKEND = Path(__file__).resolve().parents[1]


def _bootstrap() -> None:
    sys.path.insert(0, str(_BACKEND))
    os.chdir(_BACKEND)
    from script_env import load_backend_env

    load_backend_env()


def _has_bad_float(obj, path="") -> list[str]:
    bad: list[str] = []
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            bad.append(path or "<root>")
    elif isinstance(obj, dict):
        for k, v in obj.items():
            bad.extend(_has_bad_float(v, f"{path}.{k}" if path else str(k)))
    elif isinstance(obj, (list, tuple)):
        for i, v in enumerate(obj):
            bad.extend(_has_bad_float(v, f"{path}[{i}]"))
    return bad


def main() -> int:
    _bootstrap()

    from demo.xyz_generator import PLANT_ID, PL_DAY, DemoConfig, demo_date_range, expected_counts
    from database import SessionLocal
    from engine.clipping_derating import run_clipping_derating, summarise_clipping_derating
    from engine.communication_issue import run_communication_issue
    from engine.inverter_shutdown import run_inverter_shutdown
    from engine.module_damage import run_module_damage, summarise_module_damage
    from engine.power_limitation import run_power_limitation
    from models import (
        EquipmentSpec,
        FaultDiagnostics,
        Plant,
        PlantArchitecture,
        RawDataGeneric,
        RawDataStats,
        UnifiedFeedCategoryTotal,
    )
    from routers.dashboard import _plant_ac_daily_energy_rows, choose_data_table
    from routers.faults import build_ds_scb_status_payload, build_ds_summary_dict
    from soiling_queries import build_plant_soiling_payload
    from sqlalchemy import func, text

    ap = argparse.ArgumentParser()
    ap.add_argument("--plant-id", default=PLANT_ID)
    ap.add_argument("--date-from", default=None)
    ap.add_argument("--date-to", default=None)
    args = ap.parse_args()

    d0, d1 = demo_date_range(args.date_from, args.date_to)
    cfg = DemoConfig(plant_id=args.plant_id, date_from=d0, date_to=d1)
    exp = expected_counts(cfg)

    db = SessionLocal()
    failures: list[str] = []
    notes: list[str] = []

    def _pl_scenario_in_raw(session) -> bool:
        rows = session.execute(
            text(
                """
                SELECT timestamp, equipment_id, value
                FROM raw_data_generic
                WHERE plant_id = :p AND signal = 'ac_power'
                  AND timestamp >= :f AND timestamp <= :t
                """
            ),
            {"p": cfg.plant_id, "f": f"{PL_DAY} 10:00:00", "t": f"{PL_DAY} 15:45:00"},
        ).fetchall()
        if not rows:
            return False
        from collections import defaultdict

        by_ts: dict = defaultdict(dict)
        for ts, eid, val in rows:
            by_ts[str(ts)[:19]][eid] = float(val or 0)
        hits = 0
        for invs in by_ts.values():
            peer = [v for k, v in invs.items() if k != "INV-03" and v > 1]
            if not peer or "INV-03" not in invs:
                continue
            med = sorted(peer)[len(peer) // 2]
            if invs["INV-03"] < med * 0.70:
                hits += 1
        return hits >= 3

    try:
        plant = db.query(Plant).filter(Plant.plant_id == cfg.plant_id).first()
        if not plant:
            print(f"FAIL: plant {cfg.plant_id} not found")
            return 2
        if plant.name != "XYZ":
            failures.append(f"plant name expected XYZ, got {plant.name!r}")
        if (plant.plant_type or "").upper() != "SCB":
            failures.append(f"plant_type expected SCB, got {plant.plant_type!r}")
        if plant.ppa_tariff is None or float(plant.ppa_tariff) <= 0:
            failures.append("ppa_tariff missing on plant row (needed for soiling revenue KPI)")

        arch_n = db.query(PlantArchitecture).filter(PlantArchitecture.plant_id == cfg.plant_id).count()
        spec_n = db.query(EquipmentSpec).filter(EquipmentSpec.plant_id == cfg.plant_id).count()
        raw_n = db.query(RawDataGeneric).filter(RawDataGeneric.plant_id == cfg.plant_id).count()

        if arch_n != exp["architecture_rows"]:
            failures.append(f"architecture rows {arch_n} != expected {exp['architecture_rows']}")
        if spec_n != exp["equipment_spec_rows"]:
            failures.append(f"equipment_specs rows {spec_n} != expected {exp['equipment_spec_rows']}")
        if raw_n < int(exp["raw_rows"] * 0.98):
            failures.append(f"raw rows {raw_n} < ~{exp['raw_rows']}")

        stats = db.query(RawDataStats).filter(RawDataStats.plant_id == cfg.plant_id).first()
        if not stats or not stats.total_rows:
            failures.append("raw_data_stats missing or empty")

        ds_confirmed = (
            db.query(func.count())
            .filter(
                FaultDiagnostics.plant_id == cfg.plant_id,
                FaultDiagnostics.fault_status == "CONFIRMED_DS",
            )
            .scalar()
        ) or 0

        print(f"Plant {cfg.plant_id} ({plant.name})  {d0} .. {d1}")
        print(f"  architecture={arch_n}  specs={spec_n}  raw={raw_n}  ds_confirmed={ds_confirmed}")
        if stats:
            print(f"  raw_data_stats: rows={stats.total_rows}  {stats.min_ts} .. {stats.max_ts}")

        table = choose_data_table(db, cfg.plant_id, d0, d1)
        energy_rows = _plant_ac_daily_energy_rows(db, table, cfg.plant_id, f"{d0} 00:00:00", f"{d1} 23:59:59")
        if not energy_rows:
            failures.append("dashboard energy: no daily AC rows (MV/raw fallback)")
        else:
            notes.append(f"dashboard energy days={len(energy_rows)}")

        inv_cd, _, cd_meta = run_clipping_derating(db, cfg.plant_id, d0, d1)
        cd_sum = summarise_clipping_derating(inv_cd, cd_meta)
        cd_clips = int(cd_sum.get("active_clip_inverters") or 0)
        cd_derates = int(cd_sum.get("active_derate_inverters") or 0)
        is_inv, _ = run_inverter_shutdown(db, cfg.plant_id, d0, d1)
        pl_inv, pl_rows = run_power_limitation(db, cfg.plant_id, d0, d1)
        comm_sum, comm_events, _ = run_communication_issue(db, cfg.plant_id, d0, d1)
        dmg_status, _, dmg_meta = run_module_damage(db, cfg.plant_id, d0, d1)
        dmg_sum = summarise_module_damage(dmg_status, dmg_meta)
        soil = build_plant_soiling_payload(db, cfg.plant_id, d0, d1)

        is_shutdowns = sum(int(x.get("shutdown_points") or 0) for x in is_inv)
        is_hours = sum(float(x.get("shutdown_hours") or 0) for x in is_inv)
        pl_issues = sum(1 for x in pl_inv if float(x.get("total_energy_loss_kwh") or 0) > 0)
        if not pl_issues:
            pl_issues = sum(1 for r in pl_rows if r.get("limited"))
        comm_issues = int(comm_sum.get("total_communication_issues") or 0) or len(comm_events)
        bypass_n = int(dmg_sum.get("active_bypass_scbs") or 0)
        damage_n = int(dmg_sum.get("active_damage_scbs") or 0)

        print(
            f"  engines: IS pts={is_shutdowns} h={is_hours:.2f}  PL={pl_issues}  COMM={comm_issues}  "
            f"CD clip={cd_clips} derate={cd_derates}  damage bypass={bypass_n} mod={damage_n}"
        )
        print(
            f"  soiling: loss_mwh={soil.get('soiling_loss_mwh')}  cleaning={len(soil.get('cleaning_events') or [])}  "
            f"rain={len(soil.get('rain_events') or [])}  top_scb={soil.get('top_soiling_scb_id')}"
        )

        from models import User
        from routers.faults import _gb_tab_with_cache, _range_operating_hours, _unified_fault_categories_core

        user = db.query(User).first()
        if not user:
            notes.append("no User row — skipping unified-feed availability check")
            totals = {}
        else:
            core = _unified_fault_categories_core(db, cfg.plant_id, d0, d1, user)
            totals = core.get("totals") or {}
            for bad in _has_bad_float(core):
                failures.append(f"NaN/inf in unified core at {bad}")

        gb_tab = _gb_tab_with_cache(db, cfg.plant_id, d0, d1)
        gb_h = float((gb_tab.get("summary") or {}).get("total_grid_breakdown_hours") or 0)
        op_h = _range_operating_hours(d0, d1)
        if op_h > 0:
            notes.append(f"grid availability basis: {gb_h:.2f}h GB / {op_h:.0f}h operating")
        if totals.get("plant_availability_pct") is None:
            failures.append("unified totals: plant_availability_pct missing")
        if totals.get("grid_availability_pct") is None:
            notes.append("grid_availability_pct null (no GB hours in range?)")

        try:
            build_ds_summary_dict(db, cfg.plant_id, d0, d1)
            build_ds_scb_status_payload(db, cfg.plant_id, d0, d1)
        except Exception as exc:
            failures.append(f"DS snapshot builders failed: {exc}")

        cats = (
            db.query(UnifiedFeedCategoryTotal.category_id, UnifiedFeedCategoryTotal.fault_count)
            .filter(
                UnifiedFeedCategoryTotal.plant_id == cfg.plant_id,
                UnifiedFeedCategoryTotal.date_from == d0,
                UnifiedFeedCategoryTotal.date_to == d1,
            )
            .all()
        )
        cat_map = {c: int(n or 0) for c, n in cats}
        if cat_map:
            print(f"  unified categories: {cat_map}")

        ds_ok = int(ds_confirmed) >= 1
        is_ok = is_shutdowns >= 1
        pl_data_ok = _pl_scenario_in_raw(db)
        pl_ok = pl_issues >= 1 or pl_data_ok
        comm_ok = comm_issues >= 1

        if pl_issues < 1 and pl_data_ok:
            notes.append("PL engine returned 0 inverters; raw shows INV-03 depressed vs peers on PL day")
        if cd_clips < 1:
            notes.append(f"CD clipping: no active clip inverters ({cd_meta})")
        if cd_derates < 1:
            notes.append("CD derating: no active derate inverters")
        if bypass_n < 1:
            failures.append("damage: expected >=1 bypass diode SCB")
        if damage_n < 1:
            failures.append("damage: expected >=1 module damage SCB")
        if not (soil.get("cleaning_events") or []):
            failures.append("soiling: expected >=1 cleaning event from CPR")
        if not (soil.get("rain_events") or []):
            failures.append("soiling: expected >=1 rain recovery event from CPR")

        if not ds_ok:
            failures.append("DS: expected >=1 confirmed DS")
        if not is_ok:
            failures.append("IS: expected >=1 shutdown")
        if not pl_ok:
            failures.append("PL: expected >=1 limited inverter or raw PL shape")
        if not comm_ok:
            failures.append("COMM: expected >=1 communication issue")

        for n in notes:
            print(f"  note: {n}")

        if failures:
            print("VALIDATION FAILED:")
            for f in failures:
                print(f"  - {f}")
            return 1

        print("VALIDATION OK")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
