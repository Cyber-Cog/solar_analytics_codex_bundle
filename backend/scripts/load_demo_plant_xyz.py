"""
Load CEO demo plant XYZ (4×2×8 SCB topology, 14 days @ 15 min).

Run from backend/ with DATABASE_URL in .env:
  python scripts/load_demo_plant_xyz.py --reset

Grant plant access: add XYZ to the demo user's allowed_plants (comma-separated)
or log in as an admin user (allowed_plants = all).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

_BACKEND = Path(__file__).resolve().parents[1]


def _bootstrap() -> None:
    sys.path.insert(0, str(_BACKEND))
    os.chdir(_BACKEND)
    from script_env import load_backend_env

    load_backend_env()
    if not os.environ.get("DB_STATEMENT_TIMEOUT_MS"):
        os.environ["DB_STATEMENT_TIMEOUT_MS"] = "600000"


def delete_plant_xyz(db, plant_id: str) -> None:
    from models import (
        DCHierarchyDerived,
        DsStatusSnapshot,
        DsSummarySnapshot,
        EquipmentSpec,
        FaultCache,
        FaultDiagnostics,
        FaultEpisode,
        FaultEpisodeDay,
        FaultEvent,
        FaultRuntimeSnapshot,
        LossAnalysisSnapshot,
        Plant,
        PlantArchitecture,
        PlantComputeStatus,
        PlantEquipment,
        PrecomputeJob,
        RawDataGeneric,
        RawDataStats,
        ScbFaultReview,
        SupportTicket,
        UnifiedFaultSnapshot,
        UnifiedFeedCategoryTotal,
        User,
    )

    for user in db.query(User).all():
        allowed = str(user.allowed_plants or "").strip()
        if not allowed or allowed == "*":
            continue
        plants = [p.strip() for p in allowed.split(",") if p.strip()]
        if plant_id not in plants:
            continue
        user.allowed_plants = ",".join(p for p in plants if p != plant_id) or None

    for model in (
        RawDataGeneric,
        DCHierarchyDerived,
        PlantArchitecture,
        EquipmentSpec,
        SupportTicket,
        FaultDiagnostics,
        FaultEpisode,
        FaultEpisodeDay,
        FaultEvent,
        PlantEquipment,
        RawDataStats,
        ScbFaultReview,
        FaultRuntimeSnapshot,
        DsSummarySnapshot,
        DsStatusSnapshot,
        UnifiedFaultSnapshot,
        LossAnalysisSnapshot,
        UnifiedFeedCategoryTotal,
        PlantComputeStatus,
    ):
        db.query(model).filter(model.plant_id == plant_id).delete(synchronize_session=False)

    db.query(PrecomputeJob).filter(PrecomputeJob.plant_id == plant_id).delete(synchronize_session=False)
    db.query(FaultCache).filter(FaultCache.cache_key.like(f"ds_summary:{plant_id}%")).delete(
        synchronize_session=False
    )
    db.query(FaultCache).filter(FaultCache.cache_key.like(f"inv_eff:%{plant_id}%")).delete(
        synchronize_session=False
    )
    db.query(FaultCache).filter(FaultCache.cache_key.like(f"inv_eff_v2:{plant_id}:%")).delete(
        synchronize_session=False
    )
    db.query(FaultCache).filter(FaultCache.cache_key.like(f"loss_gen_snapshot:{plant_id}:%")).delete(
        synchronize_session=False
    )

    plant = db.query(Plant).filter(Plant.plant_id == plant_id).first()
    if plant:
        db.delete(plant)
    db.commit()


def bulk_insert_raw(db, plant_id: str, raw_df) -> int:
    from models import RawDataGeneric

    batch: list[RawDataGeneric] = []
    n = 0
    for row in raw_df.itertuples(index=False):
        batch.append(
            RawDataGeneric(
                plant_id=plant_id,
                timestamp=row.timestamp,
                equipment_level=row.equipment_level,
                equipment_id=row.equipment_id,
                signal=row.signal,
                value=float(row.value),
                source="demo_generator",
            )
        )
        if len(batch) >= 5000:
            db.bulk_save_objects(batch)
            db.commit()
            n += len(batch)
            batch = []
    if batch:
        db.bulk_save_objects(batch)
        db.commit()
        n += len(batch)
    return n


def resolve_precompute_user(db):
    from models import User

    user = db.query(User).filter(User.is_admin.is_(True)).first()
    if not user:
        user = db.query(User).filter(User.id == 1).first()
    if not user:
        user = db.query(User).first()
    return user


def main() -> int:
    _bootstrap()

    from demo.xyz_generator import (
        PLANT_ID,
        DemoConfig,
        build_architecture_rows,
        build_ds_detection_dataframe,
        build_equipment_specs,
        build_plant_row,
        build_raw_dataframe,
        demo_date_range,
        expected_counts,
    )
    from database import SessionLocal
    from engine.ds_detection import run_ds_detection
    from models import EquipmentSpec, FaultDiagnostics, Plant, PlantArchitecture, RawDataStats
    from module_precompute import compute_snapshots_for_range, update_plant_compute_status
    from routers.metadata import _refresh_equipment_mat, _refresh_plant_stats
    from sqlalchemy import func

    ap = argparse.ArgumentParser(description="Load demo plant XYZ into PostgreSQL")
    ap.add_argument("--reset", action="store_true", help="Delete existing XYZ data first")
    ap.add_argument("--date-from", default=None)
    ap.add_argument("--date-to", default=None)
    ap.add_argument("--skip-precompute", action="store_true")
    ap.add_argument("--plant-id", default=PLANT_ID)
    args = ap.parse_args()

    d0, d1 = demo_date_range(args.date_from, args.date_to)
    cfg = DemoConfig(plant_id=args.plant_id, date_from=d0, date_to=d1)

    db = SessionLocal()
    summary: dict = {"plant_id": cfg.plant_id, "date_from": d0, "date_to": d1}
    try:
        if args.reset:
            delete_plant_xyz(db, cfg.plant_id)
            summary["reset"] = True

        existing = db.query(Plant).filter(Plant.plant_id == cfg.plant_id).first()
        if existing and not args.reset:
            print(f"Plant {cfg.plant_id} already exists — use --reset or delete manually.")
            return 1

        plant_row = build_plant_row(cfg)
        db.add(Plant(**plant_row))
        db.commit()

        arch_rows = build_architecture_rows(cfg)
        db.bulk_save_objects([PlantArchitecture(**r) for r in arch_rows])
        db.commit()

        spec_rows, _ = build_equipment_specs(cfg)
        db.bulk_save_objects([EquipmentSpec(**r) for r in spec_rows])
        db.commit()

        raw_df = build_raw_dataframe(cfg)
        raw_n = bulk_insert_raw(db, cfg.plant_id, raw_df)
        summary["raw_rows_inserted"] = raw_n

        _refresh_plant_stats(db, cfg.plant_id)
        _refresh_equipment_mat(db, cfg.plant_id)

        try:
            from fault_runtime_snapshot import clear_snapshots_for_plant

            clear_snapshots_for_plant(db, cfg.plant_id)
        except Exception:
            pass

        ds_df = build_ds_detection_dataframe(raw_df, cfg)
        if not ds_df.empty:
            run_ds_detection(cfg.plant_id, ds_df, db)
            db.commit()

        ds_confirmed = (
            db.query(func.count())
            .filter(
                FaultDiagnostics.plant_id == cfg.plant_id,
                FaultDiagnostics.fault_status == "CONFIRMED_DS",
            )
            .scalar()
        )
        summary["ds_confirmed_rows"] = int(ds_confirmed or 0)

        stats = db.query(RawDataStats).filter(RawDataStats.plant_id == cfg.plant_id).first()
        summary["raw_data_stats"] = {
            "total_rows": int(stats.total_rows or 0) if stats else 0,
            "min_ts": stats.min_ts if stats else None,
            "max_ts": stats.max_ts if stats else None,
        }

        min_ts = stats.min_ts if stats else f"{d0} 06:00:00"
        max_ts = stats.max_ts if stats else f"{d1} 18:45:00"

        if not args.skip_precompute:
            user = resolve_precompute_user(db)
            if not user:
                summary["precompute"] = {"skipped": True, "reason": "no users in DB"}
            else:
                update_plant_compute_status(db, cfg.plant_id, status="running", date_from=d0, date_to=d1)
                try:
                    pre = compute_snapshots_for_range(db, cfg.plant_id, d0, d1, user)
                    sec = max(1, int((pre.get("total_ms") or 0) / 1000))
                    update_plant_compute_status(
                        db, cfg.plant_id, status="done", date_from=d0, date_to=d1, duration_seconds=sec
                    )
                    summary["precompute"] = pre
                except Exception as exc:
                    db.rollback()
                    update_plant_compute_status(
                        db,
                        cfg.plant_id,
                        status="failed",
                        date_from=d0,
                        date_to=d1,
                        error_message=str(exc)[:4000],
                    )
                    summary["precompute"] = {"error": str(exc)}
                    print(json.dumps(summary, indent=2, default=str))
                    return 1

        from models import UnifiedFeedCategoryTotal

        cats = (
            db.query(UnifiedFeedCategoryTotal.category_id, UnifiedFeedCategoryTotal.fault_count)
            .filter(
                UnifiedFeedCategoryTotal.plant_id == cfg.plant_id,
                UnifiedFeedCategoryTotal.date_from == d0,
                UnifiedFeedCategoryTotal.date_to == d1,
            )
            .all()
        )
        summary["unified_category_counts"] = {c: int(n or 0) for c, n in cats}
        summary["expected"] = expected_counts(cfg)
        print(json.dumps(summary, indent=2, default=str))
        return 0
    except Exception as exc:
        db.rollback()
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
