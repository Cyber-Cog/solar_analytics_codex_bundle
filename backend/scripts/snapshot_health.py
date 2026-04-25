"""
Read-only snapshot/data freshness report.

Run from repo root or backend directory:
    python backend/scripts/snapshot_health.py

Use --repair-stats to update raw_data_stats from raw_data_generic before
reporting. That scan can take tens of seconds on large plants.
"""

from __future__ import annotations

import argparse
import os
import sys

from sqlalchemy import text

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BACKEND not in sys.path:
    sys.path.insert(0, BACKEND)


def _load_env() -> None:
    env_path = os.path.join(BACKEND, ".env")
    if not os.path.isfile(env_path):
        return
    with open(env_path, encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())


def main() -> int:
    parser = argparse.ArgumentParser(description="Report raw stats and snapshot freshness.")
    parser.add_argument("--plant-id", default="", help="Optional plant_id filter")
    parser.add_argument("--repair-stats", action="store_true", help="Refresh raw_data_stats first")
    args = parser.parse_args()

    _load_env()
    from database import SessionLocal
    from module_precompute import validate_or_refresh_raw_data_stats

    db = SessionLocal()
    try:
        plant_filter = "WHERE plant_id = :p" if args.plant_id else ""
        params = {"p": args.plant_id} if args.plant_id else {}

        if args.repair_stats:
            plants = db.execute(
                text(f"SELECT DISTINCT plant_id FROM raw_data_generic {plant_filter} ORDER BY plant_id"),
                params,
            ).fetchall()
            for row in plants:
                validate_or_refresh_raw_data_stats(db, str(row[0]))

        print("\nRAW STATS VS RAW TABLE")
        rows = db.execute(
            text(
                f"""
                WITH actual AS (
                  SELECT plant_id, COUNT(*)::bigint AS actual_rows,
                         MIN(timestamp)::text AS actual_min_ts,
                         MAX(timestamp)::text AS actual_max_ts
                    FROM raw_data_generic
                    {plant_filter}
                   GROUP BY plant_id
                )
                SELECT a.plant_id,
                       s.total_rows AS stats_rows,
                       a.actual_rows,
                       s.min_ts AS stats_min_ts,
                       a.actual_min_ts,
                       s.max_ts AS stats_max_ts,
                       a.actual_max_ts,
                       s.updated_at,
                       (COALESCE(s.total_rows, -1) = a.actual_rows
                        AND COALESCE(s.min_ts, '') = COALESCE(a.actual_min_ts, '')
                        AND COALESCE(s.max_ts, '') = COALESCE(a.actual_max_ts, '')) AS stats_match
                  FROM actual a
                  LEFT JOIN raw_data_stats s ON s.plant_id = a.plant_id
                 ORDER BY a.plant_id
                """
            ),
            params,
        ).fetchall()
        for r in rows:
            print(dict(r._mapping))

        print("\nSNAPSHOT SUMMARY")
        snap_params = params
        snap_filter = "WHERE plant_id = :p" if args.plant_id else ""
        rows = db.execute(
            text(
                f"""
                SELECT kind, plant_id, COUNT(*) AS snapshots,
                       MIN(date_from) AS min_from,
                       MAX(date_to) AS max_to,
                       MAX(computed_at) AS newest
                FROM (
                  SELECT 'ds_summary' kind, plant_id, date_from, date_to, computed_at FROM ds_summary_snapshot
                  UNION ALL
                  SELECT 'ds_status' kind, plant_id, date_from, date_to, computed_at FROM ds_status_snapshot
                  UNION ALL
                  SELECT 'unified' kind, plant_id, date_from, date_to, computed_at FROM unified_fault_snapshot
                  UNION ALL
                  SELECT 'loss' kind, plant_id, date_from, date_to, computed_at FROM loss_analysis_snapshot
                  UNION ALL
                  SELECT 'runtime:' || kind, plant_id, date_from, date_to, updated_at FROM fault_runtime_snapshot
                ) s
                {snap_filter}
                GROUP BY kind, plant_id
                ORDER BY plant_id, kind
                """
            ),
            snap_params,
        ).fetchall()
        for r in rows:
            print(dict(r._mapping))

        print("\nSTALE SNAPSHOTS")
        rows = db.execute(
            text(
                f"""
                SELECT s.kind, s.plant_id, s.date_from, s.date_to, s.computed_at, r.updated_at AS raw_stats_updated_at
                  FROM (
                    SELECT 'ds_summary' kind, plant_id, date_from, date_to, computed_at FROM ds_summary_snapshot
                    UNION ALL
                    SELECT 'ds_status' kind, plant_id, date_from, date_to, computed_at FROM ds_status_snapshot
                    UNION ALL
                    SELECT 'unified' kind, plant_id, date_from, date_to, computed_at FROM unified_fault_snapshot
                    UNION ALL
                    SELECT 'loss' kind, plant_id, date_from, date_to, computed_at FROM loss_analysis_snapshot
                    UNION ALL
                    SELECT 'runtime:' || kind, plant_id, date_from, date_to, updated_at FROM fault_runtime_snapshot
                  ) s
                  JOIN raw_data_stats r ON r.plant_id = s.plant_id
                 WHERE s.computed_at < r.updated_at
                   {"AND s.plant_id = :p" if args.plant_id else ""}
                 ORDER BY s.plant_id, s.kind, s.date_from, s.date_to
                 LIMIT 100
                """
            ),
            params,
        ).fetchall()
        for r in rows:
            print(dict(r._mapping))
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
