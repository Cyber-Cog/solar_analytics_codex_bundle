"""
Baseline performance: index inventory + optional EXPLAIN for analytics timeseries.

Usage:
  cd backend && python -m scripts.baseline_perf_queries --inventory-only
  python -m scripts.baseline_perf_queries --plant-id PLANT --date-from 2026-03-01 --date-to 2026-03-07 \\
      --equipment-id INV1 --signals ac_power --explain-analyze

Requires DATABASE_URL (PostgreSQL) in environment or backend/.env.
"""

from __future__ import annotations

import argparse
import os
import sys

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BACKEND not in sys.path:
    sys.path.insert(0, BACKEND)

# Load .env like main app
_env = os.path.join(BACKEND, ".env")
if os.path.isfile(_env):
    for line in open(_env, encoding="utf-8", errors="ignore"):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            if k.strip() not in os.environ:
                os.environ[k.strip()] = v.strip()


def _engine():
    from sqlalchemy import create_engine

    url = os.environ.get("DATABASE_URL", "").strip()
    if not url.startswith("postgres"):
        raise SystemExit("DATABASE_URL must be set to a postgresql URL")
    return create_engine(url, pool_pre_ping=True)


def inventory_only(conn) -> None:
    from sqlalchemy import text

    q = text(
        """
        SELECT tablename, indexname, indexdef
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND tablename IN (
            'raw_data_generic',
            'dc_hierarchy_derived',
            'fault_diagnostics',
            'fault_runtime_snapshot',
            'ds_status_snapshot'
          )
        ORDER BY tablename, indexname
        """
    )
    rows = conn.execute(q).fetchall()
    print(f"-- Found {len(rows)} indexes on core tables --")
    for r in rows:
        print(f"\n-- {r[0]} / {r[1]}\n{r[2]};")

    q2 = text(
        """
        SELECT extname, extversion FROM pg_extension
        WHERE extname IN ('timescaledb', 'pg_stat_statements')
        ORDER BY 1
        """
    )
    ex = conn.execute(q2).fetchall()
    print("\n-- Extensions --")
    for e in ex:
        print(f"  {e[0]} {e[1]}")


def build_timeseries_explain_sql(
    plant_id: str,
    date_from: str,
    date_to: str,
    equipment_ids: list[str],
    signals: list[str],
) -> str:
    """SQL shape aligned with routers/analytics.py get_timeseries (raw branch + dc_hierarchy union)."""
    ids = ",".join("'" + str(i).replace("'", "''") + "'" for i in equipment_ids)
    sigs = ",".join("'" + str(s).replace("'", "''") + "'" for s in signals)
    from_ts = f"{date_from} 00:00:00"
    to_ts = f"{date_to} 23:59:59"
    return f"""
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT DATE_TRUNC('minute', CAST("timestamp" AS TIMESTAMP)) AS ts,
       equipment_id, signal, AVG(value) AS value, 1 AS precedence
FROM raw_data_generic
WHERE plant_id = '{plant_id.replace("'", "''")}'
  AND equipment_id IN ({ids})
  AND signal IN ({sigs})
  AND "timestamp" BETWEEN '{from_ts}' AND '{to_ts}'
GROUP BY 1, 2, 3
UNION ALL
SELECT DATE_TRUNC('minute', CAST("timestamp" AS TIMESTAMP)),
       equipment_id, signal, AVG(value), 2
FROM dc_hierarchy_derived
WHERE plant_id = '{plant_id.replace("'", "''")}'
  AND equipment_id IN ({ids})
  AND signal IN ({sigs})
  AND "timestamp" BETWEEN '{from_ts}' AND '{to_ts}'
GROUP BY 1, 2, 3
ORDER BY ts, equipment_id, precedence
LIMIT 100000;
"""


def main() -> None:
    p = argparse.ArgumentParser(description="DB perf baseline: indexes + EXPLAIN")
    p.add_argument("--inventory-only", action="store_true", help="Only list indexes on core tables")
    p.add_argument("--plant-id", default="", help="Plant id for EXPLAIN")
    p.add_argument("--date-from", default="", help="YYYY-MM-DD")
    p.add_argument("--date-to", default="", help="YYYY-MM-DD")
    p.add_argument("--equipment-id", default="", help="Single equipment id (comma for multiple)")
    p.add_argument("--signals", default="ac_power", help="Comma-separated signals")
    p.add_argument("--explain-analyze", action="store_true", help="Run EXPLAIN ANALYZE (writes to DB)")
    args = p.parse_args()

    eng = _engine()
    from sqlalchemy import text

    with eng.connect() as conn:
        inventory_only(conn)
        if args.inventory_only:
            return
        if not args.explain_analyze:
            return

        if not args.plant_id or not args.date_from or not args.date_to:
            print("For EXPLAIN, pass --plant-id --date-from --date-to --equipment-id", file=sys.stderr)
            return

        eqs = [x.strip() for x in args.equipment_id.split(",") if x.strip()]
        if not eqs:
            print("Need --equipment-id", file=sys.stderr)
            sys.exit(1)
        sigs = [x.strip() for x in args.signals.split(",") if x.strip()]
        sql = build_timeseries_explain_sql(args.plant_id, args.date_from, args.date_to, eqs, sigs)
        print("\n-- Running EXPLAIN ANALYZE (may take minutes on large data) --\n")
        for row in conn.execute(text(sql)):
            print(row[0])


if __name__ == "__main__":
    main()
