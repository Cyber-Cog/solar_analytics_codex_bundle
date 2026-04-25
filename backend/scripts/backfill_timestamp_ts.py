"""
Backfill raw_data_generic.timestamp_ts in batches (safe for large tables).

  cd backend && python -m scripts.backfill_timestamp_ts --batch-size 50000

Requires column timestamp_ts (Alembic 20260429_0004).
"""

from __future__ import annotations

import argparse
import os
import sys
import time

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BACKEND not in sys.path:
    sys.path.insert(0, BACKEND)

_env = os.path.join(BACKEND, ".env")
if os.path.isfile(_env):
    for line in open(_env, encoding="utf-8", errors="ignore"):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            if k.strip() not in os.environ:
                os.environ[k.strip()] = v.strip()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--batch-size", type=int, default=50_000)
    p.add_argument("--plant-id", default="", help="Optional filter")
    args = p.parse_args()

    from sqlalchemy import create_engine, text

    url = os.environ.get("DATABASE_URL", "").strip()
    if not url.startswith("postgres"):
        sys.exit("DATABASE_URL must be PostgreSQL")
    eng = create_engine(url, pool_pre_ping=True)

    plant_filter = ""
    params: dict = {"lim": args.batch_size}
    if args.plant_id:
        plant_filter = "AND plant_id = :pid"
        params["pid"] = args.plant_id

    sql = text(
        f"""
        WITH cte AS (
          SELECT ctid FROM raw_data_generic
          WHERE timestamp_ts IS NULL
            AND "timestamp" IS NOT NULL
            AND trim(cast("timestamp" as text)) <> ''
            {plant_filter}
          LIMIT :lim
        )
        UPDATE raw_data_generic r
        SET timestamp_ts = trim(cast(r."timestamp" as text))::timestamptz
        FROM cte
        WHERE r.ctid = cte.ctid
        """
    )

    total = 0
    while True:
        t0 = time.perf_counter()
        with eng.begin() as conn:
            res = conn.execute(sql, params)
            n = res.rowcount or 0
        total += n
        dt = time.perf_counter() - t0
        print(f"updated {n} rows in {dt:.1f}s (total {total})")
        if n == 0:
            break


if __name__ == "__main__":
    main()
