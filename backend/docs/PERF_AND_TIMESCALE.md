# Performance baseline, PostgreSQL tuning, and TimescaleDB

This document maps major UI flows to APIs and data stores, lists environment flags for rollout, and describes how to run baseline diagnostics on RDS.

## Screen to API to tables (summary)

| UI area | Key HTTP APIs | Primary tables / artifacts |
|--------|----------------|----------------------------|
| Analytics Lab | `/api/analytics/equipment`, `/signals`, `/timeseries` | `raw_data_generic`, `dc_hierarchy_derived`, `plant_architecture`, optional `solar_raw_data_1m_cagg` (Timescale CAGG) |
| Dashboard bundle | `/api/dashboard/*` | Same telemetry tables + caches |
| Fault Diagnostics | `/api/faults/unified-feed`, `ds-scb-status`, `runtime-tabs-bundle`, tab endpoints | Snapshots (`ds_status_snapshot`, `unified_fault_snapshot`, …), `raw_data_generic`, `fault_diagnostics` |
| Loss Analysis | `/api/loss-analysis/bridge` | `loss_analysis_snapshot`, unified category sources |

## Phase A — Baseline on RDS (no code deploy required)

1. **Index inventory** (compare to `backend/db_perf.py` `_BG_INDEXES`):

   ```bash
   cd backend
   python -m scripts.baseline_perf_queries --inventory-only
   ```

2. **EXPLAIN for Analytics timeseries** (uses `DATABASE_URL` from `backend/.env`):

   ```bash
   python -m scripts.baseline_perf_queries --plant-id YOUR_PLANT --date-from 2026-03-01 --date-to 2026-03-07 --equipment-id INV1 --signals ac_power,dc_current --explain-analyze
   ```

   Paste output into your RCA doc. Look for `Seq Scan` on `raw_data_generic`, high `actual rows`, and buffer hits vs reads.

3. **Infra checklist:** RDS instance class, gp3 IOPS, same region as API, `max_connections`, `statement_timeout`, burst balance (if applicable).

## Phase B — Ensure indexes on the database

Heavy `CREATE INDEX CONCURRENTLY` runs from app boot via `ensure_performance_objects_bg` (`backend/main.py` warmup thread), unless `SOLAR_WARMUP_ON_BOOT=skip`.

**One-shot (maintenance window or bastion):**

```bash
cd backend
python -m scripts.ensure_db_perf
```

This applies fast indexes, then background indexes + `ANALYZE`, matching `backend/db_perf.py`.

## TimescaleDB hosting options

| Option | Notes |
|--------|--------|
| Timescale Cloud | Managed hypertables + CAGGs; set `DATABASE_URL` to the service URL. |
| Self-hosted Postgres + Timescale extension | EC2/ECS/EKS; install `timescaledb` extension before Alembic `20260429_0005`. |
| Amazon RDS | Timescale is available on select RDS Postgres versions; verify regional support before relying on `CREATE EXTENSION timescaledb`. |

**Connectivity:** Vercel serverless should use a **pooler** (PgBouncer) or short-lived connections to the same region as the database. Long analytic queries belong on a worker or long-timeout API, not a 10s cold function, unless connection pooling is configured.

## Hypertable primary key (Timescale requirement)

Many TimescaleDB versions require **unique constraints that include the partitioning column** (`timestamp_ts`). If `create_hypertable` fails with a primary-key error, plan a maintenance window to adjust constraints (example pattern: composite primary key on `(id, timestamp_ts)`), then re-run migration `20260429_0005` or the equivalent SQL from that revision. Always test on a snapshot first.

## `timestamp_ts` column and backfill

Alembic `20260429_0004` adds `raw_data_generic.timestamp_ts` (typed time) plus a trigger to populate it on **INSERT/UPDATE**. Existing rows must be backfilled before hypertable/CAGG:

```bash
cd backend
python -m scripts.backfill_timestamp_ts --batch-size 50000
```

Re-run until it reports 0 rows updated. Then apply `20260429_0005` on a database where `CREATE EXTENSION timescaledb` succeeds.

## Feature flags (rollout / rollback)

| Variable | Purpose |
|----------|---------|
| `SOLAR_ANALYTICS_USE_TIMESCALE_CAGG` | `1` = read minute buckets from `solar_raw_data_1m_cagg` for the primary raw branch when safe; merges with `dc_hierarchy_derived` as before. |
| `SOLAR_ANALYTICS_TS_CACHE_SEC` | Override timeseries in-memory cache TTL (default 600). |
| `SOLAR_ANALYTICS_SCB_ROLLUP_MAX_SCB` | Max mapped SCBs for inverter DC SCB roll-up (default 8000); above this the roll-up branch is skipped to protect the DB. |
| `SOLAR_WARMUP_ON_BOOT` | `skip` disables background index creation at boot. |
| `SOLAR_TIMESCALE_CAGG_LOG_LAG` | `1` = log approximate CAGG refresh lag when Timescale job stats are available. |

**Rollback:** set `SOLAR_ANALYTICS_USE_TIMESCALE_CAGG=0` (or unset). No schema rollback required for reads.

## Ingestion and hypertables

Excel/CSV bulk inserts (`modules/data_setup/uploader.py` and related loaders) continue to write the same `raw_data_generic` columns as before. The trigger from Alembic `20260429_0004` populates `timestamp_ts` on each **INSERT** or **UPDATE** so new telemetry lands in the correct Timescale chunks and can be picked up by the continuous aggregate refresh policy without changing upload code.

## RCA template (one page)

1. **Symptom:** URL, plant, date range, user action (e.g. first open Analytics Lab, 7-day plot).
2. **Network:** slow request name, TTFB vs download, status code.
3. **DB:** `EXPLAIN (ANALYZE, BUFFERS)` excerpt + index list delta vs `db_perf.py`.
4. **Hypothesis:** missing index / seq scan / cross-region / parallel API storm / snapshot miss.
5. **Action:** index DDL, `ANALYZE`, query change, scale instance, enable CAGG path.
