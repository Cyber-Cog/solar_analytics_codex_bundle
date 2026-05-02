# TimescaleDB Migration Runbook

## Why TimescaleDB?

The core performance bottleneck on Analytics Lab (2-3 min load for 7-day 1-min data) is
the `UNION ALL + GROUP BY` query pattern over millions of rows in `raw_data_generic`.
TimescaleDB continuous aggregates (CAGGs) pre-materialise per-minute rollups, reducing
query time from minutes to < 2 seconds.

---

## Phase 4A — Move DB to TimescaleDB-capable host

### Option 1: Timescale Cloud (recommended, minimal ops)

1. Create a Timescale Cloud project at https://console.cloud.timescale.com
   - Region: eu-north-1 to match current RDS (minimise cross-AZ latency to FastAPI on Vercel)
   - Plan: "Dynamic Storage" starts at ~$30/month for 10 GB
2. Get connection string in the form:
   `postgresql://tsdbadmin:<password>@<project>.tsdb.cloud:5432/<dbname>?sslmode=require`
3. Run a full `pg_dump` from the RDS instance:
   ```bash
   pg_dump "$OLD_DATABASE_URL" \
     --no-owner --no-acl \
     -f solar_dump.sql
   ```
4. Restore to Timescale Cloud:
   ```bash
   psql "$NEW_DATABASE_URL" -f solar_dump.sql
   ```
5. Update `DATABASE_URL` environment variable on Vercel / backend host.

### Option 2: EC2 with Timescale extension

1. Launch EC2 t3.medium with Ubuntu 22.04.
2. Install PostgreSQL 15 + TimescaleDB:
   ```bash
   sudo apt install -y timescaledb-2-postgresql-15
   sudo timescaledb-tune --quiet --yes
   sudo systemctl restart postgresql
   ```
3. Create DB, restore dump as above.
4. Update security groups to allow FastAPI → 5432.

---

## Phase 4B — Run the Timescale migration

After confirming the new DB is up and `DATABASE_URL` updated:

```bash
cd backend
python -m alembic upgrade head   # applies 0005 if TimescaleDB is present
```

The migration `20260429_0005_timescale_hypertable_cagg.py` is idempotent:
- If TimescaleDB is not installed → no-op (skipped).
- If `timestamp_ts` column is not populated → no-op (backfill first).

### Backfill `timestamp_ts` (one-time, for existing data)

```bash
python scripts/backfill_timestamp_ts.py
```

This is a batched UPDATE; safe to re-run.

### Verify hypertable and CAGG

```sql
-- Check hypertable
SELECT hypertable_name, num_chunks
FROM timescaledb_information.hypertables
WHERE hypertable_name = 'raw_data_generic';

-- Check CAGG exists
SELECT view_name, materialization_hypertable_name
FROM timescaledb_information.continuous_aggregates;

-- Check CAGG lag
SELECT view_name, last_run_started_at, last_run_duration
FROM timescaledb_information.job_stats
JOIN timescaledb_information.continuous_aggregate_stats USING (job_id);
```

---

## Phase 4C — Enable CAGG read path in FastAPI

Set the following environment variable on your backend/Vercel deployment:

```
SOLAR_ANALYTICS_USE_TIMESCALE_CAGG=1
SOLAR_ANALYTICS_TS_CACHE_SEC=120
```

This activates the `analytics_timescale.fetch_cagg_minute_rows()` code path in
`backend/routers/analytics.py` instead of the raw UNION-ALL query.

Expected result: Analytics Lab 7-day query drops from ~2 minutes to < 3 seconds.

---

## Phase 4D — Deploy the Next.js frontend

### Development

```bash
cd frontend-next
npm install
npm run dev   # starts on http://localhost:3000
```

Set `NEXT_PUBLIC_API_URL=http://localhost:8000` in `.env.local`.

### Production (Vercel)

1. Import the `frontend-next/` folder as a new Vercel project.
2. Set environment variables:
   - `NEXT_PUBLIC_API_URL=https://your-api.vercel.app`
   - `NEXTAUTH_SECRET=<random 32-char string>`
   - `NEXTAUTH_URL=https://your-frontend.vercel.app`
3. Deploy.

### Switch backends to Next.js-only mode

Once the Next.js frontend is live, disable the legacy static frontend in FastAPI:

```
SOLAR_USE_NEXTJS_FRONTEND=1
```

This env var causes FastAPI to skip mounting `frontend/` as static files.

---

## Environment Variables Reference

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | — | PostgreSQL connection string |
| `SOLAR_ANALYTICS_USE_TIMESCALE_CAGG` | `0` | Enable CAGG read path for Analytics Lab |
| `SOLAR_ANALYTICS_TS_CACHE_SEC` | `120` | Analytics API cache TTL (seconds) |
| `SOLAR_ANALYTICS_SCB_ROLLUP_MAX_SCB` | `150` | Max SCBs per Analytics query |
| `SOLAR_TIMESCALE_CAGG_LOG_LAG` | `0` | Log CAGG refresh lag in API startup |
| `SOLAR_SNAPSHOT_READ_ONLY` | `1` | Dashboard reads from snapshots |
| `SOLAR_SNAPSHOT_ALLOW_STALE` | `1` | Allow stale snapshots (skip live compute) |
| `SOLAR_WARMUP_ON_BOOT` | `0` | Pre-warm snapshot cache on startup |
| `SOLAR_USE_NEXTJS_FRONTEND` | `0` | Disable legacy static frontend mount |
| `SOLAR_NEXTJS_URL` | — | Next.js production URL (added to CORS) |
| `DS_IRRADIANCE_MIN` | `150` | Minimum irradiance for DS detection |
| `DS_CURRENT_DROP_PCT` | `0.30` | Current drop threshold (30 %) |
| `DS_PERSISTENCE_MINUTES` | `30` | Wall-clock persistence for DS confirmation |
