# Structured Architecture for Fast Data Processing

This document outlines a path to make Solar Analytics data processing faster and more scalable.

## Current State

- **SQLite** (or **PostgreSQL** via `DATABASE_URL`): single DB for metadata, raw time-series, and fault results.
- **15‑minute cache** (`raw_data_15m`): SQLite-only aggregation to reduce query load; not used with PostgreSQL yet.
- **Fault result cache** (`fault_cache` table): DS summary and inverter-efficiency API responses are stored by key (plant + date range). Repeat requests are served from cache (TTL 10–15 min). Cache is invalidated when new fault diagnostics are written (e.g. after raw data upload and DS detection), so the next fetch is fresh.
- All analytics (KPIs, charts, faults) query the same DB; heavy date-range queries can be slow on large datasets.

## Recommended Directions for Speed

### 1. PostgreSQL as primary (done)

- Use **PostgreSQL** in production for concurrency, indexing, and scalability.
- Set `DATABASE_URL=postgresql://...` and run the app; with PG, queries hit `raw_data_generic` only (no 15m cache table yet).

### 2. Indexing

- Ensure indexes exist on:
  - `(plant_id, timestamp)`, `(plant_id, equipment_level, signal, timestamp)` on `raw_data_generic`.
  - Same pattern on `fault_diagnostics`.
- These are created in `db_perf.ensure_performance_objects` for SQLite; add equivalent `CREATE INDEX` for PostgreSQL if you create tables manually, or run the same logic with PG‑compatible DDL.

### 3. Pre-aggregated / materialized data (next step)

- **Materialized views** (PostgreSQL): e.g. daily energy per plant, daily KPIs, so the dashboard does not recompute from raw rows every time.
- **Refresh strategy**: refresh after new data upload (e.g. trigger or job after Metadata upload / NTPC import).
- **15m cache on PostgreSQL**: implement a PG‑compatible version of `raw_data_15m` (table + `refresh_15m_cache` using `date_trunc` and `ON CONFLICT` instead of SQLite `INSERT OR REPLACE`).

### 4. Time-series / analytics DB (optional, for very large data)

- For very large volumes (e.g. 1‑min data, many plants, years):
  - Consider **TimescaleDB** (PostgreSQL extension) or **InfluxDB** for raw time-series.
  - Keep **PostgreSQL** for metadata, users, plants, and fault results; sync or aggregate from the time-series store into PG for dashboards.

### 5. Caching layer (optional)

- **Redis** (or in-memory cache): cache KPI responses and heavy chart payloads per `(plant_id, date_from, date_to)` with a short TTL (e.g. 5–15 minutes).
- Invalidate on new data upload for that plant.

### 6. ETL / background jobs

- Move heavy computations (e.g. DS detection, inverter efficiency, daily rollups) to **background tasks** (Celery, RQ, or FastAPI background tasks) so uploads return quickly and the UI stays responsive.
- Write results to PostgreSQL; API only reads.

### 7. API and frontend

- Keep **FastAPI** as the single API; it can read from PostgreSQL, cache, and (later) from a time-series DB or materialized views.
- **Frontend**: already uses the same API; no change needed for DB or caching.

## Suggested order of implementation

1. **PostgreSQL + indexes** (in place / complete).
2. **PG‑compatible 15m cache or materialized daily view** for dashboard KPIs and energy charts.
3. **Response caching** (Redis or in-memory) for KPI and chart endpoints.
4. **Background jobs** for DS detection and heavy analytics after upload.
5. **Time-series DB** only if raw data size justifies it.

This gives a structured path from “single SQLite/PostgreSQL” to “fast, scalable data processing” without rewriting the whole app at once.
