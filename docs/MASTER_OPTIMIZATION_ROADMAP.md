# Solar Analytics — Master Optimization Roadmap

**Status:** planning only — no application code has been changed under this document.  
**Scope:** Converges prior Phase A / B / C recommendations into **Wave 1–3** with controlled execution.

---

## Execution principles (zero chaos)

1. **One wave at a time** — merge and deploy Wave 1 fully before starting Wave 2, unless a task is explicitly marked safe to parallelize.
2. **Measure before/after** for bundle latency (p50/p95), pool wait (if exposed), and error rate; keep a **rollback** story per task (revert commit or flip env).
3. **Parity first for query changes** — any change that replaces raw-table integration with MV-based SQL must be validated on **sample plants and date ranges** (numeric tolerances where approximations exist).
4. **Config before behavior** — prefer env-based tuning (timeouts, `REDIS_URL`, pool settings) in Wave 1 where it de-risks later waves.

---

## Wave 1 — Safest, highest ROI

| ID | Task | Files impacted (primary) | Est. speed gain | Risk | Rollback difficulty | Dependencies |
|----|------|-------------------------|-----------------|------|---------------------|--------------|
| W1.1 | **Use `ReadSessionLocal` in `dashboard_bundle` thread workers** (read-only work should use the read pool) | [`backend/routers/dashboard.py`](../backend/routers/dashboard.py) (imports from [`backend/database.py`](../backend/database.py), replace `SessionLocal()` in `_q_*` closings with `ReadSessionLocal()`) | **Medium** on self-hosted (isolates read vs write pool); **Low** on serverless (both pools use same small config today, but future-proofs) | **Low** | **Easy** — single-file revert | None; must keep write `SessionLocal` only if any sub-task mutates (none today in bundle) |
| W1.2 | **Align `dashboard_bundle` sub-queries with MV + fallback used by standalone GETs** | [`backend/routers/dashboard.py`](../backend/routers/dashboard.py); [`backend/dashboard_mv_sql.py`](../backend/dashboard_mv_sql.py); shared helpers in [`backend/dashboard_helpers.py`](../backend/dashboard_helpers.py) if row finalization is reused; possibly [`backend/ac_power_energy_sql.py`](../backend/ac_power_energy_sql.py) for energy branch | **High** when `mv_inverter_power_1min` (and weather MV for GTI) exist | **Low–medium** (response JSON must match consumer) | **Medium** — revert or feature-flag branch | MVs must exist in target DB; optional staging toggle |
| W1.3 | **Remove / reduce serverless pool vs. 6× concurrent sessions mismatch** (see options below) | [`backend/database.py`](../backend/database.py) (env keys for `pool_size` / `max_overflow` on serverless); or [`backend/routers/dashboard.py`](../backend/routers/dashboard.py) (reduce `max_workers` / merge query batches); [`backend/.env.example`](../backend/.env.example) | **High** on Vercel under concurrent users | **Low–medium** (watch total DB connections) | **Easy** (env) to **medium** (code structure) | Know Postgres `max_connections` and Vercel concurrency; may combine with W1.2 to reduce **duration** of hold |
| W1.4 | **Operationalize `REDIS_URL` for dashboard cache in serverless** | [`backend/dashboard_cache.py`](../backend/dashboard_cache.py) (already supports Redis); deployment secrets / [../vercel.json](../vercel.json) is **not** the secret store — use host env UI; [`.env.example`](../backend/.env.example) documentation | **High** for repeat traffic across cold starts | **Low** (fallback to in-proc already coded) | **Easy** — unset `REDIS_URL` | Redis instance reachable from serverless; network ACLs |
| W1.5 | **Client: bounded retry for `Dashboard.bundle` + stop silent blank cells** | [`frontend/js/pages.js`](../frontend/js/pages.js), [`frontend/js/api.js`](../frontend/js/api.js) (optional `apiFetch` retry helper) | **Medium** (perceived reliability / fewer full-page blanks) | **Low** (limit retries, backoff) | **Easy** | None |
| W1.6 | **Document / set `DB_STATEMENT_TIMEOUT_MS` for production** (aligned with Vercel `maxDuration`) | [`backend/.env.example`](../backend/.env.example); host env; [../vercel.json](../vercel.json) shows `maxDuration: 60` for app | **Variable** (fewer “mystery” cancels or fewer hung requests) | **Low** if raised cautiously; **medium** if too high vs function budget | **Easy** (env) | W1.2 — shorter queries may allow **lower** effective need for long timeouts |

**W1.3 options (pick one after measurement):**

- **3a** — Increase `pool_size` / `max_overflow` under `SOLAR_SERVERLESS` in [`database.py`](../backend/database.py) *only* if DB `max_connections` and provider limits allow.
- **3b** — Reduce `ThreadPoolExecutor(max_workers=6)` to match pool budget (e.g. 3) and/or **stage** work in two waves inside one request.
- **3c** — **Best long-term** with W1.2: fewer, faster queries may reduce concurrent sessions naturally.

---

## Wave 2 — Medium-risk data / query improvements

| ID | Task | Files impacted | Est. speed gain | Risk | Rollback difficulty | Dependencies |
|----|------|----------------|-----------------|------|---------------------|--------------|
| W2.1 | **MV-backed plant AC totals for `/kpis` and bundle KPI block** (same definition as product expects: energy from integration vs MV sum) | [`backend/routers/dashboard.py`](../backend/routers/dashboard.py); new or extended SQL in [`backend/dashboard_mv_sql.py`](../backend/dashboard_mv_sql.py) or [`backend/ac_power_energy_sql.py`](../backend/ac_power_energy_sql.py) | **High** for KPI path when MV present | **Medium** (numeric parity) | **Medium** — keep old function behind flag | `mv_inverter_power_1min` refreshed; W1.2 done or coordinated |
| W2.2 | **Unify WMS/insolation and KPI assembly** so the same data sources are used in bundle and `/wms-kpis` / `_wms_tilt_insolation_kwh_m2` (avoid duplicate heavy pulls where possible) | [`backend/routers/dashboard.py`](../backend/routers/dashboard.py); possibly [`backend/db_perf.py`](../backend/db_perf.py) `choose_data_table` | **Medium** | **Medium** | **Medium** | W1.2 |
| W2.3 | **Structured error surface for dashboard** — return partial 200 with `meta.errors` *or* clear 5xx with JSON; avoid empty KPI object on timeout | [`backend/routers/dashboard.py`](../backend/routers/dashboard.py), FastAPI exception handlers in [`backend/main.py`](../backend/main.py) if needed; [`frontend/js/pages.js`](../frontend/js/pages.js) | **UX / debug**; indirect speed (less “retry storms”) | **Medium** (API contract) | **Medium** | W1.5 |
| W2.4 | **Add automated parity checks** (script or pytest) for bundle vs decomposed GETs for fixed plant+range | `tests/` (new or existing), samples under `backend/scripts/` | N/A (safety) | **Low** | **Easy** | W1.2, W2.1 as applicable |

---

## Wave 3 — Advanced infra / database tuning

| ID | Task | Files impacted | Est. speed gain | Risk | Rollback difficulty | Dependencies |
|----|------|----------------|-----------------|------|---------------------|--------------|
| W3.1 | **Index / statistics review** on `raw_data_generic` and `dc_hierarchy_derived` for hot filters (`plant_id`, `timestamp`, `signal`, `equipment_level`) | Alembic or [`backend/migrations/`](../backend/migrations/) as used by the project; [`backend/db_perf.py`](../backend/db_perf.py) background index path if applicable | **High** when MVs missing/stale; **low** when MV path dominates | **Medium–high** (write amplification, migration lock time) | **Harder** (DROP INDEX) | Staging + `EXPLAIN`; maintenance window for large tables |
| W3.2 | **Materialized view refresh policy** (schedule, lag SLA, post-upload invalidation) | [`backend/db_perf.py`](../backend/db_perf.py) `refresh_15m_cache` and callers; any [`backend/jobs/`](../backend/jobs/); deploy cron / hosted scheduler | **Medium–high** (fresher MVs) | **Medium** (ops) | **Medium** | V3.1 only if raw fallback remains common |
| W3.3 | **Connection budget model** (serverless invocations × pool × replicas) + document caps | Runbooks, [`backend/.env.example`](../backend/.env.example), infra provider console | **Prevents outages**; indirect | **Low** (doc) to **high** (wrong math) | **Easy** (doc) | W1.3, W1.4 |
| W3.4 | **Vercel `maxDuration` vs `DB_STATEMENT_TIMEOUT_MS` and FastAPI timeout alignment** | [`vercel.json`](../vercel.json), [`app.py`](../app.py) or entry, [`backend/database.py`](../backend/database.py) | **Avoids** hung/ambiguous failures | **Medium** | **Easy** (config) | W1.6, W2.3 |
| W3.5 | **(Optional) HTTP edge caching** for **non-user-specific** public assets only** | Already partially in [`backend/main.py`](../backend/main.py) for static; **do not** blindly cache `/api/*` with auth | **Low** for API; keep scope narrow | **High** for `/api` mistakes | N/A if skipped | **Not recommended** for authenticated dashboard JSON without dedicated design |

---

## Recommended implementation order (zero chaos)

1. **W1.4** — `REDIS_URL` in prod (if not already) — instant cross-invocation cache hits; easy rollback.  
2. **W1.1** — `ReadSessionLocal` in bundle workers — small diff, clear ownership of read pool.  
3. **W1.6** — set explicit `DB_STATEMENT_TIMEOUT_MS` in prod to match your function budget; document.  
4. **W1.2** — bundle uses same MV fast paths as GET endpoints — **largest** query win; deploy with monitoring.  
5. **W1.3** — tune pool *or* workers after W1.2 shows actual concurrency/ duration (measure, then choose 3a/3b/3c).  
6. **W1.5** — client retry + non-silent error handling.  
7. **W2.4** then **W2.1** — parity harness, then MV-backed KPI totals.  
8. **W2.2** / **W2.3** — dedupe WMS/insolation pulls and improve API error model.  
9. **W3.1** → **W3.2** — indexes and refresh discipline for raw/MV fallbacks.  
10. **W3.3** / **W3.4** — harden ops and timeout alignment.  
11. **W3.5** — only if a **separate** product decision justifies it; default **skip**.

**Why this order:** Fix **shared cache and read-pool usage** first (cheap, low risk), then **largest server-side win** (bundle/MV alignment), then **tune the pool to measured reality** (avoids over-provisioning). Query parity (**W2.4**) before aggressive KPI changes (**W2.1**). Index work (**W3.1**) last among dev tasks when you know which paths still hit raw tables in production.

---

## Summary: waves at a glance

| Wave | Theme | When it is “done” |
|------|--------|-------------------|
| **1** | Cache ops, read pool usage, bundle≈GET MV paths, pool/worker balance, client resilience, timeout policy | Dashboard p95 down materially on warm + cold; fewer timeouts; no regression in JSON shape |
| **2** | MV-backed KPIs, deduped WMS, API error clarity, tests | Parity tests green; KPIs consistent with chart data |
| **3** | Indexes, refresh SLO, capacity math, platform timeouts | Raw fallback acceptable; DB healthy under peak |

---

*This document is the master execution plan. Implementation should proceed in order above after explicit go-ahead to change code; each W* task should be a **separate small PR** where possible for clean rollback.*
