# Performance & fault pipeline — audit and operator brief (for ChatGPT / handover)

This document summarizes **what was optimized**, **expected performance effects** (with honest limits on measured vs architectural), **which “fault” systems exist and when they run**, and **how to run the full warm-up from Admin**. Paste sections into ChatGPT for context when debugging or extending the platform.

---

## 1. Performance: what improved (architecture)

### 1.1 Precompute-first reads (major)

**Before (conceptual):** `/api/faults/unified-feed`, `/api/faults/ds-summary`, and loss `/bridge` could execute heavy Python + SQL on **every cache miss**, which is painful on **serverless** (cold starts, short timeouts).

**After:** The same payloads can be **stored in PostgreSQL** as JSON snapshots (`unified_fault_snapshot`, `ds_summary_snapshot`, `loss_analysis_snapshot`) plus a **narrow SQL table** `unified_feed_category_totals` for category KPIs. Freshness is tied to `raw_data_stats.updated_at`.

**Expected effect:** First miss or after new raw data may still be slow until a **background job** (`python -m jobs.precompute_runner`) or **admin “full pipeline”** (see §4) completes. Steady-state API latency for those routes should drop to **mostly JSON read + parse** (milliseconds to low tens of ms on a warm DB), versus multi-second compute.

**Optional serverless mode:** `SOLAR_SNAPSHOT_READ_ONLY=1` forces **no** heavy compute on the request path (503 or stale payload). See `backend/docs/PRECOMPUTE_OPERATIONS.md`.

> **Honest note:** We do not ship A/B benchmark numbers in-repo. Treat the above as **expected behavior** from design; measure on your DB with `EXPLAIN`/`EXPLAIN ANALYZE` and `record_compute_ms` logs for your plant sizes.

### 1.2 Dashboard bundle (from earlier roadmap work)

**Conceptual gains** (as implemented in this codebase family): **lite bundle** + **parallel** `/target-generation`, **debounced** date loads, **Redis / in-memory** dashboard cache where configured, **materialized-view** paths where views exist, row limits on heavy series. These reduce **TTFB** and duplicate work on the client.

**Measure locally:** browser Network tab (TTFB vs download), server logs for `dashboard bundle` timings, and `GET /api/dashboard/bundle` with `include_target_generation=0` vs `1`.

### 1.3 Resilience on expected/actual KPIs

**Target generation** (`compute_plant_expected_actual_mwh_for_range` and `_fetch_target_generation_payload`) returns **safe JSON** on failure instead of uncaught 500s; the UI respects `compute_error` and avoids duplicate bridge calls.

---

## 2. “Fault” systems — what runs where (checklist)

| Layer | What it is | When it runs |
|-------|------------|--------------|
| **Disconnected string detection** | `run_ds_detection` in `backend/engine/ds_detection.py` writes **`fault_diagnostics`**. | **Data ingest** (metadata upload path), not the Admin full pipeline below. |
| **Module snapshots** | `compute_snapshots_for_range`: DS summary JSON, unified feed JSON, loss bridge JSON, `unified_feed_category_totals` rows. | **Precompute worker** (`precompute_jobs`) after ingest; **Admin full pipeline** (§4). |
| **Fault Diagnostics “runtime” tabs** | Power limitation, inverter shutdown, grid breakdown, communication, clipping/derating — implemented in `backend/engine/*` and invoked via `routers/faults.py` `_*_tab_with_cache`. | **On demand** when user opens tabs; **warmed** by Admin full pipeline (§4). |
| **Unified feed row expansion** | Heavy path includes DS SCB rows + merges; uses `fault_diagnostics` + unified category math. | Snapshot when precompute/admin run; else on miss. |
| **Placeholder** | `modules/fault_diagnostics/fault_engine.py` (`run_all_faults`) | **Not wired** to production HTTP path (registry only). |

**Admin “Run full fault & snapshot pipeline”** (Performance tab) does **not** re-run DS **detection** on all historical raw rows. It **does** refresh **aggregates and tab engines** for each plant’s derived date range (`resolve_recompute_day_range` from `raw_data_stats`), which is what makes the Fault UI and Loss bridge **fast** after a run.

---

## 3. Verifying that engines “run properly”

1. **After Admin run completes:** open **Fault Diagnostics** for a plant — categories and tabs should load without long spinners; check **Loss Analysis** bridge for the same range.
2. **DB:** `SELECT * FROM unified_feed_category_totals WHERE plant_id='…' LIMIT 20;` — rows should exist after pipeline for that range.
3. **Snapshots:** rows in `unified_fault_snapshot` / `ds_summary_snapshot` with recent `computed_at` **≥** `raw_data_stats.updated_at` (see `is_snapshot_fresh` in `module_snapshots.py`).
4. **Logs / event log:** Admin Performance panel **event log** shows per-plant `OK` lines for: `power_limitation`, `inverter_shutdown`, `grid_breakdown`, `communication_issue`, `clipping_derating`. Failures are logged per engine.

---

## 4. How to run the full pipeline (Admin UI)

1. Log in as **admin**.
2. **Admin → Performance** (⚡ Performance tab on Admin page; lazy-loads `perf_admin.js`).
3. Click **“Run full fault & snapshot pipeline”**.
4. The UI **polls ~every 1.2s** while `running` is true. You get:
   - **Progress %** and **bar**
   - **Plants done / total**
   - **Current plant** and **step** (`module_snapshots` vs `fault_tab_engines`)
   - **Elapsed** and **estimated remaining** (average time per plant × remaining)
   - **Scrollable event log** (last lines of detail)

**API (same as UI):** `POST /api/admin/perf/run-precompute` (name kept for compatibility) — status: `GET /api/admin/perf/precompute-status`.

**Backend implementation:** `backend/routers/perf_monitor.py` — `_run_full_fault_pipeline_background`.

---

## 5. Queue-based precompute (ingest / historical)

- **Durable jobs:** `precompute_jobs` table; **worker:** `python -m jobs.precompute_runner` (cron/scheduler).
- **Enqueue** after successful `raw_data_stats` refresh: `routers/metadata.py` `_post_upload_refresh` → `enqueue_precompute_after_ingest` (if `SOLAR_MODULE_PRECOMPUTE` enabled).
- **Manual repair / backfill:** Admin → **Analytics precompute** tab — `POST /api/admin/precompute/enqueue`.

---

## 6. File index (for developers)

| Area | File |
|------|------|
| Full admin pipeline (thread + progress) | `backend/routers/perf_monitor.py` |
| Module snapshots + category totals | `backend/module_precompute.py`, `backend/module_snapshots.py` |
| Ingest → stats → enqueue | `backend/routers/metadata.py`, `backend/jobs/enqueue.py` |
| Worker | `backend/jobs/precompute_runner.py` |
| DS detection engine | `backend/engine/ds_detection.py` |
| Fault tab engines | `backend/engine/power_limitation.py`, `inverter_shutdown.py`, `grid_breakdown.py`, `communication_issue.py`, `clipping_derating.py` |
| Admin Performance UI | `frontend/js/perf_admin.js` |
| Ops | `backend/docs/PRECOMPUTE_OPERATIONS.md` |

---

## 7. Suggested text for ChatGPT (short)

> Solar Analytics uses PostgreSQL-backed **JSON snapshots** and a **precompute job queue** to avoid heavy fault/unified/loss compute on every HTTP request. **Disconnected-string detection** runs on **ingest** into `fault_diagnostics`. The **Admin Performance** tab can run a **full fault & snapshot pipeline** that, per plant, runs **module precompute** (DS summary, unified feed, loss bridge, category total rows) then **warms five fault tab engines in parallel** (PL, IS, GB, comm, CD). **Snapshot-only** mode exists for serverless. See `docs/PERFORMANCE_AND_FAULT_PIPELINE_AUDIT.md` and `backend/docs/PRECOMPUTE_OPERATIONS.md`.

---

*Last updated: aligned with precompute + perf_monitor full pipeline implementation.*
