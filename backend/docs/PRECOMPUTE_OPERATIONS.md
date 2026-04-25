# Precompute operations — static fault / analytics reads

This document describes how heavy fault and loss aggregates are computed **once** (background job), stored in DB tables, and served quickly to the API. It complements the implementation in [`module_precompute.py`](../module_precompute.py) and [`module_snapshots.py`](../module_snapshots.py).

## Data flow (ingest → anchor → queue → worker)

1. **Raw data upload** (Metadata router) finishes and schedules [`_post_upload_refresh`](../routers/metadata.py) in a background task.
2. **`_refresh_plant_stats`** recomputes [`raw_data_stats`](../models.py) for the plant and sets **`updated_at`** explicitly. This timestamp is the **freshness anchor** for module snapshots: a snapshot is valid only if `snapshot.computed_at >= raw_data_stats.updated_at` (see `is_snapshot_fresh` in `module_snapshots.py`).
3. **Enqueue** runs only if stats refresh **succeeded**: `enqueue_precompute_after_ingest` inserts or merges a row in **`precompute_jobs`** (pending) for the derived date range (`resolve_recompute_day_range`).
4. **Worker** (`python -m jobs.precompute_runner`) claims jobs, runs `compute_snapshots_for_range`, which UPSERTs:
   - `ds_summary_snapshot`
   - `unified_fault_snapshot` (JSON: categories, rows, totals)
   - `unified_feed_category_totals` (narrow rows per `category_id` for SQL/BI)
   - `loss_analysis_snapshot` (plant-scope loss bridge from precompute path)

**Gap closed in code:** precompute was previously enqueued even when `raw_data_stats` failed to update, which could leave snapshots misaligned with the anchor. Enqueue now runs only after a successful stats refresh.

## Production scheduling

- Run the worker on a **schedule** (cron, systemd timer, Kubernetes CronJob, ECS scheduled task), not only manually:

  ```bash
  cd backend && python -m jobs.precompute_runner --once --max-jobs 20
  ```

- Keep running until **`GET /api/admin/precompute/queue`** shows `pending: 0` after large ingests.
- Tune **`SOLAR_PRECOMPUTE_STALE_LOCK_MINUTES`** (see `precompute_runner.py`) so stuck `running` jobs reset to `pending`.

## Failure monitoring (alerts)

| Signal | Action |
|--------|--------|
| `precompute_jobs.pending` growing without bound | Scale workers or check DB/CPU; inspect `recent_jobs[].error_message` |
| Jobs stuck in `running` | Stale-lock reset runs on worker start; reduce job size or increase `chunk_days` split via admin enqueue |
| API **503** `snapshot_unavailable` with `SOLAR_SNAPSHOT_READ_ONLY=1` | Run worker or enqueue jobs; ensure `SOLAR_MODULE_PRECOMPUTE` is not `0` on ingest |
| Snapshots always “stale” for a plant | Confirm `raw_data_stats.updated_at` advances on upload; confirm worker completes without error |

## Environment variables

| Variable | Meaning |
|----------|---------|
| `SOLAR_MODULE_PRECOMPUTE` | `0` disables enqueue after ingest (default `1`). |
| `SOLAR_PRECOMPUTE_MAX_SPAN_DAYS` | Caps date range per `resolve_recompute_day_range` (default 366). |
| `SOLAR_SNAPSHOT_READ_ONLY` | `1` = `/api/faults/ds-summary`, `/api/faults/ds-scb-status`, `/api/faults/unified-feed`, and loss `/bridge` **do not** run heavy compute on miss; they return **503** or a **stale** payload (see below). Use on serverless (e.g. Vercel) to avoid timeouts. |
| `SOLAR_SNAPSHOT_ALLOW_STALE` | When read-only: `1` (default) returns the last snapshot with `_snapshot_meta.stale` if the anchor is newer than `computed_at`; `0` returns **503** instead. |
| `SNAP_RETENTION_DAYS` | Old snapshot rows (including `unified_feed_category_totals`) pruned by `apply_snapshot_retention`. |

## Normalized category totals (SQL reporting)

Table **`unified_feed_category_totals`** (see migration `20260423_0002`) holds one row per `(plant_id, date_from, date_to, category_id)` with `loss_mwh` and `fault_count`, written whenever the unified JSON snapshot is saved (precompute or first HTTP miss). Full detail rows remain **only** in `unified_fault_snapshot.payload_json`.

**Admin read:** `GET /api/admin/precompute/unified-category-totals?plant_id=&date_from=&date_to=` (admin only).

**Example ad hoc SQL**

```sql
SELECT category_id, loss_mwh, fault_count, computed_at
  FROM unified_feed_category_totals
 WHERE plant_id = 'YOUR_PLANT' AND date_from = '2026-01-01' AND date_to = '2026-01-31'
 ORDER BY category_id;
```

## Related files

- [`../jobs/enqueue.py`](../jobs/enqueue.py) — merge enqueue after ingest; historical chunk enqueue.
- [`../jobs/precompute_runner.py`](../jobs/precompute_runner.py) — worker CLI.
- [`../routers/faults.py`](../routers/faults.py) — HTTP snapshot-first routes + read-only mode.
- [`../routers/loss_analysis.py`](../routers/loss_analysis.py) — loss bridge snapshot + read-only mode.
