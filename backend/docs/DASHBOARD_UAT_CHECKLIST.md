# Dashboard UAT checklist

**Important:** Automated checks in this repo cover **date-range resolution** and **insolation math helpers** only (`backend/tests/test_dashboard_helpers.py`). They do **not** hit PostgreSQL or the browser. Full UAT still requires **you** to verify UI + data against your plant.

## Automated (run locally)

From `solar_analytics_codex_bundle/backend` (stdlib only):

```bash
python -m unittest discover -s tests -p "test_*.py" -v
```

## Backend smoke (optional)

With API running and a valid JWT:

1. `GET /api/dashboard/bundle?plant_id=<id>&date_from=YYYY-MM-DD&date_to=YYYY-MM-DD`
2. Confirm JSON keys: `station`, `kpis`, `wms`, `energy`, `inverter_performance`, `power_vs_gti`.
3. Single-day range: `power_vs_gti` timestamps should fall on that calendar day only.
4. After deploy, first load may be uncached; repeat within cache window to confirm `bundle_v6` behaviour.

## Manual UI checks (Photon Intelligence Centre → Dashboard)

| # | Area | What to verify |
|---|------|----------------|
| 1 | Date range | Set **one day** → Power vs GTI shows **one** daily curve; x-axis shows `HH:MM` only. |
| 2 | Date range | Set **3+ days** → x-axis ticks show **day + time** (`DD/MM HH:MM` style). |
| 3 | Energy Export / Net Gen | Values match order of magnitude vs SQL / SCADA for same range (kWh vs MWh labels). |
| 4 | Target vs Actual | Same **from** and **to** day → blue “Actual” follows **daily export** from bundle (not stuck at 0). |
| 5 | WMS cards | GHI/GTI insolation and tilt irradiance non-zero when raw WMS rows exist; temps/wind match common signal names. |
| 6 | Inverter table | Rows, sort, CSV export; PR/PLF plausible vs energy + insolation. |
| 7 | Heatmap | Gradient reflects min→max PR. |
| 8 | Empty range | Date with no data → onboarding / empty states, no crash. |
| 9 | Hard refresh | After backend changes, Ctrl+F5 so `index.html` / `bundle_v*` cache updates. |

## Known modelling assumptions (not bugs)

- WMS **insolation** (kWh/m²) uses **Σ (W/m²) / 60000** — consistent with **1-minute** sampled WMS rows. If your telemetry is 15-minute only, absolute insolation numbers may need a different divisor (separate change).
- **PR** uses the same tilt insolation model as the WMS GTI block (including **`irradiance`** fallback when **`gti`** is absent).

## If something still looks wrong

1. In DB, confirm `plant_id`, `equipment_level`, and `signal` casing for that plant (queries are **case-insensitive** for dashboard power + WMS sums).
2. Confirm `raw_data_generic` (or chosen table) has rows in `[date_from 00:00, date_to 23:59]`.
3. Clear dashboard bundle cache by waiting for TTL or restarting the API process.
