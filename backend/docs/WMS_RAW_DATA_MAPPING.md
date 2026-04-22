# WMS / plant-level weather — equipment_level mapping (UAT reference)

Weather / irradiance rows in `raw_data_generic` may use **either** storage convention:

| `equipment_level` | Typical source |
|-------------------|----------------|
| `plant`           | Built-in NTPC importers (`import_ntpc_*`), older templates |
| `wms`             | Excel “dynamic” uploads, external SCADA exports |

**Both mean the same thing for the app:** plant-level meteo (GTI, GHI, ambient temp, etc.).  
PostgreSQL string comparisons are **case-sensitive**; we normalize with `LOWER(TRIM(...))` in SQL where it matters.

## UI ↔ API ↔ DB

| Surface | Behaviour |
|---------|-----------|
| **Metadata → Raw Data → Level “WMS”** | Sends `equipment_level=wms`. Backend matches **`plant` OR `wms`** (any casing). |
| **Analytics Lab → hierarchy “WMS”** | API level `wms`. Equipment list + signals read **`plant` + `wms`** rows. |
| **Dashboard / DS engine** | GTI / weather KPIs aggregate **`plant` + `wms`**. |

## Analytics Lab `/api/analytics/equipment?level=wms`

The handler **always merges** `plant_equipment` (levels `plant` / `wms`) **with** `DISTINCT equipment_id` from `raw_data_generic` for that plant.  
Previously, any non-empty `plant_equipment` row caused an **early return** that skipped `raw_data_generic`, so Excel `wms` uploads could never appear in the picker.

## Materialized `plant_equipment`

After ingest, `_refresh_equipment_mat()` scans `raw_data_generic` and fills `plant_equipment` for  
`inverter`, `scb`, `string`, and **all rows whose level is `plant` or `wms` (case-insensitive)**.

If Analytics Lab still looks empty after a DB bulk load **outside** the app:

1. **Metadata → Raw Data → Refresh** — triggers `POST /api/metadata/reindex-raw-equipment` (rebuilds `plant_equipment` + clears caches).
2. **Analytics Lab date range** must include timestamps that exist in `raw_data_generic` (summary shows min/max dates).

## Canonical recommendation for new uploads

Prefer **`equipment_level = wms`** and **`equipment_id = <plant_id>`** for a single-station WMS, *or* keep using **`plant`** for consistency with NTPC pipelines — both are supported.
