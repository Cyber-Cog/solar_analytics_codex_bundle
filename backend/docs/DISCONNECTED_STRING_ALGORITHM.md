# Disconnected String (DS) Algorithm — Step-by-Step Process

This document describes the logic implemented for Disconnected String detection in the Solar Analytics platform.

---

## 1. Current Terminology (Actual vs Normalized/Reference)

The algorithm compares **actual measured current** against a **plant-level reference current**. These terms are used consistently throughout this document and in the code.

| Term | Code Variable | DB Column | Description |
|------|---------------|-----------|-------------|
| **Actual SCB current** | `scb_current` | — | Raw measured DC current (A) at the SCB. Read from `raw_data_generic` (signal `dc_current`). This is the **measured value**. |
| **Actual per-string current** | `per_string_current` | `virtual_string_current` | Actual SCB current divided by string count: `scb_current / string_count`. Represents the **actual** current per string. |
| **Reference (normalized) per-string current** | `ref_current` | `expected_current` | Plant-level benchmark per-string current (A). Median of the top 25% inverters' per-string current. This is the **reference/normalized** value used for comparison. |
| **Expected SCB current** | `expected_scb_current` | — | What the SCB current *should* be if all strings were working: `ref_current × string_count`. Used only for `missing_current` calculation, not stored in DB. |

**In short:**
- **Actual** = measured: `scb_current` (SCB level), `virtual_string_current` (per-string level).
- **Normalized/Reference** = plant-level benchmark: `ref_current` / `expected_current` (per-string).

---

## 2. Input Data Requirements

| Requirement | Source | Description |
|-------------|--------|--------------|
| `timestamp` | `raw_data_generic` | Timestamp of the reading |
| `inverter_id` | `plant_architecture` | Parent inverter for each SCB |
| `scb_id` | `raw_data_generic` | Solar Combiner Box identifier |
| `scb_current` | `raw_data_generic` (signal: `dc_current`) | DC current in Amperes |
| `dc_voltage` | `raw_data_generic` (optional) | DC voltage for energy loss calculation |
| `strings_per_scb` | `plant_architecture` | Number of strings per SCB |
| `spare_flag` | `plant_architecture` | If true, SCB is excluded from DS logic |

---

## 2. Step-by-Step Algorithm

### Step 1: Load Architecture & Exclude Spare SCBs

1. Query `plant_architecture` for `scb_id`, `strings_per_scb`, `spare_flag`
2. Build `arch_map`: `{scb_id: string_count}` for SCBs with valid `strings_per_scb > 0`
3. Build `spare_scbs`: set of SCB IDs where `spare_flag = true`
4. **Filter:** Keep only rows where `scb_id` is in `arch_map` and **not** in `spare_scbs`
5. Set `string_count` per row from `arch_map`
6. Drop rows with missing `scb_current` or `string_count`

---

### Step 2: High Outlier Filter

**Purpose:** Remove physically impossible or invalid readings.

1. Fetch `Isc_STC` from `equipment_specs` (module type); default = **10.0 A** if not found
2. **Remove rows** where:
   - `scb_current < 0`
   - `scb_current > Isc_STC × string_count`
3. Track removed SCBs for filter summary (UI display)

---

### Step 3: Constant / Flatline Data Filter

**Purpose:** Exclude frozen or stuck SCADA signals (bad data).

Per SCB, per day:

1. Sort by `(scb_id, timestamp)`
2. Find runs of consecutive identical `scb_current` values
3. **If run length > 120 timestamps:** Mark entire SCB-day as bad → exclude all rows for that (scb_id, day)
4. **If run length ≥ 10 timestamps:** Drop only those frozen points (do not exclude whole day)
5. Track SCBs removed by flatline for filter summary

**Constants:**  
- `CONSTANT_CONSECUTIVE_THRESHOLD = 10`  
- `FLATLINE_BAD_DATA_THRESHOLD = 120`

---

### Step 4: Leakage Data Filter

**Purpose:** Exclude days where an SCB never reaches meaningful current (possible leakage or bad data).

1. For each (date, scb_id), compute `max(scb_current)` over the day
2. **Remove all rows** for (scb_id, day) where `max_current < 20 A`
3. Track removed SCBs for filter summary

**Constant:** `LEAKAGE_MAX_CURRENT_A = 20.0`

---

### Step 5: Actual Per-String Current (→ `virtual_string_current` in DB)

Compute the **actual** per-string current for each row. This value is written to the DB column `virtual_string_current`.

For each row:

```
virtual_string_current = scb_current / string_count
```

In code this is computed as `per_string_current`; the same value is persisted as `virtual_string_current` in `fault_diagnostics`.

---

### Step 6: Virtual String Reference (Plant-Level)

**Purpose:** Compute the expected per-string current from top-performing inverters.

At each **timestamp**:

1. **Aggregate per inverter:**
   - `total_current = SUM(scb_current)` over SCBs under that inverter
   - `total_strings = SUM(string_count)`
   - `per_string = total_current / total_strings`
2. **Rank inverters** by `per_string` (descending)
3. **Take top 25%** of inverters (by count): `top_n = ceil(len(inverters) × 0.25)`
4. **Virtual string current:** `ref_current = MEDIAN(per_string of top 25%)`
5. Merge `ref_current` back to the main DataFrame by `timestamp`

**Constant:** `TOP_PERCENTILE = 0.25`

---

### Step 7: Low Irradiance Filter

**Purpose:** Skip nighttime and low-light periods where current is noise.

1. **Remove all rows** where `ref_current < 2.0 A`

**Constant:** `LOW_IRRADIANCE_THRESHOLD_A = 2.0`

---

### Step 8: Expected Current & Missing Strings

1. **Expected SCB current:**
   ```
   expected_scb_current = ref_current × string_count
   ```

2. **Missing current:**
   ```
   missing_current = max(0, expected_scb_current - scb_current)
   ```

3. **Missing strings** (do not run for negative actual current):
   - If `scb_current < 0` → `missing_strings = 0`
   - Else if `ref_current > 0`:
     ```
     missing_strings = round(missing_current / ref_current)
     ```
   - If `missing_strings < 1` → set to `0`
   - `is_candidate` = (missing_strings ≥ 1)  
   - `is_clear` = (missing_strings < 1)

---

### Step 9: Interval Inference

From the timestamps, infer the typical interval (e.g., 5 min):

1. Sort unique timestamps, compute diffs in minutes
2. Use mode (or median) of diffs as `interval_minutes`
3. Default = **5** if cannot infer

**Derived values:**
- `confirm_points = ceil(30 / interval_minutes)` — e.g. 6 for 5‑min data
- `recovery_points = ceil(15 / interval_minutes)` — e.g. 3 for 5‑min data
- `tolerance_sec = interval_minutes × 60 × 1.5` — consecutive points must be within this

**Constants:**
- `PERSISTENCE_MINUTES = 30`
- `RECOVERY_MINUTES = 15`

---

### Step 10: State Machine (Fault Persistence & Recovery)

Per SCB, over time-ordered rows:

| State | Meaning |
|-------|---------|
| **0** | Normal (no fault) |
| **1** | Confirmed DS fault |

**Rules:**

1. **State 0 → State 1:**  
   - When `is_candidate` is true for `confirm_points` **consecutive** timestamps (within `tolerance_sec`)  
   - Mark all points in that run as `CONFIRMED_DS`  
   - Compute power/energy loss for those points

2. **State 1 → State 0:**  
   - When `is_clear` is true for `recovery_points` **consecutive** timestamps (within `tolerance_sec`)  
   - Mark the last point as `NORMAL` and clear power/energy for it  
   - Transition back to State 0

3. **While in State 1:**  
   - Each point remains `CONFIRMED_DS` until recovery  
   - Power loss: `power_loss_kw = (dc_voltage × missing_current) / 1000`  
   - Energy loss: `energy_loss_kwh = power_loss_kw × (interval_minutes / 60)`  
   - If `dc_voltage` is missing/infinite, power and energy = 0

---

### Step 11: Write to fault_diagnostics

For each row, insert:

| Column | Value |
|--------|-------|
| `timestamp` | Timestamp string |
| `plant_id` | Plant ID |
| `inverter_id` | Inverter ID |
| `scb_id` | SCB ID |
| `virtual_string_current` | `per_string_current` |
| `expected_current` | `ref_current` |
| `missing_current` | Computed missing current |
| `missing_strings` | Rounded count |
| `power_loss_kw` | From state machine |
| `energy_loss_kwh` | From state machine |
| `fault_status` | `CONFIRMED_DS` or `NORMAL` |

---

### Step 12: UI Aggregation — Range Minimum

For the heatmap and table, the displayed value per SCB is:

```
range_min[scb_id] = MIN(missing_strings) over ALL timestamps in the selected date range
```

- Uses **all** rows (both NORMAL and CONFIRMED_DS)
- Cast to integer; cap each row at 2000 before MIN to avoid bad outliers
- If **any** timestamp has `missing_strings = 0`, the displayed value is **0**

---

## 3. Data Flow Diagram

```
raw_data_generic (dc_current, dc_voltage)
        │
        ▼
plant_architecture (strings_per_scb, spare_flag, inverter_id)
        │
        ▼
[Step 1] Exclude spare SCBs, apply string_count
        │
        ▼
[Step 2] High outlier filter (Isc, negative)
        │
        ▼
[Step 3] Constant / flatline filter (10, 120)
        │
        ▼
[Step 4] Leakage filter (max < 20 A)
        │
        ▼
[Step 5–7] per_string_current, virtual string, low irradiance
        │
        ▼
[Step 8] expected_current, missing_current, missing_strings
        │
        ▼
[Step 9–10] State machine (confirm 30 min, recover 15 min)
        │
        ▼
fault_diagnostics table
        │
        ▼
[Step 12] _range_min_disconnected_strings() → MIN over range → UI
```

---

## 4. Constants Summary

| Constant | Value | File |
|----------|-------|------|
| `TOP_PERCENTILE` | 0.25 | ds_detection.py |
| `LOW_IRRADIANCE_THRESHOLD_A` | 2.0 | ds_detection.py |
| `PERSISTENCE_MINUTES` | 30 | ds_detection.py |
| `RECOVERY_MINUTES` | 15 | ds_detection.py |
| `CONSTANT_CONSECUTIVE_THRESHOLD` | 10 | ds_detection.py |
| `FLATLINE_BAD_DATA_THRESHOLD` | 120 | ds_detection.py |
| `LEAKAGE_MAX_CURRENT_A` | 20.0 | ds_detection.py |
| `DEFAULT_ISC_STC_A` | 10.0 | ds_detection.py |
| Range-min cap | 2000 | faults.py |

---

## 5. Key Files

| File | Role |
|------|------|
| `backend/engine/ds_detection.py` | Core algorithm; invoked on raw data upload |
| `backend/routers/faults.py` | API; `_range_min_disconnected_strings` for heatmap/table |
| `backend/scripts/recompute_ds_faults.py` | Batch recompute from raw_data_generic |
| `backend/routers/metadata.py` | Triggers DS detection after raw data upload |

---

*Generated from implementation in ds_detection.py, faults.py, and recompute_ds_faults.py.*
