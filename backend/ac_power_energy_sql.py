"""
Integrate AC power (kW) into energy (kWh) from irregular timestamps.

Replaces the old assumption that every row is exactly 15 minutes (SUM/4).
Uses forward rectangles: E = Σ P_i × Δt_i with Δt from LEAD(timestamp) − timestamp.

Long gaps (> typical intraday step) are capped to min(gap, 8 × median_step)
so overnight / outage gaps do not multiply the last daytime reading across many hours.
Median step uses only gaps in (0, 6h] to estimate sampling cadence (1‑min vs 15‑min, etc.).
"""

from __future__ import annotations


def sql_plant_ac_totals(table: str) -> str:
    """One row: total_kwh, peak_kw, avg_kw for the plant (all inverters summed per timestamp)."""
    return f"""
        WITH plant_power AS (
            SELECT timestamp, SUM(value)::double precision AS total_kw
            FROM {table}
            WHERE plant_id = :plant_id
              AND LOWER(TRIM(signal::text)) = 'ac_power'
              AND LOWER(TRIM(equipment_level::text)) = 'inverter'
              AND timestamp BETWEEN :f AND :t
            GROUP BY timestamp
        ),
        plant_gaps AS (
            SELECT
                total_kw,
                EXTRACT(EPOCH FROM (
                    LEAD(timestamp::timestamp) OVER (ORDER BY timestamp) - timestamp::timestamp
                )) / 3600.0 AS dt_h
            FROM plant_power
        ),
        step_median AS (
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dt_h) AS m
            FROM plant_gaps
            WHERE dt_h IS NOT NULL AND dt_h > 0 AND dt_h <= 6.0
        ),
        plant_integrated AS (
            SELECT
                total_kw,
                CASE
                    WHEN dt_h IS NULL THEN COALESCE((SELECT m FROM step_median), 0.25)
                    WHEN dt_h <= 0 THEN COALESCE((SELECT m FROM step_median), 0.25)
                    ELSE LEAST(
                        dt_h,
                        GREATEST(
                            8.0 * COALESCE((SELECT m FROM step_median), 0.25),
                            1.0 / 60.0
                        )
                    )
                END AS dt_eff_h
            FROM plant_gaps
        )
        SELECT
            (SELECT SUM(total_kw * dt_eff_h) FROM plant_integrated) AS total_kwh,
            (SELECT MAX(total_kw) FROM plant_power) AS peak_kw,
            (SELECT AVG(total_kw) FROM plant_power) AS avg_kw
    """


def sql_plant_ac_daily_energy(table: str) -> str:
    """Per calendar day: actual_kwh (plant total)."""
    return f"""
        WITH plant_power AS (
            SELECT timestamp, SUM(value)::double precision AS total_kw
            FROM {table}
            WHERE plant_id = :plant_id
              AND LOWER(TRIM(signal::text)) = 'ac_power'
              AND LOWER(TRIM(equipment_level::text)) = 'inverter'
              AND timestamp BETWEEN :from_ts AND :to_ts
            GROUP BY timestamp
        ),
        plant_gaps AS (
            SELECT
                DATE(timestamp) AS day,
                total_kw,
                EXTRACT(EPOCH FROM (
                    LEAD(timestamp::timestamp) OVER (ORDER BY timestamp) - timestamp::timestamp
                )) / 3600.0 AS dt_h
            FROM plant_power
        ),
        step_median AS (
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dt_h) AS m
            FROM plant_gaps
            WHERE dt_h IS NOT NULL AND dt_h > 0 AND dt_h <= 6.0
        ),
        rows_eff AS (
            SELECT
                day,
                total_kw,
                CASE
                    WHEN dt_h IS NULL THEN COALESCE((SELECT m FROM step_median), 0.25)
                    WHEN dt_h <= 0 THEN COALESCE((SELECT m FROM step_median), 0.25)
                    ELSE LEAST(
                        dt_h,
                        GREATEST(
                            8.0 * COALESCE((SELECT m FROM step_median), 0.25),
                            1.0 / 60.0
                        )
                    )
                END AS dt_eff_h
            FROM plant_gaps
        )
        SELECT day, SUM(total_kw * dt_eff_h) AS actual_kwh
        FROM rows_eff
        GROUP BY day
        ORDER BY day
    """


def sql_inverter_performance_with_energy(table: str) -> str:
    """Per inverter: dc_power, ac_power (avg), energy_kwh (integrated), dc_cap_kw."""
    return f"""
        WITH inv_step AS (
            SELECT equipment_id, timestamp, value::double precision AS kw
            FROM {table}
            WHERE plant_id = :plant_id
              AND LOWER(TRIM(equipment_level::text)) = 'inverter'
              AND LOWER(TRIM(signal::text)) = 'ac_power'
              AND timestamp BETWEEN :f AND :t
        ),
        inv_gapped AS (
            SELECT
                equipment_id,
                kw,
                EXTRACT(EPOCH FROM (
                    LEAD(timestamp::timestamp) OVER (PARTITION BY equipment_id ORDER BY timestamp) - timestamp::timestamp
                )) / 3600.0 AS dt_h
            FROM inv_step
        ),
        step_median AS (
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dt_h) AS m
            FROM inv_gapped
            WHERE dt_h IS NOT NULL AND dt_h > 0 AND dt_h <= 6.0
        ),
        inv_energy AS (
            SELECT
                equipment_id,
                SUM(
                    kw * (
                        CASE
                            WHEN dt_h IS NULL THEN COALESCE((SELECT m FROM step_median), 0.25)
                            WHEN dt_h <= 0 THEN COALESCE((SELECT m FROM step_median), 0.25)
                            ELSE LEAST(
                                dt_h,
                                GREATEST(
                                    8.0 * COALESCE((SELECT m FROM step_median), 0.25),
                                    1.0 / 60.0
                                )
                            )
                        END
                    )
                ) AS energy_kwh
            FROM inv_gapped
            GROUP BY equipment_id
        ),
        inv_avg AS (
            SELECT
                r.equipment_id,
                AVG(CASE WHEN LOWER(TRIM(r.signal::text)) = 'dc_power' THEN r.value END) AS dc_power,
                AVG(CASE WHEN LOWER(TRIM(r.signal::text)) = 'ac_power' THEN r.value END) AS ac_power,
                MAX(a.dc_capacity_kw) AS dc_cap_kw
            FROM {table} r
            LEFT JOIN (
                SELECT inverter_id, SUM(dc_capacity_kw) AS dc_capacity_kw
                FROM plant_architecture
                WHERE plant_id = :plant_id
                GROUP BY inverter_id
            ) a ON r.equipment_id = a.inverter_id
            WHERE r.plant_id = :plant_id
              AND LOWER(TRIM(r.equipment_level::text)) = 'inverter'
              AND r.timestamp BETWEEN :f AND :t
            GROUP BY r.equipment_id
        )
        SELECT
            inv_avg.equipment_id,
            inv_avg.dc_power,
            inv_avg.ac_power,
            inv_energy.energy_kwh,
            inv_avg.dc_cap_kw
        FROM inv_avg
        LEFT JOIN inv_energy ON inv_energy.equipment_id = inv_avg.equipment_id
        ORDER BY inv_avg.equipment_id
    """


def sql_inverter_ac_daily_energy(table: str) -> str:
    """
    Per calendar day and inverter: integrated AC energy (kWh), same gap logic as
    sql_inverter_performance_with_energy. PostgreSQL-oriented (PERCENTILE_CONT, DATE).
    """
    return f"""
        WITH inv_step AS (
            SELECT equipment_id, timestamp, value::double precision AS kw
            FROM {table}
            WHERE plant_id = :plant_id
              AND LOWER(TRIM(equipment_level::text)) = 'inverter'
              AND LOWER(TRIM(signal::text)) = 'ac_power'
              AND timestamp BETWEEN :f AND :t
        ),
        inv_gapped AS (
            SELECT
                equipment_id,
                timestamp,
                kw,
                EXTRACT(EPOCH FROM (
                    LEAD(timestamp::timestamp) OVER (PARTITION BY equipment_id ORDER BY timestamp) - timestamp::timestamp
                )) / 3600.0 AS dt_h
            FROM inv_step
        ),
        step_median AS (
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dt_h) AS m
            FROM inv_gapped
            WHERE dt_h IS NOT NULL AND dt_h > 0 AND dt_h <= 6.0
        ),
        rows_eff AS (
            SELECT
                DATE(timestamp) AS day,
                equipment_id,
                kw * (
                    CASE
                        WHEN dt_h IS NULL THEN COALESCE((SELECT m FROM step_median), 0.25)
                        WHEN dt_h <= 0 THEN COALESCE((SELECT m FROM step_median), 0.25)
                        ELSE LEAST(
                            dt_h,
                            GREATEST(
                                8.0 * COALESCE((SELECT m FROM step_median), 0.25),
                                1.0 / 60.0
                            )
                        )
                    END
                ) AS energy_inc
            FROM inv_gapped
        )
        SELECT day::text AS day, equipment_id, SUM(energy_inc) AS energy_kwh
        FROM rows_eff
        GROUP BY day, equipment_id
        ORDER BY day, equipment_id
    """


def sql_wms_irradiance_daily_sums(table: str) -> str:
    """Per calendar day: summed GTI and legacy irradiance (W/m² samples) at plant/WMS level."""
    return f"""
        SELECT
            DATE(timestamp)::text AS day,
            COALESCE(SUM(CASE WHEN LOWER(TRIM(signal::text)) IN ('gti') THEN value::double precision ELSE 0 END), 0)::double precision AS gti_sum,
            COALESCE(SUM(CASE WHEN LOWER(TRIM(signal::text)) IN ('irradiance') THEN value::double precision ELSE 0 END), 0)::double precision AS irr_sum
        FROM {table}
        WHERE plant_id = :plant_id
          AND LOWER(TRIM(equipment_level::text)) IN ('plant', 'wms')
          AND timestamp BETWEEN :f AND :t
        GROUP BY DATE(timestamp)
        ORDER BY day
    """
