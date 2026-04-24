"""
Centralized SQL generators for pivoting data from Materialized Views instead of raw_data_generic.
"""

def sql_mv_inverter_performance(plant_id: str) -> str:
    return """
    WITH inv_energy AS (
        SELECT 
            equipment_id,
            SUM(ac_power * dt_h) AS energy_kwh
        FROM (
            SELECT 
                equipment_id, 
                ac_power,
                EXTRACT(EPOCH FROM (LEAD(timestamp) OVER (PARTITION BY equipment_id ORDER BY timestamp) - timestamp)) / 3600.0 AS dt_h
            FROM mv_inverter_power_1min
            WHERE plant_id = :plant_id AND timestamp BETWEEN :f AND :t
        ) sub
        WHERE dt_h > 0 AND dt_h <= 1.0
        GROUP BY equipment_id
    ),
    inv_avg AS (
        SELECT 
            r.equipment_id,
            AVG(r.dc_power) AS dc_power,
            AVG(r.ac_power) AS ac_power,
            MAX(a.dc_capacity_kw) AS dc_cap_kw
        FROM mv_inverter_power_1min r
        LEFT JOIN (
            SELECT inverter_id, SUM(dc_capacity_kw) AS dc_capacity_kw
            FROM plant_architecture
            WHERE plant_id = :plant_id
            GROUP BY inverter_id
        ) a ON r.equipment_id = a.inverter_id
        WHERE r.plant_id = :plant_id AND r.timestamp BETWEEN :f AND :t
        GROUP BY r.equipment_id
    )
    SELECT 
        a.equipment_id, a.dc_power, a.ac_power, e.energy_kwh, a.dc_cap_kw
    FROM inv_avg a
    LEFT JOIN inv_energy e ON e.equipment_id = a.equipment_id
    """

def sql_mv_weather_timeline() -> str:
    return """
    SELECT timestamp, ghi, gti, ambient_temp, module_temp, wind_speed
    FROM mv_weather_1min
    WHERE plant_id = :plant_id AND timestamp BETWEEN :f AND :t
    ORDER BY timestamp ASC
    LIMIT 500
    """

def sql_mv_power_vs_gti() -> str:
    return """
    WITH ip AS (
        SELECT timestamp, SUM(ac_power) as active_power
        FROM mv_inverter_power_1min
        WHERE plant_id = :plant_id AND timestamp BETWEEN :f AND :t
        GROUP BY timestamp
    ),
    pg AS (
        SELECT timestamp, gti
        FROM mv_weather_1min
        WHERE plant_id = :plant_id AND timestamp BETWEEN :f AND :t
    )
    SELECT ip.timestamp, ip.active_power, pg.gti
    FROM ip
    LEFT JOIN pg ON pg.timestamp = ip.timestamp
    ORDER BY ip.timestamp ASC
    LIMIT :limit
    """

def sql_mv_plant_ac_daily_energy() -> str:
    return """
    WITH plant_power AS (
        SELECT timestamp, SUM(ac_power) as total_kw
        FROM mv_inverter_power_1min
        WHERE plant_id = :plant_id AND timestamp BETWEEN :from_ts AND :to_ts
        GROUP BY timestamp
    ),
    energy_calc AS (
        SELECT 
            DATE(timestamp) as day,
            total_kw * (EXTRACT(EPOCH FROM (LEAD(timestamp) OVER (ORDER BY timestamp) - timestamp)) / 3600.0) as inc_kwh
        FROM plant_power
    )
    SELECT day, SUM(inc_kwh) as actual_kwh
    FROM energy_calc
    WHERE day IS NOT NULL
    GROUP BY day
    ORDER BY day
    """
