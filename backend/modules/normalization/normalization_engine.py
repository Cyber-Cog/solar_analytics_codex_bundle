"""
modules/normalization/normalization_engine.py
=============================================
Solar Analytics Platform — Normalization Engine

Normalizes DC current and DC power by irradiance and/or DC capacity.
Normalized values help compare strings/SCBs irrespective of weather conditions.

Normalization formulas:
  normalized_current = dc_current / (irradiance / reference_irradiance)
  normalized_power   = dc_power   / (dc_capacity_kw * irradiance / 1000)

Reference irradiance (Standard Test Conditions) = 1000 W/m²
"""

import pandas as pd
import numpy as np
from typing import Optional, List
from common.database import execute_query_df, execute_many
from common.helpers import get_logger

logger = get_logger(__name__, "analytics.log")

# Standard Test Conditions irradiance
STC_IRRADIANCE = 1000.0  # W/m²

# Insert SQL for normalized results (we reuse dc_hierarchy_derived for now)
# In production, you'd add a 'normalized_data' table.
_INSERT_SQL = """
    INSERT INTO dc_hierarchy_derived
        (timestamp, equipment_level, equipment_id, signal, value, source)
    VALUES (?, ?, ?, ?, ?, ?)
"""


def fetch_irradiance(date_from: str, date_to: str) -> pd.DataFrame:
    """
    Fetch irradiance time-series from raw_data_generic.

    Returns:
        DataFrame with columns: timestamp | irradiance
    """
    sql = """
        SELECT timestamp, AVG(value) AS irradiance
        FROM raw_data_generic
        WHERE signal = 'irradiance'
          AND timestamp BETWEEN ? AND ?
        GROUP BY timestamp
        ORDER BY timestamp
    """
    df = execute_query_df(sql, (date_from, date_to))
    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def normalize_current(
    df: pd.DataFrame,
    irradiance_df: pd.DataFrame,
    reference_irradiance: float = STC_IRRADIANCE,
) -> pd.DataFrame:
    """
    Normalize DC current by irradiance ratio.

    Formula:
        normalized_current = dc_current / (irradiance / reference_irradiance)

    Args:
        df                   : Long-format DataFrame with dc_current rows.
        irradiance_df        : DataFrame with timestamp + irradiance columns.
        reference_irradiance : Reference irradiance in W/m² (default 1000).

    Returns:
        DataFrame with an additional 'normalized_current' column.
    """
    if df.empty or irradiance_df.empty:
        logger.warning("Cannot normalize: missing current or irradiance data.")
        return pd.DataFrame()

    current_df = df[df["signal"] == "dc_current"].copy()
    current_df["timestamp"] = pd.to_datetime(current_df["timestamp"])

    # Merge irradiance onto current data by nearest timestamp
    merged = pd.merge_asof(
        current_df.sort_values("timestamp"),
        irradiance_df.sort_values("timestamp"),
        on        = "timestamp",
        tolerance = pd.Timedelta("15min"),
        direction = "nearest",
    )

    # Avoid division by zero or near-zero irradiance
    merged["irr_ratio"]           = merged["irradiance"] / reference_irradiance
    merged["irr_ratio"]           = merged["irr_ratio"].replace(0, np.nan)
    merged["normalized_current"]  = merged["value"] / merged["irr_ratio"]

    return merged[["timestamp", "equipment_id", "equipment_level", "normalized_current"]]


def normalize_power(
    df: pd.DataFrame,
    irradiance_df: pd.DataFrame,
    dc_capacity_kw: float,
    reference_irradiance: float = STC_IRRADIANCE,
) -> pd.DataFrame:
    """
    Normalize DC power by capacity and irradiance (Performance Ratio style).

    Formula:
        normalized_power = dc_power / (dc_capacity_kw * irradiance / STC)

    Args:
        df               : Long-format DataFrame with dc_power rows.
        irradiance_df    : Irradiance time-series DataFrame.
        dc_capacity_kw   : Nameplate DC capacity of the selected equipment.
        reference_irradiance : STC irradiance (default 1000 W/m²).

    Returns:
        DataFrame with 'normalized_power' column.
    """
    if df.empty or irradiance_df.empty:
        return pd.DataFrame()

    power_df = df[df["signal"] == "dc_power"].copy()
    power_df["timestamp"] = pd.to_datetime(power_df["timestamp"])

    merged = pd.merge_asof(
        power_df.sort_values("timestamp"),
        irradiance_df.sort_values("timestamp"),
        on        = "timestamp",
        tolerance = pd.Timedelta("15min"),
        direction = "nearest",
    )

    denominator = dc_capacity_kw * (merged["irradiance"] / reference_irradiance)
    denominator = denominator.replace(0, np.nan)

    merged["normalized_power"] = merged["value"] / denominator

    return merged[["timestamp", "equipment_id", "equipment_level", "normalized_power"]]


def run_normalization(
    equipment_ids: List[str],
    date_from: str,
    date_to: str,
    dc_capacity_kw: float = 1.0,
) -> pd.DataFrame:
    """
    End-to-end normalization for selected equipment and date range.

    Args:
        equipment_ids  : List of equipment ID strings.
        date_from      : Start datetime string.
        date_to        : End datetime string.
        dc_capacity_kw : Nameplate DC capacity (kW) for power normalization.

    Returns:
        DataFrame with normalized_current and normalized_power columns.
    """
    if not equipment_ids:
        return pd.DataFrame()

    placeholders = ", ".join("?" for _ in equipment_ids)
    sql = f"""
        SELECT timestamp, equipment_id, equipment_level, signal, value
        FROM raw_data_generic
        WHERE equipment_id IN ({placeholders})
          AND signal IN ('dc_current', 'dc_power')
          AND timestamp BETWEEN ? AND ?
    """
    params = tuple(equipment_ids) + (date_from, date_to)
    df = execute_query_df(sql, params)

    irr_df = fetch_irradiance(date_from, date_to)

    norm_current = normalize_current(df, irr_df)
    norm_power   = normalize_power(df, irr_df, dc_capacity_kw)

    results = []
    if not norm_current.empty:
        results.append(norm_current)
    if not norm_power.empty:
        results.append(norm_power)

    if results:
        return pd.concat(results).sort_values("timestamp")
    return pd.DataFrame()
