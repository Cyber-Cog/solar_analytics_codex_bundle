"""
modules/analytics_lab/query_engine.py
======================================
Solar Analytics Platform — Data Query Engine for Analytics Lab

This module handles ALL database queries needed by the Analytics Lab.
Callers use these helpers instead of embedding raw SQL in multiple places.

Key queries:
  - Get available equipment by level
  - Get available date range for selected equipment
  - Get data availability percentage
  - Fetch time-series data for plotting
"""

import pandas as pd
from typing import List, Optional, Tuple
from common.database import execute_query_df, execute_query
from common.helpers import get_logger

logger = get_logger(__name__, "analytics.log")

# ── Equipment Queries ─────────────────────────────────────────────────────────

def get_equipment_by_level(level: str) -> List[str]:
    """
    Return a sorted list of all unique equipment IDs at the given level.

    Searches both raw_data_generic and dc_hierarchy_derived.

    Args:
        level : 'inverter', 'scb', or 'string'

    Returns:
        Sorted list of equipment ID strings.
    """
    sql = """
        SELECT DISTINCT equipment_id FROM raw_data_generic
        WHERE equipment_level = ?
        UNION
        SELECT DISTINCT equipment_id FROM dc_hierarchy_derived
        WHERE equipment_level = ?
        ORDER BY equipment_id
    """
    rows = execute_query(sql, (level, level))
    ids  = [row["equipment_id"] for row in rows]
    logger.debug(f"get_equipment_by_level('{level}') → {len(ids)} IDs")
    return ids


def get_available_date_range(
    equipment_ids: List[str],
    signal: str,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Return the (min_timestamp, max_timestamp) for a list of equipment IDs
    and a specific signal.

    Args:
        equipment_ids : List of equipment ID strings.
        signal        : Signal name, e.g. 'dc_current'.

    Returns:
        Tuple of (min_ts_str, max_ts_str), both can be None if no data.
    """
    if not equipment_ids:
        return None, None

    placeholders = ", ".join("?" for _ in equipment_ids)
    sql = f"""
        SELECT MIN(timestamp) AS min_ts, MAX(timestamp) AS max_ts
        FROM (
            SELECT timestamp FROM raw_data_generic
            WHERE equipment_id IN ({placeholders}) AND signal = ?
            UNION ALL
            SELECT timestamp FROM dc_hierarchy_derived
            WHERE equipment_id IN ({placeholders}) AND signal = ?
        )
    """
    params = tuple(equipment_ids) + (signal,) + tuple(equipment_ids) + (signal,)
    rows = execute_query(sql, params)

    if rows and rows[0]["min_ts"]:
        return rows[0]["min_ts"], rows[0]["max_ts"]
    return None, None


def get_data_availability_pct(
    equipment_ids: List[str],
    signal: str,
    date_from: str,
    date_to: str,
    interval_minutes: int = 15,
) -> float:
    """
    Calculate data availability as a percentage.

    Formula:
        actual_records / expected_records × 100

    Expected records is based on total time ÷ interval_minutes
    across all selected equipment IDs.

    Args:
        equipment_ids     : List of equipment IDs.
        signal            : Signal name.
        date_from         : Start datetime string (ISO format).
        date_to           : End datetime string (ISO format).
        interval_minutes  : Expected data frequency in minutes (default 15).

    Returns:
        Float percentage (0.0 – 100.0).
    """
    if not equipment_ids:
        return 0.0

    placeholders = ", ".join("?" for _ in equipment_ids)
    sql = f"""
        SELECT COUNT(*) AS cnt FROM (
            SELECT timestamp, equipment_id FROM raw_data_generic
            WHERE equipment_id IN ({placeholders})
              AND signal = ?
              AND timestamp BETWEEN ? AND ?
            UNION ALL
            SELECT timestamp, equipment_id FROM dc_hierarchy_derived
            WHERE equipment_id IN ({placeholders})
              AND signal = ?
              AND timestamp BETWEEN ? AND ?
        )
    """
    params = (
        tuple(equipment_ids) + (signal, date_from, date_to) +
        tuple(equipment_ids) + (signal, date_from, date_to)
    )
    rows = execute_query(sql, params)
    actual = rows[0]["cnt"] if rows else 0

    # Calculate expected
    from datetime import datetime
    try:
        dt_from = datetime.fromisoformat(date_from)
        dt_to   = datetime.fromisoformat(date_to)
        minutes  = max((dt_to - dt_from).total_seconds() / 60, 1)
        expected = int(minutes / interval_minutes) * len(equipment_ids)
    except Exception:
        expected = 1

    pct = min((actual / expected) * 100, 100.0) if expected > 0 else 0.0
    logger.debug(f"Data availability: {pct:.1f}% ({actual}/{expected})")
    return pct


# ── Time-Series Data Queries ──────────────────────────────────────────────────

def fetch_timeseries(
    equipment_ids: List[str],
    signals: List[str],
    date_from: str,
    date_to: str,
) -> pd.DataFrame:
    """
    Fetch time-series data for the selected equipment and signals.

    Result shape:
        timestamp | equipment_id | signal | value

    Merges raw and derived data. Derived data is used as fallback
    when raw data is missing for that equipment + signal combination.

    Args:
        equipment_ids : Selected equipment IDs.
        signals       : List of signal names to fetch.
        date_from     : Start datetime (ISO string).
        date_to       : End datetime (ISO string).

    Returns:
        Long-format DataFrame sorted by timestamp and equipment_id.
    """
    if not equipment_ids or not signals:
        return pd.DataFrame(columns=["timestamp", "equipment_id", "signal", "value"])

    eq_placeholders  = ", ".join("?" for _ in equipment_ids)
    sig_placeholders = ", ".join("?" for _ in signals)

    sql = f"""
        SELECT timestamp, equipment_id, signal, value, 'raw' AS data_source
        FROM raw_data_generic
        WHERE equipment_id IN ({eq_placeholders})
          AND signal       IN ({sig_placeholders})
          AND timestamp BETWEEN ? AND ?

        UNION ALL

        SELECT timestamp, equipment_id, signal, value, 'derived' AS data_source
        FROM dc_hierarchy_derived
        WHERE equipment_id IN ({eq_placeholders})
          AND signal       IN ({sig_placeholders})
          AND timestamp BETWEEN ? AND ?

        ORDER BY timestamp, equipment_id
    """
    params = (
        tuple(equipment_ids) + tuple(signals) + (date_from, date_to) +
        tuple(equipment_ids) + tuple(signals) + (date_from, date_to)
    )

    df = execute_query_df(sql, params)

    if df.empty:
        logger.info("fetch_timeseries returned no data for the selected filters.")
    else:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["value"]     = pd.to_numeric(df["value"], errors="coerce")
        logger.info(f"fetch_timeseries: {len(df)} rows for {len(equipment_ids)} equipment.")

    return df


def pivot_timeseries(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot long-format data into a wide DataFrame suitable for plotting.

    Output columns: timestamp | equipment_id | dc_current | dc_voltage | ...

    Args:
        df : Long-format DataFrame from fetch_timeseries.

    Returns:
        Wide-format DataFrame.
    """
    if df.empty:
        return df

    wide = df.pivot_table(
        index   = ["timestamp", "equipment_id"],
        columns = "signal",
        values  = "value",
        aggfunc = "mean",
    ).reset_index()

    wide.columns.name = None  # Remove the 'signal' label from column axis
    return wide


def get_unique_signals_in_db(level: str) -> List[str]:
    """
    Return all signal names present in the database for a given equipment level.
    Used to dynamically populate the parameter selector in the UI.
    """
    sql = """
        SELECT DISTINCT signal FROM raw_data_generic
        WHERE equipment_level = ?
        ORDER BY signal
    """
    rows = execute_query(sql, (level,))
    return [r["signal"] for r in rows]
