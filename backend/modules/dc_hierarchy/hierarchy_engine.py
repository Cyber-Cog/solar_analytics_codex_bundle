"""
modules/dc_hierarchy/hierarchy_engine.py
=========================================
Solar Analytics Platform — DC Hierarchy Derivation Engine

This engine derives SCB-level and Inverter-level DC values from
string-level raw data when those higher-level measurements are absent.

Derivation Rules:
  • SCB dc_current  = SUM of constituent string dc_currents
  • SCB dc_voltage  = MEAN of constituent string dc_voltages
  • SCB dc_power    = SUM of constituent string dc_powers

  • Inverter dc_current = SUM of constituent SCB dc_currents
  • Inverter dc_voltage = MEAN of constituent SCB dc_voltages
  • Inverter dc_power   = SUM of constituent SCB dc_powers

Results are written to dc_hierarchy_derived table.
"""

import pandas as pd
from typing import Optional
from common.database import execute_query_df, execute_many
from common.helpers import (
    get_logger,
    extract_parent_scb,
    extract_parent_inverter,
)

logger = get_logger(__name__, "ingestion.log")

# INSERT SQL for derived results
_INSERT_DERIVED_SQL = """
    INSERT INTO dc_hierarchy_derived
        (timestamp, equipment_level, equipment_id, signal, value, source)
    VALUES (?, ?, ?, ?, ?, ?)
"""

# Aggregation rules per signal
_AGG_RULES = {
    "dc_current": "sum",
    "dc_voltage": "mean",
    "dc_power":   "sum",
}


def _fetch_string_data(date_from: Optional[str] = None, date_to: Optional[str] = None) -> pd.DataFrame:
    """
    Fetch all string-level dc signals from raw_data_generic.

    Args:
        date_from : Optional start datetime filter.
        date_to   : Optional end datetime filter.

    Returns:
        Long-format DataFrame: timestamp | equipment_id | signal | value
    """
    base_sql = """
        SELECT timestamp, equipment_id, signal, value
        FROM raw_data_generic
        WHERE equipment_level = 'string'
          AND signal IN ('dc_current', 'dc_voltage', 'dc_power')
    """
    params = ()
    if date_from and date_to:
        base_sql += " AND timestamp BETWEEN ? AND ?"
        params = (date_from, date_to)

    df = execute_query_df(base_sql, params)
    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["value"]     = pd.to_numeric(df["value"], errors="coerce")
    return df


def _derive_scb_values(string_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate string data upward to SCB level.

    Args:
        string_df : Long-format string-level DataFrame.

    Returns:
        Long-format SCB-level DataFrame ready for DB insertion.
    """
    if string_df.empty:
        return pd.DataFrame()

    # Add parent SCB column
    string_df = string_df.copy()
    string_df["scb_id"] = string_df["equipment_id"].apply(extract_parent_scb)
    string_df = string_df.dropna(subset=["scb_id"])

    records = []
    for (ts, scb_id, signal), group in string_df.groupby(["timestamp", "scb_id", "signal"]):
        agg_fn = _AGG_RULES.get(signal, "sum")
        value  = group["value"].dropna().agg(agg_fn)
        records.append({
            "timestamp":       ts,
            "equipment_level": "scb",
            "equipment_id":    scb_id,
            "signal":          signal,
            "value":           value,
        })

    return pd.DataFrame(records) if records else pd.DataFrame()


def _derive_inverter_values(scb_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate SCB data upward to Inverter level.

    Args:
        scb_df : Long-format SCB-level DataFrame (from _derive_scb_values).

    Returns:
        Long-format Inverter-level DataFrame ready for DB insertion.
    """
    if scb_df.empty:
        return pd.DataFrame()

    scb_df = scb_df.copy()
    scb_df["inverter_id"] = scb_df["equipment_id"].apply(extract_parent_inverter)
    scb_df = scb_df.dropna(subset=["inverter_id"])

    records = []
    for (ts, inv_id, signal), group in scb_df.groupby(["timestamp", "inverter_id", "signal"]):
        agg_fn = _AGG_RULES.get(signal, "sum")
        value  = group["value"].dropna().agg(agg_fn)
        records.append({
            "timestamp":       ts,
            "equipment_level": "inverter",
            "equipment_id":    inv_id,
            "signal":          signal,
            "value":           value,
        })

    return pd.DataFrame(records) if records else pd.DataFrame()


def _df_to_insert_tuples(df: pd.DataFrame) -> list:
    """Convert a derived DataFrame to list of tuples for executemany."""
    if df.empty:
        return []
    return [
        (str(row["timestamp"]), row["equipment_level"],
         row["equipment_id"], row["signal"], row["value"], "derived")
        for _, row in df.iterrows()
    ]


def run_hierarchy_derivation(
    date_from: Optional[str] = None,
    date_to:   Optional[str] = None,
) -> dict:
    """
    Full derivation pipeline: String → SCB → Inverter.

    Args:
        date_from : Optional start filter.
        date_to   : Optional end filter.

    Returns:
        Dict with counts: {'scb_records': N, 'inverter_records': M}
    """
    logger.info("Starting DC hierarchy derivation...")

    # Step 1: Fetch raw string data
    string_df = _fetch_string_data(date_from, date_to)
    if string_df.empty:
        logger.warning("No string-level data found. Derivation skipped.")
        return {"scb_records": 0, "inverter_records": 0}

    logger.info(f"String records fetched: {len(string_df)}")

    # Step 2: Derive SCB values
    scb_df    = _derive_scb_values(string_df)
    scb_rows  = _df_to_insert_tuples(scb_df)

    if scb_rows:
        execute_many(_INSERT_DERIVED_SQL, scb_rows)
        logger.info(f"SCB derived records inserted: {len(scb_rows)}")
    else:
        logger.warning("No SCB records could be derived.")

    # Step 3: Derive Inverter values from SCB
    inv_df   = _derive_inverter_values(scb_df)
    inv_rows = _df_to_insert_tuples(inv_df)

    if inv_rows:
        execute_many(_INSERT_DERIVED_SQL, inv_rows)
        logger.info(f"Inverter derived records inserted: {len(inv_rows)}")
    else:
        logger.warning("No Inverter records could be derived.")

    return {
        "scb_records":      len(scb_rows),
        "inverter_records": len(inv_rows),
    }
