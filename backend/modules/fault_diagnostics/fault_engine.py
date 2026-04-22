"""
modules/fault_diagnostics/fault_engine.py
==========================================
Solar Analytics Platform — Fault Diagnostics Engine (Placeholder)

This module is a structured placeholder for fault detection rules.
Each fault rule is an independent function that accepts a DataFrame
and returns a results DataFrame with flagged anomalies.

HOW TO ADD A NEW FAULT RULE:
  1. Write a function that accepts a DataFrame and returns a DataFrame.
  2. The output DataFrame must have columns:
       timestamp | equipment_id | fault_type | severity | description
  3. Register the function in FAULT_REGISTRY at the bottom of this file.
  4. The run_all_faults() function will automatically pick it up.

Current rules (placeholder implementations):
  • low_current_fault    — flags strings with current below threshold
  • string_disconnected  — flags strings with zero/null current
  • scb_unbalance        — flags SCBs where one string deviates too much
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Any
from common.helpers import get_logger

logger = get_logger(__name__, "faults.log")

# ── Fault Result Schema ───────────────────────────────────────────────────────
FAULT_COLUMNS = ["timestamp", "equipment_id", "fault_type", "severity", "description"]


def _empty_result() -> pd.DataFrame:
    """Return an empty DataFrame matching the fault result schema."""
    return pd.DataFrame(columns=FAULT_COLUMNS)


# ── Fault Rule 1: Low Current ─────────────────────────────────────────────────

def low_current_fault(
    df: pd.DataFrame,
    threshold_fraction: float = 0.5,
) -> pd.DataFrame:
    """
    Flag timestamps where a string's current is below a threshold
    relative to the median current of all strings at that timestamp.

    Args:
        df                 : Long-format DataFrame (equipment_id, signal, value).
        threshold_fraction : Fraction of median below which a fault is raised.
                             Default 0.5 = flag if current < 50% of median.

    Returns:
        Fault result DataFrame.
    """
    curr_df = df[df["signal"] == "dc_current"].copy()
    if curr_df.empty:
        return _empty_result()

    # Median current per timestamp across all strings
    medians = curr_df.groupby("timestamp")["value"].median().reset_index()
    medians.columns = ["timestamp", "median_current"]

    merged  = curr_df.merge(medians, on="timestamp")
    threshold = merged["median_current"] * threshold_fraction

    faults  = merged[merged["value"] < threshold].copy()
    faults["fault_type"]   = "low_current_fault"
    faults["severity"]     = "WARNING"
    faults["description"]  = faults.apply(
        lambda r: f"Current {r['value']:.2f} A is below {threshold_fraction*100:.0f}% "
                  f"of median {r['median_current']:.2f} A",
        axis=1
    )

    logger.info(f"low_current_fault: {len(faults)} fault events detected.")
    return faults[FAULT_COLUMNS]


# ── Fault Rule 2: String Disconnected ────────────────────────────────────────

def string_disconnected(df: pd.DataFrame, zero_threshold: float = 0.1) -> pd.DataFrame:
    """
    Flag timestamps where a string's current is effectively zero
    (likely disconnected or open-circuit).

    Args:
        df              : Long-format DataFrame.
        zero_threshold  : Values below this are treated as zero.

    Returns:
        Fault result DataFrame.
    """
    curr_df = df[df["signal"] == "dc_current"].copy()
    if curr_df.empty:
        return _empty_result()

    # Only flag during daylight hours (irradiance > 50 W/m² as proxy)
    # For now, assume all records are during producing hours.
    disconnected = curr_df[
        (curr_df["value"].isna()) | (curr_df["value"] < zero_threshold)
    ].copy()

    disconnected["fault_type"]  = "string_disconnected"
    disconnected["severity"]    = "CRITICAL"
    disconnected["description"] = "String current is zero or null — possible disconnection."

    logger.info(f"string_disconnected: {len(disconnected)} fault events detected.")
    return disconnected[FAULT_COLUMNS]


# ── Fault Rule 3: SCB Imbalance ───────────────────────────────────────────────

def scb_unbalance(
    df: pd.DataFrame,
    imbalance_threshold: float = 0.3,
) -> pd.DataFrame:
    """
    Flag SCBs where one or more strings deviate significantly from
    the average current of all strings in that SCB at a given timestamp.

    Args:
        df                   : String-level long-format DataFrame.
        imbalance_threshold  : Fraction deviation that triggers a flag.
                               Default 0.3 = flag if >30% from average.

    Returns:
        Fault result DataFrame.
    """
    curr_df = df[df["signal"] == "dc_current"].copy()
    if curr_df.empty:
        return _empty_result()

    # Derive SCB parent from string ID
    from common.helpers import extract_parent_scb
    curr_df["scb_id"] = curr_df["equipment_id"].apply(extract_parent_scb)
    curr_df = curr_df.dropna(subset=["scb_id"])

    # Average per SCB per timestamp
    scb_mean = (
        curr_df.groupby(["timestamp", "scb_id"])["value"]
        .mean()
        .reset_index(name="scb_mean")
    )

    merged = curr_df.merge(scb_mean, on=["timestamp", "scb_id"])
    merged["deviation"] = (merged["value"] - merged["scb_mean"]).abs() / merged["scb_mean"].replace(0, np.nan)

    faults = merged[merged["deviation"] > imbalance_threshold].copy()
    faults["fault_type"]  = "scb_unbalance"
    faults["severity"]    = "WARNING"
    faults["description"] = faults.apply(
        lambda r: f"String current {r['value']:.2f} A deviates {r['deviation']*100:.1f}% "
                  f"from SCB mean {r['scb_mean']:.2f} A",
        axis=1
    )

    logger.info(f"scb_unbalance: {len(faults)} fault events detected.")
    return faults[FAULT_COLUMNS]


# ── Fault Registry ────────────────────────────────────────────────────────────
# Add new fault rule functions here. They will be discovered by run_all_faults().

FAULT_REGISTRY: Dict[str, Any] = {
    "low_current_fault":   low_current_fault,
    "string_disconnected": string_disconnected,
    "scb_unbalance":       scb_unbalance,
}


# ── Run All Faults ─────────────────────────────────────────────────────────────

def run_all_faults(df: pd.DataFrame) -> pd.DataFrame:
    """
    Execute all registered fault rules against the provided DataFrame.

    Args:
        df : Long-format time-series DataFrame (from query_engine.fetch_timeseries).

    Returns:
        Combined DataFrame of all fault events from all rules.
    """
    all_results = []

    for fault_name, fault_fn in FAULT_REGISTRY.items():
        try:
            result = fault_fn(df)
            if not result.empty:
                all_results.append(result)
        except Exception as e:
            logger.error(f"Fault rule '{fault_name}' crashed: {e}")

    if all_results:
        return pd.concat(all_results).sort_values("timestamp").reset_index(drop=True)

    return _empty_result()
