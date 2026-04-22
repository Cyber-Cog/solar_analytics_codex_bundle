"""
modules/data_setup/validators.py
================================
Solar Analytics Platform — Data Validation Functions

Validates uploaded CSV/Excel data before it is written to the database.
Returns a list of error messages (empty list = valid data).

Rules:
  1. Required columns must exist.
  2. 'timestamp' column must be parseable.
  3. 'equipment_id' must follow the naming convention.
  4. Numeric signal columns must be numeric (NaN is allowed).
"""

import pandas as pd
from typing import List
from common.helpers import normalise_timestamp, is_valid_equipment_id, VALID_SIGNALS
from common.templates import TEMPLATE_COLUMNS, TEMPLATE_LEVEL_MAP


def validate_columns(df: pd.DataFrame, template_name: str) -> List[str]:
    """
    Check that all required columns for the given template are present.

    Args:
        df            : Uploaded DataFrame.
        template_name : Name of the template (e.g. 'String Data').

    Returns:
        List of error strings. Empty list = column check passed.
    """
    errors = []
    required = TEMPLATE_COLUMNS.get(template_name, [])

    for col in required:
        if col not in df.columns:
            errors.append(f"Missing required column: '{col}'")

    return errors


def validate_timestamps(df: pd.DataFrame) -> List[str]:
    """
    Attempt to parse every value in the 'timestamp' column.

    Returns up to 5 error messages with the first bad rows.
    """
    errors = []
    if "timestamp" not in df.columns:
        return ["Column 'timestamp' not found — cannot validate timestamps."]

    bad_rows = []
    for idx, ts in enumerate(df["timestamp"]):
        if normalise_timestamp(str(ts)) is None:
            bad_rows.append((idx + 2, ts))  # +2 because row 1 is the header
            if len(bad_rows) >= 5:
                break

    for row_num, ts_val in bad_rows:
        errors.append(f"Row {row_num}: Cannot parse timestamp '{ts_val}'")

    return errors


def validate_equipment_ids(df: pd.DataFrame, template_name: str) -> List[str]:
    """
    Check that all equipment_id values match the naming convention
    for the level declared in the template.

    Returns up to 10 error messages.
    """
    errors = []
    if "equipment_id" not in df.columns:
        return ["Column 'equipment_id' not found."]

    level = TEMPLATE_LEVEL_MAP.get(template_name)
    if level is None:
        return []

    bad_rows = []
    for idx, eq_id in enumerate(df["equipment_id"]):
        if not is_valid_equipment_id(str(eq_id).strip(), level):
            bad_rows.append((idx + 2, eq_id))
            if len(bad_rows) >= 10:
                break

    for row_num, eq_id in bad_rows:
        errors.append(
            f"Row {row_num}: Invalid equipment_id '{eq_id}' for level '{level}'. "
            f"Expected pattern — inverter: INV-01, scb: INV-01-SCB-01, "
            f"string: INV-01-SCB-01-STR-01"
        )
    return errors


def validate_numeric_columns(df: pd.DataFrame, template_name: str) -> List[str]:
    """
    Verify that all signal columns can be converted to numeric.
    Blank cells (NaN) are allowed and skipped.

    Returns up to 5 error messages.
    """
    errors = []
    required_cols = TEMPLATE_COLUMNS.get(template_name, [])
    signal_cols = [c for c in required_cols if c in VALID_SIGNALS and c in df.columns]

    for col in signal_cols:
        non_numeric = df[col].apply(
            lambda v: pd.isna(v) is False and not _is_numeric(v)
        )
        bad_indices = df.index[non_numeric].tolist()[:5]
        for idx in bad_indices:
            errors.append(
                f"Column '{col}', Row {idx + 2}: "
                f"Non-numeric value '{df.at[idx, col]}'"
            )

    return errors


def _is_numeric(value) -> bool:
    """Return True if value can be converted to float."""
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False


def run_all_validations(df: pd.DataFrame, template_name: str) -> List[str]:
    """
    Run all validation checks in sequence.

    Args:
        df            : Uploaded DataFrame.
        template_name : Name of the chosen template.

    Returns:
        Combined list of all errors. Empty list = data is valid.
    """
    all_errors = []
    all_errors += validate_columns(df, template_name)

    # Stop early if columns are wrong — subsequent checks will fail
    if all_errors:
        return all_errors

    all_errors += validate_timestamps(df)
    all_errors += validate_equipment_ids(df, template_name)
    all_errors += validate_numeric_columns(df, template_name)

    return all_errors
