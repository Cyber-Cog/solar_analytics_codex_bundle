"""
modules/data_setup/uploader.py
==============================
Solar Analytics Platform — File Upload & DB Insertion

Handles:
  1. Loading uploaded CSV/Excel files into DataFrames.
  2. Transforming wide-format rows into the long-format
     required by raw_data_generic (one row per signal per timestamp).
  3. Bulk-inserting records into the database.

Wide format (how users upload):
  timestamp | equipment_id | dc_current | dc_voltage | dc_power

Long format (how DB stores):
  timestamp | equipment_level | equipment_id | signal | value | source
"""

import pandas as pd
from io import BytesIO
from typing import Tuple, List

from common.database import execute_many
from common.helpers import (
    get_logger,
    normalise_timestamp,
    derive_level_from_id,
    VALID_SIGNALS,
)
from common.templates import TEMPLATE_LEVEL_MAP

logger = get_logger(__name__, "ingestion.log")

# INSERT statement for raw_data_generic
_INSERT_SQL = """
    INSERT INTO raw_data_generic
        (timestamp, equipment_level, equipment_id, signal, value, source)
    VALUES (?, ?, ?, ?, ?, ?)
"""


def load_file(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """
    Load a CSV or XLSX file from bytes into a DataFrame.

    Args:
        file_bytes : Raw bytes from the uploaded file.
        filename   : Original filename (used to detect CSV vs XLSX).

    Returns:
        pandas.DataFrame with headers from the first row.

    Raises:
        ValueError if the file extension is not supported.
    """
    buf = BytesIO(file_bytes)
    ext = filename.rsplit(".", 1)[-1].lower()

    if ext == "csv":
        df = pd.read_csv(buf, dtype=str)
    elif ext in {"xlsx", "xls"}:
        df = pd.read_excel(buf, dtype=str)
    else:
        raise ValueError(f"Unsupported file type: '.{ext}'. Only CSV and XLSX accepted.")

    # Strip column name whitespace
    df.columns = [c.strip() for c in df.columns]
    logger.info(f"Loaded file '{filename}' — {len(df)} rows, {len(df.columns)} columns.")
    return df


def wide_to_long(df: pd.DataFrame, template_name: str) -> List[tuple]:
    """
    Transform a wide-format DataFrame into a list of DB row tuples.

    Each signal column becomes a separate row:
      (timestamp, equipment_level, equipment_id, signal, value, source)

    Args:
        df            : Wide-format DataFrame (output of load_file).
        template_name : Template name to determine equipment level.

    Returns:
        List of tuples ready for executemany INSERT.
    """
    records    = []
    level      = TEMPLATE_LEVEL_MAP.get(template_name, None)
    signal_cols = [c for c in df.columns if c in VALID_SIGNALS]

    for _, row in df.iterrows():
        # Normalise timestamp
        ts = normalise_timestamp(str(row.get("timestamp", "")))
        if ts is None:
            logger.warning(f"Skipping row — unparseable timestamp: {row.get('timestamp')}")
            continue

        eq_id = str(row.get("equipment_id", "")).strip()

        # Derive level from ID if not already known
        eq_level = level or derive_level_from_id(eq_id)
        if eq_level is None:
            logger.warning(f"Skipping row — cannot determine level for: {eq_id}")
            continue

        for signal in signal_cols:
            raw_val = row.get(signal)
            try:
                value = float(raw_val) if (raw_val is not None and str(raw_val).strip() not in {"", "nan", "NaN", "N/A"}) else None
            except (ValueError, TypeError):
                value = None

            records.append((ts, eq_level, eq_id, signal, value, "excel_upload"))

    logger.info(f"Converted {len(df)} rows → {len(records)} signal records.")
    return records


def insert_records(records: List[tuple]) -> int:
    """
    Bulk-insert a list of signal records into raw_data_generic.

    Args:
        records : List of tuples from wide_to_long().

    Returns:
        Number of rows inserted.
    """
    count = execute_many(_INSERT_SQL, records)
    logger.info(f"Inserted {len(records)} records into raw_data_generic.")
    return len(records)


def upload_file_to_db(
    file_bytes: bytes,
    filename: str,
    template_name: str,
) -> Tuple[int, List[str]]:
    """
    Complete pipeline: load → transform → insert.

    Args:
        file_bytes    : Raw bytes from the uploaded file.
        filename      : Original filename.
        template_name : Template type used to parse the file.

    Returns:
        (rows_inserted, list_of_warning_strings)
    """
    warnings = []

    df = load_file(file_bytes, filename)
    records = wide_to_long(df, template_name)

    if not records:
        warnings.append("No valid records found after processing the file.")
        return 0, warnings

    count = insert_records(records)
    return count, warnings
