"""
modules/ingestion/wms_ingestor.py
==================================
Solar Analytics Platform — Weather Monitoring Station Ingestor

Handles ingestion of WMS (Weather Monitoring Station) data.
WMS data contains plant-level signals: irradiance, temperature, wind_speed.

This module can be extended in the future to pull data from APIs
(e.g., Solargis, SolarEdge Monitoring, direct WMS RS485 feeds).
"""

import pandas as pd
from io import BytesIO
from typing import Tuple, List

from common.database import execute_many
from common.helpers import get_logger, normalise_timestamp

logger = get_logger(__name__, "ingestion.log")

# INSERT SQL — same target table as other ingestors
_INSERT_SQL = """
    INSERT INTO raw_data_generic
        (timestamp, equipment_level, equipment_id, signal, value, source)
    VALUES (?, ?, ?, ?, ?, ?)
"""

# WMS signals expected in the DataFrame
WMS_SIGNALS = ["irradiance", "temperature", "wind_speed"]


def ingest_wms_dataframe(df: pd.DataFrame, plant_id: str, source: str = "wms_upload") -> int:
    """
    Ingest a WMS DataFrame (already loaded) into raw_data_generic.

    Expected columns:
        timestamp | irradiance | temperature | wind_speed

    Args:
        df       : WMS DataFrame.
        plant_id : Plant equipment ID, e.g. 'PLANT-WMS-01'.
        source   : Data source label (default: 'wms_upload').

    Returns:
        Number of records inserted.
    """
    records = []

    for _, row in df.iterrows():
        ts = normalise_timestamp(str(row.get("timestamp", "")))
        if ts is None:
            logger.warning(f"Skipping WMS row — bad timestamp: {row.get('timestamp')}")
            continue

        for signal in WMS_SIGNALS:
            if signal not in row:
                continue
            try:
                raw = row[signal]
                value = float(raw) if (raw is not None and str(raw).strip() not in ("", "nan", "N/A")) else None
            except (ValueError, TypeError):
                value = None

            records.append((ts, "plant", plant_id, signal, value, source))

    if records:
        execute_many(_INSERT_SQL, records)
        logger.info(f"WMS: inserted {len(records)} records for plant '{plant_id}'.")

    return len(records)


def ingest_wms_file(
    file_bytes: bytes,
    filename: str,
    plant_id: str,
    source: str = "wms_upload",
) -> Tuple[int, List[str]]:
    """
    Load a WMS CSV/Excel file and ingest it.

    Args:
        file_bytes : Raw bytes of the uploaded file.
        filename   : Original filename (used to detect extension).
        plant_id   : WMS plant equipment ID.
        source     : Origin label.

    Returns:
        (rows_inserted, list_of_warnings)
    """
    warnings = []
    buf = BytesIO(file_bytes)
    ext = filename.rsplit(".", 1)[-1].lower()

    try:
        if ext == "csv":
            df = pd.read_csv(buf, dtype=str)
        elif ext in {"xlsx", "xls"}:
            df = pd.read_excel(buf, dtype=str)
        else:
            return 0, [f"Unsupported file type: '.{ext}'"]
    except Exception as e:
        return 0, [f"Could not read WMS file: {e}"]

    df.columns = [c.strip() for c in df.columns]

    if "timestamp" not in df.columns:
        return 0, ["WMS file must contain a 'timestamp' column."]

    count = ingest_wms_dataframe(df, plant_id, source)

    if count == 0:
        warnings.append("No valid WMS records were found in the file.")

    return count, warnings
