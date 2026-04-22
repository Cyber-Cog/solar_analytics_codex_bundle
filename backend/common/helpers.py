"""
common/helpers.py
=================
Solar Analytics Platform — General Purpose Utilities

Small, reusable helper functions used across all modules.
No UI logic. No database logic. Pure Python utilities.
"""

import re
import os
import logging
from datetime import datetime
from typing import Optional

# ── Logging Setup ─────────────────────────────────────────────────────────────
_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_LOG_DIR  = os.path.join(_ROOT_DIR, "logs")


def get_logger(name: str, log_file: str) -> logging.Logger:
    """
    Create and return a logger that writes to both console and a log file.

    Args:
        name     : Logger name (usually module __name__).
        log_file : Filename (not full path) inside the logs/ directory.

    Returns:
        Configured logging.Logger instance.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # Already configured — avoid duplicate handlers

    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")

    # File handler
    fh = logging.FileHandler(os.path.join(_LOG_DIR, log_file), encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


# ── Naming Convention Helpers ─────────────────────────────────────────────────

# Regex patterns for valid equipment IDs
_INVERTER_PATTERN = re.compile(r"^INV-\d{2,}$")
_SCB_PATTERN      = re.compile(r"^INV-\d{2,}-SCB-\d{2,}$")
_STRING_PATTERN   = re.compile(r"^INV-\d{2,}-SCB-\d{2,}-STR-\d{2,}$")
_PLANT_PATTERN    = re.compile(r"^PLANT-[A-Z0-9\-]+$")

# Valid signal names (canonical list)
VALID_SIGNALS = {
    "dc_current", "dc_voltage", "dc_power",
    "ac_power", "irradiance", "temperature", "wind_speed"
}

# Valid equipment levels
VALID_LEVELS = {"plant", "inverter", "scb", "string"}


def is_valid_inverter_id(eq_id: str) -> bool:
    """Return True if eq_id matches INV-XX format."""
    return bool(_INVERTER_PATTERN.match(eq_id))


def is_valid_scb_id(eq_id: str) -> bool:
    """Return True if eq_id matches INV-XX-SCB-XX format."""
    return bool(_SCB_PATTERN.match(eq_id))


def is_valid_string_id(eq_id: str) -> bool:
    """Return True if eq_id matches INV-XX-SCB-XX-STR-XX format."""
    return bool(_STRING_PATTERN.match(eq_id))


def is_valid_equipment_id(eq_id: str, level: str) -> bool:
    """
    Validate equipment ID against its declared hierarchy level.

    Args:
        eq_id : Equipment identifier string.
        level : One of 'plant', 'inverter', 'scb', 'string'.

    Returns:
        True if the ID matches the pattern for the given level.
    """
    if level == "inverter":
        return is_valid_inverter_id(eq_id)
    elif level == "scb":
        return is_valid_scb_id(eq_id)
    elif level == "string":
        return is_valid_string_id(eq_id)
    elif level == "plant":
        return bool(_PLANT_PATTERN.match(eq_id))
    return False


def derive_level_from_id(eq_id: str) -> Optional[str]:
    """
    Automatically determine the hierarchy level from the equipment ID.

    Examples:
        INV-01               → 'inverter'
        INV-01-SCB-01        → 'scb'
        INV-01-SCB-01-STR-01 → 'string'
        PLANT-WMS-01         → 'plant'

    Returns:
        Level string or None if the ID is unrecognised.
    """
    if is_valid_string_id(eq_id):
        return "string"
    elif is_valid_scb_id(eq_id):
        return "scb"
    elif is_valid_inverter_id(eq_id):
        return "inverter"
    elif _PLANT_PATTERN.match(eq_id):
        return "plant"
    return None


def extract_parent_scb(string_id: str) -> Optional[str]:
    """
    Extract the parent SCB ID from a string ID.

    Example:
        INV-01-SCB-02-STR-04 → INV-01-SCB-02
    """
    parts = string_id.rsplit("-STR-", 1)
    return parts[0] if len(parts) == 2 else None


def extract_parent_inverter(scb_id: str) -> Optional[str]:
    """
    Extract the parent inverter ID from an SCB ID.

    Example:
        INV-01-SCB-02 → INV-01
    """
    parts = scb_id.rsplit("-SCB-", 1)
    return parts[0] if len(parts) == 2 else None


# ── Timestamp Helpers ─────────────────────────────────────────────────────────

def parse_timestamp(ts: str) -> Optional[datetime]:
    """
    Try common timestamp formats and return a datetime object.
    Returns None if parsing fails.

    Supported formats:
        2025-01-01 05:00:00
        2025-01-01 05:00
        2025-01-01T05:00
        2025-01-01T05:00:00
        01/01/2025 05:00
    """
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%d/%m/%Y %H:%M",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(str(ts).strip(), fmt)
        except ValueError:
            continue
    return None


def normalise_timestamp(ts: str) -> Optional[str]:
    """
    Parse and re-format a timestamp to the canonical ISO format.

    Returns:
        'YYYY-MM-DD HH:MM:SS' string or None if parsing fails.
    """
    dt = parse_timestamp(ts)
    return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else None


# ── Number Helpers ────────────────────────────────────────────────────────────

def safe_float(value) -> Optional[float]:
    """
    Convert a value to float, returning None on failure.
    Handles blank strings, 'N/A', '-', None, etc.
    """
    if value is None:
        return None
    s = str(value).strip()
    if s in {"", "N/A", "NA", "-", "nan", "NaN"}:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def format_number(value: float, decimals: int = 2) -> str:
    """Format a number to a fixed number of decimal places."""
    if value is None:
        return "—"
    return f"{value:.{decimals}f}"


# ── File Helpers ──────────────────────────────────────────────────────────────

def ensure_dir(path: str) -> None:
    """Create a directory (and parents) if it does not already exist."""
    os.makedirs(path, exist_ok=True)
