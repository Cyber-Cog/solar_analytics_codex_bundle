"""
common/templates.py
===================
Solar Analytics Platform — Excel Template Definitions

Defines the exact column structure for each downloadable template.
When users download a template and fill it in, the validators module
knows exactly what columns to expect.

Each template is returned as a pandas DataFrame with empty rows
(just the header row) and sample rows to guide the user.
"""

import io
import pandas as pd


# ── Template Definitions ──────────────────────────────────────────────────────
# Each template is a dictionary mapping the template name to its column list.

TEMPLATE_COLUMNS = {
    "Inverter Data": [
        "timestamp",
        "equipment_id",       # e.g. INV-01
        "dc_current",         # A
        "dc_voltage",         # V
        "dc_power",           # kW
        "ac_power",           # kW
    ],
    "SCB Data": [
        "timestamp",
        "equipment_id",       # e.g. INV-01-SCB-01
        "dc_current",         # A
        "dc_voltage",         # V
        "dc_power",           # kW
    ],
    "String Data": [
        "timestamp",
        "equipment_id",       # e.g. INV-01-SCB-01-STR-01
        "dc_current",         # A
        "dc_voltage",         # V
        "dc_power",           # kW
    ],
    "WMS Data": [
        "timestamp",
        "equipment_id",       # e.g. PLANT-WMS-01
        "irradiance",         # W/m²
        "temperature",        # °C
        "wind_speed",         # m/s
    ],
}

# Sample rows help users understand the expected format
TEMPLATE_SAMPLES = {
    "Inverter Data": [
        ["2025-01-01 05:00:00", "INV-01", 180.5, 550.0, 99.3, 95.1],
        ["2025-01-01 05:15:00", "INV-01", 182.0, 551.0, 100.2, 96.0],
    ],
    "SCB Data": [
        ["2025-01-01 05:00:00", "INV-01-SCB-01", 80.2, 548.0, 43.9],
        ["2025-01-01 05:00:00", "INV-01-SCB-02", 81.5, 549.0, 44.7],
    ],
    "String Data": [
        ["2025-01-01 05:00:00", "INV-01-SCB-01-STR-01", 8.5, 545.0, 4.63],
        ["2025-01-01 05:00:00", "INV-01-SCB-01-STR-02", 8.6, 546.0, 4.70],
    ],
    "WMS Data": [
        ["2025-01-01 05:00:00", "PLANT-WMS-01", 820.0, 25.3, 3.2],
        ["2025-01-01 05:15:00", "PLANT-WMS-01", 830.5, 25.8, 3.0],
    ],
}

# Maps template name to the equipment level it belongs to
TEMPLATE_LEVEL_MAP = {
    "Inverter Data": "inverter",
    "SCB Data":      "scb",
    "String Data":   "string",
    "WMS Data":      "plant",
}


def build_template_dataframe(template_name: str) -> pd.DataFrame:
    """
    Build and return a pandas DataFrame for a given template.

    Args:
        template_name : One of the keys in TEMPLATE_COLUMNS.

    Returns:
        DataFrame with columns populated and sample rows included.

    Raises:
        ValueError if template_name is not recognised.
    """
    if template_name not in TEMPLATE_COLUMNS:
        raise ValueError(f"Unknown template: '{template_name}'. "
                         f"Valid options: {list(TEMPLATE_COLUMNS.keys())}")

    cols    = TEMPLATE_COLUMNS[template_name]
    samples = TEMPLATE_SAMPLES.get(template_name, [])
    df = pd.DataFrame(samples, columns=cols)
    return df


def get_template_as_excel_bytes(template_name: str) -> bytes:
    """
    Build a template DataFrame and return it as an in-memory Excel file.

    Args:
        template_name : Template name string.

    Returns:
        Raw bytes of the .xlsx file, ready for download endpoints or disk write.
    """
    df = build_template_dataframe(template_name)

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Data")

        # Format the header row for clarity
        workbook  = writer.book
        worksheet = writer.sheets["Data"]
        header_fmt = workbook.add_format({
            "bold":       True,
            "font_color": "#FFFFFF",
            "bg_color":   "#1A3A5C",
            "border":     1,
        })
        for col_num, col_name in enumerate(df.columns):
            worksheet.write(0, col_num, col_name, header_fmt)
            worksheet.set_column(col_num, col_num, 22)

    return buffer.getvalue()


def get_all_template_names() -> list:
    """Return all available template names."""
    return list(TEMPLATE_COLUMNS.keys())
