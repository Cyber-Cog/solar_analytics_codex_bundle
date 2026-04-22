"""
modules/ingestion/raw_excel_ingestor.py
=======================================
Solar Analytics Platform — Raw Excel Ingestor

A lower-level batch ingestion script for power users / system admins.
Use this when uploading large files programmatically (not via the UI).

Usage:
    python modules/ingestion/raw_excel_ingestor.py \
        --file path/to/data.xlsx \
        --template "String Data"
"""

import argparse
import sys
import os

# Allow running this script from the project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from common.helpers import get_logger
from modules.data_setup.validators import run_all_validations
from modules.data_setup.uploader import load_file, upload_file_to_db
from common.templates import get_all_template_names

logger = get_logger(__name__, "ingestion.log")


def ingest(file_path: str, template_name: str, skip_validation: bool = False) -> int:
    """
    Ingest an Excel or CSV file into raw_data_generic.

    Args:
        file_path       : Absolute or relative path to the file.
        template_name   : Template name string (must match TEMPLATE_COLUMNS keys).
        skip_validation : If True, bypass validation checks. USE WITH CAUTION.

    Returns:
        Number of records inserted.
    """
    logger.info(f"Starting ingestion: file='{file_path}', template='{template_name}'")

    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return 0

    with open(file_path, "rb") as f:
        file_bytes = f.read()

    filename = os.path.basename(file_path)

    # Load into DataFrame
    try:
        df = load_file(file_bytes, filename)
    except ValueError as e:
        logger.error(f"Cannot load file: {e}")
        return 0

    # Validation step
    if not skip_validation:
        errors = run_all_validations(df, template_name)
        if errors:
            logger.error(f"Validation failed with {len(errors)} error(s):")
            for err in errors:
                logger.error(f"  → {err}")
            return 0
        else:
            logger.info("Validation passed.")

    # Upload
    count, warnings = upload_file_to_db(file_bytes, filename, template_name)
    for w in warnings:
        logger.warning(w)

    logger.info(f"Ingestion complete. Inserted {count} records.")
    return count


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Ingest solar data from Excel/CSV into the database."
    )
    parser.add_argument(
        "--file", required=True, help="Path to the Excel or CSV file."
    )
    parser.add_argument(
        "--template",
        required=True,
        choices=get_all_template_names(),
        help="Template type matching the file structure.",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip all data validation checks (not recommended).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    result = ingest(args.file, args.template, args.skip_validation)
    print(f"Records inserted: {result}")
