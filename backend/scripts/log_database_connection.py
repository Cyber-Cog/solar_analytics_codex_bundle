#!/usr/bin/env python3
"""
Log database connection details for troubleshooting.

Run from repo root or backend/:
  python backend/scripts/log_database_connection.py
  python backend/scripts/log_database_connection.py --read
  python backend/scripts/log_database_connection.py --url postgresql://user:pass@host:5432/db
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError


BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from script_env import load_backend_env  # noqa: E402


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Attempt a PostgreSQL connection and log the result."
    )
    parser.add_argument(
        "--read",
        action="store_true",
        help="Use DATABASE_URL_READ instead of DATABASE_URL.",
    )
    parser.add_argument(
        "--url",
        help="Optional explicit PostgreSQL URL override.",
    )
    return parser.parse_args()


def resolve_url(args: argparse.Namespace) -> tuple[str, str]:
    if args.url:
        return args.url.strip(), "explicit --url"
    if args.read:
        value = (os.environ.get("DATABASE_URL_READ") or os.environ.get("DATABASE_URL") or "").strip()
        return value, "DATABASE_URL_READ"
    return (os.environ.get("DATABASE_URL") or "").strip(), "DATABASE_URL"


def mask_url(raw_url: str) -> str:
    url = make_url(raw_url)
    return url.render_as_string(hide_password=True)


def main() -> int:
    configure_logging()
    load_backend_env()
    args = parse_args()
    raw_url, source = resolve_url(args)

    if not raw_url:
        logging.error("No database URL found from %s.", source)
        logging.error("Set DATABASE_URL in backend/.env or pass --url.")
        return 1

    try:
        masked = mask_url(raw_url)
    except Exception as exc:
        logging.exception("Invalid database URL from %s: %s", source, exc)
        return 1

    logging.info("Using database URL from %s", source)
    logging.info("Connecting to %s", masked)

    engine = create_engine(raw_url, pool_pre_ping=True)
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    select
                        current_database() as database_name,
                        current_user as session_user,
                        inet_server_addr()::text as server_address,
                        inet_server_port() as server_port,
                        version() as server_version
                    """
                )
            ).mappings().one()
            logging.info("Connection successful.")
            logging.info("Database: %s", row["database_name"])
            logging.info("User: %s", row["session_user"])
            logging.info("Server: %s:%s", row["server_address"], row["server_port"])
            logging.info("Version: %s", row["server_version"])
            return 0
    except SQLAlchemyError as exc:
        logging.exception("Database connection failed: %s", exc)
        return 1
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
