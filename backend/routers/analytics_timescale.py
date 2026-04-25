"""
TimescaleDB helpers for Analytics Lab /timeseries (continuous aggregate solar_raw_data_1m_cagg).

See backend/docs/PERF_AND_TIMESCALE.md for rollout.
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any, List, Optional, Sequence

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

log = logging.getLogger(__name__)

CAGG_RELNAME = "solar_raw_data_1m_cagg"


def analytics_use_cagg() -> bool:
    return os.environ.get("SOLAR_ANALYTICS_USE_TIMESCALE_CAGG", "").strip().lower() in ("1", "true", "yes")


def timescaledb_present(db: "Session") -> bool:
    try:
        from sqlalchemy import text

        return bool(db.execute(text("SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'")).scalar())
    except Exception:
        return False


def cagg_exists(db: "Session") -> bool:
    try:
        from sqlalchemy import text

        r = db.execute(
            text("SELECT to_regclass('public.solar_raw_data_1m_cagg')::text")
        ).scalar()
        return bool(r and r != "")
    except Exception:
        return False


def maybe_log_cagg_lag(db: "Session") -> None:
    if os.environ.get("SOLAR_TIMESCALE_CAGG_LOG_LAG", "").strip().lower() not in ("1", "true", "yes"):
        return
    try:
        from sqlalchemy import text

        rows = db.execute(
            text(
                """
                SELECT job_id, hypertable_schema, hypertable_name, last_run_status, last_successful_finish
                FROM timescaledb_information.job_stats
                LIMIT 8
                """
            )
        ).fetchall()
        log.info("timescaledb_information.job_stats rows=%s", rows)
    except Exception as exc:
        log.debug("timescale job_stats unavailable: %s", exc)


def timescale_status_payload(db: "Session") -> dict[str, Any]:
    """Admin JSON: extension, CAGG presence, null timestamp_ts count, env flags."""
    from sqlalchemy import text

    out: dict[str, Any] = {
        "timescaledb_extension": False,
        "cagg_relname": CAGG_RELNAME,
        "cagg_exists": False,
        "timestamp_ts_column": False,
        "raw_data_null_timestamp_ts": None,
        "env_SOLAR_ANALYTICS_USE_TIMESCALE_CAGG": analytics_use_cagg(),
        "env_SOLAR_TIMESCALE_CAGG_LOG_LAG": os.environ.get("SOLAR_TIMESCALE_CAGG_LOG_LAG", ""),
    }
    try:
        out["timescaledb_extension"] = timescaledb_present(db)
    except Exception as exc:
        out["timescaledb_extension_error"] = str(exc)
    try:
        r = db.execute(
            text(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_schema='public' AND table_name='raw_data_generic' "
                "AND column_name='timestamp_ts'"
            )
        ).scalar()
        out["timestamp_ts_column"] = bool(r)
    except Exception as exc:
        out["timestamp_ts_column_error"] = str(exc)
    try:
        out["cagg_exists"] = cagg_exists(db)
    except Exception:
        pass
    try:
        out["raw_data_null_timestamp_ts"] = int(
            db.execute(
                text("SELECT count(*) FROM raw_data_generic WHERE timestamp_ts IS NULL")
            ).scalar()
            or 0
        )
    except Exception:
        pass
    return out


def fetch_cagg_minute_rows(
    db: "Session",
    plant_id: str,
    equipment_ids: Sequence[str],
    query_signals: Sequence[str],
    from_ts: str,
    to_ts: str,
) -> Optional[List[Any]]:
    """
    Return SQLAlchemy Row objects (timestamp, equipment_id, signal, value, precedence)
    for the primary raw branch, or None to fall back to classic SQL.
    """
    if not analytics_use_cagg() or not timescaledb_present(db) or not cagg_exists(db):
        return None

    clean_ids = [str(i).strip().replace("'", "''") for i in equipment_ids if str(i).strip()]
    clean_sigs = [str(s).strip().replace("'", "''") for s in query_signals if str(s).strip()]
    if not clean_ids or not clean_sigs:
        return None

    id_placeholders = ",".join(f"'{i}'" for i in clean_ids)
    sig_placeholders = ",".join(f"'{s}'" for s in clean_sigs)

    from sqlalchemy import text

    sql = text(
        f"""
        SELECT bucket AS timestamp, equipment_id, signal, value_avg AS value, 1 AS precedence
        FROM {CAGG_RELNAME}
        WHERE plant_id = :plant_id
          AND equipment_id IN ({id_placeholders})
          AND signal IN ({sig_placeholders})
          AND bucket >= CAST(:from_ts AS timestamptz)
          AND bucket <= CAST(:to_ts AS timestamptz)
        ORDER BY bucket, equipment_id, signal
        """
    )
    try:
        rows = db.execute(
            sql,
            {"plant_id": plant_id, "from_ts": from_ts, "to_ts": to_ts},
        ).fetchall()
        maybe_log_cagg_lag(db)
        return list(rows)
    except Exception as exc:
        log.warning("analytics CAGG read failed, falling back: %s", exc)
        return None
