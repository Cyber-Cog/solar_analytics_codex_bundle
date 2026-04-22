"""
backend/db_perf.py
==================
Database performance objects: indexes, ANALYZE, and query routing helpers.

Exports:
  ensure_performance_objects(engine)
  ensure_performance_objects_bg(engine)
  choose_data_table(db, plant_id, from, to)
  refresh_15m_cache(db, plant_id, min, max)
"""

import logging
import os

logger = logging.getLogger(__name__)

_DDL_LOCK_TIMEOUT_MS = int(os.environ.get("SOLAR_DDL_LOCK_TIMEOUT_MS", "5000"))
_DDL_STATEMENT_TIMEOUT_MS = int(
    os.environ.get("SOLAR_DDL_STATEMENT_TIMEOUT_MS", "600000")
)


_FAST_INDEXES = [
    """
    CREATE INDEX IF NOT EXISTS idx_fd_plant_ts
        ON fault_diagnostics (plant_id, timestamp)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fd_plant_scb
        ON fault_diagnostics (plant_id, scb_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fe_plant_status
        ON fault_episodes (plant_id, status)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fd_plant_status_ts
        ON fault_diagnostics (plant_id, fault_status, timestamp)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fed_episode_day
        ON fault_episode_days (episode_id, day)
    """,
]


_BG_INDEXES = [
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rdg_plant_equip_signal_ts "
    "ON raw_data_generic (plant_id, equipment_id, signal, timestamp)",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dchd_plant_equip_signal_ts "
    "ON dc_hierarchy_derived (plant_id, equipment_id, signal, timestamp)",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rdg_plant_equip_ts "
    "ON raw_data_generic (plant_id, equipment_id, timestamp)",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rdg_plant_level_signal_ts "
    "ON raw_data_generic (plant_id, equipment_level, signal, timestamp)",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rdg_plant_signal_equip "
    "ON raw_data_generic (plant_id, signal, equipment_id)",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dchd_plant_equip_ts "
    "ON dc_hierarchy_derived (plant_id, equipment_id, timestamp)",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dchd_plant_level_signal_ts "
    "ON dc_hierarchy_derived (plant_id, equipment_level, signal, timestamp)",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rdg_inv_ac_power_ts "
    "ON raw_data_generic (plant_id, timestamp, equipment_id) "
    "WHERE LOWER(TRIM(equipment_level::text)) = 'inverter' "
    "AND LOWER(TRIM(signal::text)) = 'ac_power'",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rdg_wms_irr_ts "
    "ON raw_data_generic (plant_id, timestamp, signal) "
    "WHERE LOWER(TRIM(equipment_level::text)) IN ('plant', 'wms') "
    "AND LOWER(TRIM(signal::text)) IN ('gti', 'irradiance', 'ghi')",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_pa_plant_inv_scb "
    "ON plant_architecture (plant_id, inverter_id, scb_id)",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_frs_plant_kind "
    "ON fault_runtime_snapshot (plant_id, kind)",
]

_ANALYZE_TABLES = [
    "raw_data_generic",
    "dc_hierarchy_derived",
    "fault_diagnostics",
    "fault_episodes",
    "fault_episode_days",
    "plant_architecture",
]


def ensure_performance_objects(engine) -> None:
    """Apply only startup-safe performance objects."""
    try:
        with engine.connect() as conn:
            for ddl in _FAST_INDEXES:
                try:
                    conn.execute(_text(ddl))
                except Exception as exc:
                    logger.debug("db_perf fast index skipped: %s", exc)
            _commit(conn)
        logger.info("db_perf: fast performance objects applied.")
    except Exception as exc:
        logger.warning("db_perf.ensure_performance_objects failed (non-fatal): %s", exc)


def ensure_performance_objects_bg(engine) -> None:
    """Apply heavier indexes and ANALYZE in the background."""
    if os.environ.get("SOLAR_WARMUP_ON_BOOT", "").strip().lower() == "skip":
        logger.info("db_perf: warmup skipped (SOLAR_WARMUP_ON_BOOT=skip)")
        return

    for ddl in _BG_INDEXES:
        try:
            with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                conn.execute(_text(f"SET lock_timeout = {_DDL_LOCK_TIMEOUT_MS}"))
                conn.execute(_text(f"SET statement_timeout = {_DDL_STATEMENT_TIMEOUT_MS}"))
                conn.execute(_text(ddl))
        except Exception as exc:
            logger.info("db_perf bg index deferred: %s", str(exc)[:200])

    try:
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(_text(f"SET lock_timeout = {_DDL_LOCK_TIMEOUT_MS}"))
            for table in _ANALYZE_TABLES:
                try:
                    conn.execute(_text(f"ANALYZE {table}"))
                except Exception as exc:
                    logger.debug("db_perf ANALYZE %s skipped: %s", table, exc)
        logger.info("db_perf: background performance objects applied.")
    except Exception as exc:
        logger.warning(
            "db_perf.ensure_performance_objects_bg ANALYZE failed (non-fatal): %s",
            exc,
        )


def choose_data_table(db, plant_id: str, date_from: str, date_to: str) -> str:
    """
    Return the best table for a given plant + date range.

    Preference order:
      1. dc_hierarchy_derived if it has rows for the requested range
      2. raw_data_generic as the universal fallback
    """
    cache_key = f"_cdt:{plant_id}:{date_from}:{date_to}"
    try:
        from dashboard_cache import get_any

        cached = get_any(cache_key, 60)
        if cached is not None:
            return cached
    except Exception:
        pass

    table = "raw_data_generic"
    try:
        from sqlalchemy import text as _t

        f_ts = f"{date_from} 00:00:00"
        t_ts = f"{date_to} 23:59:59"
        row = db.execute(
            _t(
                "SELECT 1 FROM dc_hierarchy_derived "
                "WHERE plant_id = :p AND timestamp BETWEEN :f AND :t "
                "LIMIT 1"
            ),
            {"p": plant_id, "f": f_ts, "t": t_ts},
        ).fetchone()
        if row:
            table = "dc_hierarchy_derived"
    except Exception:
        pass

    try:
        from dashboard_cache import set_any

        set_any(cache_key, table, 60)
    except Exception:
        pass

    return table


def refresh_15m_cache(db, plant_id: str, min_ts: str, max_ts: str) -> None:
    """Invalidate dashboard and fault caches after new telemetry uploads."""
    try:
        from dashboard_cache import invalidate_plant

        invalidate_plant(plant_id)
    except Exception:
        pass

    try:
        from sqlalchemy import text as _t

        db.execute(
            _t("DELETE FROM fault_cache WHERE cache_key LIKE :pat"),
            {"pat": f"%{plant_id}%"},
        )
        db.commit()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass

    logger.info(
        "db_perf.refresh_15m_cache: cache invalidated for plant=%s range=%s->%s",
        plant_id,
        min_ts,
        max_ts,
    )


def _text(sql: str):
    """Wrap raw SQL for both SQLAlchemy 1.x and 2.x."""
    try:
        from sqlalchemy import text

        return text(sql.strip())
    except ImportError:
        return sql.strip()


def _commit(conn):
    try:
        conn.commit()
    except Exception:
        pass
