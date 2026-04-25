"""Optional TimescaleDB: hypertable on raw_data_generic + 1-minute continuous aggregate

Revision ID: 20260429_0005
Revises: 20260429_0004
Create Date: 2026-04-29

No-ops unless:
  - PostgreSQL
  - Extension `timescaledb` is installed
  - `raw_data_generic.timestamp_ts` exists and has zero NULLs (run backfill first)

If `create_hypertable` fails (e.g. primary key must include partition column), see
backend/docs/PERF_AND_TIMESCALE.md for manual DBA steps. Downgrade drops the CAGG only.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260429_0005"
down_revision = "20260429_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        print("[20260429_0005] skip: not postgresql")
        return

    has_ts = bind.execute(
        sa.text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name='raw_data_generic' "
            "AND column_name='timestamp_ts'"
        )
    ).scalar()
    if not has_ts:
        print("[20260429_0005] skip: timestamp_ts column missing")
        return

    ext = bind.execute(
        sa.text("SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'")
    ).scalar()
    if not ext:
        print("[20260429_0005] skip: timescaledb extension not installed")
        return

    nulls = bind.execute(
        sa.text("SELECT count(*) FROM raw_data_generic WHERE timestamp_ts IS NULL")
    ).scalar()
    if nulls is not None and int(nulls) > 0:
        print(f"[20260429_0005] skip: {nulls} rows still have NULL timestamp_ts (run backfill_timestamp_ts)")
        return

    # Hypertable + CAGG DDL often requires autocommit (Timescale / Postgres).
    try:
        with op.get_context().autocommit_block():
            op.execute(
                "SELECT create_hypertable("
                "'raw_data_generic', 'timestamp_ts', "
                "chunk_time_interval => INTERVAL '7 days', migrate_data => true, if_not_exists => true)"
            )
            op.execute("DROP MATERIALIZED VIEW IF EXISTS solar_raw_data_1m_cagg CASCADE")
            op.execute(
                """
                CREATE MATERIALIZED VIEW solar_raw_data_1m_cagg
                WITH (timescaledb.continuous) AS
                SELECT
                  time_bucket(INTERVAL '1 minute', timestamp_ts) AS bucket,
                  plant_id,
                  equipment_level,
                  equipment_id,
                  signal,
                  avg(value) AS value_avg,
                  count(*)::bigint AS sample_n
                FROM raw_data_generic
                WHERE timestamp_ts IS NOT NULL
                GROUP BY 1, 2, 3, 4, 5, 6
                """
            )
    except Exception as exc:
        print(f"[20260429_0005] hypertable/CAGG DDL failed (see PERF_AND_TIMESCALE.md): {exc}")
        return

    try:
        with op.get_context().autocommit_block():
            op.execute(
                """
                SELECT add_continuous_aggregate_policy(
                  'solar_raw_data_1m_cagg',
                  start_offset => INTERVAL '3 days',
                  end_offset => INTERVAL '1 hour',
                  schedule_interval => INTERVAL '1 hour'
                )
                """
            )
    except Exception as exc:
        print(f"[20260429_0005] add_continuous_aggregate_policy warning: {exc}")


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    try:
        bind.execute(sa.text("DROP MATERIALIZED VIEW IF EXISTS solar_raw_data_1m_cagg CASCADE"))
    except Exception:
        pass
