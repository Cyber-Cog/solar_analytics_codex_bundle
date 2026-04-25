"""Add timestamp_ts + trigger on raw_data_generic for Timescale / faster time predicates

Revision ID: 20260429_0004
Revises: 20260426_0003
Create Date: 2026-04-29

Adds typed timestamptz column (nullable), trigger to populate on INSERT/UPDATE,
and supporting index. Backfill existing rows with scripts/backfill_timestamp_ts.py
before enabling Timescale hypertable (0005).
"""

from __future__ import annotations

from alembic import op

revision = "20260429_0004"
down_revision = "20260426_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE raw_data_generic
        ADD COLUMN IF NOT EXISTS timestamp_ts TIMESTAMPTZ NULL;
        """
    )
    op.execute(
        """
        CREATE OR REPLACE FUNCTION solar_raw_data_generic_set_timestamp_ts()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
          BEGIN
            NEW.timestamp_ts := trim(cast(NEW."timestamp" AS text))::timestamptz;
          EXCEPTION WHEN OTHERS THEN
            NEW.timestamp_ts := NULL;
          END;
          RETURN NEW;
        END;
        $$;
        """
    )
    op.execute(
        """
        DROP TRIGGER IF EXISTS trg_raw_data_generic_timestamp_ts ON raw_data_generic;
        CREATE TRIGGER trg_raw_data_generic_timestamp_ts
        BEFORE INSERT OR UPDATE ON raw_data_generic
        FOR EACH ROW
        EXECUTE PROCEDURE solar_raw_data_generic_set_timestamp_ts();
        """
    )
    # PG14+ uses EXECUTE FUNCTION; if cluster is older, replace with PROCEDURE in a manual hotfix.
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_rdg_plant_equip_signal_timestamp_ts
        ON raw_data_generic (plant_id, equipment_id, signal, timestamp_ts)
        WHERE timestamp_ts IS NOT NULL;
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_raw_data_generic_timestamp_ts ON raw_data_generic;")
    op.execute("DROP FUNCTION IF EXISTS solar_raw_data_generic_set_timestamp_ts();")
    op.execute("DROP INDEX IF EXISTS idx_rdg_plant_equip_signal_timestamp_ts;")
    op.execute("ALTER TABLE raw_data_generic DROP COLUMN IF EXISTS timestamp_ts;")
