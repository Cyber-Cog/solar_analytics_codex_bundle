"""Plant type config and interval fault events

Revision ID: 20260429_0006
Revises: 20260429_0005
Create Date: 2026-04-29
"""

from __future__ import annotations

from alembic import op

revision = "20260429_0006"
down_revision = "20260429_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE plants
        ADD COLUMN IF NOT EXISTS plant_type VARCHAR(16) NOT NULL DEFAULT 'SCB';
        """
    )
    op.execute(
        """
        UPDATE plants
           SET plant_type = 'MPPT'
         WHERE UPPER(COALESCE(plant_id, '') || ' ' || COALESCE(name, '')) LIKE '%TIGER%';
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'chk_plants_plant_type'
          ) THEN
            ALTER TABLE plants
            ADD CONSTRAINT chk_plants_plant_type
            CHECK (plant_type IN ('SCB', 'MPPT'));
          END IF;
        END $$;
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS fault_events (
            id BIGSERIAL PRIMARY KEY,
            plant_id VARCHAR NOT NULL,
            inverter_id VARCHAR,
            equipment_level VARCHAR NOT NULL,
            equipment_id VARCHAR NOT NULL,
            fault_type VARCHAR NOT NULL DEFAULT 'DS',
            start_time TIMESTAMPTZ NOT NULL,
            end_time TIMESTAMPTZ,
            duration_minutes DOUBLE PRECISION,
            status VARCHAR NOT NULL DEFAULT 'closed',
            severity VARCHAR,
            detection_confidence DOUBLE PRECISION,
            missing_strings INTEGER,
            start_reason TEXT,
            close_reason TEXT,
            created_at TIMESTAMPTZ DEFAULT now(),
            updated_at TIMESTAMPTZ DEFAULT now()
        );
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fault_events_lookup
        ON fault_events (plant_id, equipment_id, fault_type, start_time, end_time);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fault_events_inverter_range
        ON fault_events (plant_id, inverter_id, start_time, end_time);
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS fault_events;")
    op.execute("ALTER TABLE plants DROP CONSTRAINT IF EXISTS chk_plants_plant_type;")
    op.execute("ALTER TABLE plants DROP COLUMN IF EXISTS plant_type;")
