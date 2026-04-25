"""ds_status_snapshot for fast Fault Diagnostics first paint

Revision ID: 20260426_0003
Revises: 20260423_0002
Create Date: 2026-04-26
"""

from __future__ import annotations

from alembic import op

revision = "20260426_0003"
down_revision = "20260423_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS ds_status_snapshot (
            id SERIAL PRIMARY KEY,
            plant_id VARCHAR NOT NULL,
            date_from VARCHAR(32) NOT NULL DEFAULT '',
            date_to VARCHAR(32) NOT NULL DEFAULT '',
            payload_json TEXT NOT NULL,
            computed_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
            CONSTRAINT uq_ds_status_snapshot UNIQUE (plant_id, date_from, date_to)
        );
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ds_status_snapshot_plant_computed
        ON ds_status_snapshot (plant_id, computed_at);
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS ds_status_snapshot CASCADE;")
