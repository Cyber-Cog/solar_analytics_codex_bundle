"""unified_feed_category_totals (narrow precomputed category KPIs for SQL/BI)

Revision ID: 20260423_0002
Revises: 20260224_0001
Create Date: 2026-04-23
"""

from __future__ import annotations

from alembic import op

revision = "20260423_0002"
down_revision = "20260224_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS unified_feed_category_totals (
            id SERIAL PRIMARY KEY,
            plant_id VARCHAR NOT NULL,
            date_from VARCHAR(32) NOT NULL,
            date_to VARCHAR(32) NOT NULL,
            category_id VARCHAR(32) NOT NULL,
            loss_mwh DOUBLE PRECISION NOT NULL DEFAULT 0,
            fault_count INTEGER NOT NULL DEFAULT 0,
            computed_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now()
        );
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_unified_feed_category_totals
        ON unified_feed_category_totals (plant_id, date_from, date_to, category_id);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ufct_plant_computed
        ON unified_feed_category_totals (plant_id, computed_at);
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS unified_feed_category_totals CASCADE;")
