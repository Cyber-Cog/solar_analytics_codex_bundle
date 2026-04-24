"""precompute_jobs table + snapshot read indexes

Revision ID: 20260224_0001
Revises:
Create Date: 2026-02-24

Creates `precompute_jobs` (durable queue for module precompute worker).
Adds supporting indexes on snapshot tables if missing (idempotent IF NOT EXISTS).

Downgrade drops the job table only (indexes dropped with table).
"""

from __future__ import annotations

from alembic import op

revision = "20260224_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS precompute_jobs (
            id BIGSERIAL PRIMARY KEY,
            plant_id VARCHAR NOT NULL,
            date_from VARCHAR(32) NOT NULL,
            date_to VARCHAR(32) NOT NULL,
            status VARCHAR(16) NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 5,
            worker_id VARCHAR(64),
            locked_at TIMESTAMP,
            error_message TEXT,
            created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
            updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
        );
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_precompute_jobs_status_created "
        "ON precompute_jobs (status, created_at);"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_precompute_jobs_plant_status "
        "ON precompute_jobs (plant_id, status);"
    )
    # Snapshot indexes only if tables exist (Alembic-only deploy may create jobs first).
    op.execute(
        r"""
        DO $body$
        BEGIN
          IF to_regclass('public.ds_summary_snapshot') IS NOT NULL THEN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_ds_summary_snapshot_plant_dates '
              'ON ds_summary_snapshot (plant_id, date_from, date_to)';
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_ds_summary_snapshot_plant_computed '
              'ON ds_summary_snapshot (plant_id, computed_at)';
          END IF;
          IF to_regclass('public.unified_fault_snapshot') IS NOT NULL THEN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_unified_fault_snapshot_plant_dates '
              'ON unified_fault_snapshot (plant_id, date_from, date_to)';
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_unified_fault_snapshot_plant_computed '
              'ON unified_fault_snapshot (plant_id, computed_at)';
          END IF;
          IF to_regclass('public.loss_analysis_snapshot') IS NOT NULL THEN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_loss_analysis_snapshot_lookup '
              'ON loss_analysis_snapshot (plant_id, date_from, date_to, scope, equipment_id)';
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_loss_analysis_snapshot_plant_computed '
              'ON loss_analysis_snapshot (plant_id, computed_at)';
          END IF;
        END
        $body$;
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS precompute_jobs CASCADE;")
    for idx in (
        "idx_ds_summary_snapshot_plant_dates",
        "idx_ds_summary_snapshot_plant_computed",
        "idx_unified_fault_snapshot_plant_dates",
        "idx_unified_fault_snapshot_plant_computed",
        "idx_loss_analysis_snapshot_lookup",
        "idx_loss_analysis_snapshot_plant_computed",
    ):
        op.execute(f"DROP INDEX IF EXISTS {idx};")
