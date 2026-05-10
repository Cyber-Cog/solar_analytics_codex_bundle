"""precompute_jobs.job_spec_json for selective module snapshots

Revision ID: 20260510_0007
Revises: 20260429_0006
Create Date: 2026-05-10
"""

from __future__ import annotations

from alembic import op

revision = "20260510_0007"
down_revision = "20260429_0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE precompute_jobs
        ADD COLUMN IF NOT EXISTS job_spec_json TEXT;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE precompute_jobs
        DROP COLUMN IF EXISTS job_spec_json;
        """
    )
