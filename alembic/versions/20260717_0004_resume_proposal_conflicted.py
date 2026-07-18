"""Allow stale Resume AI proposals to become explicitly conflicted.

Revision ID: 20260717_0004
Revises: 20260715_0003
Create Date: 2026-07-17
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260717_0004"
down_revision = "20260715_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add the contract-defined terminal conflicted status."""
    op.drop_constraint(
        "resume_proposals_status", "proposals", schema="resume", type_="check"
    )
    op.create_check_constraint(
        "resume_proposals_status",
        "proposals",
        sa.text(
            "status IN ('pending', 'accepted', 'partially_accepted', "
            "'rejected', 'expired', 'conflicted')"
        ),
        schema="resume",
    )


def downgrade() -> None:
    """Restore the original status set after converting conflicts to expired."""
    op.execute(
        "UPDATE resume.proposals SET status = 'expired' WHERE status = 'conflicted'"
    )
    op.drop_constraint(
        "resume_proposals_status", "proposals", schema="resume", type_="check"
    )
    op.create_check_constraint(
        "resume_proposals_status",
        "proposals",
        sa.text(
            "status IN ('pending', 'accepted', 'partially_accepted', 'rejected', 'expired')"
        ),
        schema="resume",
    )
