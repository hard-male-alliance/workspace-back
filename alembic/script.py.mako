"""@brief Alembic revision 模板 / Alembic revision template."""

"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}
branch_labels = ${repr(branch_labels)}
depends_on = ${repr(depends_on)}


def upgrade() -> None:
    """@brief 升级此 revision / Upgrade this revision.

    @return 无返回值。
    """
    ${upgrades if upgrades else "pass"}


def downgrade() -> None:
    """@brief 降级此 revision / Downgrade this revision.

    @return 无返回值。
    """
    ${downgrades if downgrades else "pass"}
