"""Allow the Agent worker to persist SDK-native pause/resume checkpoints.

Revision ID: 20260727_0030
Revises: 20260727_0029
Create Date: 2026-07-27
"""

from __future__ import annotations

import re

from alembic import op

revision = "20260727_0030"
down_revision = "20260727_0029"
branch_labels = None
depends_on = None

_ROLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _app_role() -> str:
    configuration = op.get_context().config
    if configuration is None:
        raise RuntimeError("Alembic migration context has no configuration")
    value = configuration.get_main_option("aiws.app_role")
    if (
        not value
        or _ROLE_IDENTIFIER_PATTERN.fullmatch(value) is None
        or len(value.encode("utf-8")) > 63
    ):
        raise RuntimeError("missing or invalid dbctl role option: app_role")
    return '"' + value.replace('"', '""') + '"'


def upgrade() -> None:
    """Grant only the existing private extension column needed by Agent Run CAS updates."""

    op.execute(
        f"GRANT UPDATE (extensions) ON TABLE agent.runs TO {_app_role()}"
    )


def downgrade() -> None:
    """Remove the checkpoint-column update privilege."""

    op.execute(
        f"REVOKE UPDATE (extensions) ON TABLE agent.runs FROM {_app_role()}"
    )
