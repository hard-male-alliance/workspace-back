"""Create durable OAuth public-client authorization transactions.

Revision ID: 20260722_0009
Revises: 20260722_0008
Create Date: 2026-07-22
"""

from __future__ import annotations

import re
from typing import Literal

import sqlalchemy as sa
from alembic import op

revision = "20260722_0009"
down_revision = "20260722_0008"
branch_labels = None
depends_on = None

RuntimeRoleOption = Literal["app_role", "dashboard_role", "migrator_role"]
_ROLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_POSTGRES_IDENTIFIER_MAX_BYTES = 63
_TABLE = "identity.oauth_authorization_requests"


def _configured_role(option: RuntimeRoleOption) -> str:
    configuration = op.get_context().config
    if configuration is None:
        raise RuntimeError("Alembic migration context has no configuration")
    value = configuration.get_main_option(f"aiws.{option}")
    if (
        not value
        or _ROLE_IDENTIFIER_PATTERN.fullmatch(value) is None
        or len(value.encode("utf-8")) > _POSTGRES_IDENTIFIER_MAX_BYTES
    ):
        raise RuntimeError(f"missing or invalid dbctl role option: {option}")
    return '"' + value.replace('"', '""') + '"'


def upgrade() -> None:
    """Create short-lived, PKCE-S256-only authorization request persistence."""

    op.create_table(
        "oauth_authorization_requests",
        sa.Column("id", sa.String(length=128), primary_key=True),
        sa.Column("client_id", sa.String(length=128), nullable=False),
        sa.Column("redirect_uri", sa.Text(), nullable=False),
        sa.Column("scope", sa.Text(), nullable=False),
        sa.Column("state", sa.String(length=512), nullable=False),
        sa.Column("nonce", sa.String(length=512), nullable=False),
        sa.Column("code_challenge", sa.String(length=128), nullable=False),
        sa.Column("code_challenge_method", sa.String(length=8), nullable=False),
        sa.Column("prompt", sa.String(length=128), nullable=False, server_default=""),
        sa.Column("screen_hint", sa.String(length=32)),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="pending"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('pending', 'authenticated', 'consented', 'code_issued', 'expired', 'cancelled')",
            name="oauth_authorization_requests_status",
        ),
        sa.CheckConstraint(
            "code_challenge_method = 'S256'",
            name="oauth_authorization_requests_pkce_s256",
        ),
        schema="identity",
    )
    op.create_index(
        "ix_oauth_authorization_requests_client_id",
        "oauth_authorization_requests",
        ["client_id"],
        schema="identity",
    )
    op.create_index(
        "ix_oauth_authorization_requests_expires",
        "oauth_authorization_requests",
        ["expires_at"],
        schema="identity",
    )

    app_role = _configured_role("app_role")
    dashboard_role = _configured_role("dashboard_role")
    migrator_role = _configured_role("migrator_role")
    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE {_TABLE} "
        f"FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}"
    )
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {_TABLE} TO {app_role}")


def downgrade() -> None:
    """Refuse to discard live authorization state during an unsafe downgrade."""

    connection = op.get_bind()
    count = connection.execute(sa.text(f"SELECT count(*) FROM {_TABLE}")).scalar_one()
    if count:
        raise RuntimeError("cannot drop non-empty OAuth authorization request table")
    op.drop_index(
        "ix_oauth_authorization_requests_expires",
        table_name="oauth_authorization_requests",
        schema="identity",
    )
    op.drop_index(
        "ix_oauth_authorization_requests_client_id",
        table_name="oauth_authorization_requests",
        schema="identity",
    )
    op.drop_table("oauth_authorization_requests", schema="identity")
