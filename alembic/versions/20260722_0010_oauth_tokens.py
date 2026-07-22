"""Create one-time authorization code, refresh rotation, and access revocation tables.

Revision ID: 20260722_0010
Revises: 20260722_0009
Create Date: 2026-07-22
"""

from __future__ import annotations

import re
from typing import Literal

import sqlalchemy as sa
from alembic import op

revision = "20260722_0010"
down_revision = "20260722_0009"
branch_labels = None
depends_on = None

RuntimeRoleOption = Literal["app_role", "dashboard_role", "migrator_role"]
_ROLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_POSTGRES_IDENTIFIER_MAX_BYTES = 63
_TABLES = (
    "identity.oauth_authorization_codes",
    "identity.oauth_refresh_token_families",
    "identity.oauth_refresh_tokens",
    "identity.oauth_revoked_access_tokens",
)


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
    """Create hashed token state and the refresh-token reuse revocation boundary."""

    op.create_table(
        "oauth_authorization_codes",
        sa.Column("id", sa.String(length=128), primary_key=True),
        sa.Column("code_hash", sa.String(length=64), nullable=False, unique=True),
        sa.Column(
            "authorization_request_id",
            sa.String(length=128),
            sa.ForeignKey("identity.oauth_authorization_requests.id", ondelete="RESTRICT"),
            nullable=False,
            unique=True,
        ),
        sa.Column("subject", sa.String(length=320), nullable=False),
        sa.Column(
            "user_id",
            sa.String(length=128),
            sa.ForeignKey("identity.users.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("client_id", sa.String(length=128), nullable=False),
        sa.Column("redirect_uri", sa.Text(), nullable=False),
        sa.Column("scope", sa.Text(), nullable=False),
        sa.Column("nonce", sa.String(length=512), nullable=False),
        sa.Column("code_challenge", sa.String(length=128), nullable=False),
        sa.Column("auth_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("consumed_at", sa.DateTime(timezone=True)),
        schema="identity",
    )
    op.create_index(
        "ix_oauth_authorization_codes_expires",
        "oauth_authorization_codes",
        ["expires_at"],
        schema="identity",
    )
    op.create_table(
        "oauth_refresh_token_families",
        sa.Column("id", sa.String(length=128), primary_key=True),
        sa.Column("subject", sa.String(length=320), nullable=False),
        sa.Column(
            "user_id",
            sa.String(length=128),
            sa.ForeignKey("identity.users.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("client_id", sa.String(length=128), nullable=False),
        sa.Column("scope", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("revoked_at", sa.DateTime(timezone=True)),
        sa.Column("reuse_detected_at", sa.DateTime(timezone=True)),
        schema="identity",
    )
    op.create_index(
        "ix_oauth_refresh_token_families_client_id",
        "oauth_refresh_token_families",
        ["client_id"],
        schema="identity",
    )
    op.create_table(
        "oauth_refresh_tokens",
        sa.Column("id", sa.String(length=128), primary_key=True),
        sa.Column(
            "family_id",
            sa.String(length=128),
            sa.ForeignKey("identity.oauth_refresh_token_families.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("token_hash", sa.String(length=64), nullable=False, unique=True),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("consumed_at", sa.DateTime(timezone=True)),
        sa.Column("replaced_by_token_id", sa.String(length=128)),
        sa.UniqueConstraint("family_id", "sequence", name="oauth_refresh_tokens_family_sequence"),
        schema="identity",
    )
    op.create_index(
        "ix_oauth_refresh_tokens_family_id",
        "oauth_refresh_tokens",
        ["family_id"],
        schema="identity",
    )
    op.create_index(
        "ix_oauth_refresh_tokens_expires",
        "oauth_refresh_tokens",
        ["expires_at"],
        schema="identity",
    )
    op.create_table(
        "oauth_revoked_access_tokens",
        sa.Column("jti_hash", sa.String(length=64), primary_key=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        schema="identity",
    )
    op.create_index(
        "ix_oauth_revoked_access_tokens_expires",
        "oauth_revoked_access_tokens",
        ["expires_at"],
        schema="identity",
    )

    app_role = _configured_role("app_role")
    dashboard_role = _configured_role("dashboard_role")
    migrator_role = _configured_role("migrator_role")
    for table in _TABLES:
        op.execute(
            f"REVOKE ALL PRIVILEGES ON TABLE {table} "
            f"FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}"
        )
        op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {table} TO {app_role}")


def downgrade() -> None:
    """Never discard issued or revoked token evidence during downgrade."""

    connection = op.get_bind()
    if any(connection.execute(sa.text(f"SELECT count(*) FROM {table}")).scalar_one() for table in _TABLES):
        raise RuntimeError("cannot drop non-empty OAuth token tables")
    op.drop_index("ix_oauth_revoked_access_tokens_expires", table_name="oauth_revoked_access_tokens", schema="identity")
    op.drop_table("oauth_revoked_access_tokens", schema="identity")
    op.drop_index("ix_oauth_refresh_tokens_expires", table_name="oauth_refresh_tokens", schema="identity")
    op.drop_index("ix_oauth_refresh_tokens_family_id", table_name="oauth_refresh_tokens", schema="identity")
    op.drop_table("oauth_refresh_tokens", schema="identity")
    op.drop_index("ix_oauth_refresh_token_families_client_id", table_name="oauth_refresh_token_families", schema="identity")
    op.drop_table("oauth_refresh_token_families", schema="identity")
    op.drop_index("ix_oauth_authorization_codes_expires", table_name="oauth_authorization_codes", schema="identity")
    op.drop_table("oauth_authorization_codes", schema="identity")
