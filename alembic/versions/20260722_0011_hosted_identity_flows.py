"""Create hosted identity browser bindings and finite-state flows.

Revision ID: 20260722_0011
Revises: 20260722_0010
Create Date: 2026-07-22
"""

from __future__ import annotations

import re
from typing import Literal

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260722_0011"
down_revision = "20260722_0010"
branch_labels = None
depends_on = None

RuntimeRoleOption = Literal["app_role", "dashboard_role", "migrator_role"]
_ROLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TABLES = ("identity.identity_browser_sessions", "identity.identity_flows")


def _configured_role(option: RuntimeRoleOption) -> str:
    value = op.get_context().config.get_main_option(f"aiws.{option}")
    if not value or _ROLE_IDENTIFIER_PATTERN.fullmatch(value) is None or len(value.encode()) > 63:
        raise RuntimeError(f"missing or invalid dbctl role option: {option}")
    return '"' + value.replace('"', '""') + '"'


def upgrade() -> None:
    """Create hashed browser-session state and secret-free identity flows."""

    op.create_table(
        "identity_browser_sessions",
        sa.Column("id", sa.String(128), primary_key=True),
        sa.Column(
            "authorization_request_id",
            sa.String(128),
            sa.ForeignKey("identity.oauth_authorization_requests.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("browser_secret_hash", sa.String(64), nullable=False),
        sa.Column("csrf_token_hash", sa.String(64), nullable=False),
        sa.Column(
            "user_id", sa.String(128), sa.ForeignKey("identity.users.id", ondelete="SET NULL")
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        schema="identity",
    )
    op.create_index(
        "ix_identity_browser_sessions_authorization_request_id",
        "identity_browser_sessions",
        ["authorization_request_id"],
        schema="identity",
    )
    op.create_index(
        "ix_identity_browser_sessions_expires",
        "identity_browser_sessions",
        ["expires_at"],
        schema="identity",
    )
    op.create_table(
        "identity_flows",
        sa.Column("id", sa.String(128), primary_key=True),
        sa.Column("purpose", sa.String(32), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("allowed_steps", postgresql.JSONB(), nullable=False),
        sa.Column(
            "authorization_request_id",
            sa.String(128),
            sa.ForeignKey("identity.oauth_authorization_requests.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "browser_session_id",
            sa.String(128),
            sa.ForeignKey("identity.identity_browser_sessions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("client_id", sa.String(128), nullable=False),
        sa.Column("redirect_uri", sa.Text(), nullable=False),
        sa.Column("code_challenge", sa.String(128), nullable=False),
        sa.Column("authorization_resume_uri", sa.Text()),
        sa.Column("webauthn_options", postgresql.JSONB()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "purpose IN ('register', 'login', 'recover', 'reauthenticate')",
            name="identity_flows_purpose",
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'verified', 'completed', 'failed', 'expired')",
            name="identity_flows_status",
        ),
        schema="identity",
    )
    op.create_index(
        "ix_identity_flows_authorization_request_id",
        "identity_flows",
        ["authorization_request_id"],
        schema="identity",
    )
    op.create_index(
        "ix_identity_flows_browser_session_id",
        "identity_flows",
        ["browser_session_id"],
        schema="identity",
    )
    op.create_index(
        "ix_identity_flows_expires", "identity_flows", ["expires_at"], schema="identity"
    )
    app_role = _configured_role("app_role")
    dashboard_role = _configured_role("dashboard_role")
    migrator_role = _configured_role("migrator_role")
    for table in _TABLES:
        op.execute(
            f"REVOKE ALL PRIVILEGES ON TABLE {table} FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}"
        )
        op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {table} TO {app_role}")


def downgrade() -> None:
    """Refuse to discard non-empty identity security state."""

    connection = op.get_bind()
    if any(
        connection.execute(sa.text(f"SELECT count(*) FROM {table}")).scalar_one()
        for table in _TABLES
    ):
        raise RuntimeError("cannot drop non-empty hosted identity tables")
    op.drop_index("ix_identity_flows_expires", table_name="identity_flows", schema="identity")
    op.drop_index(
        "ix_identity_flows_browser_session_id", table_name="identity_flows", schema="identity"
    )
    op.drop_index(
        "ix_identity_flows_authorization_request_id", table_name="identity_flows", schema="identity"
    )
    op.drop_table("identity_flows", schema="identity")
    op.drop_index(
        "ix_identity_browser_sessions_expires",
        table_name="identity_browser_sessions",
        schema="identity",
    )
    op.drop_index(
        "ix_identity_browser_sessions_authorization_request_id",
        table_name="identity_browser_sessions",
        schema="identity",
    )
    op.drop_table("identity_browser_sessions", schema="identity")
