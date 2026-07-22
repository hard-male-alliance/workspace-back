"""Create hosted identity credential and login-session state.

Revision ID: 20260722_0012
Revises: 20260722_0011
Create Date: 2026-07-22
"""

from __future__ import annotations

import re
from typing import Literal

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260722_0012"
down_revision = "20260722_0011"
branch_labels = None
depends_on = None

RuntimeRoleOption = Literal["app_role", "dashboard_role", "migrator_role"]
_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TABLES = (
    "identity.identity_flow_steps",
    "identity.identity_authenticators",
    "identity.identity_login_sessions",
)


def _role(option: RuntimeRoleOption) -> str:
    value = op.get_context().config.get_main_option(f"aiws.{option}")
    if not value or _PATTERN.fullmatch(value) is None or len(value.encode()) > 63:
        raise RuntimeError(f"missing or invalid dbctl role option: {option}")
    return '"' + value.replace('"', '""') + '"'


def upgrade() -> None:
    """Add verifier-only authenticators and bounded login sessions."""

    op.add_column(
        "users",
        sa.Column("email_verified", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        schema="identity",
    )
    op.create_unique_constraint("users_email_unique", "users", ["email"], schema="identity")
    op.add_column(
        "identity_flows",
        sa.Column(
            "user_id", sa.String(128), sa.ForeignKey("identity.users.id", ondelete="SET NULL")
        ),
        schema="identity",
    )
    op.add_column(
        "identity_flows",
        sa.Column(
            "internal_state",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        schema="identity",
    )
    op.create_table(
        "identity_flow_steps",
        sa.Column("id", sa.String(128), primary_key=True),
        sa.Column(
            "flow_id",
            sa.String(128),
            sa.ForeignKey("identity.identity_flows.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("step_id", sa.String(160), nullable=False),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("flow_id", "step_id", name="identity_flow_steps_flow_step"),
        schema="identity",
    )
    op.create_table(
        "identity_authenticators",
        sa.Column("id", sa.String(128), primary_key=True),
        sa.Column(
            "user_id",
            sa.String(128),
            sa.ForeignKey("identity.users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("display_name", sa.String(200), nullable=False),
        sa.Column("verifier", sa.Text(), nullable=False),
        sa.Column("credential_id", sa.String(1024), unique=True),
        sa.Column(
            "credential_metadata",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("last_used_at", sa.DateTime(timezone=True)),
        sa.Column("revoked_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint(
            "kind IN ('passkey', 'password', 'recovery_code')", name="identity_authenticators_kind"
        ),
        schema="identity",
    )
    op.create_index(
        "ix_identity_authenticators_user_id",
        "identity_authenticators",
        ["user_id"],
        schema="identity",
    )
    op.create_table(
        "identity_login_sessions",
        sa.Column("id", sa.String(128), primary_key=True),
        sa.Column(
            "user_id",
            sa.String(128),
            sa.ForeignKey("identity.users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("client_id", sa.String(128), nullable=False),
        sa.Column("client_name", sa.String(120), nullable=False),
        sa.Column("device_name", sa.String(200)),
        sa.Column("session_secret_hash", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("idle_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("absolute_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True)),
        schema="identity",
    )
    op.create_index(
        "ix_identity_login_sessions_user_id",
        "identity_login_sessions",
        ["user_id"],
        schema="identity",
    )
    op.create_index(
        "ix_identity_login_sessions_expires",
        "identity_login_sessions",
        ["absolute_expires_at"],
        schema="identity",
    )
    app_role, dashboard_role, migrator_role = (
        _role("app_role"),
        _role("dashboard_role"),
        _role("migrator_role"),
    )
    for table in _TABLES:
        op.execute(
            f"REVOKE ALL PRIVILEGES ON TABLE {table} FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}"
        )
        op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {table} TO {app_role}")


def downgrade() -> None:
    """Refuse to discard live authenticators or sessions."""

    connection = op.get_bind()
    if any(
        connection.execute(sa.text(f"SELECT count(*) FROM {table}")).scalar_one()
        for table in _TABLES
    ):
        raise RuntimeError("cannot drop non-empty identity credential tables")
    op.drop_index(
        "ix_identity_login_sessions_expires",
        table_name="identity_login_sessions",
        schema="identity",
    )
    op.drop_index(
        "ix_identity_login_sessions_user_id",
        table_name="identity_login_sessions",
        schema="identity",
    )
    op.drop_table("identity_login_sessions", schema="identity")
    op.drop_index(
        "ix_identity_authenticators_user_id",
        table_name="identity_authenticators",
        schema="identity",
    )
    op.drop_table("identity_authenticators", schema="identity")
    op.drop_table("identity_flow_steps", schema="identity")
    op.drop_column("identity_flows", "internal_state", schema="identity")
    op.drop_column("identity_flows", "user_id", schema="identity")
    op.drop_constraint("users_email_unique", "users", schema="identity", type_="unique")
    op.drop_column("users", "email_verified", schema="identity")
