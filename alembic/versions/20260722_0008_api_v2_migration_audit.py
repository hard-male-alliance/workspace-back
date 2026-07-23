"""Create the append-only v1-to-v2 migration audit ledger.

Revision ID: 20260722_0008
Revises: 20260721_0007
Create Date: 2026-07-22
"""

from __future__ import annotations

import re
from typing import Literal

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260722_0008"
down_revision = "20260721_0007"
branch_labels = None
depends_on = None

RuntimeRoleOption = Literal["app_role", "dashboard_role", "migrator_role"]
_ROLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_POSTGRES_IDENTIFIER_MAX_BYTES = 63
_TABLE = "identity.api_migration_audits"


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
    """Create an immutable event ledger for backup, backfill, and shadow-read evidence."""

    op.create_table(
        "api_migration_audits",
        sa.Column("id", sa.String(length=128), primary_key=True),
        sa.Column("migration_id", sa.String(length=128), nullable=False),
        sa.Column("phase", sa.SmallInteger(), nullable=False),
        sa.Column("event_type", sa.String(length=32), nullable=False),
        sa.Column("source_api_version", sa.String(length=16), nullable=False),
        sa.Column("target_api_version", sa.String(length=16), nullable=False),
        sa.Column("source_snapshot_sha256", sa.String(length=64)),
        sa.Column("request_id", sa.String(length=128)),
        sa.Column("actor_id", sa.String(length=128)),
        sa.Column(
            "details",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "occurred_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("phase BETWEEN 0 AND 5", name="api_migration_audits_phase"),
        sa.CheckConstraint(
            "event_type IN ('backup_created', 'started', 'verified', 'completed', 'failed')",
            name="api_migration_audits_event_type",
        ),
        sa.CheckConstraint(
            "source_api_version = 'v1' AND target_api_version = 'v2'",
            name="api_migration_audits_version_pair",
        ),
        sa.CheckConstraint(
            "source_snapshot_sha256 IS NULL "
            "OR source_snapshot_sha256 ~ '^[0-9a-f]{64}$'",
            name="api_migration_audits_snapshot_sha256",
        ),
        schema="identity",
    )
    op.create_index(
        "ix_api_migration_audits_migration_occurred",
        "api_migration_audits",
        ["migration_id", "occurred_at", "id"],
        schema="identity",
    )
    op.execute(
        """
        CREATE FUNCTION identity.reject_api_migration_audit_mutation()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        BEGIN
            RAISE EXCEPTION 'identity.api_migration_audits is append-only'
                USING ERRCODE = '55000';
        END;
        $function$
        """
    )
    op.execute(
        "CREATE TRIGGER api_migration_audits_append_only "
        f"BEFORE UPDATE OR DELETE ON {_TABLE} FOR EACH ROW "
        "EXECUTE FUNCTION identity.reject_api_migration_audit_mutation()"
    )

    app_role = _configured_role("app_role")
    dashboard_role = _configured_role("dashboard_role")
    migrator_role = _configured_role("migrator_role")
    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE {_TABLE} "
        f"FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}"
    )
    op.execute(f"GRANT SELECT, INSERT ON TABLE {_TABLE} TO {app_role}")
    op.execute(f"GRANT SELECT ON TABLE {_TABLE} TO {dashboard_role}")


def downgrade() -> None:
    """Remove only the empty migration ledger; retained evidence blocks destructive downgrade."""

    connection = op.get_bind()
    count = connection.execute(sa.text(f"SELECT count(*) FROM {_TABLE}")).scalar_one()
    if count:
        raise RuntimeError("cannot drop non-empty API migration audit ledger")
    op.execute(f"DROP TRIGGER api_migration_audits_append_only ON {_TABLE}")
    op.execute("DROP FUNCTION identity.reject_api_migration_audit_mutation()")
    op.drop_index(
        "ix_api_migration_audits_migration_occurred",
        table_name="api_migration_audits",
        schema="identity",
    )
    op.drop_table("api_migration_audits", schema="identity")
