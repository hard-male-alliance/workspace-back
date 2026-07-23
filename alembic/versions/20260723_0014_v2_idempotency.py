"""@brief 创建 API V2 幂等 receipt store / Create the API V2 idempotency receipt store.

Revision ID: 20260723_0014
Revises: 20260723_0013
Create Date: 2026-07-23
"""

from __future__ import annotations

import re
from typing import Literal

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260723_0014"
"""@brief 当前 Alembic revision / Current Alembic revision."""

down_revision = "20260723_0013"
"""@brief API V2 identity/workspace revision / API V2 identity/workspace predecessor."""

branch_labels = None
"""@brief 此迁移不创建分支 / This migration creates no branch."""

depends_on = None
"""@brief 此迁移没有额外依赖 / This migration has no extra dependency."""

RuntimeRoleOption = Literal["owner_role", "app_role", "dashboard_role", "migrator_role"]
"""@brief 本 revision 使用的数据库角色配置 / Database-role options used by this revision."""

_ROLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
"""@brief PostgreSQL 角色标识符白名单 / PostgreSQL role-identifier allowlist."""

_POSTGRES_IDENTIFIER_MAX_BYTES = 63
"""@brief PostgreSQL 标识符最大字节数 / PostgreSQL identifier byte limit."""

_TABLE = "identity.api_v2_idempotency_records"
"""@brief 新 V2 receipt 表的限定名称 / Qualified name of the new V2 receipt table."""


def _configured_role(option: RuntimeRoleOption) -> str:
    """@brief 返回安全引用的运行时角色 / Return a safely quoted runtime role.

    @param option Alembic ``aiws.*`` 角色选项 / Alembic ``aiws.*`` role option.
    @return 双引号引用的 PostgreSQL 标识符 / Double-quoted PostgreSQL identifier.
    @raise RuntimeError 配置缺失或不是合法标识符时抛出 / Raised for missing or unsafe input.
    """
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
    """@brief 创建最小权限、强 RLS 的 V2 幂等表 / Create the least-privilege, forced-RLS V2 table.

    @return 无返回值 / No return value.

    @note nullable Workspace 通过 ``UNIQUE NULLS NOT DISTINCT`` 参与唯一 scope；因此用户级
        命令不会像普通 SQL NULL unique 语义那样重复插入。
    """
    app_role = _configured_role("app_role")
    dashboard_role = _configured_role("dashboard_role")
    migrator_role = _configured_role("migrator_role")

    op.create_table(
        "api_v2_idempotency_records",
        sa.Column("id", sa.String(128), primary_key=True),
        sa.Column(
            "user_id",
            sa.String(128),
            sa.ForeignKey("identity.users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "workspace_id",
            sa.String(128),
            sa.ForeignKey("identity.workspaces.id", ondelete="CASCADE"),
        ),
        sa.Column("method", sa.String(16), nullable=False),
        sa.Column("canonical_path", sa.String(512), nullable=False),
        sa.Column("idempotency_key", sa.String(128), nullable=False),
        sa.Column("request_fingerprint", sa.String(64), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("claim_token_hash", sa.String(64)),
        sa.Column("response_status", sa.SmallInteger()),
        sa.Column("response_headers", postgresql.JSONB(astext_type=sa.Text())),
        sa.Column("response_body", sa.LargeBinary()),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint(
            "user_id",
            "workspace_id",
            "method",
            "canonical_path",
            "idempotency_key",
            name="uq_api_v2_idempotency_scope",
            postgresql_nulls_not_distinct=True,
        ),
        sa.CheckConstraint("method ~ '^[A-Z]+$'", name="ck_api_v2_idempotency_method"),
        sa.CheckConstraint(
            "left(canonical_path, 1) = '/' "
            "AND position('?' in canonical_path) = 0 "
            "AND position('#' in canonical_path) = 0",
            name="ck_api_v2_idempotency_canonical_path",
        ),
        sa.CheckConstraint(
            "idempotency_key ~ '^[A-Za-z0-9._~-]{16,128}$'",
            name="ck_api_v2_idempotency_key",
        ),
        sa.CheckConstraint(
            "request_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_api_v2_idempotency_fingerprint",
        ),
        sa.CheckConstraint(
            "expires_at >= created_at + interval '24 hours'",
            name="ck_api_v2_idempotency_retention",
        ),
        sa.CheckConstraint(
            "(status = 'pending' "
            "AND claim_token_hash IS NOT NULL "
            "AND claim_token_hash ~ '^[0-9a-f]{64}$' "
            "AND response_status IS NULL "
            "AND response_headers IS NULL "
            "AND response_body IS NULL) "
            "OR (status = 'completed' "
            "AND claim_token_hash IS NULL "
            "AND response_status IS NOT NULL "
            "AND response_status BETWEEN 100 AND 599 "
            "AND response_headers IS NOT NULL "
            "AND jsonb_typeof(response_headers) = 'array' "
            "AND response_body IS NOT NULL)",
            name="ck_api_v2_idempotency_state",
        ),
        schema="identity",
    )
    op.create_index(
        "ix_api_v2_idempotency_completed_expiry",
        "api_v2_idempotency_records",
        ["expires_at"],
        unique=False,
        schema="identity",
        postgresql_where=sa.text("status = 'completed'"),
    )

    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE {_TABLE} "
        f"FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}"
    )
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {_TABLE} TO {app_role}")
    op.execute(f"ALTER TABLE {_TABLE} ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {_TABLE} FORCE ROW LEVEL SECURITY")

    scope_predicate = (
        "user_id = current_setting('app.actor_id', true) "
        "AND workspace_id IS NOT DISTINCT FROM "
        "NULLIF(current_setting('app.workspace_id', true), '')"
    )
    op.execute(
        f"CREATE POLICY api_v2_idempotency_actor_scope ON {_TABLE} "
        f"TO {app_role} USING ({scope_predicate}) WITH CHECK ({scope_predicate})"
    )


def downgrade() -> None:
    """@brief 仅允许空表 downgrade / Downgrade only when the receipt table is empty.

    @return 无返回值 / No return value.
    @raise RuntimeError 表含 pending 或 completed receipt 时拒绝破坏数据 / Raised rather than
        destroying pending or completed receipts.
    """
    owner_role = _configured_role("owner_role")
    op.execute(
        f"CREATE POLICY api_v2_idempotency_migration_owner ON {_TABLE} "
        f"AS PERMISSIVE FOR ALL TO {owner_role} USING (true) WITH CHECK (true)"
    )
    row_count = int(
        op.get_bind()
        .execute(sa.text("SELECT count(*) FROM identity.api_v2_idempotency_records"))
        .scalar_one()
    )
    if row_count:
        raise RuntimeError("cannot downgrade non-empty API V2 idempotency receipts")
    op.drop_index(
        "ix_api_v2_idempotency_completed_expiry",
        table_name="api_v2_idempotency_records",
        schema="identity",
    )
    op.drop_table("api_v2_idempotency_records", schema="identity")
