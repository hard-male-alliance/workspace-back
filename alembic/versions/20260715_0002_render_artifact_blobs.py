"""@brief PostgreSQL 渲染产物二进制负载 / PostgreSQL render-artifact binary payload.

Revision ID: 20260715_0002
Revises: 20260715_0001
Create Date: 2026-07-15

@note v0.1 尚未引入对象存储（object storage）适配器。此表使 PDF 和 source map
在后端进程重启后仍可读取，同时保留 ``render_artifacts.storage_key`` 作为未来迁移
到外部对象存储时的稳定抽象边界。
"""

from __future__ import annotations

import re

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "20260715_0002"
down_revision = "20260715_0001"
branch_labels = None
depends_on = None

_ROLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
"""@brief dbctl 传入的 PostgreSQL role 标识符白名单 / Allowed dbctl PostgreSQL role identifiers."""


def _app_role() -> str:
    """@brief 读取并安全引用 app role / Read and quote the dbctl-provided app role.

    @return 可嵌入固定 DDL 的双引号 identifier。
    @raise RuntimeError dbctl 未提供有效 role 时抛出。
    """
    migration_config = op.get_context().config
    if migration_config is None:
        raise RuntimeError("Alembic migration context has no configuration")
    value = migration_config.get_main_option("aiws.app_role")
    if not value or not _ROLE_IDENTIFIER_PATTERN.fullmatch(value):
        raise RuntimeError("missing or invalid dbctl role option: app_role")
    return '"' + value.replace('"', '""') + '"'


def upgrade() -> None:
    """@brief 创建带 RLS 的二进制产物表 / Create the RLS-protected artifact blob table.

    @return 无返回值。
    """
    app_role = _app_role()
    op.create_table(
        "artifact_blobs",
        sa.Column("id", sa.String(length=128), primary_key=True),
        sa.Column(
            "workspace_id",
            sa.String(length=128),
            sa.ForeignKey("identity.workspaces.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "resource_owner_id",
            sa.String(length=128),
            sa.ForeignKey("identity.users.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "artifact_id",
            sa.String(length=128),
            sa.ForeignKey("resume.render_artifacts.id", ondelete="CASCADE"),
            nullable=False,
            unique=True,
        ),
        sa.Column("content", sa.LargeBinary(), nullable=False),
        sa.Column("source_map", postgresql.JSONB(astext_type=sa.Text())),
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
        sa.Column("revision", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column(
            "extensions",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        schema="resume",
    )
    op.create_index(
        "ix_artifact_blobs_workspace_id",
        "artifact_blobs",
        ["workspace_id"],
        schema="resume",
    )
    op.create_index(
        "ix_artifact_blobs_resource_owner_id",
        "artifact_blobs",
        ["resource_owner_id"],
        schema="resume",
    )
    op.execute("ALTER TABLE resume.artifact_blobs ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE resume.artifact_blobs FORCE ROW LEVEL SECURITY")
    op.execute(
        f"""
        CREATE POLICY workspace_app_tenant_scope
        ON resume.artifact_blobs
        AS PERMISSIVE
        FOR ALL
        TO {app_role}
        USING (
            workspace_id = current_setting('app.workspace_id', true)
            AND resource_owner_id = current_setting('app.resource_owner_id', true)
        )
        WITH CHECK (
            workspace_id = current_setting('app.workspace_id', true)
            AND resource_owner_id = current_setting('app.resource_owner_id', true)
        )
        """
    )
    op.execute(
        f"GRANT SELECT, INSERT, UPDATE, DELETE ON resume.artifact_blobs TO {app_role}"
    )


def downgrade() -> None:
    """@brief 删除二进制产物表 / Drop the artifact blob table.

    @return 无返回值。
    """
    op.drop_table("artifact_blobs", schema="resume")
