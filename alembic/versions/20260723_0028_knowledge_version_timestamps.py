"""@brief 修复并约束 Knowledge 版本时间顺序 / Repair and constrain Knowledge version timestamps.

Revision ID: 20260723_0028
Revises: 20260723_0027
Create Date: 2026-07-23

PostgreSQL ``now()`` is fixed at transaction start.  Historical Knowledge ingestion could use a
later application timestamp for ``created_at`` and then let SQLAlchemy's database-side on-update
fallback write the earlier transaction timestamp into ``updated_at``.  This revision repairs only
those inverted rows and prevents recurrence at the storage boundary.
"""

from __future__ import annotations

import re

import sqlalchemy as sa
from alembic import op

revision = "20260723_0028"
"""@brief 当前 Alembic revision / Current Alembic revision."""

down_revision = "20260723_0027"
"""@brief 前驱统一 outbox 生命周期 revision / Preceding unified-outbox lifecycle revision."""

branch_labels = None
"""@brief 本迁移不创建分支 / This migration creates no branch."""

depends_on = None
"""@brief 本迁移没有额外依赖 / This migration has no additional dependency."""

_ROLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
"""@brief PostgreSQL role 标识符白名单 / PostgreSQL role-identifier allowlist."""

_POSTGRES_IDENTIFIER_MAX_BYTES = 63
"""@brief PostgreSQL 标识符最大字节数 / PostgreSQL identifier byte limit."""

_MIGRATION_POLICY = "knowledge_version_timestamps_owner_0028"
"""@brief FORCE-RLS 表上的临时 owner policy / Temporary owner policy on the FORCE-RLS table."""

_CONSTRAINT = "knowledge_source_versions_timestamps"
"""@brief 时间顺序约束名称 / Timestamp-order constraint name."""


def _owner_role() -> str:
    """@brief 返回安全引用的 schema-owner role / Return the safely quoted schema-owner role."""

    configuration = op.get_context().config
    if configuration is None:
        raise RuntimeError("Alembic migration context has no configuration")
    value = configuration.get_main_option("aiws.owner_role")
    if (
        not value
        or _ROLE_IDENTIFIER_PATTERN.fullmatch(value) is None
        or len(value.encode("utf-8")) > _POSTGRES_IDENTIFIER_MAX_BYTES
    ):
        raise RuntimeError("missing or invalid dbctl role option: owner_role")
    return '"' + value.replace('"', '""') + '"'


def upgrade() -> None:
    """@brief 回填倒置时间并添加数据库约束 / Repair inverted timestamps and add a database constraint."""

    owner_role = _owner_role()
    op.execute(
        f"CREATE POLICY {_MIGRATION_POLICY} ON knowledge.source_versions "
        f"AS PERMISSIVE FOR ALL TO {owner_role} USING (true) WITH CHECK (true)"
    )
    op.execute("LOCK TABLE knowledge.source_versions IN SHARE ROW EXCLUSIVE MODE")
    op.execute(
        sa.text(
            "UPDATE knowledge.source_versions "
            "SET updated_at = created_at "
            "WHERE updated_at < created_at"
        )
    )
    remaining = op.get_bind().scalar(
        sa.text("SELECT count(*) FROM knowledge.source_versions WHERE updated_at < created_at")
    )
    if int(remaining or 0) != 0:
        raise RuntimeError("Knowledge source-version timestamp repair did not converge")
    op.create_check_constraint(
        _CONSTRAINT,
        "source_versions",
        "updated_at >= created_at",
        schema="knowledge",
    )
    op.execute(f"DROP POLICY {_MIGRATION_POLICY} ON knowledge.source_versions")


def downgrade() -> None:
    """@brief 移除时间约束但保留安全的数据修复 / Drop the constraint while retaining the safe repair."""

    op.drop_constraint(
        _CONSTRAINT,
        "source_versions",
        schema="knowledge",
        type_="check",
    )
