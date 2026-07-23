"""@brief 创建加密身份邮件 outbox 与原子频控账本 / Create encrypted identity-email outbox and atomic rate ledger.

Revision ID: 20260723_0020
Revises: 20260723_0019
Create Date: 2026-07-23

请求事务只写入 AES-256-GCM 密文；独立 worker 以有界租约发送。终态行立即清除密文，
仅保留不可逆收件人摘要、投递结果和时间戳作为有限期审计证据。
"""

from __future__ import annotations

import re
from typing import Literal

import sqlalchemy as sa
from alembic import op

revision = "20260723_0020"
"""@brief 当前 Alembic revision / Current Alembic revision."""

down_revision = "20260723_0019"
"""@brief Knowledge V2 persistence revision / Knowledge V2 persistence predecessor."""

branch_labels = None
"""@brief 此迁移不创建分支 / This migration creates no branch."""

depends_on = None
"""@brief 此迁移没有额外依赖 / This migration has no extra dependency."""

RuntimeRoleOption = Literal["app_role", "dashboard_role", "migrator_role"]
"""@brief 本 revision 使用的数据库角色配置 / Database-role options used by this revision."""

_ROLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
"""@brief PostgreSQL role 标识白名单 / PostgreSQL role identifier allowlist."""

_TABLES = (
    "identity.identity_email_rate_limits",
    "identity.identity_email_outbox",
)
"""@brief 本迁移拥有的全局基础设施表 / Global infrastructure tables owned here."""


def _configured_role(option: RuntimeRoleOption) -> str:
    """@brief 返回安全引用的运行时 role / Return a safely quoted runtime role.

    @param option Alembic ``aiws.*`` role 配置键 / Alembic role configuration key.
    @return 可安全拼入固定 DDL 的引用标识符 / Quoted identifier safe for static DDL.
    @raise RuntimeError 配置缺失或非法时抛出 / Raised for missing or invalid configuration.
    """

    configuration = op.get_context().config
    if configuration is None:
        raise RuntimeError("Alembic migration context has no configuration")
    value = configuration.get_main_option(f"aiws.{option}")
    if (
        not value
        or _ROLE_IDENTIFIER_PATTERN.fullmatch(value) is None
        or len(value.encode("utf-8")) > 63
    ):
        raise RuntimeError(f"missing or invalid dbctl role option: {option}")
    return '"' + value.replace('"', '""') + '"'


def upgrade() -> None:
    """@brief 发布原子频控与 durable encrypted outbox / Publish atomic rate limiting and a durable encrypted outbox.

    @return 无返回值 / No return value.
    """

    op.create_table(
        "identity_email_rate_limits",
        sa.Column("dimension_kind", sa.String(16), nullable=False),
        sa.Column("dimension_digest", sa.LargeBinary(), nullable=False),
        sa.Column("window_started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("request_count", sa.Integer(), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("transaction_timestamp()"),
        ),
        sa.PrimaryKeyConstraint(
            "dimension_kind",
            "dimension_digest",
            "window_started_at",
            name="pk_identity_email_rate_limits",
        ),
        sa.CheckConstraint(
            "dimension_kind IN ('account', 'device', 'network')",
            name="ck_identity_email_rate_limits_kind",
        ),
        sa.CheckConstraint(
            "octet_length(dimension_digest) = 32",
            name="ck_identity_email_rate_limits_digest",
        ),
        sa.CheckConstraint(
            "request_count BETWEEN 1 AND 100000",
            name="ck_identity_email_rate_limits_count",
        ),
        sa.CheckConstraint(
            "window_started_at = date_trunc('hour', window_started_at)",
            name="ck_identity_email_rate_limits_window",
        ),
        schema="identity",
    )
    op.create_index(
        "ix_identity_email_rate_limits_window",
        "identity_email_rate_limits",
        ["window_started_at", "dimension_kind", "dimension_digest"],
        schema="identity",
    )

    op.create_table(
        "identity_email_outbox",
        sa.Column("id", sa.String(160), primary_key=True),
        sa.Column("message_kind", sa.String(32), nullable=False),
        sa.Column("recipient_digest", sa.LargeBinary(), nullable=False),
        sa.Column("key_id", sa.String(64), nullable=False),
        sa.Column("aad_version", sa.SmallInteger(), nullable=False, server_default=sa.text("1")),
        sa.Column("nonce", sa.LargeBinary()),
        sa.Column("ciphertext", sa.LargeBinary()),
        sa.Column("status", sa.String(16), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("transaction_timestamp()"),
        ),
        sa.Column("lease_owner", sa.String(160)),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True)),
        sa.Column("last_failure_code", sa.String(64)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("transaction_timestamp()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("transaction_timestamp()"),
        ),
        sa.Column("sent_at", sa.DateTime(timezone=True)),
        sa.Column("dead_at", sa.DateTime(timezone=True)),
        sa.Column("payload_cleared_at", sa.DateTime(timezone=True)),
        sa.Column("retain_until", sa.DateTime(timezone=True)),
        sa.CheckConstraint(
            "message_kind IN ('verification_code', 'recovery_notification')",
            name="ck_identity_email_outbox_kind",
        ),
        sa.CheckConstraint(
            "octet_length(recipient_digest) = 32",
            name="ck_identity_email_outbox_recipient_digest",
        ),
        sa.CheckConstraint(
            "key_id ~ '^[A-Za-z0-9._-]{1,64}$' AND aad_version = 1",
            name="ck_identity_email_outbox_encryption_metadata",
        ),
        sa.CheckConstraint(
            "attempts BETWEEN 0 AND 1000 AND updated_at >= created_at",
            name="ck_identity_email_outbox_counters",
        ),
        sa.CheckConstraint(
            "(status = 'pending' AND nonce IS NOT NULL AND octet_length(nonce) = 12 "
            "AND ciphertext IS NOT NULL AND octet_length(ciphertext) >= 16 "
            "AND lease_owner IS NULL AND lease_expires_at IS NULL "
            "AND sent_at IS NULL AND dead_at IS NULL AND payload_cleared_at IS NULL "
            "AND retain_until IS NULL) OR "
            "(status = 'leased' AND nonce IS NOT NULL AND octet_length(nonce) = 12 "
            "AND ciphertext IS NOT NULL AND octet_length(ciphertext) >= 16 "
            "AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL "
            "AND sent_at IS NULL AND dead_at IS NULL AND payload_cleared_at IS NULL "
            "AND retain_until IS NULL) OR "
            "(status = 'sent' AND nonce IS NULL AND ciphertext IS NULL "
            "AND lease_owner IS NULL AND lease_expires_at IS NULL "
            "AND sent_at IS NOT NULL AND dead_at IS NULL "
            "AND payload_cleared_at IS NOT NULL AND retain_until IS NOT NULL) OR "
            "(status = 'dead' AND nonce IS NULL AND ciphertext IS NULL "
            "AND lease_owner IS NULL AND lease_expires_at IS NULL "
            "AND sent_at IS NULL AND dead_at IS NOT NULL "
            "AND payload_cleared_at IS NOT NULL AND retain_until IS NOT NULL)",
            name="ck_identity_email_outbox_state",
        ),
        schema="identity",
    )
    op.create_index(
        "ix_identity_email_outbox_pending_due",
        "identity_email_outbox",
        ["available_at", "id"],
        schema="identity",
        postgresql_where=sa.text("status = 'pending'"),
    )
    op.create_index(
        "ix_identity_email_outbox_expired_lease",
        "identity_email_outbox",
        ["lease_expires_at", "id"],
        schema="identity",
        postgresql_where=sa.text("status = 'leased'"),
    )
    op.create_index(
        "ix_identity_email_outbox_terminal_retention",
        "identity_email_outbox",
        ["retain_until", "id"],
        schema="identity",
        postgresql_where=sa.text("status IN ('sent', 'dead')"),
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
    """@brief 仅允许空表 downgrade / Downgrade only when both security tables are empty.

    @return 无返回值 / No return value.
    @raise RuntimeError 存在频控或邮件审计状态时拒绝破坏数据 / Raised for retained state.
    """

    connection = op.get_bind()
    populated = [
        table
        for table in _TABLES
        if int(connection.scalar(sa.text(f"SELECT count(*) FROM {table}")) or 0) > 0
    ]
    if populated:
        raise RuntimeError(
            "cannot downgrade non-empty identity email state: " + ", ".join(populated)
        )
    op.drop_table("identity_email_outbox", schema="identity")
    op.drop_table("identity_email_rate_limits", schema="identity")
