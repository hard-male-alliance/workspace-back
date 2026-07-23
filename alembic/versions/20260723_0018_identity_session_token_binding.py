"""@brief 将 OAuth code/family 绑定到精确 login session / Bind OAuth codes and families to exact login sessions.

Revision ID: 20260723_0018
Revises: 20260723_0017
Create Date: 2026-07-23

历史实现没有记录 token family 来自哪一个登录会话。迁移不能猜测多设备归属：它保留
所有历史记录，但原子消费尚未兑换的 authorization code、吊销尚未吊销的 refresh
family，并把计数写入追加式迁移账本。此后所有活动 code/family 都必须带 session FK。
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260723_0018"
"""@brief 当前 Alembic revision / Current Alembic revision."""

down_revision = "20260723_0017"
"""@brief 线性前驱 revision / Linear predecessor revision."""

branch_labels = None
"""@brief 此迁移不创建分支 / This migration creates no branch."""

depends_on = None
"""@brief 此迁移没有额外依赖 / This migration has no extra dependency."""

_AUDIT_ID = "audit_20260723_0018_identity_session_binding"
"""@brief 历史 token 安全失效的追加式审计 ID / Append-only audit ID for legacy-token invalidation."""


def _count(statement: str) -> int:
    """@brief 执行本模块固定 count SQL / Execute a static count query from this module.

    @param statement 不含外部输入的 SQL / SQL containing no external input.
    @return 非负计数 / Non-negative count.
    """

    value = op.get_bind().scalar(sa.text(statement))
    return int(value or 0)


def upgrade() -> None:
    """@brief 发布精确 session→token-family 撤销关系 / Publish exact session-to-token-family revocation relationships."""

    op.add_column(
        "oauth_authorization_codes",
        sa.Column("login_session_id", sa.String(128)),
        schema="identity",
    )
    op.add_column(
        "oauth_refresh_token_families",
        sa.Column("login_session_id", sa.String(128)),
        schema="identity",
    )

    active_codes = _count(
        "SELECT count(*) FROM identity.oauth_authorization_codes "
        "WHERE consumed_at IS NULL"
    )
    active_families = _count(
        "SELECT count(*) FROM identity.oauth_refresh_token_families "
        "WHERE revoked_at IS NULL"
    )
    # Historical rows have no trustworthy device/session discriminator. Invalidating only the
    # active credentials is safer than fabricating ownership or deleting forensic history.
    op.execute(
        "UPDATE identity.oauth_authorization_codes "
        "SET consumed_at = transaction_timestamp() WHERE consumed_at IS NULL"
    )
    op.execute(
        "UPDATE identity.oauth_refresh_token_families "
        "SET revoked_at = transaction_timestamp() WHERE revoked_at IS NULL"
    )

    op.create_foreign_key(
        "fk_oauth_authorization_codes_login_session",
        "oauth_authorization_codes",
        "identity_login_sessions",
        ["login_session_id"],
        ["id"],
        source_schema="identity",
        referent_schema="identity",
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "fk_oauth_refresh_token_families_login_session",
        "oauth_refresh_token_families",
        "identity_login_sessions",
        ["login_session_id"],
        ["id"],
        source_schema="identity",
        referent_schema="identity",
        ondelete="RESTRICT",
    )
    op.create_index(
        "ix_oauth_authorization_codes_login_session_id",
        "oauth_authorization_codes",
        ["login_session_id"],
        schema="identity",
    )
    op.create_index(
        "ix_oauth_refresh_token_families_login_session_id",
        "oauth_refresh_token_families",
        ["login_session_id"],
        schema="identity",
    )
    op.create_check_constraint(
        "oauth_authorization_codes_active_session",
        "oauth_authorization_codes",
        "consumed_at IS NOT NULL OR login_session_id IS NOT NULL",
        schema="identity",
    )
    op.create_check_constraint(
        "oauth_refresh_token_families_active_session",
        "oauth_refresh_token_families",
        "revoked_at IS NOT NULL OR login_session_id IS NOT NULL",
        schema="identity",
    )

    if active_codes or active_families:
        op.execute(
            sa.text(
                """
                INSERT INTO identity.api_migration_audits (
                    id, migration_id, phase, event_type,
                    source_api_version, target_api_version, details
                ) VALUES (
                    :audit_id, :migration_id, 1, 'completed', 'v1', 'v2',
                    jsonb_build_object(
                        'invalidated_unbound_authorization_codes', :active_codes,
                        'revoked_unbound_refresh_families', :active_families,
                        'rule', 'fail closed because historical login-session ownership is unknowable'
                    )
                )
                """
            ).bindparams(
                audit_id=_AUDIT_ID,
                migration_id=revision,
                active_codes=active_codes,
                active_families=active_families,
            )
        )


def downgrade() -> None:
    """@brief 仅在没有新绑定状态与审计证据时回退 / Downgrade only without new bound state or audit evidence."""

    bound_rows = _count(
        "SELECT (SELECT count(*) FROM identity.oauth_authorization_codes "
        "WHERE login_session_id IS NOT NULL) + "
        "(SELECT count(*) FROM identity.oauth_refresh_token_families "
        "WHERE login_session_id IS NOT NULL)"
    )
    audit_rows = _count(
        "SELECT count(*) FROM identity.api_migration_audits "
        f"WHERE id = '{_AUDIT_ID}'"
    )
    if bound_rows or audit_rows:
        raise RuntimeError(
            "cannot downgrade session-bound OAuth state or discard legacy-token invalidation evidence"
        )
    op.drop_constraint(
        "oauth_refresh_token_families_active_session",
        "oauth_refresh_token_families",
        schema="identity",
        type_="check",
    )
    op.drop_constraint(
        "oauth_authorization_codes_active_session",
        "oauth_authorization_codes",
        schema="identity",
        type_="check",
    )
    op.drop_index(
        "ix_oauth_refresh_token_families_login_session_id",
        table_name="oauth_refresh_token_families",
        schema="identity",
    )
    op.drop_index(
        "ix_oauth_authorization_codes_login_session_id",
        table_name="oauth_authorization_codes",
        schema="identity",
    )
    op.drop_constraint(
        "fk_oauth_refresh_token_families_login_session",
        "oauth_refresh_token_families",
        schema="identity",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_oauth_authorization_codes_login_session",
        "oauth_authorization_codes",
        schema="identity",
        type_="foreignkey",
    )
    op.drop_column("oauth_refresh_token_families", "login_session_id", schema="identity")
    op.drop_column("oauth_authorization_codes", "login_session_id", schema="identity")
