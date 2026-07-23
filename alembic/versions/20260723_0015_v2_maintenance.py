"""@brief 安装 API V2 时间驱动维护函数 / Install API V2 time-driven maintenance functions.

Revision ID: 20260723_0015
Revises: 20260723_0014
Create Date: 2026-07-23
"""

from __future__ import annotations

import re
from typing import Literal

import sqlalchemy as sa
from alembic import op

revision = "20260723_0015"
"""@brief 当前 Alembic revision / Current Alembic revision."""

down_revision = "20260723_0014"
"""@brief V2 idempotency revision / V2 idempotency predecessor."""

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

_INVITATION_FUNCTION = (
    "identity.expire_due_workspace_invitations(timestamp with time zone, integer)"
)
"""@brief 邀请函数完整签名 / Complete invitation-function signature."""

_IDEMPOTENCY_FUNCTION = (
    "identity.maintain_api_v2_idempotency_receipts(timestamp with time zone, integer)"
)
"""@brief 幂等维护函数完整签名 / Complete idempotency-maintenance function signature."""


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


def _create_due_indexes() -> None:
    """@brief 为全局有界扫描创建 partial indexes / Create partial indexes for bounded global scans.

    @return 无返回值 / No return value.

    @note 两个表均在紧邻的 0013/0014 才引入；使用普通事务型建索引可让函数、policy 与
        indexes 原子发布。若未来在大表上重建，应由独立运维 revision 使用
        ``CREATE INDEX CONCURRENTLY``。
    """
    op.create_index(
        "ix_workspace_invitations_pending_expiry",
        "workspace_invitations",
        ["expires_at", "id"],
        schema="identity",
        postgresql_where=sa.text("status = 'pending'"),
    )
    op.create_index(
        "ix_api_v2_idempotency_pending_expiry",
        "api_v2_idempotency_records",
        ["expires_at", "id"],
        schema="identity",
        postgresql_where=sa.text("status = 'pending'"),
    )


def _create_owner_maintenance_policies(owner_role: str) -> None:
    """@brief 在 FORCE RLS 下仅开放到期状态推进 / Permit only due-state advancement under FORCE RLS.

    @param owner_role SECURITY DEFINER 函数所有者 / Owner of the security-definer functions.
    @return 无返回值 / No return value.

    @note policy 只允许 owner 看见已经到期的 pending/completed 行；不会给予未到期行、
        accepted/revoked 邀请或 completed receipt body 的全表能力。
    """
    op.execute(
        "CREATE POLICY maintenance_owner_due_invitation_select "
        "ON identity.workspace_invitations AS PERMISSIVE FOR SELECT "
        f"TO {owner_role} USING ("
        "(status = 'pending' AND expires_at <= statement_timestamp()) OR "
        "(status = 'expired' AND resolved_at IS NOT NULL "
        "AND updated_at = resolved_at AND resolved_at <= statement_timestamp()))"
    )
    op.execute(
        "CREATE POLICY maintenance_owner_due_invitation_update "
        "ON identity.workspace_invitations AS PERMISSIVE FOR UPDATE "
        f"TO {owner_role} USING ("
        "status = 'pending' AND expires_at <= statement_timestamp()) "
        "WITH CHECK (status = 'expired' AND resolved_at IS NOT NULL "
        "AND accepted_by_user_id IS NULL AND updated_at = resolved_at)"
    )
    due_receipt = "expires_at <= statement_timestamp()"
    op.execute(
        "CREATE POLICY maintenance_owner_due_idempotency_select "
        "ON identity.api_v2_idempotency_records AS PERMISSIVE FOR SELECT "
        f"TO {owner_role} USING ({due_receipt} AND status IN ('pending', 'completed'))"
    )
    op.execute(
        "CREATE POLICY maintenance_owner_completed_idempotency_delete "
        "ON identity.api_v2_idempotency_records AS PERMISSIVE FOR DELETE "
        f"TO {owner_role} USING ({due_receipt} AND status = 'completed')"
    )
    op.execute(
        "CREATE POLICY maintenance_owner_completed_idempotency_lock "
        "ON identity.api_v2_idempotency_records AS PERMISSIVE FOR UPDATE "
        f"TO {owner_role} USING ({due_receipt} AND status = 'completed') "
        "WITH CHECK (false)"
    )


def _create_maintenance_functions() -> None:
    """@brief 创建有界、跳锁的两项维护函数 / Create two bounded, skip-locked maintenance functions.

    @return 无返回值 / No return value.

    @note caller 提供的时间只可令维护变慢，不能令其提前：``effective_now`` 被数据库
        ``statement_timestamp()`` 截断。
    """
    op.execute(
        """
        CREATE FUNCTION identity.expire_due_workspace_invitations(
            candidate_now timestamp with time zone,
            batch_limit integer
        )
        RETURNS integer
        LANGUAGE plpgsql
        VOLATILE
        SECURITY DEFINER
        SET search_path = pg_catalog, identity
        SET row_security = on
        AS $function$
        DECLARE
            effective_now timestamp with time zone;
            affected_count integer;
        BEGIN
            IF candidate_now IS NULL THEN
                RAISE EXCEPTION 'candidate_now must not be null' USING ERRCODE = '22004';
            END IF;
            IF batch_limit IS NULL OR batch_limit < 1 OR batch_limit > 1000 THEN
                RAISE EXCEPTION 'batch_limit must be between 1 and 1000' USING ERRCODE = '22023';
            END IF;
            effective_now := LEAST(candidate_now, statement_timestamp());

            WITH candidates AS MATERIALIZED (
                SELECT invitation.id
                FROM identity.workspace_invitations AS invitation
                WHERE invitation.status = 'pending'
                  AND invitation.expires_at <= effective_now
                  AND invitation.updated_at <= effective_now
                ORDER BY invitation.expires_at, invitation.id
                LIMIT batch_limit
                FOR UPDATE SKIP LOCKED
            )
            UPDATE identity.workspace_invitations AS invitation
            SET status = 'expired',
                revision = invitation.revision + 1,
                updated_at = effective_now,
                resolved_at = effective_now
            FROM candidates
            WHERE invitation.id = candidates.id;

            GET DIAGNOSTICS affected_count = ROW_COUNT;
            RETURN affected_count;
        END
        $function$
        """
    )
    op.execute(
        """
        CREATE FUNCTION identity.maintain_api_v2_idempotency_receipts(
            candidate_now timestamp with time zone,
            batch_limit integer
        )
        RETURNS TABLE (
            purged_completed_receipts integer,
            stranded_pending_receipts bigint,
            has_more_stranded_pending_receipts boolean,
            oldest_stranded_expires_at timestamp with time zone
        )
        LANGUAGE plpgsql
        VOLATILE
        SECURITY DEFINER
        SET search_path = pg_catalog, identity
        SET row_security = on
        AS $function$
        DECLARE
            effective_now timestamp with time zone;
            deleted_count integer;
        BEGIN
            IF candidate_now IS NULL THEN
                RAISE EXCEPTION 'candidate_now must not be null' USING ERRCODE = '22004';
            END IF;
            IF batch_limit IS NULL OR batch_limit < 1 OR batch_limit > 1000 THEN
                RAISE EXCEPTION 'batch_limit must be between 1 and 1000' USING ERRCODE = '22023';
            END IF;
            effective_now := LEAST(candidate_now, statement_timestamp());

            WITH candidates AS MATERIALIZED (
                SELECT receipt.id
                FROM identity.api_v2_idempotency_records AS receipt
                WHERE receipt.status = 'completed'
                  AND receipt.expires_at <= effective_now
                ORDER BY receipt.expires_at, receipt.id
                LIMIT batch_limit
                FOR UPDATE SKIP LOCKED
            )
            DELETE FROM identity.api_v2_idempotency_records AS receipt
            USING candidates
            WHERE receipt.id = candidates.id;

            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            RETURN QUERY
                WITH stranded AS MATERIALIZED (
                    SELECT receipt.expires_at, receipt.id
                    FROM identity.api_v2_idempotency_records AS receipt
                    WHERE receipt.status = 'pending'
                      AND receipt.expires_at <= effective_now
                    ORDER BY receipt.expires_at, receipt.id
                    LIMIT batch_limit + 1
                )
                SELECT deleted_count,
                       LEAST(count(*), batch_limit)::bigint,
                       count(*) > batch_limit,
                       min(stranded.expires_at)
                FROM stranded;
        END
        $function$
        """
    )


def _secure_functions(
    *,
    owner_role: str,
    app_role: str,
    dashboard_role: str,
    migrator_role: str,
) -> None:
    """@brief 设置 owner 并只给 app EXECUTE / Set ownership and grant EXECUTE only to app.

    @param owner_role FORCE RLS 下的函数所有者 / Function owner under forced RLS.
    @param app_role 唯一运行时 EXECUTE grantee / Sole runtime EXECUTE grantee.
    @param dashboard_role 无执行权的 dashboard 角色 / Dashboard role denied execution.
    @param migrator_role 无执行权的迁移 orchestrator / Migrator role denied execution.
    @return 无返回值 / No return value.
    """
    for signature in (_INVITATION_FUNCTION, _IDEMPOTENCY_FUNCTION):
        op.execute(
            f"REVOKE ALL PRIVILEGES ON FUNCTION {signature} "
            f"FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}"
        )
        op.execute(f"ALTER FUNCTION {signature} OWNER TO {owner_role}")
        op.execute(f"GRANT EXECUTE ON FUNCTION {signature} TO {app_role}")


def upgrade() -> None:
    """@brief 原子安装 V2 维护能力 / Atomically install V2 maintenance capability.

    @return 无返回值 / No return value.
    """
    owner_role = _configured_role("owner_role")
    app_role = _configured_role("app_role")
    dashboard_role = _configured_role("dashboard_role")
    migrator_role = _configured_role("migrator_role")
    _create_due_indexes()
    _create_owner_maintenance_policies(owner_role)
    _create_maintenance_functions()
    _secure_functions(
        owner_role=owner_role,
        app_role=app_role,
        dashboard_role=dashboard_role,
        migrator_role=migrator_role,
    )


def downgrade() -> None:
    """@brief 移除调度能力但保留合法终态数据 / Remove scheduling capability while retaining valid terminal data.

    @return 无返回值 / No return value.

    @note 已 expired 的邀请仍是 0013 合法状态；已清理的过期 receipt 无业务恢复价值，
        因此 downgrade 不伪造反向数据迁移。
    """
    op.execute(f"DROP FUNCTION {_IDEMPOTENCY_FUNCTION}")
    op.execute(f"DROP FUNCTION {_INVITATION_FUNCTION}")
    op.execute(
        "DROP POLICY maintenance_owner_completed_idempotency_lock "
        "ON identity.api_v2_idempotency_records"
    )
    op.execute(
        "DROP POLICY maintenance_owner_completed_idempotency_delete "
        "ON identity.api_v2_idempotency_records"
    )
    op.execute(
        "DROP POLICY maintenance_owner_due_idempotency_select "
        "ON identity.api_v2_idempotency_records"
    )
    op.execute(
        "DROP POLICY maintenance_owner_due_invitation_update "
        "ON identity.workspace_invitations"
    )
    op.execute(
        "DROP POLICY maintenance_owner_due_invitation_select "
        "ON identity.workspace_invitations"
    )
    op.drop_index(
        "ix_api_v2_idempotency_pending_expiry",
        table_name="api_v2_idempotency_records",
        schema="identity",
    )
    op.drop_index(
        "ix_workspace_invitations_pending_expiry",
        table_name="workspace_invitations",
        schema="identity",
    )
