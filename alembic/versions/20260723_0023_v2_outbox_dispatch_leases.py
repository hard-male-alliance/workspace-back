"""@brief 为统一 outbox 增加可恢复租约调度 / Add recoverable lease dispatch to the unified outbox.

Revision ID: 20260723_0023
Revises: 20260723_0022
Create Date: 2026-07-23

原有 ``processing`` 只有状态而没有租约，worker 崩溃会永久卡死。本迁移将
无法证明所有权的旧 processing 行安全重置为 pending，然后通过
``SKIP LOCKED``、token digest、过期续租与有界重试函数恢复至少一次处理。
"""

from __future__ import annotations

import re
from typing import Literal

import sqlalchemy as sa
from alembic import op

revision = "20260723_0023"
"""@brief 当前 Alembic revision / Current Alembic revision."""

down_revision = "20260723_0022"
"""@brief Interview V2 persistence 前驱 / Interview V2 persistence predecessor."""

branch_labels = None
"""@brief 本迁移不创建分支 / This migration creates no branch."""

depends_on = None
"""@brief 本迁移没有额外依赖 / This migration has no extra dependency."""

RuntimeRoleOption = Literal[
    "owner_role",
    "app_role",
    "dashboard_role",
    "migrator_role",
]
"""@brief Alembic 接收的 dbctl role 选项 / dbctl role options accepted by Alembic."""

_ROLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
"""@brief PostgreSQL role 标识白名单 / PostgreSQL role identifier allowlist."""

_MIGRATION_POLICY = "outbox_dispatch_migration_0023"
"""@brief upgrade/downgrade 事务内的临时 owner policy / Temporary owner policy inside migration transactions."""

_OWNER_SELECT_POLICY = "outbox_dispatch_owner_select_0023"
"""@brief 窄函数仅可见待处理状态 / Narrow functions can see only dispatchable states."""

_OWNER_UPDATE_POLICY = "outbox_dispatch_owner_update_0023"
"""@brief 窄函数只能从待处理状态推进 / Narrow functions update only from dispatchable states."""

_FUNCTION_SIGNATURES = (
    "agent.claim_outbox_events(text,timestamp with time zone,integer,integer,integer,text[])",
    "agent.renew_outbox_event_lease(text,text,timestamp with time zone,integer)",
    "agent.complete_outbox_event(text,text,timestamp with time zone)",
    "agent.retry_outbox_event(text,text,text,timestamp with time zone,integer)",
)
"""@brief 本 revision 拥有的窄函数签名 / Narrow-function signatures owned by this revision."""


def _configured_role(option: RuntimeRoleOption) -> str:
    """@brief 返回经白名单校验并引用的 role / Return an allowlisted and quoted role.

    @param option dbctl role 配置键 / dbctl role configuration key.
    @return 可安全拼入固定 DDL 的引用 role / Quoted role safe for static DDL.
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


def _install_migration_policy(owner_role: str) -> None:
    """@brief 在当前 migration 事务中安装临时 owner policy / Install a temporary owner policy in this migration transaction.

    @param owner_role 已引用 schema owner / Quoted schema owner.
    """
    op.execute(
        f"CREATE POLICY {_MIGRATION_POLICY} ON agent.outbox_events "
        f"AS PERMISSIVE FOR ALL TO {owner_role} USING (true) WITH CHECK (true)"
    )


def _install_runtime_owner_policies(owner_role: str) -> None:
    """@brief 用状态限定 policy 取代临时全可见策略 / Replace migration visibility with state-bounded policies.

    @param owner_role 已引用 schema owner / Quoted schema owner.
    """
    op.execute(f"DROP POLICY {_MIGRATION_POLICY} ON agent.outbox_events")
    op.execute(
        f"CREATE POLICY {_OWNER_SELECT_POLICY} ON agent.outbox_events "
        f"AS PERMISSIVE FOR SELECT TO {owner_role} "
        "USING (status IN ('pending', 'processing') OR "
        "(status IN ('published', 'failed') AND updated_at = statement_timestamp()))"
    )
    op.execute(
        f"CREATE POLICY {_OWNER_UPDATE_POLICY} ON agent.outbox_events "
        f"AS PERMISSIVE FOR UPDATE TO {owner_role} "
        "USING (status IN ('pending', 'processing')) "
        "WITH CHECK (status IN ('pending', 'processing', 'published', 'failed'))"
    )


def _backfill_and_constrain() -> None:
    """@brief expand、恢复 stranded processing、再添加关联约束 / Expand, recover stranded processing, then constrain."""
    op.add_column(
        "outbox_events",
        sa.Column("lease_token_hash", sa.String(64)),
        schema="agent",
    )
    op.add_column(
        "outbox_events",
        sa.Column("lease_expires_at", sa.DateTime(timezone=True)),
        schema="agent",
    )
    op.add_column(
        "outbox_events",
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("transaction_timestamp()"),
        ),
        schema="agent",
    )
    op.add_column(
        "outbox_events",
        sa.Column("last_error_code", sa.String(101)),
        schema="agent",
    )
    op.execute(
        """
        DO $migration$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM agent.outbox_events
                WHERE status <> 'published' AND published_at IS NOT NULL
            ) THEN
                RAISE EXCEPTION
                    'outbox contains non-published rows with published_at; repair before 0023'
                    USING ERRCODE = 'check_violation';
            END IF;
        END
        $migration$
        """
    )
    op.execute(
        """
        UPDATE agent.outbox_events
        SET status = CASE WHEN status = 'processing' THEN 'pending' ELSE status END,
            published_at = CASE
                WHEN status = 'published'
                THEN COALESCE(published_at, updated_at, occurred_at)
                ELSE NULL
            END,
            next_attempt_at = COALESCE(updated_at, occurred_at),
            updated_at = statement_timestamp()
        """
    )
    op.alter_column("outbox_events", "next_attempt_at", schema="agent", nullable=False)
    op.create_check_constraint(
        "outbox_events_dispatch_lease",
        "outbox_events",
        "(status = 'pending' AND lease_token_hash IS NULL AND lease_expires_at IS NULL "
        "AND published_at IS NULL) OR "
        "(status = 'processing' AND lease_token_hash IS NOT NULL "
        "AND lease_expires_at IS NOT NULL AND published_at IS NULL) OR "
        "(status = 'published' AND lease_token_hash IS NULL AND lease_expires_at IS NULL "
        "AND published_at IS NOT NULL) OR "
        "(status = 'failed' AND lease_token_hash IS NULL AND lease_expires_at IS NULL "
        "AND published_at IS NULL)",
        schema="agent",
    )
    op.create_check_constraint(
        "outbox_events_dispatch_values",
        "outbox_events",
        "attempt_count >= 0 "
        "AND (lease_token_hash IS NULL OR lease_token_hash ~ '^[a-f0-9]{64}$') "
        "AND (last_error_code IS NULL OR "
        "last_error_code ~ '^[a-z][a-z0-9_.-]{2,100}$')",
        schema="agent",
    )
    op.create_index(
        "ix_outbox_events_dispatch_pending",
        "outbox_events",
        ["next_attempt_at", "occurred_at", "id"],
        schema="agent",
        postgresql_where=sa.text("status = 'pending'"),
    )
    op.create_index(
        "ix_outbox_events_dispatch_processing",
        "outbox_events",
        ["lease_expires_at", "occurred_at", "id"],
        schema="agent",
        postgresql_where=sa.text("status = 'processing'"),
    )


def _create_claim_function() -> None:
    """@brief 创建可恢复 SKIP LOCKED claim 函数 / Create the recoverable SKIP-LOCKED claim function."""
    op.execute(
        """
        CREATE FUNCTION agent.claim_outbox_events(
            candidate_lease_token_hash text,
            candidate_now timestamp with time zone,
            candidate_lease_seconds integer,
            candidate_batch_size integer,
            candidate_maximum_attempts integer,
            candidate_event_types text[] DEFAULT NULL
        )
        RETURNS TABLE (
            event_id text,
            workspace_id text,
            actor_id text,
            aggregate_type text,
            aggregate_id text,
            subject_revision integer,
            event_type text,
            payload jsonb,
            attempt_count integer,
            lease_expires_at timestamp with time zone
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, agent
        SET row_security = on
        AS $function$
        DECLARE
            effective_now timestamp with time zone;
        BEGIN
            IF candidate_lease_token_hash IS NULL
               OR candidate_lease_token_hash !~ '^[a-f0-9]{64}$'
               OR candidate_now IS NULL
               OR candidate_lease_seconds NOT BETWEEN 5 AND 900
               OR candidate_batch_size NOT BETWEEN 1 AND 100
               OR candidate_maximum_attempts NOT BETWEEN 1 AND 100
               OR (candidate_event_types IS NOT NULL AND (
                    cardinality(candidate_event_types) NOT BETWEEN 1 AND 32
                    OR EXISTS (
                        SELECT 1
                        FROM unnest(candidate_event_types) AS candidate_event_type
                        WHERE candidate_event_type IS NULL
                           OR length(candidate_event_type) NOT BETWEEN 3 AND 128
                           OR candidate_event_type !~ '^[a-z][a-z0-9_.-]{2,127}$'
                    )
                    OR cardinality(candidate_event_types)
                        <> cardinality(ARRAY(SELECT DISTINCT unnest(candidate_event_types)))
               )) THEN
                RAISE EXCEPTION 'invalid outbox claim arguments'
                    USING ERRCODE = '22023';
            END IF;
            effective_now := LEAST(candidate_now, statement_timestamp());
            RETURN QUERY
            WITH candidates AS (
                SELECT event.id
                FROM agent.outbox_events AS event
                WHERE event.attempt_count < candidate_maximum_attempts
                  AND (
                    candidate_event_types IS NULL
                    OR event.event_type = ANY(candidate_event_types)
                  )
                  AND (
                    (event.status = 'pending' AND event.next_attempt_at <= effective_now)
                    OR
                    (event.status = 'processing' AND event.lease_expires_at <= effective_now)
                  )
                ORDER BY
                    CASE WHEN event.status = 'processing' THEN 0 ELSE 1 END,
                    COALESCE(event.lease_expires_at, event.next_attempt_at),
                    event.occurred_at,
                    event.id
                FOR UPDATE SKIP LOCKED
                LIMIT candidate_batch_size
            ), claimed AS (
                UPDATE agent.outbox_events AS event
                SET status = 'processing',
                    attempt_count = event.attempt_count + 1,
                    lease_token_hash = candidate_lease_token_hash,
                    lease_expires_at = statement_timestamp()
                        + candidate_lease_seconds * interval '1 second',
                    updated_at = statement_timestamp()
                FROM candidates
                WHERE event.id = candidates.id
                RETURNING
                    event.id,
                    event.workspace_id,
                    event.resource_owner_id,
                    event.aggregate_type,
                    event.aggregate_id,
                    event.subject_revision,
                    event.event_type,
                    event.payload,
                    event.attempt_count,
                    event.lease_expires_at,
                    event.occurred_at
            )
            SELECT
                claimed.id::text,
                claimed.workspace_id::text,
                claimed.resource_owner_id::text,
                claimed.aggregate_type::text,
                claimed.aggregate_id::text,
                claimed.subject_revision,
                claimed.event_type::text,
                claimed.payload,
                claimed.attempt_count,
                claimed.lease_expires_at
            FROM claimed
            ORDER BY claimed.occurred_at, claimed.id;
        END
        $function$
        """
    )


def _create_transition_functions() -> None:
    """@brief 创建 renew/complete/retry token-CAS 函数 / Create renew, complete, and retry token-CAS functions."""
    op.execute(
        """
        CREATE FUNCTION agent.renew_outbox_event_lease(
            candidate_event_id text,
            candidate_lease_token_hash text,
            candidate_now timestamp with time zone,
            candidate_lease_seconds integer
        ) RETURNS boolean
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, agent
        SET row_security = on
        AS $function$
        BEGIN
            IF candidate_event_id IS NULL OR candidate_event_id = ''
               OR candidate_lease_token_hash IS NULL
               OR candidate_lease_token_hash !~ '^[a-f0-9]{64}$'
               OR candidate_now IS NULL
               OR candidate_lease_seconds NOT BETWEEN 5 AND 900 THEN
                RAISE EXCEPTION 'invalid outbox renew arguments' USING ERRCODE = '22023';
            END IF;
            UPDATE agent.outbox_events AS event
            SET lease_expires_at = statement_timestamp()
                    + candidate_lease_seconds * interval '1 second',
                updated_at = statement_timestamp()
            WHERE event.id = candidate_event_id
              AND event.status = 'processing'
              AND event.lease_token_hash = candidate_lease_token_hash;
            RETURN FOUND;
        END
        $function$
        """
    )
    op.execute(
        """
        CREATE FUNCTION agent.complete_outbox_event(
            candidate_event_id text,
            candidate_lease_token_hash text,
            candidate_completed_at timestamp with time zone
        ) RETURNS boolean
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, agent
        SET row_security = on
        AS $function$
        BEGIN
            IF candidate_event_id IS NULL OR candidate_event_id = ''
               OR candidate_lease_token_hash IS NULL
               OR candidate_lease_token_hash !~ '^[a-f0-9]{64}$'
               OR candidate_completed_at IS NULL THEN
                RAISE EXCEPTION 'invalid outbox completion arguments' USING ERRCODE = '22023';
            END IF;
            UPDATE agent.outbox_events AS event
            SET status = 'published',
                published_at = GREATEST(
                    event.occurred_at,
                    LEAST(candidate_completed_at, statement_timestamp())
                ),
                lease_token_hash = NULL,
                lease_expires_at = NULL,
                last_error_code = NULL,
                updated_at = statement_timestamp()
            WHERE event.id = candidate_event_id
              AND event.status = 'processing'
              AND event.lease_token_hash = candidate_lease_token_hash;
            RETURN FOUND;
        END
        $function$
        """
    )
    op.execute(
        """
        CREATE FUNCTION agent.retry_outbox_event(
            candidate_event_id text,
            candidate_lease_token_hash text,
            candidate_error_code text,
            candidate_retry_at timestamp with time zone,
            candidate_maximum_attempts integer
        ) RETURNS boolean
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, agent
        SET row_security = on
        AS $function$
        BEGIN
            IF candidate_event_id IS NULL OR candidate_event_id = ''
               OR candidate_lease_token_hash IS NULL
               OR candidate_lease_token_hash !~ '^[a-f0-9]{64}$'
               OR candidate_error_code IS NULL
               OR candidate_error_code !~ '^[a-z][a-z0-9_.-]{2,100}$'
               OR candidate_retry_at IS NULL
               OR candidate_maximum_attempts NOT BETWEEN 1 AND 100 THEN
                RAISE EXCEPTION 'invalid outbox retry arguments' USING ERRCODE = '22023';
            END IF;
            UPDATE agent.outbox_events AS event
            SET status = CASE
                    WHEN event.attempt_count >= candidate_maximum_attempts
                    THEN 'failed'
                    ELSE 'pending'
                END,
                next_attempt_at = GREATEST(
                    statement_timestamp(),
                    LEAST(
                        candidate_retry_at,
                        statement_timestamp() + interval '1 day'
                    )
                ),
                lease_token_hash = NULL,
                lease_expires_at = NULL,
                last_error_code = candidate_error_code,
                updated_at = statement_timestamp()
            WHERE event.id = candidate_event_id
              AND event.status = 'processing'
              AND event.lease_token_hash = candidate_lease_token_hash;
            RETURN FOUND;
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
    """@brief 转移函数所有权并仅授予 app EXECUTE / Transfer ownership and grant only app execution."""
    for signature in _FUNCTION_SIGNATURES:
        op.execute(f"ALTER FUNCTION {signature} OWNER TO {owner_role}")
        op.execute(
            f"REVOKE ALL PRIVILEGES ON FUNCTION {signature} "
            f"FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}"
        )
        op.execute(f"GRANT EXECUTE ON FUNCTION {signature} TO {app_role}")


def upgrade() -> None:
    """@brief 发布租约式 durable outbox dispatcher / Publish the lease-based durable outbox dispatcher."""
    owner_role = _configured_role("owner_role")
    app_role = _configured_role("app_role")
    dashboard_role = _configured_role("dashboard_role")
    migrator_role = _configured_role("migrator_role")
    _install_migration_policy(owner_role)
    _backfill_and_constrain()
    _install_runtime_owner_policies(owner_role)
    _create_claim_function()
    _create_transition_functions()
    _secure_functions(
        owner_role=owner_role,
        app_role=app_role,
        dashboard_role=dashboard_role,
        migrator_role=migrator_role,
    )
    op.execute(f"REVOKE UPDATE ON TABLE agent.outbox_events FROM {app_role}")


def downgrade() -> None:
    """@brief 仅在无活动租约/重试证据时回退 / Downgrade only without active leases or retry evidence."""
    owner_role = _configured_role("owner_role")
    app_role = _configured_role("app_role")
    op.execute(f"DROP POLICY {_OWNER_UPDATE_POLICY} ON agent.outbox_events")
    op.execute(f"DROP POLICY {_OWNER_SELECT_POLICY} ON agent.outbox_events")
    _install_migration_policy(owner_role)
    op.execute(
        """
        DO $migration$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM agent.outbox_events
                WHERE status = 'processing'
                   OR lease_token_hash IS NOT NULL
                   OR lease_expires_at IS NOT NULL
                   OR last_error_code IS NOT NULL
            ) THEN
                RAISE EXCEPTION
                    'cannot downgrade 0023 while outbox leases or retry evidence exist'
                    USING ERRCODE = 'object_not_in_prerequisite_state';
            END IF;
        END
        $migration$
        """
    )
    for signature in reversed(_FUNCTION_SIGNATURES):
        op.execute(f"DROP FUNCTION {signature}")
    op.execute(
        f"GRANT UPDATE (status, published_at, attempt_count, updated_at) "
        f"ON TABLE agent.outbox_events TO {app_role}"
    )
    op.drop_index(
        "ix_outbox_events_dispatch_processing",
        table_name="outbox_events",
        schema="agent",
    )
    op.drop_index(
        "ix_outbox_events_dispatch_pending",
        table_name="outbox_events",
        schema="agent",
    )
    op.drop_constraint(
        "outbox_events_dispatch_values",
        "outbox_events",
        schema="agent",
        type_="check",
    )
    op.drop_constraint(
        "outbox_events_dispatch_lease",
        "outbox_events",
        schema="agent",
        type_="check",
    )
    for column in (
        "last_error_code",
        "next_attempt_at",
        "lease_expires_at",
        "lease_token_hash",
    ):
        op.drop_column("outbox_events", column, schema="agent")
    op.execute(f"DROP POLICY {_MIGRATION_POLICY} ON agent.outbox_events")
