"""@brief 闭合统一 outbox work/notification 生命周期 / Close unified-outbox work/notification lifecycles.

Revision ID: 20260723_0027
Revises: 20260723_0026
Create Date: 2026-07-23

Durable work remains claimable until its consumer completes it.  Replay/SSE notifications already
exist durably when their business transaction commits, so this revision marks them published at
insert-time, precisely backfills untouched historical pending notifications, and rejects unknown
or ambiguous event types.  A narrow retention function may delete only replay-expired terminal
rows; pending and processing work are never swept.
"""

from __future__ import annotations

import re
from typing import Literal

import sqlalchemy as sa
from alembic import op

revision = "20260723_0027"
"""@brief 当前 Alembic revision / Current Alembic revision."""

down_revision = "20260723_0026"
"""@brief Job 取消补偿迁移前驱 / Job-cancellation compensation predecessor."""

branch_labels = None
"""@brief 本迁移不创建分支 / This migration creates no branch."""

depends_on = None
"""@brief 本迁移没有额外依赖 / This migration has no additional dependency."""

RuntimeRoleOption = Literal[
    "owner_role",
    "app_role",
    "dashboard_role",
    "migrator_role",
]
"""@brief Alembic 接受的 dbctl role 选项 / Dbctl role options accepted by Alembic."""

_ROLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
"""@brief PostgreSQL role 标识符白名单 / PostgreSQL role-identifier allowlist."""

_POSTGRES_IDENTIFIER_MAX_BYTES = 63
"""@brief PostgreSQL 标识符最大字节数 / Maximum PostgreSQL identifier size."""

_MIGRATION_POLICY = "outbox_lifecycle_migration_0027"
"""@brief upgrade/downgrade 事务的临时 owner policy / Temporary owner policy for migration transactions."""

_RETENTION_SELECT_POLICY = "outbox_retention_owner_select_0027"
"""@brief 清理函数仅可锁定已过期终态行 / Retention function may select only expired terminal rows."""

_RETENTION_DELETE_POLICY = "outbox_retention_owner_delete_0027"
"""@brief 清理函数仅可删除已过期终态行 / Retention function may delete only expired terminal rows."""

_MIGRATION_MARKER = "_migration_0027"
"""@brief 记录精确回填前状态的 extensions marker / Extensions marker recording exact pre-backfill state."""

_MIGRATION_REASON = "outbox_notification_publish_on_insert"
"""@brief marker 内的稳定语义标签 / Stable semantic label stored in the marker."""

_WORK_EVENT_TYPES = (
    "agent.run.queued",
    "agent.tool_decision.recorded",
    "connection.revocation_requested",
    "interview.job.queued",
    "knowledge_source.deletion_requested",
    "knowledge_source.job_created",
    "resume.job_created",
)
"""@brief 需要 durable dispatcher 的工作事件闭集 / Closed durable-dispatch work-event set."""

_NOTIFICATION_EVENT_TYPES = (
    "agent.citation.added",
    "agent.message.completed",
    "agent.message.delta",
    "agent.run.cancelled",
    "agent.run.completed",
    "agent.run.failed",
    "agent.run.started",
    "agent.run.updated",
    "agent.status",
    "agent.tool_approval.expired",
    "agent.tool_approval.required",
    "connection.created",
    "job.updated",
    "knowledge_source.created",
    "knowledge_source.updated",
    "knowledge_source.version_created",
    "resume.created",
    "resume.deleted",
    "resume.metadata_updated",
    "resume.operations_applied",
    "resume.proposal_decided",
    "resume.updated",
)
"""@brief 写入即 published 的 replay/SSE 通知闭集 / Closed replay/SSE notification set published on insert."""

_FUNCTION_SIGNATURE = (
    "agent.purge_expired_outbox_events(timestamp with time zone,integer)"
)
"""@brief 本 revision 拥有的窄函数签名 / Narrow-function signature owned by this revision."""


def _configured_role(option: RuntimeRoleOption) -> str:
    """@brief 返回经白名单校验并引用的 role / Return an allowlisted and quoted role.

    @param option dbctl role 配置键 / Dbctl role configuration key.
    @return 可安全拼入固定 DDL 的引用 role / Quoted role safe for static DDL.
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


def _sql_values(values: tuple[str, ...]) -> str:
    """@brief 把模块内闭集生成固定 SQL literal list / Build a static SQL literal list from a module-owned closed set.

    @param values 仅由本 migration 定义的事件名 / Event names defined only by this migration.
    @return 逗号分隔的 SQL literals / Comma-separated SQL literals.
    """

    if not values or len(values) != len(set(values)):
        raise AssertionError("outbox migration event sets must be non-empty and unique")
    if any(re.fullmatch(r"[a-z][a-z0-9_.-]{2,127}", value) is None for value in values):
        raise AssertionError("outbox migration contains an invalid event type")
    return ", ".join(f"'{value}'" for value in values)


_WORK_SQL = _sql_values(_WORK_EVENT_TYPES)
"""@brief work 闭集的固定 SQL list / Static SQL list for the work-event set."""

_NOTIFICATION_SQL = _sql_values(_NOTIFICATION_EVENT_TYPES)
"""@brief notification 闭集的固定 SQL list / Static SQL list for the notification-event set."""

_KNOWN_SQL = _sql_values((*_WORK_EVENT_TYPES, *_NOTIFICATION_EVENT_TYPES))
"""@brief 全部允许事件的固定 SQL list / Static SQL list for every accepted event."""


def _install_migration_policy(owner_role: str) -> None:
    """@brief 在当前 migration 事务安装临时可见性 / Install temporary visibility in the current migration transaction.

    @param owner_role 已安全引用的 schema owner / Safely quoted schema owner.
    """

    op.execute(
        f"CREATE POLICY {_MIGRATION_POLICY} ON agent.outbox_events "
        f"AS PERMISSIVE FOR ALL TO {owner_role} USING (true) WITH CHECK (true)"
    )


def _remove_migration_policy() -> None:
    """@brief 移除 migration-only 全表可见性 / Remove migration-only whole-table visibility."""

    op.execute(f"DROP POLICY {_MIGRATION_POLICY} ON agent.outbox_events")


def _lock_outbox() -> None:
    """@brief 冻结 producer/dispatcher 写入以获得单一快照 / Freeze producer/dispatcher writes for one migration snapshot."""

    op.execute("LOCK TABLE agent.outbox_events IN SHARE ROW EXCLUSIVE MODE")


def _count(statement: str) -> int:
    """@brief 执行仅由本模块常量组成的 count SQL / Execute count SQL composed only from module constants.

    @param statement 无外部输入的 SQL / SQL containing no external input.
    @return 非负 count / Non-negative count.
    """

    value = op.get_bind().scalar(sa.text(statement))
    return int(value or 0)


def _preflight_upgrade() -> None:
    """@brief 拒绝未知事件、marker 冲突与暧昧 notification 执行证据 / Reject unknown events, marker collisions, and ambiguous notification execution evidence.

    @raise RuntimeError 任何行无法安全分类或精确回填时抛出 / Raised when any row
        cannot be classified or backfilled exactly.
    """

    marker_collisions = _count(
        f"SELECT count(*) FROM agent.outbox_events "
        f"WHERE extensions ? '{_MIGRATION_MARKER}'"
    )
    unknown_events = _count(
        f"SELECT count(*) FROM agent.outbox_events WHERE event_type NOT IN ({_KNOWN_SQL})"
    )
    ambiguous_notifications = _count(
        f"""
        SELECT count(*)
        FROM agent.outbox_events
        WHERE event_type IN ({_NOTIFICATION_SQL})
          AND (
              status IN ('processing', 'failed')
              OR (
                  status = 'pending'
                  AND (
                      attempt_count <> 0
                      OR lease_token_hash IS NOT NULL
                      OR lease_expires_at IS NOT NULL
                      OR last_error_code IS NOT NULL
                      OR published_at IS NOT NULL
                  )
              )
          )
        """
    )
    if marker_collisions:
        raise RuntimeError("0027 outbox lifecycle migration marker already exists")
    if unknown_events:
        raise RuntimeError(
            "0027 found unclassified outbox event types; classify them explicitly before upgrade"
        )
    if ambiguous_notifications:
        raise RuntimeError(
            "0027 found notification rows with processing, failure, retry, lease, or publication "
            "evidence; reconcile them before upgrade"
        )


def _backfill_notifications() -> None:
    """@brief 仅将未经 dispatcher 的 pending notification 精确转为 published / Precisely publish only untouched pending notifications."""

    op.execute(
        f"""
        UPDATE agent.outbox_events AS event
        SET status = 'published',
            published_at = event.occurred_at,
            updated_at = statement_timestamp(),
            extensions = event.extensions || jsonb_build_object(
                '{_MIGRATION_MARKER}',
                jsonb_build_object(
                    'reason', '{_MIGRATION_REASON}',
                    'previous_status', 'pending',
                    'previous_updated_at', to_jsonb(event.updated_at)
                )
            )
        WHERE event.event_type IN ({_NOTIFICATION_SQL})
          AND event.status = 'pending'
        """
    )


def _add_delivery_constraint_and_index() -> None:
    """@brief 在数据回填后强制闭合生命周期并建立清理索引 / Enforce the closed lifecycle and add a retention index after backfill."""

    op.create_check_constraint(
        "outbox_events_delivery_class",
        "outbox_events",
        f"(event_type IN ({_WORK_SQL}) "
        "AND status IN ('pending', 'processing', 'published', 'failed')) OR "
        f"(event_type IN ({_NOTIFICATION_SQL}) "
        "AND status = 'published' AND published_at IS NOT NULL)",
        schema="agent",
    )
    op.create_index(
        "ix_outbox_events_terminal_replay_expiry",
        "outbox_events",
        ["replay_expires_at", "id"],
        schema="agent",
        postgresql_where=sa.text("status IN ('published', 'failed')"),
    )


def _install_retention_policies(owner_role: str) -> None:
    """@brief 只向 owner-owned 窄函数暴露已过期终态行 / Expose only expired terminal rows to the owner-owned narrow function.

    @param owner_role 已安全引用的 schema owner / Safely quoted schema owner.
    """

    predicate = (
        "status IN ('published', 'failed') "
        "AND replay_expires_at <= statement_timestamp()"
    )
    op.execute(
        f"CREATE POLICY {_RETENTION_SELECT_POLICY} ON agent.outbox_events "
        f"AS PERMISSIVE FOR SELECT TO {owner_role} USING ({predicate})"
    )
    op.execute(
        f"CREATE POLICY {_RETENTION_DELETE_POLICY} ON agent.outbox_events "
        f"AS PERMISSIVE FOR DELETE TO {owner_role} USING ({predicate})"
    )


def _create_retention_function() -> None:
    """@brief 创建有界且仅删终态的 retention 函数 / Create a bounded, terminal-only retention function.

    @note 不使用 ``FOR UPDATE``，因为 FORCE RLS 下行锁需要扩大 owner UPDATE policy；
        并发清理最多会在相同候选行上短暂等待并返回较少删除数，不会越过终态
        predicate 或批量上限。/ ``FOR UPDATE`` is intentionally absent because row locks under
        FORCE RLS would require broadening the owner's UPDATE policy. Concurrent purges may briefly
        wait on identical candidates and return fewer deletions, but cannot cross the terminal
        predicate or batch bound.
    """

    op.execute(
        """
        CREATE FUNCTION agent.purge_expired_outbox_events(
            candidate_now timestamp with time zone,
            candidate_batch_size integer
        ) RETURNS integer
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, agent
        SET row_security = on
        AS $function$
        DECLARE
            effective_now timestamp with time zone;
            deleted_count integer;
        BEGIN
            IF candidate_now IS NULL
               OR candidate_batch_size NOT BETWEEN 1 AND 1000 THEN
                RAISE EXCEPTION 'invalid outbox retention arguments' USING ERRCODE = '22023';
            END IF;
            effective_now := LEAST(candidate_now, statement_timestamp());
            WITH candidates AS (
                SELECT event.id
                FROM agent.outbox_events AS event
                WHERE event.status IN ('published', 'failed')
                  AND event.replay_expires_at <= effective_now
                ORDER BY event.replay_expires_at, event.id
                LIMIT candidate_batch_size
            ), deleted AS (
                DELETE FROM agent.outbox_events AS event
                USING candidates
                WHERE event.id = candidates.id
                  AND event.status IN ('published', 'failed')
                  AND event.replay_expires_at <= effective_now
                RETURNING 1
            )
            SELECT count(*)::integer INTO deleted_count FROM deleted;
            RETURN deleted_count;
        END
        $function$
        """
    )


def _replace_claim_function(*, recover_exhausted_processing: bool) -> None:
    """@brief 切换耗尽租约的 crash-recovery 语义 / Switch crash-recovery semantics for exhausted leases.

    @param recover_exhausted_processing 是否允许重领已达尝试上限的过期
        processing 行 / Whether expired processing rows at the attempt cap may be reclaimed.
    @note pending 行始终要求 ``attempt_count < maximum``。耗尽行重领时不再递增
        attempt，避免超过 100 的数据库上限；它只用于重放幂等领域补偿并将
        outbox CAS 为 failed。/ Pending rows always require attempts below the maximum. An
        exhausted reclaim does not increment again, preventing overflow beyond 100; it exists only
        to replay idempotent domain compensation and CAS the outbox row to failed.
    """

    processing_predicate = (
        "event.status = 'processing' AND event.lease_expires_at <= effective_now"
        if recover_exhausted_processing
        else "event.status = 'processing' AND event.lease_expires_at <= effective_now "
        "AND event.attempt_count < candidate_maximum_attempts"
    )
    attempt_expression = (
        "CASE WHEN event.status = 'processing' "
        "AND event.attempt_count >= candidate_maximum_attempts "
        "THEN event.attempt_count ELSE event.attempt_count + 1 END"
        if recover_exhausted_processing
        else "event.attempt_count + 1"
    )
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION agent.claim_outbox_events(
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
               OR candidate_lease_token_hash !~ '^[a-f0-9]{{64}}$'
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
                           OR candidate_event_type !~ '^[a-z][a-z0-9_.-]{{2,127}}$'
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
                WHERE (
                    candidate_event_types IS NULL
                    OR event.event_type = ANY(candidate_event_types)
                )
                  AND (
                    (
                        event.status = 'pending'
                        AND event.attempt_count < candidate_maximum_attempts
                        AND event.next_attempt_at <= effective_now
                    )
                    OR ({processing_predicate})
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
                    attempt_count = {attempt_expression},
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


def _secure_retention_function(
    *,
    owner_role: str,
    app_role: str,
    dashboard_role: str,
    migrator_role: str,
) -> None:
    """@brief 转移函数所有权并只授予 app EXECUTE / Transfer function ownership and grant only app execution.

    @param owner_role 已引用 owner role / Quoted owner role.
    @param app_role 已引用 runtime role / Quoted runtime role.
    @param dashboard_role 已引用 dashboard role / Quoted dashboard role.
    @param migrator_role 已引用 migrator role / Quoted migrator role.
    """

    op.execute(f"ALTER FUNCTION {_FUNCTION_SIGNATURE} OWNER TO {owner_role}")
    op.execute(
        f"REVOKE ALL PRIVILEGES ON FUNCTION {_FUNCTION_SIGNATURE} "
        f"FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}"
    )
    op.execute(f"GRANT EXECUTE ON FUNCTION {_FUNCTION_SIGNATURE} TO {app_role}")


def _preflight_downgrade_markers() -> None:
    """@brief 证明所有 marker 行仍是本迁移写入的精确状态 / Prove every marker row still has the exact state written by this migration.

    @raise RuntimeError marker 或行状态被改写时拒绝降级 / Raised when a marker or row
        has been changed.
    """

    invalid = _count(
        f"""
        SELECT count(*)
        FROM agent.outbox_events AS event
        WHERE event.extensions ? '{_MIGRATION_MARKER}'
          AND (
              jsonb_typeof(event.extensions -> '{_MIGRATION_MARKER}')
                  IS DISTINCT FROM 'object'
              OR jsonb_array_length(
                  jsonb_path_query_array(
                      CASE
                          WHEN jsonb_typeof(event.extensions -> '{_MIGRATION_MARKER}') = 'object'
                          THEN event.extensions -> '{_MIGRATION_MARKER}'
                          ELSE '{{}}'::jsonb
                      END,
                      '$.keyvalue()'
                  )
              ) <> 3
              OR event.extensions -> '{_MIGRATION_MARKER}' ->> 'reason'
                  IS DISTINCT FROM '{_MIGRATION_REASON}'
              OR event.extensions -> '{_MIGRATION_MARKER}' ->> 'previous_status'
                  IS DISTINCT FROM 'pending'
              OR jsonb_typeof(
                  event.extensions -> '{_MIGRATION_MARKER}' -> 'previous_updated_at'
              ) IS DISTINCT FROM 'string'
              OR event.event_type NOT IN ({_NOTIFICATION_SQL})
              OR event.status <> 'published'
              OR event.published_at IS DISTINCT FROM event.occurred_at
              OR event.attempt_count <> 0
              OR event.lease_token_hash IS NOT NULL
              OR event.lease_expires_at IS NOT NULL
              OR event.last_error_code IS NOT NULL
          )
        """
    )
    if invalid:
        raise RuntimeError(
            "cannot downgrade 0027 because a notification backfill marker or lifecycle changed"
        )


def _restore_backfilled_notifications() -> None:
    """@brief 只恢复 marker 证明由 0027 修改的行 / Restore only rows proven by markers to have been changed by 0027."""

    op.execute(
        f"""
        UPDATE agent.outbox_events AS event
        SET status = 'pending',
            published_at = NULL,
            updated_at = (
                event.extensions -> '{_MIGRATION_MARKER}' ->> 'previous_updated_at'
            )::timestamp with time zone,
            extensions = event.extensions - '{_MIGRATION_MARKER}'
        WHERE event.extensions ? '{_MIGRATION_MARKER}'
        """
    )


def upgrade() -> None:
    """@brief 发布闭合 outbox 生命周期与安全 retention / Publish closed outbox lifecycles and safe retention."""

    owner_role = _configured_role("owner_role")
    app_role = _configured_role("app_role")
    dashboard_role = _configured_role("dashboard_role")
    migrator_role = _configured_role("migrator_role")
    _install_migration_policy(owner_role)
    _lock_outbox()
    _preflight_upgrade()
    _backfill_notifications()
    _add_delivery_constraint_and_index()
    _replace_claim_function(recover_exhausted_processing=True)
    _install_retention_policies(owner_role)
    _create_retention_function()
    _secure_retention_function(
        owner_role=owner_role,
        app_role=app_role,
        dashboard_role=dashboard_role,
        migrator_role=migrator_role,
    )
    _remove_migration_policy()


def downgrade() -> None:
    """@brief 精确恢复由 0027 回填且未改写的通知 / Precisely restore unchanged notifications backfilled by 0027."""

    owner_role = _configured_role("owner_role")
    _configured_role("app_role")
    _configured_role("dashboard_role")
    _configured_role("migrator_role")
    _install_migration_policy(owner_role)
    _lock_outbox()
    _preflight_downgrade_markers()
    op.drop_constraint(
        "outbox_events_delivery_class",
        "outbox_events",
        schema="agent",
        type_="check",
    )
    _restore_backfilled_notifications()
    _replace_claim_function(recover_exhausted_processing=False)
    op.execute(f"DROP FUNCTION {_FUNCTION_SIGNATURE}")
    op.execute(f"DROP POLICY {_RETENTION_DELETE_POLICY} ON agent.outbox_events")
    op.execute(f"DROP POLICY {_RETENTION_SELECT_POLICY} ON agent.outbox_events")
    op.drop_index(
        "ix_outbox_events_terminal_replay_expiry",
        table_name="outbox_events",
        schema="agent",
    )
    _remove_migration_policy()
