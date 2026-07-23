"""@brief 原地演进 API V2 Agent 持久化与 worker 身份边界 / Evolve API V2 Agent persistence and worker identity in place.

Revision ID: 20260723_0021
Revises: 20260723_0020
Create Date: 2026-07-23

迁移按 preflight→expand→backfill→constrain→secure 执行。旧数据只在已持久完整
``extensions.agent_v2`` 冻结 spec/grant/binding，且能与统一 Job 及 append-only Message
无损对齐时转换；无法证明的行在 DDL 之前失败，不伪造授权或模型路由。
"""

from __future__ import annotations

import re
from typing import Literal

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260723_0021"
"""@brief 当前 Alembic revision / Current Alembic revision."""

down_revision = "20260723_0020"
"""@brief 线性前驱 revision / Linear predecessor revision."""

branch_labels = None
"""@brief 此迁移不创建分支 / This migration creates no branch."""

depends_on = None
"""@brief 此迁移没有额外依赖 / This migration has no extra dependency."""

RuntimeRoleOption = Literal[
    "owner_role",
    "app_role",
    "dashboard_role",
    "migrator_role",
]
"""@brief 本 revision 使用的 dbctl role 选项 / dbctl role options used by this revision."""

_ROLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
"""@brief PostgreSQL role 标识白名单 / PostgreSQL-role identifier allowlist."""

_MIGRATION_POLICY = "agent_owner_migration_0021"
"""@brief FORCE-RLS 表的临时 owner policy / Temporary owner policy for FORCE-RLS tables."""

_MIGRATION_AUDIT_ID = "api-v2-agent-persistence-0021"
"""@brief 追加式迁移证据标识 / Append-only migration-evidence identifier."""

_RLS_TABLES = (
    "agent.jobs",
    "agent.outbox_events",
    "agent.conversations",
    "agent.messages",
    "agent.runs",
    "agent.run_events",
    "agent.tool_approvals",
)
"""@brief 迁移需要精确可见性的旧 RLS 表 / Legacy RLS tables requiring exact migration visibility."""

_AGENT_TABLES = (
    "agent.conversations",
    "agent.messages",
    "agent.runs",
    "agent.tool_approvals",
)
"""@brief V2 Agent 业务表 / V2 Agent business tables."""


def _configured_role(option: RuntimeRoleOption) -> str:
    """@brief 返回经白名单校验并引用的 role / Return an allowlisted and quoted role.

    @param option dbctl ``aiws.*`` role 配置键 / dbctl ``aiws.*`` role option.
    @return 可安全嵌入固定 DDL 的引用标识 / Quoted identifier safe for static DDL.
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


def _install_migration_visibility(owner_role: str) -> None:
    """@brief 为精确表集安装临时 owner visibility / Install temporary owner visibility for the exact table set."""
    for table in _RLS_TABLES:
        op.execute(
            f"CREATE POLICY {_MIGRATION_POLICY} ON {table} AS PERMISSIVE FOR ALL "
            f"TO {owner_role} USING (true) WITH CHECK (true)"
        )


def _remove_migration_visibility() -> None:
    """@brief 移除尚存表上的临时 owner visibility / Remove temporary owner visibility from remaining tables."""
    for table in reversed(_RLS_TABLES):
        if table != "agent.run_events":
            op.execute(f"DROP POLICY {_MIGRATION_POLICY} ON {table}")


def _count(statement: str) -> int:
    """@brief 执行仅来自本 revision 常量的 count SQL / Execute count SQL supplied only by this revision."""
    return int(op.get_bind().scalar(sa.text(statement)) or 0)


def _reject(statement: str, message: str) -> None:
    """@brief 在出现任一不可表示行时失败 / Fail when any unrepresentable row exists."""
    if _count(statement):
        raise RuntimeError(message)


def _preflight() -> dict[str, int]:
    """@brief 在任何破坏性 DDL 前分类验证旧 Agent 状态 / Classify legacy Agent state before destructive DDL."""
    counts = {
        "conversations": _count("SELECT count(*) FROM agent.conversations"),
        "messages": _count("SELECT count(*) FROM agent.messages"),
        "runs": _count("SELECT count(*) FROM agent.runs"),
        "approvals": _count("SELECT count(*) FROM agent.tool_approvals"),
    }
    _reject(
        r"""
        SELECT count(*) FROM agent.conversations
        WHERE id !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR capability IS NULL
           OR capability NOT IN ('general','resume_edit','knowledge_query','interview_coach')
           OR (title IS NOT NULL AND length(title) > 300)
           OR revision < 1 OR updated_at < created_at
           OR jsonb_typeof(extensions) <> 'object'
           OR (deleted_at IS NOT NULL AND deleted_at NOT BETWEEN created_at AND updated_at)
           OR (archived_at IS NOT NULL AND archived_at NOT BETWEEN created_at AND updated_at)
        """,
        "legacy Agent conversations contain an invalid identity, capability, title, timeline, or extension",
    )
    _reject(
        r"""
        SELECT count(*)
        FROM agent.messages AS message
        WHERE message.id !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR message.sequence < 1
           OR message.role NOT IN ('system','user','assistant')
           OR message.revision <> 1 OR message.updated_at <> message.created_at
           OR jsonb_typeof(message.extensions) <> 'object'
           OR jsonb_typeof(message.content_parts) <> 'array'
           OR jsonb_array_length(message.content_parts) NOT BETWEEN 1 AND 100
           OR message.model_metadata <> '{}'::jsonb
           OR (message.final_at IS NOT NULL AND message.final_at <> message.created_at)
           OR EXISTS (
                SELECT 1 FROM jsonb_array_elements(message.content_parts) AS part
                WHERE jsonb_typeof(part) <> 'object'
                   OR part ->> 'type' NOT IN ('text','citation','resume_proposal')
                   OR (message.role IN ('system','user') AND part ->> 'type' <> 'text')
           )
           OR (message.role = 'assistant' AND (
                message.extensions #>> '{agent_v2,source_run_id}' IS NULL
                OR message.extensions #>> '{agent_v2,source_run_id}'
                   !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'))
           OR (message.extensions #>> '{agent_v2,parent_message_id}' IS NOT NULL AND
                message.extensions #>> '{agent_v2,parent_message_id}'
                   !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$')
        """,
        "legacy Agent messages contain a non-append-only, tool/private, malformed, or unmapped row",
    )
    _reject(
        """
        SELECT count(*)
        FROM agent.messages AS child
        LEFT JOIN agent.messages AS parent
          ON parent.id = child.extensions #>> '{agent_v2,parent_message_id}'
         AND parent.workspace_id = child.workspace_id
         AND parent.conversation_id = child.conversation_id
        WHERE child.extensions #>> '{agent_v2,parent_message_id}' IS NOT NULL
          AND (parent.id IS NULL OR parent.sequence >= child.sequence)
        """,
        "legacy Agent message parent references are absent, cross-scope, or non-causal",
    )
    _reject(
        "SELECT count(*) FROM agent.run_events",
        "legacy agent.run_events cannot be proven equivalent to the unified safe outbox",
    )
    _preflight_runs()
    _preflight_approvals()
    return counts


def _preflight_runs() -> None:
    """@brief 验证旧 Run 拥有无损 spec/grant/view 快照和对齐 Job / Validate lossless Run snapshots and aligned Jobs."""
    _reject(
        r"""
        SELECT count(*)
        FROM agent.runs AS run
        LEFT JOIN agent.jobs AS job
          ON job.id = run.job_id AND job.workspace_id = run.workspace_id
        LEFT JOIN agent.messages AS input_message
          ON input_message.id = run.input_message_id
         AND input_message.workspace_id = run.workspace_id
         AND input_message.conversation_id = run.conversation_id
        WHERE run.id !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR run.input_message_id IS NULL OR run.job_id IS NULL
           OR run.capability NOT IN ('general','resume_edit','knowledge_query','interview_coach')
           OR run.status NOT IN ('queued','running','succeeded','failed','cancelled')
           OR run.revision < 1 OR run.updated_at < run.created_at
           OR jsonb_typeof(run.extensions #> '{agent_v2,spec}') <> 'object'
           OR jsonb_typeof(run.extensions #> '{agent_v2,execution_grant}') <> 'object'
           OR jsonb_typeof(run.extensions #> '{agent_v2,spec,context_refs}') <> 'array'
           OR jsonb_array_length(run.extensions #> '{agent_v2,spec,context_refs}') > 100
           OR jsonb_typeof(run.extensions #> '{agent_v2,spec,output_modes}') <> 'array'
           OR jsonb_array_length(run.extensions #> '{agent_v2,spec,output_modes}') NOT BETWEEN 1 AND 3
           OR run.extensions #>> '{agent_v2,spec,conversation_id}' <> run.conversation_id
           OR run.extensions #>> '{agent_v2,spec,input_message_id}' <> run.input_message_id
           OR run.extensions #>> '{agent_v2,spec,capability}' <> run.capability
           OR run.extensions #>> '{agent_v2,spec,response_locale}' <> run.response_locale
           OR run.extensions #> '{agent_v2,spec,inference}' <> run.inference_intent
           OR run.extensions #> '{agent_v2,spec,knowledge}' <> run.effective_knowledge_selection
           OR run.extensions #>> '{agent_v2,execution_grant,session_ref,resource_type}'
              <> 'conversation'
           OR run.extensions #>> '{agent_v2,execution_grant,session_ref,id}' <> run.conversation_id
           OR COALESCE(
                run.extensions #>> '{agent_v2,execution_grant,session_ref,revision}', ''
              ) !~ '^[1-9][0-9]*$'
           OR run.extensions #>> '{agent_v2,execution_grant,model_ref,resource_type}' <> 'model'
           OR run.extensions #>> '{agent_v2,execution_grant,model_ref,id}'
              !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR COALESCE(
                run.extensions #>> '{agent_v2,execution_grant,model_ref,revision}', ''
              ) !~ '^[1-9][0-9]*$'
           OR run.extensions #>> '{agent_v2,execution_grant,model_region}'
              NOT IN ('cn','global','private_deployment')
           OR jsonb_typeof(
                run.extensions #> '{agent_v2,execution_grant,external_model_processing}'
              ) <> 'boolean'
           OR jsonb_typeof(run.extensions #> '{agent_v2,execution_grant,context_refs}') <> 'array'
           OR jsonb_array_length(
                run.extensions #> '{agent_v2,execution_grant,context_refs}'
              ) > 100
           OR jsonb_typeof(
                run.extensions #> '{agent_v2,execution_grant,knowledge_contexts}'
              ) <> 'array'
           OR jsonb_array_length(
                run.extensions #> '{agent_v2,execution_grant,knowledge_contexts}'
              ) > 200
           OR COALESCE(
                run.extensions #>> '{agent_v2,execution_grant,policy_version}', ''
              ) !~ '^[1-9][0-9]*$'
           OR run.extensions #>> '{agent_v2,execution_grant,agent_scope}'
              <> run.extensions #>> '{agent_v2,spec,knowledge,agent_scope}'
           OR run.token_usage <> '{}'::jsonb OR run.cost <> '{}'::jsonb OR run.error IS NOT NULL
           OR run.provider IS NOT NULL
           OR (run.model IS NOT NULL AND
               run.model <> run.extensions #>> '{agent_v2,execution_grant,model_ref,id}')
           OR (run.model_revision IS NOT NULL AND
               run.model_revision <> run.extensions #>> '{agent_v2,execution_grant,model_ref,revision}')
           OR input_message.id IS NULL OR input_message.role <> 'user'
           OR job.id IS NULL OR job.job_type <> 'agent.run'
           OR job.target_resource_type <> 'agent_run' OR job.target_resource_id <> run.id
           OR job.resource_owner_id <> run.resource_owner_id
           OR job.status <> run.status
           OR run.started_at IS DISTINCT FROM job.started_at
           OR run.finished_at IS DISTINCT FROM job.finished_at
           OR jsonb_typeof(COALESCE(run.extensions #> '{agent_v2,proposal_refs}', '[]'::jsonb)) <> 'array'
           OR jsonb_array_length(COALESCE(run.extensions #> '{agent_v2,proposal_refs}', '[]'::jsonb)) > 100
           OR (run.status = 'succeeded' AND
               run.extensions #>> '{agent_v2,output_message_id}' IS NULL)
           OR (run.status <> 'succeeded' AND
               run.extensions #>> '{agent_v2,output_message_id}' IS NOT NULL)
           OR (run.status = 'failed' AND
               jsonb_typeof(run.extensions #> '{agent_v2,problem}') <> 'object')
           OR (run.status <> 'failed' AND
               run.extensions #> '{agent_v2,problem}' IS NOT NULL)
           OR run.extensions #>> '{agent_v2,pending_approval_id}' IS NOT NULL
           OR run.extensions #>> '{agent_v2,active_tool_call_id}' IS NOT NULL
        """,
        "legacy Agent runs lack a lossless frozen spec/grant/view or an aligned unified Job",
    )
    _reject(
        r"""
        SELECT count(*)
        FROM agent.runs AS run
        LEFT JOIN agent.messages AS output_message
          ON output_message.id = run.extensions #>> '{agent_v2,output_message_id}'
         AND output_message.workspace_id = run.workspace_id
         AND output_message.conversation_id = run.conversation_id
        WHERE run.extensions #>> '{agent_v2,output_message_id}' IS NOT NULL
          AND (output_message.id IS NULL OR output_message.role <> 'assistant'
               OR output_message.extensions #>> '{agent_v2,source_run_id}' <> run.id)
        """,
        "legacy Agent output messages are absent, cross-scope, or not bound to their source run",
    )
    _reject(
        r"""
        SELECT count(*)
        FROM agent.messages AS message
        LEFT JOIN agent.runs AS run
          ON run.id = message.extensions #>> '{agent_v2,source_run_id}'
         AND run.workspace_id = message.workspace_id
         AND run.conversation_id = message.conversation_id
        WHERE message.role = 'assistant' AND run.id IS NULL
        """,
        "legacy assistant messages reference an absent or cross-scope source run",
    )


def _preflight_approvals() -> None:
    """@brief 验证旧 approval 已将私有参数移入安全 invocation store / Validate legacy approvals moved private arguments to a secure invocation store."""
    _reject(
        r"""
        SELECT count(*)
        FROM agent.tool_approvals AS approval
        LEFT JOIN agent.runs AS run
          ON run.id = approval.run_id AND run.workspace_id = approval.workspace_id
        WHERE approval.id !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR run.id IS NULL OR approval.resource_owner_id <> run.resource_owner_id
           OR jsonb_typeof(approval.extensions) <> 'object'
           OR approval.tool_name !~ '^[a-z][a-z0-9_.-]{2,100}$'
           OR approval.status NOT IN ('pending','approved','rejected','expired')
           OR approval.revision < 1 OR approval.updated_at < approval.created_at
           OR approval.expires_at IS NULL OR approval.expires_at <= approval.created_at
           OR approval.request_payload <> '{}'::jsonb
           OR approval.decision_payload IS NOT NULL
           OR approval.extensions #>> '{agent_v2,tool_call_id}'
              !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR approval.extensions #>> '{agent_v2,summary}' IS NULL
           OR length(approval.extensions #>> '{agent_v2,summary}') NOT BETWEEN 1 AND 2000
           OR approval.extensions #>> '{agent_v2,risk}' NOT IN ('low','medium','high')
           OR approval.extensions #>> '{agent_v2,invocation_ref,resource_type}' <> 'tool_invocation'
           OR approval.extensions #>> '{agent_v2,invocation_ref,id}'
              !~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'
           OR (approval.extensions #>> '{agent_v2,invocation_ref,revision}' IS NOT NULL AND
               approval.extensions #>> '{agent_v2,invocation_ref,revision}' !~ '^[1-9][0-9]*$')
           OR (approval.status = 'pending' AND (
                approval.extensions #> '{agent_v2,decision_by}' IS NOT NULL
                OR approval.decided_by_actor_id IS NOT NULL OR approval.decided_at IS NOT NULL))
           OR (approval.status <> 'pending' AND (
                jsonb_typeof(approval.extensions #> '{agent_v2,decision_by}') <> 'object'
                OR approval.revision < 2
                OR approval.extensions #>> '{agent_v2,decision_by,resource_type}'
                   !~ '^[a-z][a-z0-9_.-]{2,100}$'
                OR approval.extensions #>> '{agent_v2,decision_by,id}' IS NULL
                OR (approval.extensions #>> '{agent_v2,decision_by,revision}' IS NOT NULL AND
                    approval.extensions #>> '{agent_v2,decision_by,revision}'
                       !~ '^[1-9][0-9]*$')
                OR approval.decided_at IS DISTINCT FROM approval.updated_at
                OR (approval.decided_by_actor_id IS NOT NULL AND
                    approval.decided_by_actor_id <>
                    approval.extensions #>> '{agent_v2,decision_by,id}')))
        """,
        "legacy Tool approvals contain raw arguments or lack a lossless safe binding/decision",
    )
    _reject(
        """
        SELECT count(*) FROM (
            SELECT workspace_id, run_id, extensions #>> '{agent_v2,tool_call_id}' AS tool_call_id
            FROM agent.tool_approvals
            GROUP BY workspace_id, run_id, extensions #>> '{agent_v2,tool_call_id}'
            HAVING count(*) > 1
        ) AS duplicate
        """,
        "legacy Tool approvals contain duplicate run/tool-call bindings",
    )


def _expand_v2_columns() -> None:
    """@brief 在旧列仍可回查时增加 nullable V2 列 / Add nullable V2 columns while legacy values remain inspectable."""
    op.alter_column(
        "runs",
        "status",
        schema="agent",
        existing_type=sa.String(16),
        type_=sa.String(32),
        existing_nullable=False,
    )
    op.add_column(
        "conversations",
        sa.Column("status", sa.String(16), server_default=sa.text("'active'")),
        schema="agent",
    )
    op.add_column(
        "conversations",
        sa.Column("message_sequence", sa.BigInteger(), server_default=sa.text("0")),
        schema="agent",
    )
    op.add_column("messages", sa.Column("parent_message_id", sa.String(160)), schema="agent")
    op.add_column("messages", sa.Column("source_run_id", sa.String(160)), schema="agent")
    for column in (
        sa.Column("spec", postgresql.JSONB(astext_type=sa.Text())),
        sa.Column("execution_grant", postgresql.JSONB(astext_type=sa.Text())),
        sa.Column("output_message_id", sa.String(160)),
        sa.Column(
            "proposal_refs",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column("pending_approval_id", sa.String(160)),
        sa.Column("usage", postgresql.JSONB(astext_type=sa.Text())),
        sa.Column("problem", postgresql.JSONB(astext_type=sa.Text())),
        sa.Column("active_tool_call_id", sa.String(160)),
    ):
        op.add_column("runs", column, schema="agent")
    for column in (
        sa.Column("tool_call_id", sa.String(160)),
        sa.Column("summary", sa.String(2000)),
        sa.Column("risk", sa.String(16)),
        sa.Column("invocation_type", sa.String(101)),
        sa.Column("invocation_id", sa.String(160)),
        sa.Column("invocation_revision", sa.Integer()),
        sa.Column("decision_by_type", sa.String(101)),
        sa.Column("decision_by_id", sa.String(160)),
        sa.Column("decision_by_revision", sa.Integer()),
    ):
        op.add_column("tool_approvals", column, schema="agent")


def _backfill_v2_columns() -> None:
    """@brief 从已验证的冻结 extension 原样回填 typed V2 state / Backfill typed V2 state exactly from verified frozen extensions."""
    op.execute(
        """
        WITH counters AS (
            SELECT conversation.id, conversation.workspace_id,
                   COALESCE(max(message.sequence), 0) AS message_sequence
            FROM agent.conversations AS conversation
            LEFT JOIN agent.messages AS message
              ON message.conversation_id = conversation.id
             AND message.workspace_id = conversation.workspace_id
            GROUP BY conversation.id, conversation.workspace_id
        )
        UPDATE agent.conversations AS conversation
        SET status = CASE
                WHEN conversation.archived_at IS NOT NULL OR conversation.deleted_at IS NOT NULL
                THEN 'archived' ELSE 'active' END,
            message_sequence = counters.message_sequence,
            extensions = CASE WHEN conversation.archived_at IS NULL
                THEN conversation.extensions
                ELSE conversation.extensions || jsonb_build_object(
                    '_migration_0021',
                    COALESCE(conversation.extensions -> '_migration_0021', '{}'::jsonb) ||
                    jsonb_build_object('legacy_archived_at', to_jsonb(conversation.archived_at))
                ) END
        FROM counters
        WHERE counters.id = conversation.id
          AND counters.workspace_id = conversation.workspace_id
        """
    )
    op.execute(
        """
        UPDATE agent.messages
        SET role = CASE role WHEN 'system' THEN 'system_notice' ELSE role END,
            parent_message_id = extensions #>> '{agent_v2,parent_message_id}',
            source_run_id = extensions #>> '{agent_v2,source_run_id}',
            extensions = extensions - 'agent_v2'
        """
    )
    op.execute(
        """
        UPDATE agent.runs
        SET spec = extensions #> '{agent_v2,spec}',
            execution_grant = extensions #> '{agent_v2,execution_grant}',
            output_message_id = extensions #>> '{agent_v2,output_message_id}',
            proposal_refs = COALESCE(extensions #> '{agent_v2,proposal_refs}', '[]'::jsonb),
            pending_approval_id = extensions #>> '{agent_v2,pending_approval_id}',
            usage = extensions #> '{agent_v2,usage}',
            problem = extensions #> '{agent_v2,problem}',
            active_tool_call_id = extensions #>> '{agent_v2,active_tool_call_id}',
            extensions = extensions - 'agent_v2'
        """
    )
    op.execute(
        """
        UPDATE agent.tool_approvals
        SET tool_call_id = extensions #>> '{agent_v2,tool_call_id}',
            summary = extensions #>> '{agent_v2,summary}',
            risk = extensions #>> '{agent_v2,risk}',
            invocation_type = extensions #>> '{agent_v2,invocation_ref,resource_type}',
            invocation_id = extensions #>> '{agent_v2,invocation_ref,id}',
            invocation_revision = CASE
                WHEN extensions #>> '{agent_v2,invocation_ref,revision}' IS NULL THEN NULL
                ELSE (extensions #>> '{agent_v2,invocation_ref,revision}')::integer END,
            decision_by_type = extensions #>> '{agent_v2,decision_by,resource_type}',
            decision_by_id = extensions #>> '{agent_v2,decision_by,id}',
            decision_by_revision = CASE
                WHEN extensions #>> '{agent_v2,decision_by,revision}' IS NULL THEN NULL
                ELSE (extensions #>> '{agent_v2,decision_by,revision}')::integer END,
            extensions = extensions - 'agent_v2'
        """
    )


def _verify_backfill() -> None:
    """@brief 在删除 legacy 列前验证 typed 投影完整 / Verify typed projections before dropping legacy columns."""
    _reject(
        """
        SELECT count(*) FROM agent.conversations
        WHERE status IS NULL OR message_sequence IS NULL OR message_sequence < 0
        """,
        "0021 conversation backfill produced incomplete state",
    )
    _reject(
        """
        SELECT count(*) FROM agent.runs
        WHERE spec IS NULL OR execution_grant IS NULL OR proposal_refs IS NULL
        """,
        "0021 run backfill produced incomplete snapshots",
    )
    _reject(
        """
        SELECT count(*) FROM agent.tool_approvals
        WHERE tool_call_id IS NULL OR summary IS NULL OR risk IS NULL
           OR invocation_type IS NULL OR invocation_id IS NULL
        """,
        "0021 approval backfill produced incomplete safe bindings",
    )


def _drop_legacy_relations() -> None:
    """@brief 先移除阻挡 ID 扩宽的 legacy owner-coupled 外键 / Remove legacy owner-coupled relations before widening IDs."""
    external_relations = (
        ("resume", "proposals", "fk_tnt_proposals_agent_run_id_scope"),
        ("resume", "proposals", "proposals_agent_run_id_fkey"),
        ("knowledge", "citations", "fk_tnt_citations_run_id_scope"),
        ("knowledge", "citations", "citations_run_id_fkey"),
        ("knowledge", "access_snapshots", "fk_tnt_access_snapshots_agent_run_id_scope"),
        ("knowledge", "access_snapshots", "access_snapshots_agent_run_id_fkey"),
    )
    for schema, table, constraint in external_relations:
        op.drop_constraint(constraint, table, schema=schema, type_="foreignkey")

    agent_relations = (
        ("messages", "fk_tnt_messages_conversation_id_scope"),
        ("messages", "messages_conversation_id_fkey"),
        ("messages", "fk_tnt_messages_workspace_scope"),
        ("runs", "fk_tnt_runs_conversation_id_scope"),
        ("runs", "runs_conversation_id_fkey"),
        ("runs", "fk_tnt_runs_input_message_id_scope"),
        ("runs", "runs_input_message_id_fkey"),
        ("runs", "fk_tnt_runs_job_id_scope"),
        ("runs", "runs_job_id_fkey"),
        ("runs", "fk_tnt_runs_workspace_scope"),
        ("tool_approvals", "fk_tnt_tool_approvals_run_id_scope"),
        ("tool_approvals", "tool_approvals_run_id_fkey"),
        ("tool_approvals", "fk_tnt_tool_approvals_workspace_scope"),
        ("conversations", "fk_tnt_conversations_workspace_scope"),
    )
    for table, constraint in agent_relations:
        op.drop_constraint(constraint, table, schema="agent", type_="foreignkey")
    # run_events has no representable rows after preflight and still depends on runs' owner key.
    op.drop_table("run_events", schema="agent")
    for table in ("conversations", "messages", "runs", "tool_approvals"):
        op.drop_constraint(
            f"uq_tnt_{table}_id_ws_owner",
            table,
            schema="agent",
            type_="unique",
        )


def _widen_agent_ids() -> None:
    """@brief 把 Agent 及其外部引用统一扩到 OpaqueId 160 / Widen Agent and external references to 160-character OpaqueId."""
    for table, column in (
        ("conversations", "id"),
        ("messages", "id"),
        ("messages", "conversation_id"),
        ("runs", "id"),
        ("runs", "conversation_id"),
        ("runs", "input_message_id"),
        ("tool_approvals", "id"),
        ("tool_approvals", "run_id"),
    ):
        op.alter_column(table, column, schema="agent", type_=sa.String(160))
    op.alter_column("proposals", "agent_run_id", schema="resume", type_=sa.String(160))
    op.alter_column("citations", "run_id", schema="knowledge", type_=sa.String(160))
    op.alter_column("access_snapshots", "agent_run_id", schema="knowledge", type_=sa.String(160))
    op.alter_column("conversations", "title", schema="agent", type_=sa.String(300))
    op.alter_column("conversations", "capability", schema="agent", type_=sa.String(32))
    op.alter_column("runs", "capability", schema="agent", type_=sa.String(32))
    op.alter_column("tool_approvals", "tool_name", schema="agent", type_=sa.String(101))


def _drop_legacy_shape() -> None:
    """@brief typed 投影稳定后删除重复或敏感 legacy 列 / Drop duplicate or sensitive legacy columns after projection."""
    op.drop_constraint("messages_conversation_sequence", "messages", schema="agent", type_="unique")
    op.drop_constraint("messages_role", "messages", schema="agent", type_="check")
    op.drop_constraint("agent_runs_status", "runs", schema="agent", type_="check")
    op.drop_constraint("tool_approvals_status", "tool_approvals", schema="agent", type_="check")
    for index, table in (
        ("ix_conversations_workspace_id_updated_at", "conversations"),
        ("ix_messages_conversation_id_sequence", "messages"),
        ("ix_runs_conversation_id_created_at", "runs"),
        ("ix_tool_approvals_run_id_status", "tool_approvals"),
    ):
        op.drop_index(index, table_name=table, schema="agent")
    op.drop_column("conversations", "archived_at", schema="agent")
    op.drop_column("messages", "final_at", schema="agent")
    op.drop_column("messages", "model_metadata", schema="agent")
    for column in (
        "response_locale",
        "inference_intent",
        "effective_knowledge_selection",
        "provider",
        "model",
        "model_revision",
        "token_usage",
        "cost",
        "error",
        "started_at",
        "finished_at",
    ):
        op.drop_column("runs", column, schema="agent")
    for column in (
        "request_payload",
        "decision_payload",
        "decided_by_actor_id",
        "decided_at",
    ):
        op.drop_column("tool_approvals", column, schema="agent")


def _constrain_v2_shape() -> None:
    """@brief 安装 V2 relational invariants 与 scope-local 外键 / Install V2 relational invariants and scope-local foreign keys."""
    for table, columns in (
        ("conversations", ("capability", "status", "message_sequence")),
        ("runs", ("input_message_id", "job_id", "spec", "execution_grant", "proposal_refs")),
        (
            "tool_approvals",
            ("tool_call_id", "summary", "risk", "invocation_type", "invocation_id", "expires_at"),
        ),
    ):
        for column in columns:
            op.alter_column(table, column, schema="agent", nullable=False)

    conversation_checks = {
        "conversations_v2_id": "id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'",
        "conversations_v2_title": "title IS NULL OR length(title) <= 300",
        "conversations_v2_capability": (
            "capability IN ('general', 'resume_edit', 'knowledge_query', 'interview_coach')"
        ),
        "conversations_v2_status": "status IN ('active', 'archived')",
        "conversations_v2_state": (
            "message_sequence >= 0 AND (deleted_at IS NULL OR status = 'archived')"
        ),
    }
    for name, condition in conversation_checks.items():
        op.create_check_constraint(name, "conversations", condition, schema="agent")
    message_checks = {
        "messages_v2_identity": (
            "id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$' AND sequence >= 1"
        ),
        "messages_v2_role": "role IN ('user', 'assistant', 'system_notice')",
        "messages_v2_content": (
            "jsonb_typeof(content_parts) = 'array' "
            "AND jsonb_array_length(content_parts) BETWEEN 1 AND 100"
        ),
        "messages_v2_append_only": "revision = 1 AND updated_at = created_at",
        "messages_v2_source_run": (
            "(role = 'assistant' AND source_run_id IS NOT NULL) OR "
            "(role <> 'assistant' AND source_run_id IS NULL)"
        ),
        "messages_v2_parent": "parent_message_id IS NULL OR parent_message_id <> id",
    }
    for name, condition in message_checks.items():
        op.create_check_constraint(name, "messages", condition, schema="agent")
    run_checks = {
        "agent_runs_v2_id": "id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'",
        "agent_runs_v2_capability": (
            "capability IN ('general', 'resume_edit', 'knowledge_query', 'interview_coach')"
        ),
        "agent_runs_v2_status": (
            "status IN ('queued', 'running', 'waiting_for_approval', "
            "'succeeded', 'failed', 'cancelled')"
        ),
        "agent_runs_v2_snapshots": (
            "jsonb_typeof(spec) = 'object' AND jsonb_typeof(execution_grant) = 'object' "
            "AND jsonb_typeof(proposal_refs) = 'array' "
            "AND jsonb_array_length(proposal_refs) <= 100"
        ),
        "agent_runs_v2_approval_state": (
            "(status = 'waiting_for_approval' AND pending_approval_id IS NOT NULL "
            "AND active_tool_call_id IS NOT NULL AND problem IS NULL) OR "
            "(status <> 'waiting_for_approval' AND pending_approval_id IS NULL "
            "AND active_tool_call_id IS NULL)"
        ),
        "agent_runs_v2_problem": (
            "(status = 'failed' AND problem IS NOT NULL) OR "
            "(status <> 'failed' AND problem IS NULL)"
        ),
        "agent_runs_v2_output": (
            "(status = 'succeeded' AND output_message_id IS NOT NULL) OR "
            "(status <> 'succeeded' AND output_message_id IS NULL)"
        ),
        "agent_runs_v2_terminal_results": (
            "status IN ('succeeded', 'failed', 'cancelled') OR "
            "(jsonb_array_length(proposal_refs) = 0 AND usage IS NULL)"
        ),
    }
    for name, condition in run_checks.items():
        op.create_check_constraint(name, "runs", condition, schema="agent")
    approval_checks = {
        "tool_approvals_v2_identity": (
            "id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$' "
            "AND tool_call_id ~ '^[A-Za-z][A-Za-z0-9_-]{7,159}$'"
        ),
        "tool_approvals_v2_binding": (
            "tool_name ~ '^[a-z][a-z0-9_.-]{2,100}$' "
            "AND length(summary) BETWEEN 1 AND 2000 "
            "AND risk IN ('low', 'medium', 'high')"
        ),
        "tool_approvals_v2_status": (
            "status IN ('pending', 'approved', 'rejected', 'expired')"
        ),
        "tool_approvals_v2_decision": (
            "(status = 'pending' AND decision_by_type IS NULL AND decision_by_id IS NULL "
            "AND decision_by_revision IS NULL) OR "
            "(status <> 'pending' AND decision_by_type IS NOT NULL "
            "AND decision_by_id IS NOT NULL AND revision >= 2)"
        ),
        "tool_approvals_v2_invocation": (
            "invocation_type = 'tool_invocation' AND expires_at > created_at"
        ),
    }
    for name, condition in approval_checks.items():
        op.create_check_constraint(name, "tool_approvals", condition, schema="agent")

    op.create_unique_constraint(
        "conversations_v2_id_workspace",
        "conversations",
        ["id", "workspace_id"],
        schema="agent",
    )
    op.create_unique_constraint(
        "agent_jobs_v2_id_workspace", "jobs", ["id", "workspace_id"], schema="agent"
    )
    op.create_unique_constraint(
        "messages_v2_conversation_sequence",
        "messages",
        ["workspace_id", "conversation_id", "sequence"],
        schema="agent",
    )
    op.create_unique_constraint(
        "messages_v2_id_workspace_conversation",
        "messages",
        ["id", "workspace_id", "conversation_id"],
        schema="agent",
    )
    op.create_unique_constraint(
        "agent_runs_v2_id_workspace", "runs", ["id", "workspace_id"], schema="agent"
    )
    op.create_unique_constraint(
        "tool_approvals_v2_run_call",
        "tool_approvals",
        ["workspace_id", "run_id", "tool_call_id"],
        schema="agent",
    )
    op.create_unique_constraint(
        "tool_approvals_v2_id_workspace",
        "tool_approvals",
        ["id", "workspace_id"],
        schema="agent",
    )

    _create_v2_foreign_keys()
    _create_v2_indexes()


def _create_v2_foreign_keys() -> None:
    """@brief 创建可组合的 Workspace-local FK 图 / Create the composable Workspace-local foreign-key graph."""
    definitions = (
        (
            "fk_messages_v2_conversation_workspace",
            "messages",
            "conversations",
            ["conversation_id", "workspace_id"],
            ["id", "workspace_id"],
            "CASCADE",
            False,
        ),
        (
            "fk_messages_v2_parent_scope",
            "messages",
            "messages",
            ["parent_message_id", "workspace_id", "conversation_id"],
            ["id", "workspace_id", "conversation_id"],
            "RESTRICT",
            True,
        ),
        (
            "fk_agent_runs_v2_conversation_workspace",
            "runs",
            "conversations",
            ["conversation_id", "workspace_id"],
            ["id", "workspace_id"],
            "CASCADE",
            False,
        ),
        (
            "fk_agent_runs_v2_input_message_scope",
            "runs",
            "messages",
            ["input_message_id", "workspace_id", "conversation_id"],
            ["id", "workspace_id", "conversation_id"],
            "RESTRICT",
            True,
        ),
        (
            "fk_agent_runs_v2_output_message_scope",
            "runs",
            "messages",
            ["output_message_id", "workspace_id", "conversation_id"],
            ["id", "workspace_id", "conversation_id"],
            "RESTRICT",
            True,
        ),
        (
            "fk_agent_runs_v2_job_workspace",
            "runs",
            "jobs",
            ["job_id", "workspace_id"],
            ["id", "workspace_id"],
            "RESTRICT",
            True,
        ),
        (
            "fk_tool_approvals_v2_run_workspace",
            "tool_approvals",
            "runs",
            ["run_id", "workspace_id"],
            ["id", "workspace_id"],
            "CASCADE",
            False,
        ),
        (
            "fk_agent_runs_v2_pending_approval_workspace",
            "runs",
            "tool_approvals",
            ["pending_approval_id", "workspace_id"],
            ["id", "workspace_id"],
            "RESTRICT",
            True,
        ),
        (
            "fk_messages_v2_source_run_workspace",
            "messages",
            "runs",
            ["source_run_id", "workspace_id"],
            ["id", "workspace_id"],
            "RESTRICT",
            True,
        ),
    )
    for name, source, target, local, remote, ondelete, deferred in definitions:
        op.create_foreign_key(
            name,
            source,
            target,
            local,
            remote,
            source_schema="agent",
            referent_schema="agent",
            ondelete=ondelete,
            deferrable=deferred or None,
            initially="DEFERRED" if deferred else None,
        )
    for name, schema, table, local, ondelete in (
        (
            "fk_resume_proposals_agent_run_workspace",
            "resume",
            "proposals",
            ["agent_run_id", "workspace_id"],
            "SET NULL (agent_run_id)",
        ),
        (
            "fk_knowledge_citations_run_workspace",
            "knowledge",
            "citations",
            ["run_id", "workspace_id"],
            "CASCADE",
        ),
        (
            "fk_knowledge_access_snapshots_agent_run_workspace",
            "knowledge",
            "access_snapshots",
            ["agent_run_id", "workspace_id"],
            "CASCADE",
        ),
    ):
        op.create_foreign_key(
            name,
            table,
            "runs",
            local,
            ["id", "workspace_id"],
            source_schema=schema,
            referent_schema="agent",
            ondelete=ondelete,
        )


def _create_v2_indexes() -> None:
    """@brief 创建稳定 keyset 与常用状态查询索引 / Create stable keyset and common-state indexes."""
    for name, table, columns in (
        (
            "ix_conversations_workspace_created_id",
            "conversations",
            ["workspace_id", "created_at", "id"],
        ),
        (
            "ix_messages_workspace_conversation_sequence_id",
            "messages",
            ["workspace_id", "conversation_id", "sequence", "id"],
        ),
        (
            "ix_agent_runs_workspace_conversation_created_id",
            "runs",
            ["workspace_id", "conversation_id", "created_at", "id"],
        ),
        (
            "ix_tool_approvals_workspace_run_status",
            "tool_approvals",
            ["workspace_id", "run_id", "status"],
        ),
    ):
        op.create_index(name, table, columns, schema="agent")


def _secure_agent_tables(
    *, app_role: str, dashboard_role: str, migrator_role: str
) -> None:
    """@brief 用 Workspace RLS 与列级更新权限实现最小授权 / Enforce least privilege with Workspace RLS and column updates."""
    for table in _AGENT_TABLES:
        op.execute(f"DROP POLICY workspace_app_tenant_scope ON {table}")
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")
        op.execute(
            f"REVOKE ALL PRIVILEGES ON TABLE {table} "
            f"FROM PUBLIC, {app_role}, {dashboard_role}, {migrator_role}"
        )
        op.execute(f"GRANT SELECT, INSERT ON TABLE {table} TO {app_role}")
        op.execute(
            f"CREATE POLICY agent_v2_workspace_select ON {table} "
            f"AS PERMISSIVE FOR SELECT TO {app_role} "
            "USING (workspace_id = current_setting('app.workspace_id', true) AND "
            "current_setting('app.actor_id', true) IS NOT NULL)"
        )
        op.execute(
            f"CREATE POLICY agent_v2_workspace_insert ON {table} "
            f"AS PERMISSIVE FOR INSERT TO {app_role} WITH CHECK ("
            "workspace_id = current_setting('app.workspace_id', true) AND "
            "resource_owner_id = current_setting('app.actor_id', true))"
        )
    updates = {
        "agent.conversations": (
            "title, status, message_sequence, deleted_at, updated_at, revision"
        ),
        "agent.runs": (
            "status, output_message_id, proposal_refs, pending_approval_id, usage, problem, "
            "active_tool_call_id, updated_at, revision"
        ),
        "agent.tool_approvals": (
            "status, decision_by_type, decision_by_id, decision_by_revision, updated_at, revision"
        ),
    }
    for table, columns in updates.items():
        op.execute(f"GRANT UPDATE ({columns}) ON TABLE {table} TO {app_role}")
        op.execute(
            f"CREATE POLICY agent_v2_workspace_update ON {table} "
            f"AS PERMISSIVE FOR UPDATE TO {app_role} "
            "USING (workspace_id = current_setting('app.workspace_id', true) AND "
            "current_setting('app.actor_id', true) IS NOT NULL) "
            "WITH CHECK (workspace_id = current_setting('app.workspace_id', true) AND "
            "current_setting('app.actor_id', true) IS NOT NULL)"
        )


def _write_migration_audit(counts: dict[str, int]) -> None:
    """@brief 为非空原位转换追加 evidence / Append evidence for a non-empty in-place conversion.

    @param counts 预检时各 legacy 业务表行数 / Legacy business-row counts at preflight.
    """
    if not any(counts.values()):
        return
    details = (
        '{"conversation_rows":'
        f"{counts['conversations']},\"message_rows\":{counts['messages']},"
        f"\"run_rows\":{counts['runs']},\"approval_rows\":{counts['approvals']}}}"
    )
    op.execute(
        sa.text(
            """
            INSERT INTO identity.api_migration_audits (
                id, migration_id, phase, event_type,
                source_api_version, target_api_version, details
            ) VALUES (
                :id, :migration_id, 5, 'completed', 'v1', 'v2', CAST(:details AS jsonb)
            )
            """
        ).bindparams(
            id="migration_0021_agent_persistence",
            migration_id=_MIGRATION_AUDIT_ID,
            details=details,
        )
    )


def upgrade() -> None:
    """@brief 原位发布 API V2 Agent persistence / Publish API V2 Agent persistence in place."""
    owner_role = _configured_role("owner_role")
    app_role = _configured_role("app_role")
    dashboard_role = _configured_role("dashboard_role")
    migrator_role = _configured_role("migrator_role")
    _install_migration_visibility(owner_role)
    counts = _preflight()
    _expand_v2_columns()
    _backfill_v2_columns()
    _verify_backfill()
    _drop_legacy_relations()
    _widen_agent_ids()
    _drop_legacy_shape()
    _constrain_v2_shape()
    _secure_agent_tables(
        app_role=app_role,
        dashboard_role=dashboard_role,
        migrator_role=migrator_role,
    )
    _write_migration_audit(counts)
    _remove_migration_visibility()


def _install_downgrade_visibility(owner_role: str) -> None:
    """@brief 为 downgrade 空态预检开放仍存在的精确表集 / Expose the exact surviving tables for empty-state downgrade preflight."""
    for table in (*_AGENT_TABLES, "agent.jobs", "agent.outbox_events"):
        op.execute(
            f"CREATE POLICY {_MIGRATION_POLICY} ON {table} AS PERMISSIVE FOR ALL "
            f"TO {owner_role} USING (true) WITH CHECK (true)"
        )


def _remove_downgrade_visibility() -> None:
    """@brief 移除 downgrade 临时 owner 可见性 / Remove temporary downgrade owner visibility."""
    for table in reversed((*_AGENT_TABLES, "agent.jobs", "agent.outbox_events")):
        op.execute(f"DROP POLICY {_MIGRATION_POLICY} ON {table}")


def _require_empty_downgrade() -> None:
    """@brief 仅允许无 Agent V2 状态与迁移证据时回退 / Allow rollback only without Agent V2 state or evidence."""
    counts = {
        "conversations": _count("SELECT count(*) FROM agent.conversations"),
        "messages": _count("SELECT count(*) FROM agent.messages"),
        "runs": _count("SELECT count(*) FROM agent.runs"),
        "approvals": _count("SELECT count(*) FROM agent.tool_approvals"),
        "agent jobs": _count("SELECT count(*) FROM agent.jobs WHERE job_type = 'agent.run'"),
        "agent outbox": _count(
            "SELECT count(*) FROM agent.outbox_events "
            "WHERE aggregate_type IN ('conversation','message','agent_run','tool_approval') "
            "OR event_type LIKE 'agent.%'"
        ),
        "migration evidence": _count(
            "SELECT count(*) FROM identity.api_migration_audits "
            f"WHERE migration_id = '{_MIGRATION_AUDIT_ID}'"
        ),
    }
    if any(counts.values()):
        populated = ", ".join(name for name, value in counts.items() if value)
        raise RuntimeError(f"cannot downgrade non-empty API V2 Agent state: {populated}")


def _drop_v2_security(app_role: str) -> None:
    """@brief 移除 V2 policy 并恢复 0020 Workspace 协作策略 / Remove V2 policies and restore the 0020 collaborative policy.

    @param app_role 应用数据库 role / Application database role.
    """
    for table in reversed(_AGENT_TABLES):
        op.execute(f"DROP POLICY IF EXISTS agent_v2_workspace_update ON {table}")
        op.execute(f"DROP POLICY agent_v2_workspace_insert ON {table}")
        op.execute(f"DROP POLICY agent_v2_workspace_select ON {table}")
        op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {table} TO {app_role}")
        op.execute(
            f"CREATE POLICY workspace_app_tenant_scope ON {table} "
            f"AS PERMISSIVE FOR ALL TO {app_role} "
            "USING (workspace_id = current_setting('app.workspace_id', true)) "
            "WITH CHECK (workspace_id = current_setting('app.workspace_id', true))"
        )


def _drop_v2_constraints() -> None:
    """@brief 空库 downgrade 时拆除 V2 FK、索引与约束 / Remove V2 foreign keys, indexes, and constraints on an empty downgrade."""
    for schema, table, constraint in (
        ("resume", "proposals", "fk_resume_proposals_agent_run_workspace"),
        ("knowledge", "citations", "fk_knowledge_citations_run_workspace"),
        (
            "knowledge",
            "access_snapshots",
            "fk_knowledge_access_snapshots_agent_run_workspace",
        ),
    ):
        op.drop_constraint(constraint, table, schema=schema, type_="foreignkey")
    for table, constraint in (
        ("messages", "fk_messages_v2_source_run_workspace"),
        ("runs", "fk_agent_runs_v2_pending_approval_workspace"),
        ("tool_approvals", "fk_tool_approvals_v2_run_workspace"),
        ("runs", "fk_agent_runs_v2_job_workspace"),
        ("runs", "fk_agent_runs_v2_output_message_scope"),
        ("runs", "fk_agent_runs_v2_input_message_scope"),
        ("runs", "fk_agent_runs_v2_conversation_workspace"),
        ("messages", "fk_messages_v2_parent_scope"),
        ("messages", "fk_messages_v2_conversation_workspace"),
    ):
        op.drop_constraint(constraint, table, schema="agent", type_="foreignkey")
    for name, table in (
        ("ix_tool_approvals_workspace_run_status", "tool_approvals"),
        ("ix_agent_runs_workspace_conversation_created_id", "runs"),
        ("ix_messages_workspace_conversation_sequence_id", "messages"),
        ("ix_conversations_workspace_created_id", "conversations"),
    ):
        op.drop_index(name, table_name=table, schema="agent")
    for table, constraints in (
        (
            "tool_approvals",
            (
                "tool_approvals_v2_invocation",
                "tool_approvals_v2_decision",
                "tool_approvals_v2_status",
                "tool_approvals_v2_binding",
                "tool_approvals_v2_identity",
            ),
        ),
        (
            "runs",
            (
                "agent_runs_v2_terminal_results",
                "agent_runs_v2_output",
                "agent_runs_v2_problem",
                "agent_runs_v2_approval_state",
                "agent_runs_v2_snapshots",
                "agent_runs_v2_status",
                "agent_runs_v2_capability",
                "agent_runs_v2_id",
            ),
        ),
        (
            "messages",
            (
                "messages_v2_parent",
                "messages_v2_source_run",
                "messages_v2_append_only",
                "messages_v2_content",
                "messages_v2_role",
                "messages_v2_identity",
            ),
        ),
        (
            "conversations",
            (
                "conversations_v2_state",
                "conversations_v2_status",
                "conversations_v2_capability",
                "conversations_v2_title",
                "conversations_v2_id",
            ),
        ),
    ):
        for constraint in constraints:
            op.drop_constraint(constraint, table, schema="agent", type_="check")
    for table, constraint in (
        ("tool_approvals", "tool_approvals_v2_id_workspace"),
        ("tool_approvals", "tool_approvals_v2_run_call"),
        ("runs", "agent_runs_v2_id_workspace"),
        ("messages", "messages_v2_id_workspace_conversation"),
        ("messages", "messages_v2_conversation_sequence"),
        ("conversations", "conversations_v2_id_workspace"),
        ("jobs", "agent_jobs_v2_id_workspace"),
    ):
        op.drop_constraint(constraint, table, schema="agent", type_="unique")


def _restore_legacy_columns() -> None:
    """@brief 在空表上恢复 0020 legacy 列形状 / Restore the 0020 legacy columns on empty tables."""
    op.add_column(
        "conversations", sa.Column("archived_at", sa.DateTime(timezone=True)), schema="agent"
    )
    op.add_column("messages", sa.Column("final_at", sa.DateTime(timezone=True)), schema="agent")
    op.add_column(
        "messages",
        sa.Column(
            "model_metadata",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        schema="agent",
    )
    for column in (
        sa.Column(
            "response_locale",
            sa.String(32),
            nullable=False,
            server_default=sa.text("'zh-CN'"),
        ),
        sa.Column("inference_intent", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "effective_knowledge_selection",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("provider", sa.String(128)),
        sa.Column("model", sa.String(256)),
        sa.Column("model_revision", sa.String(256)),
        sa.Column(
            "token_usage",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "cost",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("error", postgresql.JSONB(astext_type=sa.Text())),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
    ):
        op.add_column("runs", column, schema="agent")
    for column in (
        sa.Column("request_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("decision_payload", postgresql.JSONB(astext_type=sa.Text())),
        sa.Column("decided_by_actor_id", sa.String(128)),
        sa.Column("decided_at", sa.DateTime(timezone=True)),
    ):
        op.add_column("tool_approvals", column, schema="agent")

    for column_name in ("status", "message_sequence"):
        op.drop_column("conversations", column_name, schema="agent")
    for column_name in ("source_run_id", "parent_message_id"):
        op.drop_column("messages", column_name, schema="agent")
    for column_name in (
        "active_tool_call_id",
        "problem",
        "usage",
        "pending_approval_id",
        "proposal_refs",
        "output_message_id",
        "execution_grant",
        "spec",
    ):
        op.drop_column("runs", column_name, schema="agent")
    for column_name in (
        "decision_by_revision",
        "decision_by_id",
        "decision_by_type",
        "invocation_revision",
        "invocation_id",
        "invocation_type",
        "risk",
        "summary",
        "tool_call_id",
    ):
        op.drop_column("tool_approvals", column_name, schema="agent")

    op.alter_column("conversations", "id", schema="agent", type_=sa.String(128))
    op.alter_column("conversations", "title", schema="agent", type_=sa.String(512))
    op.alter_column(
        "conversations", "capability", schema="agent", type_=sa.String(128), nullable=True
    )
    for table_name, column_name in (
        ("messages", "id"),
        ("messages", "conversation_id"),
        ("runs", "id"),
        ("runs", "conversation_id"),
        ("runs", "input_message_id"),
        ("tool_approvals", "id"),
        ("tool_approvals", "run_id"),
    ):
        op.alter_column(table_name, column_name, schema="agent", type_=sa.String(128))
    op.alter_column("runs", "capability", schema="agent", type_=sa.String(128))
    op.alter_column(
        "runs",
        "status",
        schema="agent",
        existing_type=sa.String(32),
        type_=sa.String(16),
        existing_nullable=False,
    )
    op.alter_column("runs", "input_message_id", schema="agent", nullable=True)
    op.alter_column("runs", "job_id", schema="agent", nullable=True)
    op.alter_column("tool_approvals", "tool_name", schema="agent", type_=sa.String(128))
    op.alter_column("tool_approvals", "expires_at", schema="agent", nullable=True)
    op.alter_column("proposals", "agent_run_id", schema="resume", type_=sa.String(128))
    op.alter_column("citations", "run_id", schema="knowledge", type_=sa.String(128))
    op.alter_column("access_snapshots", "agent_run_id", schema="knowledge", type_=sa.String(128))


def _restore_legacy_relations_and_indexes() -> None:
    """@brief 重建 0020 的 owner-coupled relation graph 与索引 / Recreate the 0020 owner-coupled relation graph and indexes."""
    op.create_unique_constraint(
        "messages_conversation_sequence",
        "messages",
        ["conversation_id", "sequence"],
        schema="agent",
    )
    op.create_check_constraint(
        "messages_role",
        "messages",
        "role IN ('system', 'user', 'assistant', 'tool')",
        schema="agent",
    )
    op.create_check_constraint(
        "agent_runs_status",
        "runs",
        "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled', 'expired')",
        schema="agent",
    )
    op.create_check_constraint(
        "tool_approvals_status",
        "tool_approvals",
        "status IN ('pending', 'approved', 'rejected', 'expired')",
        schema="agent",
    )
    for table in ("conversations", "messages", "runs", "tool_approvals"):
        op.create_unique_constraint(
            f"uq_tnt_{table}_id_ws_owner",
            table,
            ["id", "workspace_id", "resource_owner_id"],
            schema="agent",
        )
        op.create_foreign_key(
            f"fk_tnt_{table}_workspace_scope",
            table,
            "workspaces",
            ["workspace_id", "resource_owner_id"],
            ["id", "resource_owner_id"],
            source_schema="agent",
            referent_schema="identity",
            ondelete="RESTRICT",
        )
    for name, source, target, column, ondelete, nullable in (
        (
            "fk_tnt_messages_conversation_id_scope",
            "messages",
            "conversations",
            "conversation_id",
            "CASCADE",
            False,
        ),
        (
            "fk_tnt_runs_conversation_id_scope",
            "runs",
            "conversations",
            "conversation_id",
            "CASCADE",
            False,
        ),
        (
            "fk_tnt_runs_input_message_id_scope",
            "runs",
            "messages",
            "input_message_id",
            "SET NULL (input_message_id)",
            True,
        ),
        (
            "fk_tnt_runs_job_id_scope",
            "runs",
            "jobs",
            "job_id",
            "SET NULL (job_id)",
            True,
        ),
        (
            "fk_tnt_tool_approvals_run_id_scope",
            "tool_approvals",
            "runs",
            "run_id",
            "CASCADE",
            False,
        ),
    ):
        del nullable
        op.create_foreign_key(
            name,
            source,
            target,
            [column, "workspace_id", "resource_owner_id"],
            ["id", "workspace_id", "resource_owner_id"],
            source_schema="agent",
            referent_schema="agent",
            ondelete=ondelete,
        )
    for name, source, target, column, ondelete in (
        (
            "messages_conversation_id_fkey",
            "messages",
            "conversations",
            "conversation_id",
            "CASCADE",
        ),
        (
            "runs_conversation_id_fkey",
            "runs",
            "conversations",
            "conversation_id",
            "CASCADE",
        ),
        ("runs_input_message_id_fkey", "runs", "messages", "input_message_id", "SET NULL"),
        ("runs_job_id_fkey", "runs", "jobs", "job_id", "SET NULL"),
        ("tool_approvals_run_id_fkey", "tool_approvals", "runs", "run_id", "CASCADE"),
    ):
        op.create_foreign_key(
            name,
            source,
            target,
            [column],
            ["id"],
            source_schema="agent",
            referent_schema="agent",
            ondelete=ondelete,
        )
    _restore_external_legacy_relations()
    for name, table, columns in (
        (
            "ix_conversations_workspace_id_updated_at",
            "conversations",
            ["workspace_id", "updated_at"],
        ),
        ("ix_messages_conversation_id_sequence", "messages", ["conversation_id", "sequence"]),
        ("ix_runs_conversation_id_created_at", "runs", ["conversation_id", "created_at"]),
        ("ix_tool_approvals_run_id_status", "tool_approvals", ["run_id", "status"]),
    ):
        op.create_index(name, table, columns, schema="agent")


def _restore_external_legacy_relations() -> None:
    """@brief 恢复 Resume/Knowledge 到 Run 的 0020 关系 / Restore 0020 Resume/Knowledge references to Run."""
    for schema, table, column, single_name, scoped_name, ondelete in (
        (
            "resume",
            "proposals",
            "agent_run_id",
            "proposals_agent_run_id_fkey",
            "fk_tnt_proposals_agent_run_id_scope",
            "SET NULL",
        ),
        (
            "knowledge",
            "citations",
            "run_id",
            "citations_run_id_fkey",
            "fk_tnt_citations_run_id_scope",
            "CASCADE",
        ),
        (
            "knowledge",
            "access_snapshots",
            "agent_run_id",
            "access_snapshots_agent_run_id_fkey",
            "fk_tnt_access_snapshots_agent_run_id_scope",
            "CASCADE",
        ),
    ):
        op.create_foreign_key(
            single_name,
            table,
            "runs",
            [column],
            ["id"],
            source_schema=schema,
            referent_schema="agent",
            ondelete=ondelete,
        )
        scoped_delete = f"SET NULL ({column})" if ondelete == "SET NULL" else ondelete
        op.create_foreign_key(
            scoped_name,
            table,
            "runs",
            [column, "workspace_id", "resource_owner_id"],
            ["id", "workspace_id", "resource_owner_id"],
            source_schema=schema,
            referent_schema="agent",
            ondelete=scoped_delete,
        )


def _restore_run_events(app_role: str) -> None:
    """@brief 仅为空态 rollback 重建 legacy run_events 表 / Recreate the legacy run_events table only for an empty rollback.

    @param app_role 应用数据库 role / Application database role.
    """
    op.create_table(
        "run_events",
        sa.Column("id", sa.String(128), primary_key=True),
        sa.Column("workspace_id", sa.String(128), nullable=False),
        sa.Column("resource_owner_id", sa.String(128), nullable=False),
        sa.Column("run_id", sa.String(128), nullable=False),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column("event_type", sa.String(128), nullable=False),
        sa.Column(
            "occurred_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("trace_id", sa.String(128)),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("revision", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column(
            "extensions",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.UniqueConstraint("run_id", "sequence", name="agent_run_events_run_sequence"),
        sa.UniqueConstraint(
            "id", "workspace_id", "resource_owner_id", name="uq_tnt_run_events_id_ws_owner"
        ),
        schema="agent",
    )
    op.create_foreign_key(
        "run_events_run_id_fkey",
        "run_events",
        "runs",
        ["run_id"],
        ["id"],
        source_schema="agent",
        referent_schema="agent",
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_tnt_run_events_run_id_scope",
        "run_events",
        "runs",
        ["run_id", "workspace_id", "resource_owner_id"],
        ["id", "workspace_id", "resource_owner_id"],
        source_schema="agent",
        referent_schema="agent",
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_tnt_run_events_workspace_scope",
        "run_events",
        "workspaces",
        ["workspace_id", "resource_owner_id"],
        ["id", "resource_owner_id"],
        source_schema="agent",
        referent_schema="identity",
        ondelete="RESTRICT",
    )
    for name, columns in (
        ("ix_run_events_workspace_id", ["workspace_id"]),
        ("ix_run_events_resource_owner_id", ["resource_owner_id"]),
        ("ix_run_events_run_id_sequence", ["run_id", "sequence"]),
    ):
        op.create_index(name, "run_events", columns, schema="agent")
    op.execute("ALTER TABLE agent.run_events ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE agent.run_events FORCE ROW LEVEL SECURITY")
    op.execute(
        f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE agent.run_events TO {app_role}"
    )
    op.execute(
        f"CREATE POLICY workspace_app_tenant_scope ON agent.run_events "
        f"AS PERMISSIVE FOR ALL TO {app_role} "
        "USING (workspace_id = current_setting('app.workspace_id', true)) "
        "WITH CHECK (workspace_id = current_setting('app.workspace_id', true))"
    )


def downgrade() -> None:
    """@brief 仅在严格空态恢复 0020 Agent schema / Restore the 0020 Agent schema only in a strict empty state."""
    owner_role = _configured_role("owner_role")
    app_role = _configured_role("app_role")
    _install_downgrade_visibility(owner_role)
    _require_empty_downgrade()
    _remove_downgrade_visibility()
    _drop_v2_security(app_role)
    _drop_v2_constraints()
    _restore_legacy_columns()
    _restore_legacy_relations_and_indexes()
    _restore_run_events(app_role)
