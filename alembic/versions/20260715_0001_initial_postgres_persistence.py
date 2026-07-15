"""@brief v0.1 PostgreSQL 持久化初始结构 / v0.1 initial PostgreSQL persistence structure.

Revision ID: 20260715_0001
Revises:
Create Date: 2026-07-15

@note 运行此前必须由 ``workspace-dbctl bootstrap`` 创建 ``workspace_owner``、
``workspace_migrator``、``workspace_app`` 与 ``workspace_dashboard`` 四个角色，
并授予 migrator ``SET ROLE workspace_owner`` 的权限。本 revision 以 owner 身份
创建 DDL，应用运行时账号没有 DDL 权限。
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

import sqlalchemy as sa
from alembic import op
from pgvector.sqlalchemy import Vector
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "20260715_0001"
down_revision = None
branch_labels = None
depends_on = None


SCHEMAS = ("identity", "resume", "agent", "interview", "knowledge", "observability")
"""@brief 本服务拥有的 PostgreSQL schema / PostgreSQL schemas owned by this service."""


TENANT_TABLES = (
    "identity.workspace_members",
    "identity.audit_events",
    "identity.idempotency_records",
    "resume.template_versions",
    "resume.documents",
    "resume.revisions",
    "resume.operation_batches",
    "resume.operations",
    "resume.proposals",
    "resume.proposal_operations",
    "resume.render_artifacts",
    "resume.render_jobs",
    "resume.pdf_source_map_entries",
    "agent.jobs",
    "agent.outbox_events",
    "agent.conversations",
    "agent.messages",
    "agent.runs",
    "agent.run_events",
    "agent.tool_approvals",
    "interview.scenarios",
    "interview.sessions",
    "interview.events",
    "interview.transcript_segments",
    "interview.reports",
    "interview.report_jobs",
    "interview.recording_artifacts",
    "knowledge.sources",
    "knowledge.source_versions",
    "knowledge.visibility_policies",
    "knowledge.visibility_grants",
    "knowledge.chunks",
    "knowledge.embedding_spaces",
    "knowledge.embeddings",
    "knowledge.citations",
    "knowledge.ingestion_jobs",
    "knowledge.access_snapshots",
    "observability.telemetry_records",
)
"""@brief 需要 workspace/resource-owner RLS 的表 / Tables requiring workspace/resource-owner RLS."""

_ROLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
"""@brief dbctl 传入的 PostgreSQL role 标识符白名单 / Allowed dbctl PostgreSQL role identifiers."""


def _id_column() -> sa.Column[str]:
    """@brief 生成不透明字符串主键列 / Create an opaque-string primary-key column.

    @return 未绑定的 SQLAlchemy 主键 Column。
    """
    return sa.Column("id", sa.String(length=128), primary_key=True)


def _lifecycle_columns() -> list[sa.Column[Any]]:
    """@brief 生成资源通用生命周期列 / Create common lifecycle columns.

    @return created_at、updated_at、revision 与 extensions 列。
    """
    return [
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("revision", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column(
            "extensions",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    ]


def _tenant_columns() -> list[sa.Column[Any]]:
    """@brief 生成租户边界列 / Create tenant-boundary columns.

    @return workspace_id 与 resource_owner_id 外键列。
    """
    return [
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
    ]


def _create_indexes(indexes: Iterable[tuple[str, str, tuple[str, ...]]]) -> None:
    """@brief 创建一组普通 B-tree 索引 / Create a group of ordinary B-tree indexes.

    @param indexes ``(schema, table, columns)`` 三元组序列。
    @return 无返回值。
    """
    for schema, table, columns in indexes:
        op.create_index(
            f"ix_{table}_{'_'.join(columns)}",
            table,
            list(columns),
            unique=False,
            schema=schema,
        )


def _set_owner_role() -> None:
    """@brief 验证 Alembic 已切换至 DDL owner / Assert Alembic already uses the DDL owner.

    @return 无返回值。

    @note 角色成员关系由 dbctl bootstrap 建立；`alembic/env.py` 在版本表创建前已
    以 dbctl 校验过的配置执行 ``SET ROLE``。此函数保留作迁移步骤的显式边界，避免
    revision 将默认 role 名硬编码为 ``workspace_owner`` 并破坏受支持的自定义名称。
    """
    return None


def _configured_role(option: str) -> str:
    """@brief 读取并安全引用 dbctl 提供的 PostgreSQL role / Read and quote a dbctl-provided PostgreSQL role.

    @param option ``owner_role``、``app_role`` 或 ``dashboard_role`` 配置键。
    @return 可嵌入固定 migration SQL 的双引号 role identifier。
    @raise RuntimeError dbctl 未提供或提供非法 role 时抛出。

    @note revision 不能把 ``workspace_app`` 等默认名写死；dbctl 已验证的名称仅通过
    Alembic 内存 Config 传入，既不进入 argv，也不从不受信任 SQL 输入取得。
    """
    migration_config = op.get_context().config
    if migration_config is None:
        raise RuntimeError("Alembic migration context has no configuration")
    value = migration_config.get_main_option(f"aiws.{option}")
    if not value or not _ROLE_IDENTIFIER_PATTERN.fullmatch(value):
        raise RuntimeError(f"missing or invalid dbctl role option: {option}")
    return '"' + value.replace('"', '""') + '"'


def _create_identity_tables() -> None:
    """@brief 创建 identity schema 表 / Create identity-schema tables.

    @return 无返回值。
    """
    op.create_table(
        "users",
        _id_column(),
        sa.Column("external_subject", sa.String(length=320), nullable=False),
        sa.Column("display_name", sa.String(length=256)),
        sa.Column("email", sa.String(length=320)),
        sa.Column("locale", sa.String(length=32), nullable=False, server_default=sa.text("'zh-CN'")),
        sa.Column("deleted_at", sa.DateTime(timezone=True)),
        *_lifecycle_columns(),
        sa.UniqueConstraint("external_subject", name="users_external_subject"),
        schema="identity",
    )
    op.create_table(
        "workspaces",
        _id_column(),
        sa.Column(
            "resource_owner_id",
            sa.String(length=128),
            sa.ForeignKey("identity.users.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("name", sa.String(length=256), nullable=False),
        sa.Column(
            "default_locale", sa.String(length=32), nullable=False, server_default=sa.text("'zh-CN'")
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True)),
        *_lifecycle_columns(),
        schema="identity",
    )
    op.create_table(
        "workspace_members",
        _id_column(),
        *_tenant_columns(),
        sa.Column(
            "user_id",
            sa.String(length=128),
            sa.ForeignKey("identity.users.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("role", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default=sa.text("'active'")),
        sa.Column("invited_by_actor_id", sa.String(length=128)),
        sa.Column("joined_at", sa.DateTime(timezone=True)),
        *_lifecycle_columns(),
        sa.UniqueConstraint("workspace_id", "user_id", name="workspace_members_workspace_user"),
        sa.CheckConstraint("role IN ('owner', 'admin', 'editor', 'viewer')", name="workspace_members_role"),
        sa.CheckConstraint("status IN ('active', 'invited', 'disabled')", name="workspace_members_status"),
        schema="identity",
    )
    op.create_table(
        "audit_events",
        _id_column(),
        *_tenant_columns(),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("actor_id", sa.String(length=128)),
        sa.Column("action", sa.String(length=128), nullable=False),
        sa.Column("resource_type", sa.String(length=64), nullable=False),
        sa.Column("resource_id", sa.String(length=128)),
        sa.Column("request_id", sa.String(length=128)),
        sa.Column("outcome", sa.String(length=32), nullable=False),
        sa.Column(
            "details", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")
        ),
        *_lifecycle_columns(),
        schema="identity",
    )
    op.create_table(
        "idempotency_records",
        _id_column(),
        *_tenant_columns(),
        sa.Column("actor_id", sa.String(length=128), nullable=False),
        sa.Column("request_target", sa.String(length=256), nullable=False),
        sa.Column("idempotency_key", sa.String(length=256), nullable=False),
        sa.Column("request_hash", sa.String(length=128), nullable=False),
        sa.Column("response_status", sa.SmallInteger()),
        sa.Column("response_body", postgresql.JSONB(astext_type=sa.Text())),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        *_lifecycle_columns(),
        sa.UniqueConstraint(
            "workspace_id",
            "resource_owner_id",
            "actor_id",
            "request_target",
            "idempotency_key",
            name="idempotency_scope_target_key",
        ),
        schema="identity",
    )
    _create_indexes(
        (
            ("identity", "workspaces", ("resource_owner_id", "updated_at")),
            ("identity", "workspace_members", ("workspace_id",)),
            ("identity", "workspace_members", ("resource_owner_id",)),
            ("identity", "workspace_members", ("user_id",)),
            ("identity", "audit_events", ("workspace_id", "occurred_at")),
            ("identity", "audit_events", ("resource_owner_id",)),
            ("identity", "audit_events", ("actor_id",)),
            ("identity", "idempotency_records", ("workspace_id",)),
            ("identity", "idempotency_records", ("resource_owner_id",)),
            ("identity", "idempotency_records", ("expires_at",)),
        )
    )


def _create_agent_tables() -> None:
    """@brief 创建 agent schema 表 / Create agent-schema tables.

    @return 无返回值。
    """
    op.create_table(
        "jobs",
        _id_column(),
        *_tenant_columns(),
        sa.Column("job_type", sa.String(length=96), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default=sa.text("'queued'")),
        sa.Column("phase", sa.String(length=64), nullable=False, server_default=sa.text("'queued'")),
        sa.Column("completed_units", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("total_units", sa.Integer()),
        sa.Column("percent", sa.Float()),
        sa.Column("request_id", sa.String(length=128)),
        sa.Column("target_resource_type", sa.String(length=64)),
        sa.Column("target_resource_id", sa.String(length=128)),
        sa.Column("result", postgresql.JSONB(astext_type=sa.Text())),
        sa.Column("error", postgresql.JSONB(astext_type=sa.Text())),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("expires_at", sa.DateTime(timezone=True)),
        *_lifecycle_columns(),
        sa.CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled', 'expired')",
            name="jobs_status",
        ),
        schema="agent",
    )
    op.create_table(
        "outbox_events",
        _id_column(),
        *_tenant_columns(),
        sa.Column("aggregate_type", sa.String(length=64), nullable=False),
        sa.Column("aggregate_id", sa.String(length=128), nullable=False),
        sa.Column("event_type", sa.String(length=128), nullable=False),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("trace_id", sa.String(length=128)),
        sa.Column("status", sa.String(length=16), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        *_lifecycle_columns(),
        sa.CheckConstraint("status IN ('pending', 'processing', 'published', 'failed')", name="outbox_status"),
        schema="agent",
    )
    op.create_table(
        "conversations",
        _id_column(),
        *_tenant_columns(),
        sa.Column("title", sa.String(length=512)),
        sa.Column("capability", sa.String(length=128)),
        sa.Column("archived_at", sa.DateTime(timezone=True)),
        sa.Column("deleted_at", sa.DateTime(timezone=True)),
        *_lifecycle_columns(),
        schema="agent",
    )
    op.create_table(
        "messages",
        _id_column(),
        *_tenant_columns(),
        sa.Column(
            "conversation_id",
            sa.String(length=128),
            sa.ForeignKey("agent.conversations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column("role", sa.String(length=16), nullable=False),
        sa.Column("content_parts", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("final_at", sa.DateTime(timezone=True)),
        sa.Column(
            "model_metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")
        ),
        *_lifecycle_columns(),
        sa.UniqueConstraint("conversation_id", "sequence", name="messages_conversation_sequence"),
        sa.CheckConstraint("role IN ('system', 'user', 'assistant', 'tool')", name="messages_role"),
        schema="agent",
    )
    op.create_table(
        "runs",
        _id_column(),
        *_tenant_columns(),
        sa.Column(
            "conversation_id",
            sa.String(length=128),
            sa.ForeignKey("agent.conversations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "input_message_id", sa.String(length=128), sa.ForeignKey("agent.messages.id", ondelete="SET NULL")
        ),
        sa.Column("job_id", sa.String(length=128), sa.ForeignKey("agent.jobs.id", ondelete="SET NULL"), unique=True),
        sa.Column("capability", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default=sa.text("'queued'")),
        sa.Column("response_locale", sa.String(length=32), nullable=False, server_default=sa.text("'zh-CN'")),
        sa.Column("inference_intent", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("effective_knowledge_selection", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("provider", sa.String(length=128)),
        sa.Column("model", sa.String(length=256)),
        sa.Column("model_revision", sa.String(length=256)),
        sa.Column(
            "token_usage", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")
        ),
        sa.Column("cost", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("error", postgresql.JSONB(astext_type=sa.Text())),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        *_lifecycle_columns(),
        sa.CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled', 'expired')",
            name="agent_runs_status",
        ),
        schema="agent",
    )
    op.create_table(
        "run_events",
        _id_column(),
        *_tenant_columns(),
        sa.Column("run_id", sa.String(length=128), sa.ForeignKey("agent.runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column("event_type", sa.String(length=128), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("trace_id", sa.String(length=128)),
        *_lifecycle_columns(),
        sa.UniqueConstraint("run_id", "sequence", name="agent_run_events_run_sequence"),
        schema="agent",
    )
    op.create_table(
        "tool_approvals",
        _id_column(),
        *_tenant_columns(),
        sa.Column("run_id", sa.String(length=128), sa.ForeignKey("agent.runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("tool_name", sa.String(length=128), nullable=False),
        sa.Column("request_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("decision_payload", postgresql.JSONB(astext_type=sa.Text())),
        sa.Column("decided_by_actor_id", sa.String(length=128)),
        sa.Column("decided_at", sa.DateTime(timezone=True)),
        sa.Column("expires_at", sa.DateTime(timezone=True)),
        *_lifecycle_columns(),
        sa.CheckConstraint("status IN ('pending', 'approved', 'rejected', 'expired')", name="tool_approvals_status"),
        schema="agent",
    )
    _create_indexes(
        (
            ("agent", "jobs", ("workspace_id",)),
            ("agent", "jobs", ("resource_owner_id",)),
            ("agent", "jobs", ("workspace_id", "status", "created_at")),
            ("agent", "outbox_events", ("workspace_id",)),
            ("agent", "outbox_events", ("resource_owner_id",)),
            ("agent", "outbox_events", ("status", "occurred_at")),
            ("agent", "conversations", ("workspace_id",)),
            ("agent", "conversations", ("resource_owner_id",)),
            ("agent", "conversations", ("workspace_id", "updated_at")),
            ("agent", "messages", ("workspace_id",)),
            ("agent", "messages", ("resource_owner_id",)),
            ("agent", "messages", ("conversation_id", "sequence")),
            ("agent", "runs", ("workspace_id",)),
            ("agent", "runs", ("resource_owner_id",)),
            ("agent", "runs", ("conversation_id", "created_at")),
            ("agent", "run_events", ("workspace_id",)),
            ("agent", "run_events", ("resource_owner_id",)),
            ("agent", "run_events", ("run_id", "sequence")),
            ("agent", "tool_approvals", ("workspace_id",)),
            ("agent", "tool_approvals", ("resource_owner_id",)),
            ("agent", "tool_approvals", ("run_id", "status")),
        )
    )


def _create_resume_tables() -> None:
    """@brief 创建 resume schema 表 / Create resume-schema tables.

    @return 无返回值。
    """
    op.create_table(
        "template_versions",
        _id_column(),
        *_tenant_columns(),
        sa.Column("template_id", sa.String(length=128), nullable=False),
        sa.Column("template_version", sa.String(length=128), nullable=False),
        sa.Column("manifest", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("renderer_binding", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.Column("retired_at", sa.DateTime(timezone=True)),
        *_lifecycle_columns(),
        sa.UniqueConstraint(
            "workspace_id", "resource_owner_id", "template_id", "template_version", name="templates_scope_version"
        ),
        schema="resume",
    )
    op.create_table(
        "documents",
        _id_column(),
        *_tenant_columns(),
        sa.Column(
            "template_version_id",
            sa.String(length=128),
            sa.ForeignKey("resume.template_versions.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("title", sa.String(length=512), nullable=False),
        sa.Column("locale", sa.String(length=32), nullable=False, server_default=sa.text("'zh-CN'")),
        sa.Column("current_revision_no", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column("deleted_at", sa.DateTime(timezone=True)),
        *_lifecycle_columns(),
        schema="resume",
    )
    op.create_table(
        "revisions",
        _id_column(),
        *_tenant_columns(),
        sa.Column(
            "resume_id",
            sa.String(length=128),
            sa.ForeignKey("resume.documents.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("revision_no", sa.Integer(), nullable=False),
        sa.Column("semantic_document", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("content_hash", sa.String(length=128), nullable=False),
        sa.Column("created_by_actor_id", sa.String(length=128)),
        sa.Column("source", sa.String(length=32), nullable=False, server_default=sa.text("'user'")),
        *_lifecycle_columns(),
        sa.UniqueConstraint("resume_id", "revision_no", name="resume_revisions_document_revision"),
        schema="resume",
    )
    op.create_table(
        "operation_batches",
        _id_column(),
        *_tenant_columns(),
        sa.Column(
            "resume_id",
            sa.String(length=128),
            sa.ForeignKey("resume.documents.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("client_batch_id", sa.String(length=128), nullable=False),
        sa.Column("base_revision_no", sa.Integer(), nullable=False),
        sa.Column("applied_revision_no", sa.Integer()),
        sa.Column("conflict_strategy", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default=sa.text("'received'")),
        sa.Column(
            "idempotency_record_id",
            sa.String(length=128),
            sa.ForeignKey("identity.idempotency_records.id", ondelete="SET NULL"),
        ),
        *_lifecycle_columns(),
        sa.UniqueConstraint(
            "workspace_id", "resource_owner_id", "resume_id", "client_batch_id", name="resume_batches_client_id"
        ),
        sa.CheckConstraint("status IN ('received', 'applied', 'conflicted', 'rejected')", name="resume_batches_status"),
        schema="resume",
    )
    op.create_table(
        "operations",
        _id_column(),
        *_tenant_columns(),
        sa.Column(
            "batch_id",
            sa.String(length=128),
            sa.ForeignKey("resume.operation_batches.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("operation_id", sa.String(length=128), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("operation_type", sa.String(length=64), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        *_lifecycle_columns(),
        sa.UniqueConstraint("batch_id", "ordinal", name="resume_operations_batch_ordinal"),
        sa.UniqueConstraint("operation_id", name="resume_operations_operation_id"),
        schema="resume",
    )
    op.create_table(
        "proposals",
        _id_column(),
        *_tenant_columns(),
        sa.Column(
            "resume_id",
            sa.String(length=128),
            sa.ForeignKey("resume.documents.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("agent_run_id", sa.String(length=128), sa.ForeignKey("agent.runs.id", ondelete="SET NULL")),
        sa.Column("base_revision_no", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=24), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("decision_payload", postgresql.JSONB(astext_type=sa.Text())),
        sa.Column("decided_by_actor_id", sa.String(length=128)),
        sa.Column("decided_at", sa.DateTime(timezone=True)),
        sa.Column("expires_at", sa.DateTime(timezone=True)),
        *_lifecycle_columns(),
        sa.CheckConstraint(
            "status IN ('pending', 'accepted', 'partially_accepted', 'rejected', 'expired')",
            name="resume_proposals_status",
        ),
        schema="resume",
    )
    op.create_table(
        "proposal_operations",
        _id_column(),
        *_tenant_columns(),
        sa.Column(
            "proposal_id",
            sa.String(length=128),
            sa.ForeignKey("resume.proposals.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("operation_type", sa.String(length=64), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("decision", sa.String(length=16)),
        *_lifecycle_columns(),
        sa.UniqueConstraint("proposal_id", "ordinal", name="resume_proposal_operations_ordinal"),
        schema="resume",
    )
    op.create_table(
        "render_artifacts",
        _id_column(),
        *_tenant_columns(),
        sa.Column(
            "resume_id",
            sa.String(length=128),
            sa.ForeignKey("resume.documents.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "resume_revision_id",
            sa.String(length=128),
            sa.ForeignKey("resume.revisions.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("artifact_kind", sa.String(length=32), nullable=False),
        sa.Column("format", sa.String(length=32), nullable=False),
        sa.Column("storage_key", sa.String(length=1024), nullable=False),
        sa.Column("content_sha256", sa.String(length=64), nullable=False),
        sa.Column("content_bytes", sa.BigInteger(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True)),
        *_lifecycle_columns(),
        sa.UniqueConstraint("storage_key", name="render_artifacts_storage_key"),
        schema="resume",
    )
    op.create_table(
        "render_jobs",
        _id_column(),
        *_tenant_columns(),
        sa.Column(
            "job_id", sa.String(length=128), sa.ForeignKey("agent.jobs.id", ondelete="CASCADE"), nullable=False, unique=True
        ),
        sa.Column(
            "resume_id",
            sa.String(length=128),
            sa.ForeignKey("resume.documents.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "resume_revision_id",
            sa.String(length=128),
            sa.ForeignKey("resume.revisions.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "artifact_id", sa.String(length=128), sa.ForeignKey("resume.render_artifacts.id", ondelete="SET NULL")
        ),
        sa.Column("render_profile", sa.String(length=64), nullable=False),
        sa.Column(
            "diagnostics", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")
        ),
        *_lifecycle_columns(),
        schema="resume",
    )
    op.create_table(
        "pdf_source_map_entries",
        _id_column(),
        *_tenant_columns(),
        sa.Column(
            "artifact_id",
            sa.String(length=128),
            sa.ForeignKey("resume.render_artifacts.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("node_kind", sa.String(length=64), nullable=False),
        sa.Column("node_id", sa.String(length=128), nullable=False),
        sa.Column("field_path", postgresql.ARRAY(sa.String(length=128)), nullable=False),
        sa.Column("page", sa.Integer(), nullable=False),
        sa.Column("rects", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        *_lifecycle_columns(),
        schema="resume",
    )
    _create_indexes(
        (
            ("resume", "template_versions", ("workspace_id",)),
            ("resume", "template_versions", ("resource_owner_id",)),
            ("resume", "documents", ("workspace_id",)),
            ("resume", "documents", ("resource_owner_id",)),
            ("resume", "documents", ("workspace_id", "updated_at")),
            ("resume", "revisions", ("workspace_id",)),
            ("resume", "revisions", ("resource_owner_id",)),
            ("resume", "revisions", ("resume_id", "revision_no")),
            ("resume", "operation_batches", ("workspace_id",)),
            ("resume", "operation_batches", ("resource_owner_id",)),
            ("resume", "operations", ("workspace_id",)),
            ("resume", "operations", ("resource_owner_id",)),
            ("resume", "proposals", ("workspace_id",)),
            ("resume", "proposals", ("resource_owner_id",)),
            ("resume", "proposals", ("resume_id", "status")),
            ("resume", "proposal_operations", ("workspace_id",)),
            ("resume", "proposal_operations", ("resource_owner_id",)),
            ("resume", "render_artifacts", ("workspace_id",)),
            ("resume", "render_artifacts", ("resource_owner_id",)),
            ("resume", "render_artifacts", ("resume_id", "resume_revision_id")),
            ("resume", "render_jobs", ("workspace_id",)),
            ("resume", "render_jobs", ("resource_owner_id",)),
            ("resume", "pdf_source_map_entries", ("workspace_id",)),
            ("resume", "pdf_source_map_entries", ("resource_owner_id",)),
            ("resume", "pdf_source_map_entries", ("artifact_id", "node_id")),
        )
    )


def _create_interview_tables() -> None:
    """@brief 创建 interview schema 表 / Create interview-schema tables.

    @return 无返回值。
    """
    op.create_table(
        "scenarios",
        _id_column(),
        *_tenant_columns(),
        sa.Column("title", sa.String(length=512), nullable=False),
        sa.Column("locale", sa.String(length=32), nullable=False, server_default=sa.text("'zh-CN'")),
        sa.Column("role_target", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("rubric", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("is_template", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("deleted_at", sa.DateTime(timezone=True)),
        *_lifecycle_columns(),
        schema="interview",
    )
    op.create_table(
        "sessions",
        _id_column(),
        *_tenant_columns(),
        sa.Column(
            "scenario_id",
            sa.String(length=128),
            sa.ForeignKey("interview.scenarios.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "resume_revision_id", sa.String(length=128), sa.ForeignKey("resume.revisions.id", ondelete="SET NULL")
        ),
        sa.Column("state", sa.String(length=32), nullable=False, server_default=sa.text("'created'")),
        sa.Column("job_target", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("effective_knowledge_selection", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("inference_intent", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("media_capabilities", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("avatar_output_mode", sa.String(length=32), nullable=False),
        sa.Column("consent", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("recording_retention_until", sa.DateTime(timezone=True)),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("ended_at", sa.DateTime(timezone=True)),
        sa.Column("failure", postgresql.JSONB(astext_type=sa.Text())),
        *_lifecycle_columns(),
        sa.CheckConstraint(
            "state IN ('created', 'preparing', 'ready', 'connecting', 'in_progress', "
            "'ending', 'processing_report', 'completed', 'aborted', 'expired', 'failed')",
            name="interview_sessions_state",
        ),
        schema="interview",
    )
    op.create_table(
        "events",
        _id_column(),
        *_tenant_columns(),
        sa.Column(
            "session_id",
            sa.String(length=128),
            sa.ForeignKey("interview.sessions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column("ack_sequence", sa.BigInteger()),
        sa.Column("event_type", sa.String(length=128), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("trace_id", sa.String(length=128)),
        *_lifecycle_columns(),
        sa.UniqueConstraint("session_id", "sequence", name="interview_events_session_sequence"),
        schema="interview",
    )
    op.create_table(
        "transcript_segments",
        _id_column(),
        *_tenant_columns(),
        sa.Column(
            "session_id",
            sa.String(length=128),
            sa.ForeignKey("interview.sessions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column("speaker", sa.String(length=16), nullable=False),
        sa.Column("start_ms", sa.BigInteger(), nullable=False),
        sa.Column("end_ms", sa.BigInteger()),
        sa.Column("text_content", sa.Text(), nullable=False),
        sa.Column("is_final", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("generated_text", sa.Text()),
        sa.Column("media_scheduled_at", sa.DateTime(timezone=True)),
        sa.Column("media_played_ack_at", sa.DateTime(timezone=True)),
        *_lifecycle_columns(),
        sa.UniqueConstraint("session_id", "sequence", name="transcript_segments_session_sequence"),
        sa.CheckConstraint("speaker IN ('candidate', 'interviewer', 'system')", name="transcript_segments_speaker"),
        schema="interview",
    )
    op.create_table(
        "reports",
        _id_column(),
        *_tenant_columns(),
        sa.Column(
            "session_id",
            sa.String(length=128),
            sa.ForeignKey("interview.sessions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("report_version", sa.Integer(), nullable=False),
        sa.Column("rubric_version", sa.String(length=128), nullable=False),
        sa.Column("engine_version", sa.String(length=256), nullable=False),
        sa.Column("report", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        *_lifecycle_columns(),
        sa.UniqueConstraint("session_id", "report_version", name="interview_reports_session_version"),
        schema="interview",
    )
    op.create_table(
        "report_jobs",
        _id_column(),
        *_tenant_columns(),
        sa.Column(
            "job_id", sa.String(length=128), sa.ForeignKey("agent.jobs.id", ondelete="CASCADE"), nullable=False, unique=True
        ),
        sa.Column(
            "session_id",
            sa.String(length=128),
            sa.ForeignKey("interview.sessions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("report_id", sa.String(length=128), sa.ForeignKey("interview.reports.id", ondelete="SET NULL")),
        *_lifecycle_columns(),
        schema="interview",
    )
    op.create_table(
        "recording_artifacts",
        _id_column(),
        *_tenant_columns(),
        sa.Column(
            "session_id",
            sa.String(length=128),
            sa.ForeignKey("interview.sessions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("storage_key", sa.String(length=1024), nullable=False),
        sa.Column("content_sha256", sa.String(length=64), nullable=False),
        sa.Column("content_bytes", sa.BigInteger(), nullable=False),
        sa.Column("media_kind", sa.String(length=32), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True)),
        sa.Column("deleted_at", sa.DateTime(timezone=True)),
        *_lifecycle_columns(),
        sa.UniqueConstraint("storage_key", name="interview_recordings_storage_key"),
        schema="interview",
    )
    _create_indexes(
        (
            ("interview", "scenarios", ("workspace_id",)),
            ("interview", "scenarios", ("resource_owner_id",)),
            ("interview", "scenarios", ("workspace_id", "updated_at")),
            ("interview", "sessions", ("workspace_id",)),
            ("interview", "sessions", ("resource_owner_id",)),
            ("interview", "sessions", ("workspace_id", "created_at")),
            ("interview", "events", ("workspace_id",)),
            ("interview", "events", ("resource_owner_id",)),
            ("interview", "events", ("session_id", "sequence")),
            ("interview", "transcript_segments", ("workspace_id",)),
            ("interview", "transcript_segments", ("resource_owner_id",)),
            ("interview", "transcript_segments", ("session_id", "start_ms")),
            ("interview", "reports", ("workspace_id",)),
            ("interview", "reports", ("resource_owner_id",)),
            ("interview", "report_jobs", ("workspace_id",)),
            ("interview", "report_jobs", ("resource_owner_id",)),
            ("interview", "recording_artifacts", ("workspace_id",)),
            ("interview", "recording_artifacts", ("resource_owner_id",)),
        )
    )


def _create_knowledge_tables() -> None:
    """@brief 创建 knowledge schema 表及 pgvector 索引 / Create knowledge-schema tables and pgvector index.

    @return 无返回值。

    @note v0.1 使用 ``vector(1024)`` 与 cosine HNSW（Hierarchical Navigable Small
    World）索引；要切换模型、维度或度量，必须新建 embedding space 并迁移数据。
    """
    op.create_table(
        "sources",
        _id_column(),
        *_tenant_columns(),
        sa.Column("source_type", sa.String(length=32), nullable=False),
        sa.Column("title", sa.String(length=512), nullable=False),
        sa.Column("config", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("revision_mode", sa.String(length=16), nullable=False, server_default=sa.text("'latest'")),
        sa.Column("ingestion_state", sa.String(length=16), nullable=False, server_default=sa.text("'new'")),
        sa.Column("sync_schedule", postgresql.JSONB(astext_type=sa.Text())),
        sa.Column("deleted_at", sa.DateTime(timezone=True)),
        *_lifecycle_columns(),
        sa.CheckConstraint(
            "source_type IN ('resume', 'file', 'url', 'git_repository', 'manual_note')",
            name="knowledge_sources_type",
        ),
        sa.CheckConstraint(
            "ingestion_state IN ('new', 'queued', 'indexing', 'ready', 'stale', 'deleted', 'failed')",
            name="knowledge_sources_ingestion_state",
        ),
        schema="knowledge",
    )
    op.create_table(
        "source_versions",
        _id_column(),
        *_tenant_columns(),
        sa.Column(
            "source_id",
            sa.String(length=128),
            sa.ForeignKey("knowledge.sources.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("version_no", sa.Integer(), nullable=False),
        sa.Column("content_hash", sa.String(length=128), nullable=False),
        sa.Column("origin", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "parser_metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")
        ),
        sa.Column("indexed_at", sa.DateTime(timezone=True)),
        *_lifecycle_columns(),
        sa.UniqueConstraint("source_id", "version_no", name="knowledge_source_versions_source_number"),
        schema="knowledge",
    )
    op.create_table(
        "visibility_policies",
        _id_column(),
        *_tenant_columns(),
        sa.Column(
            "source_id",
            sa.String(length=128),
            sa.ForeignKey("knowledge.sources.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("policy_version", sa.Integer(), nullable=False),
        sa.Column("default_effect", sa.String(length=8), nullable=False, server_default=sa.text("'deny'")),
        sa.Column("sensitivity", sa.String(length=32), nullable=False),
        sa.Column("session_override_allowed", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column(
            "allow_external_model_processing", sa.Boolean(), nullable=False, server_default=sa.text("false")
        ),
        sa.Column(
            "allowed_model_regions",
            postgresql.ARRAY(sa.String(length=64)),
            nullable=False,
            server_default=sa.text("ARRAY[]::varchar[]"),
        ),
        sa.Column("retention_days", sa.Integer()),
        *_lifecycle_columns(),
        sa.UniqueConstraint("source_id", "policy_version", name="knowledge_visibility_policy_version"),
        sa.CheckConstraint("default_effect IN ('allow', 'deny')", name="knowledge_visibility_default_effect"),
        schema="knowledge",
    )
    op.create_table(
        "visibility_grants",
        _id_column(),
        *_tenant_columns(),
        sa.Column(
            "policy_id",
            sa.String(length=128),
            sa.ForeignKey("knowledge.visibility_policies.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("agent_scope", sa.String(length=128), nullable=False),
        sa.Column("effect", sa.String(length=8), nullable=False),
        sa.Column(
            "allowed_operations",
            postgresql.ARRAY(sa.String(length=32)),
            nullable=False,
            server_default=sa.text("ARRAY[]::varchar[]"),
        ),
        *_lifecycle_columns(),
        sa.UniqueConstraint("policy_id", "agent_scope", name="knowledge_visibility_grants_scope"),
        sa.CheckConstraint("effect IN ('allow', 'deny')", name="knowledge_visibility_grants_effect"),
        schema="knowledge",
    )
    op.create_table(
        "chunks",
        _id_column(),
        *_tenant_columns(),
        sa.Column(
            "source_version_id",
            sa.String(length=128),
            sa.ForeignKey("knowledge.source_versions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("text_content", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.String(length=128), nullable=False),
        sa.Column("origin", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("token_count", sa.Integer()),
        *_lifecycle_columns(),
        sa.UniqueConstraint("source_version_id", "ordinal", name="knowledge_chunks_version_ordinal"),
        schema="knowledge",
    )
    op.create_table(
        "embedding_spaces",
        _id_column(),
        *_tenant_columns(),
        sa.Column("provider", sa.String(length=128), nullable=False),
        sa.Column("model", sa.String(length=256), nullable=False),
        sa.Column("model_revision", sa.String(length=256), nullable=False),
        sa.Column("dimension", sa.SmallInteger(), nullable=False, server_default=sa.text("1024")),
        sa.Column("distance_metric", sa.String(length=32), nullable=False),
        sa.Column("normalization", sa.String(length=64), nullable=False),
        sa.Column("retired_at", sa.DateTime(timezone=True)),
        *_lifecycle_columns(),
        sa.UniqueConstraint(
            "workspace_id",
            "resource_owner_id",
            "provider",
            "model",
            "model_revision",
            "dimension",
            "distance_metric",
            "normalization",
            name="embedding_spaces_identity",
        ),
        sa.CheckConstraint("dimension = 1024", name="embedding_spaces_v01_dimension"),
        sa.CheckConstraint(
            "distance_metric IN ('cosine', 'l2', 'inner_product')",
            name="embedding_spaces_distance_metric",
        ),
        schema="knowledge",
    )
    op.create_table(
        "embeddings",
        _id_column(),
        *_tenant_columns(),
        sa.Column(
            "chunk_id",
            sa.String(length=128),
            sa.ForeignKey("knowledge.chunks.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "embedding_space_id",
            sa.String(length=128),
            sa.ForeignKey("knowledge.embedding_spaces.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("embedding", Vector(1024), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("revision", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column(
            "extensions",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.UniqueConstraint("chunk_id", "embedding_space_id", name="knowledge_embeddings_chunk_space"),
        schema="knowledge",
    )
    op.create_table(
        "citations",
        _id_column(),
        *_tenant_columns(),
        sa.Column(
            "run_id", sa.String(length=128), sa.ForeignKey("agent.runs.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column(
            "chunk_id", sa.String(length=128), sa.ForeignKey("knowledge.chunks.id", ondelete="RESTRICT"), nullable=False
        ),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("retrieval_score", sa.Float()),
        sa.Column("quote", sa.Text()),
        sa.Column("origin", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        *_lifecycle_columns(),
        sa.UniqueConstraint("run_id", "ordinal", name="knowledge_citations_run_ordinal"),
        schema="knowledge",
    )
    op.create_table(
        "ingestion_jobs",
        _id_column(),
        *_tenant_columns(),
        sa.Column(
            "job_id", sa.String(length=128), sa.ForeignKey("agent.jobs.id", ondelete="CASCADE"), nullable=False, unique=True
        ),
        sa.Column(
            "source_id",
            sa.String(length=128),
            sa.ForeignKey("knowledge.sources.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "source_version_id", sa.String(length=128), sa.ForeignKey("knowledge.source_versions.id", ondelete="SET NULL")
        ),
        sa.Column("operation", sa.String(length=32), nullable=False),
        sa.Column(
            "statistics", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")
        ),
        *_lifecycle_columns(),
        schema="knowledge",
    )
    op.create_table(
        "access_snapshots",
        _id_column(),
        *_tenant_columns(),
        sa.Column("agent_run_id", sa.String(length=128), sa.ForeignKey("agent.runs.id", ondelete="CASCADE")),
        sa.Column(
            "interview_session_id",
            sa.String(length=128),
            sa.ForeignKey("interview.sessions.id", ondelete="CASCADE"),
        ),
        sa.Column("selection", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("policy_evaluation", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        *_lifecycle_columns(),
        sa.CheckConstraint(
            "agent_run_id IS NOT NULL OR interview_session_id IS NOT NULL",
            name="knowledge_access_snapshot_subject",
        ),
        schema="knowledge",
    )
    _create_indexes(
        (
            ("knowledge", "sources", ("workspace_id",)),
            ("knowledge", "sources", ("resource_owner_id",)),
            ("knowledge", "sources", ("workspace_id", "ingestion_state")),
            ("knowledge", "source_versions", ("workspace_id",)),
            ("knowledge", "source_versions", ("resource_owner_id",)),
            ("knowledge", "source_versions", ("source_id", "version_no")),
            ("knowledge", "visibility_policies", ("workspace_id",)),
            ("knowledge", "visibility_policies", ("resource_owner_id",)),
            ("knowledge", "visibility_grants", ("workspace_id",)),
            ("knowledge", "visibility_grants", ("resource_owner_id",)),
            ("knowledge", "chunks", ("workspace_id",)),
            ("knowledge", "chunks", ("resource_owner_id",)),
            ("knowledge", "chunks", ("source_version_id", "ordinal")),
            ("knowledge", "embedding_spaces", ("workspace_id",)),
            ("knowledge", "embedding_spaces", ("resource_owner_id",)),
            ("knowledge", "embeddings", ("workspace_id",)),
            ("knowledge", "embeddings", ("resource_owner_id",)),
            ("knowledge", "embeddings", ("embedding_space_id", "chunk_id")),
            ("knowledge", "citations", ("workspace_id",)),
            ("knowledge", "citations", ("resource_owner_id",)),
            ("knowledge", "ingestion_jobs", ("workspace_id",)),
            ("knowledge", "ingestion_jobs", ("resource_owner_id",)),
            ("knowledge", "access_snapshots", ("workspace_id",)),
            ("knowledge", "access_snapshots", ("resource_owner_id",)),
        )
    )
    op.execute(
        "CREATE INDEX ix_embeddings_embedding_hnsw_cosine "
        "ON knowledge.embeddings USING hnsw (embedding vector_cosine_ops) "
        "WITH (m = 16, ef_construction = 64)"
    )


def _create_observability_objects() -> None:
    """@brief 创建可观测性表与 Dashboard 只读视图 / Create observability table and Dashboard read-only view.

    @return 无返回值。

    @note telemetry sink 应使用有界异步队列和批量写入；本表不会记录每一条内部
    SQL client span，避免递归、自我观测噪声和不必要的 PostgreSQL 负担。
    """
    op.create_table(
        "telemetry_records",
        _id_column(),
        *_tenant_columns(),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("kind", sa.String(length=16), nullable=False),
        sa.Column("service", sa.String(length=128), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("value", sa.Float()),
        sa.Column("severity", sa.String(length=16)),
        sa.Column("request_id", sa.String(length=128)),
        sa.Column("trace_id", sa.String(length=128)),
        sa.Column("span_id", sa.String(length=128)),
        sa.Column("parent_span_id", sa.String(length=128)),
        sa.Column(
            "attributes", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")
        ),
        *_lifecycle_columns(),
        sa.CheckConstraint("kind IN ('metric', 'log', 'span')", name="telemetry_kind"),
        sa.CheckConstraint("kind <> 'metric' OR value IS NOT NULL", name="telemetry_metric_requires_value"),
        schema="observability",
    )
    _create_indexes(
        (
            ("observability", "telemetry_records", ("workspace_id",)),
            ("observability", "telemetry_records", ("resource_owner_id",)),
            ("observability", "telemetry_records", ("workspace_id", "occurred_at")),
            ("observability", "telemetry_records", ("workspace_id", "name", "occurred_at")),
        )
    )
    op.execute(
        """
        CREATE VIEW observability.dashboard_metric_samples
        WITH (security_barrier = true)
        AS
        SELECT
            workspace_id,
            occurred_at AS observed_at,
            service,
            name AS metric_name,
            value,
            attributes AS dimensions
        FROM observability.telemetry_records
        WHERE kind = 'metric'
          AND name IN ('requests', 'errors', 'latency_ms', 'saturation')
        """
    )


def _enable_row_level_security() -> None:
    """@brief 为所有租户表启用 RLS / Enable RLS for all tenant tables.

    @return 无返回值。

    @note ``AsyncDatabase.transaction`` 以事务本地 GUC 写入 app.workspace_id 与
    app.resource_owner_id；没有这两个值时 PostgreSQL 默认拒绝访问。
    """
    tenant_predicate = (
        "workspace_id = current_setting('app.workspace_id', true) "
        "AND resource_owner_id = current_setting('app.resource_owner_id', true)"
    )
    app_role = _configured_role("app_role")
    owner_role = _configured_role("owner_role")
    for qualified_name in TENANT_TABLES:
        op.execute(f"ALTER TABLE {qualified_name} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {qualified_name} FORCE ROW LEVEL SECURITY")
        op.execute(
            f"""
            CREATE POLICY workspace_app_tenant_scope
            ON {qualified_name}
            AS PERMISSIVE
            FOR ALL
            TO {app_role}
            USING ({tenant_predicate})
            WITH CHECK ({tenant_predicate})
            """
        )
    op.execute("ALTER TABLE identity.users ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE identity.users FORCE ROW LEVEL SECURITY")
    op.execute(
        f"""
        CREATE POLICY workspace_app_identity_self
        ON identity.users
        AS PERMISSIVE
        FOR ALL
        TO {app_role}
        USING (
            id = current_setting('app.actor_id', true)
            OR id = current_setting('app.resource_owner_id', true)
        )
        WITH CHECK (
            id = current_setting('app.actor_id', true)
            OR id = current_setting('app.resource_owner_id', true)
        )
        """
    )
    op.execute("ALTER TABLE identity.workspaces ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE identity.workspaces FORCE ROW LEVEL SECURITY")
    op.execute(
        f"""
        CREATE POLICY workspace_app_workspace_scope
        ON identity.workspaces
        AS PERMISSIVE
        FOR ALL
        TO {app_role}
        USING (
            id = current_setting('app.workspace_id', true)
            AND resource_owner_id = current_setting('app.resource_owner_id', true)
        )
        WITH CHECK (
            id = current_setting('app.workspace_id', true)
            AND resource_owner_id = current_setting('app.resource_owner_id', true)
        )
        """
    )
    op.execute(
        f"""
        CREATE POLICY workspace_owner_telemetry_view
        ON observability.telemetry_records
        AS PERMISSIVE
        FOR SELECT
        TO {owner_role}
        USING (true)
        """
    )


def _grant_runtime_privileges() -> None:
    """@brief 收紧 schema 权限并授予最小运行时权限 / Tighten schema permissions and grant minimal runtime privileges.

    @return 无返回值。

    @note 此函数依赖 bootstrap 已创建四个预定义 PostgreSQL roles。新表 migration
    必须同时补充 GRANT 与 RLS policy，不能依赖 application code “自觉隔离”。
    """
    business_schemas = ("identity", "resume", "agent", "interview", "knowledge")
    owner_role = _configured_role("owner_role")
    app_role = _configured_role("app_role")
    dashboard_role = _configured_role("dashboard_role")
    for schema in SCHEMAS:
        op.execute(f"REVOKE ALL ON SCHEMA {schema} FROM PUBLIC")
    for schema in business_schemas:
        op.execute(f"GRANT USAGE ON SCHEMA {schema} TO {app_role}")
        op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {schema} TO {app_role}")
        op.execute(
            f"ALTER DEFAULT PRIVILEGES FOR ROLE {owner_role} IN SCHEMA {schema} "
            f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {app_role}"
        )
    op.execute(f"GRANT USAGE ON SCHEMA observability TO {app_role}, {dashboard_role}")
    op.execute(f"GRANT INSERT ON observability.telemetry_records TO {app_role}")
    op.execute(f"GRANT SELECT ON observability.dashboard_metric_samples TO {dashboard_role}")
    op.execute(f"REVOKE ALL ON observability.telemetry_records FROM {dashboard_role}")
    op.execute(
        f"ALTER DEFAULT PRIVILEGES FOR ROLE {owner_role} IN SCHEMA observability "
        f"REVOKE ALL ON TABLES FROM {app_role}, {dashboard_role}"
    )
    op.execute(
        f"ALTER DEFAULT PRIVILEGES FOR ROLE {owner_role} IN SCHEMA observability "
        f"GRANT INSERT ON TABLES TO {app_role}"
    )


def upgrade() -> None:
    """@brief 建立 v0.1 PostgreSQL 持久化结构 / Create the v0.1 PostgreSQL persistence structure.

    @return 无返回值。

    @note ``CREATE EXTENSION vector`` 可能要求数据库管理员预安装扩展；dbctl
    bootstrap 应以管理员 DSN 完成该前置条件，本语句保留为幂等安全检查。
    """
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    _set_owner_role()
    for schema in SCHEMAS:
        op.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    _create_identity_tables()
    _create_agent_tables()
    _create_resume_tables()
    _create_interview_tables()
    _create_knowledge_tables()
    _create_observability_objects()
    _enable_row_level_security()
    _grant_runtime_privileges()


def downgrade() -> None:
    """@brief 删除本 revision 创建的对象 / Drop objects created by this revision.

    @return 无返回值。

    @note 不删除 ``vector`` extension：它可能在同一数据库被非本服务对象使用，盲目
    删除会破坏 userspace。schema 若已被后续 migration 或人工对象占用会显式失败。
    """
    _set_owner_role()
    op.execute("DROP VIEW observability.dashboard_metric_samples")
    op.drop_table("telemetry_records", schema="observability")
    op.drop_table("access_snapshots", schema="knowledge")
    op.drop_table("ingestion_jobs", schema="knowledge")
    op.drop_table("citations", schema="knowledge")
    op.execute("DROP INDEX knowledge.ix_embeddings_embedding_hnsw_cosine")
    op.drop_table("embeddings", schema="knowledge")
    op.drop_table("embedding_spaces", schema="knowledge")
    op.drop_table("chunks", schema="knowledge")
    op.drop_table("visibility_grants", schema="knowledge")
    op.drop_table("visibility_policies", schema="knowledge")
    op.drop_table("source_versions", schema="knowledge")
    op.drop_table("sources", schema="knowledge")
    op.drop_table("recording_artifacts", schema="interview")
    op.drop_table("report_jobs", schema="interview")
    op.drop_table("reports", schema="interview")
    op.drop_table("transcript_segments", schema="interview")
    op.drop_table("events", schema="interview")
    op.drop_table("sessions", schema="interview")
    op.drop_table("scenarios", schema="interview")
    op.drop_table("pdf_source_map_entries", schema="resume")
    op.drop_table("render_jobs", schema="resume")
    op.drop_table("render_artifacts", schema="resume")
    op.drop_table("proposal_operations", schema="resume")
    op.drop_table("proposals", schema="resume")
    op.drop_table("operations", schema="resume")
    op.drop_table("operation_batches", schema="resume")
    op.drop_table("revisions", schema="resume")
    op.drop_table("documents", schema="resume")
    op.drop_table("template_versions", schema="resume")
    op.drop_table("tool_approvals", schema="agent")
    op.drop_table("run_events", schema="agent")
    op.drop_table("runs", schema="agent")
    op.drop_table("messages", schema="agent")
    op.drop_table("conversations", schema="agent")
    op.drop_table("outbox_events", schema="agent")
    op.drop_table("jobs", schema="agent")
    op.drop_table("idempotency_records", schema="identity")
    op.drop_table("audit_events", schema="identity")
    op.drop_table("workspace_members", schema="identity")
    op.drop_table("workspaces", schema="identity")
    op.drop_table("users", schema="identity")
    for schema in reversed(SCHEMAS):
        op.execute(f"DROP SCHEMA {schema}")
