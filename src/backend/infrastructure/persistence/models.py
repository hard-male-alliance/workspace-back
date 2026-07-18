"""@brief PostgreSQL ORM 模型与稳定表元数据 / PostgreSQL ORM models and stable metadata.

这些模型是持久化映射，不是领域实体。面向用户的语义文档、Agent 输出和 rubric 等
可演进负载保存在 JSONB（JSON Binary）列中；资源之间的身份、版本、归属、事件与
检索关系仍使用外键及独立表表达，避免把整个系统退化为无约束 JSON 文档库。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    MetaData,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

JsonObject = dict[str, Any]


NAMING_CONVENTION: dict[str, str] = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_N_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}
"""@brief 约束与索引的稳定命名约定 / Stable naming convention for constraints and indexes."""


class Base(DeclarativeBase):
    """@brief 所有持久化映射的 Declarative 基类 / Declarative base for persistence mappings.

    @note Alembic 仅把 ``Base.metadata`` 当作 autogenerate 的目标；初始 migration
    本身是冻结的显式 DDL，不依赖未来模型演进。
    """

    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class TimestampedRevisionMixin:
    """@brief 资源生命周期与乐观版本字段 / Lifecycle and optimistic-version fields.

    此 mixin 不含主键，允许少数全局 identity 资源与普通 tenant 资源复用相同的
    审计时间和 ``revision`` 语义。
    """

    __abstract__ = True

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    revision: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("1"))
    extensions: Mapped[JsonObject] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )


class TenantScopedMixin(TimestampedRevisionMixin):
    """@brief 多租户资源字段 / Multi-tenant resource fields.

    ``workspace_id`` 是隔离分区，``resource_owner_id`` 指向该资源的个人所有者。
    两者同时参与应用层谓词和 PostgreSQL RLS（Row-Level Security）策略。
    """

    __abstract__ = True

    workspace_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("identity.workspaces.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    resource_owner_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("identity.users.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )


class UserRecord(Base, TimestampedRevisionMixin):
    """@brief 登录主体的稳定身份记录 / Stable identity record for a login principal.

    用户是全局 identity，不属于某一个 workspace，因此不继承 TenantScopedMixin；
    实际业务资源始终通过 ``resource_owner_id`` 关联到它。
    """

    __tablename__ = "users"
    __table_args__ = (
        UniqueConstraint("external_subject", name="users_external_subject"),
        {"schema": "identity"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    external_subject: Mapped[str] = mapped_column(String(320), nullable=False)
    display_name: Mapped[str | None] = mapped_column(String(256))
    email: Mapped[str | None] = mapped_column(String(320))
    locale: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'zh-CN'"))
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class WorkspaceRecord(Base, TimestampedRevisionMixin):
    """@brief 工作区租户根记录 / Workspace tenant-root record."""

    __tablename__ = "workspaces"
    __table_args__ = (
        Index("ix_workspaces_resource_owner_updated", "resource_owner_id", "updated_at"),
        {"schema": "identity"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    resource_owner_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("identity.users.id", ondelete="RESTRICT"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    default_locale: Mapped[str] = mapped_column(
        String(32), nullable=False, server_default=text("'zh-CN'")
    )
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class WorkspaceMemberRecord(Base, TenantScopedMixin):
    """@brief 工作区成员及角色记录 / Workspace membership and role record."""

    __tablename__ = "workspace_members"
    __table_args__ = (
        UniqueConstraint("workspace_id", "user_id", name="workspace_members_workspace_user"),
        CheckConstraint(
            "role IN ('owner', 'admin', 'editor', 'viewer')", name="workspace_members_role"
        ),
        CheckConstraint(
            "status IN ('active', 'invited', 'disabled')", name="workspace_members_status"
        ),
        {"schema": "identity"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    user_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("identity.users.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    role: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, server_default=text("'active'"))
    invited_by_actor_id: Mapped[str | None] = mapped_column(String(128))
    joined_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class AuditEventRecord(Base, TenantScopedMixin):
    """@brief 面向审计的安全与数据访问事件 / Security and data-access audit event."""

    __tablename__ = "audit_events"
    __table_args__ = (
        Index("ix_audit_events_workspace_occurred", "workspace_id", "occurred_at"),
        {"schema": "identity"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    actor_id: Mapped[str | None] = mapped_column(String(128), index=True)
    action: Mapped[str] = mapped_column(String(128), nullable=False)
    resource_type: Mapped[str] = mapped_column(String(64), nullable=False)
    resource_id: Mapped[str | None] = mapped_column(String(128))
    request_id: Mapped[str | None] = mapped_column(String(128))
    outcome: Mapped[str] = mapped_column(String(32), nullable=False)
    details: Mapped[JsonObject] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )


class IdempotencyRecord(Base, TenantScopedMixin):
    """@brief 幂等请求的持久化去重记录 / Persisted idempotent-request deduplication record."""

    __tablename__ = "idempotency_records"
    __table_args__ = (
        UniqueConstraint(
            "workspace_id",
            "resource_owner_id",
            "actor_id",
            "request_target",
            "idempotency_key",
            name="idempotency_scope_target_key",
        ),
        Index("ix_idempotency_records_expires", "expires_at"),
        {"schema": "identity"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    actor_id: Mapped[str] = mapped_column(String(128), nullable=False)
    request_target: Mapped[str] = mapped_column(String(256), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(256), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    response_status: Mapped[int | None] = mapped_column(SmallInteger)
    response_body: Mapped[JsonObject | None] = mapped_column(JSONB)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class JobRecord(Base, TenantScopedMixin):
    """@brief 跨子领域的长任务资源 / Cross-subdomain long-running job resource."""

    __tablename__ = "jobs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled', 'expired')",
            name="jobs_status",
        ),
        Index("ix_jobs_workspace_status_created", "workspace_id", "status", "created_at"),
        {"schema": "agent"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    job_type: Mapped[str] = mapped_column(String(96), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, server_default=text("'queued'"))
    phase: Mapped[str] = mapped_column(String(64), nullable=False, server_default=text("'queued'"))
    completed_units: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    total_units: Mapped[int | None] = mapped_column(Integer)
    percent: Mapped[float | None] = mapped_column(Float)
    request_id: Mapped[str | None] = mapped_column(String(128))
    target_resource_type: Mapped[str | None] = mapped_column(String(64))
    target_resource_id: Mapped[str | None] = mapped_column(String(128))
    result: Mapped[JsonObject | None] = mapped_column(JSONB)
    error: Mapped[JsonObject | None] = mapped_column(JSONB)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class OutboxEventRecord(Base, TenantScopedMixin):
    """@brief 事务发件箱事件 / Transactional outbox event.

    业务事务与事件写入共用同一个数据库提交；后台消费者在提交后异步投递，避免
    “资源已保存而消息丢失”的双写失败（dual-write failure）。
    """

    __tablename__ = "outbox_events"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'processing', 'published', 'failed')", name="outbox_status"
        ),
        Index("ix_outbox_events_status_occurred", "status", "occurred_at"),
        {"schema": "agent"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    aggregate_type: Mapped[str] = mapped_column(String(64), nullable=False)
    aggregate_id: Mapped[str] = mapped_column(String(128), nullable=False)
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    payload: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    trace_id: Mapped[str | None] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default=text("'pending'")
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))


class ConversationRecord(Base, TenantScopedMixin):
    """@brief Agent 对话会话 / Agent conversation session."""

    __tablename__ = "conversations"
    __table_args__ = (
        Index("ix_conversations_workspace_updated", "workspace_id", "updated_at"),
        {"schema": "agent"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    title: Mapped[str | None] = mapped_column(String(512))
    capability: Mapped[str | None] = mapped_column(String(128))
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ChatMessageRecord(Base, TenantScopedMixin):
    """@brief 对话中的结构化消息 / Structured message within a conversation."""

    __tablename__ = "messages"
    __table_args__ = (
        UniqueConstraint("conversation_id", "sequence", name="messages_conversation_sequence"),
        CheckConstraint("role IN ('system', 'user', 'assistant', 'tool')", name="messages_role"),
        Index("ix_messages_conversation_sequence", "conversation_id", "sequence"),
        {"schema": "agent"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    conversation_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("agent.conversations.id", ondelete="CASCADE"), nullable=False
    )
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    role: Mapped[str] = mapped_column(String(16), nullable=False)
    content_parts: Mapped[list[JsonObject]] = mapped_column(JSONB, nullable=False)
    final_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    model_metadata: Mapped[JsonObject] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )


class AgentRunRecord(Base, TenantScopedMixin):
    """@brief 一次可审计 Agent 执行 / One auditable Agent execution."""

    __tablename__ = "runs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled', 'expired')",
            name="agent_runs_status",
        ),
        Index("ix_agent_runs_conversation_created", "conversation_id", "created_at"),
        {"schema": "agent"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    conversation_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("agent.conversations.id", ondelete="CASCADE"), nullable=False
    )
    input_message_id: Mapped[str | None] = mapped_column(
        String(128), ForeignKey("agent.messages.id", ondelete="SET NULL")
    )
    job_id: Mapped[str | None] = mapped_column(
        String(128), ForeignKey("agent.jobs.id", ondelete="SET NULL"), unique=True
    )
    capability: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, server_default=text("'queued'"))
    response_locale: Mapped[str] = mapped_column(
        String(32), nullable=False, server_default=text("'zh-CN'")
    )
    inference_intent: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    effective_knowledge_selection: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    provider: Mapped[str | None] = mapped_column(String(128))
    model: Mapped[str | None] = mapped_column(String(256))
    model_revision: Mapped[str | None] = mapped_column(String(256))
    token_usage: Mapped[JsonObject] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )
    cost: Mapped[JsonObject] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )
    error: Mapped[JsonObject | None] = mapped_column(JSONB)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class AgentRunEventRecord(Base, TenantScopedMixin):
    """@brief 可重放的 Agent SSE 事件 / Replayable Agent SSE event."""

    __tablename__ = "run_events"
    __table_args__ = (
        UniqueConstraint("run_id", "sequence", name="agent_run_events_run_sequence"),
        Index("ix_agent_run_events_run_sequence", "run_id", "sequence"),
        {"schema": "agent"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    run_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("agent.runs.id", ondelete="CASCADE"), nullable=False
    )
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    payload: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    trace_id: Mapped[str | None] = mapped_column(String(128))


class ToolApprovalRecord(Base, TenantScopedMixin):
    """@brief 需要用户决定的 Agent 工具审批 / Agent tool approval requiring a user decision."""

    __tablename__ = "tool_approvals"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'approved', 'rejected', 'expired')", name="tool_approvals_status"
        ),
        Index("ix_tool_approvals_run_status", "run_id", "status"),
        {"schema": "agent"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    run_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("agent.runs.id", ondelete="CASCADE"), nullable=False
    )
    tool_name: Mapped[str] = mapped_column(String(128), nullable=False)
    request_payload: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default=text("'pending'")
    )
    decision_payload: Mapped[JsonObject | None] = mapped_column(JSONB)
    decided_by_actor_id: Mapped[str | None] = mapped_column(String(128))
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ResumeTemplateRecord(Base, TenantScopedMixin):
    """@brief 不可变简历模板版本 / Immutable resume-template version."""

    __tablename__ = "template_versions"
    __table_args__ = (
        UniqueConstraint(
            "workspace_id",
            "resource_owner_id",
            "template_id",
            "template_version",
            name="templates_scope_version",
        ),
        {"schema": "resume"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    template_id: Mapped[str] = mapped_column(String(128), nullable=False)
    template_version: Mapped[str] = mapped_column(String(128), nullable=False)
    manifest: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    renderer_binding: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    retired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ResumeDocumentRecord(Base, TenantScopedMixin):
    """@brief 简历聚合根元数据 / Resume aggregate-root metadata."""

    __tablename__ = "documents"
    __table_args__ = (
        Index("ix_resume_documents_workspace_updated", "workspace_id", "updated_at"),
        {"schema": "resume"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    template_version_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("resume.template_versions.id", ondelete="RESTRICT"), nullable=False
    )
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    locale: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'zh-CN'"))
    current_revision_no: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("1")
    )
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ResumeRevisionRecord(Base, TenantScopedMixin):
    """@brief 不可变的简历语义快照 / Immutable resume semantic snapshot."""

    __tablename__ = "revisions"
    __table_args__ = (
        UniqueConstraint("resume_id", "revision_no", name="resume_revisions_document_revision"),
        Index("ix_resume_revisions_resume_number", "resume_id", "revision_no"),
        {"schema": "resume"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    resume_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("resume.documents.id", ondelete="CASCADE"), nullable=False
    )
    revision_no: Mapped[int] = mapped_column(Integer, nullable=False)
    semantic_document: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    content_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    created_by_actor_id: Mapped[str | None] = mapped_column(String(128))
    source: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'user'"))


class ResumeOperationBatchRecord(Base, TenantScopedMixin):
    """@brief 简历领域操作批次 / Resume domain-operation batch."""

    __tablename__ = "operation_batches"
    __table_args__ = (
        UniqueConstraint(
            "workspace_id",
            "resource_owner_id",
            "resume_id",
            "client_batch_id",
            name="resume_batches_client_id",
        ),
        CheckConstraint(
            "status IN ('received', 'applied', 'conflicted', 'rejected')",
            name="resume_batches_status",
        ),
        {"schema": "resume"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    resume_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("resume.documents.id", ondelete="CASCADE"), nullable=False
    )
    client_batch_id: Mapped[str] = mapped_column(String(128), nullable=False)
    base_revision_no: Mapped[int] = mapped_column(Integer, nullable=False)
    applied_revision_no: Mapped[int | None] = mapped_column(Integer)
    conflict_strategy: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default=text("'received'")
    )
    idempotency_record_id: Mapped[str | None] = mapped_column(
        String(128), ForeignKey("identity.idempotency_records.id", ondelete="SET NULL")
    )


class ResumeOperationRecord(Base, TenantScopedMixin):
    """@brief 操作批次中的单个稳定 ID 操作 / One stable-ID operation in a resume batch."""

    __tablename__ = "operations"
    __table_args__ = (
        UniqueConstraint("batch_id", "ordinal", name="resume_operations_batch_ordinal"),
        UniqueConstraint("operation_id", name="resume_operations_operation_id"),
        {"schema": "resume"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    batch_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("resume.operation_batches.id", ondelete="CASCADE"), nullable=False
    )
    operation_id: Mapped[str] = mapped_column(String(128), nullable=False)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    operation_type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)


class ResumeProposalRecord(Base, TenantScopedMixin):
    """@brief Agent 生成、用户可审批的简历建议 / User-approvable resume proposal produced by an Agent."""

    __tablename__ = "proposals"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'accepted', 'partially_accepted', 'rejected', 'expired', 'conflicted')",
            name="resume_proposals_status",
        ),
        Index("ix_resume_proposals_resume_status", "resume_id", "status"),
        {"schema": "resume"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    resume_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("resume.documents.id", ondelete="CASCADE"), nullable=False
    )
    agent_run_id: Mapped[str | None] = mapped_column(
        String(128), ForeignKey("agent.runs.id", ondelete="SET NULL")
    )
    base_revision_no: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(
        String(24), nullable=False, server_default=text("'pending'")
    )
    decision_payload: Mapped[JsonObject | None] = mapped_column(JSONB)
    decided_by_actor_id: Mapped[str | None] = mapped_column(String(128))
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ResumeProposalOperationRecord(Base, TenantScopedMixin):
    """@brief 简历建议中的单个可选择操作 / Individually selectable operation in a resume proposal."""

    __tablename__ = "proposal_operations"
    __table_args__ = (
        UniqueConstraint("proposal_id", "ordinal", name="resume_proposal_operations_ordinal"),
        {"schema": "resume"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    proposal_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("resume.proposals.id", ondelete="CASCADE"), nullable=False
    )
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    operation_type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    decision: Mapped[str | None] = mapped_column(String(16))


class RenderArtifactRecord(Base, TenantScopedMixin):
    """@brief 简历渲染产物元数据 / Resume rendering artifact metadata."""

    __tablename__ = "render_artifacts"
    __table_args__ = (
        UniqueConstraint("storage_key", name="render_artifacts_storage_key"),
        Index("ix_render_artifacts_resume_revision", "resume_id", "resume_revision_id"),
        {"schema": "resume"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    resume_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("resume.documents.id", ondelete="CASCADE"), nullable=False
    )
    resume_revision_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("resume.revisions.id", ondelete="RESTRICT"), nullable=False
    )
    artifact_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    format: Mapped[str] = mapped_column(String(32), nullable=False)
    storage_key: Mapped[str] = mapped_column(String(1024), nullable=False)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    content_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class RenderArtifactBlobRecord(Base, TenantScopedMixin):
    """@brief 渲染产物数据库二进制负载 / Database binary payload for a render artifact.

    v0.1 没有外部对象存储（object storage）端口。为保证 PostgreSQL 模式重启后仍能
    下载 PDF，本表以 ``BYTEA`` 保存受大小限制的渲染结果；``storage_key`` 仍保留在
    元数据表中，以便未来无破坏地迁移到对象存储。
    """

    __tablename__ = "artifact_blobs"
    __table_args__ = ({"schema": "resume"},)

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    artifact_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("resume.render_artifacts.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    content: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    source_map: Mapped[JsonObject | None] = mapped_column(JSONB)


class ResumeRenderJobRecord(Base, TenantScopedMixin):
    """@brief 简历渲染任务与输入/输出的关联 / Association of a resume render job with inputs and outputs."""

    __tablename__ = "render_jobs"
    __table_args__ = ({"schema": "resume"},)

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    job_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("agent.jobs.id", ondelete="CASCADE"), nullable=False, unique=True
    )
    resume_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("resume.documents.id", ondelete="CASCADE"), nullable=False
    )
    resume_revision_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("resume.revisions.id", ondelete="RESTRICT"), nullable=False
    )
    artifact_id: Mapped[str | None] = mapped_column(
        String(128), ForeignKey("resume.render_artifacts.id", ondelete="SET NULL")
    )
    render_profile: Mapped[str] = mapped_column(String(64), nullable=False)
    diagnostics: Mapped[JsonObject] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )


class PdfSourceMapEntryRecord(Base, TenantScopedMixin):
    """@brief PDF 语义节点到页面坐标的映射 / Mapping from semantic node to PDF coordinates."""

    __tablename__ = "pdf_source_map_entries"
    __table_args__ = (
        Index("ix_pdf_source_map_entries_artifact_node", "artifact_id", "node_id"),
        {"schema": "resume"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    artifact_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("resume.render_artifacts.id", ondelete="CASCADE"), nullable=False
    )
    node_kind: Mapped[str] = mapped_column(String(64), nullable=False)
    node_id: Mapped[str] = mapped_column(String(128), nullable=False)
    field_path: Mapped[list[str]] = mapped_column(ARRAY(String(128)), nullable=False)
    page: Mapped[int] = mapped_column(Integer, nullable=False)
    rects: Mapped[list[JsonObject]] = mapped_column(JSONB, nullable=False)


class InterviewScenarioRecord(Base, TenantScopedMixin):
    """@brief 可版本化的面试场景与量表 / Versioned interview scenario and rubric."""

    __tablename__ = "scenarios"
    __table_args__ = (
        Index("ix_interview_scenarios_workspace_updated", "workspace_id", "updated_at"),
        {"schema": "interview"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    locale: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'zh-CN'"))
    role_target: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    rubric: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    is_template: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class InterviewSessionRecord(Base, TenantScopedMixin):
    """@brief 数字人模拟面试会话 / Digital-human mock interview session."""

    __tablename__ = "sessions"
    __table_args__ = (
        CheckConstraint(
            "state IN ('created', 'preparing', 'ready', 'connecting', 'in_progress', "
            "'ending', 'processing_report', 'completed', 'aborted', 'expired', 'failed')",
            name="interview_sessions_state",
        ),
        Index("ix_interview_sessions_workspace_created", "workspace_id", "created_at"),
        {"schema": "interview"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    scenario_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("interview.scenarios.id", ondelete="RESTRICT"), nullable=False
    )
    resume_revision_id: Mapped[str | None] = mapped_column(
        String(128), ForeignKey("resume.revisions.id", ondelete="SET NULL")
    )
    state: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'created'"))
    job_target: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    effective_knowledge_selection: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    inference_intent: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    media_capabilities: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    avatar_output_mode: Mapped[str] = mapped_column(String(32), nullable=False)
    consent: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    recording_retention_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    failure: Mapped[JsonObject | None] = mapped_column(JSONB)


class InterviewEventRecord(Base, TenantScopedMixin):
    """@brief 可重放的面试控制事件 / Replayable interview control event."""

    __tablename__ = "events"
    __table_args__ = (
        UniqueConstraint("session_id", "sequence", name="interview_events_session_sequence"),
        Index("ix_interview_events_session_sequence", "session_id", "sequence"),
        {"schema": "interview"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    session_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("interview.sessions.id", ondelete="CASCADE"), nullable=False
    )
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    ack_sequence: Mapped[int | None] = mapped_column(BigInteger)
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    payload: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    trace_id: Mapped[str | None] = mapped_column(String(128))


class TranscriptSegmentRecord(Base, TenantScopedMixin):
    """@brief 面试转录的有序片段 / Ordered interview transcript segment."""

    __tablename__ = "transcript_segments"
    __table_args__ = (
        UniqueConstraint("session_id", "sequence", name="transcript_segments_session_sequence"),
        CheckConstraint(
            "speaker IN ('candidate', 'interviewer', 'system')", name="transcript_segments_speaker"
        ),
        Index("ix_transcript_segments_session_start", "session_id", "start_ms"),
        {"schema": "interview"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    session_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("interview.sessions.id", ondelete="CASCADE"), nullable=False
    )
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    speaker: Mapped[str] = mapped_column(String(16), nullable=False)
    start_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    end_ms: Mapped[int | None] = mapped_column(BigInteger)
    text_content: Mapped[str] = mapped_column(Text, nullable=False)
    is_final: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    generated_text: Mapped[str | None] = mapped_column(Text)
    media_scheduled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    media_played_ack_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class InterviewReportRecord(Base, TenantScopedMixin):
    """@brief 基于转录证据生成的面试报告 / Interview report generated from transcript evidence."""

    __tablename__ = "reports"
    __table_args__ = (
        UniqueConstraint("session_id", "report_version", name="interview_reports_session_version"),
        {"schema": "interview"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    session_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("interview.sessions.id", ondelete="CASCADE"), nullable=False
    )
    report_version: Mapped[int] = mapped_column(Integer, nullable=False)
    rubric_version: Mapped[str] = mapped_column(String(128), nullable=False)
    engine_version: Mapped[str] = mapped_column(String(256), nullable=False)
    report: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    generated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class InterviewReportJobRecord(Base, TenantScopedMixin):
    """@brief 面试报告 Job 与报告产物的关联 / Association between report job and report artifact."""

    __tablename__ = "report_jobs"
    __table_args__ = ({"schema": "interview"},)

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    job_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("agent.jobs.id", ondelete="CASCADE"), nullable=False, unique=True
    )
    session_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("interview.sessions.id", ondelete="CASCADE"), nullable=False
    )
    report_id: Mapped[str | None] = mapped_column(
        String(128), ForeignKey("interview.reports.id", ondelete="SET NULL")
    )


class RecordingArtifactRecord(Base, TenantScopedMixin):
    """@brief 原始音视频录制的元数据 / Metadata for retained raw audio-video recording."""

    __tablename__ = "recording_artifacts"
    __table_args__ = (
        UniqueConstraint("storage_key", name="interview_recordings_storage_key"),
        {"schema": "interview"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    session_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("interview.sessions.id", ondelete="CASCADE"), nullable=False
    )
    storage_key: Mapped[str] = mapped_column(String(1024), nullable=False)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    content_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    media_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class KnowledgeSourceRecord(Base, TenantScopedMixin):
    """@brief 个人知识来源元数据 / Personal knowledge-source metadata."""

    __tablename__ = "sources"
    __table_args__ = (
        CheckConstraint(
            "source_type IN ('resume', 'file', 'url', 'git_repository', 'manual_note')",
            name="knowledge_sources_type",
        ),
        CheckConstraint(
            "ingestion_state IN ('new', 'queued', 'indexing', 'ready', 'stale', 'deleted', 'failed')",
            name="knowledge_sources_ingestion_state",
        ),
        Index("ix_knowledge_sources_workspace_state", "workspace_id", "ingestion_state"),
        {"schema": "knowledge"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    source_type: Mapped[str] = mapped_column(String(32), nullable=False)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    config: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    revision_mode: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default=text("'latest'")
    )
    ingestion_state: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default=text("'new'")
    )
    sync_schedule: Mapped[JsonObject | None] = mapped_column(JSONB)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class KnowledgeSourceVersionRecord(Base, TenantScopedMixin):
    """@brief 不可变知识来源版本 / Immutable version of a knowledge source."""

    __tablename__ = "source_versions"
    __table_args__ = (
        UniqueConstraint("source_id", "version_no", name="knowledge_source_versions_source_number"),
        Index("ix_knowledge_source_versions_source_number", "source_id", "version_no"),
        {"schema": "knowledge"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    source_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("knowledge.sources.id", ondelete="CASCADE"), nullable=False
    )
    version_no: Mapped[int] = mapped_column(Integer, nullable=False)
    content_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    origin: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    parser_metadata: Mapped[JsonObject] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )
    indexed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class KnowledgeVisibilityPolicyRecord(Base, TenantScopedMixin):
    """@brief 知识来源的版本化可见性策略 / Versioned visibility policy for a knowledge source."""

    __tablename__ = "visibility_policies"
    __table_args__ = (
        UniqueConstraint("source_id", "policy_version", name="knowledge_visibility_policy_version"),
        CheckConstraint(
            "default_effect IN ('allow', 'deny')", name="knowledge_visibility_default_effect"
        ),
        {"schema": "knowledge"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    source_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("knowledge.sources.id", ondelete="CASCADE"), nullable=False
    )
    policy_version: Mapped[int] = mapped_column(Integer, nullable=False)
    default_effect: Mapped[str] = mapped_column(
        String(8), nullable=False, server_default=text("'deny'")
    )
    sensitivity: Mapped[str] = mapped_column(String(32), nullable=False)
    session_override_allowed: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
    allow_external_model_processing: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
    allowed_model_regions: Mapped[list[str]] = mapped_column(
        ARRAY(String(64)), nullable=False, server_default=text("ARRAY[]::varchar[]")
    )
    retention_days: Mapped[int | None] = mapped_column(Integer)


class KnowledgeVisibilityGrantRecord(Base, TenantScopedMixin):
    """@brief 策略内针对 Agent scope 的显式授权 / Explicit policy grant for an Agent scope."""

    __tablename__ = "visibility_grants"
    __table_args__ = (
        UniqueConstraint("policy_id", "agent_scope", name="knowledge_visibility_grants_scope"),
        CheckConstraint("effect IN ('allow', 'deny')", name="knowledge_visibility_grants_effect"),
        {"schema": "knowledge"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    policy_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("knowledge.visibility_policies.id", ondelete="CASCADE"),
        nullable=False,
    )
    agent_scope: Mapped[str] = mapped_column(String(128), nullable=False)
    effect: Mapped[str] = mapped_column(String(8), nullable=False)
    allowed_operations: Mapped[list[str]] = mapped_column(
        ARRAY(String(32)), nullable=False, server_default=text("ARRAY[]::varchar[]")
    )


class KnowledgeChunkRecord(Base, TenantScopedMixin):
    """@brief 可检索来源版本的分块文本 / Chunked text for a retrievable source version."""

    __tablename__ = "chunks"
    __table_args__ = (
        UniqueConstraint("source_version_id", "ordinal", name="knowledge_chunks_version_ordinal"),
        Index("ix_knowledge_chunks_version_ordinal", "source_version_id", "ordinal"),
        {"schema": "knowledge"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    source_version_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("knowledge.source_versions.id", ondelete="CASCADE"), nullable=False
    )
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    text_content: Mapped[str] = mapped_column(Text, nullable=False)
    content_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    origin: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    token_count: Mapped[int | None] = mapped_column(Integer)


class EmbeddingSpaceRecord(Base, TenantScopedMixin):
    """@brief 不可混用的 embedding 向量空间 / Non-interchangeable embedding vector space.

    任何 provider、model、revision、dimension、distance metric 或 normalization 的
    变化都必须创建新记录；v0.1 的实际向量列固定为 1024 维。
    """

    __tablename__ = "embedding_spaces"
    __table_args__ = (
        UniqueConstraint(
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
        CheckConstraint("dimension = 1024", name="embedding_spaces_v01_dimension"),
        CheckConstraint(
            "distance_metric IN ('cosine', 'l2', 'inner_product')",
            name="embedding_spaces_distance_metric",
        ),
        {"schema": "knowledge"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    provider: Mapped[str] = mapped_column(String(128), nullable=False)
    model: Mapped[str] = mapped_column(String(256), nullable=False)
    model_revision: Mapped[str] = mapped_column(String(256), nullable=False)
    dimension: Mapped[int] = mapped_column(
        SmallInteger, nullable=False, server_default=text("1024")
    )
    distance_metric: Mapped[str] = mapped_column(String(32), nullable=False)
    normalization: Mapped[str] = mapped_column(String(64), nullable=False)
    retired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class KnowledgeEmbeddingRecord(Base, TenantScopedMixin):
    """@brief chunk 在指定向量空间中的 embedding / A chunk embedding in a specified vector space."""

    __tablename__ = "embeddings"
    __table_args__ = (
        UniqueConstraint("chunk_id", "embedding_space_id", name="knowledge_embeddings_chunk_space"),
        Index("ix_knowledge_embeddings_space_chunk", "embedding_space_id", "chunk_id"),
        Index(
            "ix_embeddings_embedding_hnsw_cosine",
            "embedding",
            postgresql_using="hnsw",
            postgresql_with={"m": 16, "ef_construction": 64},
            postgresql_ops={"embedding": "vector_cosine_ops"},
        ),
        {"schema": "knowledge"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    chunk_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("knowledge.chunks.id", ondelete="CASCADE"), nullable=False
    )
    embedding_space_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("knowledge.embedding_spaces.id", ondelete="RESTRICT"),
        nullable=False,
    )
    embedding: Mapped[list[float]] = mapped_column(Vector(1024), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class KnowledgeCitationRecord(Base, TenantScopedMixin):
    """@brief Agent 输出与知识 chunk 的可审计引用 / Auditable citation from Agent output to a knowledge chunk."""

    __tablename__ = "citations"
    __table_args__ = (
        UniqueConstraint("run_id", "ordinal", name="knowledge_citations_run_ordinal"),
        {"schema": "knowledge"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    run_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("agent.runs.id", ondelete="CASCADE"), nullable=False
    )
    chunk_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("knowledge.chunks.id", ondelete="RESTRICT"), nullable=False
    )
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    retrieval_score: Mapped[float | None] = mapped_column(Float)
    quote: Mapped[str | None] = mapped_column(Text)
    origin: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)


class KnowledgeIngestionJobRecord(Base, TenantScopedMixin):
    """@brief 知识导入/同步 Job 的子领域关联 / Knowledge ingestion or sync job association."""

    __tablename__ = "ingestion_jobs"
    __table_args__ = ({"schema": "knowledge"},)

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    job_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("agent.jobs.id", ondelete="CASCADE"), nullable=False, unique=True
    )
    source_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("knowledge.sources.id", ondelete="CASCADE"), nullable=False
    )
    source_version_id: Mapped[str | None] = mapped_column(
        String(128), ForeignKey("knowledge.source_versions.id", ondelete="SET NULL")
    )
    operation: Mapped[str] = mapped_column(String(32), nullable=False)
    statistics: Mapped[JsonObject] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )


class KnowledgeAccessSnapshotRecord(Base, TenantScopedMixin):
    """@brief 固化的知识可见性决策快照 / Frozen knowledge-visibility decision snapshot."""

    __tablename__ = "access_snapshots"
    __table_args__ = (
        CheckConstraint(
            "agent_run_id IS NOT NULL OR interview_session_id IS NOT NULL",
            name="knowledge_access_snapshot_subject",
        ),
        {"schema": "knowledge"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    agent_run_id: Mapped[str | None] = mapped_column(
        String(128), ForeignKey("agent.runs.id", ondelete="CASCADE")
    )
    interview_session_id: Mapped[str | None] = mapped_column(
        String(128), ForeignKey("interview.sessions.id", ondelete="CASCADE")
    )
    selection: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    policy_evaluation: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)


class TelemetryRecord(Base, TenantScopedMixin):
    """@brief 低基数业务 telemetry 原始记录 / Low-cardinality business telemetry raw record.

    该表只接收业务 metric/log/span。数据库客户端自身的 SQL span 不应写入，避免
    telemetry sink 递归；完整 prompt、用户文本、URL 和异常堆栈等高基数值也不得
    放进 ``attributes``。
    """

    __tablename__ = "telemetry_records"
    __table_args__ = (
        CheckConstraint("kind IN ('metric', 'log', 'span')", name="telemetry_kind"),
        CheckConstraint(
            "kind <> 'metric' OR value IS NOT NULL", name="telemetry_metric_requires_value"
        ),
        Index("ix_telemetry_workspace_occurred", "workspace_id", "occurred_at"),
        Index("ix_telemetry_workspace_name_occurred", "workspace_id", "name", "occurred_at"),
        {"schema": "observability"},
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    actor_id: Mapped[str] = mapped_column(String(128), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    kind: Mapped[str] = mapped_column(String(16), nullable=False)
    service: Mapped[str] = mapped_column(String(128), nullable=False)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    value: Mapped[float | None] = mapped_column(Float)
    severity: Mapped[str | None] = mapped_column(String(16))
    request_id: Mapped[str | None] = mapped_column(String(128))
    trace_id: Mapped[str | None] = mapped_column(String(128))
    span_id: Mapped[str | None] = mapped_column(String(128))
    parent_span_id: Mapped[str | None] = mapped_column(String(128))
    attributes: Mapped[JsonObject] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )


__all__ = [
    "AgentRunEventRecord",
    "AgentRunRecord",
    "AuditEventRecord",
    "Base",
    "ChatMessageRecord",
    "ConversationRecord",
    "EmbeddingSpaceRecord",
    "IdempotencyRecord",
    "InterviewEventRecord",
    "InterviewReportJobRecord",
    "InterviewReportRecord",
    "InterviewScenarioRecord",
    "InterviewSessionRecord",
    "JobRecord",
    "KnowledgeAccessSnapshotRecord",
    "KnowledgeChunkRecord",
    "KnowledgeCitationRecord",
    "KnowledgeEmbeddingRecord",
    "KnowledgeIngestionJobRecord",
    "KnowledgeSourceRecord",
    "KnowledgeSourceVersionRecord",
    "KnowledgeVisibilityGrantRecord",
    "KnowledgeVisibilityPolicyRecord",
    "OutboxEventRecord",
    "PdfSourceMapEntryRecord",
    "RecordingArtifactRecord",
    "RenderArtifactBlobRecord",
    "RenderArtifactRecord",
    "ResumeDocumentRecord",
    "ResumeOperationBatchRecord",
    "ResumeOperationRecord",
    "ResumeProposalOperationRecord",
    "ResumeProposalRecord",
    "ResumeRenderJobRecord",
    "ResumeRevisionRecord",
    "ResumeTemplateRecord",
    "TelemetryRecord",
    "TenantScopedMixin",
    "TimestampedRevisionMixin",
    "ToolApprovalRecord",
    "TranscriptSegmentRecord",
    "UserRecord",
    "WorkspaceMemberRecord",
    "WorkspaceRecord",
]
