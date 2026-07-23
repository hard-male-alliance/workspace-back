"""@brief API v2 Conversation、Message 与 Agent 领域核心 / API v2 Conversation, Message, and Agent domain core.

本模块只表达 ``contracts/v2`` 5.4 的稳定业务语义。私有推理轨迹（chain-of-thought）
没有任何公开值对象、持久字段或事件入口；模型只能返回用户可见内容、Proposal 引用、
工具调用摘要和计量结果。HTTP、数据库、模型 provider 与工具执行均留在外层 adapter。
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Literal, NewType

from backend.domain.knowledge_retrieval import (
    InferenceIntent,
    KnowledgeCitation,
    KnowledgeSelection,
    KnowledgeSelectionMode,
)
from backend.domain.knowledge_sources import (
    KnowledgeSourceId,
    KnowledgeSourceVersionId,
    ModelRegion,
)
from backend.domain.platform import Job, JobId, JobStatus, JsonValue, ProblemDetails
from backend.domain.principals import DomainInvariantError, ResourceMeta, UserId, WorkspaceId
from backend.domain.resources import ResourceRef
from backend.domain.resumes import ResumeDocument

ConversationId = NewType("ConversationId", str)
"""@brief Conversation 不透明标识 / Opaque Conversation identifier."""

MessageId = NewType("MessageId", str)
"""@brief Message 不透明标识 / Opaque Message identifier."""

AgentRunId = NewType("AgentRunId", str)
"""@brief AgentRun 不透明标识 / Opaque AgentRun identifier."""

ToolApprovalId = NewType("ToolApprovalId", str)
"""@brief ToolApproval 不透明标识 / Opaque ToolApproval identifier."""

ToolCallId = NewType("ToolCallId", str)
"""@brief provider 生成的精确工具调用标识 / Exact provider-issued tool-call identifier."""

AgentOutboxId = NewType("AgentOutboxId", str)
"""@brief Agent transactional-outbox 记录标识 / Agent transactional-outbox record identifier."""

AGENT_RUN_JOB_KIND = "agent.run"
"""@brief 统一 platform Job 中的 Agent Run kind / Agent Run kind in the unified platform Job."""

_OPAQUE_ID = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{7,159}$")
"""@brief API v2 不透明 ID 语法 / API v2 opaque-ID grammar."""

_STABLE_NAME = re.compile(r"^[a-z][a-z0-9_.-]{2,100}$")
"""@brief agent scope、tool 与 model 稳定名语法 / Stable agent-scope, tool, and model-name grammar."""

_LOCALE = re.compile(r"^[A-Za-z]{2,8}(?:-[A-Za-z0-9]{1,8})*$")
"""@brief BCP-47 子集 locale 语法 / Contract BCP-47-subset locale grammar."""


class AgentDomainError(DomainInvariantError):
    """@brief Agent V2 领域不变量错误 / Agent V2 domain-invariant error."""


class ConversationUnavailable(AgentDomainError):
    """@brief Conversation 已软删除或当前不可写 / Conversation is soft-deleted or not writable."""


class AgentRunTransitionError(AgentDomainError):
    """@brief AgentRun 状态机拒绝迁移 / AgentRun state machine rejected a transition.

    @param current 当前状态 / Current state.
    @param requested 请求状态 / Requested state.
    """

    current: AgentRunStatus
    """@brief 当前 AgentRun 状态 / Current AgentRun state."""

    requested: AgentRunStatus
    """@brief 请求的 AgentRun 状态 / Requested AgentRun state."""

    def __init__(self, current: AgentRunStatus, requested: AgentRunStatus) -> None:
        """@brief 初始化非法迁移 / Initialize an invalid transition."""
        super().__init__(f"agent run cannot transition from {current.value} to {requested.value}")
        self.current = current
        self.requested = requested


class ToolApprovalDecisionError(AgentDomainError):
    """@brief ToolApproval 已过期、已决定或绑定不匹配 / ToolApproval expired, decided, or mismatched."""


class ConversationCapability(StrEnum):
    """@brief 契约冻结的 Conversation 能力 / Contract-frozen Conversation capabilities."""

    GENERAL = "general"
    RESUME_EDIT = "resume_edit"
    KNOWLEDGE_QUERY = "knowledge_query"
    INTERVIEW_COACH = "interview_coach"


class ConversationStatus(StrEnum):
    """@brief 公开 Conversation 状态 / Public Conversation states."""

    ACTIVE = "active"
    ARCHIVED = "archived"


class MessageRole(StrEnum):
    """@brief 公开 Message role 判别值 / Public Message-role discriminants."""

    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM_NOTICE = "system_notice"


class AgentOutputMode(StrEnum):
    """@brief AgentRun 可请求的输出模式 / AgentRun output modes."""

    TEXT = "text"
    CITATIONS = "citations"
    RESUME_OPERATIONS = "resume_operations"


class AgentRunStatus(StrEnum):
    """@brief 契约冻结的 AgentRun 状态 / Contract-frozen AgentRun states."""

    QUEUED = "queued"
    RUNNING = "running"
    WAITING_FOR_APPROVAL = "waiting_for_approval"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        """@brief 判断是否终态 / Test whether this is a terminal state.

        @return 成功、失败或取消时为真 / True for succeeded, failed, or cancelled.
        """
        return self in {
            AgentRunStatus.SUCCEEDED,
            AgentRunStatus.FAILED,
            AgentRunStatus.CANCELLED,
        }


class ToolRisk(StrEnum):
    """@brief 工具调用风险等级 / Tool-call risk levels."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class ToolApprovalStatus(StrEnum):
    """@brief ToolApproval 状态 / ToolApproval states."""

    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"


class ToolDecision(StrEnum):
    """@brief 用户可提交的工具决定 / User-submittable tool decisions."""

    APPROVE = "approve"
    REJECT = "reject"


@dataclass(frozen=True, slots=True)
class ConversationPatch:
    """@brief Conversation merge-patch 的领域表示 / Domain representation of a Conversation merge patch.

    @param title_supplied 请求是否包含 title；允许显式 null / Whether the request contains title; explicit null is allowed.
    @param title title 的完整目标值 / Complete target title value.
    @param status 可选完整目标状态 / Optional complete target status.
    """

    title_supplied: bool = False
    title: str | None = None
    status: ConversationStatus | None = None

    def __post_init__(self) -> None:
        """@brief 拒绝空 patch 并校验 title / Reject an empty patch and validate title."""
        if not self.title_supplied and self.title is not None:
            raise AgentDomainError("conversation title value requires title_supplied")
        if not self.title_supplied and self.status is None:
            raise AgentDomainError("conversation patch must contain at least one field")
        if self.title is not None:
            _require_text(self.title, "conversation title", minimum=0, maximum=300)


@dataclass(frozen=True, slots=True)
class Conversation:
    """@brief 可软删除的 Conversation 聚合 / Soft-deletable Conversation aggregate.

    @param meta 强 revision 资源元数据 / Strong-revision resource metadata.
    @param workspace_id 所属 Workspace / Owning Workspace.
    @param title 可空标题 / Optional title.
    @param capability 创建后不可变的能力 / Immutable capability selected at creation.
    @param status active 或 archived / Active or archived public status.
    @param deleted_at 内部软删除时间；不进入公开 Schema / Internal soft-delete instant, excluded from the public schema.
    """

    meta: ResourceMeta[ConversationId]
    workspace_id: WorkspaceId
    title: str | None
    capability: ConversationCapability
    status: ConversationStatus = ConversationStatus.ACTIVE
    deleted_at: datetime | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        """@brief 校验 Conversation identity、时间与软删除关联 / Validate identity, time, and soft-delete association."""
        _require_opaque_id(self.meta.id, "conversation id")
        _require_opaque_id(self.workspace_id, "conversation workspace id")
        if self.title is not None:
            _require_text(self.title, "conversation title", minimum=0, maximum=300)
        if self.deleted_at is not None:
            _require_aware(self.deleted_at, "conversation deleted_at")
            if self.deleted_at < self.meta.created_at or self.deleted_at > self.meta.updated_at:
                raise AgentDomainError("conversation deleted_at must lie in its resource timeline")
            if self.status is not ConversationStatus.ARCHIVED:
                raise AgentDomainError("soft-deleted conversation must be archived")

    @property
    def is_deleted(self) -> bool:
        """@brief 判断是否已软删除 / Test whether the Conversation is soft-deleted."""
        return self.deleted_at is not None

    @property
    def is_writable(self) -> bool:
        """@brief 判断能否追加 Message 或创建 Run / Test whether messages or runs may be appended."""
        return not self.is_deleted and self.status is ConversationStatus.ACTIVE

    def update(self, patch: ConversationPatch, *, at: datetime) -> Conversation:
        """@brief 原子应用一个 merge patch / Atomically apply one merge patch.

        @param patch 已校验 patch / Validated patch.
        @param at 修改时刻 / Modification instant.
        @return revision 增一的新聚合 / New aggregate with an incremented revision.
        """
        self._require_not_deleted()
        return replace(
            self,
            meta=self.meta.advance(at),
            title=patch.title if patch.title_supplied else self.title,
            status=self.status if patch.status is None else patch.status,
        )

    def soft_delete(self, *, at: datetime) -> Conversation:
        """@brief 软删除 Conversation 并阻止后续跨边界写入 / Soft-delete and prevent later bounded-context writes."""
        self._require_not_deleted()
        return replace(
            self,
            meta=self.meta.advance(at),
            status=ConversationStatus.ARCHIVED,
            deleted_at=at,
        )

    def require_writable(self) -> None:
        """@brief 要求 Conversation 仍 active 且未删除 / Require an active, non-deleted Conversation."""
        if not self.is_writable:
            raise ConversationUnavailable("conversation is not writable")

    def _require_not_deleted(self) -> None:
        """@brief 拒绝修改已软删除 Conversation / Reject mutation of a soft-deleted Conversation."""
        if self.is_deleted:
            raise ConversationUnavailable("conversation has been deleted")


@dataclass(frozen=True, slots=True)
class TextContentPart:
    """@brief 用户可见文本 content part / User-visible text content part."""

    text: str
    type: Literal["text"] = "text"

    def __post_init__(self) -> None:
        """@brief 校验公开文本长度 / Validate public-text length."""
        _require_text(self.text, "message text", minimum=1, maximum=200_000)


@dataclass(frozen=True, slots=True)
class CitationContentPart:
    """@brief Knowledge citation content part / Knowledge-citation content part."""

    citation: KnowledgeCitation
    type: Literal["citation"] = "citation"


@dataclass(frozen=True, slots=True)
class ResumeProposalContentPart:
    """@brief Resume Proposal 引用 content part / Resume-Proposal reference content part."""

    proposal_ref: ResourceRef
    type: Literal["resume_proposal"] = "resume_proposal"

    def __post_init__(self) -> None:
        """@brief 保证引用 Proposal 而非直接 Resume 写入 / Ensure a Proposal reference, never a direct Resume write."""
        if self.proposal_ref.resource_type != "resume_proposal":
            raise AgentDomainError("resume proposal content must reference resume_proposal")


type MessageContentPart = TextContentPart | CitationContentPart | ResumeProposalContentPart
"""@brief Message content 的封闭判别联合 / Closed discriminated union for Message content."""


@dataclass(frozen=True, slots=True)
class Message:
    """@brief append-only Message 实体 / Append-only Message entity.

    @param meta 创建后永远保持 revision 1 的元数据 / Metadata permanently remaining at revision one after creation.
    @param workspace_id 所属 Workspace / Owning Workspace.
    @param conversation_id 所属 Conversation / Owning Conversation.
    @param sequence 持久化端口原子分配的会话内序号 / Conversation-local sequence atomically allocated by persistence.
    @param role 消息角色 / Message role.
    @param parent_message_id 可空父消息 / Optional parent Message.
    @param content 用户可见判别联合 / User-visible discriminated content.
    @param source_run_id assistant 消息的内部 Run 绑定 / Internal Run binding for assistant messages.
    """

    meta: ResourceMeta[MessageId]
    workspace_id: WorkspaceId
    conversation_id: ConversationId
    sequence: int = field(repr=False)
    role: MessageRole
    parent_message_id: MessageId | None
    content: tuple[MessageContentPart, ...]
    source_run_id: AgentRunId | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        """@brief 校验 append-only identity、顺序和 role/content 关联 / Validate append-only identity, order, and role/content associations."""
        _require_opaque_id(self.meta.id, "message id")
        _require_opaque_id(self.workspace_id, "message workspace id")
        _require_opaque_id(self.conversation_id, "message conversation id")
        if self.parent_message_id is not None:
            _require_opaque_id(self.parent_message_id, "parent message id")
            if self.parent_message_id == self.meta.id:
                raise AgentDomainError("message cannot parent itself")
        if self.sequence < 1:
            raise AgentDomainError("message sequence must be at least one")
        if self.meta.revision != 1 or self.meta.updated_at != self.meta.created_at:
            raise AgentDomainError("append-only message metadata must remain at creation revision")
        if not 1 <= len(self.content) <= 100:
            raise AgentDomainError("message content must contain 1 to 100 parts")
        if self.role in {MessageRole.USER, MessageRole.SYSTEM_NOTICE} and any(
            not isinstance(part, TextContentPart) for part in self.content
        ):
            raise AgentDomainError("user and system-notice messages may contain only text")
        if self.role is MessageRole.ASSISTANT:
            if self.source_run_id is None:
                raise AgentDomainError("assistant message requires an exact source run")
            _require_opaque_id(self.source_run_id, "assistant source run id")
        elif self.source_run_id is not None:
            raise AgentDomainError("only assistant messages may carry a source run")


@dataclass(frozen=True, slots=True)
class AgentRunSpec:
    """@brief CreateAgentRunRequest 的不可变执行快照 / Immutable execution snapshot of CreateAgentRunRequest.

    @note 此类型只含用户输入和公开控制面；不存在 reasoning、thought 或 scratchpad 字段。
        / This type contains only user input and public controls; no reasoning, thought, or scratchpad field exists.
    """

    conversation_id: ConversationId
    input_message_id: MessageId
    capability: ConversationCapability
    context_refs: tuple[ResourceRef, ...]
    knowledge: KnowledgeSelection
    inference: InferenceIntent
    output_modes: tuple[AgentOutputMode, ...]
    response_locale: str

    def __post_init__(self) -> None:
        """@brief 校验 Run 请求的有界集合和组合约束 / Validate bounded collections and cross-field constraints."""
        _require_opaque_id(self.conversation_id, "agent run conversation id")
        _require_opaque_id(self.input_message_id, "agent run input message id")
        if len(self.context_refs) > 100:
            raise AgentDomainError("agent run context references cannot exceed 100")
        context_keys = tuple((item.resource_type, item.id, item.revision) for item in self.context_refs)
        if len(set(context_keys)) != len(context_keys):
            raise AgentDomainError("agent run context references must be unique")
        if not 1 <= len(self.output_modes) <= 3 or len(set(self.output_modes)) != len(
            self.output_modes
        ):
            raise AgentDomainError("agent run output modes must be non-empty and unique")
        if (
            AgentOutputMode.RESUME_OPERATIONS in self.output_modes
            and self.capability is not ConversationCapability.RESUME_EDIT
        ):
            raise AgentDomainError("resume operations require the resume_edit capability")
        if AgentOutputMode.RESUME_OPERATIONS in self.output_modes and (
            len(self.context_refs) != 1
            or self.context_refs[0].resource_type != "resume"
        ):
            raise AgentDomainError(
                "resume operations require exactly one Resume context"
            )
        if (
            AgentOutputMode.CITATIONS in self.output_modes
            and self.knowledge.mode is KnowledgeSelectionMode.NONE
        ):
            raise AgentDomainError("citation output requires a Knowledge selection")
        if not 2 <= len(self.response_locale) <= 35 or _LOCALE.fullmatch(self.response_locale) is None:
            raise AgentDomainError("agent response locale is invalid")


@dataclass(frozen=True, slots=True)
class AuthorizedKnowledgeContext:
    """@brief 执行时重新授权的精确 Knowledge source/version / Exact Knowledge source/version reauthorized for execution."""

    source_id: KnowledgeSourceId
    version_id: KnowledgeSourceVersionId
    policy_version: int

    def __post_init__(self) -> None:
        """@brief 校验 Knowledge context identity 与 policy 水位 / Validate Knowledge identity and policy watermark."""
        _require_opaque_id(self.source_id, "authorized knowledge source id")
        _require_opaque_id(self.version_id, "authorized knowledge version id")
        if self.policy_version < 1:
            raise AgentDomainError("knowledge policy version must be positive")


@dataclass(frozen=True, slots=True)
class AgentExecutionGrant:
    """@brief Agent/session/Knowledge/model 约束交集的执行证明 / Execution proof for the agent/session/Knowledge/model-policy intersection."""

    session_ref: ResourceRef
    agent_scope: str
    model_ref: ResourceRef
    model_region: ModelRegion
    external_model_processing: bool
    context_refs: tuple[ResourceRef, ...]
    knowledge_contexts: tuple[AuthorizedKnowledgeContext, ...]
    policy_version: int

    def __post_init__(self) -> None:
        """@brief 校验 grant 自身结构 / Validate the grant structure."""
        if self.session_ref.resource_type != "conversation" or self.session_ref.revision is None:
            raise AgentDomainError("execution grant requires an exact conversation revision")
        _require_stable_name(self.agent_scope, "execution agent scope")
        if self.model_ref.resource_type != "model" or self.model_ref.revision is None:
            raise AgentDomainError("execution grant requires an exact model revision")
        if self.policy_version < 1:
            raise AgentDomainError("execution policy version must be positive")
        if len(self.context_refs) > 100 or any(item.revision is None for item in self.context_refs):
            raise AgentDomainError("execution context must contain at most 100 exact revisions")
        if len(self.knowledge_contexts) > 200:
            raise AgentDomainError("execution knowledge context cannot exceed 200 sources")
        source_ids = tuple(item.source_id for item in self.knowledge_contexts)
        if len(set(source_ids)) != len(source_ids):
            raise AgentDomainError("execution knowledge contexts must be source-unique")

    def validate_for(self, conversation: Conversation, spec: AgentRunSpec) -> None:
        """@brief 交叉验证授权证明与精确 Run 请求 / Cross-check the grant against the exact Run request."""
        if conversation.is_deleted or conversation.status is not ConversationStatus.ACTIVE:
            raise AgentDomainError("execution grant cannot target an unavailable conversation")
        if (
            self.session_ref.id != conversation.meta.id
            or self.session_ref.revision != conversation.meta.revision
            or spec.conversation_id != conversation.meta.id
            or spec.capability is not conversation.capability
        ):
            raise AgentDomainError("execution grant does not match the conversation snapshot")
        if self.agent_scope != spec.knowledge.agent_scope:
            raise AgentDomainError("execution grant agent scope does not match KnowledgeSelection")
        if self.model_region is not spec.inference.data_region:
            raise AgentDomainError("execution model region does not match inference intent")
        if self.external_model_processing and not spec.inference.allow_external_model_processing:
            raise AgentDomainError("execution grant exceeds external-model-processing intent")
        if not _resolved_refs_match(spec.context_refs, self.context_refs):
            raise AgentDomainError("execution grant context does not exactly resolve request refs")
        selected = {item.source_id for item in self.knowledge_contexts}
        excluded = set(spec.knowledge.exclude_source_ids)
        if selected & excluded:
            raise AgentDomainError("execution grant includes an explicitly excluded source")
        if spec.knowledge.mode is KnowledgeSelectionMode.NONE and selected:
            raise AgentDomainError("none selection cannot grant Knowledge context")
        if spec.knowledge.mode is KnowledgeSelectionMode.EXPLICIT and selected != set(
            spec.knowledge.include_source_ids
        ):
            raise AgentDomainError("explicit selection must authorize every requested source")
        pins = {item.source_id: item.version_id for item in spec.knowledge.pinned_versions}
        for context in self.knowledge_contexts:
            pinned = pins.get(context.source_id)
            if pinned is not None and pinned != context.version_id:
                raise AgentDomainError("execution grant violates a pinned Knowledge version")


@dataclass(frozen=True, slots=True)
class AgentUsage:
    """@brief 契约公开的 Agent 计量 / Contract-public Agent metering."""

    input_tokens: int
    output_tokens: int
    cost_micro_usd: str

    def __post_init__(self) -> None:
        """@brief 校验非负 token 与十进制成本 / Validate non-negative tokens and decimal cost."""
        if self.input_tokens < 0 or self.output_tokens < 0:
            raise AgentDomainError("agent token usage cannot be negative")
        if not self.cost_micro_usd.isascii() or not self.cost_micro_usd.isdecimal():
            raise AgentDomainError("agent cost_micro_usd must be an unsigned decimal string")


@dataclass(frozen=True, slots=True)
class AgentKnowledgeEvidence:
    """@brief 模型可选择但不可篡改的服务端证据 / Server evidence selectable but not forgeable by the model.

    @param label 本次请求内的稳定整数标签 / Stable request-local integer label.
    @param chunk_id 私有不可变 chunk 标识 / Private immutable chunk identifier.
    @param citation 由检索器物化的公开 citation / Public citation materialized by retrieval.
    """

    label: int
    chunk_id: str = field(repr=False)
    citation: KnowledgeCitation

    def __post_init__(self) -> None:
        """@brief 校验请求内标签与 chunk 标识 / Validate request-local label and chunk identity."""
        if isinstance(self.label, bool) or not 0 <= self.label < 100:
            raise AgentDomainError("knowledge evidence label must be between zero and 99")
        _require_opaque_id(self.chunk_id, "knowledge evidence chunk id")


@dataclass(frozen=True, slots=True)
class AgentResumeContext:
    """@brief 提供给模型的精确 Resume 快照 / Exact Resume snapshot supplied to the model."""

    resume_ref: ResourceRef
    document: ResumeDocument = field(repr=False)

    def __post_init__(self) -> None:
        """@brief 保证引用与 SIR 为同一修订 / Ensure the reference and SIR identify one revision."""
        if (
            self.resume_ref.resource_type != "resume"
            or self.resume_ref.revision is None
            or self.resume_ref.id != self.document.meta.id
            or self.resume_ref.revision != self.document.meta.revision
        ):
            raise AgentDomainError("Agent Resume context is not an exact SIR snapshot")


@dataclass(frozen=True, slots=True)
class AgentResumeOperationDraft:
    """@brief 不可信模型返回的无身份 Resume operation 草案 / Identity-free untrusted Resume-operation draft.

    ``operation_id`` 与新 entity ID 必须由 Resume 边界基于 Run 稳定派生。
    / The Resume boundary derives operation and new-entity IDs deterministically from the Run.
    """

    payload: Mapping[str, JsonValue]

    def __post_init__(self) -> None:
        """@brief 冻结有界 JSON 并拒绝模型选择 operation ID / Freeze bounded JSON and reject model-selected operation IDs."""
        if "operation_id" in self.payload:
            raise AgentDomainError("provider Resume drafts cannot choose operation_id")
        frozen = _freeze_provider_json(self.payload, depth=0)
        if not isinstance(frozen, Mapping):
            raise AgentDomainError("provider Resume draft must be an object")
        object.__setattr__(self, "payload", frozen)


@dataclass(frozen=True, slots=True)
class AgentRunView:
    """@brief 严格对应公开 AgentRun Schema 的投影 / Projection matching the public AgentRun schema."""

    meta: ResourceMeta[AgentRunId]
    workspace_id: WorkspaceId
    conversation_id: ConversationId
    input_message_id: MessageId
    capability: ConversationCapability
    status: AgentRunStatus
    output_message_id: MessageId | None = None
    proposal_refs: tuple[ResourceRef, ...] = ()
    pending_approval_id: ToolApprovalId | None = None
    usage: AgentUsage | None = None
    problem: ProblemDetails | None = None

    def __post_init__(self) -> None:
        """@brief 穷尽校验 AgentRun 公开状态判别字段 / Exhaustively validate public AgentRun state associations."""
        _require_opaque_id(self.meta.id, "agent run id")
        _require_opaque_id(self.workspace_id, "agent run workspace id")
        _require_opaque_id(self.conversation_id, "agent run conversation id")
        _require_opaque_id(self.input_message_id, "agent run input message id")
        if self.output_message_id is not None:
            _require_opaque_id(self.output_message_id, "agent run output message id")
        if len(self.proposal_refs) > 100 or any(
            not _is_proposal_ref(item) for item in self.proposal_refs
        ):
            raise AgentDomainError("agent run results may contain only Proposal references")
        if self.status is AgentRunStatus.WAITING_FOR_APPROVAL:
            if self.pending_approval_id is None or self.problem is not None:
                raise AgentDomainError("waiting run requires approval and forbids a problem")
            _require_opaque_id(self.pending_approval_id, "pending approval id")
        elif self.pending_approval_id is not None:
            raise AgentDomainError("only a waiting run may expose a pending approval")
        if self.status is AgentRunStatus.FAILED:
            if self.problem is None:
                raise AgentDomainError("failed run requires ProblemDetails")
        elif self.problem is not None:
            raise AgentDomainError("only a failed run may expose ProblemDetails")
        if self.status is AgentRunStatus.SUCCEEDED and self.output_message_id is None:
            raise AgentDomainError("succeeded run requires an output message")
        if not self.status.is_terminal and (
            self.output_message_id is not None or self.proposal_refs or self.usage is not None
        ):
            raise AgentDomainError("non-terminal run cannot expose terminal results")


@dataclass(frozen=True, slots=True)
class AgentRun:
    """@brief 含私有执行快照的 AgentRun 聚合 / AgentRun aggregate with a private execution snapshot."""

    view: AgentRunView
    job_id: JobId
    created_by: UserId
    spec: AgentRunSpec = field(repr=False)
    grant: AgentExecutionGrant = field(repr=False)
    active_tool_call_id: ToolCallId | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        """@brief 校验公开投影、执行请求与 Job 绑定 / Validate public projection, execution request, and Job binding."""
        _require_opaque_id(self.job_id, "agent run job id")
        _require_opaque_id(self.created_by, "agent run creator id")
        if (
            self.view.conversation_id != self.spec.conversation_id
            or self.view.input_message_id != self.spec.input_message_id
            or self.view.capability is not self.spec.capability
        ):
            raise AgentDomainError("agent run view does not match its immutable request")
        if self.view.status is AgentRunStatus.WAITING_FOR_APPROVAL:
            if self.active_tool_call_id is None:
                raise AgentDomainError("waiting run requires an exact active tool call")
            _require_opaque_id(self.active_tool_call_id, "active tool call id")
        elif self.active_tool_call_id is not None:
            raise AgentDomainError("only a waiting run may retain an active tool call")

    @property
    def meta(self) -> ResourceMeta[AgentRunId]:
        """@brief 返回聚合强 revision 元数据 / Return aggregate strong-revision metadata."""
        return self.view.meta

    @property
    def workspace_id(self) -> WorkspaceId:
        """@brief 返回所属 Workspace / Return the owning Workspace."""
        return self.view.workspace_id

    @property
    def is_terminal(self) -> bool:
        """@brief 判断 Run 是否终态 / Test whether the Run is terminal."""
        return self.view.status.is_terminal

    def start(
        self,
        *,
        at: datetime,
        grant: AgentExecutionGrant | None = None,
    ) -> AgentRun:
        """@brief 用执行时刷新 grant 完成 queued → running / Transition queued to running with a refreshed execution grant.

        @param at 状态迁移时刻 / State-transition instant.
        @param grant 执行时重新授权得到的证明；省略时保留原证明 / Grant returned by
            execution-time reauthorization; retain the original grant when omitted.
        @return running Run / Running Run.
        """
        self._require_state(AgentRunStatus.RUNNING, {AgentRunStatus.QUEUED})
        return replace(
            self,
            view=replace(
                self.view,
                meta=self.meta.advance(at),
                status=AgentRunStatus.RUNNING,
            ),
            grant=self.grant if grant is None else grant,
        )

    def wait_for_tool(self, approval_id: ToolApprovalId, tool_call_id: ToolCallId, *, at: datetime) -> AgentRun:
        """@brief running → waiting_for_approval 并绑定精确 tool call / Wait and bind the exact tool call."""
        self._require_state(AgentRunStatus.WAITING_FOR_APPROVAL, {AgentRunStatus.RUNNING})
        _require_opaque_id(approval_id, "approval id")
        _require_opaque_id(tool_call_id, "tool call id")
        return replace(
            self,
            view=replace(
                self.view,
                meta=self.meta.advance(at),
                status=AgentRunStatus.WAITING_FOR_APPROVAL,
                pending_approval_id=approval_id,
            ),
            active_tool_call_id=tool_call_id,
        )

    def resume_after_decision(
        self,
        approval_id: ToolApprovalId,
        tool_call_id: ToolCallId,
        *,
        at: datetime,
    ) -> AgentRun:
        """@brief 精确 approval/tool call 决定后恢复 running / Resume after the exact approval/tool-call decision."""
        self._require_state(AgentRunStatus.RUNNING, {AgentRunStatus.WAITING_FOR_APPROVAL})
        if self.view.pending_approval_id != approval_id or self.active_tool_call_id != tool_call_id:
            raise ToolApprovalDecisionError("tool approval does not match the waiting run")
        return replace(
            self,
            view=replace(
                self.view,
                meta=self.meta.advance(at),
                status=AgentRunStatus.RUNNING,
                pending_approval_id=None,
            ),
            active_tool_call_id=None,
        )

    def succeed(
        self,
        output_message_id: MessageId,
        proposal_refs: Sequence[ResourceRef],
        usage: AgentUsage,
        *,
        at: datetime,
    ) -> AgentRun:
        """@brief running → succeeded，仅保存 Message/Proposal 引用 / Succeed with only Message and Proposal references."""
        self._require_state(AgentRunStatus.SUCCEEDED, {AgentRunStatus.RUNNING})
        _require_opaque_id(output_message_id, "output message id")
        return replace(
            self,
            view=replace(
                self.view,
                meta=self.meta.advance(at),
                status=AgentRunStatus.SUCCEEDED,
                output_message_id=output_message_id,
                proposal_refs=tuple(proposal_refs),
                usage=usage,
            ),
        )

    def fail(self, problem: ProblemDetails, *, at: datetime) -> AgentRun:
        """@brief 非终态 → failed / Transition a non-terminal Run to failed."""
        self._require_state(
            AgentRunStatus.FAILED,
            {
                AgentRunStatus.QUEUED,
                AgentRunStatus.RUNNING,
                AgentRunStatus.WAITING_FOR_APPROVAL,
            },
        )
        return replace(
            self,
            view=replace(
                self.view,
                meta=self.meta.advance(at),
                status=AgentRunStatus.FAILED,
                pending_approval_id=None,
                problem=problem,
            ),
            active_tool_call_id=None,
        )

    def cancel(self, *, at: datetime) -> AgentRun:
        """@brief 非终态 → cancelled / Transition a non-terminal Run to cancelled."""
        self._require_state(
            AgentRunStatus.CANCELLED,
            {
                AgentRunStatus.QUEUED,
                AgentRunStatus.RUNNING,
                AgentRunStatus.WAITING_FOR_APPROVAL,
            },
        )
        return replace(
            self,
            view=replace(
                self.view,
                meta=self.meta.advance(at),
                status=AgentRunStatus.CANCELLED,
                pending_approval_id=None,
            ),
            active_tool_call_id=None,
        )

    def _require_state(
        self,
        requested: AgentRunStatus,
        allowed: set[AgentRunStatus],
    ) -> None:
        """@brief 检查显式状态迁移边 / Check an explicit state-transition edge."""
        if self.view.status not in allowed:
            raise AgentRunTransitionError(self.view.status, requested)


@dataclass(frozen=True, slots=True)
class ToolCallBinding:
    """@brief approval 绑定的精确工具调用 / Exact tool call bound to an approval.

    @param invocation_ref 私有工具参数在安全 provider store 中的引用 / Reference to private tool arguments in a secure provider store.
    """

    tool_call_id: ToolCallId
    tool_name: str
    summary: str
    risk: ToolRisk
    expires_at: datetime
    invocation_ref: ResourceRef = field(repr=False)

    def __post_init__(self) -> None:
        """@brief 校验工具名称、摘要、期限和私有引用 / Validate tool name, summary, deadline, and private reference."""
        _require_opaque_id(self.tool_call_id, "tool call id")
        _require_stable_name(self.tool_name, "tool name")
        _require_text(self.summary, "tool summary", minimum=1, maximum=2_000)
        _require_aware(self.expires_at, "tool call expires_at")
        if self.invocation_ref.resource_type != "tool_invocation":
            raise AgentDomainError("tool call must reference a private tool_invocation")


@dataclass(frozen=True, slots=True)
class ToolApprovalView:
    """@brief 严格对应公开 ToolApproval Schema 的投影 / Projection matching the public ToolApproval schema."""

    meta: ResourceMeta[ToolApprovalId]
    workspace_id: WorkspaceId
    run_id: AgentRunId
    tool_name: str
    summary: str
    risk: ToolRisk
    status: ToolApprovalStatus
    expires_at: datetime
    decision_by: ResourceRef | None

    def __post_init__(self) -> None:
        """@brief 校验 ToolApproval 公开状态关联 / Validate public ToolApproval state associations."""
        _require_opaque_id(self.meta.id, "tool approval id")
        _require_opaque_id(self.workspace_id, "tool approval workspace id")
        _require_opaque_id(self.run_id, "tool approval run id")
        _require_stable_name(self.tool_name, "tool approval name")
        _require_text(self.summary, "tool approval summary", minimum=1, maximum=2_000)
        _require_aware(self.expires_at, "tool approval expires_at")
        if self.expires_at <= self.meta.created_at:
            raise AgentDomainError("tool approval must expire after creation")
        if self.status is ToolApprovalStatus.PENDING:
            if self.decision_by is not None:
                raise AgentDomainError("pending approval cannot have decision_by")
        elif self.decision_by is None:
            raise AgentDomainError("terminal approval requires decision_by")


@dataclass(frozen=True, slots=True)
class ToolApproval:
    """@brief 含精确 tool-call 私有绑定的 ToolApproval 聚合 / ToolApproval aggregate with an exact private tool-call binding."""

    view: ToolApprovalView
    binding: ToolCallBinding = field(repr=False)

    def __post_init__(self) -> None:
        """@brief 校验公开字段与私有调用绑定一致 / Validate public fields against the private call binding."""
        if (
            self.view.tool_name != self.binding.tool_name
            or self.view.summary != self.binding.summary
            or self.view.risk is not self.binding.risk
            or self.view.expires_at != self.binding.expires_at
        ):
            raise AgentDomainError("tool approval view does not match its exact binding")

    @property
    def meta(self) -> ResourceMeta[ToolApprovalId]:
        """@brief 返回强 revision 元数据 / Return strong-revision metadata."""
        return self.view.meta

    @property
    def workspace_id(self) -> WorkspaceId:
        """@brief 返回所属 Workspace / Return owning Workspace."""
        return self.view.workspace_id

    @classmethod
    def create(
        cls,
        meta: ResourceMeta[ToolApprovalId],
        workspace_id: WorkspaceId,
        run_id: AgentRunId,
        binding: ToolCallBinding,
    ) -> ToolApproval:
        """@brief 从 provider 工具调用创建 pending approval / Create a pending approval from a provider tool call."""
        return cls(
            ToolApprovalView(
                meta=meta,
                workspace_id=workspace_id,
                run_id=run_id,
                tool_name=binding.tool_name,
                summary=binding.summary,
                risk=binding.risk,
                status=ToolApprovalStatus.PENDING,
                expires_at=binding.expires_at,
                decision_by=None,
            ),
            binding,
        )

    def decide(self, decision: ToolDecision, actor: ResourceRef, *, at: datetime) -> ToolApproval:
        """@brief 在期限内一次性批准或拒绝 / Approve or reject exactly once before expiry."""
        self._require_pending()
        _require_aware(at, "tool approval decision time")
        if at >= self.view.expires_at:
            raise ToolApprovalDecisionError("tool approval has expired")
        status = (
            ToolApprovalStatus.APPROVED
            if decision is ToolDecision.APPROVE
            else ToolApprovalStatus.REJECTED
        )
        return replace(
            self,
            view=replace(
                self.view,
                meta=self.meta.advance(at),
                status=status,
                decision_by=actor,
            ),
        )

    def expire(self, actor: ResourceRef, *, at: datetime) -> ToolApproval:
        """@brief 到期后一次性迁移为 expired / Transition once to expired after the deadline."""
        self._require_pending()
        _require_aware(at, "tool approval expiration time")
        if at < self.view.expires_at:
            raise ToolApprovalDecisionError("tool approval has not expired")
        return replace(
            self,
            view=replace(
                self.view,
                meta=self.meta.advance(at),
                status=ToolApprovalStatus.EXPIRED,
                decision_by=actor,
            ),
        )

    def matches_waiting_run(self, run: AgentRun) -> bool:
        """@brief 验证 approval 精确匹配 waiting Run 与 tool call / Verify exact waiting-Run and tool-call binding."""
        return (
            run.view.status is AgentRunStatus.WAITING_FOR_APPROVAL
            and run.meta.id == self.view.run_id
            and run.view.pending_approval_id == self.meta.id
            and run.active_tool_call_id == self.binding.tool_call_id
            and run.workspace_id == self.workspace_id
        )

    def _require_pending(self) -> None:
        """@brief 保证 decision 只消费 pending 一次 / Ensure a decision consumes pending only once."""
        if self.view.status is not ToolApprovalStatus.PENDING:
            raise ToolApprovalDecisionError("tool approval was already decided")


@dataclass(frozen=True, slots=True)
class AgentProviderRequest:
    """@brief 发送给模型 provider 的无私有推理执行请求 / Execution request sent to a model provider without private reasoning."""

    run_id: AgentRunId
    spec: AgentRunSpec
    grant: AgentExecutionGrant
    input_message: Message
    knowledge_evidence: tuple[AgentKnowledgeEvidence, ...] = ()
    resume_context: AgentResumeContext | None = None

    def __post_init__(self) -> None:
        """@brief 校验输入消息与 Run 请求一致 / Validate input Message against the Run request."""
        if (
            self.input_message.meta.id != self.spec.input_message_id
            or self.input_message.conversation_id != self.spec.conversation_id
            or self.input_message.role is not MessageRole.USER
        ):
            raise AgentDomainError("provider request requires the exact user input message")
        labels = tuple(item.label for item in self.knowledge_evidence)
        if labels != tuple(range(len(labels))):
            raise AgentDomainError("provider evidence labels must be contiguous from zero")
        allowed_knowledge = {
            (item.source_id, item.version_id) for item in self.grant.knowledge_contexts
        }
        if any(
            (item.citation.source_id, item.citation.version_id) not in allowed_knowledge
            for item in self.knowledge_evidence
        ):
            raise AgentDomainError("provider evidence exceeds the execution grant")
        wants_resume = AgentOutputMode.RESUME_OPERATIONS in self.spec.output_modes
        if wants_resume != (self.resume_context is not None):
            raise AgentDomainError("provider Resume context does not match output modes")
        if self.resume_context is not None and self.resume_context.resume_ref not in self.grant.context_refs:
            raise AgentDomainError("provider Resume context exceeds the execution grant")


@dataclass(frozen=True, slots=True)
class AgentProviderCompleted:
    """@brief provider 返回的用户可见最终结果 / User-visible final result returned by a provider.

    @note Adapter 必须丢弃 provider 私有 reasoning，只构造这些公开字段。
        / Adapters must discard private provider reasoning and construct only these public fields.
    """

    content: tuple[MessageContentPart, ...]
    proposal_refs: tuple[ResourceRef, ...]
    usage: AgentUsage
    resume_operations: tuple[AgentResumeOperationDraft, ...] = ()
    proposal_title: str | None = None

    def __post_init__(self) -> None:
        """@brief 校验最终内容和 Proposal-only 写入 / Validate final content and Proposal-only writes."""
        if len(self.content) > 100:
            raise AgentDomainError("provider completion cannot exceed 100 public content parts")
        if len(self.proposal_refs) > 100 or any(
            not _is_proposal_ref(item) for item in self.proposal_refs
        ):
            raise AgentDomainError("provider may return only Proposal references")
        proposal_keys = tuple(
            (item.resource_type, item.id, item.revision) for item in self.proposal_refs
        )
        if len(set(proposal_keys)) != len(proposal_keys):
            raise AgentDomainError("provider Proposal references must be unique")
        public_proposals = {
            (part.proposal_ref.resource_type, part.proposal_ref.id, part.proposal_ref.revision)
            for part in self.content
            if isinstance(part, ResumeProposalContentPart)
        }
        if not public_proposals <= set(proposal_keys):
            raise AgentDomainError("message Proposal parts must be present in run proposal_refs")
        if not 0 <= len(self.resume_operations) <= 200:
            raise AgentDomainError("provider Resume drafts exceed contract bounds")
        if self.proposal_title is not None:
            _require_text(
                self.proposal_title,
                "provider proposal title",
                minimum=1,
                maximum=300,
            )
        if bool(self.resume_operations) != (self.proposal_title is not None):
            raise AgentDomainError(
                "provider Resume drafts and proposal title must appear together"
            )

    def validate_for(self, request: AgentProviderRequest) -> None:
        """@brief 对精确请求验证完整模式与证据 provenance / Validate complete modes and evidence provenance.

        @param request 包含服务端证据的精确 provider 请求 / Exact provider request
            carrying server evidence.
        @raise AgentDomainError 模型遗漏、越界或伪造结果时抛出 / Raised for
            omitted, extra, or fabricated output.
        """
        allowed = set(request.spec.output_modes)
        if self.proposal_refs or any(
            isinstance(part, ResumeProposalContentPart) for part in self.content
        ):
            raise AgentDomainError(
                "provider cannot inject Proposal references before server materialization"
            )
        text_count = sum(isinstance(part, TextContentPart) for part in self.content)
        citations = tuple(
            part.citation
            for part in self.content
            if isinstance(part, CitationContentPart)
        )
        for part in self.content:
            if isinstance(part, TextContentPart) and AgentOutputMode.TEXT not in allowed:
                raise AgentDomainError("provider returned unrequested text")
            if isinstance(part, CitationContentPart) and AgentOutputMode.CITATIONS not in allowed:
                raise AgentDomainError("provider returned unrequested citations")
            if (
                isinstance(part, ResumeProposalContentPart)
                and AgentOutputMode.RESUME_OPERATIONS not in allowed
            ):
                raise AgentDomainError("provider returned unrequested Resume proposals")
        if self.proposal_refs and AgentOutputMode.RESUME_OPERATIONS not in allowed:
            raise AgentDomainError("provider returned unrequested Proposal references")
        if (AgentOutputMode.TEXT in allowed) != (text_count == 1):
            raise AgentDomainError("provider must return exactly one requested text output")
        if (AgentOutputMode.CITATIONS in allowed) != bool(citations):
            raise AgentDomainError("provider must return every requested citation output")
        if len(set(citations)) != len(citations):
            raise AgentDomainError("provider citation selections must be unique")
        evidence = {item.citation for item in request.knowledge_evidence}
        if not set(citations) <= evidence:
            raise AgentDomainError("provider returned a citation outside server evidence")
        has_resume = bool(self.resume_operations)
        if (AgentOutputMode.RESUME_OPERATIONS in allowed) != has_resume:
            raise AgentDomainError("provider must return every requested Resume output")
        if has_resume and len(self.content) >= 100:
            raise AgentDomainError(
                "provider content leaves no room for the server Proposal reference"
            )

    def validate_modes(self, modes: tuple[AgentOutputMode, ...]) -> None:
        """@brief 保留给纯领域测试的越界检查 / Retain an over-production check for pure-domain callers.

        @note worker 必须使用 ``validate_for`` 才能同时校验遗漏模式与证据来源。
            / Workers use ``validate_for`` to check omissions and evidence provenance as well.
        """
        allowed = set(modes)
        for part in self.content:
            if isinstance(part, TextContentPart) and AgentOutputMode.TEXT not in allowed:
                raise AgentDomainError("provider returned unrequested text")
            if isinstance(part, CitationContentPart) and AgentOutputMode.CITATIONS not in allowed:
                raise AgentDomainError("provider returned unrequested citations")
            if isinstance(part, ResumeProposalContentPart):
                raise AgentDomainError("provider cannot directly return Resume Proposal refs")
        if self.resume_operations and AgentOutputMode.RESUME_OPERATIONS not in allowed:
            raise AgentDomainError("provider returned unrequested Resume operations")


@dataclass(frozen=True, slots=True)
class AgentProviderApprovalRequired:
    """@brief provider 暂停并请求精确工具批准 / Provider paused for an exact tool approval."""

    binding: ToolCallBinding


type AgentProviderOutcome = AgentProviderCompleted | AgentProviderApprovalRequired
"""@brief 模型 provider 的封闭结果联合 / Closed model-provider outcome union."""


@dataclass(frozen=True, slots=True)
class AgentRunQueuedDispatch:
    """@brief 同事务写入统一 outbox 的 Run 排队信号 / Run-queued signal written to the unified outbox transactionally."""

    id: AgentOutboxId
    workspace_id: WorkspaceId
    actor_id: UserId
    run_ref: ResourceRef
    job_ref: ResourceRef
    occurred_at: datetime
    kind: Literal["agent.run.queued"] = "agent.run.queued"

    def __post_init__(self) -> None:
        """@brief 校验排队信号身份 / Validate queued-dispatch identity."""
        _validate_dispatch(
            self.id,
            self.workspace_id,
            self.actor_id,
            self.run_ref,
            self.job_ref,
            self.occurred_at,
        )

    def as_payload(self) -> Mapping[str, JsonValue]:
        """@brief 生成固定白名单 JSON payload / Build a fixed-allowlist JSON payload."""
        return MappingProxyType(
            {
                "actor_id": self.actor_id,
                "run_id": self.run_ref.id,
                "job_id": self.job_ref.id,
            }
        )


@dataclass(frozen=True, slots=True)
class AgentRunCancellationDispatch:
    """@brief 同事务写入统一 outbox 的取消信号 / Cancellation signal written to the unified outbox transactionally."""

    id: AgentOutboxId
    workspace_id: WorkspaceId
    actor_id: UserId
    run_ref: ResourceRef
    job_ref: ResourceRef
    occurred_at: datetime
    kind: Literal["agent.run.cancelled"] = "agent.run.cancelled"

    def __post_init__(self) -> None:
        """@brief 校验取消信号身份 / Validate cancellation-dispatch identity."""
        _validate_dispatch(
            self.id,
            self.workspace_id,
            self.actor_id,
            self.run_ref,
            self.job_ref,
            self.occurred_at,
        )

    def as_payload(self) -> Mapping[str, JsonValue]:
        """@brief 生成固定白名单 JSON payload / Build a fixed-allowlist JSON payload."""
        return MappingProxyType(
            {
                "actor_id": self.actor_id,
                "run_id": self.run_ref.id,
                "job_id": self.job_ref.id,
            }
        )


@dataclass(frozen=True, slots=True)
class AgentRunStateDispatch:
    """@brief worker 提交状态后写入的统一变化信号 / Unified change signal written after a worker state commit.

    @param id outbox 标识 / Outbox identifier.
    @param workspace_id 所属 Workspace / Owning Workspace.
    @param run_ref 精确 Run revision / Exact Run revision.
    @param job_ref 对齐的统一 Job revision / Aligned unified Job revision.
    @param status 已提交公开状态 / Committed public status.
    @param occurred_at 提交时刻 / Commit instant.
    """

    id: AgentOutboxId
    workspace_id: WorkspaceId
    actor_id: UserId
    run_ref: ResourceRef
    job_ref: ResourceRef
    status: AgentRunStatus
    occurred_at: datetime
    kind: Literal["agent.run.updated"] = "agent.run.updated"

    def __post_init__(self) -> None:
        """@brief 校验 Run/Job 引用与状态 / Validate Run/Job references and status."""
        _validate_dispatch(
            self.id,
            self.workspace_id,
            self.actor_id,
            self.run_ref,
            self.job_ref,
            self.occurred_at,
        )

    def as_payload(self) -> Mapping[str, JsonValue]:
        """@brief 生成公开安全状态提示 / Build a public-safe state hint."""
        return MappingProxyType(
            {
                "run_id": self.run_ref.id,
                "job_id": self.job_ref.id,
                "actor_id": self.actor_id,
                "status": self.status.value,
            }
        )


@dataclass(frozen=True, slots=True)
class ToolDecisionDispatch:
    """@brief approval 决定后提交的精确工具恢复信号 / Exact tool-resume signal committed after an approval decision."""

    id: AgentOutboxId
    workspace_id: WorkspaceId
    actor_id: UserId
    run_ref: ResourceRef
    job_ref: ResourceRef
    approval_ref: ResourceRef
    tool_call_id: ToolCallId
    decision: ToolDecision
    occurred_at: datetime
    kind: Literal["agent.tool_decision.recorded"] = "agent.tool_decision.recorded"

    def __post_init__(self) -> None:
        """@brief 校验 run/job/approval/tool-call 精确绑定 / Validate exact run/job/approval/tool-call binding."""
        _validate_dispatch(
            self.id,
            self.workspace_id,
            self.actor_id,
            self.run_ref,
            self.job_ref,
            self.occurred_at,
        )
        if (
            self.approval_ref.resource_type != "tool_approval"
            or self.run_ref.revision is None
            or self.job_ref.revision is None
            or self.approval_ref.revision is None
        ):
            raise AgentDomainError("tool decision dispatch requires a tool_approval reference")
        _require_opaque_id(self.tool_call_id, "tool decision call id")

    def as_payload(self) -> Mapping[str, JsonValue]:
        """@brief 生成不含工具参数或私有推理的白名单 payload / Build an allowlisted payload without arguments or private reasoning."""
        return MappingProxyType(
            {
                "run_id": self.run_ref.id,
                "run_revision": self.run_ref.revision,
                "job_id": self.job_ref.id,
                "job_revision": self.job_ref.revision,
                "actor_id": self.actor_id,
                "approval_id": self.approval_ref.id,
                "approval_revision": self.approval_ref.revision,
                "tool_call_id": self.tool_call_id,
                "decision": self.decision.value,
            }
        )


@dataclass(frozen=True, slots=True)
class ToolApprovalExpiredDispatch:
    """@brief 到期 approval 终止 Run 的统一 outbox 信号 / Unified outbox signal terminating a Run after approval expiry."""

    id: AgentOutboxId
    workspace_id: WorkspaceId
    actor_id: UserId
    run_ref: ResourceRef
    job_ref: ResourceRef
    approval_ref: ResourceRef
    tool_call_id: ToolCallId
    occurred_at: datetime
    kind: Literal["agent.tool_approval.expired"] = "agent.tool_approval.expired"

    def __post_init__(self) -> None:
        """@brief 校验 expired 信号精确绑定 / Validate exact binding of an expiry signal."""
        _validate_dispatch(
            self.id,
            self.workspace_id,
            self.actor_id,
            self.run_ref,
            self.job_ref,
            self.occurred_at,
        )
        if self.approval_ref.resource_type != "tool_approval":
            raise AgentDomainError("approval expiry dispatch requires a tool_approval reference")
        _require_opaque_id(self.tool_call_id, "expired tool call id")

    def as_payload(self) -> Mapping[str, JsonValue]:
        """@brief 生成不含工具参数的固定白名单 payload / Build a fixed-allowlist payload without tool arguments."""
        return MappingProxyType(
            {
                "run_id": self.run_ref.id,
                "job_id": self.job_ref.id,
                "actor_id": self.actor_id,
                "approval_id": self.approval_ref.id,
                "tool_call_id": self.tool_call_id,
            }
        )


type AgentOutboxRecord = (
    AgentRunQueuedDispatch
    | AgentRunCancellationDispatch
    | AgentRunStateDispatch
    | ToolDecisionDispatch
    | ToolApprovalExpiredDispatch
)
"""@brief 允许进入 Agent outbox 的封闭记录联合 / Closed record union allowed into the Agent outbox."""


def validate_run_job_alignment(run: AgentRun, job: Job) -> None:
    """@brief 验证 AgentRun 与统一 platform Job 的身份和状态对齐 / Validate identity and status alignment with the unified platform Job.

    @param run AgentRun 聚合 / AgentRun aggregate.
    @param job 复用的 platform Job / Reused platform Job.
    @raise AgentDomainError Job 不属于该 Run 或状态漂移时抛出 / Raised for ownership or state drift.
    """
    if (
        job.meta.id != run.job_id
        or job.workspace_id != run.workspace_id
        or job.kind != AGENT_RUN_JOB_KIND
        or job.subject.resource_type != "agent_run"
        or job.subject.id != run.meta.id
    ):
        raise AgentDomainError("agent run is not bound to the supplied unified Job")
    expected = {
        AgentRunStatus.QUEUED: JobStatus.QUEUED,
        AgentRunStatus.RUNNING: JobStatus.RUNNING,
        AgentRunStatus.WAITING_FOR_APPROVAL: JobStatus.RUNNING,
        AgentRunStatus.SUCCEEDED: JobStatus.SUCCEEDED,
        AgentRunStatus.FAILED: JobStatus.FAILED,
        AgentRunStatus.CANCELLED: JobStatus.CANCELLED,
    }[run.view.status]
    if job.status is not expected:
        raise AgentDomainError("agent run and unified Job statuses are inconsistent")


def _validate_dispatch(
    dispatch_id: AgentOutboxId,
    workspace_id: WorkspaceId,
    actor_id: UserId,
    run_ref: ResourceRef,
    job_ref: ResourceRef,
    occurred_at: datetime,
) -> None:
    """@brief 校验通用 Agent dispatch identity / Validate common Agent-dispatch identity."""
    _require_opaque_id(dispatch_id, "agent outbox id")
    _require_opaque_id(workspace_id, "agent outbox workspace id")
    _require_opaque_id(actor_id, "agent outbox actor id")
    _require_aware(occurred_at, "agent outbox occurred_at")
    if run_ref.resource_type != "agent_run" or job_ref.resource_type != "job":
        raise AgentDomainError("agent outbox requires agent_run and job references")


def _resolved_refs_match(requested: Sequence[ResourceRef], granted: Sequence[ResourceRef]) -> bool:
    """@brief 比较请求引用与解析后的精确 revision / Compare requested refs with exact resolved revisions."""
    if len(requested) != len(granted):
        return False
    requested_by_key = {(item.resource_type, item.id): item.revision for item in requested}
    granted_by_key = {(item.resource_type, item.id): item.revision for item in granted}
    if requested_by_key.keys() != granted_by_key.keys() or any(
        revision is None for revision in granted_by_key.values()
    ):
        return False
    return all(
        revision is None or granted_by_key[key] == revision
        for key, revision in requested_by_key.items()
    )


def _is_proposal_ref(reference: ResourceRef) -> bool:
    """@brief 判断引用是否为显式 Proposal 而非权威资源 / Test whether a ref is an explicit Proposal rather than an authoritative resource."""
    return reference.resource_type == "resume_proposal"


def _freeze_provider_json(value: object, *, depth: int) -> JsonValue:
    """@brief 深度冻结有界 provider JSON / Deep-freeze bounded provider JSON.

    @param value 不可信 JSON 值 / Untrusted JSON value.
    @param depth 当前嵌套深度 / Current nesting depth.
    @return 深度不可变 JSON / Deeply immutable JSON.
    @raise AgentDomainError 类型、大小或深度越界时抛出 / Raised for invalid
        type, size, or depth.
    """
    if depth > 20:
        raise AgentDomainError("provider JSON exceeds the maximum depth")
    if value is None or isinstance(value, (bool, str)):
        if isinstance(value, str) and len(value) > 200_000:
            raise AgentDomainError("provider JSON string exceeds bounds")
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        if not (-1.0e308 < value < 1.0e308):
            raise AgentDomainError("provider JSON number is not finite")
        return value
    if isinstance(value, Mapping):
        if len(value) > 500:
            raise AgentDomainError("provider JSON object exceeds bounds")
        frozen: dict[str, JsonValue] = {}
        for key, item in value.items():
            if not isinstance(key, str) or not key or len(key) > 200:
                raise AgentDomainError("provider JSON object key is invalid")
            frozen[key] = _freeze_provider_json(item, depth=depth + 1)
        return MappingProxyType(frozen)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        if len(value) > 1_000:
            raise AgentDomainError("provider JSON array exceeds bounds")
        return tuple(_freeze_provider_json(item, depth=depth + 1) for item in value)
    raise AgentDomainError("provider output contains a non-JSON value")


def _require_opaque_id(value: str, label: str) -> None:
    """@brief 校验 API v2 opaque ID / Validate an API v2 opaque ID."""
    if _OPAQUE_ID.fullmatch(value) is None:
        raise AgentDomainError(f"{label} is not a valid opaque identifier")


def _require_stable_name(value: str, label: str) -> None:
    """@brief 校验稳定小写名称 / Validate a stable lowercase name."""
    if _STABLE_NAME.fullmatch(value) is None:
        raise AgentDomainError(f"{label} is not a stable name")


def _require_text(value: str, label: str, *, minimum: int, maximum: int) -> None:
    """@brief 校验公开文本长度和控制字符 / Validate public-text length and control characters."""
    if not minimum <= len(value) <= maximum or any(
        ord(char) < 32 and char not in "\n\r\t" for char in value
    ):
        raise AgentDomainError(f"{label} violates contract bounds")


def _require_aware(value: datetime, label: str) -> None:
    """@brief 校验带时区 datetime / Validate a timezone-aware datetime."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise AgentDomainError(f"{label} must be timezone-aware")


__all__ = [
    "AGENT_RUN_JOB_KIND",
    "AgentDomainError",
    "AgentExecutionGrant",
    "AgentKnowledgeEvidence",
    "AgentOutboxId",
    "AgentOutboxRecord",
    "AgentOutputMode",
    "AgentProviderApprovalRequired",
    "AgentProviderCompleted",
    "AgentProviderOutcome",
    "AgentProviderRequest",
    "AgentResumeContext",
    "AgentResumeOperationDraft",
    "AgentRun",
    "AgentRunCancellationDispatch",
    "AgentRunId",
    "AgentRunQueuedDispatch",
    "AgentRunSpec",
    "AgentRunStateDispatch",
    "AgentRunStatus",
    "AgentRunTransitionError",
    "AgentRunView",
    "AgentUsage",
    "AuthorizedKnowledgeContext",
    "CitationContentPart",
    "Conversation",
    "ConversationCapability",
    "ConversationId",
    "ConversationPatch",
    "ConversationStatus",
    "ConversationUnavailable",
    "Message",
    "MessageContentPart",
    "MessageId",
    "MessageRole",
    "ResumeProposalContentPart",
    "TextContentPart",
    "ToolApproval",
    "ToolApprovalDecisionError",
    "ToolApprovalExpiredDispatch",
    "ToolApprovalId",
    "ToolApprovalStatus",
    "ToolApprovalView",
    "ToolCallBinding",
    "ToolCallId",
    "ToolDecision",
    "ToolDecisionDispatch",
    "ToolRisk",
    "validate_run_job_alignment",
]
