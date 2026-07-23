"""@brief API v2 Conversation 与 Agent 应用 Ports / API v2 Conversation and Agent application ports.

Repository 签名始终先接收 ``workspace_id``，并显式携带 ``for_update``、旧 revision、
原子 Message sequence 分配和稳定 keyset。外部模型与工具 Ports 故意不属于 UoW，令网络
调用只能发生在数据库事务退出之后。PostgreSQL adapter 必须让本 UoW 加入外层 v2
幂等事务，使领域写入、统一 Job、统一 outbox、审计与逐字 receipt 一起提交。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from types import TracebackType
from typing import Protocol, Self

from backend.domain.agent_v2 import (
    AgentExecutionGrant,
    AgentKnowledgeEvidence,
    AgentOutboxId,
    AgentOutboxRecord,
    AgentProviderOutcome,
    AgentProviderRequest,
    AgentResumeContext,
    AgentResumeOperationDraft,
    AgentRun,
    AgentRunId,
    AgentRunSpec,
    Conversation,
    ConversationId,
    Message,
    MessageId,
    ToolApproval,
    ToolApprovalId,
    ToolCallBinding,
    ToolCallId,
    ToolDecision,
)
from backend.domain.knowledge_sources import ModelRegion
from backend.domain.platform import AuditEvent, Job, JobId, ProblemDetails
from backend.domain.principals import TokenPrincipal, UserId, WorkspaceId
from backend.domain.resources import ResourceRef


class AgentCasMismatch(RuntimeError):
    """@brief revision 条件写没有精确影响一行 / Revision CAS did not affect exactly one row."""


class AgentPolicyDenied(RuntimeError):
    """@brief Agent/session/Knowledge/model 约束交集拒绝执行 / Agent/session/Knowledge/model intersection denied execution."""


class AgentProviderFailure(RuntimeError):
    """@brief 模型调用的公开安全失败 / Public-safe model-provider failure.

    @param problem 可持久化到 Run/Job 的 ProblemDetails / Problem Details safe to persist on
        the Run and Job.
    """

    problem: ProblemDetails
    """@brief 已脱敏且可持久化的问题 / Redacted persistable problem."""

    def __init__(self, problem: ProblemDetails) -> None:
        """@brief 初始化 provider 失败 / Initialize a provider failure.

        @param problem 已脱敏 ProblemDetails / Redacted Problem Details.
        """
        super().__init__(problem.code)
        self.problem = problem


class AgentProposalFailure(RuntimeError):
    """@brief Resume Proposal 物化的公开安全失败 / Public-safe Resume-Proposal materialization failure."""

    problem: ProblemDetails
    """@brief 可持久化问题 / Persistable problem."""

    def __init__(self, problem: ProblemDetails) -> None:
        """@brief 初始化 Proposal 边界失败 / Initialize a Proposal-boundary failure."""
        super().__init__(problem.code)
        self.problem = problem


class AgentPermission(StrEnum):
    """@brief 5.4 每个用例的独立精确权限 / Independent exact permissions for section 5.4 use cases.

    @note 这些值由一个集中 adapter 映射到 ``AccessAuthorizer``；业务服务不得临时复用
        ``WorkspaceAction.READ/UPDATE``。/ A single adapter maps these values to
        ``AccessAuthorizer``; business services must not borrow coarse Workspace actions.
    """

    LIST_CONVERSATIONS = "conversation.list"
    CREATE_CONVERSATION = "conversation.create"
    READ_CONVERSATION = "conversation.read"
    UPDATE_CONVERSATION = "conversation.update"
    DELETE_CONVERSATION = "conversation.delete"
    LIST_MESSAGES = "conversation.messages.list"
    CREATE_MESSAGE = "conversation.messages.create"
    CREATE_AGENT_RUN = "agent_run.create"
    READ_AGENT_RUN = "agent_run.read"
    CANCEL_AGENT_RUN = "agent_run.cancel"
    READ_TOOL_APPROVAL = "tool_approval.read"
    DECIDE_TOOL_APPROVAL = "tool_approval.decide"


@dataclass(frozen=True, slots=True)
class AgentPermissionRequest:
    """@brief 一次路由级精确权限请求 / One route-level exact permission request.

    @param workspace_id 路径 Workspace / Path Workspace.
    @param permission 唯一请求权限 / Sole requested permission.
    @param target 精确 Workspace 或资源目标 / Exact Workspace or resource target.
    """

    workspace_id: WorkspaceId
    permission: AgentPermission
    target: ResourceRef

    def __post_init__(self) -> None:
        """@brief 校验 permission 与 target 类型关联 / Validate permission-to-target association."""
        expected = {
            AgentPermission.LIST_CONVERSATIONS: "workspace",
            AgentPermission.CREATE_CONVERSATION: "workspace",
            AgentPermission.READ_CONVERSATION: "conversation",
            AgentPermission.UPDATE_CONVERSATION: "conversation",
            AgentPermission.DELETE_CONVERSATION: "conversation",
            AgentPermission.LIST_MESSAGES: "conversation",
            AgentPermission.CREATE_MESSAGE: "conversation",
            AgentPermission.CREATE_AGENT_RUN: "conversation",
            AgentPermission.READ_AGENT_RUN: "agent_run",
            AgentPermission.CANCEL_AGENT_RUN: "agent_run",
            AgentPermission.READ_TOOL_APPROVAL: "tool_approval",
            AgentPermission.DECIDE_TOOL_APPROVAL: "tool_approval",
        }[self.permission]
        if self.target.resource_type != expected:
            raise ValueError("agent permission target does not match the exact permission")
        if expected == "workspace" and self.target.id != self.workspace_id:
            raise ValueError("workspace permission target must equal the path workspace")


@dataclass(frozen=True, slots=True)
class AgentPermissionGrant:
    """@brief 集中 authorizer 返回的精确授权证明 / Exact grant returned by the central authorizer."""

    actor_id: UserId
    request: AgentPermissionRequest


@dataclass(frozen=True, slots=True)
class AgentPageRequest:
    """@brief 绑定 principal/Workspace/filter 后的内部 keyset 请求 / Internal keyset request after principal/Workspace/filter binding."""

    limit: int = 50
    after: str | None = None

    def __post_init__(self) -> None:
        """@brief 校验契约分页边界 / Validate contract pagination bounds."""
        if isinstance(self.limit, bool) or not 1 <= self.limit <= 200:
            raise ValueError("agent page limit must be between one and 200")
        if self.after is not None and (not self.after or len(self.after) > 2_048):
            raise ValueError("agent keyset position must be non-empty and at most 2048 characters")


@dataclass(frozen=True, slots=True)
class AgentPage[ItemT]:
    """@brief 稳定排序的 keyset 页面 / Stably ordered keyset page."""

    items: tuple[ItemT, ...]
    next_position: str | None


@dataclass(frozen=True, slots=True)
class MessageSequenceReservation:
    """@brief 持久化端口原子保留的 Message sequence / Message sequence atomically reserved by persistence.

    @param sequence Conversation 内严格递增序号 / Strictly increasing Conversation-local sequence.
    @param conversation_revision sequence 分配后 Conversation revision / Conversation revision after allocation.
    @param updated_at 与 revision 一起提交的时刻 / Instant committed with the revision.
    """

    sequence: int
    conversation_revision: int
    updated_at: datetime

    def __post_init__(self) -> None:
        """@brief 校验 reservation 正值 / Validate positive reservation values."""
        if self.sequence < 1 or self.conversation_revision < 2:
            raise ValueError("message sequence reservation is invalid")


@dataclass(frozen=True, slots=True)
class AgentRunPolicyRequest:
    """@brief 交给本地策略引擎的完整 Run 授权请求 / Complete Run authorization request for the local policy engine."""

    actor_id: UserId
    workspace_id: WorkspaceId
    conversation: Conversation
    input_message: Message
    spec: AgentRunSpec


@dataclass(frozen=True, slots=True)
class AgentModelRoute:
    """@brief 策略可选的不可变模型路由 / Immutable model route selectable by policy.

    @param model_ref 带精确 revision 的模型引用 / Exact revision-bearing model reference.
    @param data_region 模型处理区域 / Model processing region.
    @param external_processing 是否越出 Workspace 托管边界 / Whether processing leaves the hosted Workspace boundary.
    """

    model_ref: ResourceRef
    data_region: ModelRegion
    external_processing: bool

    def __post_init__(self) -> None:
        """@brief 要求精确 model revision / Require an exact model revision."""
        if self.model_ref.resource_type != "model" or self.model_ref.revision is None:
            raise ValueError("agent model route requires an exact model revision")


class AgentContextResolver(Protocol):
    """@brief 可组合、失败关闭的跨域上下文解析 Port / Composable fail-closed cross-domain context resolver."""

    async def resolve(
        self,
        workspace_id: WorkspaceId,
        references: tuple[ResourceRef, ...],
    ) -> tuple[ResourceRef, ...]:
        """@brief 批量解析并验证 Workspace/revision / Batch-resolve and validate Workspace/revision.

        @note 未知 resource type、不可用的跨域真相或不属于 Workspace 的引用必须拒绝，
            不得跳过校验 / Unknown resource types, unavailable cross-domain truth, and
            references outside the Workspace must be rejected rather than silently skipped.
        """


@dataclass(frozen=True, slots=True)
class ToolExecutionReceipt:
    """@brief 外部工具完成后仅保留的公开安全摘要 / Public-safe receipt retained after external tool execution.

    @note 工具原始参数、secret 和私有模型推理不属于此类型。
        / Raw tool arguments, secrets, and private model reasoning are absent from this type.
    """

    tool_call_id: ToolCallId
    summary: str
    result_refs: tuple[ResourceRef, ...]

    def __post_init__(self) -> None:
        """@brief 校验工具回执有界 / Validate bounded tool receipt."""
        if not 1 <= len(self.summary) <= 2_000 or len(self.result_refs) > 50:
            raise ValueError("tool execution receipt violates bounds")


@dataclass(frozen=True, slots=True)
class AgentKnowledgeRetrievalRequest:
    """@brief 仅在刷新 grant 内执行的 Agent Knowledge 检索 / Agent Knowledge retrieval confined to a refreshed grant."""

    workspace_id: WorkspaceId
    actor_id: UserId
    grant: AgentExecutionGrant
    query: str
    top_k: int

    def __post_init__(self) -> None:
        """@brief 校验查询与结果上限 / Validate query and result bounds."""
        if not 1 <= len(self.query) <= 8_000 or not self.query.strip():
            raise ValueError("Agent Knowledge query must contain one to 8000 characters")
        if isinstance(self.top_k, bool) or not 1 <= self.top_k <= 100:
            raise ValueError("Agent Knowledge top_k must be between one and 100")


class AgentKnowledgeRetriever(Protocol):
    """@brief 对刷新 execution grant 执行真实检索的窄 Port / Narrow real-retrieval port over a refreshed execution grant."""

    async def retrieve(
        self,
        request: AgentKnowledgeRetrievalRequest,
    ) -> tuple[AgentKnowledgeEvidence, ...]:
        """@brief 返回带请求内 label 的服务端证据 / Return server evidence with request-local labels.

        @note embedding/search I/O 必须发生在 Agent UoW 之外 / Embedding and search I/O
            must occur outside the Agent UoW.
        """


@dataclass(frozen=True, slots=True)
class AgentResumeProposalCommand:
    """@brief 在 Agent 最终事务中物化 Proposal 的强类型命令 / Typed command materializing a Proposal in the Agent final transaction."""

    workspace_id: WorkspaceId
    actor_id: UserId
    run_id: AgentRunId
    base: AgentResumeContext
    title: str
    operations: tuple[AgentResumeOperationDraft, ...]
    evidence: tuple[AgentKnowledgeEvidence, ...]
    created_at: datetime


class AgentResumeProposalBoundary(Protocol):
    """@brief 绑定当前 Agent 事务的 Resume Proposal 应用边界 / Resume Proposal application boundary bound to the current Agent transaction."""

    async def load_base(
        self,
        workspace_id: WorkspaceId,
        resume_ref: ResourceRef,
    ) -> AgentResumeContext:
        """@brief 读取精确、未删除的 Resume SIR / Load an exact non-deleted Resume SIR."""

    async def create(self, command: AgentResumeProposalCommand) -> ResourceRef:
        """@brief 幂等创建可审核 Proposal，不写 Resume / Idempotently create a reviewable Proposal without writing Resume."""


class AgentToolRegistry(Protocol):
    """@brief 服务端 tool allowlist 与当前请求语义门禁 / Server tool allowlist and current-request semantic gate."""

    def allows(self, request: AgentProviderRequest, binding: ToolCallBinding) -> bool:
        """@brief 仅对已注册、已授权且语义兼容的调用返回 true / Return true only for a registered, authorized, semantically compatible call."""


class AgentPermissionAuthorizer(Protocol):
    """@brief 独立精确 Agent 权限 Port / Independent exact Agent-permission port."""

    async def authorize(
        self,
        principal: TokenPrincipal,
        request: AgentPermissionRequest,
    ) -> AgentPermissionGrant:
        """@brief 验证 token scope、membership 与资源动作 / Verify token scope, membership, and resource action.

        @note 生产 adapter 必须用一个穷尽映射集中调用既有 ``AccessAuthorizer``，未知权限
            fail closed；不得在各用例散落粗粒度 WorkspaceAction。/ The production adapter
            must use one exhaustive mapping into ``AccessAuthorizer`` and fail closed on unknown
            permissions, rather than scattering coarse Workspace actions across use cases.
        """


class AgentRunPolicy(Protocol):
    """@brief 本地 Agent/session/Knowledge/model 策略交集 Port / Local Agent/session/Knowledge/model policy-intersection port."""

    async def authorize_run(self, request: AgentRunPolicyRequest) -> AgentExecutionGrant:
        """@brief 解析并授权精确执行上下文 / Resolve and authorize the exact execution context.

        @note 实现必须验证 context ref 的 Workspace/revision、Knowledge pin/visibility、agent
            scope、session selection、model data region 和 external-processing 标志；这是本地
            DB/policy 操作，不得发起 provider 网络调用。/ Implementations verify context
            ownership/revisions, Knowledge pins/visibility, agent scope, session selection, model
            data region, and external-processing flags locally without provider network I/O.
        """


class AgentRepository(Protocol):
    """@brief Workspace-first、CAS 与 row-lock 友好的 5.4 repository / Workspace-first section-5.4 repository supporting CAS and row locks."""

    async def list_conversations(
        self,
        workspace_id: WorkspaceId,
        page: AgentPageRequest,
    ) -> AgentPage[Conversation]:
        """@brief 按 ``(created_at,id)`` 稳定列出未删除 Conversation / List non-deleted Conversations by stable ``(created_at,id)`` keyset."""

    async def get_conversation(
        self,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        *,
        for_update: bool = False,
        include_deleted: bool = False,
    ) -> Conversation | None:
        """@brief 在 Workspace 内读取 Conversation / Read a Conversation inside one Workspace."""

    async def add_conversation(self, conversation: Conversation) -> None:
        """@brief 添加 Conversation / Add a Conversation."""

    async def save_conversation(
        self,
        conversation: Conversation,
        *,
        expected_revision: int,
    ) -> None:
        """@brief 以旧 revision CAS 保存更新或软删除 / CAS an update or soft delete using the old revision."""

    async def has_nonterminal_runs(
        self,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
    ) -> bool:
        """@brief 判断软删除前是否仍有非终态 Run / Test for non-terminal Runs before soft deletion."""

    async def list_messages(
        self,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        page: AgentPageRequest,
    ) -> AgentPage[Message]:
        """@brief 按 ``(sequence,id)`` 稳定列出 append-only Message / List append-only Messages by ``(sequence,id)`` keyset."""

    async def get_message(
        self,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        message_id: MessageId,
    ) -> Message | None:
        """@brief 以 Workspace+Conversation+Message 三元组读取 / Read by Workspace+Conversation+Message tuple."""

    async def allocate_message_sequence(
        self,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        *,
        expected_conversation_revision: int | None,
        at: datetime,
    ) -> MessageSequenceReservation:
        """@brief 原子递增 sequence 与 Conversation revision / Atomically increment sequence and Conversation revision.

        @note PostgreSQL adapter 应使用单条条件 ``UPDATE ... RETURNING`` 或等价行锁操作；
            非空 expected revision 用于 HTTP If-Match，空值仅供已锁定的内部 worker append。
            / PostgreSQL adapters use one conditional ``UPDATE ... RETURNING`` or equivalent;
            a non-null expected revision represents HTTP If-Match, while null is only for an
            internally locked worker append.
        """

    async def add_message(self, message: Message) -> None:
        """@brief 添加创建后永不更新的 Message / Add a Message that is never updated after creation."""

    async def list_runs_for_conversation(
        self,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        page: AgentPageRequest,
    ) -> AgentPage[AgentRun]:
        """@brief 内部稳定列出 Conversation Runs / Internally list Conversation Runs by stable keyset."""

    async def get_run(
        self,
        workspace_id: WorkspaceId,
        run_id: AgentRunId,
        *,
        for_update: bool = False,
    ) -> AgentRun | None:
        """@brief 在 Workspace 内读取 Run / Read a Run inside one Workspace."""

    async def add_run(self, run: AgentRun) -> None:
        """@brief 添加含私有安全执行快照的 Run / Add a Run with its private safe execution snapshot."""

    async def save_run(self, run: AgentRun, *, expected_revision: int) -> None:
        """@brief 以旧 revision CAS 保存 Run 状态 / CAS Run state using the old revision."""

    async def get_approval(
        self,
        workspace_id: WorkspaceId,
        approval_id: ToolApprovalId,
        *,
        for_update: bool = False,
    ) -> ToolApproval | None:
        """@brief 在 Workspace 内读取 approval / Read an approval inside one Workspace."""

    async def add_approval(self, approval: ToolApproval) -> None:
        """@brief 添加精确绑定 tool call 的 approval / Add an approval exactly bound to a tool call."""

    async def save_approval(
        self,
        approval: ToolApproval,
        *,
        expected_revision: int,
    ) -> None:
        """@brief 以旧 revision CAS 一次性决定 approval / CAS a one-time approval decision."""


class AgentJobStore(Protocol):
    """@brief 直接复用统一 platform Job 存储的 Port / Port directly reusing the unified platform Job store."""

    async def add(self, job: Job) -> None:
        """@brief 写入统一 Job 表，不建 Agent 专用 Job 表 / Insert into the unified Job table, never an Agent-specific Job table."""

    async def get(
        self,
        workspace_id: WorkspaceId,
        job_id: JobId,
        *,
        for_update: bool = False,
    ) -> Job | None:
        """@brief 在 Workspace 内读取统一 Job / Read a unified Job inside one Workspace."""

    async def save(self, job: Job, *, expected_revision: int) -> None:
        """@brief 以旧 revision CAS 统一 Job / CAS the unified Job using its old revision."""


class AgentOutbox(Protocol):
    """@brief 写入平台统一 transactional outbox 的 Port / Port writing the platform-wide transactional outbox."""

    async def add(self, record: AgentOutboxRecord) -> None:
        """@brief 添加封闭、无私有推理的 Agent 信号 / Add a closed, private-reasoning-free Agent signal.

        @note Adapter 写入现有统一 outbox 表；不得创建 agent_outbox 平行表。
            / Adapters write the existing unified outbox table and never create an agent_outbox table.
        """


class AgentAuditSink(Protocol):
    """@brief 同事务统一 AuditEvent sink / Same-transaction unified AuditEvent sink."""

    async def add(self, event: AuditEvent) -> None:
        """@brief 写入平台统一审计表 / Write to the platform-wide audit table."""


class AgentModelProvider(Protocol):
    """@brief 外部模型 provider Port / External model-provider port."""

    async def execute(self, request: AgentProviderRequest) -> AgentProviderOutcome:
        """@brief 执行一次模型步骤 / Execute one model step.

        @note 只能在 UoW 完全退出后调用。Adapter 不得返回或记录私有 chain-of-thought；
            只能构造封闭的 user-visible completion 或 approval-required 结果。
            / Call only after the UoW has fully exited. Adapters never return or persist private
            chain-of-thought and construct only the closed public outcome types.
        """


class AgentToolExecutor(Protocol):
    """@brief 已批准工具调用的外部执行 Port / External execution port for an approved tool call."""

    async def execute(
        self,
        dispatch: AgentToolDecisionClaim,
        invocation_ref: ResourceRef,
    ) -> ToolExecutionReceipt:
        """@brief 在决定事务提交后执行精确 invocation / Execute the exact invocation after the decision transaction commits.

        @note 调用必须在 DB 行锁和 UoW 外；实现重新核验 dispatch、tool_call_id、短期凭据与
            invocation reference，并以 ``dispatch.id`` 原子去重外部副作用。/ Invocation occurs
            outside DB locks/UoW, revalidates the dispatch, tool-call ID, short-lived credentials,
            and invocation reference, and atomically deduplicates external effects by ``dispatch.id``.
        """


class AgentToolDecisionClaim(Protocol):
    """@brief 执行已提交工具决定所需的最小 durable claim / Minimal durable claim for a committed tool decision.

    ``ToolDecisionDispatch`` 与统一 outbox 的严格 adapter 都实现此结构协议。事件 ID 是
    外部工具调用的稳定幂等键；精确 Run、Job、Approval revision 防止旧决定绑定到新状态。
    / Both ``ToolDecisionDispatch`` and the strict unified-outbox adapter implement this structural
    protocol. The event ID is the stable external-tool idempotency key, while exact Run, Job, and
    Approval revisions prevent a stale decision from binding to newer state.
    """

    @property
    def id(self) -> AgentOutboxId:
        """@brief 返回稳定 outbox/operation ID / Return the stable outbox/operation ID."""

    @property
    def workspace_id(self) -> WorkspaceId:
        """@brief 返回已提交 Workspace / Return the committed Workspace."""

    @property
    def actor_id(self) -> UserId:
        """@brief 返回 Run creator 快照 / Return the Run-creator snapshot."""

    @property
    def run_ref(self) -> ResourceRef:
        """@brief 返回决定后的精确 Run revision / Return the exact post-decision Run revision."""

    @property
    def job_ref(self) -> ResourceRef:
        """@brief 返回决定后的精确 Job revision / Return the exact post-decision Job revision."""

    @property
    def approval_ref(self) -> ResourceRef:
        """@brief 返回已决定 Approval revision / Return the decided Approval revision."""

    @property
    def tool_call_id(self) -> ToolCallId:
        """@brief 返回已批准或拒绝的工具调用 ID / Return the approved or rejected tool-call ID."""

    @property
    def decision(self) -> ToolDecision:
        """@brief 返回不可变决定 / Return the immutable decision."""


class AgentRunExecutionClaim(Protocol):
    """@brief 执行 queued Run 所需的最小 durable claim / Minimal durable claim required to execute a queued Run.

    ``AgentRunQueuedDispatch`` 与统一 outbox claim adapter 都实现此结构协议，从而不为
    worker 伪造原事件时间等未参与授权的字段。/ Both ``AgentRunQueuedDispatch`` and the
    unified-outbox claim adapter implement this structural protocol, avoiding fabricated original
    event fields that are irrelevant to worker authorization.
    """

    @property
    def workspace_id(self) -> WorkspaceId:
        """@brief 返回已提交事件的 Workspace / Return the Workspace captured by the committed event."""

    @property
    def actor_id(self) -> UserId:
        """@brief 返回已提交 Run creator 快照 / Return the committed Run-creator snapshot."""

    @property
    def run_ref(self) -> ResourceRef:
        """@brief 返回精确 queued Run 引用 / Return the exact queued-Run reference."""

    @property
    def job_ref(self) -> ResourceRef:
        """@brief 返回与 Run 对齐的统一 Job 引用 / Return the unified Job reference aligned with the Run."""


class AgentRunExhaustionClaim(Protocol):
    """@brief outbox 尝试耗尽后的最小领域补偿 claim / Minimal domain-compensation claim after outbox exhaustion.

    payload 在 handler 失败时可能不可信，因此补偿只依赖 outbox header 中由数据库列约束的
    event、Workspace、actor 与 Run subject。/ The payload may be untrustworthy when a handler
    fails, so compensation relies only on the event, Workspace, actor, and Run subject constrained
    by dedicated outbox columns.
    """

    @property
    def id(self) -> AgentOutboxId:
        """@brief 返回失败事件的稳定关联 ID / Return the failed event's stable correlation ID."""

    @property
    def workspace_id(self) -> WorkspaceId:
        """@brief 返回已提交 Workspace / Return the committed Workspace."""

    @property
    def actor_id(self) -> UserId:
        """@brief 返回 Run creator 快照 / Return the Run-creator snapshot."""

    @property
    def run_ref(self) -> ResourceRef:
        """@brief 返回 outbox header 的 Run subject / Return the Run subject from the outbox header."""


class AgentUnitOfWork(Protocol):
    """@brief Conversation、Message、Run、Approval、Job、outbox、audit 的单一原子 UoW / Single atomic UoW for all Agent aggregates and journals."""

    @property
    def authorizer(self) -> AgentPermissionAuthorizer:
        """@brief 返回精确权限 adapter / Return the exact-permission adapter."""

    @property
    def policy(self) -> AgentRunPolicy:
        """@brief 返回本地执行约束策略 / Return the local execution-constraint policy."""

    @property
    def repository(self) -> AgentRepository:
        """@brief 返回事务绑定 repository / Return the transaction-bound repository."""

    @property
    def jobs(self) -> AgentJobStore:
        """@brief 返回统一 Job store / Return the unified Job store."""

    @property
    def outbox(self) -> AgentOutbox:
        """@brief 返回统一 outbox / Return the unified outbox."""

    @property
    def audit(self) -> AgentAuditSink:
        """@brief 返回统一 audit sink / Return the unified audit sink."""

    @property
    def resume_proposals(self) -> AgentResumeProposalBoundary:
        """@brief 返回同事务 Resume Proposal 边界 / Return the same-transaction Resume Proposal boundary."""

    async def __aenter__(self) -> Self:
        """@brief 开始工作单元 / Enter the unit of work."""

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        """@brief 异常或未提交时回滚 / Roll back on exception or absent commit."""

    async def commit(self) -> None:
        """@brief 原子提交全部聚合、Job、outbox、audit 与外层幂等 receipt / Atomically commit aggregates, Job, outbox, audit, and outer receipt."""

    async def rollback(self) -> None:
        """@brief 幂等回滚 / Roll back idempotently."""


class AgentUnitOfWorkFactory(Protocol):
    """@brief 为每个 5.4 公开用例创建未授权 UoW / Create an unscoped UoW for each public section-5.4 use case."""

    def __call__(self) -> AgentUnitOfWork:
        """@brief 创建未进入的 UoW / Create a not-yet-entered UoW."""


class AgentWorkerUnitOfWorkFactory(Protocol):
    """@brief 仅从 durable dispatch 创建显式身份的 worker UoW / Create explicitly scoped worker UoWs only from durable dispatches."""

    def __call__(
        self,
        workspace_id: WorkspaceId,
        actor_id: UserId,
    ) -> AgentUnitOfWork:
        """@brief 以 Run 创建者快照创建未进入 UoW / Create an unentered UoW scoped to the Run-creator snapshot."""


__all__ = [
    "AgentAuditSink",
    "AgentCasMismatch",
    "AgentContextResolver",
    "AgentJobStore",
    "AgentKnowledgeRetrievalRequest",
    "AgentKnowledgeRetriever",
    "AgentModelProvider",
    "AgentModelRoute",
    "AgentOutbox",
    "AgentPage",
    "AgentPageRequest",
    "AgentPermission",
    "AgentPermissionAuthorizer",
    "AgentPermissionGrant",
    "AgentPermissionRequest",
    "AgentPolicyDenied",
    "AgentProposalFailure",
    "AgentProviderFailure",
    "AgentRepository",
    "AgentResumeProposalBoundary",
    "AgentResumeProposalCommand",
    "AgentRunExecutionClaim",
    "AgentRunExhaustionClaim",
    "AgentRunPolicy",
    "AgentRunPolicyRequest",
    "AgentToolDecisionClaim",
    "AgentToolExecutor",
    "AgentToolRegistry",
    "AgentUnitOfWork",
    "AgentUnitOfWorkFactory",
    "AgentWorkerUnitOfWorkFactory",
    "MessageSequenceReservation",
    "ToolExecutionReceipt",
]
