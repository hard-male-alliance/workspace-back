"""@brief API v2 Conversation 与 Agent 应用用例 / API v2 Conversation and Agent use cases.

本服务逐一覆盖 ``contract.md`` 5.4 的 12 个路由。公开请求使用独立精确权限请求，
所有资源读取均以路径 Workspace 为首键；Message sequence 由 persistence 原子分配；
Run、ToolApproval、统一 Job、outbox 与 audit 在一个 UoW 中提交。外部模型/工具调用只由
``AgentWorkerService`` 在 UoW 退出后执行，绝不持有数据库行锁跨网络等待。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from backend.application.ports.agent_v2 import (
    AgentCasMismatch,
    AgentKnowledgeRetrievalRequest,
    AgentKnowledgeRetriever,
    AgentModelProvider,
    AgentPage,
    AgentPageRequest,
    AgentPermission,
    AgentPermissionGrant,
    AgentPermissionRequest,
    AgentPolicyDenied,
    AgentProposalFailure,
    AgentProviderFailure,
    AgentResumeProposalCommand,
    AgentRunExecutionClaim,
    AgentRunExhaustionClaim,
    AgentRunPolicyRequest,
    AgentToolDecisionClaim,
    AgentToolExecutor,
    AgentToolRegistry,
    AgentUnitOfWork,
    AgentUnitOfWorkFactory,
    AgentWorkerUnitOfWorkFactory,
    ToolExecutionReceipt,
)
from backend.domain.agent_v2 import (
    AGENT_RUN_JOB_KIND,
    AgentDomainError,
    AgentExecutionGrant,
    AgentKnowledgeEvidence,
    AgentOutboxId,
    AgentOutputMode,
    AgentProviderApprovalRequired,
    AgentProviderCompleted,
    AgentProviderOutcome,
    AgentProviderRequest,
    AgentResumeContext,
    AgentRun,
    AgentRunCancellationDispatch,
    AgentRunId,
    AgentRunQueuedDispatch,
    AgentRunSpec,
    AgentRunStateDispatch,
    AgentRunStatus,
    AgentRunView,
    AgentUsage,
    Conversation,
    ConversationCapability,
    ConversationId,
    ConversationPatch,
    Message,
    MessageId,
    MessageRole,
    ResumeProposalContentPart,
    TextContentPart,
    ToolApproval,
    ToolApprovalExpiredDispatch,
    ToolApprovalId,
    ToolApprovalStatus,
    ToolApprovalView,
    ToolDecision,
    ToolDecisionDispatch,
    validate_run_job_alignment,
)
from backend.domain.platform import (
    AuditEvent,
    AuditEventId,
    AuditOutcome,
    Job,
    JobId,
    JobProgress,
    JobProgressUnit,
    ProblemDetails,
)
from backend.domain.principals import ResourceMeta, TokenPrincipal, UserId, WorkspaceId
from backend.domain.resources import ResourceRef
from workspace_shared.ids import new_opaque_id

V2_AGENT_ENDPOINT_METHODS = (
    "list_conversations",
    "create_conversation",
    "get_conversation",
    "update_conversation",
    "delete_conversation",
    "list_messages",
    "create_message",
    "create_agent_run",
    "get_agent_run",
    "cancel_agent_run",
    "get_tool_approval",
    "decide_tool_approval",
)
"""@brief 5.4 实际 12 个路由对应的应用方法 / Application methods for the 12 actual section-5.4 routes."""


class Clock(Protocol):
    """@brief 应用层可替换时钟 / Replaceable application clock."""

    def now(self) -> datetime:
        """@brief 返回带时区当前时刻 / Return the current timezone-aware instant."""


class OpaqueIdFactory(Protocol):
    """@brief 可替换 opaque-ID 工厂 / Replaceable opaque-ID factory."""

    def __call__(self, prefix: str) -> str:
        """@brief 生成指定稳定前缀的 ID / Generate an ID with a stable prefix."""


class UtcClock:
    """@brief 使用 UTC 的生产时钟 / Production clock using UTC."""

    def now(self) -> datetime:
        """@brief 返回 UTC 当前时刻 / Return the current UTC instant."""
        return datetime.now(UTC)


class NewOpaqueIdFactory:
    """@brief 使用共享 ULID 风格 opaque-ID 生成器 / Shared ULID-style opaque-ID generator."""

    def __call__(self, prefix: str) -> str:
        """@brief 生成新 ID / Generate a new ID."""
        return new_opaque_id(prefix)


class AgentApplicationError(Exception):
    """@brief 可稳定映射为 RFC 9457 problem 的应用错误 / Application error mappable to an RFC 9457 problem."""

    code: str
    """@brief 稳定机器错误码 / Stable machine-readable error code."""

    detail: str
    """@brief 不泄漏跨 Workspace 信息的公开说明 / Public detail without cross-Workspace disclosure."""

    def __init__(self, code: str, detail: str) -> None:
        """@brief 初始化应用错误 / Initialize an application error."""
        super().__init__(detail)
        self.code = code
        self.detail = detail


class AgentResourceNotFound(AgentApplicationError):
    """@brief 资源不存在或为防枚举隐藏 / Resource absent or hidden to prevent enumeration."""

    def __init__(self, resource: str) -> None:
        """@brief 创建统一 404 来源 / Create a uniform not-found result."""
        super().__init__(f"{resource}.not_found", f"{resource} was not found")


class AgentPreconditionFailed(AgentApplicationError):
    """@brief 强 ETag 对应 revision 已过期 / Revision represented by a strong ETag is stale."""

    def __init__(self) -> None:
        """@brief 创建统一 412 来源 / Create a uniform precondition-failed result."""
        super().__init__("http.precondition_failed", "resource revision precondition failed")


class AgentConflict(AgentApplicationError):
    """@brief 当前状态拒绝命令 / Current state rejects a command."""


class AgentPortProtocolError(AgentApplicationError):
    """@brief Adapter 返回越过 Workspace 或授权边界的数据 / Adapter returned data outside Workspace or authorization bounds."""


class InvalidAgentCommand(AgentApplicationError):
    """@brief 请求命令不满足 5.4 边界 / Request command violates section-5.4 bounds."""


@dataclass(frozen=True, slots=True)
class AgentMutationContext:
    """@brief 写请求的审计关联 / Audit correlation for a write request."""

    request_id: str

    def __post_init__(self) -> None:
        """@brief 校验 request ID 基本边界 / Validate basic request-ID bounds."""
        if not 8 <= len(self.request_id) <= 160 or not self.request_id[0].isalpha():
            raise InvalidAgentCommand("request.invalid_id", "request id is invalid")


@dataclass(frozen=True, slots=True)
class CreateConversationCommand:
    """@brief CreateConversationRequest 的类型化命令 / Typed CreateConversationRequest command."""

    capability: ConversationCapability
    title: str | None


@dataclass(frozen=True, slots=True)
class CreateMessageCommand:
    """@brief CreateMessageRequest 的类型化命令 / Typed CreateMessageRequest command."""

    parent_message_id: MessageId | None
    content: tuple[TextContentPart, ...]

    def __post_init__(self) -> None:
        """@brief 校验客户端最多提交 20 个 text parts / Validate the client limit of 20 text parts."""
        if not 1 <= len(self.content) <= 20:
            raise InvalidAgentCommand(
                "message.invalid_content",
                "message request must contain 1 to 20 text parts",
            )


@dataclass(frozen=True, slots=True)
class ToolApprovalDecisionCommand:
    """@brief ToolApprovalDecisionRequest 的类型化命令 / Typed ToolApprovalDecisionRequest command."""

    decision: ToolDecision


class AgentApplicationService:
    """@brief 5.4 十二个 HTTP 无关应用用例 / Twelve transport-independent section-5.4 use cases."""

    def __init__(
        self,
        uow_factory: AgentUnitOfWorkFactory,
        *,
        clock: Clock | None = None,
        id_factory: OpaqueIdFactory | None = None,
    ) -> None:
        """@brief 注入单一 UoW、时钟和 ID 工厂 / Inject the single UoW, clock, and ID factory."""
        self._uow_factory = uow_factory
        self._clock = clock or UtcClock()
        self._ids = id_factory or NewOpaqueIdFactory()

    async def list_conversations(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        page: AgentPageRequest,
    ) -> AgentPage[Conversation]:
        """@brief 列出未软删除 Conversation / List non-soft-deleted Conversations."""
        async with self._uow_factory() as uow:
            await self._authorize(
                uow,
                principal,
                workspace_id,
                AgentPermission.LIST_CONVERSATIONS,
                _workspace_ref(workspace_id),
            )
            result = await uow.repository.list_conversations(workspace_id, page)
            if any(item.workspace_id != workspace_id or item.is_deleted for item in result.items):
                raise AgentPortProtocolError(
                    "agent.repository_scope_violation",
                    "conversation repository returned an invalid item",
                )
            await uow.commit()
            return result

    async def create_conversation(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        command: CreateConversationCommand,
        context: AgentMutationContext,
    ) -> Conversation:
        """@brief 创建 active Conversation / Create an active Conversation."""
        now = self._clock.now()
        conversation = Conversation(
            meta=ResourceMeta(
                ConversationId(self._ids("conv")),
                1,
                now,
                now,
            ),
            workspace_id=workspace_id,
            title=command.title,
            capability=command.capability,
        )
        async with self._uow_factory() as uow:
            await self._authorize(
                uow,
                principal,
                workspace_id,
                AgentPermission.CREATE_CONVERSATION,
                _workspace_ref(workspace_id),
            )
            await uow.repository.add_conversation(conversation)
            await uow.audit.add(
                self._audit(
                    principal,
                    workspace_id,
                    "conversation.create",
                    _conversation_ref(conversation),
                    context,
                    now,
                )
            )
            await uow.commit()
            return conversation

    async def get_conversation(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
    ) -> Conversation:
        """@brief 读取单个未删除 Conversation / Read one non-deleted Conversation."""
        async with self._uow_factory() as uow:
            await self._authorize(
                uow,
                principal,
                workspace_id,
                AgentPermission.READ_CONVERSATION,
                ResourceRef("conversation", conversation_id),
            )
            conversation = await self._conversation(
                uow,
                workspace_id,
                conversation_id,
            )
            await uow.commit()
            return conversation

    async def get_conversation_for_update(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
    ) -> Conversation:
        """@brief 用 update 权限读取 If-Match snapshot / Read an If-Match snapshot with update permission.

        @param principal 已验证 principal / Verified principal.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param conversation_id Conversation 标识 / Conversation identifier.
        @return 与强 ETag 同一表示的 Conversation / Conversation represented by the strong ETag.
        """
        return await self._conversation_mutation_snapshot(
            principal,
            workspace_id,
            conversation_id,
            AgentPermission.UPDATE_CONVERSATION,
        )

    async def get_conversation_for_deletion(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
    ) -> Conversation:
        """@brief 用 delete 权限读取 If-Match snapshot / Read an If-Match snapshot with delete permission.

        @param principal 已验证 principal / Verified principal.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param conversation_id Conversation 标识 / Conversation identifier.
        @return 与强 ETag 同一表示的 Conversation / Conversation represented by the strong ETag.
        """
        return await self._conversation_mutation_snapshot(
            principal,
            workspace_id,
            conversation_id,
            AgentPermission.DELETE_CONVERSATION,
        )

    async def get_conversation_for_message_creation(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
    ) -> Conversation:
        """@brief 用 message-create 权限读取 Conversation snapshot / Read the Conversation snapshot with message-create permission.

        @param principal 已验证 principal / Verified principal.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param conversation_id Conversation 标识 / Conversation identifier.
        @return 与 If-Match 表示一致的 Conversation / Conversation matching the If-Match representation.
        """
        return await self._conversation_mutation_snapshot(
            principal,
            workspace_id,
            conversation_id,
            AgentPermission.CREATE_MESSAGE,
        )

    async def update_conversation(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        patch: ConversationPatch,
        *,
        expected_revision: int,
        context: AgentMutationContext,
    ) -> Conversation:
        """@brief 以强 If-Match 原子更新 Conversation / Atomically update a Conversation with strong If-Match."""
        now = self._clock.now()
        try:
            async with self._uow_factory() as uow:
                await self._authorize(
                    uow,
                    principal,
                    workspace_id,
                    AgentPermission.UPDATE_CONVERSATION,
                    ResourceRef("conversation", conversation_id),
                )
                before = await self._conversation(
                    uow,
                    workspace_id,
                    conversation_id,
                    for_update=True,
                )
                _require_revision(before.meta.revision, expected_revision)
                after = before.update(patch, at=now)
                await uow.repository.save_conversation(
                    after,
                    expected_revision=before.meta.revision,
                )
                await uow.audit.add(
                    self._audit(
                        principal,
                        workspace_id,
                        "conversation.update",
                        _conversation_ref(after),
                        context,
                        now,
                    )
                )
                await uow.commit()
                return after
        except AgentCasMismatch as error:
            raise AgentPreconditionFailed from error

    async def delete_conversation(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        *,
        expected_revision: int,
        context: AgentMutationContext,
    ) -> None:
        """@brief 无活动 Run 时软删除 Conversation / Soft-delete a Conversation when no Run is active."""
        now = self._clock.now()
        try:
            async with self._uow_factory() as uow:
                await self._authorize(
                    uow,
                    principal,
                    workspace_id,
                    AgentPermission.DELETE_CONVERSATION,
                    ResourceRef("conversation", conversation_id),
                )
                before = await self._conversation(
                    uow,
                    workspace_id,
                    conversation_id,
                    for_update=True,
                )
                _require_revision(before.meta.revision, expected_revision)
                if await uow.repository.has_nonterminal_runs(workspace_id, conversation_id):
                    raise AgentConflict(
                        "conversation.has_active_run",
                        "conversation has a non-terminal agent run",
                    )
                after = before.soft_delete(at=now)
                await uow.repository.save_conversation(
                    after,
                    expected_revision=before.meta.revision,
                )
                await uow.audit.add(
                    self._audit(
                        principal,
                        workspace_id,
                        "conversation.delete",
                        _conversation_ref(after),
                        context,
                        now,
                    )
                )
                await uow.commit()
        except AgentCasMismatch as error:
            raise AgentPreconditionFailed from error

    async def list_messages(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        page: AgentPageRequest,
    ) -> AgentPage[Message]:
        """@brief 按持久化 sequence 稳定列出 Message / List Messages stably by persisted sequence."""
        async with self._uow_factory() as uow:
            await self._authorize(
                uow,
                principal,
                workspace_id,
                AgentPermission.LIST_MESSAGES,
                ResourceRef("conversation", conversation_id),
            )
            await self._conversation(uow, workspace_id, conversation_id)
            result = await uow.repository.list_messages(workspace_id, conversation_id, page)
            if any(
                item.workspace_id != workspace_id or item.conversation_id != conversation_id
                for item in result.items
            ):
                raise AgentPortProtocolError(
                    "agent.repository_scope_violation",
                    "message repository returned an invalid item",
                )
            ordered = tuple((item.sequence, item.meta.id) for item in result.items)
            if ordered != tuple(sorted(ordered)):
                raise AgentPortProtocolError(
                    "agent.repository_order_violation",
                    "message repository returned an unstable order",
                )
            await uow.commit()
            return result

    async def create_message(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        command: CreateMessageCommand,
        *,
        expected_conversation_revision: int,
        context: AgentMutationContext,
    ) -> Message:
        """@brief 以 Conversation If-Match 原子追加 user Message / Atomically append a user Message with Conversation If-Match."""
        now = self._clock.now()
        try:
            async with self._uow_factory() as uow:
                await self._authorize(
                    uow,
                    principal,
                    workspace_id,
                    AgentPermission.CREATE_MESSAGE,
                    ResourceRef("conversation", conversation_id),
                )
                conversation = await self._conversation(
                    uow,
                    workspace_id,
                    conversation_id,
                    for_update=True,
                )
                conversation.require_writable()
                _require_revision(conversation.meta.revision, expected_conversation_revision)
                parent: Message | None = None
                if command.parent_message_id is not None:
                    parent = await uow.repository.get_message(
                        workspace_id,
                        conversation_id,
                        command.parent_message_id,
                    )
                    if parent is None:
                        raise AgentResourceNotFound("message")
                    _require_message_scope(parent, workspace_id, conversation_id)
                reservation = await uow.repository.allocate_message_sequence(
                    workspace_id,
                    conversation_id,
                    expected_conversation_revision=expected_conversation_revision,
                    at=now,
                )
                if reservation.conversation_revision != expected_conversation_revision + 1:
                    raise AgentPortProtocolError(
                        "message.sequence_protocol_violation",
                        "message sequence allocation returned an invalid conversation revision",
                    )
                message = Message(
                    meta=ResourceMeta(MessageId(self._ids("msg")), 1, now, now),
                    workspace_id=workspace_id,
                    conversation_id=conversation_id,
                    sequence=reservation.sequence,
                    role=MessageRole.USER,
                    parent_message_id=command.parent_message_id,
                    content=command.content,
                )
                if parent is not None and parent.sequence >= message.sequence:
                    raise AgentPortProtocolError(
                        "message.sequence_protocol_violation",
                        "parent message must precede its child",
                    )
                await uow.repository.add_message(message)
                await uow.audit.add(
                    self._audit(
                        principal,
                        workspace_id,
                        "conversation.message.create",
                        ResourceRef("message", message.meta.id, 1),
                        context,
                        now,
                    )
                )
                await uow.commit()
                return message
        except AgentCasMismatch as error:
            raise AgentPreconditionFailed from error

    async def create_agent_run(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        spec: AgentRunSpec,
        context: AgentMutationContext,
    ) -> AgentRunView:
        """@brief 授权执行约束并原子创建 Run、统一 Job 和 outbox / Authorize constraints and atomically create Run, unified Job, and outbox."""
        now = self._clock.now()
        async with self._uow_factory() as uow:
            await self._authorize(
                uow,
                principal,
                workspace_id,
                AgentPermission.CREATE_AGENT_RUN,
                ResourceRef("conversation", spec.conversation_id),
            )
            conversation = await self._conversation(
                uow,
                workspace_id,
                spec.conversation_id,
                for_update=True,
            )
            conversation.require_writable()
            input_message = await uow.repository.get_message(
                workspace_id,
                spec.conversation_id,
                spec.input_message_id,
            )
            if input_message is None:
                raise AgentResourceNotFound("message")
            _require_message_scope(input_message, workspace_id, spec.conversation_id)
            if input_message.role is not MessageRole.USER:
                raise InvalidAgentCommand(
                    "agent_run.invalid_input_message",
                    "agent run input must be a user message",
                )
            if spec.capability is not conversation.capability:
                raise InvalidAgentCommand(
                    "agent_run.capability_mismatch",
                    "agent run capability must match its conversation",
                )
            grant = await uow.policy.authorize_run(
                AgentRunPolicyRequest(
                    actor_id=principal.user_id,
                    workspace_id=workspace_id,
                    conversation=conversation,
                    input_message=input_message,
                    spec=spec,
                )
            )
            grant.validate_for(conversation, spec)
            run_id = AgentRunId(self._ids("run"))
            job_id = JobId(self._ids("job"))
            run = AgentRun(
                view=AgentRunView(
                    meta=ResourceMeta(run_id, 1, now, now),
                    workspace_id=workspace_id,
                    conversation_id=spec.conversation_id,
                    input_message_id=spec.input_message_id,
                    capability=spec.capability,
                    status=AgentRunStatus.QUEUED,
                ),
                job_id=job_id,
                created_by=principal.user_id,
                spec=spec,
                grant=grant,
            )
            job = Job(
                meta=ResourceMeta(job_id, 1, now, now),
                workspace_id=workspace_id,
                kind=AGENT_RUN_JOB_KIND,
                subject=ResourceRef("agent_run", run_id),
            )
            validate_run_job_alignment(run, job)
            await uow.repository.add_run(run)
            await uow.jobs.add(job)
            await uow.outbox.add(
                AgentRunQueuedDispatch(
                    id=AgentOutboxId(self._ids("outbox")),
                    workspace_id=workspace_id,
                    actor_id=run.created_by,
                    run_ref=ResourceRef("agent_run", run_id, 1),
                    job_ref=ResourceRef("job", job_id, 1),
                    occurred_at=now,
                )
            )
            await uow.audit.add(
                self._audit(
                    principal,
                    workspace_id,
                    "agent_run.create",
                    ResourceRef("agent_run", run_id, 1),
                    context,
                    now,
                )
            )
            await uow.commit()
            return run.view

    async def get_agent_run(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        run_id: AgentRunId,
    ) -> AgentRunView:
        """@brief 读取单个 AgentRun / Read one AgentRun."""
        async with self._uow_factory() as uow:
            await self._authorize(
                uow,
                principal,
                workspace_id,
                AgentPermission.READ_AGENT_RUN,
                ResourceRef("agent_run", run_id),
            )
            run = await self._run(uow, workspace_id, run_id)
            await uow.commit()
            return run.view

    async def get_agent_run_for_cancellation(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        run_id: AgentRunId,
    ) -> AgentRunView:
        """@brief 用 cancel 权限读取 Run If-Match snapshot / Read a Run If-Match snapshot with cancel permission.

        @param principal 已验证 principal / Verified principal.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param run_id AgentRun 标识 / AgentRun identifier.
        @return 仅含公开字段的 Run snapshot / Run snapshot containing only public fields.
        """
        async with self._uow_factory() as uow:
            await self._authorize(
                uow,
                principal,
                workspace_id,
                AgentPermission.CANCEL_AGENT_RUN,
                ResourceRef("agent_run", run_id),
            )
            run = await self._run(uow, workspace_id, run_id)
            await uow.commit()
            return run.view

    async def cancel_agent_run(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        run_id: AgentRunId,
        *,
        expected_revision: int,
        context: AgentMutationContext,
    ) -> AgentRunView:
        """@brief 原子取消 Run、Job 与待决 approval / Atomically cancel a Run, Job, and pending approval."""
        now = self._clock.now()
        actor = _actor_ref(principal)
        try:
            async with self._uow_factory() as uow:
                await self._authorize(
                    uow,
                    principal,
                    workspace_id,
                    AgentPermission.CANCEL_AGENT_RUN,
                    ResourceRef("agent_run", run_id),
                )
                before = await self._run(uow, workspace_id, run_id, for_update=True)
                _require_revision(before.meta.revision, expected_revision)
                if before.is_terminal:
                    raise AgentConflict("agent_run.terminal", "agent run is already terminal")
                job = await self._job(uow, workspace_id, before.job_id, for_update=True)
                validate_run_job_alignment(before, job)
                approval: ToolApproval | None = None
                if before.view.pending_approval_id is not None:
                    approval = await self._approval(
                        uow,
                        workspace_id,
                        before.view.pending_approval_id,
                        for_update=True,
                    )
                    if not approval.matches_waiting_run(before):
                        raise AgentPortProtocolError(
                            "tool_approval.binding_violation",
                            "pending approval does not match the waiting run",
                        )
                    closed = (
                        approval.expire(actor, at=now)
                        if now >= approval.view.expires_at
                        else approval.decide(ToolDecision.REJECT, actor, at=now)
                    )
                    await uow.repository.save_approval(
                        closed,
                        expected_revision=approval.meta.revision,
                    )
                after = before.cancel(at=now)
                cancelled_job = job.cancel(at=now)
                validate_run_job_alignment(after, cancelled_job)
                await uow.repository.save_run(after, expected_revision=before.meta.revision)
                await uow.jobs.save(cancelled_job, expected_revision=job.meta.revision)
                await uow.outbox.add(
                    AgentRunCancellationDispatch(
                        id=AgentOutboxId(self._ids("outbox")),
                        workspace_id=workspace_id,
                        actor_id=before.created_by,
                        run_ref=ResourceRef("agent_run", run_id, after.meta.revision),
                        job_ref=ResourceRef(
                            "job",
                            cancelled_job.meta.id,
                            cancelled_job.meta.revision,
                        ),
                        occurred_at=now,
                    )
                )
                await uow.audit.add(
                    self._audit(
                        principal,
                        workspace_id,
                        "agent_run.cancel",
                        ResourceRef("agent_run", run_id, after.meta.revision),
                        context,
                        now,
                    )
                )
                await uow.commit()
                return after.view
        except AgentCasMismatch as error:
            raise AgentPreconditionFailed from error

    async def get_tool_approval(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        approval_id: ToolApprovalId,
    ) -> ToolApprovalView:
        """@brief 读取单个 ToolApproval / Read one ToolApproval."""
        async with self._uow_factory() as uow:
            await self._authorize(
                uow,
                principal,
                workspace_id,
                AgentPermission.READ_TOOL_APPROVAL,
                ResourceRef("tool_approval", approval_id),
            )
            approval = await self._approval(uow, workspace_id, approval_id)
            await uow.commit()
            return approval.view

    async def get_tool_approval_for_decision(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        approval_id: ToolApprovalId,
    ) -> ToolApprovalView:
        """@brief 用 decision 权限读取 ToolApproval snapshot / Read a ToolApproval snapshot with decision permission.

        @param principal 已验证 principal / Verified principal.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param approval_id ToolApproval 标识 / ToolApproval identifier.
        @return 不含私有 tool-call binding 的 snapshot / Snapshot without the private tool-call binding.
        """
        async with self._uow_factory() as uow:
            await self._authorize(
                uow,
                principal,
                workspace_id,
                AgentPermission.DECIDE_TOOL_APPROVAL,
                ResourceRef("tool_approval", approval_id),
            )
            approval = await self._approval(uow, workspace_id, approval_id)
            await uow.commit()
            return approval.view

    async def decide_tool_approval(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        approval_id: ToolApprovalId,
        command: ToolApprovalDecisionCommand,
        *,
        expected_revision: int,
        context: AgentMutationContext,
    ) -> ToolApprovalView:
        """@brief 原子决定 approval 并恢复 Run/Job、写 outbox/audit / Atomically decide and resume Run/Job with outbox/audit."""
        now = self._clock.now()
        actor = _actor_ref(principal)
        expired = False
        result: ToolApprovalView | None = None
        try:
            async with self._uow_factory() as uow:
                await self._authorize(
                    uow,
                    principal,
                    workspace_id,
                    AgentPermission.DECIDE_TOOL_APPROVAL,
                    ResourceRef("tool_approval", approval_id),
                )
                before = await self._approval(
                    uow,
                    workspace_id,
                    approval_id,
                    for_update=True,
                )
                _require_revision(before.meta.revision, expected_revision)
                run = await self._run(uow, workspace_id, before.view.run_id, for_update=True)
                if not before.matches_waiting_run(run):
                    raise AgentConflict(
                        "tool_approval.binding_mismatch",
                        "tool approval no longer matches the waiting run",
                    )
                job = await self._job(uow, workspace_id, run.job_id, for_update=True)
                validate_run_job_alignment(run, job)
                if now >= before.view.expires_at:
                    after = before.expire(_service_ref(), at=now)
                    problem = _tool_approval_expired_problem(context.request_id)
                    failed_run = run.fail(problem, at=now)
                    failed_job = job.fail(problem, at=now)
                    validate_run_job_alignment(failed_run, failed_job)
                    await uow.repository.save_approval(
                        after,
                        expected_revision=before.meta.revision,
                    )
                    await uow.repository.save_run(
                        failed_run,
                        expected_revision=run.meta.revision,
                    )
                    await uow.jobs.save(failed_job, expected_revision=job.meta.revision)
                    await uow.outbox.add(
                        ToolApprovalExpiredDispatch(
                            id=AgentOutboxId(self._ids("outbox")),
                            workspace_id=workspace_id,
                            actor_id=run.created_by,
                            run_ref=ResourceRef(
                                "agent_run",
                                failed_run.meta.id,
                                failed_run.meta.revision,
                            ),
                            job_ref=ResourceRef(
                                "job",
                                failed_job.meta.id,
                                failed_job.meta.revision,
                            ),
                            approval_ref=ResourceRef(
                                "tool_approval",
                                after.meta.id,
                                after.meta.revision,
                            ),
                            tool_call_id=before.binding.tool_call_id,
                            occurred_at=now,
                        )
                    )
                    await uow.audit.add(
                        self._audit(
                            principal,
                            workspace_id,
                            "tool_approval.decide",
                            ResourceRef("tool_approval", after.meta.id, after.meta.revision),
                            context,
                            now,
                            outcome=AuditOutcome.DENIED,
                        )
                    )
                    await uow.commit()
                    expired = True
                    result = after.view
                else:
                    after = before.decide(command.decision, actor, at=now)
                    resumed_run = run.resume_after_decision(
                        approval_id,
                        before.binding.tool_call_id,
                        at=now,
                    )
                    resumed_job = job.report_progress(
                        JobProgress(
                            phase="tool_decision_recorded",
                            completed=0,
                            total=None,
                            unit=JobProgressUnit.STEPS,
                        ),
                        at=now,
                    )
                    validate_run_job_alignment(resumed_run, resumed_job)
                    await uow.repository.save_approval(
                        after,
                        expected_revision=before.meta.revision,
                    )
                    await uow.repository.save_run(
                        resumed_run,
                        expected_revision=run.meta.revision,
                    )
                    await uow.jobs.save(resumed_job, expected_revision=job.meta.revision)
                    await uow.outbox.add(
                        ToolDecisionDispatch(
                            id=AgentOutboxId(self._ids("outbox")),
                            workspace_id=workspace_id,
                            actor_id=run.created_by,
                            run_ref=ResourceRef(
                                "agent_run",
                                resumed_run.meta.id,
                                resumed_run.meta.revision,
                            ),
                            job_ref=ResourceRef(
                                "job",
                                resumed_job.meta.id,
                                resumed_job.meta.revision,
                            ),
                            approval_ref=ResourceRef(
                                "tool_approval",
                                after.meta.id,
                                after.meta.revision,
                            ),
                            tool_call_id=before.binding.tool_call_id,
                            decision=command.decision,
                            occurred_at=now,
                        )
                    )
                    await uow.audit.add(
                        self._audit(
                            principal,
                            workspace_id,
                            "tool_approval.decide",
                            ResourceRef("tool_approval", after.meta.id, after.meta.revision),
                            context,
                            now,
                        )
                    )
                    await uow.commit()
                    result = after.view
        except AgentCasMismatch as error:
            raise AgentPreconditionFailed from error
        if expired:
            raise AgentConflict(
                "tool_approval.expired",
                "tool approval has expired",
            )
        if result is None:
            raise AgentPortProtocolError(
                "tool_approval.decision_protocol_violation",
                "tool approval decision produced no result",
            )
        return result

    async def _authorize(
        self,
        uow: AgentUnitOfWork,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        permission: AgentPermission,
        target: ResourceRef,
    ) -> AgentPermissionGrant:
        """@brief 发出并核验精确 permission request / Issue and verify an exact permission request."""
        request = AgentPermissionRequest(workspace_id, permission, target)
        grant = await uow.authorizer.authorize(principal, request)
        if grant.actor_id != principal.user_id or grant.request != request:
            raise AgentPortProtocolError(
                "agent.authorization_protocol_violation",
                "agent authorizer returned a mismatched grant",
            )
        return grant

    async def _conversation_mutation_snapshot(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        permission: AgentPermission,
    ) -> Conversation:
        """@brief 以精确 mutation 权限取得 Conversation snapshot / Obtain a Conversation snapshot with exact mutation permission.

        @param principal 已验证 principal / Verified principal.
        @param workspace_id 路径 Workspace / Path Workspace.
        @param conversation_id Conversation 标识 / Conversation identifier.
        @param permission update、delete 或 message-create 权限 / Update, delete, or message-create permission.
        @return 未删除的 Conversation snapshot / Non-deleted Conversation snapshot.
        @raise ValueError 调用方传入非 Conversation mutation 权限时 fail closed / Fails closed for a non-Conversation mutation permission.
        """
        if permission not in {
            AgentPermission.UPDATE_CONVERSATION,
            AgentPermission.DELETE_CONVERSATION,
            AgentPermission.CREATE_MESSAGE,
        }:
            raise ValueError("conversation snapshot requires an exact mutation permission")
        async with self._uow_factory() as uow:
            await self._authorize(
                uow,
                principal,
                workspace_id,
                permission,
                ResourceRef("conversation", conversation_id),
            )
            conversation = await self._conversation(uow, workspace_id, conversation_id)
            await uow.commit()
            return conversation

    async def _conversation(
        self,
        uow: AgentUnitOfWork,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        *,
        for_update: bool = False,
    ) -> Conversation:
        """@brief 读取并二次校验 Conversation 边界 / Read and revalidate a Conversation boundary."""
        conversation = await uow.repository.get_conversation(
            workspace_id,
            conversation_id,
            for_update=for_update,
            include_deleted=True,
        )
        if conversation is None or conversation.workspace_id != workspace_id or conversation.is_deleted:
            raise AgentResourceNotFound("conversation")
        return conversation

    async def _run(
        self,
        uow: AgentUnitOfWork,
        workspace_id: WorkspaceId,
        run_id: AgentRunId,
        *,
        for_update: bool = False,
    ) -> AgentRun:
        """@brief 读取并二次校验 Run 边界 / Read and revalidate a Run boundary."""
        run = await uow.repository.get_run(workspace_id, run_id, for_update=for_update)
        if run is None or run.workspace_id != workspace_id:
            raise AgentResourceNotFound("agent_run")
        return run

    async def _approval(
        self,
        uow: AgentUnitOfWork,
        workspace_id: WorkspaceId,
        approval_id: ToolApprovalId,
        *,
        for_update: bool = False,
    ) -> ToolApproval:
        """@brief 读取并二次校验 approval 边界 / Read and revalidate an approval boundary."""
        approval = await uow.repository.get_approval(
            workspace_id,
            approval_id,
            for_update=for_update,
        )
        if approval is None or approval.workspace_id != workspace_id:
            raise AgentResourceNotFound("tool_approval")
        return approval

    async def _job(
        self,
        uow: AgentUnitOfWork,
        workspace_id: WorkspaceId,
        job_id: JobId,
        *,
        for_update: bool = False,
    ) -> Job:
        """@brief 读取并二次校验统一 Job 边界 / Read and revalidate a unified Job boundary."""
        job = await uow.jobs.get(workspace_id, job_id, for_update=for_update)
        if job is None or job.workspace_id != workspace_id:
            raise AgentPortProtocolError(
                "agent.job_protocol_violation",
                "agent run is missing its unified Job",
            )
        return job

    def _audit(
        self,
        principal: TokenPrincipal,
        workspace_id: WorkspaceId,
        action: str,
        target: ResourceRef,
        context: AgentMutationContext,
        at: datetime,
        *,
        outcome: AuditOutcome = AuditOutcome.ALLOWED,
    ) -> AuditEvent:
        """@brief 构造统一平台 AuditEvent / Build a unified platform AuditEvent."""
        return AuditEvent(
            id=AuditEventId(self._ids("audit")),
            workspace_id=workspace_id,
            occurred_at=at,
            actor=_actor_ref(principal),
            action=action,
            target=target,
            outcome=outcome,
            request_id=context.request_id,
        )


@dataclass(frozen=True, slots=True)
class _PreparedAgentExecution:
    """@brief 两个短事务之间唯一允许携带的执行快照 / Sole execution snapshot carried between two short transactions."""

    run: AgentRun
    """@brief 已提交为 running 的 Run / Run committed in the running state."""

    job: Job
    """@brief 与 Run revision 对齐的 running Job / Running Job aligned with the Run revision."""

    input_message: Message
    """@brief 精确用户输入 / Exact user input."""

    resume_context: AgentResumeContext | None
    """@brief 可选精确 Resume SIR / Optional exact Resume SIR."""


class AgentWorkerService:
    """@brief 在短事务之间隔离 provider/tool 网络 I/O 的 Agent worker / Agent worker isolating provider/tool network I/O between short transactions."""

    def __init__(
        self,
        uow_factory: AgentWorkerUnitOfWorkFactory,
        model_provider: AgentModelProvider,
        tool_executor: AgentToolExecutor,
        *,
        knowledge_retriever: AgentKnowledgeRetriever | None = None,
        tool_registry: AgentToolRegistry | None = None,
        clock: Clock | None = None,
        id_factory: OpaqueIdFactory | None = None,
    ) -> None:
        """@brief 注入 worker Ports / Inject worker ports.

        @param uow_factory durable dispatch 作用域的 UoW 工厂 / UoW factory scoped by a durable dispatch.
        @param model_provider 严格结构化模型边界 / Strict structured model boundary.
        @param tool_executor 已批准调用的执行边界 / Execution boundary for approved calls.
        @param knowledge_retriever 事务外、grant 约束的检索器 / Out-of-transaction retriever
            constrained by a grant.
        @param tool_registry 服务端工具 allowlist；缺省即空 allowlist / Server-side tool
            allowlist; omission means an empty allowlist.
        @param clock 可替换时钟 / Replaceable clock.
        @param id_factory 可替换 opaque-ID 工厂 / Replaceable opaque-ID factory.
        """
        self._uow_factory = uow_factory
        self._model_provider = model_provider
        self._tool_executor = tool_executor
        self._knowledge_retriever = knowledge_retriever
        self._tool_registry = tool_registry
        self._clock = clock or UtcClock()
        self._ids = id_factory or NewOpaqueIdFactory()

    async def execute_run(
        self,
        dispatch: AgentRunExecutionClaim,
    ) -> AgentRunView:
        """@brief 重新授权、检索并提交一次 durable Run / Reauthorize, retrieve, and commit one durable Run.

        @param dispatch 已提交且交叉验证的工作 claim / Committed, cross-validated work claim.
        @return 当前或新终态 Run view / Current or newly terminal Run view.
        @note 模型与检索 I/O 只发生在两个短事务之间；最终提交前再次授权，并在同一事务中
            物化 Message、Proposal、Run、Job 与 outbox。/ Model and retrieval I/O occur only
            between two short transactions. Authorization is refreshed again before Message,
            Proposal, Run, Job, and outbox are materialized in one final transaction.
        """
        run_id = AgentRunId(dispatch.run_ref.id)
        try:
            prepared = await self._prepare_execution(dispatch)
        except AgentPolicyDenied:
            return await self._record_preflight_failure(
                dispatch,
                _authorization_revoked_problem(run_id),
            )
        except AgentProposalFailure as error:
            return await self._record_preflight_failure(dispatch, error.problem)
        if isinstance(prepared, AgentRunView):
            return prepared
        try:
            request = await self._build_provider_request(dispatch, prepared)
            outcome = await self._model_provider.execute(request)
            if isinstance(outcome, AgentProviderCompleted):
                outcome.validate_for(request)
        except asyncio.CancelledError:
            # Leave the RUNNING pair resumable by the durable outbox retry path.
            raise
        except AgentProviderFailure as error:
            return await self._record_run_failure(
                dispatch.workspace_id,
                run_id,
                prepared.run,
                prepared.job,
                error.problem,
            )
        except AgentDomainError:
            return await self._record_run_failure(
                dispatch.workspace_id,
                run_id,
                prepared.run,
                prepared.job,
                _provider_protocol_problem(run_id),
            )
        except Exception:
            return await self._record_run_failure(
                dispatch.workspace_id,
                run_id,
                prepared.run,
                prepared.job,
                _unexpected_provider_problem(run_id),
            )

        try:
            return await self._commit_provider_outcome(
                dispatch,
                prepared,
                request,
                outcome,
            )
        except AgentPolicyDenied:
            problem = _authorization_revoked_problem(run_id)
        except (AgentProposalFailure, AgentProviderFailure) as error:
            problem = error.problem
        return await self._record_run_failure(
            dispatch.workspace_id,
            run_id,
            prepared.run,
            prepared.job,
            problem,
        )

    async def _prepare_execution(
        self,
        dispatch: AgentRunExecutionClaim,
    ) -> _PreparedAgentExecution | AgentRunView:
        """@brief 在第一段短事务中重新授权并提交 running / Reauthorize and commit running in the first short transaction."""
        workspace_id = dispatch.workspace_id
        run_id = AgentRunId(dispatch.run_ref.id)
        started_at = self._clock.now()
        try:
            async with self._uow_factory(workspace_id, dispatch.actor_id) as uow:
                run = await _worker_run(uow, workspace_id, run_id, for_update=True)
                _require_worker_dispatch(run, dispatch)
                job = await _worker_job(uow, workspace_id, run.job_id, for_update=True)
                validate_run_job_alignment(run, job)
                early = await self._early_execution_result(uow, dispatch, run, job)
                if early is not None:
                    await uow.commit()
                    return early
                conversation, input_message = await _worker_run_input(
                    uow,
                    workspace_id,
                    run,
                )
                grant = await _refresh_execution_grant(
                    uow,
                    dispatch.actor_id,
                    workspace_id,
                    run,
                    conversation,
                    input_message,
                )
                if run.view.status is AgentRunStatus.QUEUED:
                    _require_queued_handoff_revision(
                        run,
                        job,
                        dispatch,
                        revision_offset=0,
                        phase=None,
                    )
                    started_run = run.start(at=started_at, grant=grant)
                    started_job = job.start(
                        at=started_at,
                        progress=JobProgress(
                            phase="model_execution",
                            completed=0,
                            total=None,
                            unit=JobProgressUnit.STEPS,
                        ),
                    )
                    validate_run_job_alignment(started_run, started_job)
                    await uow.repository.save_run(
                        started_run,
                        expected_revision=run.meta.revision,
                    )
                    await uow.jobs.save(
                        started_job,
                        expected_revision=job.meta.revision,
                    )
                    await uow.outbox.add(
                        self._run_state_dispatch(started_run, started_job, started_at)
                    )
                else:
                    if (
                        run.view.status is not AgentRunStatus.RUNNING
                        or job.progress is None
                        or job.progress.phase != "model_execution"
                    ):
                        raise AgentPortProtocolError(
                            "agent_run.worker_state_mismatch",
                            "agent run and job cannot be resumed from their current state",
                        )
                    _require_queued_handoff_revision(
                        run,
                        job,
                        dispatch,
                        revision_offset=1,
                        phase="model_execution",
                    )
                    if grant != run.grant:
                        raise AgentPolicyDenied(
                            "execution grant changed while a provider attempt was resumable"
                        )
                    started_run = run
                    started_job = job
                resume_context = await _load_resume_context(
                    uow,
                    workspace_id,
                    started_run,
                )
                await uow.commit()
                return _PreparedAgentExecution(
                    started_run,
                    started_job,
                    input_message,
                    resume_context,
                )
        except AgentCasMismatch as error:
            raise AgentConflict(
                "agent_run.worker_race",
                "agent run changed before execution started",
            ) from error

    async def _early_execution_result(
        self,
        uow: AgentUnitOfWork,
        dispatch: AgentRunExecutionClaim,
        run: AgentRun,
        job: Job,
    ) -> AgentRunView | None:
        """@brief 处理幂等重放中无需模型 I/O 的状态 / Handle replay states requiring no model I/O."""
        if run.is_terminal:
            return run.view
        if run.view.status is AgentRunStatus.WAITING_FOR_APPROVAL:
            _require_queued_handoff_revision(
                run,
                job,
                dispatch,
                revision_offset=2,
                phase="waiting_for_approval",
            )
            return run.view
        if (
            run.view.status is AgentRunStatus.RUNNING
            and job.progress is not None
            and job.progress.phase == "tool_decision_recorded"
        ):
            _require_queued_handoff_revision(
                run,
                job,
                dispatch,
                revision_offset=3,
                phase="tool_decision_recorded",
            )
            return run.view
        return None

    async def _build_provider_request(
        self,
        dispatch: AgentRunExecutionClaim,
        prepared: _PreparedAgentExecution,
    ) -> AgentProviderRequest:
        """@brief 在事务外取得授权证据并构造模型请求 / Retrieve authorized evidence and build a model request outside transactions."""
        evidence: tuple[AgentKnowledgeEvidence, ...] = ()
        if prepared.run.grant.knowledge_contexts:
            if self._knowledge_retriever is None:
                raise AgentProviderFailure(
                    _knowledge_retrieval_problem(prepared.run.meta.id)
                )
            try:
                evidence = await self._knowledge_retriever.retrieve(
                    AgentKnowledgeRetrievalRequest(
                        workspace_id=dispatch.workspace_id,
                        actor_id=dispatch.actor_id,
                        grant=prepared.run.grant,
                        query=_message_text(prepared.input_message),
                        top_k=20,
                    )
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                raise AgentProviderFailure(
                    _knowledge_retrieval_problem(prepared.run.meta.id)
                ) from error
        return AgentProviderRequest(
            run_id=prepared.run.meta.id,
            spec=prepared.run.spec,
            grant=prepared.run.grant,
            input_message=prepared.input_message,
            knowledge_evidence=evidence,
            resume_context=prepared.resume_context,
        )

    async def _commit_provider_outcome(
        self,
        dispatch: AgentRunExecutionClaim,
        prepared: _PreparedAgentExecution,
        request: AgentProviderRequest,
        outcome: AgentProviderOutcome,
    ) -> AgentRunView:
        """@brief 再次授权并原子提交 provider 结果 / Reauthorize and atomically commit a provider outcome."""
        workspace_id = dispatch.workspace_id
        finished_at = self._clock.now()
        try:
            async with self._uow_factory(workspace_id, prepared.run.created_by) as uow:
                current = await _worker_run(
                    uow,
                    workspace_id,
                    prepared.run.meta.id,
                    for_update=True,
                )
                current_job = await _worker_job(
                    uow,
                    workspace_id,
                    current.job_id,
                    for_update=True,
                )
                if current.view.status is AgentRunStatus.CANCELLED:
                    await uow.commit()
                    return current.view
                if (
                    current.meta.revision != prepared.run.meta.revision
                    or current_job.meta.revision != prepared.job.meta.revision
                ):
                    raise AgentConflict(
                        "agent_run.worker_race",
                        "agent run changed during provider execution",
                    )
                validate_run_job_alignment(current, current_job)
                conversation, input_message = await _worker_run_input(
                    uow,
                    workspace_id,
                    current,
                )
                grant = await _refresh_execution_grant(
                    uow,
                    dispatch.actor_id,
                    workspace_id,
                    current,
                    conversation,
                    input_message,
                )
                if grant != request.grant:
                    raise AgentPolicyDenied(
                        "execution authorization changed during provider execution"
                    )
                resume_context = await _load_resume_context(
                    uow,
                    workspace_id,
                    current,
                )
                if resume_context != request.resume_context:
                    raise AgentPolicyDenied(
                        "Resume context changed during provider execution"
                    )
                if isinstance(outcome, AgentProviderCompleted):
                    result = await self._commit_completion(
                        uow,
                        current,
                        current_job,
                        request,
                        outcome,
                        finished_at,
                    )
                else:
                    result = await self._commit_tool_approval(
                        uow,
                        current,
                        current_job,
                        request,
                        outcome,
                        finished_at,
                    )
                await uow.commit()
                return result
        except AgentCasMismatch as error:
            raise AgentConflict(
                "agent_run.worker_race",
                "agent run changed while provider result was committed",
            ) from error

    async def _commit_completion(
        self,
        uow: AgentUnitOfWork,
        run: AgentRun,
        job: Job,
        request: AgentProviderRequest,
        outcome: AgentProviderCompleted,
        finished_at: datetime,
    ) -> AgentRunView:
        """@brief 同事务物化 Message、可选 Proposal 与终态 / Materialize Message, optional Proposal, and terminal state together."""
        proposal_refs: tuple[ResourceRef, ...] = ()
        content = outcome.content
        if outcome.resume_operations:
            if request.resume_context is None or outcome.proposal_title is None:
                raise AgentProviderFailure(_provider_protocol_problem(run.meta.id))
            proposal_ref = await uow.resume_proposals.create(
                AgentResumeProposalCommand(
                    workspace_id=run.workspace_id,
                    actor_id=run.created_by,
                    run_id=run.meta.id,
                    base=request.resume_context,
                    title=outcome.proposal_title,
                    operations=outcome.resume_operations,
                    evidence=request.knowledge_evidence,
                    created_at=finished_at,
                )
            )
            proposal_refs = (proposal_ref,)
            content = (*content, ResumeProposalContentPart(proposal_ref))
        reservation = await uow.repository.allocate_message_sequence(
            run.workspace_id,
            run.spec.conversation_id,
            expected_conversation_revision=None,
            at=finished_at,
        )
        output_id = MessageId(self._ids("msg"))
        output = Message(
            meta=ResourceMeta(output_id, 1, finished_at, finished_at),
            workspace_id=run.workspace_id,
            conversation_id=run.spec.conversation_id,
            sequence=reservation.sequence,
            role=MessageRole.ASSISTANT,
            parent_message_id=run.spec.input_message_id,
            content=content,
            source_run_id=run.meta.id,
        )
        await uow.repository.add_message(output)
        completed_run = run.succeed(
            output_id,
            proposal_refs,
            outcome.usage,
            at=finished_at,
        )
        result_refs = [ResourceRef("message", output_id, 1), *proposal_refs]
        completed_job = job.succeed(result_refs, at=finished_at)
        validate_run_job_alignment(completed_run, completed_job)
        await uow.repository.save_run(
            completed_run,
            expected_revision=run.meta.revision,
        )
        await uow.jobs.save(completed_job, expected_revision=job.meta.revision)
        await uow.outbox.add(
            self._run_state_dispatch(completed_run, completed_job, finished_at)
        )
        return completed_run.view

    async def _commit_tool_approval(
        self,
        uow: AgentUnitOfWork,
        run: AgentRun,
        job: Job,
        request: AgentProviderRequest,
        outcome: AgentProviderApprovalRequired,
        finished_at: datetime,
    ) -> AgentRunView:
        """@brief 只为服务端注册的工具创建可达审批 / Create a reachable approval only for a server-registered tool."""
        if self._tool_registry is None or not self._tool_registry.allows(
            request,
            outcome.binding,
        ):
            raise AgentProviderFailure(_tool_unavailable_problem(run.meta.id))
        approval_id = ToolApprovalId(self._ids("approval"))
        approval = ToolApproval.create(
            ResourceMeta(approval_id, 1, finished_at, finished_at),
            run.workspace_id,
            run.meta.id,
            outcome.binding,
        )
        waiting_run = run.wait_for_tool(
            approval_id,
            outcome.binding.tool_call_id,
            at=finished_at,
        )
        waiting_job = job.report_progress(
            JobProgress(
                phase="waiting_for_approval",
                completed=0,
                total=None,
                unit=JobProgressUnit.STEPS,
            ),
            at=finished_at,
        )
        validate_run_job_alignment(waiting_run, waiting_job)
        await uow.repository.add_approval(approval)
        await uow.repository.save_run(
            waiting_run,
            expected_revision=run.meta.revision,
        )
        await uow.jobs.save(waiting_job, expected_revision=job.meta.revision)
        await uow.outbox.add(
            self._run_state_dispatch(waiting_run, waiting_job, finished_at)
        )
        return waiting_run.view

    async def _record_preflight_failure(
        self,
        dispatch: AgentRunExecutionClaim,
        problem: ProblemDetails,
    ) -> AgentRunView:
        """@brief 无 provider I/O 地闭合执行前失败 / Close a pre-provider failure without provider I/O.

        @param dispatch 原始 durable claim / Original durable claim.
        @param problem 已脱敏的授权或上下文问题 / Redacted authorization or context problem.
        @return failed、cancelled 或先前终态 Run / Failed, cancelled, or previously terminal Run.
        @note Job 不允许 ``queued → failed``，因此未开始工作在同一事务内先进入 running，
            再和 Run 一起失败。/ Because Jobs forbid ``queued → failed``, never-started work
            enters running and then fails with the Run in one transaction.
        """
        failed_at = self._clock.now()
        try:
            async with self._uow_factory(
                dispatch.workspace_id,
                dispatch.actor_id,
            ) as uow:
                run = await _worker_run(
                    uow,
                    dispatch.workspace_id,
                    AgentRunId(dispatch.run_ref.id),
                    for_update=True,
                )
                _require_worker_dispatch(run, dispatch)
                job = await _worker_job(
                    uow,
                    dispatch.workspace_id,
                    run.job_id,
                    for_update=True,
                )
                validate_run_job_alignment(run, job)
                if run.is_terminal:
                    await uow.commit()
                    return run.view
                if run.view.status is AgentRunStatus.QUEUED:
                    _require_queued_handoff_revision(
                        run,
                        job,
                        dispatch,
                        revision_offset=0,
                        phase=None,
                    )
                    active_run = run.start(at=failed_at)
                    active_job = job.start(
                        at=failed_at,
                        progress=JobProgress(
                            phase="authorization_failed",
                            completed=0,
                            total=None,
                            unit=JobProgressUnit.STEPS,
                        ),
                    )
                elif (
                    run.view.status is AgentRunStatus.RUNNING
                    and job.progress is not None
                    and job.progress.phase == "model_execution"
                ):
                    _require_queued_handoff_revision(
                        run,
                        job,
                        dispatch,
                        revision_offset=1,
                        phase="model_execution",
                    )
                    active_run = run
                    active_job = job
                else:
                    raise AgentPortProtocolError(
                        "agent_run.worker_state_mismatch",
                        "agent run cannot accept a preflight failure in its current state",
                    )
                failed_run = active_run.fail(problem, at=failed_at)
                failed_job = active_job.fail(problem, at=failed_at)
                validate_run_job_alignment(failed_run, failed_job)
                await uow.repository.save_run(
                    failed_run,
                    expected_revision=run.meta.revision,
                )
                await uow.jobs.save(
                    failed_job,
                    expected_revision=job.meta.revision,
                )
                await uow.outbox.add(
                    self._run_state_dispatch(failed_run, failed_job, failed_at)
                )
                await uow.commit()
                return failed_run.view
        except AgentCasMismatch as error:
            raise AgentConflict(
                "agent_run.worker_race",
                "agent run changed while a preflight failure was committed",
            ) from error

    async def _record_run_failure(
        self,
        workspace_id: WorkspaceId,
        run_id: AgentRunId,
        started_run: AgentRun,
        started_job: Job,
        problem: ProblemDetails,
    ) -> AgentRunView:
        """@brief 在独立短事务中终结执行失败 / Finalize an execution failure in a separate short transaction.

        @param workspace_id Run 所属 Workspace / Run Workspace.
        @param run_id Run 标识 / Run identifier.
        @param started_run 外部 I/O 前提交的 Run snapshot / Run snapshot committed before I/O.
        @param started_job 外部 I/O 前提交的 Job snapshot / Job snapshot committed before I/O.
        @param problem 已脱敏执行问题 / Redacted execution problem.
        @return failed 或并发取消后的 Run view / Failed or concurrently cancelled Run view.
        """
        failed_at = self._clock.now()
        try:
            async with self._uow_factory(workspace_id, started_run.created_by) as uow:
                current = await _worker_run(uow, workspace_id, run_id, for_update=True)
                current_job = await _worker_job(
                    uow,
                    workspace_id,
                    current.job_id,
                    for_update=True,
                )
                if current.view.status is AgentRunStatus.CANCELLED:
                    await uow.commit()
                    return current.view
                if (
                    current.meta.revision != started_run.meta.revision
                    or current_job.meta.revision != started_job.meta.revision
                ):
                    raise AgentConflict(
                        "agent_run.worker_race",
                        "agent run changed during provider execution",
                    )
                validate_run_job_alignment(current, current_job)
                failed_run = current.fail(problem, at=failed_at)
                failed_job = current_job.fail(problem, at=failed_at)
                validate_run_job_alignment(failed_run, failed_job)
                await uow.repository.save_run(
                    failed_run,
                    expected_revision=current.meta.revision,
                )
                await uow.jobs.save(
                    failed_job,
                    expected_revision=current_job.meta.revision,
                )
                await uow.outbox.add(
                    self._run_state_dispatch(failed_run, failed_job, failed_at)
                )
                await uow.commit()
                return failed_run.view
        except AgentCasMismatch as error:
            raise AgentConflict(
                "agent_run.worker_race",
                "agent run changed while provider failure was committed",
            ) from error

    def _run_state_dispatch(
        self,
        run: AgentRun,
        job: Job,
        occurred_at: datetime,
    ) -> AgentRunStateDispatch:
        """@brief 构造与 Run/Job revision 对齐的变化信号 / Build a change signal aligned to Run/Job revisions.

        @param run 已提交候选 Run / Candidate committed Run.
        @param job 已对齐统一 Job / Aligned unified Job.
        @param occurred_at 状态改变时刻 / State-change instant.
        @return 不含 prompt、tool 参数或推理的 outbox record / Outbox record without prompt,
            tool arguments, or reasoning.
        """
        validate_run_job_alignment(run, job)
        return AgentRunStateDispatch(
            id=AgentOutboxId(self._ids("outbox")),
            workspace_id=run.workspace_id,
            actor_id=run.created_by,
            run_ref=ResourceRef("agent_run", run.meta.id, run.meta.revision),
            job_ref=ResourceRef("job", job.meta.id, job.meta.revision),
            status=run.view.status,
            occurred_at=occurred_at,
        )

    async def fail_exhausted(
        self,
        dispatch: AgentRunExhaustionClaim,
    ) -> AgentRunView:
        """@brief 幂等闭合已耗尽 outbox 事件拥有的 Agent 工作 / Idempotently close Agent work owned by an exhausted outbox event.

        @param dispatch 仅依赖 outbox header 的可信最小 claim / Trusted minimal claim derived
            only from the outbox header.
        @return 已取消、失败或先前已终结的 Run / Cancelled, failed, or previously terminal Run.
        @note V2 禁止 ``queued -> failed``，故从未开始的 Run/Job 取消；已运行或等待审批的
            Run/Job 失败。待决 ToolApproval 与二者在同一事务中关闭。/ V2 forbids
            ``queued -> failed``, so never-started Run/Job pairs are cancelled while running or
            approval-waiting pairs fail. A pending ToolApproval is closed in the same transaction.
        """
        terminal_at = self._clock.now()
        try:
            async with self._uow_factory(dispatch.workspace_id, dispatch.actor_id) as uow:
                run = await _worker_run(
                    uow,
                    dispatch.workspace_id,
                    AgentRunId(dispatch.run_ref.id),
                    for_update=True,
                )
                _require_exhaustion_dispatch(run, dispatch)
                job = await _worker_job(
                    uow,
                    dispatch.workspace_id,
                    run.job_id,
                    for_update=True,
                )
                validate_run_job_alignment(run, job)
                if run.is_terminal:
                    await uow.commit()
                    return run.view

                if run.view.pending_approval_id is not None:
                    approval = await uow.repository.get_approval(
                        dispatch.workspace_id,
                        run.view.pending_approval_id,
                        for_update=True,
                    )
                    if approval is None or not approval.matches_waiting_run(run):
                        raise AgentPortProtocolError(
                            "agent.exhaustion_approval_mismatch",
                            "exhausted agent run is missing its pending approval",
                        )
                    closed_approval = (
                        approval.expire(_service_ref(), at=terminal_at)
                        if terminal_at >= approval.view.expires_at
                        else approval.decide(
                            ToolDecision.REJECT,
                            _service_ref(),
                            at=terminal_at,
                        )
                    )
                    await uow.repository.save_approval(
                        closed_approval,
                        expected_revision=approval.meta.revision,
                    )

                if run.view.status is AgentRunStatus.QUEUED:
                    terminal_run = run.cancel(at=terminal_at)
                    terminal_job = job.cancel(at=terminal_at)
                else:
                    problem = _dispatch_exhausted_problem(dispatch.id)
                    terminal_run = run.fail(problem, at=terminal_at)
                    terminal_job = job.fail(problem, at=terminal_at)
                validate_run_job_alignment(terminal_run, terminal_job)
                await uow.repository.save_run(
                    terminal_run,
                    expected_revision=run.meta.revision,
                )
                await uow.jobs.save(
                    terminal_job,
                    expected_revision=job.meta.revision,
                )
                await uow.outbox.add(
                    self._run_state_dispatch(terminal_run, terminal_job, terminal_at)
                )
                await uow.commit()
                return terminal_run.view
        except AgentCasMismatch as error:
            raise AgentConflict(
                "agent_run.worker_race",
                "agent run changed while outbox exhaustion was finalized",
            ) from error

    async def execute_approved_tool(
        self,
        dispatch: AgentToolDecisionClaim,
    ) -> AgentRunView:
        """@brief 幂等消费已提交工具决定并把 Run/Job 推入终态 / Idempotently consume a committed tool decision and terminate its Run/Job.

        @param dispatch 严格绑定 event、Run、Job、Approval 与 tool call 的 claim / Claim strictly
            binding the event, Run, Job, Approval, and tool call.
        @return succeeded、failed 或并发 cancelled 的 Run / Succeeded, failed, or concurrently
            cancelled Run.
        @note 工具 I/O 只发生在验证事务和终结事务之间；event ID 是 adapter 的稳定幂等键。
            Reject 不执行工具并持久失败；任意未分类工具异常也持久为公开安全终态，避免
            ``running`` 永久悬挂。/ Tool I/O occurs only between the validation and finalization
            transactions, with the event ID serving as the adapter's stable idempotency key. A reject
            never executes the tool and is persisted as failed; any unclassified tool exception is
            likewise persisted as a public-safe terminal failure rather than stranding ``running``.
        """

        prepared = await self._prepare_tool_decision(dispatch)
        if isinstance(prepared, AgentRunView):
            return prepared
        if dispatch.decision is ToolDecision.REJECT:
            return await self._record_tool_failure(
                dispatch,
                _tool_rejected_problem(dispatch.id),
            )
        try:
            receipt = await self._tool_executor.execute(dispatch, prepared)
        except asyncio.CancelledError:
            raise
        except Exception:
            return await self._record_tool_failure(
                dispatch,
                _tool_execution_failed_problem(dispatch.id),
            )
        try:
            _validate_tool_receipt(dispatch, receipt)
        except (AgentDomainError, TypeError, ValueError):
            return await self._record_tool_failure(
                dispatch,
                _tool_execution_failed_problem(dispatch.id),
            )
        return await self._record_tool_success(dispatch, receipt)

    async def _prepare_tool_decision(
        self,
        dispatch: AgentToolDecisionClaim,
    ) -> ResourceRef | AgentRunView:
        """@brief 在短事务中验证决定并取得私有 invocation ref / Validate a decision and load its private invocation ref in a short transaction."""

        async with self._uow_factory(dispatch.workspace_id, dispatch.actor_id) as uow:
            run, job, approval = await _worker_tool_decision_state(uow, dispatch)
            _require_tool_decision_binding(run, job, approval, dispatch)
            await uow.commit()
            return run.view if run.is_terminal else approval.binding.invocation_ref

    async def _record_tool_failure(
        self,
        dispatch: AgentToolDecisionClaim,
        problem: ProblemDetails,
    ) -> AgentRunView:
        """@brief 原子持久化 reject/unavailable 工具终态 / Atomically persist a rejected or unavailable tool terminal state."""

        failed_at = self._clock.now()
        try:
            async with self._uow_factory(dispatch.workspace_id, dispatch.actor_id) as uow:
                run, job, approval = await _worker_tool_decision_state(uow, dispatch)
                _require_tool_decision_binding(run, job, approval, dispatch)
                if run.is_terminal:
                    await uow.commit()
                    return run.view
                failed_run = run.fail(problem, at=failed_at)
                failed_job = job.fail(problem, at=failed_at)
                validate_run_job_alignment(failed_run, failed_job)
                await uow.repository.save_run(
                    failed_run,
                    expected_revision=run.meta.revision,
                )
                await uow.jobs.save(
                    failed_job,
                    expected_revision=job.meta.revision,
                )
                await uow.outbox.add(
                    self._run_state_dispatch(failed_run, failed_job, failed_at)
                )
                await uow.commit()
                return failed_run.view
        except AgentCasMismatch as error:
            raise AgentConflict(
                "agent_run.worker_race",
                "agent run changed while a tool decision was being finalized",
            ) from error

    async def _record_tool_success(
        self,
        dispatch: AgentToolDecisionClaim,
        receipt: ToolExecutionReceipt,
    ) -> AgentRunView:
        """@brief 原子追加公开回执 Message 并完成 Run/Job / Atomically append a public receipt Message and complete the Run/Job."""

        finished_at = self._clock.now()
        try:
            async with self._uow_factory(dispatch.workspace_id, dispatch.actor_id) as uow:
                run, job, approval = await _worker_tool_decision_state(uow, dispatch)
                _require_tool_decision_binding(run, job, approval, dispatch)
                if run.is_terminal:
                    await uow.commit()
                    return run.view
                conversation = await uow.repository.get_conversation(
                    dispatch.workspace_id,
                    run.spec.conversation_id,
                    for_update=True,
                    include_deleted=True,
                )
                if conversation is None or not conversation.is_writable:
                    cancelled_run = run.cancel(at=finished_at)
                    cancelled_job = job.cancel(at=finished_at)
                    validate_run_job_alignment(cancelled_run, cancelled_job)
                    await uow.repository.save_run(
                        cancelled_run,
                        expected_revision=run.meta.revision,
                    )
                    await uow.jobs.save(
                        cancelled_job,
                        expected_revision=job.meta.revision,
                    )
                    await uow.outbox.add(
                        self._run_state_dispatch(cancelled_run, cancelled_job, finished_at)
                    )
                    await uow.commit()
                    return cancelled_run.view
                reservation = await uow.repository.allocate_message_sequence(
                    dispatch.workspace_id,
                    run.spec.conversation_id,
                    expected_conversation_revision=None,
                    at=finished_at,
                )
                output_id = MessageId(self._ids("msg"))
                output = Message(
                    meta=ResourceMeta(output_id, 1, finished_at, finished_at),
                    workspace_id=dispatch.workspace_id,
                    conversation_id=run.spec.conversation_id,
                    sequence=reservation.sequence,
                    role=MessageRole.ASSISTANT,
                    parent_message_id=run.spec.input_message_id,
                    content=(TextContentPart(receipt.summary),),
                    source_run_id=run.meta.id,
                )
                await uow.repository.add_message(output)
                proposal_refs = tuple(
                    reference
                    for reference in receipt.result_refs
                    if reference.resource_type == "resume_proposal"
                    or reference.resource_type.endswith(".proposal")
                )
                completed_run = run.succeed(
                    output_id,
                    proposal_refs,
                    AgentUsage(0, 0, "0"),
                    at=finished_at,
                )
                completed_job = job.succeed(receipt.result_refs, at=finished_at)
                validate_run_job_alignment(completed_run, completed_job)
                await uow.repository.save_run(
                    completed_run,
                    expected_revision=run.meta.revision,
                )
                await uow.jobs.save(
                    completed_job,
                    expected_revision=job.meta.revision,
                )
                await uow.outbox.add(
                    self._run_state_dispatch(completed_run, completed_job, finished_at)
                )
                await uow.commit()
                return completed_run.view
        except AgentCasMismatch as error:
            raise AgentConflict(
                "agent_run.worker_race",
                "agent run changed while a tool result was being committed",
            ) from error


async def _worker_run_input(
    uow: AgentUnitOfWork,
    workspace_id: WorkspaceId,
    run: AgentRun,
) -> tuple[Conversation, Message]:
    """@brief 读取并重验 Run 的精确会话与输入 / Load and revalidate the exact conversation and input for a Run.

    @param uow 当前短事务 / Current short transaction.
    @param workspace_id durable claim Workspace / Durable-claim Workspace.
    @param run 已锁定 Run / Locked Run.
    @return 可执行 Conversation 与原始用户 Message / Executable Conversation and original user Message.
    @raise AgentPolicyDenied 会话已不可用时抛出 / Raised when the conversation is no longer available.
    @raise AgentPortProtocolError 输入持久状态损坏时抛出 / Raised when persisted input state is corrupt.
    """
    conversation = await uow.repository.get_conversation(
        workspace_id,
        run.spec.conversation_id,
        for_update=True,
        include_deleted=True,
    )
    if conversation is None or not conversation.is_writable:
        raise AgentPolicyDenied("agent run conversation is unavailable")
    input_message = await uow.repository.get_message(
        workspace_id,
        run.spec.conversation_id,
        run.spec.input_message_id,
    )
    if (
        input_message is None
        or input_message.workspace_id != workspace_id
        or input_message.conversation_id != run.spec.conversation_id
        or input_message.role is not MessageRole.USER
    ):
        raise AgentPortProtocolError(
            "agent_run.input_missing",
            "agent run input message is missing or invalid",
        )
    return conversation, input_message


async def _refresh_execution_grant(
    uow: AgentUnitOfWork,
    actor_id: UserId,
    workspace_id: WorkspaceId,
    run: AgentRun,
    conversation: Conversation,
    input_message: Message,
) -> AgentExecutionGrant:
    """@brief 根据当前策略重新计算精确 grant / Recompute the exact grant from current policy.

    @param uow 当前短事务 / Current short transaction.
    @param actor_id durable claim 中的 creator / Creator from the durable claim.
    @param workspace_id 当前 Workspace / Current Workspace.
    @param run 不可变执行请求的 Run / Run carrying the immutable execution request.
    @param conversation 当前会话 snapshot / Current conversation snapshot.
    @param input_message 原始输入 Message / Original input Message.
    @return 经领域交叉校验的当前 grant / Current grant cross-validated by the domain.
    """
    try:
        grant = await uow.policy.authorize_run(
            AgentRunPolicyRequest(
                actor_id=actor_id,
                workspace_id=workspace_id,
                conversation=conversation,
                input_message=input_message,
                spec=run.spec,
            )
        )
        grant.validate_for(conversation, run.spec)
    except AgentPolicyDenied:
        raise
    except AgentDomainError as error:
        raise AgentPolicyDenied("execution grant failed domain validation") from error
    return grant


async def _load_resume_context(
    uow: AgentUnitOfWork,
    workspace_id: WorkspaceId,
    run: AgentRun,
) -> AgentResumeContext | None:
    """@brief 为 Resume 输出锁定精确基础 SIR / Lock the exact base SIR for Resume output.

    @param uow 当前短事务 / Current short transaction.
    @param workspace_id 当前 Workspace / Current Workspace.
    @param run 已验证 Run / Validated Run.
    @return 非 Resume 模式返回空，否则返回精确 SIR / None outside Resume mode, otherwise the exact SIR.
    """
    if AgentOutputMode.RESUME_OPERATIONS not in run.spec.output_modes:
        return None
    return await uow.resume_proposals.load_base(
        workspace_id,
        run.spec.context_refs[0],
    )


def _message_text(message: Message) -> str:
    """@brief 提取有界检索查询，不携带结构化私有状态 / Extract a bounded retrieval query without structured private state.

    @param message 精确用户 Message / Exact user Message.
    @return 最多 8000 字符的非空查询 / Non-empty query of at most 8,000 characters.
    """
    value = "\n".join(
        part.text for part in message.content if isinstance(part, TextContentPart)
    ).strip()
    if not value:
        raise AgentDomainError("agent input contains no searchable text")
    return value[:8_000]


async def _worker_run(
    uow: AgentUnitOfWork,
    workspace_id: WorkspaceId,
    run_id: AgentRunId,
    *,
    for_update: bool,
) -> AgentRun:
    """@brief Worker 内部读取精确 Workspace Run / Read an exact Workspace Run for a worker."""
    run = await uow.repository.get_run(workspace_id, run_id, for_update=for_update)
    if run is None or run.workspace_id != workspace_id:
        raise AgentResourceNotFound("agent_run")
    return run


def _require_worker_dispatch(run: AgentRun, dispatch: AgentRunExecutionClaim) -> None:
    """@brief 验证 durable dispatch 精确绑定 Run 创建者与 Job / Verify a durable dispatch binds the Run creator and Job exactly.

    @param run 在 dispatch scope 内读取的 Run / Run loaded inside the dispatch scope.
    @param dispatch 已提交的排队信号 / Committed queued dispatch.
    @return 无返回值 / No return value.
    """
    if (
        run.workspace_id != dispatch.workspace_id
        or run.meta.id != dispatch.run_ref.id
        or run.job_id != dispatch.job_ref.id
        or run.created_by != dispatch.actor_id
    ):
        raise AgentPortProtocolError(
            "agent_run.worker_dispatch_mismatch",
            "agent run does not match its durable dispatch",
        )


def _require_exhaustion_dispatch(
    run: AgentRun,
    dispatch: AgentRunExhaustionClaim,
) -> None:
    """@brief 验证耗尽补偿仍绑定同一 Run 与 creator / Verify exhaustion compensation still binds the same Run and creator.

    @param run 在 worker scope 内锁定的 Run / Run locked inside the worker scope.
    @param dispatch payload 独立的耗尽 claim / Payload-independent exhaustion claim.
    @raise AgentPortProtocolError subject、actor 或 revision 不可能属于该 Run 时抛出 / Raised
        when the subject, actor, or revision cannot belong to this Run.
    """
    subject_revision = dispatch.run_ref.revision
    if (
        dispatch.run_ref.resource_type != "agent_run"
        or subject_revision is None
        or run.workspace_id != dispatch.workspace_id
        or run.meta.id != dispatch.run_ref.id
        or run.created_by != dispatch.actor_id
        or run.meta.revision < subject_revision
    ):
        raise AgentPortProtocolError(
            "agent_run.exhaustion_dispatch_mismatch",
            "agent run does not match its exhausted durable dispatch",
        )


def _require_queued_handoff_revision(
    run: AgentRun,
    job: Job,
    dispatch: AgentRunExecutionClaim,
    *,
    revision_offset: int,
    phase: str | None,
) -> None:
    """@brief 证明 queued 事件仍拥有当前步骤或已明确交棒 / Prove the queued event still owns the current step or has explicitly handed it off.

    @param run 当前 Run / Current Run.
    @param job 当前统一 Job / Current unified Job.
    @param dispatch 原始 queued claim / Original queued claim.
    @param revision_offset 从初始 revision 到当前阶段的确定增量 / Deterministic revision
        increment from the initial revision to the current phase.
    @param phase 期望 Job phase；queued 阶段为 null / Expected Job phase, or null for queued.
    @raise AgentPortProtocolError revision 或 phase 不符合状态机时抛出 / Raised when revisions
        or phase do not match the state machine.
    @note ``model_execution`` 仍归 queued 事件；``waiting_for_approval`` 与
        ``tool_decision_recorded`` 已交给人工决定链。/ ``model_execution`` remains owned by the
        queued event; ``waiting_for_approval`` and ``tool_decision_recorded`` have handed off to the
        human-decision chain.
    """

    run_revision = dispatch.run_ref.revision
    job_revision = dispatch.job_ref.revision
    progress_matches = job.progress is None
    if phase is not None:
        progress_matches = job.progress is not None and job.progress.phase == phase
    if (
        run_revision is None
        or job_revision is None
        or run.meta.revision != run_revision + revision_offset
        or job.meta.revision != job_revision + revision_offset
        or not progress_matches
    ):
        raise AgentPortProtocolError(
            "agent_run.worker_revision_mismatch",
            "agent queued dispatch does not own the current run and job revisions",
        )


async def _worker_job(
    uow: AgentUnitOfWork,
    workspace_id: WorkspaceId,
    job_id: JobId,
    *,
    for_update: bool,
) -> Job:
    """@brief Worker 内部读取精确统一 Job / Read an exact unified Job for a worker."""
    job = await uow.jobs.get(workspace_id, job_id, for_update=for_update)
    if job is None or job.workspace_id != workspace_id:
        raise AgentPortProtocolError(
            "agent.job_protocol_violation",
            "agent run is missing its unified Job",
        )
    return job


async def _worker_tool_decision_state(
    uow: AgentUnitOfWork,
    dispatch: AgentToolDecisionClaim,
) -> tuple[AgentRun, Job, ToolApproval]:
    """@brief 锁定工具决定涉及的三个聚合 / Lock the three aggregates bound by a tool decision.

    @param uow 当前短事务工作单元 / Current short-transaction unit of work.
    @param dispatch 已严格解析的 durable claim / Strictly parsed durable claim.
    @return Run、统一 Job 与 ToolApproval / Run, unified Job, and ToolApproval.
    @raise AgentPortProtocolError Approval 不存在或越过 Workspace 时抛出 / Raised when the
        Approval is absent or crosses the Workspace boundary.
    """

    run = await _worker_run(
        uow,
        dispatch.workspace_id,
        AgentRunId(dispatch.run_ref.id),
        for_update=True,
    )
    job = await _worker_job(
        uow,
        dispatch.workspace_id,
        JobId(dispatch.job_ref.id),
        for_update=True,
    )
    approval = await uow.repository.get_approval(
        dispatch.workspace_id,
        ToolApprovalId(dispatch.approval_ref.id),
        for_update=True,
    )
    if approval is None or approval.workspace_id != dispatch.workspace_id:
        raise AgentPortProtocolError(
            "agent.tool_approval_protocol_violation",
            "agent tool decision is missing its approval",
        )
    return run, job, approval


def _require_tool_decision_binding(
    run: AgentRun,
    job: Job,
    approval: ToolApproval,
    dispatch: AgentToolDecisionClaim,
) -> None:
    """@brief 验证事件、聚合身份、revision 与决定状态完全一致 / Verify exact event, aggregate identity, revision, and decision state.

    @param run 已锁定 Run / Locked Run.
    @param job 已锁定统一 Job / Locked unified Job.
    @param approval 已锁定 ToolApproval / Locked ToolApproval.
    @param dispatch durable 工具决定 claim / Durable tool-decision claim.
    @raise AgentPortProtocolError 任一绑定漂移、旧事件或非法重放时抛出 / Raised for any
        binding drift, stale event, or invalid replay.
    @note 唯一允许的重放状态是 Run/Job 已由该决定推进一个 revision 后处于对齐终态。
        / The only accepted replay is an aligned terminal Run/Job exactly one revision after this
        decision.
    """

    expected_approval_status = (
        ToolApprovalStatus.APPROVED
        if dispatch.decision is ToolDecision.APPROVE
        else ToolApprovalStatus.REJECTED
    )
    run_revision = dispatch.run_ref.revision
    job_revision = dispatch.job_ref.revision
    approval_revision = dispatch.approval_ref.revision
    if run_revision is None or job_revision is None or approval_revision is None:
        raise AgentPortProtocolError(
            "agent.tool_decision_binding_mismatch",
            "agent tool decision requires exact aggregate revisions",
        )
    common_matches = (
        dispatch.run_ref.resource_type == "agent_run"
        and dispatch.job_ref.resource_type == "job"
        and dispatch.approval_ref.resource_type == "tool_approval"
        and run.workspace_id == dispatch.workspace_id
        and run.created_by == dispatch.actor_id
        and run.meta.id == dispatch.run_ref.id
        and run.job_id == dispatch.job_ref.id
        and job.meta.id == dispatch.job_ref.id
        and approval.meta.id == dispatch.approval_ref.id
        and approval.meta.revision == approval_revision
        and approval.workspace_id == dispatch.workspace_id
        and approval.view.run_id == run.meta.id
        and approval.binding.tool_call_id == dispatch.tool_call_id
        and approval.view.status is expected_approval_status
    )
    if not common_matches:
        raise AgentPortProtocolError(
            "agent.tool_decision_binding_mismatch",
            "agent tool decision does not match its durable aggregate bindings",
        )
    validate_run_job_alignment(run, job)
    if run.is_terminal:
        if (
            run.meta.revision != run_revision + 1
            or job.meta.revision != job_revision + 1
        ):
            raise AgentPortProtocolError(
                "agent.tool_decision_replay_invalid",
                "terminal agent run is not the direct result of this tool decision",
            )
        return
    if (
        run.view.status is not AgentRunStatus.RUNNING
        or run.meta.revision != run_revision
        or job.meta.revision != job_revision
    ):
        raise AgentPortProtocolError(
            "agent.tool_decision_revision_mismatch",
            "agent tool decision revisions do not match the running aggregates",
        )


def _validate_tool_receipt(
    dispatch: AgentToolDecisionClaim,
    receipt: ToolExecutionReceipt,
) -> None:
    """@brief 在持久化前验证外部工具回执 / Validate an external-tool receipt before persistence.

    @param dispatch 已批准调用的 durable claim / Durable claim for the approved invocation.
    @param receipt 不可信 adapter 回执 / Untrusted adapter receipt.
    @raise TypeError adapter 返回错误类型时抛出 / Raised when the adapter returns the wrong type.
    @raise ValueError 回执绑定错误或引用重复时抛出 / Raised for a mismatched receipt or duplicate refs.
    """

    if not isinstance(receipt, ToolExecutionReceipt):
        raise TypeError("Agent tool executor returned an invalid receipt")
    if receipt.tool_call_id != dispatch.tool_call_id:
        raise ValueError("Agent tool receipt does not match the approved tool call")
    TextContentPart(receipt.summary)
    ref_keys = tuple(
        (reference.resource_type, reference.id, reference.revision)
        for reference in receipt.result_refs
    )
    if len(set(ref_keys)) != len(ref_keys):
        raise ValueError("Agent tool receipt result references must be unique")


def _workspace_ref(workspace_id: WorkspaceId) -> ResourceRef:
    """@brief 构造精确 Workspace target / Build an exact Workspace target."""
    return ResourceRef("workspace", workspace_id)


def _conversation_ref(conversation: Conversation) -> ResourceRef:
    """@brief 构造带 revision 的 Conversation target / Build a revision-bearing Conversation target."""
    return ResourceRef("conversation", conversation.meta.id, conversation.meta.revision)


def _actor_ref(principal: TokenPrincipal) -> ResourceRef:
    """@brief 从已验证 principal 构造审计 actor / Build an audit actor from a verified principal."""
    return ResourceRef("user", principal.user_id)


def _service_ref() -> ResourceRef:
    """@brief 构造自动到期动作的服务 actor / Build the service actor for automatic expiry."""
    return ResourceRef("service", "agent_service")


def _tool_approval_expired_problem(request_id: str) -> ProblemDetails:
    """@brief 构造不泄漏工具参数的 expiry ProblemDetails / Build expiry ProblemDetails without tool-argument leakage."""
    return ProblemDetails(
        type_uri="https://api.hmalliances.org:8022/problems/agent/tool-approval-expired",
        title="Tool approval expired",
        status=409,
        code="tool_approval.expired",
        request_id=request_id,
        retryable=False,
        detail="The tool approval expired before a decision was recorded.",
    )


def _unexpected_provider_problem(run_id: AgentRunId) -> ProblemDetails:
    """@brief 构造不泄漏异常内容的 provider failure / Build a provider failure without leaking exception text.

    @param run_id 作为稳定失败关联的 Run ID / Run ID used as stable failure correlation.
    @return 可安全持久化的问题 / Problem safe to persist.
    """
    return ProblemDetails(
        type_uri="https://api.hmalliances.org:8022/problems/agent/provider-failed",
        title="Model provider failed",
        status=503,
        code="agent.provider_failed",
        request_id=str(run_id),
        retryable=True,
        detail="The model provider could not complete this run.",
    )


def _provider_protocol_problem(run_id: AgentRunId) -> ProblemDetails:
    """@brief 构造模型结构化输出违规问题 / Build a structured-output protocol violation.

    @param run_id 稳定关联 Run / Stable correlated Run.
    @return 不包含模型原文的问题 / Problem containing no model output.
    """
    return ProblemDetails(
        type_uri="https://api.hmalliances.org:8022/problems/agent/provider-protocol-error",
        title="Model provider returned an invalid structured response",
        status=502,
        code="agent.provider_protocol_error",
        request_id=str(run_id),
        retryable=False,
        detail="The model response did not satisfy the requested output contract.",
    )


def _authorization_revoked_problem(run_id: AgentRunId) -> ProblemDetails:
    """@brief 构造执行时授权收紧问题 / Build an execution-time authorization tightening problem.

    @param run_id 稳定关联 Run / Stable correlated Run.
    @return 不泄漏 policy 细节的问题 / Problem without policy-internal detail.
    """
    return ProblemDetails(
        type_uri=(
            "https://api.hmalliances.org:8022/problems/"
            "agent/execution-authorization-revoked"
        ),
        title="Agent execution is no longer authorized",
        status=403,
        code="agent.execution_authorization_revoked",
        request_id=str(run_id),
        retryable=False,
        detail="Workspace, resource, Knowledge, or model policy changed before completion.",
    )


def _knowledge_retrieval_problem(run_id: AgentRunId) -> ProblemDetails:
    """@brief 构造授权 Knowledge 检索不可用问题 / Build an authorized-Knowledge retrieval problem.

    @param run_id 稳定关联 Run / Stable correlated Run.
    @return 可安全持久化的问题 / Safely persistable problem.
    """
    return ProblemDetails(
        type_uri="https://api.hmalliances.org:8022/problems/agent/knowledge-unavailable",
        title="Authorized Knowledge retrieval is unavailable",
        status=503,
        code="agent.knowledge_retrieval_failed",
        request_id=str(run_id),
        retryable=True,
        detail="The Agent could not retrieve the authorized evidence for this run.",
    )


def _tool_unavailable_problem(run_id: AgentRunId) -> ProblemDetails:
    """@brief 构造未注册工具建议的失败 / Build a failure for an unregistered tool suggestion.

    @param run_id 稳定关联 Run / Stable correlated Run.
    @return 明确且不暴露 invocation 的问题 / Explicit problem without invocation data.
    """
    return ProblemDetails(
        type_uri="https://api.hmalliances.org:8022/problems/agent/tool-unavailable",
        title="Requested Agent tool is unavailable",
        status=422,
        code="agent.tool_unavailable",
        request_id=str(run_id),
        retryable=False,
        detail="This deployment has not registered the proposed tool for this Agent request.",
    )


def _tool_rejected_problem(event_id: AgentOutboxId) -> ProblemDetails:
    """@brief 构造用户拒绝工具后的持久终态 / Build the durable terminal result for a rejected tool.

    @param event_id 决定事件的稳定关联 ID / Stable correlation ID of the decision event.
    @return 不含工具参数的公开 ProblemDetails / Public ProblemDetails without tool arguments.
    """

    return ProblemDetails(
        type_uri="https://api.hmalliances.org:8022/problems/agent/tool-call-rejected",
        title="Tool call rejected",
        status=409,
        code="tool_approval.rejected",
        request_id=str(event_id),
        retryable=False,
        detail="The Agent run was stopped because the proposed tool call was rejected.",
    )


def _tool_execution_failed_problem(event_id: AgentOutboxId) -> ProblemDetails:
    """@brief 构造工具不可用或执行失败的脱敏终态 / Build a redacted terminal result for unavailable or failed tool execution.

    @param event_id 工具操作的稳定幂等与关联 ID / Stable idempotency and correlation ID for the tool operation.
    @return 不泄漏 adapter 异常的公开 ProblemDetails / Public ProblemDetails without adapter errors.
    """

    return ProblemDetails(
        type_uri="https://api.hmalliances.org:8022/problems/agent/tool-execution-failed",
        title="Tool execution failed",
        status=503,
        code="agent.tool_execution_failed",
        request_id=str(event_id),
        retryable=False,
        detail="The approved tool call could not be completed by this deployment.",
    )


def _dispatch_exhausted_problem(event_id: AgentOutboxId) -> ProblemDetails:
    """@brief 构造 outbox 尝试耗尽的公开安全终态 / Build a public-safe terminal problem for exhausted outbox attempts.

    @param event_id 耗尽事件的稳定关联 ID / Stable correlation ID of the exhausted event.
    @return 不泄漏异常正文或私有 payload 的 ProblemDetails / Problem Details without exception
        text or private payload data.
    """
    return ProblemDetails(
        type_uri="https://api.hmalliances.org:8022/problems/agent/dispatch-exhausted",
        title="Agent execution could not be completed",
        status=503,
        code="agent.dispatch_exhausted",
        request_id=str(event_id),
        retryable=False,
        detail="The Agent run stopped after its durable execution attempts were exhausted.",
    )


def _require_revision(actual: int, expected: int) -> None:
    """@brief 校验强 If-Match revision / Validate a strong If-Match revision."""
    if expected < 1 or actual != expected:
        raise AgentPreconditionFailed


def _require_message_scope(
    message: Message,
    workspace_id: WorkspaceId,
    conversation_id: ConversationId,
) -> None:
    """@brief 二次校验 Message Workspace/Conversation 归属 / Revalidate Message Workspace/Conversation ownership."""
    if message.workspace_id != workspace_id or message.conversation_id != conversation_id:
        raise AgentResourceNotFound("message")


__all__ = [
    "V2_AGENT_ENDPOINT_METHODS",
    "AgentApplicationError",
    "AgentApplicationService",
    "AgentConflict",
    "AgentMutationContext",
    "AgentPortProtocolError",
    "AgentPreconditionFailed",
    "AgentResourceNotFound",
    "AgentWorkerService",
    "Clock",
    "CreateConversationCommand",
    "CreateMessageCommand",
    "InvalidAgentCommand",
    "NewOpaqueIdFactory",
    "OpaqueIdFactory",
    "ToolApprovalDecisionCommand",
    "UtcClock",
]
