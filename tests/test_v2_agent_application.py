"""API v2 Conversation 与 Agent 应用核心测试。"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta

import pytest

from backend.application.agent_v2 import (
    V2_AGENT_ENDPOINT_METHODS,
    AgentApplicationService,
    AgentConflict,
    AgentMutationContext,
    AgentPortProtocolError,
    AgentPreconditionFailed,
    AgentResourceNotFound,
    AgentWorkerService,
    CreateConversationCommand,
    CreateMessageCommand,
    ToolApprovalDecisionCommand,
)
from backend.application.ports.agent_v2 import (
    AgentCasMismatch,
    AgentPage,
    AgentPageRequest,
    AgentPermissionGrant,
    AgentPermissionRequest,
    AgentPolicyDenied,
    AgentResumeProposalCommand,
    AgentRunPolicyRequest,
    AgentToolDecisionClaim,
    AgentToolExecutor,
    MessageSequenceReservation,
    ToolExecutionReceipt,
)
from backend.domain.agent_v2 import (
    AgentExecutionGrant,
    AgentOutputMode,
    AgentProviderApprovalRequired,
    AgentProviderCompleted,
    AgentProviderRequest,
    AgentResumeContext,
    AgentRun,
    AgentRunId,
    AgentRunQueuedDispatch,
    AgentRunSpec,
    AgentRunStatus,
    AgentUsage,
    Conversation,
    ConversationCapability,
    ConversationId,
    ConversationPatch,
    Message,
    MessageId,
    TextContentPart,
    ToolApproval,
    ToolApprovalExpiredDispatch,
    ToolApprovalId,
    ToolCallBinding,
    ToolCallId,
    ToolDecision,
    ToolDecisionDispatch,
    ToolRisk,
)
from backend.domain.knowledge_retrieval import (
    InferenceCostTier,
    InferenceIntent,
    InferenceQualityTier,
    KnowledgeSelection,
    KnowledgeSelectionMode,
)
from backend.domain.knowledge_sources import ModelRegion
from backend.domain.platform import AuditEvent, Job, JobId
from backend.domain.principals import (
    ClientId,
    ResourceMeta,
    Scope,
    Subject,
    TokenPrincipal,
    UserId,
    WorkspaceId,
)
from backend.domain.resources import ResourceRef

NOW = datetime(2026, 7, 23, 2, 0, tzinfo=UTC)
WORKSPACE = WorkspaceId("workspace_0001")
OTHER_WORKSPACE = WorkspaceId("workspace_0002")
PRINCIPAL = TokenPrincipal(
    UserId("user_actor_0001"),
    Subject("subject_actor_0001"),
    ClientId("client_actor_0001"),
    frozenset({Scope("agent.read"), Scope("agent.write")}),
)
CONTEXT = AgentMutationContext("request_0001")


class FixedClock:
    def now(self) -> datetime:
        return NOW


class OffsetClock:
    def __init__(self, delta: timedelta) -> None:
        self.delta = delta

    def now(self) -> datetime:
        return NOW + self.delta


class DeterministicIds:
    def __init__(self) -> None:
        self._counts: dict[str, int] = {}

    def __call__(self, prefix: str) -> str:
        count = self._counts.get(prefix, 0) + 1
        self._counts[prefix] = count
        return f"{prefix}_{count:08d}"


@dataclass
class State:
    conversations: dict[ConversationId, Conversation] = field(default_factory=dict)
    messages: dict[MessageId, Message] = field(default_factory=dict)
    next_sequences: dict[ConversationId, int] = field(default_factory=dict)
    runs: dict[AgentRunId, AgentRun] = field(default_factory=dict)
    approvals: dict[ToolApprovalId, ToolApproval] = field(default_factory=dict)
    jobs: dict[JobId, Job] = field(default_factory=dict)
    outbox: list[object] = field(default_factory=list)
    audits: list[AuditEvent] = field(default_factory=list)
    permission_requests: list[AgentPermissionRequest] = field(default_factory=list)
    worker_scopes: list[tuple[WorkspaceId, UserId]] = field(default_factory=list)
    active_transactions: int = 0
    malicious_conversation: Conversation | None = None
    policy_denied: bool = False


class FakeAuthorizer:
    def __init__(self, state: State) -> None:
        self.state = state

    async def authorize(
        self,
        principal: TokenPrincipal,
        request: AgentPermissionRequest,
    ) -> AgentPermissionGrant:
        self.state.permission_requests.append(request)
        return AgentPermissionGrant(principal.user_id, request)


class FakePolicy:
    def __init__(self, state: State) -> None:
        self.state = state

    async def authorize_run(self, request: AgentRunPolicyRequest) -> AgentExecutionGrant:
        if self.state.policy_denied:
            raise AgentPolicyDenied("revoked in test")
        resolved_refs = tuple(
            ResourceRef(item.resource_type, item.id, item.revision or 1)
            for item in request.spec.context_refs
        )
        return AgentExecutionGrant(
            session_ref=ResourceRef(
                "conversation",
                request.conversation.meta.id,
                request.conversation.meta.revision,
            ),
            agent_scope=request.spec.knowledge.agent_scope,
            model_ref=ResourceRef("model", "model_policy_0001", 1),
            model_region=request.spec.inference.data_region,
            external_model_processing=False,
            context_refs=resolved_refs,
            knowledge_contexts=(),
            policy_version=1,
        )


class FakeRepository:
    def __init__(self, state: State) -> None:
        self.state = state

    async def list_conversations(
        self,
        workspace_id: WorkspaceId,
        page: AgentPageRequest,
    ) -> AgentPage[Conversation]:
        items = [
            item
            for item in self.state.conversations.values()
            if item.workspace_id == workspace_id and not item.is_deleted
        ]
        items.sort(key=lambda item: (item.meta.created_at, item.meta.id))
        if self.state.malicious_conversation is not None:
            items.append(self.state.malicious_conversation)
        return AgentPage(tuple(items[: page.limit]), None)

    async def get_conversation(
        self,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        *,
        for_update: bool = False,
        include_deleted: bool = False,
    ) -> Conversation | None:
        del for_update
        item = self.state.conversations.get(conversation_id)
        if item is None or item.workspace_id != workspace_id:
            return None
        if item.is_deleted and not include_deleted:
            return None
        return item

    async def add_conversation(self, conversation: Conversation) -> None:
        self.state.conversations[conversation.meta.id] = conversation
        self.state.next_sequences.setdefault(conversation.meta.id, 1)

    async def save_conversation(
        self,
        conversation: Conversation,
        *,
        expected_revision: int,
    ) -> None:
        current = self.state.conversations.get(conversation.meta.id)
        if current is None or current.meta.revision != expected_revision:
            raise AgentCasMismatch
        self.state.conversations[conversation.meta.id] = conversation

    async def has_nonterminal_runs(
        self,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
    ) -> bool:
        return any(
            run.workspace_id == workspace_id
            and run.view.conversation_id == conversation_id
            and not run.is_terminal
            for run in self.state.runs.values()
        )

    async def list_messages(
        self,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        page: AgentPageRequest,
    ) -> AgentPage[Message]:
        items = [
            item
            for item in self.state.messages.values()
            if item.workspace_id == workspace_id and item.conversation_id == conversation_id
        ]
        items.sort(key=lambda item: (item.sequence, item.meta.id))
        return AgentPage(tuple(items[: page.limit]), None)

    async def get_message(
        self,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        message_id: MessageId,
    ) -> Message | None:
        item = self.state.messages.get(message_id)
        if (
            item is None
            or item.workspace_id != workspace_id
            or item.conversation_id != conversation_id
        ):
            return None
        return item

    async def allocate_message_sequence(
        self,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        *,
        expected_conversation_revision: int | None,
        at: datetime,
    ) -> MessageSequenceReservation:
        conversation = self.state.conversations.get(conversation_id)
        if conversation is None or conversation.workspace_id != workspace_id:
            raise AgentCasMismatch
        if (
            expected_conversation_revision is not None
            and conversation.meta.revision != expected_conversation_revision
        ):
            raise AgentCasMismatch
        sequence = self.state.next_sequences.get(conversation_id, 1)
        self.state.next_sequences[conversation_id] = sequence + 1
        advanced = replace(conversation, meta=conversation.meta.advance(at))
        self.state.conversations[conversation_id] = advanced
        return MessageSequenceReservation(sequence, advanced.meta.revision, at)

    async def add_message(self, message: Message) -> None:
        self.state.messages[message.meta.id] = message

    async def list_runs_for_conversation(
        self,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        page: AgentPageRequest,
    ) -> AgentPage[AgentRun]:
        items = [
            item
            for item in self.state.runs.values()
            if item.workspace_id == workspace_id and item.view.conversation_id == conversation_id
        ]
        items.sort(key=lambda item: (item.meta.created_at, item.meta.id))
        return AgentPage(tuple(items[: page.limit]), None)

    async def get_run(
        self,
        workspace_id: WorkspaceId,
        run_id: AgentRunId,
        *,
        for_update: bool = False,
    ) -> AgentRun | None:
        del for_update
        item = self.state.runs.get(run_id)
        return item if item is not None and item.workspace_id == workspace_id else None

    async def add_run(self, run: AgentRun) -> None:
        self.state.runs[run.meta.id] = run

    async def save_run(self, run: AgentRun, *, expected_revision: int) -> None:
        current = self.state.runs.get(run.meta.id)
        if current is None or current.meta.revision != expected_revision:
            raise AgentCasMismatch
        self.state.runs[run.meta.id] = run

    async def get_approval(
        self,
        workspace_id: WorkspaceId,
        approval_id: ToolApprovalId,
        *,
        for_update: bool = False,
    ) -> ToolApproval | None:
        del for_update
        item = self.state.approvals.get(approval_id)
        return item if item is not None and item.workspace_id == workspace_id else None

    async def add_approval(self, approval: ToolApproval) -> None:
        self.state.approvals[approval.meta.id] = approval

    async def save_approval(
        self,
        approval: ToolApproval,
        *,
        expected_revision: int,
    ) -> None:
        current = self.state.approvals.get(approval.meta.id)
        if current is None or current.meta.revision != expected_revision:
            raise AgentCasMismatch
        self.state.approvals[approval.meta.id] = approval


class FakeJobs:
    def __init__(self, state: State) -> None:
        self.state = state

    async def add(self, job: Job) -> None:
        self.state.jobs[job.meta.id] = job

    async def get(
        self,
        workspace_id: WorkspaceId,
        job_id: JobId,
        *,
        for_update: bool = False,
    ) -> Job | None:
        del for_update
        item = self.state.jobs.get(job_id)
        return item if item is not None and item.workspace_id == workspace_id else None

    async def save(self, job: Job, *, expected_revision: int) -> None:
        current = self.state.jobs.get(job.meta.id)
        if current is None or current.meta.revision != expected_revision:
            raise AgentCasMismatch
        self.state.jobs[job.meta.id] = job


class FakeOutbox:
    def __init__(self, state: State) -> None:
        self.state = state

    async def add(self, record: object) -> None:
        self.state.outbox.append(record)


class FakeAudit:
    def __init__(self, state: State) -> None:
        self.state = state

    async def add(self, event: AuditEvent) -> None:
        self.state.audits.append(event)


class FakeResumeProposals:
    """通用 Agent 测试不启用 Resume Proposal 持久边界。"""

    async def load_base(
        self,
        workspace_id: WorkspaceId,
        resume_ref: ResourceRef,
    ) -> AgentResumeContext:
        del workspace_id, resume_ref
        raise AssertionError("unexpected Resume Proposal base load")

    async def create(self, command: AgentResumeProposalCommand) -> ResourceRef:
        del command
        raise AssertionError("unexpected Resume Proposal creation")


class FakeUow:
    def __init__(self, state: State) -> None:
        self.state = state
        self.authorizer = FakeAuthorizer(state)
        self.policy = FakePolicy(state)
        self.repository = FakeRepository(state)
        self.jobs = FakeJobs(state)
        self.outbox = FakeOutbox(state)
        self.audit = FakeAudit(state)
        self.resume_proposals = FakeResumeProposals()
        self.committed = False

    async def __aenter__(self) -> FakeUow:
        self.state.active_transactions += 1
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        del exc_type, exc, traceback
        self.state.active_transactions -= 1

    async def commit(self) -> None:
        self.committed = True

    async def rollback(self) -> None:
        self.committed = False


class FakeUowFactory:
    def __init__(self, state: State) -> None:
        self.state = state

    def __call__(self) -> FakeUow:
        return FakeUow(self.state)


class FakeWorkerUowFactory:
    """记录 worker 从已提交 dispatch 安装的最小 scope。"""

    def __init__(self, state: State) -> None:
        self.state = state

    def __call__(self, workspace_id: WorkspaceId, actor_id: UserId) -> FakeUow:
        self.state.worker_scopes.append((workspace_id, actor_id))
        return FakeUow(self.state)


class FakeProvider:
    def __init__(self, state: State, outcome: object) -> None:
        self.state = state
        self.outcome = outcome
        self.calls = 0

    async def execute(self, request: object) -> object:
        del request
        assert self.state.active_transactions == 0
        self.calls += 1
        return self.outcome


class FakeToolExecutor:
    def __init__(self, state: State) -> None:
        self.state = state
        self.calls = 0

    async def execute(
        self,
        dispatch: AgentToolDecisionClaim,
        invocation_ref: ResourceRef,
    ) -> ToolExecutionReceipt:
        assert self.state.active_transactions == 0
        assert dispatch.tool_call_id == ToolCallId("tool_call_0001")
        assert invocation_ref.resource_type == "tool_invocation"
        self.calls += 1
        return ToolExecutionReceipt(dispatch.tool_call_id, "工具执行完成", ())


class FailingToolExecutor:
    """模拟生产 composition 尚未配置可信工具执行器。"""

    def __init__(self, state: State) -> None:
        """绑定事务探针。"""
        self.state = state
        self.calls = 0

    async def execute(
        self,
        dispatch: AgentToolDecisionClaim,
        invocation_ref: ResourceRef,
    ) -> ToolExecutionReceipt:
        """确认事务外调用后返回脱敏边界前的异常。"""
        del dispatch, invocation_ref
        assert self.state.active_transactions == 0
        self.calls += 1
        raise RuntimeError("secret tool adapter detail")


class AllowAllToolRegistry:
    """仅供审批流程测试的显式工具注册表。"""

    def allows(
        self,
        request: AgentProviderRequest,
        binding: ToolCallBinding,
    ) -> bool:
        """允许测试显式构造的调用。"""
        del request, binding
        return True


def _service(state: State, ids: DeterministicIds) -> AgentApplicationService:
    return AgentApplicationService(
        FakeUowFactory(state),
        clock=FixedClock(),
        id_factory=ids,
    )


def _queued_dispatch(state: State, run_id: AgentRunId) -> AgentRunQueuedDispatch:
    """返回创建 Run 时原子提交的 worker dispatch。"""
    return next(
        record
        for record in state.outbox
        if isinstance(record, AgentRunQueuedDispatch) and record.run_ref.id == run_id
    )


def _spec(conversation_id: ConversationId, message_id: MessageId) -> AgentRunSpec:
    return AgentRunSpec(
        conversation_id=conversation_id,
        input_message_id=message_id,
        capability=ConversationCapability.GENERAL,
        context_refs=(),
        knowledge=KnowledgeSelection(
            KnowledgeSelectionMode.NONE,
            (),
            (),
            (),
            "general_agent",
        ),
        inference=InferenceIntent(
            InferenceQualityTier.BALANCED,
            10_000,
            InferenceCostTier.STANDARD,
            ModelRegion.CN,
            False,
            False,
        ),
        output_modes=(AgentOutputMode.TEXT,),
        response_locale="zh-CN",
    )


async def _prepare_decided_tool_run(
    state: State,
    ids: DeterministicIds,
    tool: AgentToolExecutor,
    decision: ToolDecision,
) -> tuple[AgentWorkerService, ToolDecisionDispatch, AgentRunId]:
    """创建并推进到已提交工具决定的 running 状态。"""

    service = _service(state, ids)
    conversation = await service.create_conversation(
        PRINCIPAL,
        WORKSPACE,
        CreateConversationCommand(ConversationCapability.GENERAL, "tool decision"),
        CONTEXT,
    )
    message = await service.create_message(
        PRINCIPAL,
        WORKSPACE,
        conversation.meta.id,
        CreateMessageCommand(None, (TextContentPart("需要调用工具"),)),
        expected_conversation_revision=conversation.meta.revision,
        context=CONTEXT,
    )
    created = await service.create_agent_run(
        PRINCIPAL,
        WORKSPACE,
        _spec(conversation.meta.id, message.meta.id),
        CONTEXT,
    )
    binding = ToolCallBinding(
        ToolCallId("tool_call_0001"),
        "calendar.create_event",
        "创建测试会议",
        ToolRisk.HIGH,
        NOW + timedelta(minutes=10),
        ResourceRef("tool_invocation", "invocation_0001", 1),
    )
    worker = AgentWorkerService(
        FakeWorkerUowFactory(state),
        FakeProvider(state, AgentProviderApprovalRequired(binding)),  # type: ignore[arg-type]
        tool,
        tool_registry=AllowAllToolRegistry(),
        clock=FixedClock(),
        id_factory=ids,
    )
    waiting = await worker.execute_run(_queued_dispatch(state, created.meta.id))
    assert waiting.pending_approval_id is not None
    await service.decide_tool_approval(
        PRINCIPAL,
        WORKSPACE,
        waiting.pending_approval_id,
        ToolApprovalDecisionCommand(decision),
        expected_revision=1,
        context=CONTEXT,
    )
    dispatch = next(
        item
        for item in state.outbox
        if isinstance(item, ToolDecisionDispatch) and item.run_ref.id == created.meta.id
    )
    return worker, dispatch, created.meta.id


@pytest.mark.asyncio
async def test_all_twelve_routes_and_tool_decision_are_atomic() -> None:
    state = State()
    ids = DeterministicIds()
    service = _service(state, ids)

    assert len(V2_AGENT_ENDPOINT_METHODS) == 12
    assert (await service.list_conversations(PRINCIPAL, WORKSPACE, AgentPageRequest())).items == ()

    conversation = await service.create_conversation(
        PRINCIPAL,
        WORKSPACE,
        CreateConversationCommand(ConversationCapability.GENERAL, "初始标题"),
        CONTEXT,
    )
    assert await service.get_conversation(PRINCIPAL, WORKSPACE, conversation.meta.id) == conversation
    assert (
        await service.get_conversation_for_update(
            PRINCIPAL,
            WORKSPACE,
            conversation.meta.id,
        )
        == conversation
    )
    assert str(state.permission_requests[-1].permission) == "conversation.update"
    assert (
        await service.get_conversation_for_deletion(
            PRINCIPAL,
            WORKSPACE,
            conversation.meta.id,
        )
        == conversation
    )
    assert str(state.permission_requests[-1].permission) == "conversation.delete"
    assert (
        await service.get_conversation_for_message_creation(
            PRINCIPAL,
            WORKSPACE,
            conversation.meta.id,
        )
        == conversation
    )
    assert str(state.permission_requests[-1].permission) == "conversation.messages.create"

    conversation = await service.update_conversation(
        PRINCIPAL,
        WORKSPACE,
        conversation.meta.id,
        ConversationPatch(title_supplied=True, title="更新标题"),
        expected_revision=1,
        context=CONTEXT,
    )
    assert conversation.meta.revision == 2

    assert (
        await service.list_messages(
            PRINCIPAL,
            WORKSPACE,
            conversation.meta.id,
            AgentPageRequest(),
        )
    ).items == ()
    message = await service.create_message(
        PRINCIPAL,
        WORKSPACE,
        conversation.meta.id,
        CreateMessageCommand(None, (TextContentPart("请准备一个行动。"),)),
        expected_conversation_revision=2,
        context=CONTEXT,
    )
    assert message.sequence == 1

    run_view = await service.create_agent_run(
        PRINCIPAL,
        WORKSPACE,
        _spec(conversation.meta.id, message.meta.id),
        CONTEXT,
    )
    assert run_view.status is AgentRunStatus.QUEUED
    assert await service.get_agent_run(PRINCIPAL, WORKSPACE, run_view.meta.id) == run_view
    assert (
        await service.get_agent_run_for_cancellation(
            PRINCIPAL,
            WORKSPACE,
            run_view.meta.id,
        )
        == run_view
    )
    assert str(state.permission_requests[-1].permission) == "agent_run.cancel"

    binding = ToolCallBinding(
        ToolCallId("tool_call_0001"),
        "calendar.create_event",
        "创建面试准备会议",
        ToolRisk.HIGH,
        NOW + timedelta(minutes=10),
        ResourceRef("tool_invocation", "invocation_0001", 1),
    )
    provider = FakeProvider(state, AgentProviderApprovalRequired(binding))
    tool = FakeToolExecutor(state)
    worker = AgentWorkerService(
        FakeWorkerUowFactory(state),
        provider,  # type: ignore[arg-type]
        tool,
        tool_registry=AllowAllToolRegistry(),
        clock=FixedClock(),
        id_factory=ids,
    )
    waiting = await worker.execute_run(_queued_dispatch(state, run_view.meta.id))
    assert waiting.status is AgentRunStatus.WAITING_FOR_APPROVAL
    assert provider.calls == 1
    assert waiting.pending_approval_id is not None
    assert await worker.execute_run(_queued_dispatch(state, run_view.meta.id)) == waiting
    assert provider.calls == 1

    approval = await service.get_tool_approval(
        PRINCIPAL,
        WORKSPACE,
        waiting.pending_approval_id,
    )
    assert (
        await service.get_tool_approval_for_decision(
            PRINCIPAL,
            WORKSPACE,
            approval.meta.id,
        )
        == approval
    )
    assert str(state.permission_requests[-1].permission) == "tool_approval.decide"
    decided = await service.decide_tool_approval(
        PRINCIPAL,
        WORKSPACE,
        approval.meta.id,
        ToolApprovalDecisionCommand(ToolDecision.APPROVE),
        expected_revision=1,
        context=CONTEXT,
    )
    assert decided.status.value == "approved"
    resumed = state.runs[run_view.meta.id]
    resumed_job = state.jobs[resumed.job_id]
    assert resumed.view.status is AgentRunStatus.RUNNING
    assert resumed_job.status.value == "running"
    assert await worker.execute_run(_queued_dispatch(state, run_view.meta.id)) == resumed.view
    assert provider.calls == 1
    decision_dispatch = next(
        item for item in state.outbox if isinstance(item, ToolDecisionDispatch)
    )
    completed = await worker.execute_approved_tool(decision_dispatch)
    assert completed.status is AgentRunStatus.SUCCEEDED
    assert completed.output_message_id is not None
    assert state.messages[completed.output_message_id].content == (
        TextContentPart("工具执行完成"),
    )
    assert state.jobs[resumed.job_id].status.value == "succeeded"
    assert tool.calls == 1
    assert await worker.execute_approved_tool(decision_dispatch) == completed
    assert tool.calls == 1

    cancellable = await service.create_agent_run(
        PRINCIPAL,
        WORKSPACE,
        _spec(conversation.meta.id, message.meta.id),
        CONTEXT,
    )

    cancelled = await service.cancel_agent_run(
        PRINCIPAL,
        WORKSPACE,
        cancellable.meta.id,
        expected_revision=cancellable.meta.revision,
        context=CONTEXT,
    )
    assert cancelled.status is AgentRunStatus.CANCELLED
    assert state.jobs[state.runs[cancellable.meta.id].job_id].status.value == "cancelled"

    current_conversation = state.conversations[conversation.meta.id]
    await service.delete_conversation(
        PRINCIPAL,
        WORKSPACE,
        conversation.meta.id,
        expected_revision=current_conversation.meta.revision,
        context=CONTEXT,
    )
    assert state.conversations[conversation.meta.id].is_deleted
    assert (
        await service.list_conversations(PRINCIPAL, WORKSPACE, AgentPageRequest())
    ).items == ()

    assert {request.permission.value for request in state.permission_requests} >= {
        "conversation.list",
        "conversation.create",
        "conversation.read",
        "conversation.update",
        "conversation.delete",
        "conversation.messages.list",
        "conversation.messages.create",
        "agent_run.create",
        "agent_run.read",
        "agent_run.cancel",
        "tool_approval.read",
        "tool_approval.decide",
    }
    assert state.audits


@pytest.mark.asyncio
async def test_execution_time_policy_revocation_fails_without_provider_io() -> None:
    """创建后撤销策略时，worker 在任何外部 I/O 前闭合 Run 与 Job。"""

    state = State()
    ids = DeterministicIds()
    service = _service(state, ids)
    conversation = await service.create_conversation(
        PRINCIPAL,
        WORKSPACE,
        CreateConversationCommand(ConversationCapability.GENERAL, "revocation"),
        CONTEXT,
    )
    message = await service.create_message(
        PRINCIPAL,
        WORKSPACE,
        conversation.meta.id,
        CreateMessageCommand(None, (TextContentPart("private request"),)),
        expected_conversation_revision=conversation.meta.revision,
        context=CONTEXT,
    )
    created = await service.create_agent_run(
        PRINCIPAL,
        WORKSPACE,
        _spec(conversation.meta.id, message.meta.id),
        CONTEXT,
    )
    provider = FakeProvider(
        state,
        AgentProviderCompleted(
            (TextContentPart("must be discarded"),),
            (),
            AgentUsage(1, 1, "0"),
        ),
    )
    state.policy_denied = True
    worker = AgentWorkerService(
        FakeWorkerUowFactory(state),
        provider,  # type: ignore[arg-type]
        FakeToolExecutor(state),
        clock=FixedClock(),
        id_factory=ids,
    )

    failed = await worker.execute_run(_queued_dispatch(state, created.meta.id))

    assert failed.status is AgentRunStatus.FAILED
    assert failed.problem is not None
    assert failed.problem.code == "agent.execution_authorization_revoked"
    assert state.jobs[state.runs[created.meta.id].job_id].status.value == "failed"
    assert provider.calls == 0
    assert not state.approvals


@pytest.mark.asyncio
async def test_empty_tool_registry_fails_without_unreachable_approval() -> None:
    """未注册工具不能生成用户永远无法完成的审批。"""

    state = State()
    ids = DeterministicIds()
    service = _service(state, ids)
    conversation = await service.create_conversation(
        PRINCIPAL,
        WORKSPACE,
        CreateConversationCommand(ConversationCapability.GENERAL, "empty tools"),
        CONTEXT,
    )
    message = await service.create_message(
        PRINCIPAL,
        WORKSPACE,
        conversation.meta.id,
        CreateMessageCommand(None, (TextContentPart("call a tool"),)),
        expected_conversation_revision=conversation.meta.revision,
        context=CONTEXT,
    )
    created = await service.create_agent_run(
        PRINCIPAL,
        WORKSPACE,
        _spec(conversation.meta.id, message.meta.id),
        CONTEXT,
    )
    binding = ToolCallBinding(
        ToolCallId("tool_call_unregistered1"),
        "calendar.create_event",
        "Create an unregistered meeting",
        ToolRisk.HIGH,
        NOW + timedelta(minutes=10),
        ResourceRef("tool_invocation", "invocation_unregistered1", 1),
    )
    provider = FakeProvider(state, AgentProviderApprovalRequired(binding))
    tool = FakeToolExecutor(state)
    worker = AgentWorkerService(
        FakeWorkerUowFactory(state),
        provider,  # type: ignore[arg-type]
        tool,
        clock=FixedClock(),
        id_factory=ids,
    )

    failed = await worker.execute_run(_queued_dispatch(state, created.meta.id))

    assert failed.status is AgentRunStatus.FAILED
    assert failed.problem is not None
    assert failed.problem.code == "agent.tool_unavailable"
    assert provider.calls == 1
    assert tool.calls == 0
    assert not state.approvals


@pytest.mark.asyncio
async def test_rejected_tool_decision_fails_run_without_invocation_and_replays() -> None:
    """拒绝决定不调用工具，并以同一事件幂等返回持久失败态。"""

    state = State()
    ids = DeterministicIds()
    tool = FakeToolExecutor(state)
    worker, dispatch, run_id = await _prepare_decided_tool_run(
        state,
        ids,
        tool,
        ToolDecision.REJECT,
    )

    failed = await worker.execute_approved_tool(dispatch)
    outbox_size = len(state.outbox)
    replayed = await worker.execute_approved_tool(dispatch)

    assert failed.status is AgentRunStatus.FAILED
    assert failed.problem is not None
    assert failed.problem.code == "tool_approval.rejected"
    assert replayed == failed
    assert state.jobs[state.runs[run_id].job_id].status.value == "failed"
    assert tool.calls == 0
    assert len(state.outbox) == outbox_size


@pytest.mark.asyncio
async def test_unavailable_tool_executor_persists_redacted_terminal_failure() -> None:
    """未配置工具执行器只能调用一次，且异常不得泄漏或留下 running。"""

    state = State()
    ids = DeterministicIds()
    tool = FailingToolExecutor(state)
    worker, dispatch, run_id = await _prepare_decided_tool_run(
        state,
        ids,
        tool,
        ToolDecision.APPROVE,
    )

    failed = await worker.execute_approved_tool(dispatch)
    outbox_size = len(state.outbox)
    replayed = await worker.execute_approved_tool(dispatch)

    assert failed.status is AgentRunStatus.FAILED
    assert failed.problem is not None
    assert failed.problem.code == "agent.tool_execution_failed"
    assert "secret" not in str(failed.problem)
    assert replayed == failed
    assert state.jobs[state.runs[run_id].job_id].status.value == "failed"
    assert tool.calls == 1
    assert len(state.outbox) == outbox_size


@pytest.mark.asyncio
async def test_outbox_exhaustion_cancels_never_started_run_and_job_idempotently() -> None:
    """从未开始的 queued 工作在耗尽时取消，且重放不追加第二个终态事件。"""
    state = State()
    ids = DeterministicIds()
    service = _service(state, ids)
    conversation = await service.create_conversation(
        PRINCIPAL,
        WORKSPACE,
        CreateConversationCommand(ConversationCapability.GENERAL, "exhaust queued"),
        CONTEXT,
    )
    message = await service.create_message(
        PRINCIPAL,
        WORKSPACE,
        conversation.meta.id,
        CreateMessageCommand(None, (TextContentPart("never starts"),)),
        expected_conversation_revision=conversation.meta.revision,
        context=CONTEXT,
    )
    created = await service.create_agent_run(
        PRINCIPAL,
        WORKSPACE,
        _spec(conversation.meta.id, message.meta.id),
        CONTEXT,
    )
    dispatch = _queued_dispatch(state, created.meta.id)
    worker = AgentWorkerService(
        FakeWorkerUowFactory(state),
        FakeProvider(
            state,
            AgentProviderCompleted(
                (TextContentPart("unused"),),
                (),
                AgentUsage(1, 1, "0"),
            ),
        ),  # type: ignore[arg-type]
        FakeToolExecutor(state),
        clock=FixedClock(),
        id_factory=ids,
    )

    cancelled = await worker.fail_exhausted(dispatch)
    outbox_size = len(state.outbox)
    replayed = await worker.fail_exhausted(dispatch)

    assert cancelled.status is AgentRunStatus.CANCELLED
    assert cancelled.problem is None
    assert replayed == cancelled
    assert state.jobs[state.runs[created.meta.id].job_id].status.value == "cancelled"
    assert len(state.outbox) == outbox_size


@pytest.mark.asyncio
async def test_outbox_exhaustion_fails_waiting_run_job_and_closes_pending_approval() -> None:
    """等待审批的工作耗尽时，同事务拒绝 approval 并失败 Run/Job。"""
    state = State()
    ids = DeterministicIds()
    service = _service(state, ids)
    conversation = await service.create_conversation(
        PRINCIPAL,
        WORKSPACE,
        CreateConversationCommand(ConversationCapability.GENERAL, "exhaust waiting"),
        CONTEXT,
    )
    message = await service.create_message(
        PRINCIPAL,
        WORKSPACE,
        conversation.meta.id,
        CreateMessageCommand(None, (TextContentPart("needs approval"),)),
        expected_conversation_revision=conversation.meta.revision,
        context=CONTEXT,
    )
    created = await service.create_agent_run(
        PRINCIPAL,
        WORKSPACE,
        _spec(conversation.meta.id, message.meta.id),
        CONTEXT,
    )
    dispatch = _queued_dispatch(state, created.meta.id)
    binding = ToolCallBinding(
        ToolCallId("tool_call_exhaust1"),
        "calendar.create_event",
        "Create an exhaustion test meeting",
        ToolRisk.HIGH,
        NOW + timedelta(minutes=10),
        ResourceRef("tool_invocation", "invocation_exhaust1", 1),
    )
    worker = AgentWorkerService(
        FakeWorkerUowFactory(state),
        FakeProvider(state, AgentProviderApprovalRequired(binding)),  # type: ignore[arg-type]
        FakeToolExecutor(state),
        tool_registry=AllowAllToolRegistry(),
        clock=FixedClock(),
        id_factory=ids,
    )
    waiting = await worker.execute_run(dispatch)
    assert waiting.pending_approval_id is not None

    failed = await worker.fail_exhausted(dispatch)
    outbox_size = len(state.outbox)
    replayed = await worker.fail_exhausted(dispatch)
    approval = state.approvals[waiting.pending_approval_id]

    assert failed.status is AgentRunStatus.FAILED
    assert failed.problem is not None
    assert failed.problem.code == "agent.dispatch_exhausted"
    assert failed.problem.request_id == str(dispatch.id)
    assert replayed == failed
    assert state.jobs[state.runs[created.meta.id].job_id].status.value == "failed"
    assert approval.view.status.value == "rejected"
    assert approval.view.decision_by == ResourceRef("service", "agent_service")
    assert len(state.outbox) == outbox_size


@pytest.mark.asyncio
async def test_strong_revision_rejects_stale_mutation_without_state_change() -> None:
    state = State()
    ids = DeterministicIds()
    service = _service(state, ids)
    conversation = await service.create_conversation(
        PRINCIPAL,
        WORKSPACE,
        CreateConversationCommand(ConversationCapability.GENERAL, None),
        CONTEXT,
    )

    with pytest.raises(AgentPreconditionFailed):
        await service.update_conversation(
            PRINCIPAL,
            WORKSPACE,
            conversation.meta.id,
            ConversationPatch(title_supplied=True, title="stale"),
            expected_revision=9,
            context=CONTEXT,
        )
    assert state.conversations[conversation.meta.id].meta.revision == 1
    assert state.conversations[conversation.meta.id].title is None


@pytest.mark.asyncio
async def test_workspace_boundary_fails_closed_for_reads_and_malicious_pages() -> None:
    state = State()
    ids = DeterministicIds()
    service = _service(state, ids)
    conversation = await service.create_conversation(
        PRINCIPAL,
        WORKSPACE,
        CreateConversationCommand(ConversationCapability.GENERAL, "private"),
        CONTEXT,
    )

    with pytest.raises(AgentResourceNotFound):
        await service.get_conversation(PRINCIPAL, OTHER_WORKSPACE, conversation.meta.id)

    state.malicious_conversation = Conversation(
        ResourceMeta(ConversationId("foreign_conversation_0001"), 1, NOW, NOW),
        OTHER_WORKSPACE,
        "foreign",
        ConversationCapability.GENERAL,
    )
    with pytest.raises(AgentPortProtocolError, match="invalid item"):
        await service.list_conversations(PRINCIPAL, WORKSPACE, AgentPageRequest())


@pytest.mark.asyncio
async def test_provider_network_io_occurs_outside_transactions_and_completion_appends() -> None:
    state = State()
    ids = DeterministicIds()
    service = _service(state, ids)
    conversation = await service.create_conversation(
        PRINCIPAL,
        WORKSPACE,
        CreateConversationCommand(ConversationCapability.GENERAL, "answer"),
        CONTEXT,
    )
    message = await service.create_message(
        PRINCIPAL,
        WORKSPACE,
        conversation.meta.id,
        CreateMessageCommand(None, (TextContentPart("hello"),)),
        expected_conversation_revision=1,
        context=CONTEXT,
    )
    run = await service.create_agent_run(
        PRINCIPAL,
        WORKSPACE,
        _spec(conversation.meta.id, message.meta.id),
        CONTEXT,
    )
    provider = FakeProvider(
        state,
        AgentProviderCompleted(
            (TextContentPart("world"),),
            (),
            AgentUsage(3, 2, "5"),
        ),
    )
    worker = AgentWorkerService(
        FakeWorkerUowFactory(state),
        provider,  # type: ignore[arg-type]
        FakeToolExecutor(state),
        clock=FixedClock(),
        id_factory=ids,
    )

    completed = await worker.execute_run(_queued_dispatch(state, run.meta.id))
    assert completed.status is AgentRunStatus.SUCCEEDED
    assert completed.output_message_id is not None
    output = state.messages[completed.output_message_id]
    assert output.sequence == 2
    assert output.source_run_id == run.meta.id
    assert provider.calls == 1
    assert state.active_transactions == 0


@pytest.mark.asyncio
async def test_provider_failure_is_persisted_and_never_leaks_exception_text() -> None:
    """Provider 未知异常应原子终结 Run/Job，且公开问题不含异常文本。"""
    state = State()
    ids = DeterministicIds()
    service = _service(state, ids)
    conversation = await service.create_conversation(
        PRINCIPAL,
        WORKSPACE,
        CreateConversationCommand(ConversationCapability.GENERAL, "failure"),
        CONTEXT,
    )
    message = await service.create_message(
        PRINCIPAL,
        WORKSPACE,
        conversation.meta.id,
        CreateMessageCommand(None, (TextContentPart("hello"),)),
        expected_conversation_revision=1,
        context=CONTEXT,
    )
    run = await service.create_agent_run(
        PRINCIPAL,
        WORKSPACE,
        _spec(conversation.meta.id, message.meta.id),
        CONTEXT,
    )

    class FailingProvider:
        """模拟含敏感异常文本的 provider。"""

        async def execute(self, request: object) -> object:
            """在事务外失败。"""
            del request
            assert state.active_transactions == 0
            raise RuntimeError("secret upstream body")

    worker = AgentWorkerService(
        FakeWorkerUowFactory(state),
        FailingProvider(),  # type: ignore[arg-type]
        FakeToolExecutor(state),
        clock=FixedClock(),
        id_factory=ids,
    )
    failed = await worker.execute_run(_queued_dispatch(state, run.meta.id))

    assert failed.status is AgentRunStatus.FAILED
    assert failed.problem is not None
    assert failed.problem.code == "agent.provider_failed"
    assert "secret" not in str(failed.problem)
    assert state.jobs[state.runs[run.meta.id].job_id].status.value == "failed"


@pytest.mark.asyncio
async def test_running_provider_claim_is_resumable_after_worker_cancellation() -> None:
    """进程在外部 I/O 中止后，RUNNING claim 可由 at-least-once worker 重试。"""
    state = State()
    ids = DeterministicIds()
    service = _service(state, ids)
    conversation = await service.create_conversation(
        PRINCIPAL,
        WORKSPACE,
        CreateConversationCommand(ConversationCapability.GENERAL, "resume"),
        CONTEXT,
    )
    message = await service.create_message(
        PRINCIPAL,
        WORKSPACE,
        conversation.meta.id,
        CreateMessageCommand(None, (TextContentPart("hello"),)),
        expected_conversation_revision=1,
        context=CONTEXT,
    )
    run = await service.create_agent_run(
        PRINCIPAL,
        WORKSPACE,
        _spec(conversation.meta.id, message.meta.id),
        CONTEXT,
    )

    class CancelledProvider:
        """模拟 worker shutdown 取消 provider I/O。"""

        async def execute(self, request: object) -> object:
            """传播 cancellation。"""
            del request
            assert state.active_transactions == 0
            raise asyncio.CancelledError

    cancelled_worker = AgentWorkerService(
        FakeWorkerUowFactory(state),
        CancelledProvider(),  # type: ignore[arg-type]
        FakeToolExecutor(state),
        clock=FixedClock(),
        id_factory=ids,
    )
    with pytest.raises(asyncio.CancelledError):
        await cancelled_worker.execute_run(_queued_dispatch(state, run.meta.id))

    running = state.runs[run.meta.id]
    assert running.view.status is AgentRunStatus.RUNNING
    running_revision = running.meta.revision
    retry = AgentWorkerService(
        FakeWorkerUowFactory(state),
        FakeProvider(
            state,
            AgentProviderCompleted(
                (TextContentPart("world"),),
                (),
                AgentUsage(3, 2, "5"),
            ),
        ),  # type: ignore[arg-type]
        FakeToolExecutor(state),
        clock=FixedClock(),
        id_factory=ids,
    )
    completed = await retry.execute_run(_queued_dispatch(state, run.meta.id))

    assert completed.status is AgentRunStatus.SUCCEEDED
    assert completed.meta.revision == running_revision + 1


@pytest.mark.asyncio
async def test_late_tool_decision_atomically_expires_approval_run_job_and_audits() -> None:
    state = State()
    ids = DeterministicIds()
    service = _service(state, ids)
    conversation = await service.create_conversation(
        PRINCIPAL,
        WORKSPACE,
        CreateConversationCommand(ConversationCapability.GENERAL, "expiry"),
        CONTEXT,
    )
    message = await service.create_message(
        PRINCIPAL,
        WORKSPACE,
        conversation.meta.id,
        CreateMessageCommand(None, (TextContentPart("需要审批"),)),
        expected_conversation_revision=1,
        context=CONTEXT,
    )
    run = await service.create_agent_run(
        PRINCIPAL,
        WORKSPACE,
        _spec(conversation.meta.id, message.meta.id),
        CONTEXT,
    )
    binding = ToolCallBinding(
        ToolCallId("tool_call_0001"),
        "calendar.create_event",
        "创建到期测试会议",
        ToolRisk.HIGH,
        NOW + timedelta(seconds=1),
        ResourceRef("tool_invocation", "invocation_0001", 1),
    )
    worker = AgentWorkerService(
        FakeWorkerUowFactory(state),
        FakeProvider(state, AgentProviderApprovalRequired(binding)),  # type: ignore[arg-type]
        FakeToolExecutor(state),
        tool_registry=AllowAllToolRegistry(),
        clock=FixedClock(),
        id_factory=ids,
    )
    waiting = await worker.execute_run(_queued_dispatch(state, run.meta.id))
    assert waiting.pending_approval_id is not None

    late_service = AgentApplicationService(
        FakeUowFactory(state),
        clock=OffsetClock(timedelta(seconds=2)),
        id_factory=ids,
    )
    with pytest.raises(AgentConflict, match="expired"):
        await late_service.decide_tool_approval(
            PRINCIPAL,
            WORKSPACE,
            waiting.pending_approval_id,
            ToolApprovalDecisionCommand(ToolDecision.APPROVE),
            expected_revision=1,
            context=CONTEXT,
        )

    approval = state.approvals[waiting.pending_approval_id]
    failed_run = state.runs[run.meta.id]
    failed_job = state.jobs[failed_run.job_id]
    assert approval.view.status.value == "expired"
    assert failed_run.view.status is AgentRunStatus.FAILED
    assert failed_job.status.value == "failed"
    assert any(isinstance(item, ToolApprovalExpiredDispatch) for item in state.outbox)
    assert state.audits[-1].outcome.value == "denied"
