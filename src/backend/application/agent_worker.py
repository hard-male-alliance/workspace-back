"""@brief Agent 工作事件到两段短事务 worker 的适配 / Adapt Agent work events to the two-short-transaction worker.

统一 outbox claim 只携带执行授权真正需要的 creator、Workspace 与精确资源绑定；本
adapter 分别对白名单化的 Run 排队和工具决定 payload 做穷尽验证。租约、重试和跨
Workspace 扫描仍由通用 outbox dispatcher 负责。/ A unified-outbox claim carries only the
creator, Workspace, and exact resource bindings needed for execution authorization. This adapter
exhaustively validates allowlisted Run-queued and tool-decision payloads, while the generic
dispatcher retains lease, retry, and cross-Workspace scanning responsibilities.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, TypeIs

from backend.application.ports.agent_v2 import (
    AgentRunExecutionClaim,
    AgentRunExhaustionClaim,
    AgentToolDecisionClaim,
)
from backend.application.ports.outbox_dispatch import (
    OutboxDispatchClaim,
    OutboxHandlerFailure,
)
from backend.domain.agent_v2 import (
    AgentOutboxId,
    AgentRunView,
    ToolCallId,
    ToolDecision,
)
from backend.domain.outbox import AGENT_WORK_EVENT_TYPES
from backend.domain.principals import UserId, WorkspaceId
from backend.domain.resources import ResourceRef

_RUN_QUEUED_EVENT_TYPE = "agent.run.queued"
"""@brief 新 Run 的唯一工作事件 / Sole work event for a new Run."""

_TOOL_DECISION_EVENT_TYPE = "agent.tool_decision.recorded"
"""@brief 已提交工具决定的唯一恢复事件 / Sole resume event for a committed tool decision."""

_QUEUED_PAYLOAD_FIELDS = frozenset({"actor_id", "run_id", "job_id"})
"""@brief queued payload 的封闭字段集 / Closed field set for a queued payload."""

_TOOL_DECISION_PAYLOAD_FIELDS = frozenset(
    {
        "actor_id",
        "run_id",
        "run_revision",
        "job_id",
        "job_revision",
        "approval_id",
        "approval_revision",
        "tool_call_id",
        "decision",
    }
)
"""@brief 工具决定 payload 的封闭字段集 / Closed field set for a tool-decision payload."""


@dataclass(frozen=True, slots=True)
class _AgentRunExecutionClaim:
    """@brief worker 所需的最小已验证 claim / Minimal validated claim required by the worker."""

    workspace_id: WorkspaceId
    """@brief Run Workspace / Run Workspace."""

    actor_id: UserId
    """@brief Run creator 快照 / Run-creator snapshot."""

    run_ref: ResourceRef
    """@brief 精确初始 Run 引用 / Exact initial Run reference."""

    job_ref: ResourceRef
    """@brief 精确初始 Job 引用 / Exact initial Job reference."""


@dataclass(frozen=True, slots=True)
class _AgentToolDecisionClaim:
    """@brief worker 所需的最小工具决定 claim / Minimal tool-decision claim required by the worker."""

    id: AgentOutboxId
    """@brief 稳定外部 operation ID / Stable external-operation ID."""

    workspace_id: WorkspaceId
    """@brief Run Workspace / Run Workspace."""

    actor_id: UserId
    """@brief Run creator 快照 / Run-creator snapshot."""

    run_ref: ResourceRef
    """@brief 决定后的精确 Run revision / Exact post-decision Run revision."""

    job_ref: ResourceRef
    """@brief 决定后的精确 Job revision / Exact post-decision Job revision."""

    approval_ref: ResourceRef
    """@brief 已决定 Approval revision / Decided Approval revision."""

    tool_call_id: ToolCallId
    """@brief Approval 绑定的调用 ID / Call ID bound by the Approval."""

    decision: ToolDecision
    """@brief approve 或 reject / Approve or reject."""


@dataclass(frozen=True, slots=True)
class _AgentRunExhaustionClaim:
    """@brief payload 独立的 Agent 耗尽补偿 claim / Payload-independent Agent exhaustion claim."""

    id: AgentOutboxId
    """@brief 失败事件关联 ID / Failed-event correlation ID."""

    workspace_id: WorkspaceId
    """@brief Run Workspace / Run Workspace."""

    actor_id: UserId
    """@brief Run creator 快照 / Run-creator snapshot."""

    run_ref: ResourceRef
    """@brief outbox header 中的 Run subject / Run subject from the outbox header."""


class _AgentWorker(Protocol):
    """@brief handler 所需的窄 worker 形状 / Narrow worker shape required by the handler."""

    async def execute_run(self, dispatch: AgentRunExecutionClaim) -> AgentRunView:
        """@brief 幂等执行 queued Run / Idempotently execute a queued Run."""

    async def execute_approved_tool(self, dispatch: AgentToolDecisionClaim) -> AgentRunView:
        """@brief 幂等执行或终结工具决定 / Idempotently execute or terminate a tool decision."""

    async def fail_exhausted(self, dispatch: AgentRunExhaustionClaim) -> AgentRunView:
        """@brief 幂等闭合耗尽事件拥有的 Run/Job / Idempotently close the Run/Job owned by an exhausted event."""


class AgentRunOutboxHandler:
    """@brief 校验统一 claim 并调用幂等 Agent worker / Validate a unified claim and invoke the idempotent Agent worker."""

    def __init__(self, worker: _AgentWorker) -> None:
        """@brief 绑定 lifespan-owned worker / Bind the lifespan-owned worker.

        @param worker 两段短事务 Agent worker / Two-short-transaction Agent worker.
        """
        self._worker = worker

    async def handle(self, claim: OutboxDispatchClaim) -> None:
        """@brief 执行一条严格绑定的 Agent 工作事件 / Execute one strictly bound Agent work event.

        @param claim 已由租约 dispatcher 独占的 durable claim / Durable claim exclusively leased
            by the dispatcher.
        @raise OutboxHandlerFailure event type、subject 或 payload 绑定非法时抛出 / Raised
            when the event type, subject, or payload binding is invalid.
        """
        if claim.event_type == _RUN_QUEUED_EVENT_TYPE:
            await self._worker.execute_run(_execution_claim(claim))
            return
        if claim.event_type == _TOOL_DECISION_EVENT_TYPE:
            await self._worker.execute_approved_tool(_tool_decision_claim(claim))
            return
        raise OutboxHandlerFailure("agent.event_type_unsupported")

    async def on_exhausted(
        self,
        claim: OutboxDispatchClaim,
        *,
        error_code: str,
    ) -> None:
        """@brief 在 outbox failed 前闭合 Agent 领域状态 / Close Agent domain state before outbox failure.

        @param claim 已到最后一次尝试且仍持有租约的 claim / Final-attempt claim whose lease
            is still owned.
        @param error_code dispatcher 已脱敏的失败码 / Dispatcher-redacted failure code.
        @raise OutboxHandlerFailure event type 或 header subject 非法时抛出 / Raised when the
            event type or header subject is invalid.
        @note payload 可能正是失败原因，因此补偿不读取 payload；``error_code`` 仅由通用
            dispatcher 持久化，领域 Problem 使用稳定的 Agent 专用 code。/ The payload may
            itself be the failure source, so compensation never reads it; the generic dispatcher
            persists ``error_code`` while the domain Problem uses a stable Agent-specific code.
        """
        del error_code
        if (
            claim.event_type not in AGENT_WORK_EVENT_TYPES
            or claim.subject.resource_type != "agent_run"
            or claim.subject.revision is None
        ):
            raise OutboxHandlerFailure("agent.exhaustion_event_invalid")
        await self._worker.fail_exhausted(
            _AgentRunExhaustionClaim(
                id=AgentOutboxId(str(claim.event_id)),
                workspace_id=claim.workspace_id,
                actor_id=claim.actor_id,
                run_ref=claim.subject,
            )
        )


def _execution_claim(claim: OutboxDispatchClaim) -> _AgentRunExecutionClaim:
    """@brief 防御性解析 queued payload / Defensively parse a queued payload.

    @param claim 通用 outbox claim / Generic outbox claim.
    @return 仅含已交叉验证字段的 worker claim / Worker claim containing only cross-validated fields.
    @raise OutboxHandlerFailure 任一绑定不一致时抛出 / Raised for any binding mismatch.
    """
    payload = claim.payload
    if claim.event_type != _RUN_QUEUED_EVENT_TYPE:
        raise OutboxHandlerFailure("agent.event_type_unsupported")
    if (
        claim.subject.resource_type != "agent_run"
        or claim.subject.revision != 1
        or frozenset(payload) != _QUEUED_PAYLOAD_FIELDS
    ):
        raise OutboxHandlerFailure("agent.queued_event_invalid")
    actor_id = payload.get("actor_id")
    run_id = payload.get("run_id")
    job_id = payload.get("job_id")
    if (
        not isinstance(actor_id, str)
        or actor_id != claim.actor_id
        or not isinstance(run_id, str)
        or run_id != claim.subject.id
        or not isinstance(job_id, str)
    ):
        raise OutboxHandlerFailure("agent.queued_event_invalid")
    try:
        job_ref = ResourceRef("job", job_id, 1)
    except ValueError as error:
        raise OutboxHandlerFailure("agent.queued_event_invalid") from error
    return _AgentRunExecutionClaim(
        workspace_id=claim.workspace_id,
        actor_id=claim.actor_id,
        run_ref=claim.subject,
        job_ref=job_ref,
    )


def _tool_decision_claim(claim: OutboxDispatchClaim) -> _AgentToolDecisionClaim:
    """@brief 严格解析工具决定 payload 与三组 revision 绑定 / Strictly parse a tool decision and its three revision bindings.

    @param claim 通用 outbox claim / Generic outbox claim.
    @return 只包含执行所需字段的决定 claim / Decision claim containing only execution fields.
    @raise OutboxHandlerFailure 字段、actor、subject 或 revision 不一致时抛出 / Raised for
        inconsistent fields, actor, subject, or revisions.
    """

    payload = claim.payload
    if claim.event_type != _TOOL_DECISION_EVENT_TYPE:
        raise OutboxHandlerFailure("agent.event_type_unsupported")
    if (
        claim.subject.resource_type != "agent_run"
        or claim.subject.revision is None
        or frozenset(payload) != _TOOL_DECISION_PAYLOAD_FIELDS
    ):
        raise OutboxHandlerFailure("agent.tool_decision_event_invalid")
    actor_id = payload.get("actor_id")
    run_id = payload.get("run_id")
    run_revision = payload.get("run_revision")
    job_id = payload.get("job_id")
    job_revision = payload.get("job_revision")
    approval_id = payload.get("approval_id")
    approval_revision = payload.get("approval_revision")
    tool_call_id = payload.get("tool_call_id")
    decision = payload.get("decision")
    if (
        not isinstance(actor_id, str)
        or actor_id != claim.actor_id
        or not isinstance(run_id, str)
        or run_id != claim.subject.id
        or not _is_positive_int(run_revision)
        or run_revision != claim.subject.revision
        or not isinstance(job_id, str)
        or not _is_positive_int(job_revision)
        or not isinstance(approval_id, str)
        or not _is_positive_int(approval_revision)
        or not isinstance(tool_call_id, str)
        or not isinstance(decision, str)
    ):
        raise OutboxHandlerFailure("agent.tool_decision_event_invalid")
    try:
        job_ref = ResourceRef("job", job_id, job_revision)
        approval_ref = ResourceRef("tool_approval", approval_id, approval_revision)
        ResourceRef("tool_call", tool_call_id, 1)
        parsed_decision = ToolDecision(decision)
        parsed_call_id = ToolCallId(tool_call_id)
    except ValueError as error:
        raise OutboxHandlerFailure("agent.tool_decision_event_invalid") from error
    return _AgentToolDecisionClaim(
        id=AgentOutboxId(str(claim.event_id)),
        workspace_id=claim.workspace_id,
        actor_id=claim.actor_id,
        run_ref=claim.subject,
        job_ref=job_ref,
        approval_ref=approval_ref,
        tool_call_id=parsed_call_id,
        decision=parsed_decision,
    )


def _is_positive_int(value: object) -> TypeIs[int]:
    """@brief 排除 bool 后判断正整数 / Test for a positive integer while excluding booleans."""

    return not isinstance(value, bool) and isinstance(value, int) and value >= 1


__all__ = ["AGENT_WORK_EVENT_TYPES", "AgentRunOutboxHandler"]
