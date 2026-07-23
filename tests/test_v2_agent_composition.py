"""@brief Agent V2 composition、claim 绑定与生命周期测试 / Agent V2 composition, claim-binding, and lifecycle tests."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from backend.api.v2_agent import router_v2_agent
from backend.application.agent_v2 import V2_AGENT_ENDPOINT_METHODS
from backend.application.agent_worker import AgentRunOutboxHandler
from backend.application.ports.agent_v2 import (
    AgentRunExecutionClaim,
    AgentRunExhaustionClaim,
    AgentToolDecisionClaim,
)
from backend.application.ports.outbox_dispatch import (
    OutboxDispatchClaim,
    OutboxHandlerFailure,
    OutboxLease,
)
from backend.composition import _agent_model_routes, build_container
from backend.config import BackendSettings
from backend.domain.agent_v2 import (
    AgentOutboxId,
    AgentRunQueuedDispatch,
    AgentRunView,
    ToolCallId,
    ToolDecision,
    ToolDecisionDispatch,
)
from backend.domain.knowledge_sources import ModelRegion
from backend.domain.platform import ApiEventId, JsonValue
from backend.domain.principals import UserId, WorkspaceId
from backend.domain.resources import ResourceRef
from backend.infrastructure.agent_v2 import (
    InMemoryAgentDispatchService,
    InMemoryAgentStore,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 项目根目录 / Project root."""

NOW = datetime(2026, 7, 23, 12, tzinfo=UTC)
"""@brief 固定 worker 测试时间 / Fixed worker-test instant."""


class _CapturingWorker:
    """@brief 捕获已验证 execution claim 的 worker / Worker capturing validated execution claims."""

    def __init__(self) -> None:
        """@brief 初始化空捕获 / Initialize an empty capture."""
        self.claims: list[AgentRunExecutionClaim] = []
        self.tool_decisions: list[AgentToolDecisionClaim] = []
        self.exhausted: list[AgentRunExhaustionClaim] = []

    async def execute_run(self, dispatch: AgentRunExecutionClaim) -> AgentRunView:
        """@brief 记录 claim 而不执行 provider / Record a claim without executing a provider."""
        self.claims.append(dispatch)
        return cast(AgentRunView, object())

    async def execute_approved_tool(
        self,
        dispatch: AgentToolDecisionClaim,
    ) -> AgentRunView:
        """@brief 记录严格工具决定 claim / Record a strict tool-decision claim."""
        self.tool_decisions.append(dispatch)
        return cast(AgentRunView, object())

    async def fail_exhausted(self, dispatch: AgentRunExhaustionClaim) -> AgentRunView:
        """@brief 捕获不依赖 payload 的耗尽 claim / Capture a payload-independent exhaustion claim."""
        self.exhausted.append(dispatch)
        return cast(AgentRunView, object())


def _outbox_claim(*, actor_id: str = "user_agentworker01") -> OutboxDispatchClaim:
    """@brief 构造严格 queued claim / Build a strictly bound queued claim."""
    return OutboxDispatchClaim(
        ApiEventId("event_agentworker01"),
        WorkspaceId("workspace_agentworker01"),
        UserId("user_agentworker01"),
        ResourceRef("agent_run", "agent_run_worker0001", 1),
        "agent.run.queued",
        {
            "actor_id": actor_id,
            "run_id": "agent_run_worker0001",
            "job_id": "agent_job_worker0001",
        },
        1,
        OutboxLease("agent-worker-lease-token-with-adequate-entropy"),
        NOW + timedelta(minutes=2),
    )


def _tool_decision_claim(
    *,
    actor_id: str = "user_agentworker01",
    run_revision: int = 4,
    extra_payload: bool = False,
) -> OutboxDispatchClaim:
    """@brief 构造严格绑定三聚合 revision 的工具决定 claim / Build a tool-decision claim bound to three aggregate revisions."""

    payload: dict[str, JsonValue] = {
        "actor_id": actor_id,
        "run_id": "agent_run_worker0001",
        "run_revision": run_revision,
        "job_id": "agent_job_worker0001",
        "job_revision": 4,
        "approval_id": "approval_worker0001",
        "approval_revision": 2,
        "tool_call_id": "tool_call_worker0001",
        "decision": "approve",
    }
    if extra_payload:
        payload["arguments"] = "must-not-pass"
    return OutboxDispatchClaim(
        ApiEventId("event_agentdecision01"),
        WorkspaceId("workspace_agentworker01"),
        UserId("user_agentworker01"),
        ResourceRef("agent_run", "agent_run_worker0001", 4),
        "agent.tool_decision.recorded",
        payload,
        1,
        OutboxLease("agent-decision-lease-token-with-adequate-entropy"),
        NOW + timedelta(minutes=2),
    )


@pytest.mark.asyncio
async def test_outbox_handler_cross_checks_creator_run_and_job_binding() -> None:
    """@brief handler 只把交叉验证后的最小 claim 交给 worker / Handler passes only a cross-validated minimal claim."""
    worker = _CapturingWorker()
    handler = AgentRunOutboxHandler(worker)

    await handler.handle(_outbox_claim())

    assert len(worker.claims) == 1
    execution = worker.claims[0]
    assert execution.workspace_id == WorkspaceId("workspace_agentworker01")
    assert execution.actor_id == UserId("user_agentworker01")
    assert execution.run_ref == ResourceRef("agent_run", "agent_run_worker0001", 1)
    assert execution.job_ref == ResourceRef("job", "agent_job_worker0001", 1)

    with pytest.raises(OutboxHandlerFailure, match=r"agent\.queued_event_invalid"):
        await handler.handle(_outbox_claim(actor_id="user_different0001"))
    assert len(worker.claims) == 1


@pytest.mark.asyncio
async def test_outbox_handler_strictly_binds_tool_decision_payload() -> None:
    """@brief 工具恢复事件只接受封闭字段与一致 actor/subject/revision / Tool resume events accept only closed fields and aligned bindings."""

    worker = _CapturingWorker()
    handler = AgentRunOutboxHandler(worker)

    await handler.handle(_tool_decision_claim())

    assert len(worker.tool_decisions) == 1
    decision = worker.tool_decisions[0]
    assert decision.id == AgentOutboxId("event_agentdecision01")
    assert decision.run_ref == ResourceRef("agent_run", "agent_run_worker0001", 4)
    assert decision.job_ref == ResourceRef("job", "agent_job_worker0001", 4)
    assert decision.approval_ref == ResourceRef(
        "tool_approval",
        "approval_worker0001",
        2,
    )
    assert decision.tool_call_id == ToolCallId("tool_call_worker0001")
    assert decision.decision is ToolDecision.APPROVE

    for invalid in (
        _tool_decision_claim(actor_id="user_different0001"),
        _tool_decision_claim(run_revision=3),
        _tool_decision_claim(extra_payload=True),
    ):
        with pytest.raises(
            OutboxHandlerFailure,
            match=r"agent\.tool_decision_event_invalid",
        ):
            await handler.handle(invalid)
    assert len(worker.tool_decisions) == 1


@pytest.mark.asyncio
async def test_outbox_exhaustion_uses_only_validated_header_not_broken_payload() -> None:
    """@brief 即使 payload 非法，耗尽补偿仍只按可信 header 闭合 Run / Exhaustion compensates from trusted headers even when payload is invalid."""
    worker = _CapturingWorker()
    handler = AgentRunOutboxHandler(worker)
    invalid_payload = _tool_decision_claim(extra_payload=True)

    with pytest.raises(OutboxHandlerFailure, match=r"agent\.tool_decision_event_invalid"):
        await handler.handle(invalid_payload)
    await handler.on_exhausted(invalid_payload, error_code="outbox.handler_failed")

    assert len(worker.exhausted) == 1
    exhaustion = worker.exhausted[0]
    assert exhaustion.id == AgentOutboxId("event_agentdecision01")
    assert exhaustion.workspace_id == WorkspaceId("workspace_agentworker01")
    assert exhaustion.actor_id == UserId("user_agentworker01")
    assert exhaustion.run_ref == ResourceRef("agent_run", "agent_run_worker0001", 4)


@pytest.mark.asyncio
async def test_memory_dispatch_separates_and_publishes_both_agent_work_events() -> None:
    """@brief memory loop 对两类工作各执行一次且不重复 / Memory dispatch executes each work type once without repeats."""
    store = InMemoryAgentStore()
    dispatch = AgentRunQueuedDispatch(
        AgentOutboxId("outbox_memoryworker1"),
        WorkspaceId("workspace_agentworker01"),
        UserId("user_agentworker01"),
        ResourceRef("agent_run", "agent_run_worker0001", 1),
        ResourceRef("job", "agent_job_worker0001", 1),
        NOW,
    )
    store.outbox[str(dispatch.id)] = dispatch
    decision = ToolDecisionDispatch(
        AgentOutboxId("outbox_memorydecision1"),
        WorkspaceId("workspace_agentworker01"),
        UserId("user_agentworker01"),
        ResourceRef("agent_run", "agent_run_worker0001", 4),
        ResourceRef("job", "agent_job_worker0001", 4),
        ResourceRef("tool_approval", "approval_worker0001", 2),
        ToolCallId("tool_call_worker0001"),
        ToolDecision.APPROVE,
        NOW + timedelta(seconds=1),
    )
    store.outbox[str(decision.id)] = decision
    worker = _CapturingWorker()
    service = InMemoryAgentDispatchService(store, worker)

    first = await service.run_once()
    second = await service.run_once()

    assert (first.claimed, first.completed) == (2, 2)
    assert (second.claimed, second.completed) == (0, 0)
    assert store.published_outbox_ids == {str(dispatch.id), str(decision.id)}
    assert len(worker.claims) == 1
    assert len(worker.tool_decisions) == 1


@pytest.mark.asyncio
async def test_memory_lifespan_exposes_all_agent_routes_and_owns_worker_task(
    tmp_path: Path,
) -> None:
    """@brief 真实 memory composition 为 12 条路由安装服务并启停 worker / Real memory composition installs all 12 routes and starts/stops its worker."""
    settings = BackendSettings.from_file(PROJECT_ROOT / "example.jsonc")

    async with build_container(settings, tmp_path) as container:
        assert len(router_v2_agent.routes) == len(V2_AGENT_ENDPOINT_METHODS) == 12
        assert all(callable(getattr(container.agent_v2, method)) for method in V2_AGENT_ENDPOINT_METHODS)
        assert "aiws:agent:v2-outbox" in {
            task.get_name() for task in asyncio.all_tasks()
        }
        routes = _agent_model_routes(settings)
        assert len(routes) == 1
        assert routes[0].data_region is ModelRegion.PRIVATE_DEPLOYMENT
        assert not routes[0].external_processing
        assert settings.ai.model not in routes[0].model_ref.id

    assert "aiws:agent:v2-outbox" not in {
        task.get_name() for task in asyncio.all_tasks()
    }
