"""@brief API V2 单次维护服务与 adapter 测试 / API V2 one-shot maintenance service and adapter tests."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest

from backend.application.maintenance import MaintenanceBatchSizes, V2MaintenanceService
from backend.application.ports.maintenance import IdempotencyMaintenanceResult
from backend.application.ports.v2_idempotency import (
    IdempotencyDecisionKind,
    IdempotencyRequest,
    IdempotencyScope,
    ReplayableResponse,
)
from backend.domain.principals import InvitationId, ResourceMeta, UserId, WorkspaceId
from backend.domain.workspaces import Invitation, InvitationStatus, WorkspaceRole
from backend.infrastructure.access import InMemoryAccessStore
from backend.infrastructure.maintenance import InMemoryMaintenanceRepository
from backend.infrastructure.v2_idempotency import InMemoryV2IdempotencyStore

NOW = datetime(2026, 7, 23, 12, 0, tzinfo=UTC)
"""@brief 测试使用的确定性 UTC 时刻 / Deterministic UTC instant used by tests."""


@dataclass(slots=True)
class _RecordingRepository:
    """@brief 记录 application 调用的测试端口 / Test port recording application calls."""

    expired: int = 3
    invitations_call: tuple[datetime, int] | None = None
    idempotency_call: tuple[datetime, int] | None = None
    outbox_call: tuple[datetime, int] | None = None

    async def expire_due_invitations(self, *, now: datetime, batch_size: int) -> int:
        """@brief 记录邀请调用 / Record the invitation call."""
        self.invitations_call = (now, batch_size)
        return self.expired

    async def maintain_idempotency_receipts(
        self,
        *,
        now: datetime,
        batch_size: int,
    ) -> IdempotencyMaintenanceResult:
        """@brief 记录 receipt 调用 / Record the receipt call."""
        self.idempotency_call = (now, batch_size)
        return IdempotencyMaintenanceResult(2, 1, False, NOW - timedelta(days=1))

    async def purge_expired_outbox_events(
        self,
        *,
        now: datetime,
        batch_size: int,
    ) -> int:
        """@brief 记录 outbox retention 调用 / Record the outbox-retention call."""

        self.outbox_call = (now, batch_size)
        return 4


class _BlockingRepository:
    """@brief 在第一事务等待取消的测试端口 / Test port awaiting cancellation in its first transaction."""

    def __init__(self) -> None:
        """@brief 初始化同步事件 / Initialize synchronization events."""
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.receipts_called = False

    async def expire_due_invitations(self, *, now: datetime, batch_size: int) -> int:
        """@brief 阻塞到 task 取消 / Block until the task is cancelled."""
        del now, batch_size
        self.entered.set()
        await self.release.wait()
        return 0

    async def maintain_idempotency_receipts(
        self,
        *,
        now: datetime,
        batch_size: int,
    ) -> IdempotencyMaintenanceResult:
        """@brief 标记不应到达的第二步 / Mark the second phase, which must not be reached."""
        del now, batch_size
        self.receipts_called = True
        return IdempotencyMaintenanceResult(0, 0, False, None)

    async def purge_expired_outbox_events(
        self,
        *,
        now: datetime,
        batch_size: int,
    ) -> int:
        """@brief 第一步取消后不应进入 outbox 清理 / Outbox purge is unreachable after first-step cancellation."""

        del now, batch_size
        raise AssertionError("outbox maintenance must not run after cancellation")


def _invitation(identifier: str, *, expires_at: datetime) -> Invitation:
    """@brief 构造 pending 测试邀请 / Build a pending test invitation.

    @param identifier 邀请 ID / Invitation identifier.
    @param expires_at 到期时刻 / Expiration instant.
    @return 合法 pending 邀请 / Valid pending invitation.
    """
    created_at = NOW - timedelta(days=10)
    return Invitation(
        ResourceMeta(InvitationId(identifier), 1, created_at, created_at),
        WorkspaceId("ws_maintenance"),
        f"{identifier}@example.com",
        WorkspaceRole.VIEWER,
        InvitationStatus.PENDING,
        expires_at,
    )


def _idempotency_request(identifier: str) -> IdempotencyRequest:
    """@brief 构造独立幂等 scope / Build an independent idempotency scope.

    @param identifier scope 区分值 / Scope discriminator.
    @return 规范请求 / Canonical request.
    """
    return IdempotencyRequest(
        IdempotencyScope(
            UserId("usr_maintenance"),
            WorkspaceId("ws_maintenance"),
            "POST",
            f"/api/v2/workspaces/ws_maintenance/{identifier}",
            f"maintenance-key-{identifier}",
        ),
        b"{}",
        "application/json",
        None,
    )


@pytest.mark.asyncio
async def test_run_once_uses_one_cutoff_and_returns_typed_metrics() -> None:
    """@brief 单次运行共享截止时刻并返回完整统计 / A run shares one cutoff and returns complete metrics."""
    repository = _RecordingRepository()
    times = iter((NOW, NOW + timedelta(seconds=2)))
    service = V2MaintenanceService(
        repository,
        batch_sizes=MaintenanceBatchSizes(
            invitations=17,
            idempotency_receipts=23,
            outbox_events=29,
        ),
        clock=lambda: next(times),
    )

    result = await service.run_once()

    assert repository.invitations_call == (NOW, 17)
    assert repository.idempotency_call == (NOW, 23)
    assert repository.outbox_call == (NOW, 29)
    assert result.started_at == NOW
    assert result.finished_at == NOW + timedelta(seconds=2)
    assert result.expired_invitations == 3
    assert result.idempotency.purged_completed_receipts == 2
    assert result.idempotency.stranded_pending_receipts == 1
    assert result.purged_outbox_events == 4


@pytest.mark.asyncio
async def test_run_once_propagates_task_cancellation_and_skips_later_phase() -> None:
    """@brief task 取消原样传播且不启动后续事务 / Task cancellation propagates and skips the later transaction."""
    repository = _BlockingRepository()
    task = asyncio.create_task(V2MaintenanceService(repository, clock=lambda: NOW).run_once())
    await repository.entered.wait()

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert repository.receipts_called is False


@pytest.mark.parametrize("value", [0, 1001, True, 2.5])
def test_batch_sizes_are_strictly_bounded(value: object) -> None:
    """@brief 拒绝无界或 bool 批量 / Reject unbounded or boolean batch sizes.

    @param value 非法批量 / Invalid batch value.
    """
    with pytest.raises(ValueError, match="batch size"):
        MaintenanceBatchSizes(invitations=value)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_memory_adapter_expires_oldest_pending_with_exact_revision_fields() -> None:
    """@brief 内存 adapter 只推进最老到期项并精确更新版本字段 / Memory adapter advances oldest due rows with exact revisions."""
    access_store = InMemoryAccessStore()
    idempotency_store = InMemoryV2IdempotencyStore()
    repository = InMemoryMaintenanceRepository(access_store, idempotency_store)
    due_later = _invitation("inv_due_later", expires_at=NOW - timedelta(hours=1))
    due_first = _invitation("inv_due_first", expires_at=NOW - timedelta(days=1))
    future = _invitation("inv_future", expires_at=NOW + timedelta(hours=1))
    access_store.invitations = {
        str(item.meta.id): item for item in (due_later, due_first, future)
    }

    count = await repository.expire_due_invitations(now=NOW, batch_size=1)

    assert count == 1
    expired = access_store.invitations["inv_due_first"]
    assert expired.status is InvitationStatus.EXPIRED
    assert expired.meta.revision == 2
    assert expired.meta.updated_at == NOW
    assert expired.resolved_at == NOW
    assert access_store.invitations["inv_due_later"].status is InvitationStatus.PENDING
    assert access_store.invitations["inv_future"].status is InvitationStatus.PENDING


@pytest.mark.asyncio
async def test_memory_adapter_purges_only_completed_and_surfaces_stranded_pending() -> None:
    """@brief 清理 completed 时 pending 永久保留且可观测 / Pending remains durable and observable while completed is purged."""
    access_store = InMemoryAccessStore()
    idempotency_store = InMemoryV2IdempotencyStore()
    repository = InMemoryMaintenanceRepository(access_store, idempotency_store)
    old_pending = _idempotency_request("old-pending")
    older_pending = _idempotency_request("older-pending")
    old_completed = _idempotency_request("old-completed")
    fresh_completed = _idempotency_request("fresh-completed")

    pending_decision = await idempotency_store.claim(
        old_pending,
        now=NOW - timedelta(days=3),
        expires_at=NOW - timedelta(days=2),
    )
    older_pending_decision = await idempotency_store.claim(
        older_pending,
        now=NOW - timedelta(days=4),
        expires_at=NOW - timedelta(days=3),
    )
    old_decision = await idempotency_store.claim(
        old_completed,
        now=NOW - timedelta(days=3),
        expires_at=NOW - timedelta(days=2),
    )
    fresh_decision = await idempotency_store.claim(
        fresh_completed,
        now=NOW - timedelta(hours=1),
        expires_at=NOW + timedelta(days=1),
    )
    assert pending_decision.kind is IdempotencyDecisionKind.CLAIMED
    assert older_pending_decision.kind is IdempotencyDecisionKind.CLAIMED
    assert old_decision.claim is not None
    assert fresh_decision.claim is not None
    response = ReplayableResponse(201, (("Content-Type", "application/json"),), b"{}")
    await idempotency_store.complete(
        old_decision.claim,
        response,
        completed_at=NOW - timedelta(days=2, hours=23),
        expires_at=NOW - timedelta(days=1),
    )
    await idempotency_store.complete(
        fresh_decision.claim,
        response,
        completed_at=NOW - timedelta(minutes=50),
        expires_at=NOW + timedelta(days=1),
    )

    result = await repository.maintain_idempotency_receipts(now=NOW, batch_size=1)

    assert result == IdempotencyMaintenanceResult(1, 1, True, NOW - timedelta(days=3))
    repeated_pending = await idempotency_store.claim(
        old_pending,
        now=NOW,
        expires_at=NOW + timedelta(days=1),
    )
    assert repeated_pending.kind is IdempotencyDecisionKind.IN_PROGRESS
    fresh_replay = await idempotency_store.claim(
        fresh_completed,
        now=NOW,
        expires_at=NOW + timedelta(days=1),
    )
    assert fresh_replay.kind is IdempotencyDecisionKind.REPLAY


@pytest.mark.asyncio
async def test_memory_adapter_validates_outbox_retention_inputs_without_sweeping_work() -> None:
    """@brief 内存模式不伪造 durable sweep 但仍验证边界 / Memory mode performs no fake durable sweep but validates bounds."""

    repository = InMemoryMaintenanceRepository(
        InMemoryAccessStore(),
        InMemoryV2IdempotencyStore(),
    )

    assert await repository.purge_expired_outbox_events(now=NOW, batch_size=1) == 0
    with pytest.raises(ValueError, match="batch size"):
        await repository.purge_expired_outbox_events(now=NOW, batch_size=0)


def test_stranded_metric_requires_an_oldest_timestamp() -> None:
    """@brief stranded count 与最早时间必须成对 / Stranded count and oldest timestamp must agree."""
    with pytest.raises(ValueError, match="must agree"):
        IdempotencyMaintenanceResult(0, 1, False, None)
