"""@brief API V2 统一 outbox 租约调度测试 / Unified API V2 outbox lease-dispatch tests."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta

import pytest

from backend.application.outbox_dispatch import (
    OutboxDispatchService,
    OutboxDispatchSettings,
)
from backend.application.ports.outbox_dispatch import (
    OutboxDispatchClaim,
    OutboxHandlerFailure,
    OutboxLease,
)
from backend.domain.platform import ApiEventId
from backend.domain.principals import UserId, WorkspaceId
from backend.domain.resources import ResourceRef


@dataclass(slots=True)
class _Clock:
    """@brief 可控带时区时钟 / Controllable timezone-aware clock."""

    value: datetime

    def __call__(self) -> datetime:
        """@brief 返回当前测试时间 / Return the current test instant."""
        return self.value


class _Repository:
    """@brief 精确记录 CAS 调用的内存 repository / In-memory repository recording exact CAS calls."""

    def __init__(self, claims: tuple[OutboxDispatchClaim, ...]) -> None:
        """@brief 绑定待返回 claims / Bind claims to return."""
        self.claims = claims
        self.completed: list[ApiEventId] = []
        self.retries: list[tuple[ApiEventId, str, datetime]] = []
        self.renewals = 0
        self.owns_lease = True

    async def claim(
        self,
        *,
        lease: OutboxLease,
        now: datetime,
        lease_seconds: int,
        batch_size: int,
        maximum_attempts: int,
    ) -> tuple[OutboxDispatchClaim, ...]:
        """@brief 把注入 claim 绑定到本批 lease / Bind injected claims to this batch lease."""
        del now, maximum_attempts
        assert lease_seconds == 5
        return tuple(replace(claim, lease=lease) for claim in self.claims[:batch_size])

    async def renew(
        self,
        claim: OutboxDispatchClaim,
        *,
        now: datetime,
        lease_seconds: int,
    ) -> bool:
        """@brief 记录续租 / Record renewal."""
        del claim, now, lease_seconds
        self.renewals += 1
        return self.owns_lease

    async def complete(
        self,
        claim: OutboxDispatchClaim,
        *,
        completed_at: datetime,
    ) -> bool:
        """@brief 仅在仍持有租约时完成 / Complete only while the lease is owned."""
        del completed_at
        if not self.owns_lease:
            return False
        self.completed.append(claim.event_id)
        return True

    async def retry(
        self,
        claim: OutboxDispatchClaim,
        *,
        error_code: str,
        retry_at: datetime,
        maximum_attempts: int,
    ) -> bool:
        """@brief 记录脱敏重试 / Record a redacted retry."""
        del maximum_attempts
        if not self.owns_lease:
            return False
        self.retries.append((claim.event_id, error_code, retry_at))
        return True


class _SuccessHandler:
    """@brief 记录成功处理 / Record successful handling."""

    def __init__(self) -> None:
        self.claims: list[OutboxDispatchClaim] = []

    async def handle(self, claim: OutboxDispatchClaim) -> None:
        """@brief 接受 claim / Accept a claim."""
        self.claims.append(claim)


class _ControlledFailureHandler:
    """@brief 抛出公开安全错误 / Raise a public-safe failure."""

    async def handle(self, claim: OutboxDispatchClaim) -> None:
        """@brief 以稳定 code 失败 / Fail with a stable code."""
        del claim
        raise OutboxHandlerFailure("agent.provider_unavailable")


class _UnexpectedFailureHandler:
    """@brief 抛出含 secret 的未知异常 / Raise an unexpected exception containing a secret."""

    async def handle(self, claim: OutboxDispatchClaim) -> None:
        """@brief 模拟上游泄漏性异常 / Simulate a leaky upstream exception."""
        del claim
        raise RuntimeError("provider said secret-token-should-not-persist")


class _ExhaustionFailureHandler:
    """@brief 持续失败并记录最终补偿 / Fail continuously and record final compensation."""

    def __init__(self, order: list[str] | None = None) -> None:
        """@brief 绑定可选顺序探针 / Bind an optional ordering probe."""
        self.exhausted: list[tuple[ApiEventId, str]] = []
        self.order = order

    async def handle(self, claim: OutboxDispatchClaim) -> None:
        """@brief 模拟每次均出现未分类异常 / Simulate an unclassified exception on every attempt."""
        del claim
        raise RuntimeError("secret repeated provider failure")

    async def on_exhausted(
        self,
        claim: OutboxDispatchClaim,
        *,
        error_code: str,
    ) -> None:
        """@brief 记录最后一次尝试的领域补偿 / Record final-attempt domain compensation."""
        self.exhausted.append((claim.event_id, error_code))
        if self.order is not None:
            self.order.append("compensate")


class _BrokenExhaustionHandler(_ExhaustionFailureHandler):
    """@brief 模拟领域补偿事务失败 / Simulate a failed domain-compensation transaction."""

    async def on_exhausted(
        self,
        claim: OutboxDispatchClaim,
        *,
        error_code: str,
    ) -> None:
        """@brief 明确失败且不伪造 outbox 终态 / Fail explicitly without fabricating outbox terminal state."""
        del claim, error_code
        raise RuntimeError("domain compensation unavailable")


class _OrderedRepository(_Repository):
    """@brief 记录补偿与 outbox fail 的因果顺序 / Record compensation-before-outbox-failure ordering."""

    def __init__(
        self,
        claims: tuple[OutboxDispatchClaim, ...],
        order: list[str],
    ) -> None:
        """@brief 绑定 claims 与共享顺序探针 / Bind claims and a shared ordering probe."""
        super().__init__(claims)
        self.order = order

    async def retry(
        self,
        claim: OutboxDispatchClaim,
        *,
        error_code: str,
        retry_at: datetime,
        maximum_attempts: int,
    ) -> bool:
        """@brief 在父实现前记录 retry/fail CAS / Record retry/fail CAS before delegating."""
        self.order.append("outbox_fail")
        return await super().retry(
            claim,
            error_code=error_code,
            retry_at=retry_at,
            maximum_attempts=maximum_attempts,
        )


def _claim(*, attempt_count: int = 1, event_type: str = "agent.run.queued") -> OutboxDispatchClaim:
    """@brief 构造有效 claim / Build a valid claim."""
    now = datetime(2026, 7, 23, 12, tzinfo=UTC)
    return OutboxDispatchClaim(
        ApiEventId("event_00000001"),
        WorkspaceId("workspace_00000001"),
        UserId("user_00000001"),
        ResourceRef("agent_run", "run_00000001", 1),
        event_type,
        {"run_id": "run_00000001"},
        attempt_count,
        OutboxLease("lease-token-with-at-least-thirty-two-characters"),
        now + timedelta(seconds=5),
    )


def _service(
    repository: _Repository,
    handlers: dict[str, object],
    clock: _Clock,
    *,
    attempts: int = 3,
) -> OutboxDispatchService:
    """@brief 构造确定性调度器 / Build a deterministic dispatcher."""
    return OutboxDispatchService(
        repository,
        handlers,  # type: ignore[arg-type]
        settings=OutboxDispatchSettings(
            batch_size=10,
            lease_seconds=5,
            maximum_attempts=attempts,
            retry_base_seconds=2,
            retry_cap_seconds=60,
        ),
        clock=clock,
        lease_factory=lambda: OutboxLease(
            "batch-lease-with-more-than-thirty-two-characters"
        ),
    )


@pytest.mark.asyncio
async def test_dispatch_completes_registered_and_notification_only_events() -> None:
    """@brief 已注册工作与纯通知事件都可完成 / Registered work and notification-only events complete."""
    clock = _Clock(datetime(2026, 7, 23, 12, tzinfo=UTC))
    claims = (
        _claim(),
        replace(
            _claim(event_type="knowledge_source.updated"),
            event_id=ApiEventId("event_00000002"),
        ),
    )
    repository = _Repository(claims)
    handler = _SuccessHandler()

    result = await _service(repository, {"agent.run.queued": handler}, clock).run_once()

    assert result.claimed == result.completed == 2
    assert result.retried == result.failed == result.lost_leases == 0
    assert [claim.event_id for claim in handler.claims] == [ApiEventId("event_00000001")]
    assert repository.completed == [ApiEventId("event_00000001"), ApiEventId("event_00000002")]


@pytest.mark.asyncio
async def test_dispatch_persists_only_controlled_or_generic_failure_codes() -> None:
    """@brief 重试记录不得保存异常正文 / Retry records never persist exception text."""
    clock = _Clock(datetime(2026, 7, 23, 12, tzinfo=UTC))
    controlled = _claim(event_type="agent.run.queued")
    unexpected = replace(
        _claim(event_type="interview.report.queued"),
        event_id=ApiEventId("event_00000002"),
    )
    repository = _Repository((controlled, unexpected))
    service = _service(
        repository,
        {
            "agent.run.queued": _ControlledFailureHandler(),
            "interview.report.queued": _UnexpectedFailureHandler(),
        },
        clock,
    )

    result = await service.run_once()

    assert result.retried == 2
    assert [item[1] for item in repository.retries] == [
        "agent.provider_unavailable",
        "outbox.handler_failed",
    ]
    assert "secret-token" not in repr(repository.retries)
    assert all(retry_at > clock.value for _, _, retry_at in repository.retries)


@pytest.mark.asyncio
async def test_attempt_cap_reports_terminal_failure_and_lost_completion() -> None:
    """@brief 尝试上限与丢失租约有独立结果 / Attempt cap and lost lease have distinct outcomes."""
    clock = _Clock(datetime(2026, 7, 23, 12, tzinfo=UTC))
    failed_repository = _Repository((_claim(attempt_count=3),))
    failed_result = await _service(
        failed_repository,
        {"agent.run.queued": _ControlledFailureHandler()},
        clock,
        attempts=3,
    ).run_once()
    assert failed_result.failed == 1
    assert failed_result.retried == 0

    lost_repository = _Repository((_claim(event_type="knowledge_source.updated"),))
    lost_repository.owns_lease = False
    lost_result = await _service(lost_repository, {}, clock).run_once()
    assert lost_result.lost_leases == 1
    assert lost_result.completed == 0


@pytest.mark.asyncio
async def test_continuous_unknown_failures_compensate_before_attempt_cap_failure() -> None:
    """@brief 连续未知异常仅在最后一次补偿，且补偿先于 outbox failed / Repeated unknown failures compensate only at the cap and before outbox failure."""
    clock = _Clock(datetime(2026, 7, 23, 12, tzinfo=UTC))
    handler = _ExhaustionFailureHandler()

    for attempt in (1, 2):
        repository = _Repository((_claim(attempt_count=attempt),))
        result = await _service(
            repository,
            {"agent.run.queued": handler},
            clock,
            attempts=3,
        ).run_once()
        assert result.retried == 1
        assert result.failed == 0
        assert handler.exhausted == []

    order: list[str] = []
    final_handler = _ExhaustionFailureHandler(order)
    final_repository = _OrderedRepository((_claim(attempt_count=3),), order)
    final = await _service(
        final_repository,
        {"agent.run.queued": final_handler},
        clock,
        attempts=3,
    ).run_once()

    assert final.failed == 1
    assert final.retried == 0
    assert final_handler.exhausted == [
        (ApiEventId("event_00000001"), "outbox.handler_failed")
    ]
    assert order == ["compensate", "outbox_fail"]


@pytest.mark.asyncio
async def test_failed_exhaustion_compensation_is_not_swallowed_or_marked_failed() -> None:
    """@brief 补偿事务失败时传播异常且不调用 outbox fail CAS / A failed compensation propagates and never invokes the outbox-failure CAS."""
    clock = _Clock(datetime(2026, 7, 23, 12, tzinfo=UTC))
    repository = _Repository((_claim(attempt_count=3),))

    with pytest.raises(RuntimeError, match="domain compensation unavailable"):
        await _service(
            repository,
            {"agent.run.queued": _BrokenExhaustionHandler()},
            clock,
            attempts=3,
        ).run_once()

    assert repository.retries == []


def test_required_handler_registry_fails_fast() -> None:
    """@brief 部署缺少必需 handler 时启动失败 / Startup fails when a required handler is missing."""
    repository = _Repository(())
    with pytest.raises(ValueError, match="required outbox handlers are missing"):
        OutboxDispatchService(
            repository,
            {},
            required_event_types=frozenset({"agent.run.queued"}),
        )
