"""@brief 可恢复的统一 outbox 调度用例 / Recoverable unified-outbox dispatch use case.

调度器不知道 Agent、Knowledge 或 Interview 的内部结构；composition root 以
``event_type -> handler`` 的封闭表注册已实现工作。每条 handler 执行期间定期
CAS（Compare-And-Swap）续租，进程崩溃后由过期租约自动恢复。
"""

from __future__ import annotations

import asyncio
import hashlib
import secrets
from collections.abc import Callable, Coroutine, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from backend.application.ports.outbox_dispatch import (
    OutboxClaimRepository,
    OutboxDispatchClaim,
    OutboxEventHandler,
    OutboxExhaustionHandler,
    OutboxHandlerFailure,
    OutboxLease,
)

_MAXIMUM_BATCH_SIZE = 100
"""@brief 单轮 claim 硬上限 / Hard cap for one claim batch."""

_MAXIMUM_ATTEMPTS = 100
"""@brief 可配置尝试次数硬上限 / Hard cap for configurable attempts."""


class OutboxLeaseLost(RuntimeError):
    """@brief handler 执行期间租约丢失 / Lease lost while a handler was running."""


@dataclass(frozen=True, slots=True)
class OutboxDispatchSettings:
    """@brief 调度的有界重试与租约策略 / Bounded dispatch retry and lease policy.

    @param batch_size 单轮最大事件数 / Maximum events per run.
    @param lease_seconds 每次 claim/续租寿命 / Claim and renewal lifetime.
    @param maximum_attempts 失败终结上限 / Terminal failure cap.
    @param retry_base_seconds 指数退避基数 / Exponential-backoff base.
    @param retry_cap_seconds 指数退避上限 / Exponential-backoff cap.
    """

    batch_size: int = 25
    lease_seconds: int = 120
    maximum_attempts: int = 12
    retry_base_seconds: int = 2
    retry_cap_seconds: int = 300

    def __post_init__(self) -> None:
        """@brief 校验调度策略硬边界 / Validate hard dispatch-policy bounds."""
        integer_values = (
            self.batch_size,
            self.lease_seconds,
            self.maximum_attempts,
            self.retry_base_seconds,
            self.retry_cap_seconds,
        )
        if any(isinstance(value, bool) or not isinstance(value, int) for value in integer_values):
            raise ValueError("outbox dispatch settings must contain integers")
        if not 1 <= self.batch_size <= _MAXIMUM_BATCH_SIZE:
            raise ValueError("outbox dispatch batch size must be between 1 and 100")
        if not 5 <= self.lease_seconds <= 900:
            raise ValueError("outbox dispatch lease must be between 5 and 900 seconds")
        if not 1 <= self.maximum_attempts <= _MAXIMUM_ATTEMPTS:
            raise ValueError("outbox dispatch attempts must be between 1 and 100")
        if not 1 <= self.retry_base_seconds <= self.retry_cap_seconds <= 86_400:
            raise ValueError("outbox retry backoff bounds are invalid")


@dataclass(frozen=True, slots=True)
class OutboxDispatchResult:
    """@brief 一轮调度的可观测结果 / Observable result of one dispatch run.

    @param claimed 本轮取得数 / Claims obtained.
    @param completed 已 published 数 / Events marked published.
    @param retried 已安排重试数 / Events scheduled for retry.
    @param failed 达尝试上限数 / Events reaching the attempt cap.
    @param lost_leases CAS 不再属于本 worker 的数量 / Claims no longer owned by CAS.
    """

    claimed: int
    completed: int
    retried: int
    failed: int
    lost_leases: int

    def __post_init__(self) -> None:
        """@brief 校验计数非负且不超过 claim / Validate non-negative counts bounded by claims."""
        outcomes = (self.completed, self.retried, self.failed, self.lost_leases)
        if self.claimed < 0 or any(value < 0 for value in outcomes):
            raise ValueError("outbox dispatch counts cannot be negative")
        if sum(outcomes) > self.claimed:
            raise ValueError("outbox dispatch outcomes cannot exceed claims")


class OutboxDispatchService:
    """@brief 租约心跳、脱敏重试与封闭路由调度器 / Dispatcher with leases, redacted retries, and closed routing."""

    def __init__(
        self,
        repository: OutboxClaimRepository,
        handlers: Mapping[str, OutboxEventHandler],
        *,
        required_event_types: frozenset[str] = frozenset(),
        settings: OutboxDispatchSettings | None = None,
        clock: Callable[[], datetime] | None = None,
        lease_factory: Callable[[], OutboxLease] | None = None,
    ) -> None:
        """@brief 绑定窄 repository 与穷尽 handler 表 / Bind the narrow repository and exhaustive handler map.

        @param repository 全局 claim/finalize 窄端口 / Narrow global claim/finalize port.
        @param handlers 稳定 event type 到 handler 的不可变快照 / Immutable handler snapshot.
        @param required_event_types 未注册即重试的内部工作事件 / Internal work events
            retried when unregistered.
        @param settings 有界调度策略 / Bounded dispatch policy.
        @param clock 可测试 UTC 时钟 / Testable UTC clock.
        @param lease_factory 可测试高熵租约工厂 / Testable high-entropy lease factory.
        """
        copied = dict(handlers)
        if any(not event_type or not hasattr(handler, "handle") for event_type, handler in copied.items()):
            raise ValueError("outbox handler registry is invalid")
        if not required_event_types <= copied.keys():
            # A required handler missing at startup is a deployment error, not an event-specific
            # surprise discovered after the request has already committed.
            missing = sorted(required_event_types - copied.keys())
            raise ValueError(f"required outbox handlers are missing: {', '.join(missing)}")
        self._repository = repository
        self._handlers = copied
        self._required_event_types = required_event_types
        self._settings = settings or OutboxDispatchSettings()
        self._clock = clock or _utc_now
        self._lease_factory = lease_factory or _new_lease

    async def run_once(self) -> OutboxDispatchResult:
        """@brief claim 并处理一个有界批次 / Claim and process one bounded batch.

        @return completed/retry/failed/lease-lost 计数 / Completion, retry, failure, and
            lease-loss counts.
        @raise asyncio.CancelledError 关停时原样传播，由租约过期恢复 / Propagated
            unchanged on shutdown; lease expiry performs recovery.
        """
        now = _require_aware(self._clock(), "outbox claim time")
        lease = self._lease_factory()
        claims = await self._repository.claim(
            lease=lease,
            now=now,
            lease_seconds=self._settings.lease_seconds,
            batch_size=self._settings.batch_size,
            maximum_attempts=self._settings.maximum_attempts,
        )
        completed = 0
        retried = 0
        failed = 0
        lost_leases = 0
        for claim in claims:
            if claim.lease != lease:
                raise RuntimeError("outbox repository returned a claim for a different lease")
            handler = self._handlers.get(claim.event_type)
            if handler is None:
                # Public notification-only events are already durable and visible through SSE; no
                # second side effect is required, so publishing them is the correct terminal action.
                if claim.event_type in self._required_event_types:
                    outcome = await self._retry(
                        claim,
                        "outbox.handler_unconfigured",
                        handler=None,
                    )
                    retried += outcome == "retried"
                    failed += outcome == "failed"
                    lost_leases += outcome == "lost"
                    continue
                if await self._repository.complete(
                    claim,
                    completed_at=_require_aware(self._clock(), "outbox completion time"),
                ):
                    completed += 1
                else:
                    lost_leases += 1
                continue
            try:
                await self._handle_with_renewal(handler, claim)
            except asyncio.CancelledError:
                raise
            except OutboxLeaseLost:
                lost_leases += 1
                continue
            except OutboxHandlerFailure as error:
                outcome = await self._retry(claim, error.code, handler=handler)
                retried += outcome == "retried"
                failed += outcome == "failed"
                lost_leases += outcome == "lost"
                continue
            except Exception:
                outcome = await self._retry(
                    claim,
                    "outbox.handler_failed",
                    handler=handler,
                )
                retried += outcome == "retried"
                failed += outcome == "failed"
                lost_leases += outcome == "lost"
                continue
            if await self._repository.complete(
                claim,
                completed_at=_require_aware(self._clock(), "outbox completion time"),
            ):
                completed += 1
            else:
                lost_leases += 1
        return OutboxDispatchResult(
            claimed=len(claims),
            completed=completed,
            retried=retried,
            failed=failed,
            lost_leases=lost_leases,
        )

    async def _handle_with_renewal(
        self,
        handler: OutboxEventHandler,
        claim: OutboxDispatchClaim,
    ) -> None:
        """@brief handler 执行时定期 CAS 续租 / Renew the lease periodically while a handler runs.

        @param handler 幂等领域 handler / Idempotent domain handler.
        @param claim 当前 claim / Current claim.
        @raise OutboxLeaseLost token 不再匹配或续租存储不可用时抛出 / Raised
            when the token no longer matches or renewal storage is unavailable.
        """
        await self._run_with_renewal(
            claim,
            handler.handle(claim),
            task_name=f"aiws:outbox:{claim.event_type}:{claim.event_id}",
        )

    async def _run_with_renewal(
        self,
        claim: OutboxDispatchClaim,
        operation: Coroutine[object, object, None],
        *,
        task_name: str,
    ) -> None:
        """@brief 在一个异步操作期间维持 claim 租约 / Maintain the claim lease during one async operation.

        @param claim 当前 claim / Current claim.
        @param operation handler 或耗尽补偿 coroutine / Handler or exhaustion-compensation
            coroutine.
        @param task_name 可观测且不含 secret 的任务名 / Observable secret-free task name.
        @raise OutboxLeaseLost token 不再匹配或续租存储不可用时抛出 / Raised when the
            token no longer matches or renewal storage is unavailable.
        """
        task = asyncio.create_task(operation, name=task_name)
        heartbeat_seconds = max(1.0, self._settings.lease_seconds / 3)
        try:
            while True:
                done, _ = await asyncio.wait({task}, timeout=heartbeat_seconds)
                if done:
                    await task
                    return
                try:
                    renewed = await self._repository.renew(
                        claim,
                        now=_require_aware(self._clock(), "outbox renewal time"),
                        lease_seconds=self._settings.lease_seconds,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    raise OutboxLeaseLost("outbox lease renewal failed") from error
                if not renewed:
                    raise OutboxLeaseLost("outbox lease is no longer owned")
        finally:
            if not task.done():
                task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                # The main branch has already observed or is about to normalize the handler failure.
                pass

    async def _retry(
        self,
        claim: OutboxDispatchClaim,
        error_code: str,
        *,
        handler: OutboxEventHandler | None,
    ) -> str:
        """@brief 指数退避后 CAS 重试或失败 / CAS retry or failure after exponential backoff.

        @param claim 失败 claim / Failed claim.
        @param error_code 公开安全稳定 code / Public-safe stable code.
        @param handler 可在最终失败前补偿领域状态的 handler / Handler that may compensate
            domain state before terminal failure.
        @return ``retried``、``failed`` 或 ``lost`` / Retried, failed, or lost.
        """
        if (
            claim.attempt_count >= self._settings.maximum_attempts
            and isinstance(handler, OutboxExhaustionHandler)
        ):
            await self._run_with_renewal(
                claim,
                handler.on_exhausted(claim, error_code=error_code),
                task_name=(
                    f"aiws:outbox-exhausted:{claim.event_type}:{claim.event_id}"
                ),
            )
        retry_at = _require_aware(self._clock(), "outbox retry time") + _retry_delay(
            claim,
            base_seconds=self._settings.retry_base_seconds,
            cap_seconds=self._settings.retry_cap_seconds,
        )
        changed = await self._repository.retry(
            claim,
            error_code=error_code,
            retry_at=retry_at,
            maximum_attempts=self._settings.maximum_attempts,
        )
        if not changed:
            return "lost"
        return "failed" if claim.attempt_count >= self._settings.maximum_attempts else "retried"


def _retry_delay(
    claim: OutboxDispatchClaim,
    *,
    base_seconds: int,
    cap_seconds: int,
) -> timedelta:
    """@brief 生成有界指数退避与确定性抖动 / Build bounded exponential backoff with deterministic jitter.

    @param claim 用 event ID 稳定抖动的 claim / Claim whose event ID seeds jitter.
    @param base_seconds 退避基数 / Backoff base.
    @param cap_seconds 退避上限 / Backoff cap.
    @return 80%..120% 内的正延迟 / Positive delay between 80% and 120%.
    """
    exponent = min(max(claim.attempt_count - 1, 0), 30)
    nominal = min(cap_seconds, base_seconds * (1 << exponent))
    digest = hashlib.sha256(str(claim.event_id).encode("utf-8")).digest()
    jitter_basis_points = 8_000 + int.from_bytes(digest[:2], "big") * 4_001 // 65_536
    milliseconds = max(1_000, nominal * 1_000 * jitter_basis_points // 10_000)
    return timedelta(milliseconds=milliseconds)


def _new_lease() -> OutboxLease:
    """@brief 生成高熵 URL-safe 租约 / Generate a high-entropy URL-safe lease.

    @return 含 384-bit CSPRNG 材料的租约 / Lease containing 384 bits of CSPRNG material.
    """
    return OutboxLease(secrets.token_urlsafe(48))


def _utc_now() -> datetime:
    """@brief 返回当前 UTC 时间 / Return the current UTC instant."""
    return datetime.now(UTC)


def _require_aware(value: datetime, label: str) -> datetime:
    """@brief 要求时钟值带时区 / Require a timezone-aware clock value.

    @param value 候选时间 / Candidate timestamp.
    @param label 错误上下文 / Error context.
    @return 原时间 / Original timestamp.
    """
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware")
    return value


__all__ = [
    "OutboxDispatchResult",
    "OutboxDispatchService",
    "OutboxDispatchSettings",
    "OutboxLeaseLost",
]
