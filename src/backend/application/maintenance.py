"""@brief API V2 单次维护用例 / API V2 one-shot maintenance use case.

composition 可以按部署环境选择调度器，但 application 层只暴露一次 ``run_once``；这样
不会把 leader election、退避策略或进程生命周期耦合进业务用例。
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from backend.application.ports.maintenance import (
    IdempotencyMaintenanceResult,
    MaintenanceRepository,
)

_MAXIMUM_BATCH_SIZE = 1_000
"""@brief 单事务最多推进的行数 / Maximum rows advanced in one transaction."""


@dataclass(frozen=True, slots=True)
class MaintenanceBatchSizes:
    """@brief 各类维护动作的独立批量上限 / Independent batch bounds for maintenance actions.

    @param invitations 邀请到期批量 / Invitation-expiry batch size.
    @param idempotency_receipts completed receipt 清理批量 / Completed-receipt purge batch size.
    @param outbox_events 过期 terminal outbox 清理批量 / Expired terminal-outbox purge
        batch size.
    """

    invitations: int = 250
    idempotency_receipts: int = 250
    outbox_events: int = 250

    def __post_init__(self) -> None:
        """@brief 校验批量严格有界 / Validate strict batch bounds.

        @raise ValueError 批量不是 1..1000 的整数时抛出 / Raised unless each batch is an
            integer between one and one thousand.
        """
        for name, value in (
            ("invitations", self.invitations),
            ("idempotency_receipts", self.idempotency_receipts),
            ("outbox_events", self.outbox_events),
        ):
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(f"{name} maintenance batch size must be an integer")
            if not 1 <= value <= _MAXIMUM_BATCH_SIZE:
                raise ValueError(f"{name} maintenance batch size must be between 1 and 1000")


@dataclass(frozen=True, slots=True)
class MaintenanceRunResult:
    """@brief 一次维护执行的强类型结果 / Typed result of one maintenance run.

    @param started_at 本轮一致性截止时刻 / Consistent cutoff instant for this run.
    @param finished_at 本轮完成时刻 / Completion instant for this run.
    @param expired_invitations 推进到 expired 的邀请数 / Invitations advanced to expired.
    @param idempotency 幂等 receipt 清理与 stranded 统计 / Receipt purge and stranded metrics.
    @param purged_outbox_events 已清理的过期 terminal outbox 数 / Expired terminal
        outbox rows purged.
    """

    started_at: datetime
    finished_at: datetime
    expired_invitations: int
    idempotency: IdempotencyMaintenanceResult
    purged_outbox_events: int

    def __post_init__(self) -> None:
        """@brief 校验运行结果时间与计数 / Validate run timestamps and count.

        @raise ValueError 时间无时区、倒序或计数为负时抛出 / Raised for naive or reversed
            timestamps, or a negative count.
        """
        for value in (self.started_at, self.finished_at):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("maintenance result timestamps must be timezone-aware")
        if self.finished_at < self.started_at:
            raise ValueError("maintenance finish cannot precede start")
        if self.expired_invitations < 0 or self.purged_outbox_events < 0:
            raise ValueError("maintenance counts cannot be negative")


class V2MaintenanceService:
    """@brief 可取消且不拥有定时循环的 V2 维护服务 / Cancellable V2 service without a timer loop.

    @param repository 维护持久化端口 / Maintenance persistence port.
    @param batch_sizes 每类动作的批量上限 / Per-action batch limits.
    @param clock 可测试带时区时钟 / Testable timezone-aware clock.

    @note ``asyncio.Task.cancel()`` 会原样传播 ``CancelledError``。每个 repository 调用是
        独立短事务；若取消发生在两步之间，已提交的第一步不会伪装回滚，下一轮可幂等继续。
    """

    def __init__(
        self,
        repository: MaintenanceRepository,
        *,
        batch_sizes: MaintenanceBatchSizes | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        """@brief 配置单次维护服务 / Configure the one-shot maintenance service.

        @param repository 维护持久化端口 / Maintenance persistence port.
        @param batch_sizes 可选批量上限 / Optional batch limits.
        @param clock 可选同步时钟 / Optional synchronous clock.
        """
        self._repository = repository
        self._batch_sizes = batch_sizes or MaintenanceBatchSizes()
        self._clock = clock or _utc_now

    async def run_once(self) -> MaintenanceRunResult:
        """@brief 推进一次有界维护批次 / Advance one bounded maintenance batch.

        @return 邀请、receipt 与 stranded pending 的完整统计 / Complete invitation, receipt,
            and stranded-pending statistics.
        @raise asyncio.CancelledError caller 取消 task 时原样传播 / Propagated unchanged when
            the caller cancels the task.
        @raise ValueError 时钟返回无时区或倒序时间时抛出 / Raised for a naive or reversed clock.
        """
        started_at = _require_aware(self._clock(), "maintenance start")
        await asyncio.sleep(0)
        expired_invitations = await self._repository.expire_due_invitations(
            now=started_at,
            batch_size=self._batch_sizes.invitations,
        )
        if expired_invitations < 0:
            raise RuntimeError("maintenance repository returned a negative invitation count")
        await asyncio.sleep(0)
        idempotency = await self._repository.maintain_idempotency_receipts(
            now=started_at,
            batch_size=self._batch_sizes.idempotency_receipts,
        )
        await asyncio.sleep(0)
        purged_outbox_events = await self._repository.purge_expired_outbox_events(
            now=started_at,
            batch_size=self._batch_sizes.outbox_events,
        )
        if purged_outbox_events < 0:
            raise RuntimeError("maintenance repository returned a negative outbox count")
        finished_at = _require_aware(self._clock(), "maintenance finish")
        return MaintenanceRunResult(
            started_at=started_at,
            finished_at=finished_at,
            expired_invitations=expired_invitations,
            idempotency=idempotency,
            purged_outbox_events=purged_outbox_events,
        )


def _utc_now() -> datetime:
    """@brief 返回带时区 UTC 当前时间 / Return the current timezone-aware UTC time.

    @return 当前 UTC 时间 / Current UTC instant.
    """
    return datetime.now(UTC)


def _require_aware(value: datetime, name: str) -> datetime:
    """@brief 要求时钟值含时区 / Require a timezone-aware clock value.

    @param value 待校验时间 / Timestamp to validate.
    @param name 错误上下文名称 / Error-context name.
    @return 原时间值 / Original timestamp.
    @raise ValueError 时间无 UTC offset 时抛出 / Raised when the timestamp has no UTC offset.
    """
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value


__all__ = ["MaintenanceBatchSizes", "MaintenanceRunResult", "V2MaintenanceService"]
