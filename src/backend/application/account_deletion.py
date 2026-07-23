"""@brief API V2 账户删除执行编排 / API V2 account-deletion execution orchestration.

HTTP 用例只负责在近期重新认证后创建可取消的 ``scheduled`` 请求；本模块负责冷静期后的
后台执行。每个动作都有持久化租约，擦除是幂等的，完成状态只能由同一 claim 的完整证据
通过 compare-and-swap 写入。
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from backend.application.ports.maintenance import (
    AccountDeletionErasureEvidence,
    AccountDeletionExecutionPort,
    AccountDeletionFinalizeDecision,
    AccountDeletionRetryableError,
    AccountDeletionStaleClaimError,
)
from backend.domain.users import AccountDeletionFailure

_MAXIMUM_BATCH_SIZE = 100
"""@brief 单轮删除的硬上限 / Hard upper bound for one deletion pass."""


@dataclass(frozen=True, slots=True)
class AccountDeletionRunResult:
    """@brief 单轮账户删除执行统计 / Statistics for one account-deletion pass.

    @param started_at 本轮 claim 截止时间 / Claim cutoff for this pass.
    @param finished_at 本轮结束时间 / Completion instant for this pass.
    @param claimed 已获取的持久化 claim 数 / Durable claims acquired.
    @param completed 以完整擦除证据完成的请求数 / Requests completed with full evidence.
    @param failed 以不可重试业务失败结束的请求数 / Requests ended with non-retryable failures.
    @param retryable 等待租约到期重试的请求数 / Requests left for retry after lease expiry.
    @param stale_claims finalize 时已失去所有权的请求数 / Claims no longer owned at finalize.
    """

    started_at: datetime
    finished_at: datetime
    claimed: int
    completed: int
    failed: int
    retryable: int
    stale_claims: int

    def __post_init__(self) -> None:
        """@brief 校验时间与互斥计数 / Validate timestamps and mutually exclusive counts.

        @raise ValueError 时间无时区、倒序或计数不守恒时抛出 / Raised for naive or reversed
            timestamps, or counters that do not conserve claims.
        """

        for value in (self.started_at, self.finished_at):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("account deletion result timestamps must be timezone-aware")
        counts = (self.claimed, self.completed, self.failed, self.retryable, self.stale_claims)
        if any(isinstance(value, bool) or value < 0 for value in counts):
            raise ValueError("account deletion result counts cannot be negative")
        if self.finished_at < self.started_at:
            raise ValueError("account deletion finish cannot precede start")
        if self.claimed != self.completed + self.failed + self.retryable + self.stale_claims:
            raise ValueError("account deletion result counts must conserve claimed work")


class AccountDeletionExecutionService:
    """@brief 执行到期删除且区分永久与瞬时失败 / Execute due deletions while separating permanent and transient failures.

    @param port 持久化 claim、擦除与 finalize adapter / Durable claim, erasure, and finalize adapter.
    @param batch_size 单轮严格上限 / Strict per-pass upper bound.
    @param clock 可测试带时区时钟 / Testable timezone-aware clock.

    @note 未知异常原样冒泡，因为把编程错误伪装成可重试故障会无限吞错。只有 adapter
        显式映射为 ``AccountDeletionRetryableError`` 的依赖故障才等待租约重试。
        / Unknown exceptions propagate because treating programming defects as retryable outages
        would hide them forever. Only dependency failures explicitly mapped to
        ``AccountDeletionRetryableError`` wait for lease retry.
    """

    def __init__(
        self,
        port: AccountDeletionExecutionPort,
        *,
        batch_size: int = 10,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        """@brief 配置单轮执行器 / Configure the one-pass executor.

        @param port 删除执行 port / Deletion execution port.
        @param batch_size 1..100 的有界批量 / Bounded batch between 1 and 100.
        @param clock 可选同步时钟 / Optional synchronous clock.
        @raise ValueError 批量非法时抛出 / Raised for an invalid batch size.
        """

        if (
            isinstance(batch_size, bool)
            or not isinstance(batch_size, int)
            or not 1 <= batch_size <= _MAXIMUM_BATCH_SIZE
        ):
            raise ValueError("account deletion batch size must be between 1 and 100")
        self._port = port
        self._batch_size = batch_size
        self._clock = clock or _utc_now

    async def run_once(self) -> AccountDeletionRunResult:
        """@brief claim 并执行一批到期账户删除 / Claim and execute one batch of due account deletions.

        @return 完整且守恒的单轮统计 / Complete, claim-conserving pass statistics.
        @raise asyncio.CancelledError 取消原样传播并由租约恢复 / Cancellation propagates and
            the durable lease provides recovery.
        @raise Exception 未分类实现错误原样传播 / Unclassified implementation defects propagate.
        """

        started_at = _aware(self._clock(), "account deletion start")
        claims = await self._port.claim_due(now=started_at, batch_size=self._batch_size)
        completed = failed = retryable = stale = 0
        for claim in claims:
            await asyncio.sleep(0)
            erased_at = _aware(self._clock(), "account deletion erasure")
            try:
                outcome = await self._port.erase(claim, erased_at=erased_at)
            except asyncio.CancelledError:
                raise
            except AccountDeletionRetryableError:
                retryable += 1
                continue
            except AccountDeletionStaleClaimError:
                stale += 1
                continue
            finalized_at = _aware(self._clock(), "account deletion finalize")
            decision = await self._port.finalize(
                claim,
                outcome,
                finalized_at=finalized_at,
            )
            if decision is AccountDeletionFinalizeDecision.STALE_CLAIM:
                stale += 1
            elif isinstance(outcome, AccountDeletionErasureEvidence):
                completed += 1
            elif isinstance(outcome, AccountDeletionFailure):
                failed += 1
            else:  # pragma: no cover - static union plus runtime boundary defense
                raise RuntimeError("account deletion port returned an invalid outcome")
        finished_at = _aware(self._clock(), "account deletion finish")
        return AccountDeletionRunResult(
            started_at,
            finished_at,
            len(claims),
            completed,
            failed,
            retryable,
            stale,
        )


def _utc_now() -> datetime:
    """@brief 返回带时区 UTC 当前时刻 / Return the current timezone-aware UTC instant.

    @return 当前时刻 / Current instant.
    """

    return datetime.now(UTC)


def _aware(value: datetime, name: str) -> datetime:
    """@brief 要求时钟值含时区 / Require a timezone-aware clock value.

    @param value 待校验时间 / Timestamp to validate.
    @param name 错误上下文 / Error context.
    @return 原值 / Original value.
    @raise ValueError 无 UTC offset 时抛出 / Raised when no UTC offset is available.
    """

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value


__all__ = ["AccountDeletionExecutionService", "AccountDeletionRunResult"]
