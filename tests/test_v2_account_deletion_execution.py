"""@brief API V2 账户删除执行协议测试 / API V2 account-deletion execution protocol tests."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import pytest

from backend.application.account_deletion import AccountDeletionExecutionService
from backend.application.ports.maintenance import (
    AccountDeletionClaimToken,
    AccountDeletionErasureEvidence,
    AccountDeletionExecutionClaim,
    AccountDeletionExecutionOutcome,
    AccountDeletionFinalizeDecision,
    AccountDeletionRetryableError,
)
from backend.domain.principals import UserId
from backend.domain.users import AccountDeletionFailure, AccountDeletionId

NOW = datetime(2026, 7, 23, 14, 0, tzinfo=UTC)
"""@brief 测试的确定性时刻 / Deterministic test instant."""


def _claim(identifier: str) -> AccountDeletionExecutionClaim:
    """@brief 构造有效持久化 claim / Build a valid durable claim.

    @param identifier 请求 ID 后缀 / Request-ID suffix.
    @return 有一分钟租约的 claim / Claim with a one-minute lease.
    """

    return AccountDeletionExecutionClaim(
        AccountDeletionId(f"delreq_{identifier}"),
        UserId(f"user_{identifier}"),
        AccountDeletionClaimToken(f"account-deletion-claim-token-{identifier}-0123456789"),
        2,
        NOW,
        NOW + timedelta(minutes=1),
    )


def _evidence() -> AccountDeletionErasureEvidence:
    """@brief 构造完整擦除证据 / Build complete erasure evidence.

    @return 所有不变量均已证明的 evidence / Evidence proving every invariant.
    """

    return AccountDeletionErasureEvidence(
        sessions_revoked=True,
        oauth_grants_revoked=True,
        credentials_revoked=True,
        external_connections_unlinked=True,
        identity_direct_identifiers_erased=True,
        memberships_anonymized=True,
        personal_workspaces_erased=True,
        shared_workspaces_detached=True,
        invitation_references_preserved=True,
    )


@dataclass(slots=True)
class _ExecutionPort:
    """@brief 产生所有互斥结果的记录型 port / Recording port producing every exclusive outcome."""

    claims: tuple[AccountDeletionExecutionClaim, ...]
    erased: list[str] = field(default_factory=list)
    finalized: list[str] = field(default_factory=list)

    async def claim_due(
        self,
        *,
        now: datetime,
        batch_size: int,
    ) -> tuple[AccountDeletionExecutionClaim, ...]:
        """@brief 返回有界 claims / Return bounded claims."""

        assert now == NOW
        return self.claims[:batch_size]

    async def erase(
        self,
        claim: AccountDeletionExecutionClaim,
        *,
        erased_at: datetime,
    ) -> AccountDeletionExecutionOutcome:
        """@brief 按 ID 产生成功、失败或重试 / Produce success, failure, or retry by ID."""

        assert erased_at == NOW
        identifier = str(claim.request_id)
        self.erased.append(identifier)
        if identifier.endswith("retry"):
            raise AccountDeletionRetryableError("vault temporarily unavailable")
        if identifier.endswith("failed"):
            return AccountDeletionFailure(
                "account_deletion.legal_hold",
                "a required legal hold prevents automatic erasure",
            )
        return _evidence()

    async def finalize(
        self,
        claim: AccountDeletionExecutionClaim,
        outcome: AccountDeletionExecutionOutcome,
        *,
        finalized_at: datetime,
    ) -> AccountDeletionFinalizeDecision:
        """@brief 记录 finalize 并模拟一个失效 claim / Record finalize and simulate one stale claim."""

        del outcome
        assert finalized_at == NOW
        identifier = str(claim.request_id)
        self.finalized.append(identifier)
        return (
            AccountDeletionFinalizeDecision.STALE_CLAIM
            if identifier.endswith("stale")
            else AccountDeletionFinalizeDecision.FINALIZED
        )


@pytest.mark.asyncio
async def test_execution_conserves_claims_across_all_outcomes() -> None:
    """@brief 成功、失败、重试、失效 claim 互斥且守恒 / Success, failure, retry, and stale claims are exclusive and conserved."""

    port = _ExecutionPort(
        tuple(_claim(identifier) for identifier in ("done", "failed", "retry", "stale"))
    )
    result = await AccountDeletionExecutionService(port, batch_size=4, clock=lambda: NOW).run_once()

    assert result.claimed == 4
    assert result.completed == 1
    assert result.failed == 1
    assert result.retryable == 1
    assert result.stale_claims == 1
    assert port.erased == [
        "delreq_done",
        "delreq_failed",
        "delreq_retry",
        "delreq_stale",
    ]
    assert "delreq_retry" not in port.finalized


class _BlockingPort:
    """@brief 擦除期间等待取消的 port / Port awaiting cancellation during erasure."""

    def __init__(self) -> None:
        """@brief 初始化同步事件 / Initialize synchronization events."""

        self.entered = asyncio.Event()
        self.finalized = False

    async def claim_due(
        self,
        *,
        now: datetime,
        batch_size: int,
    ) -> tuple[AccountDeletionExecutionClaim, ...]:
        """@brief 返回单个 claim / Return one claim."""

        del now, batch_size
        return (_claim("cancelled"),)

    async def erase(
        self,
        claim: AccountDeletionExecutionClaim,
        *,
        erased_at: datetime,
    ) -> AccountDeletionExecutionOutcome:
        """@brief 等待 task cancellation / Await task cancellation."""

        del claim, erased_at
        self.entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def finalize(
        self,
        claim: AccountDeletionExecutionClaim,
        outcome: AccountDeletionExecutionOutcome,
        *,
        finalized_at: datetime,
    ) -> AccountDeletionFinalizeDecision:
        """@brief 标记不应发生的 finalize / Mark an unexpected finalize."""

        del claim, outcome, finalized_at
        self.finalized = True
        return AccountDeletionFinalizeDecision.FINALIZED


@pytest.mark.asyncio
async def test_cancellation_propagates_without_false_finalization() -> None:
    """@brief 取消保留租约恢复且不伪造完成 / Cancellation leaves lease recovery and never fabricates completion."""

    port = _BlockingPort()
    task = asyncio.create_task(AccountDeletionExecutionService(port, clock=lambda: NOW).run_once())
    await port.entered.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert port.finalized is False


def test_erasure_evidence_rejects_partial_success() -> None:
    """@brief 任一未证明的不变量都拒绝 completed / Any unproven invariant rejects completed evidence."""

    values = {
        "sessions_revoked": True,
        "oauth_grants_revoked": True,
        "credentials_revoked": True,
        "external_connections_unlinked": True,
        "identity_direct_identifiers_erased": True,
        "memberships_anonymized": True,
        "personal_workspaces_erased": False,
        "shared_workspaces_detached": True,
        "invitation_references_preserved": True,
    }
    with pytest.raises(ValueError, match="all erasure evidence"):
        AccountDeletionErasureEvidence(**values)


@pytest.mark.parametrize("batch_size", [0, 101, True, 2.5])
def test_execution_batch_is_strictly_bounded(batch_size: object) -> None:
    """@brief 拒绝无界或 bool 批量 / Reject unbounded or boolean batches.

    @param batch_size 非法批量 / Invalid batch size.
    """

    with pytest.raises(ValueError, match="batch size"):
        AccountDeletionExecutionService(  # type: ignore[arg-type]
            _ExecutionPort(()),
            batch_size=batch_size,
        )
