"""@brief API V2 时间驱动维护端口 / API V2 time-driven maintenance ports.

维护端口只描述可安全重试的状态推进。账户删除采用持久化租约、幂等擦除和
compare-and-swap finalize 三段协议；进程崩溃只能留下可接管的 ``running`` 请求，不能
留下一个没有证据却声称 ``completed`` 的请求。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Protocol

from backend.domain.principals import UserId
from backend.domain.users import AccountDeletionFailure, AccountDeletionId


@dataclass(frozen=True, slots=True)
class AccountDeletionClaimToken:
    """@brief 数据库仅保存摘要的账户删除租约 / Account-deletion lease whose digest alone is persisted.

    @param token 不可记录的高熵原文 / High-entropy plaintext that must never be logged.
    """

    token: str = field(repr=False)

    def __post_init__(self) -> None:
        """@brief 校验 token 最小强度 / Validate minimum token strength.

        @raise ValueError token 太短、过长或非规范时抛出 / Raised for a short, oversized, or
            non-canonical token.
        """

        if not 32 <= len(self.token) <= 512 or self.token.strip() != self.token:
            raise ValueError("account deletion claim token must be a canonical high-entropy value")

    def reveal_to_repository(self) -> str:
        """@brief 仅向专用 adapter 交付 token / Reveal the token only to the dedicated adapter.

        @return 原始 token / Raw token.
        @note 返回值不得进入日志、遥测或业务 payload / The returned value must never enter
            logs, telemetry, or business payloads.
        """

        return self.token


@dataclass(frozen=True, slots=True)
class IdempotencyMaintenanceResult:
    """@brief 一批幂等 receipt 维护结果 / Result of one idempotency-receipt batch.

    @param purged_completed_receipts 已清理的过期 completed receipt 数 / Number of expired
        completed receipts purged.
    @param stranded_pending_receipts 本轮有界观测到的 stranded pending 数 / Stranded pending
        receipts observed within this bounded run.
    @param has_more_stranded_pending_receipts 是否至少还有一条未计入本轮观测 / Whether at
        least one further stranded pending receipt exists beyond this observation.
    @param oldest_stranded_expires_at 最早 stranded receipt 的过期边界 / Earliest expiry boundary
        among stranded receipts.

    @note pending 永不由维护任务删除或接管。它可能表示业务事务已提交但 receipt
        finalize 未完成，必须进入人工或领域专用对账流程。
    """

    purged_completed_receipts: int
    stranded_pending_receipts: int
    has_more_stranded_pending_receipts: bool
    oldest_stranded_expires_at: datetime | None

    def __post_init__(self) -> None:
        """@brief 校验统计量与可选时间 / Validate counts and optional timestamp.

        @raise ValueError 计数为负、时间无时区或零计数携带时间时抛出 / Raised for negative
            counts, a naive timestamp, or a timestamp attached to a zero count.
        """
        if self.purged_completed_receipts < 0 or self.stranded_pending_receipts < 0:
            raise ValueError("maintenance counts cannot be negative")
        if not isinstance(self.has_more_stranded_pending_receipts, bool):
            raise ValueError("stranded overflow marker must be boolean")
        if self.has_more_stranded_pending_receipts and self.stranded_pending_receipts == 0:
            raise ValueError("stranded overflow requires at least one observed receipt")
        if self.oldest_stranded_expires_at is not None and (
            self.oldest_stranded_expires_at.tzinfo is None
            or self.oldest_stranded_expires_at.utcoffset() is None
        ):
            raise ValueError("oldest stranded expiry must be timezone-aware")
        if (self.oldest_stranded_expires_at is None) is not (self.stranded_pending_receipts == 0):
            raise ValueError("stranded count and oldest expiry must agree")


class MaintenanceRepository(Protocol):
    """@brief 可重试的 V2 维护持久化端口 / Retriable V2 maintenance persistence port."""

    async def expire_due_invitations(self, *, now: datetime, batch_size: int) -> int:
        """@brief 推进一批已到期 pending 邀请 / Advance one batch of due pending invitations.

        @param now 由应用层提供的带时区截止时刻 / Timezone-aware cutoff from the application.
        @param batch_size 严格有界的最大处理数 / Strict upper bound on processed rows.
        @return 实际推进的邀请数 / Number of invitations actually advanced.
        """

    async def maintain_idempotency_receipts(
        self,
        *,
        now: datetime,
        batch_size: int,
    ) -> IdempotencyMaintenanceResult:
        """@brief 清理 completed 并观测 stranded pending / Purge completed and observe stranded pending.

        @param now 由应用层提供的带时区截止时刻 / Timezone-aware cutoff from the application.
        @param batch_size 严格有界的 completed 清理上限 / Strict completed-purge bound.
        @return 强类型清理与 stranded 统计 / Typed purge and stranded statistics.
        """

    async def purge_expired_outbox_events(
        self,
        *,
        now: datetime,
        batch_size: int,
    ) -> int:
        """@brief 清理一批已过 replay 窗口的终态 outbox 事件 / Purge one batch of terminal outbox events past replay retention.

        @param now 由应用层提供的带时区截止时刻 / Timezone-aware cutoff from the
            application.
        @param batch_size 严格有界的删除上限 / Strict deletion bound.
        @return 已删除的 published/failed 行数 / Number of published/failed rows deleted.

        @note 实现绝不得删除 pending/processing，即使它们的 replay 窗口已过期。
            / Implementations must never delete pending/processing rows, even after replay expiry.
        """


@dataclass(frozen=True, slots=True)
class AccountDeletionExecutionClaim:
    """@brief 账户删除执行的持久化独占 claim / Durable exclusive account-deletion claim.

    @param request_id 删除请求标识 / Deletion-request identifier.
    @param user_id 待匿名化用户 / User to anonymize.
    @param token 只能由 claim winner 持有的高熵令牌 / High-entropy token held only by the winner.
    @param expected_revision claim 后请求版本 / Request revision after claiming.
    @param claimed_at claim 时刻 / Claim instant.
    @param lease_expires_at 允许其他 worker 接管的时刻 / Instant after which another worker
        may take over.

    @note 正确实现必须把 token 的单向摘要持久化，并用 compare-and-swap finalize；不能用
        进程内锁或超时后盲目接管。
    """

    request_id: AccountDeletionId
    user_id: UserId
    token: AccountDeletionClaimToken = field(repr=False)
    expected_revision: int
    claimed_at: datetime
    lease_expires_at: datetime

    def __post_init__(self) -> None:
        """@brief 校验 claim 形状 / Validate the claim shape.

        @raise ValueError token、版本或时间无效时抛出 / Raised for an invalid token, revision,
            or timestamp.
        """
        if self.expected_revision < 2:
            raise ValueError("claimed account deletion revision must be at least two")
        for value in (self.claimed_at, self.lease_expires_at):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("account deletion claim times must be timezone-aware")
        if self.lease_expires_at <= self.claimed_at:
            raise ValueError("account deletion lease must expire after it is claimed")


@dataclass(frozen=True, slots=True)
class AccountDeletionErasureEvidence:
    """@brief 完成删除前必须原子核验的证据 / Evidence required before deletion completion.

    @param sessions_revoked 已撤销所有活跃 session / All active sessions were revoked.
    @param oauth_grants_revoked 已撤销 access/refresh token 与 OAuth grant / Access/refresh tokens
        and OAuth grants were revoked.
    @param credentials_revoked 已撤销密码与 WebAuthn credential / Password and WebAuthn
        credentials were revoked.
    @param external_connections_unlinked 外部连接已停用且 credential reference 已销毁 /
        External connections were disabled and credential references destroyed.
    @param identity_direct_identifiers_erased 用户直接标识符已擦除，稳定 tombstone 仍按保留
        义务视为假名化数据 / Direct identifiers were erased while the stable tombstone remains
        pseudonymous data subject to retention obligations.
    @param memberships_anonymized 共享 Workspace 的成员展示资料已匿名化 / Membership
        display profiles in shared Workspaces were anonymized.
    @param personal_workspaces_erased 仅属于该用户的 Workspace 数据已擦除 / Workspace data
        belonging only to the user was erased.
    @param shared_workspaces_detached 协作 Workspace 数据保留但删除用户已失去访问权 /
        Collaborative Workspace data was retained while the deleted user lost access.
    @param invitation_references_preserved 已保持 accepted invitation 的 ``RESTRICT`` 语义 /
        Accepted-invitation ``RESTRICT`` semantics were preserved.
    """

    sessions_revoked: bool
    oauth_grants_revoked: bool
    credentials_revoked: bool
    external_connections_unlinked: bool
    identity_direct_identifiers_erased: bool
    memberships_anonymized: bool
    personal_workspaces_erased: bool
    shared_workspaces_detached: bool
    invitation_references_preserved: bool

    def __post_init__(self) -> None:
        """@brief 拒绝不完整的“成功”证据 / Reject incomplete success evidence.

        @raise ValueError 任一删除不变量未被证明时抛出 / Raised when any deletion invariant
            remains unproven.
        """
        evidence = (
            self.sessions_revoked,
            self.oauth_grants_revoked,
            self.credentials_revoked,
            self.external_connections_unlinked,
            self.identity_direct_identifiers_erased,
            self.memberships_anonymized,
            self.personal_workspaces_erased,
            self.shared_workspaces_detached,
            self.invitation_references_preserved,
        )
        if any(not isinstance(item, bool) for item in evidence) or not all(evidence):
            raise ValueError("account deletion completion requires all erasure evidence")


type AccountDeletionExecutionOutcome = AccountDeletionErasureEvidence | AccountDeletionFailure
"""@brief 删除执行成功证据或结构化失败 / Erasure evidence or a structured execution failure."""


class AccountDeletionRetryableError(RuntimeError):
    """@brief 删除暂时无法执行且应由租约重试 / Deletion is temporarily unavailable and must retry after the lease."""


class AccountDeletionStaleClaimError(RuntimeError):
    """@brief 当前 worker 已失去删除租约 / The current worker no longer owns the deletion lease."""


class AccountDeletionFinalizeDecision(StrEnum):
    """@brief compare-and-swap finalize 判定 / Compare-and-swap finalization decision."""

    FINALIZED = "finalized"
    STALE_CLAIM = "stale_claim"


class AccountDeletionExecutionPort(Protocol):
    """@brief 账户删除的 claim/erase/finalize 端口 / Account-deletion claim/erase/finalize port.

    @note ``erase`` 必须幂等，并以 claim token 与 revision 绑定目标。外部系统暂时不可用
        时抛出 ``AccountDeletionRetryableError``，不要把瞬时故障永久写成 ``failed``。
        / ``erase`` must be idempotent and bind its target to the claim token and revision. Raise
        ``AccountDeletionRetryableError`` for transient dependencies rather than persisting a
        temporary outage as a permanent failure.
    """

    async def claim_due(
        self,
        *,
        now: datetime,
        batch_size: int,
    ) -> tuple[AccountDeletionExecutionClaim, ...]:
        """@brief 原子 claim 到期删除请求 / Atomically claim due deletion requests.

        @param now 带时区截止时刻 / Timezone-aware cutoff.
        @param batch_size 严格有界 claim 数 / Strict claim bound.
        @return 仅当前 worker 可 finalize 的 claims / Claims only this worker may finalize.
        """

    async def erase(
        self,
        claim: AccountDeletionExecutionClaim,
        *,
        erased_at: datetime,
    ) -> AccountDeletionExecutionOutcome:
        """@brief 幂等撤销凭据并处置用户数据 / Idempotently revoke credentials and dispose user data.

        @param claim 当前持久化独占 claim / Current durable exclusive claim.
        @param erased_at 带时区的擦除时刻 / Timezone-aware erasure instant.
        @return 完整证据或不可重试的结构化失败 / Complete evidence or a non-retryable
            structured failure.
        @raise AccountDeletionRetryableError 外部依赖或数据库暂时不可用 / A dependency or
            database is temporarily unavailable.
        """

    async def finalize(
        self,
        claim: AccountDeletionExecutionClaim,
        outcome: AccountDeletionExecutionOutcome,
        *,
        finalized_at: datetime,
    ) -> AccountDeletionFinalizeDecision:
        """@brief 以 token 与 revision 原子 finalize / Atomically finalize by token and revision.

        @param claim 原始独占 claim / Original exclusive claim.
        @param outcome 完整成功证据或失败原因 / Complete success evidence or failure reason.
        @param finalized_at 带时区完成时刻 / Timezone-aware finalization instant.
        @return finalized 或 stale-claim 判定 / Finalized or stale-claim decision.
        """


__all__ = [
    "AccountDeletionClaimToken",
    "AccountDeletionErasureEvidence",
    "AccountDeletionExecutionClaim",
    "AccountDeletionExecutionOutcome",
    "AccountDeletionExecutionPort",
    "AccountDeletionFinalizeDecision",
    "AccountDeletionRetryableError",
    "AccountDeletionStaleClaimError",
    "IdempotencyMaintenanceResult",
    "MaintenanceRepository",
]
