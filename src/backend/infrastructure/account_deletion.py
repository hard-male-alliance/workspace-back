"""@brief PostgreSQL 账户删除窄权限 adapter / Narrow-privilege PostgreSQL account-deletion adapter.

跨用户扫描和身份擦除只通过 owner-owned ``SECURITY DEFINER`` 函数执行。应用角色不获得
绕过 RLS（Row-Level Security）的表级写权限；claim 原文只驻留当前 worker，数据库保存
用途分离的 SHA-256 摘要。
"""

from __future__ import annotations

import hashlib
import re
import secrets
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from sqlalchemy import text
from sqlalchemy.engine import Row
from sqlalchemy.exc import DBAPIError

from backend.application.ports.maintenance import (
    AccountDeletionClaimToken,
    AccountDeletionErasureEvidence,
    AccountDeletionExecutionClaim,
    AccountDeletionExecutionOutcome,
    AccountDeletionFinalizeDecision,
    AccountDeletionRetryableError,
    AccountDeletionStaleClaimError,
)
from backend.domain.connections import ConnectionOwnership
from backend.domain.principals import UserId, WorkspaceId
from backend.domain.upload_sessions import UploadSessionId
from backend.domain.users import AccountDeletionFailure, AccountDeletionId
from backend.infrastructure.persistence.database import AsyncDatabase

_CLAIM_SQL = text(
    "SELECT request_id, user_id, expected_revision, claimed_at, lease_expires_at "
    "FROM identity.claim_due_account_deletions("
    ":claim_token_hash, :candidate_now, :lease_seconds, :batch_size, :maximum_attempts)"
)
"""@brief 到期请求 claim 窄函数 / Narrow due-request claim function."""

_ERASE_SQL = text(
    "SELECT sessions_revoked, oauth_grants_revoked, credentials_revoked, "
    "external_connections_unlinked, identity_direct_identifiers_erased, memberships_anonymized, "
    "personal_workspaces_erased, shared_workspaces_detached, "
    "invitation_references_preserved, failure_code, failure_detail "
    "FROM identity.erase_account_for_deletion("
    ":request_id, :claim_token_hash, :expected_revision, :erased_at)"
)
"""@brief token-bound 幂等擦除窄函数 / Token-bound idempotent erasure function."""

_FINALIZE_SQL = text(
    "SELECT identity.finalize_account_deletion("
    ":request_id, :claim_token_hash, :expected_revision, :outcome, "
    ":failure_code, :failure_detail, :finalized_at)"
)
"""@brief 完成或永久失败的 CAS 窄函数 / Narrow completion-or-permanent-failure CAS function."""

_CLAIM_ITEMS_SQL = text(
    "SELECT workspace_id, resource_kind, resource_id, item_attempt "
    "FROM identity.claim_account_deletion_erasure_items("
    ":request_id, :account_claim_hash, :expected_revision, :item_lease_hash, "
    ":candidate_now, :lease_seconds, :batch_size)"
)
"""@brief 外部擦除 item claim 窄函数 / Narrow external-erasure item claim function."""

_COMPLETE_ITEM_SQL = text(
    "SELECT identity.complete_account_deletion_erasure_item("
    ":request_id, :account_claim_hash, :expected_revision, :workspace_id, "
    ":resource_kind, :resource_id, :item_lease_hash)"
)
"""@brief 外部 item 成功 CAS / External-item success CAS."""

_RETRY_ITEM_SQL = text(
    "SELECT identity.retry_account_deletion_erasure_item("
    ":request_id, :account_claim_hash, :expected_revision, :workspace_id, "
    ":resource_kind, :resource_id, :item_lease_hash, :error_code, :permanent)"
)
"""@brief 外部 item 重试或失败 CAS / External-item retry-or-failure CAS."""

_EXTERNAL_STATE_SQL = text(
    "SELECT recipient_email, pending_items, failed_items "
    "FROM identity.account_deletion_external_state("
    ":request_id, :account_claim_hash, :expected_revision)"
)
"""@brief token-bound 外部状态投影 / Token-bound external-state projection."""

_RELEASE_PROGRESS_SQL = text(
    "SELECT identity.release_account_deletion_progress("
    ":request_id, :account_claim_hash, :expected_revision)"
)
"""@brief 成功分批后立即让出 account 租约 / Yield an account lease after bounded progress."""

_SHA256_HEX = re.compile(r"^[a-f0-9]{64}$")
"""@brief 租约摘要语法 / Lease-digest grammar."""

_RETRYABLE_SQLSTATES = frozenset(
    {
        "40001",  # serialization_failure
        "40P01",  # deadlock_detected
        "55P03",  # lock_not_available
        "57014",  # query_canceled, including statement timeout
    }
)
"""@brief 可以安全交给租约重试的 PostgreSQL SQLSTATE / PostgreSQL SQLSTATEs safe for lease retry."""


@dataclass(frozen=True, slots=True)
class _ErasureItem:
    """@brief 已租约的显式外部擦除项 / Explicit leased external-erasure item.

    @param workspace_id 精确 Workspace scope / Exact Workspace scope.
    @param resource_kind ``upload_object`` 或 ``credential_scope`` / Resource kind.
    @param resource_id 不透明资源标识 / Opaque resource identifier.
    @param attempt 当前持久化尝试号 / Current durable attempt number.
    """

    workspace_id: WorkspaceId
    resource_kind: str
    resource_id: str
    attempt: int


@dataclass(frozen=True, slots=True)
class _ExternalState:
    """@brief 外部擦除与待发邮件状态 / External-erasure and queued-email state."""

    recipient_email: str | None
    pending_items: int
    failed_items: int


class _UploadErasure(Protocol):
    """@brief Workspace 上传对象的有界幂等擦除器 / Bounded idempotent Workspace-upload eraser."""

    async def erase(
        self,
        workspace_id: WorkspaceId,
        upload_ids: tuple[UploadSessionId, ...],
    ) -> int:
        """@brief 擦除显式对象集 / Erase an explicit object set."""


class _CreatorSecretErasureResult(Protocol):
    """@brief creator secret 批量结果的结构类型 / Structural creator-secret batch result."""

    @property
    def has_more(self) -> bool:
        """@brief 是否仍有私密材料待擦除 / Whether private material remains to erase."""
        ...


class _CreatorSecretErasure(Protocol):
    """@brief creator+Workspace credential 加密擦除器 / Creator-and-Workspace credential eraser."""

    async def erase_created_by(
        self,
        ownership: ConnectionOwnership,
        *,
        limit: int = 1_000,
    ) -> _CreatorSecretErasureResult:
        """@brief 擦除一批 vault secret / Erase one batch of vault secrets."""


class _RecipientEmailErasure(Protocol):
    """@brief 删除前清空待发身份邮件的 Port / Port clearing queued identity mail before deletion."""

    async def erase_recipient(self, recipient: str, *, limit: int = 1_000) -> bool:
        """@brief 无在途租约且已清空时返回真 / Return true when empty without an in-flight lease."""


class PostgresAccountDeletionExecutionPort:
    """@brief 账户删除 claim/erase/finalize PostgreSQL 实现 / PostgreSQL account-deletion claim/erase/finalize implementation."""

    def __init__(
        self,
        database: AsyncDatabase,
        *,
        lease_seconds: int = 300,
        maximum_attempts: int = 12,
        upload_erasure: _UploadErasure | None = None,
        creator_secret_erasure: _CreatorSecretErasure | None = None,
        recipient_email_erasure: _RecipientEmailErasure | None = None,
        external_batch_size: int = 100,
        external_lease_seconds: int = 120,
    ) -> None:
        """@brief 绑定数据库和恢复边界 / Bind the database and recovery bounds.

        @param database lifespan-owned 异步数据库 / Lifespan-owned asynchronous database.
        @param lease_seconds 60..3600 秒持久化租约 / Durable lease between 60 and 3600 seconds.
        @param maximum_attempts 永久失败前 1..100 次上限 / Attempt cap between 1 and 100.
        @param upload_erasure 上传对象擦除器 / Upload-object eraser.
        @param creator_secret_erasure Connection vault 擦除器 / Connection-vault eraser.
        @param recipient_email_erasure 待发身份邮件擦除器 / Queued identity-email eraser.
        @param external_batch_size 单次最多 1..100 个外部 item / At most 1..100 external items.
        @param external_lease_seconds 30..600 秒 item 租约 / Item lease between 30 and 600 seconds.
        @raise ValueError 边界非法时抛出 / Raised for invalid bounds.
        """

        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, int)
            or not 60 <= lease_seconds <= 3_600
        ):
            raise ValueError("account deletion lease must be between 60 and 3600 seconds")
        if (
            isinstance(maximum_attempts, bool)
            or not isinstance(maximum_attempts, int)
            or not 1 <= maximum_attempts <= 100
        ):
            raise ValueError("account deletion attempts must be between 1 and 100")
        if (
            isinstance(external_batch_size, bool)
            or not isinstance(external_batch_size, int)
            or not 1 <= external_batch_size <= 100
        ):
            raise ValueError("account deletion external batch size must be between 1 and 100")
        if (
            isinstance(external_lease_seconds, bool)
            or not isinstance(external_lease_seconds, int)
            or not 30 <= external_lease_seconds <= 600
        ):
            raise ValueError("account deletion external lease must be between 30 and 600 seconds")
        self._database = database
        self._lease_seconds = lease_seconds
        self._maximum_attempts = maximum_attempts
        self._upload_erasure = upload_erasure
        self._creator_secret_erasure = creator_secret_erasure
        self._recipient_email_erasure = recipient_email_erasure
        self._external_batch_size = external_batch_size
        self._external_lease_seconds = external_lease_seconds

    async def claim_due(
        self,
        *,
        now: datetime,
        batch_size: int,
    ) -> tuple[AccountDeletionExecutionClaim, ...]:
        """@brief 以 SKIP LOCKED claim 一批到期或过期租约 / Claim due or expired leases with SKIP LOCKED.

        @param now 应用候选截止时间 / Application candidate cutoff.
        @param batch_size 1..100 的批量 / Batch between 1 and 100.
        @return 当前 worker 独占 claims / Claims exclusively owned by this worker.
        """

        _validate_time_and_batch(now, batch_size)
        token = AccountDeletionClaimToken(secrets.token_urlsafe(48))
        async with self._database.unscoped_transaction() as session:
            rows = (
                await session.execute(
                    _CLAIM_SQL,
                    {
                        "claim_token_hash": _claim_digest(token),
                        "candidate_now": now,
                        "lease_seconds": self._lease_seconds,
                        "batch_size": batch_size,
                        "maximum_attempts": self._maximum_attempts,
                    },
                )
            ).all()
        claims = tuple(_claim_from_row(row, token) for row in rows)
        if len(claims) > batch_size:
            raise RuntimeError("account deletion claim function exceeded its batch size")
        return claims

    async def erase(
        self,
        claim: AccountDeletionExecutionClaim,
        *,
        erased_at: datetime,
    ) -> AccountDeletionExecutionOutcome:
        """@brief 调用 token-bound 幂等擦除 / Invoke token-bound idempotent erasure.

        @param claim 当前 claim / Current claim.
        @param erased_at 应用候选擦除时刻 / Application candidate erasure instant.
        @return 完整证据或不可重试失败 / Complete evidence or non-retryable failure.
        @raise AccountDeletionStaleClaimError claim 已失效 / The claim is stale.
        @raise AccountDeletionRetryableError 确认属于瞬时数据库故障 / A confirmed transient
            database failure occurred.
        """

        _require_aware(erased_at, "account deletion erasure")
        try:
            await self._erase_external_state(claim, erased_at=erased_at)
            async with self._database.unscoped_transaction() as session:
                row = (
                    await session.execute(
                        _ERASE_SQL,
                        {
                            "request_id": str(claim.request_id),
                            "claim_token_hash": _claim_digest(claim.token),
                            "expected_revision": claim.expected_revision,
                            "erased_at": erased_at,
                        },
                    )
                ).one_or_none()
        except DBAPIError as error:
            if _is_retryable_database_error(error):
                raise AccountDeletionRetryableError(
                    "account deletion storage is temporarily unavailable"
                ) from error
            raise
        if row is None:
            raise AccountDeletionStaleClaimError("account deletion claim is no longer current")
        return _outcome_from_row(row)

    async def _erase_external_state(
        self,
        claim: AccountDeletionExecutionClaim,
        *,
        erased_at: datetime,
    ) -> None:
        """@brief 推进一批 durable 外部擦除并清理待发邮件 / Advance one durable external-erasure batch and clear queued mail.

        @param claim 当前账户 claim / Current account claim.
        @param erased_at 本轮数据库权威时间候选 / Database-authoritative time candidate.
        @raise AccountDeletionRetryableError 还有有界工作或依赖暂不可用 / Bounded work remains
            or a dependency is temporarily unavailable.
        @raise AccountDeletionStaleClaimError account/item 租约已丢失 / The account or item lease
            is stale.
        """

        item_token, items = await self._claim_external_items(claim, now=erased_at)
        for item in items:
            if item.resource_kind == "upload_object":
                await self._erase_upload_item(claim, item_token, item)
            elif item.resource_kind == "credential_scope":
                await self._erase_credential_item(claim, item_token, item)
            else:  # pragma: no cover - database constraint plus runtime boundary defense
                raise RuntimeError("account deletion returned an unsupported erasure item")
        state = await self._external_state(claim)
        if state.failed_items > 0:
            return
        if state.pending_items > 0:
            if items:
                await self._release_progress(claim)
            raise AccountDeletionRetryableError("account deletion has bounded external work left")
        if state.recipient_email is not None:
            if self._recipient_email_erasure is None:
                raise RuntimeError("account deletion email erasure is not configured")
            if not await self._recipient_email_erasure.erase_recipient(state.recipient_email):
                raise AccountDeletionRetryableError(
                    "account deletion is waiting for identity-email leases"
                )

    async def _claim_external_items(
        self,
        claim: AccountDeletionExecutionClaim,
        *,
        now: datetime,
    ) -> tuple[AccountDeletionClaimToken, tuple[_ErasureItem, ...]]:
        """@brief claim 一批 item 并只返回当前 worker 的 token / Claim one item batch with a worker-only token.

        @return item token 与强类型 items / Item token and typed items.
        """

        token = AccountDeletionClaimToken(secrets.token_urlsafe(48))
        async with self._database.unscoped_transaction() as session:
            rows = (
                await session.execute(
                    _CLAIM_ITEMS_SQL,
                    {
                        **_claim_arguments(claim),
                        "item_lease_hash": _item_digest(token),
                        "candidate_now": now,
                        "lease_seconds": self._external_lease_seconds,
                        "batch_size": self._external_batch_size,
                    },
                )
            ).all()
        items = tuple(_erasure_item_from_row(row) for row in rows)
        if len(items) > self._external_batch_size:
            raise RuntimeError("account deletion item claim exceeded its batch size")
        return token, items

    async def _erase_upload_item(
        self,
        claim: AccountDeletionExecutionClaim,
        item_token: AccountDeletionClaimToken,
        item: _ErasureItem,
    ) -> None:
        """@brief 幂等删除一个 upload object 并 CAS 完成 / Idempotently delete one upload object and CAS-complete it."""

        if self._upload_erasure is None:
            raise RuntimeError("account deletion upload erasure is not configured")
        try:
            erased = await self._upload_erasure.erase(
                item.workspace_id,
                (UploadSessionId(item.resource_id),),
            )
        except (OSError, RuntimeError) as error:
            await self._retry_external_item(
                claim,
                item_token,
                item,
                error_code="account_deletion.object_store_unavailable",
            )
            if item.attempt >= 100:
                return
            raise AccountDeletionRetryableError(
                "account deletion object storage is temporarily unavailable"
            ) from error
        if erased != 1:
            raise RuntimeError("account deletion upload eraser returned an invalid count")
        await self._complete_external_item(claim, item_token, item)

    async def _erase_credential_item(
        self,
        claim: AccountDeletionExecutionClaim,
        item_token: AccountDeletionClaimToken,
        item: _ErasureItem,
    ) -> None:
        """@brief 清空 creator vault scope 并 CAS 完成 / Clear one creator vault scope and CAS-complete it."""

        if self._creator_secret_erasure is None:
            raise RuntimeError("account deletion credential erasure is not configured")
        result = await self._creator_secret_erasure.erase_created_by(
            ConnectionOwnership(item.workspace_id, claim.user_id),
            limit=1_000,
        )
        if not isinstance(result.has_more, bool):
            raise RuntimeError("account deletion credential eraser returned invalid state")
        if result.has_more:
            await self._retry_external_item(
                claim,
                item_token,
                item,
                error_code="account_deletion.credential_batch_incomplete",
            )
            if item.attempt >= 100:
                return
            await self._release_progress(claim)
            raise AccountDeletionRetryableError(
                "account deletion has bounded credential erasure work left"
            )
        await self._complete_external_item(claim, item_token, item)

    async def _complete_external_item(
        self,
        claim: AccountDeletionExecutionClaim,
        item_token: AccountDeletionClaimToken,
        item: _ErasureItem,
    ) -> None:
        """@brief 用 item token CAS 完成 / CAS-complete with the item token."""

        async with self._database.unscoped_transaction() as session:
            completed = await session.scalar(
                _COMPLETE_ITEM_SQL,
                {
                    **_claim_arguments(claim),
                    **_item_arguments(item_token, item),
                },
            )
        if completed is not True:
            raise AccountDeletionStaleClaimError("account deletion item lease is no longer current")

    async def _retry_external_item(
        self,
        claim: AccountDeletionExecutionClaim,
        item_token: AccountDeletionClaimToken,
        item: _ErasureItem,
        *,
        error_code: str,
    ) -> None:
        """@brief 释放一个 item 供安全重试 / Release one item for safe retry."""

        async with self._database.unscoped_transaction() as session:
            released = await session.scalar(
                _RETRY_ITEM_SQL,
                {
                    **_claim_arguments(claim),
                    **_item_arguments(item_token, item),
                    "error_code": error_code,
                    "permanent": False,
                },
            )
        if released is not True:
            raise AccountDeletionStaleClaimError("account deletion item lease is no longer current")

    async def _release_progress(
        self,
        claim: AccountDeletionExecutionClaim,
    ) -> None:
        """@brief 成功推进一批后返还失败预算并让下一批接手 / Return failure budget and yield after successful bounded progress.

        @param claim 当前账户 claim / Current account claim.
        @raise AccountDeletionStaleClaimError account 租约已丢失 / The account lease is stale.
        """

        async with self._database.unscoped_transaction() as session:
            released = await session.scalar(
                _RELEASE_PROGRESS_SQL,
                _claim_arguments(claim),
            )
        if released is not True:
            raise AccountDeletionStaleClaimError("account deletion claim is no longer current")

    async def _external_state(
        self,
        claim: AccountDeletionExecutionClaim,
    ) -> _ExternalState:
        """@brief 读取 token-bound 外部完成水位 / Read the token-bound external completion watermark."""

        async with self._database.unscoped_transaction() as session:
            row = (
                await session.execute(_EXTERNAL_STATE_SQL, _claim_arguments(claim))
            ).one_or_none()
        if row is None:
            raise AccountDeletionStaleClaimError("account deletion claim is no longer current")
        return _external_state_from_row(row)

    async def finalize(
        self,
        claim: AccountDeletionExecutionClaim,
        outcome: AccountDeletionExecutionOutcome,
        *,
        finalized_at: datetime,
    ) -> AccountDeletionFinalizeDecision:
        """@brief 以 token+revision CAS 写入 completed/failed / Persist completed or failed by token-plus-revision CAS.

        @param claim 原 claim / Original claim.
        @param outcome 完整证据或永久失败 / Complete evidence or permanent failure.
        @param finalized_at 应用候选完成时刻 / Application candidate completion instant.
        @return finalized 或 stale_claim / Finalized or stale claim.
        """

        _require_aware(finalized_at, "account deletion finalize")
        if isinstance(outcome, AccountDeletionErasureEvidence):
            status = "completed"
            failure_code = failure_detail = None
        else:
            status = "failed"
            failure_code = outcome.code
            failure_detail = outcome.detail
        async with self._database.unscoped_transaction() as session:
            value = await session.scalar(
                _FINALIZE_SQL,
                {
                    "request_id": str(claim.request_id),
                    "claim_token_hash": _claim_digest(claim.token),
                    "expected_revision": claim.expected_revision,
                    "outcome": status,
                    "failure_code": failure_code,
                    "failure_detail": failure_detail,
                    "finalized_at": finalized_at,
                },
            )
        if not isinstance(value, bool):
            raise RuntimeError("account deletion finalize function returned a non-boolean result")
        return (
            AccountDeletionFinalizeDecision.FINALIZED
            if value
            else AccountDeletionFinalizeDecision.STALE_CLAIM
        )


def _claim_arguments(claim: AccountDeletionExecutionClaim) -> dict[str, object]:
    """@brief 构造不泄漏原 token 的 SQL 参数 / Build SQL arguments without exposing the raw token.

    @param claim 当前账户 claim / Current account claim.
    @return 公共标识与用途分离摘要 / Public identifiers and a domain-separated digest.
    """

    return {
        "request_id": str(claim.request_id),
        "account_claim_hash": _claim_digest(claim.token),
        "expected_revision": claim.expected_revision,
    }


def _item_arguments(
    token: AccountDeletionClaimToken,
    item: _ErasureItem,
) -> dict[str, object]:
    """@brief 构造 item CAS 参数 / Build item-CAS arguments.

    @param token item 原始租约 / Raw item lease.
    @param item 当前显式资源 / Current explicit resource.
    @return 可交给窄函数的参数 / Arguments for a narrow function.
    """

    return {
        "workspace_id": str(item.workspace_id),
        "resource_kind": item.resource_kind,
        "resource_id": item.resource_id,
        "item_lease_hash": _item_digest(token),
    }


def _erasure_item_from_row(row: Row[Any]) -> _ErasureItem:
    """@brief 防御性解析外部 item / Defensively parse an external item.

    @param row 窄函数 row / Narrow-function row.
    @return 强类型 item / Typed item.
    """

    values = tuple(row)
    if len(values) != 4:
        raise RuntimeError("account deletion item claim returned an unexpected row width")
    workspace_id, resource_kind, resource_id, attempt = values
    if not isinstance(workspace_id, str) or not workspace_id:
        raise RuntimeError("account deletion item returned an invalid Workspace ID")
    if resource_kind not in {"upload_object", "credential_scope"}:
        raise RuntimeError("account deletion item returned an invalid resource kind")
    if not isinstance(resource_id, str) or not resource_id:
        raise RuntimeError("account deletion item returned an invalid resource ID")
    if isinstance(attempt, bool) or not isinstance(attempt, int) or not 1 <= attempt <= 100:
        raise RuntimeError("account deletion item returned an invalid attempt")
    return _ErasureItem(WorkspaceId(workspace_id), resource_kind, resource_id, attempt)


def _external_state_from_row(row: Row[Any]) -> _ExternalState:
    """@brief 防御性解析外部状态 / Defensively parse external state.

    @param row 状态函数 row / State-function row.
    @return 强类型水位 / Typed watermark.
    """

    values = tuple(row)
    if len(values) != 3:
        raise RuntimeError("account deletion external state returned an unexpected row width")
    recipient, pending, failed = values
    if recipient is not None and (not isinstance(recipient, str) or not recipient):
        raise RuntimeError("account deletion external state returned an invalid recipient")
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 0
        for value in (pending, failed)
    ):
        raise RuntimeError("account deletion external state returned invalid counters")
    return _ExternalState(recipient, pending, failed)


def _claim_from_row(
    row: Row[Any],
    token: AccountDeletionClaimToken,
) -> AccountDeletionExecutionClaim:
    """@brief 防御性解析 claim row / Defensively parse a claim row.

    @param row SQLAlchemy row / SQLAlchemy row.
    @param token 当前批原始租约 / Raw lease for the current batch.
    @return 强类型 claim / Strongly typed claim.
    """

    values = tuple(row)
    if len(values) != 5:
        raise RuntimeError("account deletion claim function returned an unexpected row width")
    request_id, user_id, revision, claimed_at, lease_expires_at = values
    if not isinstance(request_id, str) or not request_id:
        raise RuntimeError("account deletion claim returned an invalid request ID")
    if not isinstance(user_id, str) or not user_id:
        raise RuntimeError("account deletion claim returned an invalid user ID")
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 2:
        raise RuntimeError("account deletion claim returned an invalid revision")
    if not isinstance(claimed_at, datetime) or not isinstance(lease_expires_at, datetime):
        raise RuntimeError("account deletion claim returned invalid lease timestamps")
    return AccountDeletionExecutionClaim(
        AccountDeletionId(request_id),
        UserId(user_id),
        token,
        revision,
        claimed_at,
        lease_expires_at,
    )


def _outcome_from_row(row: Row[Any]) -> AccountDeletionExecutionOutcome:
    """@brief 防御性解析擦除证据或失败 / Defensively parse erasure evidence or failure.

    @param row 窄函数返回行 / Narrow-function result row.
    @return 强类型 outcome / Strongly typed outcome.
    """

    values = tuple(row)
    if len(values) != 11:
        raise RuntimeError("account deletion erasure function returned an unexpected row width")
    evidence = values[:9]
    failure_code, failure_detail = values[9:]
    if failure_code is not None or failure_detail is not None:
        if not isinstance(failure_code, str) or not isinstance(failure_detail, str):
            raise RuntimeError("account deletion erasure returned an invalid failure")
        return AccountDeletionFailure(failure_code, failure_detail)
    if any(not isinstance(value, bool) for value in evidence):
        raise RuntimeError("account deletion erasure returned invalid evidence")
    return AccountDeletionErasureEvidence(*evidence)


def _claim_digest(token: AccountDeletionClaimToken) -> str:
    """@brief 生成用途分离租约摘要 / Produce a domain-separated lease digest.

    @param token 不可记录的租约 / Lease that must not be logged.
    @return 64 字符 SHA-256 hex / 64-character SHA-256 hex.
    """

    digest = hashlib.sha256(
        b"aiws:v2:account-deletion-claim\x00" + token.reveal_to_repository().encode("utf-8")
    ).hexdigest()
    if _SHA256_HEX.fullmatch(digest) is None:
        raise AssertionError("SHA-256 produced an invalid account deletion digest")
    return digest


def _item_digest(token: AccountDeletionClaimToken) -> str:
    """@brief 生成与 account claim 分离的 item 摘要 / Produce an item digest separated from the account claim.

    @param token 不可记录的 item 租约 / Item lease that must not be logged.
    @return 64 字符 SHA-256 hex / 64-character SHA-256 hex.
    """

    digest = hashlib.sha256(
        b"aiws:v2:account-deletion-item\x00" + token.reveal_to_repository().encode("utf-8")
    ).hexdigest()
    if _SHA256_HEX.fullmatch(digest) is None:
        raise AssertionError("SHA-256 produced an invalid account deletion item digest")
    return digest


def _validate_time_and_batch(now: datetime, batch_size: int) -> None:
    """@brief 校验 claim 输入 / Validate claim inputs.

    @param now 带时区时间 / Timezone-aware timestamp.
    @param batch_size 1..100 批量 / Batch between 1 and 100.
    @raise ValueError 输入非法时抛出 / Raised for invalid input.
    """

    _require_aware(now, "account deletion claim")
    if (
        isinstance(batch_size, bool)
        or not isinstance(batch_size, int)
        or not 1 <= batch_size <= 100
    ):
        raise ValueError("account deletion claim batch size must be between 1 and 100")


def _require_aware(value: datetime, label: str) -> None:
    """@brief 要求时间含 UTC offset / Require a timestamp with a UTC offset.

    @param value 待校验时间 / Timestamp to validate.
    @param label 错误标签 / Error label.
    @raise ValueError 无时区时抛出 / Raised for a naive timestamp.
    """

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} timestamp must be timezone-aware")


def _is_retryable_database_error(error: DBAPIError) -> bool:
    """@brief 只识别连接与事务级瞬时错误 / Recognize only connection- and transaction-level transient errors.

    @param error SQLAlchemy DBAPI error / SQLAlchemy DBAPI error.
    @return 租约可安全重试时为真 / True when lease retry is safe.
    """

    if error.connection_invalidated:
        return True
    sqlstate = getattr(error.orig, "sqlstate", None) or getattr(error.orig, "pgcode", None)
    return isinstance(sqlstate, str) and (
        sqlstate.startswith("08") or sqlstate in _RETRYABLE_SQLSTATES
    )


__all__ = ["PostgresAccountDeletionExecutionPort"]
