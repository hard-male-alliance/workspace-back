"""@brief 统一 outbox 租约的 PostgreSQL adapter / PostgreSQL adapter for unified outbox leases.

跨 Workspace 扫描只能通过 migration 安装的 owner-owned 窄函数；应用角色无权
直接修改 lease/status 列。函数返回事件提交时的真实
``resource_owner_id``，handler 再以该 actor 与 Workspace 安装正常 V2 RLS scope。
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from datetime import datetime
from typing import Any, cast

from sqlalchemy import text
from sqlalchemy.engine import Row

from backend.application.ports.outbox_dispatch import (
    OutboxDispatchClaim,
    OutboxLease,
)
from backend.domain.platform import ApiEventId, JsonValue
from backend.domain.principals import UserId, WorkspaceId
from backend.domain.resources import ResourceRef
from backend.infrastructure.persistence.database import AsyncDatabase

_SHA256_HEX = re.compile(r"^[a-f0-9]{64}$")
"""@brief 数据库租约摘要语法 / Database lease-digest grammar."""

_EVENT_TYPE = re.compile(r"^[a-z][a-z0-9_.-]{2,127}$")
"""@brief 统一事件类型语法 / Unified event-type grammar."""

_ERROR_CODE = re.compile(r"^[a-z][a-z0-9_.-]{2,100}$")
"""@brief 脱敏调度错误码语法 / Redacted dispatch-error grammar."""

_CLAIM_SQL = text(
    "SELECT event_id, workspace_id, actor_id, aggregate_type, aggregate_id, "
    "subject_revision, event_type, payload, attempt_count, lease_expires_at "
    "FROM agent.claim_outbox_events("
    ":lease_token_hash, :candidate_now, :lease_seconds, :batch_size, :maximum_attempts, "
    "CAST(:event_types AS text[]))"
)
"""@brief 调用窄 claim 函数 / Call the narrow claim function."""

_RENEW_SQL = text(
    "SELECT agent.renew_outbox_event_lease("
    ":event_id, :lease_token_hash, :candidate_now, :lease_seconds)"
)
"""@brief 调用租约续期函数 / Call the lease-renewal function."""

_COMPLETE_SQL = text(
    "SELECT agent.complete_outbox_event("
    ":event_id, :lease_token_hash, :completed_at)"
)
"""@brief 调用完成 CAS 函数 / Call the completion CAS function."""

_RETRY_SQL = text(
    "SELECT agent.retry_outbox_event("
    ":event_id, :lease_token_hash, :error_code, :retry_at, :maximum_attempts)"
)
"""@brief 调用 retry/fail CAS 函数 / Call the retry-or-fail CAS function."""


class PostgresOutboxClaimRepository:
    """@brief 只调用 owner-owned 窄函数的 outbox repository / Outbox repository calling owner-owned narrow functions."""

    def __init__(
        self,
        database: AsyncDatabase,
        *,
        event_types: frozenset[str],
    ) -> None:
        """@brief 绑定 lifespan-owned 数据库 / Bind the lifespan-owned database.

        @param database 共享异步数据库 / Shared asynchronous database.
        @param event_types 当前 consumer 独占的非空工作事件 allowlist / Non-empty work-event
            allowlist exclusively owned by this consumer.
        """
        self._database = database
        self._event_types = _event_type_allowlist(event_types)

    async def claim(
        self,
        *,
        lease: OutboxLease,
        now: datetime,
        lease_seconds: int,
        batch_size: int,
        maximum_attempts: int,
    ) -> tuple[OutboxDispatchClaim, ...]:
        """@brief 原子 claim 到期或租约过期事件 / Atomically claim due or lease-expired events."""
        _validate_inputs(now, lease_seconds, batch_size, maximum_attempts)
        digest = _lease_digest(lease)
        async with self._database.unscoped_transaction() as session:
            result = await session.execute(
                _CLAIM_SQL,
                {
                    "lease_token_hash": digest,
                    "candidate_now": now,
                    "lease_seconds": lease_seconds,
                    "batch_size": batch_size,
                    "maximum_attempts": maximum_attempts,
                    "event_types": list(self._event_types),
                },
            )
            rows = result.all()
        claims = tuple(_claim_from_row(row, lease) for row in rows)
        if len(claims) > batch_size:
            raise RuntimeError("outbox claim function exceeded its requested batch size")
        return claims

    async def renew(
        self,
        claim: OutboxDispatchClaim,
        *,
        now: datetime,
        lease_seconds: int,
    ) -> bool:
        """@brief 以 event+token CAS 续租 / Renew by event-plus-token CAS."""
        _validate_inputs(now, lease_seconds, 1, max(claim.attempt_count, 1))
        async with self._database.unscoped_transaction() as session:
            value = await session.scalar(
                _RENEW_SQL,
                {
                    "event_id": str(claim.event_id),
                    "lease_token_hash": _lease_digest(claim.lease),
                    "candidate_now": now,
                    "lease_seconds": lease_seconds,
                },
            )
        return _database_boolean(value, "renew")

    async def complete(
        self,
        claim: OutboxDispatchClaim,
        *,
        completed_at: datetime,
    ) -> bool:
        """@brief 以 event+token CAS 完成事件 / Complete an event by event-plus-token CAS."""
        _require_aware(completed_at, "outbox completion")
        async with self._database.unscoped_transaction() as session:
            value = await session.scalar(
                _COMPLETE_SQL,
                {
                    "event_id": str(claim.event_id),
                    "lease_token_hash": _lease_digest(claim.lease),
                    "completed_at": completed_at,
                },
            )
        return _database_boolean(value, "complete")

    async def retry(
        self,
        claim: OutboxDispatchClaim,
        *,
        error_code: str,
        retry_at: datetime,
        maximum_attempts: int,
    ) -> bool:
        """@brief 以 event+token CAS 安排重试或终结 / Schedule retry or terminal failure by CAS."""
        _require_aware(retry_at, "outbox retry")
        if _ERROR_CODE.fullmatch(error_code) is None:
            raise ValueError("outbox retry error code is invalid")
        if not 1 <= maximum_attempts <= 100:
            raise ValueError("outbox maximum attempts must be between 1 and 100")
        async with self._database.unscoped_transaction() as session:
            value = await session.scalar(
                _RETRY_SQL,
                {
                    "event_id": str(claim.event_id),
                    "lease_token_hash": _lease_digest(claim.lease),
                    "error_code": error_code,
                    "retry_at": retry_at,
                    "maximum_attempts": maximum_attempts,
                },
            )
        return _database_boolean(value, "retry")


def _claim_from_row(row: Row[Any], lease: OutboxLease) -> OutboxDispatchClaim:
    """@brief 防御性解析窄函数返回行 / Defensively parse a narrow-function result row.

    @param row SQLAlchemy 行 / SQLAlchemy row.
    @param lease 本批原始租约 / Raw lease for this batch.
    @return 强类型 claim / Strongly typed claim.
    """
    values: tuple[object, ...] = tuple(row)
    if len(values) != 10:
        raise RuntimeError("outbox claim function returned an unexpected row width")
    (
        event_id,
        workspace_id,
        actor_id,
        aggregate_type,
        aggregate_id,
        subject_revision,
        event_type,
        payload,
        attempt_count,
        lease_expires_at,
    ) = values
    strings = (event_id, workspace_id, actor_id, aggregate_type, aggregate_id, event_type)
    if any(not isinstance(value, str) or not value for value in strings):
        raise RuntimeError("outbox claim function returned invalid identifiers")
    if not isinstance(event_type, str) or _EVENT_TYPE.fullmatch(event_type) is None:
        raise RuntimeError("outbox claim function returned an invalid event type")
    if subject_revision is not None and (
        isinstance(subject_revision, bool)
        or not isinstance(subject_revision, int)
        or subject_revision < 1
    ):
        raise RuntimeError("outbox claim function returned an invalid subject revision")
    if not isinstance(payload, Mapping) or any(not isinstance(key, str) for key in payload):
        raise RuntimeError("outbox claim function returned an invalid payload")
    if isinstance(attempt_count, bool) or not isinstance(attempt_count, int):
        raise RuntimeError("outbox claim function returned an invalid attempt count")
    if not isinstance(lease_expires_at, datetime):
        raise RuntimeError("outbox claim function returned an invalid lease deadline")
    json_payload = cast(Mapping[str, JsonValue], dict(payload))
    return OutboxDispatchClaim(
        ApiEventId(cast(str, event_id)),
        WorkspaceId(cast(str, workspace_id)),
        UserId(cast(str, actor_id)),
        ResourceRef(
            cast(str, aggregate_type),
            cast(str, aggregate_id),
            subject_revision,
        ),
        event_type,
        json_payload,
        attempt_count,
        lease,
        lease_expires_at,
    )


def _lease_digest(lease: OutboxLease) -> str:
    """@brief 以用途分离 SHA-256 摘要租约 / Digest a lease with domain-separated SHA-256.

    @param lease 原始高熵租约 / Raw high-entropy lease.
    @return 数据库可存储摘要 / Database-safe digest.
    """
    digest = hashlib.sha256(
        b"aiws:v2:outbox-lease\x00" + lease.reveal_to_repository().encode("utf-8")
    ).hexdigest()
    if _SHA256_HEX.fullmatch(digest) is None:
        raise AssertionError("SHA-256 produced an invalid digest")
    return digest


def _validate_inputs(
    now: datetime,
    lease_seconds: int,
    batch_size: int,
    maximum_attempts: int,
) -> None:
    """@brief 校验与数据库函数一致的边界 / Validate bounds shared with database functions."""
    _require_aware(now, "outbox operation")
    values = (lease_seconds, batch_size, maximum_attempts)
    if any(isinstance(value, bool) or not isinstance(value, int) for value in values):
        raise ValueError("outbox repository bounds must be integers")
    if not 5 <= lease_seconds <= 900:
        raise ValueError("outbox lease must be between 5 and 900 seconds")
    if not 1 <= batch_size <= 100:
        raise ValueError("outbox batch size must be between 1 and 100")
    if not 1 <= maximum_attempts <= 100:
        raise ValueError("outbox attempts must be between 1 and 100")


def _event_type_allowlist(
    values: frozenset[str],
) -> tuple[str, ...]:
    """@brief 冻结有界 event-type 消费归属 / Freeze a bounded event-type ownership allowlist.

    @param values consumer 独占的事件名集合 / Event names exclusively owned by one consumer.
    @return 稳定排序 tuple / Stable sorted tuple.
    @raise ValueError 空集、过多事件或非法事件名时抛出 / Raised for an empty, oversized,
        or malformed allowlist.
    """
    if not values or len(values) > 32:
        raise ValueError("outbox event-type allowlist must contain between 1 and 32 entries")
    if any(_EVENT_TYPE.fullmatch(value) is None for value in values):
        raise ValueError("outbox event-type allowlist contains an invalid event type")
    return tuple(sorted(values))


def _require_aware(value: datetime, label: str) -> None:
    """@brief 要求带时区时间 / Require a timezone-aware timestamp."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} timestamp must be timezone-aware")


def _database_boolean(value: object, operation: str) -> bool:
    """@brief 要求数据库 CAS 返回 boolean / Require a boolean database CAS result."""
    if not isinstance(value, bool):
        raise RuntimeError(f"outbox {operation} function returned a non-boolean result")
    return value


__all__ = ["PostgresOutboxClaimRepository"]
