"""@brief 统一 transactional outbox 的租约调度端口 / Lease-based unified transactional-outbox ports.

本端口把全局 claim/finalize 权限收窄在一个 infrastructure adapter 中；具体领域
handler 只收到已提交事件的实际 actor/Workspace 与安全 payload，再用正常 RLS
（Row-Level Security）短事务处理。租约 token 默认脱敏，数据库只保存单向摘要。
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol, runtime_checkable

from backend.domain.platform import ApiEventId, JsonValue
from backend.domain.principals import UserId, WorkspaceId
from backend.domain.resources import ResourceRef

_EVENT_TYPE_PATTERN = re.compile(r"^[a-z][a-z0-9_.-]{2,127}$")
"""@brief 与统一 outbox 约束一致的事件名语法 / Event-name grammar matching the unified outbox constraint."""

_ERROR_CODE_PATTERN = re.compile(r"^[a-z][a-z0-9_.-]{2,100}$")
"""@brief 可持久重试错误码的窄语法 / Narrow grammar for persistable retry error codes."""


@dataclass(frozen=True, slots=True)
class OutboxLease:
    """@brief worker 持有而数据库仅存摘要的高熵租约 / High-entropy lease retained by the worker.

    @param token 不可记录的原始 token / Raw token that must never be logged.
    """

    token: str = field(repr=False)

    def __post_init__(self) -> None:
        """@brief 校验租约 token 的最小强度 / Validate the lease token's minimum strength."""
        if not 32 <= len(self.token) <= 512 or self.token.strip() != self.token:
            raise ValueError("outbox lease token must be a canonical high-entropy value")

    def reveal_to_repository(self) -> str:
        """@brief 仅向专用 repository 交付 token / Reveal the token only to the dedicated repository.

        @return 原始租约 token / Raw lease token.
        @note 返回值不得进入日志、遥测或业务 payload / The returned value must not
            enter logs, telemetry, or business payloads.
        """
        return self.token


@dataclass(frozen=True, slots=True)
class OutboxDispatchClaim:
    """@brief 一条可恢复的已提交事件 claim / One recoverable claim over a committed event.

    @param event_id 统一 ApiEvent 标识 / Unified ApiEvent identifier.
    @param workspace_id 事件 Workspace / Event Workspace.
    @param actor_id 事件提交时的真实 actor / Real actor captured at commit time.
    @param subject 事件 subject revision / Event subject revision.
    @param event_type 稳定事件类型 / Stable event type.
    @param payload 已持久、无 secret 的小型 payload / Persisted small secret-free payload.
    @param attempt_count 包含本次 claim 的尝试次数 / Attempt number including this claim.
    @param lease 当前 worker 独占租约 / Current worker's exclusive lease.
    @param lease_expires_at 数据库截断的租约终点 / Database-bounded lease deadline.
    """

    event_id: ApiEventId
    workspace_id: WorkspaceId
    actor_id: UserId
    subject: ResourceRef
    event_type: str
    payload: Mapping[str, JsonValue]
    attempt_count: int
    lease: OutboxLease = field(repr=False)
    lease_expires_at: datetime

    def __post_init__(self) -> None:
        """@brief 校验 claim 身份、有界 payload 与期限 / Validate claim identity, bounded payload, and deadline."""
        if not self.event_id or not self.workspace_id or not self.actor_id:
            raise ValueError("outbox claim requires event, workspace, and actor identifiers")
        if _EVENT_TYPE_PATTERN.fullmatch(self.event_type) is None:
            raise ValueError("outbox claim event type is invalid")
        if len(self.payload) > 40:
            raise ValueError("outbox claim payload exceeds the database envelope bound")
        if self.attempt_count < 1:
            raise ValueError("outbox claim attempt count must be positive")
        if self.lease_expires_at.tzinfo is None or self.lease_expires_at.utcoffset() is None:
            raise ValueError("outbox claim lease deadline must be timezone-aware")


class OutboxClaimRepository(Protocol):
    """@brief 狭权限的跨 Workspace claim/finalize 端口 / Narrow cross-Workspace claim/finalize port."""

    async def claim(
        self,
        *,
        lease: OutboxLease,
        now: datetime,
        lease_seconds: int,
        batch_size: int,
        maximum_attempts: int,
    ) -> tuple[OutboxDispatchClaim, ...]:
        """@brief 以 SKIP LOCKED 或等价语义 claim 一批到期事件 / Claim a due batch with SKIP-LOCKED semantics.

        @param lease 本批高熵租约 / High-entropy lease for this batch.
        @param now 应用候选时间，数据库不得信任未来值 / Application candidate time,
            bounded by database time.
        @param lease_seconds 租约秒数 / Lease duration in seconds.
        @param batch_size 有界批量 / Bounded batch size.
        @param maximum_attempts 进入 failed 前的硬上限 / Hard attempt cap before failure.
        @return 当前 worker 可处理的 claims / Claims owned by the current worker.
        """

    async def renew(
        self,
        claim: OutboxDispatchClaim,
        *,
        now: datetime,
        lease_seconds: int,
    ) -> bool:
        """@brief 仅当 token 仍匹配时延长租约 / Extend a lease only while its token still matches.

        @return 仍持有 claim 时为真 / True while the claim is still owned.
        """

    async def complete(
        self,
        claim: OutboxDispatchClaim,
        *,
        completed_at: datetime,
    ) -> bool:
        """@brief 以 event+token CAS 标记 published / Mark published using event-plus-token CAS.

        @return 完成生效时为真 / True when completion took effect.
        """

    async def retry(
        self,
        claim: OutboxDispatchClaim,
        *,
        error_code: str,
        retry_at: datetime,
        maximum_attempts: int,
    ) -> bool:
        """@brief 以 token CAS 安排重试或进入 failed / Schedule retry or enter failed using token CAS.

        @param error_code 不含异常正文的稳定 code / Stable code without exception text.
        @param retry_at 下次可 claim 时间 / Next eligible claim time.
        @param maximum_attempts 最大尝试次数 / Maximum attempts.
        @return 状态推进成功时为真 / True when the transition took effect.
        """


class OutboxEventHandler(Protocol):
    """@brief 一个稳定 event_type 的幂等处理器 / Idempotent handler for one stable event type."""

    async def handle(self, claim: OutboxDispatchClaim) -> None:
        """@brief 处理一条已提交事件 / Handle one committed event.

        @note handler 必须可至少一次重放，且外部副作用使用 event ID 或领域
            operation ID 去重 / Handlers tolerate at-least-once replay and deduplicate external
            effects by event or domain operation ID.
        """


@runtime_checkable
class OutboxExhaustionHandler(Protocol):
    """@brief 在 outbox 进入 failed 前闭合领域工作 / Close domain work before an outbox event becomes failed.

    @note hook 必须幂等，并在自身事务中把该事件拥有的非终态领域资源一起闭合；抛出
        任意异常都会阻止 dispatcher 把 outbox 标为 failed。/ The hook must be idempotent
        and close all non-terminal domain resources owned by the event in its own transaction;
        any exception prevents the dispatcher from marking the outbox event failed.
    """

    async def on_exhausted(
        self,
        claim: OutboxDispatchClaim,
        *,
        error_code: str,
    ) -> None:
        """@brief 补偿最后一次失败 / Compensate the final failed attempt.

        @param claim 仍由当前 worker 租用的最后一次 claim / Final claim still leased by the
            current worker.
        @param error_code 即将持久化的安全错误码 / Safe error code about to be persisted.
        """


class OutboxHandlerFailure(RuntimeError):
    """@brief handler 主动返回的公开安全错误码 / Public-safe error code raised deliberately by a handler."""

    code: str
    """@brief 可持久的稳定 code / Persistable stable code."""

    def __init__(self, code: str) -> None:
        """@brief 初始化已脱敏 handler 失败 / Initialize a redacted handler failure.

        @param code 稳定错误码 / Stable error code.
        """
        if _ERROR_CODE_PATTERN.fullmatch(code) is None:
            raise ValueError("outbox handler error code is invalid")
        super().__init__(code)
        self.code = code


__all__ = [
    "OutboxClaimRepository",
    "OutboxDispatchClaim",
    "OutboxEventHandler",
    "OutboxExhaustionHandler",
    "OutboxHandlerFailure",
    "OutboxLease",
]
