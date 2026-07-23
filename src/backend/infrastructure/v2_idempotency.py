"""@brief API V2 原子幂等 claim、业务与 receipt / API V2 atomic idempotency infrastructure.

PostgreSQL adapter 故意不复用 V1 ``ActorScope`` 或 ``idempotency_records``：V2 scope
由签名 ``user_id``、可空 Workspace、method、canonical path 与 key 组成，不含历史
``resource_owner_id``。生产 executor 把 claim、领域 UoW、transactional outbox 与逐字
receipt 放入同一 PostgreSQL 事务；历史或三阶段 adapter 遗留的 pending 仍永不被另一
worker 超时接管，必须由运维证据闭环处理。
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import secrets
import uuid
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    LargeBinary,
    MetaData,
    SmallInteger,
    String,
    Table,
    UniqueConstraint,
    delete,
    select,
    text,
    update,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from backend.application.ports.v2_idempotency import (
    IdempotencyClaim,
    IdempotencyConflict,
    IdempotencyDecision,
    IdempotencyDecisionKind,
    IdempotencyPreparationId,
    IdempotencyRequest,
    IdempotencyScope,
    IdempotencyStatus,
    IdempotentCommit,
    IdempotentOperation,
    IdempotentPrepare,
    ReplayableResponse,
)
from backend.infrastructure.persistence.database import AsyncDatabase

_MINIMUM_RETENTION = timedelta(hours=24)
"""@brief 契约要求的普通请求最短保留期 / Contract minimum retention for ordinary requests."""

_DEFAULT_RETRY_AFTER_SECONDS = 1
"""@brief pending 冲突建议的默认 Retry-After / Default Retry-After for a pending conflict."""

_metadata = MetaData()
"""@brief V2 幂等表的独立 SQLAlchemy Core metadata / Standalone Core metadata for the V2 table."""

api_v2_idempotency_records = Table(
    "api_v2_idempotency_records",
    _metadata,
    Column("id", String(128), primary_key=True),
    Column(
        "user_id",
        String(128),
        ForeignKey("identity.users.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "workspace_id",
        String(128),
        ForeignKey("identity.workspaces.id", ondelete="CASCADE"),
    ),
    Column("method", String(16), nullable=False),
    Column("canonical_path", String(512), nullable=False),
    Column("idempotency_key", String(128), nullable=False),
    Column("request_fingerprint", String(64), nullable=False),
    Column("status", String(16), nullable=False),
    Column("claim_token_hash", String(64)),
    Column("response_status", SmallInteger),
    Column("response_headers", JSONB(none_as_null=True)),
    Column("response_body", LargeBinary),
    Column("expires_at", DateTime(timezone=True), nullable=False),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
    Column(
        "updated_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
    UniqueConstraint(
        "user_id",
        "workspace_id",
        "method",
        "canonical_path",
        "idempotency_key",
        name="uq_api_v2_idempotency_scope",
        postgresql_nulls_not_distinct=True,
    ),
    CheckConstraint("method ~ '^[A-Z]+$'", name="ck_api_v2_idempotency_method"),
    CheckConstraint(
        "left(canonical_path, 1) = '/' "
        "AND position('?' in canonical_path) = 0 "
        "AND position('#' in canonical_path) = 0",
        name="ck_api_v2_idempotency_canonical_path",
    ),
    CheckConstraint(
        "idempotency_key ~ '^[A-Za-z0-9._~-]{16,128}$'",
        name="ck_api_v2_idempotency_key",
    ),
    CheckConstraint(
        "request_fingerprint ~ '^[0-9a-f]{64}$'",
        name="ck_api_v2_idempotency_fingerprint",
    ),
    CheckConstraint(
        "expires_at >= created_at + interval '24 hours'",
        name="ck_api_v2_idempotency_retention",
    ),
    CheckConstraint(
        "(status = 'pending' "
        "AND claim_token_hash IS NOT NULL "
        "AND claim_token_hash ~ '^[0-9a-f]{64}$' "
        "AND response_status IS NULL "
        "AND response_headers IS NULL "
        "AND response_body IS NULL) "
        "OR (status = 'completed' "
        "AND claim_token_hash IS NULL "
        "AND response_status IS NOT NULL "
        "AND response_status BETWEEN 100 AND 599 "
        "AND response_headers IS NOT NULL "
        "AND jsonb_typeof(response_headers) = 'array' "
        "AND response_body IS NOT NULL)",
        name="ck_api_v2_idempotency_state",
    ),
    schema="identity",
)
"""@brief 与 0014 migration 同构的 API V2 幂等表 / API V2 table matching migration 0014."""

Index(
    "ix_api_v2_idempotency_completed_expiry",
    api_v2_idempotency_records.c.expires_at,
    postgresql_where=text("status = 'completed'"),
)
"""@brief completed receipt 到期清理索引 / Completed-receipt expiry cleanup index."""

Index(
    "ix_api_v2_idempotency_pending_expiry",
    api_v2_idempotency_records.c.expires_at,
    api_v2_idempotency_records.c.id,
    postgresql_where=text("status = 'pending'"),
)
"""@brief stranded pending receipt 观测索引 / Stranded-pending receipt observation index."""


@dataclass(slots=True)
class _MemoryRecord:
    """@brief 内存 adapter 的一条可变记录 / One mutable in-memory adapter record.

    @param fingerprint 首次请求指纹 / First-request fingerprint.
    @param status pending 或 completed / Pending or completed state.
    @param claim_token 首次执行者的高熵令牌 / High-entropy first-executor token.
    @param response 完成后的原始响应快照 / Original response snapshot after completion.
    @param expires_at completed receipt 的保留边界 / Completed-receipt retention boundary.
    """

    fingerprint: str
    status: IdempotencyStatus
    claim_token: str | None
    response: ReplayableResponse | None
    expires_at: datetime


class InMemoryV2IdempotencyStore:
    """@brief 并发安全的进程内 V2 幂等 store / Concurrency-safe in-process V2 store.

    @note 单锁只保护极短的 claim/complete 临界区；业务 callback 总在锁外执行。多进程
        部署必须使用 PostgreSQL adapter。
    """

    def __init__(self) -> None:
        """@brief 初始化空 store / Initialize an empty store."""
        self._lock = asyncio.Lock()
        self._records: dict[IdempotencyScope, _MemoryRecord] = {}

    async def claim(
        self,
        request: IdempotencyRequest,
        *,
        now: datetime,
        expires_at: datetime,
    ) -> IdempotencyDecision:
        """@brief 原子 claim、冲突或 replay / Atomically claim, conflict, or replay.

        @param request 完整作用域请求 / Fully scoped request.
        @param now 带时区当前时刻 / Timezone-aware current instant.
        @param expires_at receipt 初始保留边界 / Initial receipt retention boundary.
        @return claim、replay 或 in-progress decision / Claim, replay, or in-progress decision.
        @raise IdempotencyConflict 同 key 不同指纹时抛出 / Raised for key reuse with a
            different fingerprint.
        """
        _validate_time_range(now, expires_at)
        fingerprint = request.fingerprint
        async with self._lock:
            record = self._records.get(request.scope)
            if record is not None:
                if record.status is IdempotencyStatus.COMPLETED and record.expires_at <= now:
                    del self._records[request.scope]
                    record = None
            if record is not None:
                if not hmac.compare_digest(record.fingerprint, fingerprint):
                    raise IdempotencyConflict("idempotency.key_reused")
                if record.status is IdempotencyStatus.PENDING:
                    return IdempotencyDecision(IdempotencyDecisionKind.IN_PROGRESS)
                if record.response is None:
                    raise RuntimeError("completed in-memory idempotency record has no response")
                return IdempotencyDecision(
                    IdempotencyDecisionKind.REPLAY,
                    replay=record.response,
                )

            token = secrets.token_urlsafe(32)
            self._records[request.scope] = _MemoryRecord(
                fingerprint=fingerprint,
                status=IdempotencyStatus.PENDING,
                claim_token=token,
                response=None,
                expires_at=expires_at,
            )
            return IdempotencyDecision(
                IdempotencyDecisionKind.CLAIMED,
                claim=IdempotencyClaim(request.scope, fingerprint, token),
            )

    async def complete(
        self,
        claim: IdempotencyClaim,
        response: ReplayableResponse,
        *,
        completed_at: datetime,
        expires_at: datetime,
    ) -> ReplayableResponse | None:
        """@brief 原子保存完整响应 receipt / Atomically store a complete response receipt.

        @param claim 首次请求的私有 claim / First request's private claim.
        @param response 原始响应快照 / Original response snapshot.
        @param completed_at 完成时刻 / Completion instant.
        @param expires_at 从完成计算的保留边界 / Retention boundary calculated from completion.
        @return 保存的响应；claim 不再属于 caller 时返回 ``None`` / Stored response, or
            ``None`` if the caller no longer owns the claim.
        """
        _validate_time_range(completed_at, expires_at)
        async with self._lock:
            record = self._records.get(claim.scope)
            if record is None or not hmac.compare_digest(record.fingerprint, claim.fingerprint):
                return None
            if record.status is IdempotencyStatus.COMPLETED:
                return record.response
            if record.claim_token is None or not hmac.compare_digest(
                record.claim_token, claim.token
            ):
                return None
            record.status = IdempotencyStatus.COMPLETED
            record.claim_token = None
            record.response = response
            record.expires_at = expires_at
            return response


class InMemoryIdempotencyExecutor:
    """@brief 开发环境的进程内幂等 executor / Development in-memory idempotency executor.

    @param store 与进程同生命周期的内存 store / Process-lifetime in-memory store.
    @param retention completed receipt 保留期，普通命令至少 24h；Resume 可传 30d / Completed
        receipt retention, at least 24h for normal commands and configurable to 30d for Resume.
    @param clock 可测试的带时区时钟 / Testable timezone-aware clock.
    @param retry_after_seconds pending 冲突建议秒数 / Suggested seconds for pending conflicts.

    @note 仅供 memory 模式与纯语义测试；进程退出时业务内存和 receipt 一起消失。持久化
        部署必须使用 ``AtomicPostgresIdempotencyExecutor``，禁止把该三阶段执行器套在
        PostgreSQL store 上。
        / This is only for memory mode and semantic tests. Durable deployments must use the atomic
        PostgreSQL executor, never this three-phase executor with a PostgreSQL store.
    """

    def __init__(
        self,
        store: InMemoryV2IdempotencyStore,
        *,
        retention: timedelta = _MINIMUM_RETENTION,
        clock: Callable[[], datetime] | None = None,
        retry_after_seconds: int = _DEFAULT_RETRY_AFTER_SECONDS,
    ) -> None:
        """@brief 配置 executor / Configure the executor.

        @param store V2 store adapter / V2 store adapter.
        @param retention completed receipt 保留期 / Completed-receipt retention.
        @param clock 返回带时区时间的同步 callable / Synchronous timezone-aware clock.
        @param retry_after_seconds pending 冲突建议秒数 / Suggested pending retry delay.
        @raise ValueError 保留期短于契约或重试秒数无效时抛出 / Raised for insufficient
            retention or invalid retry delay.
        """
        if retention < _MINIMUM_RETENTION:
            raise ValueError("API V2 idempotency retention must be at least 24 hours")
        if retry_after_seconds < 1:
            raise ValueError("idempotency Retry-After must be positive")
        self._store = store
        self._retention = retention
        self._clock = clock or _utc_now
        self._retry_after_seconds = retry_after_seconds

    async def execute(
        self,
        request: IdempotencyRequest,
        operation: IdempotentOperation,
    ) -> ReplayableResponse:
        """@brief 执行首次 callback 或逐字 replay / Execute the first callback or replay bytes.

        @param request 规范请求 / Canonical request.
        @param operation 仅 claim winner 执行的业务 callback / Business callback run only by
            the claim winner.
        @return 首次或 replay 响应 / First or replayed response.
        @raise IdempotencyConflict 指纹冲突或已有 pending 时抛出 / Raised on a fingerprint
            conflict or an existing pending claim.

        @note callback 的任何 ``BaseException``（包括取消）都会原样传播，且 pending
            不删除、不接管。这条行为是业务事务原子性缺口的安全保险。
        """
        now = self._clock()
        expires_at = now + self._retention
        decision = await self._store.claim(request, now=now, expires_at=expires_at)
        if decision.kind is IdempotencyDecisionKind.REPLAY:
            if decision.replay is None:
                raise RuntimeError("replay decision has no response")
            return decision.replay
        if decision.kind is IdempotencyDecisionKind.IN_PROGRESS:
            raise IdempotencyConflict(
                "idempotency.in_progress",
                retry_after_seconds=self._retry_after_seconds,
            )
        if decision.claim is None:
            raise RuntimeError("claimed decision has no claim")

        response = await operation()
        if not isinstance(response, ReplayableResponse):
            raise TypeError("idempotent operation must return ReplayableResponse")
        completed_at = self._clock()
        completed = await self._store.complete(
            decision.claim,
            response,
            completed_at=completed_at,
            expires_at=completed_at + self._retention,
        )
        if completed is None:
            raise IdempotencyConflict(
                "idempotency.in_progress",
                retry_after_seconds=self._retry_after_seconds,
            )
        return completed

    async def execute_prepared[PreparedT](
        self,
        request: IdempotencyRequest,
        prepare: IdempotentPrepare[PreparedT],
        commit: IdempotentCommit[PreparedT],
    ) -> ReplayableResponse:
        """@brief 以稳定 preparation ID 执行开发环境分相命令 / Execute a prepared command with a stable ID in development.

        @param request 规范请求 / Canonical request.
        @param prepare 事务外准备 callback / External preparation callback.
        @param commit 数据库提交 callback / Database commit callback.
        @return 首次或重放 response / First or replayed response.

        @note 内存 adapter 没有数据库事务；此实现保持与生产相同的 callback 顺序与稳定
            preparation ID，进程退出时所有状态一起丢失 / The memory adapter has no database
            transaction but preserves callback ordering and the stable preparation ID.
        """

        async def operation() -> ReplayableResponse:
            prepared = await prepare(_preparation_id(request))
            return await commit(prepared)

        return await self.execute(request, operation)


class AtomicPostgresIdempotencyExecutor:
    """@brief 业务与 receipt 同事务的 PostgreSQL executor / PostgreSQL atomic executor.

    @param database composition root 管理的数据库 / Database managed by the composition root.
    @param retention completed receipt 保留期 / Completed-receipt retention.
    @param clock 可测试的带时区时钟 / Testable timezone-aware clock.
    @param retry_after_seconds 活动相同 key 的建议等待秒数 / Suggested delay for an active key.

    @note 外层 connection transaction 覆盖 claim、领域 UoW、transactional outbox 与 receipt。
        领域 UoW 通过 ``AsyncDatabase.new_session`` 的 ``create_savepoint`` 模式加入；进程在
        任一点失败都会整体回滚，重试不会面对“业务成功但永久 pending”的不确定状态。
        / One connection transaction covers the claim, domain UoW, transactional outbox, and
        receipt. Domain UoWs join it with SAVEPOINTs; failure at any point rolls everything back.
    """

    def __init__(
        self,
        database: AsyncDatabase,
        *,
        retention: timedelta = _MINIMUM_RETENTION,
        clock: Callable[[], datetime] | None = None,
        retry_after_seconds: int = _DEFAULT_RETRY_AFTER_SECONDS,
    ) -> None:
        """@brief 配置原子 executor / Configure the atomic executor.

        @param database 共享 PostgreSQL 资源 / Shared PostgreSQL resource.
        @param retention receipt 保留期 / Receipt retention.
        @param clock 返回带时区时间的 callable / Timezone-aware clock callable.
        @param retry_after_seconds advisory lock 冲突建议秒数 / Advisory-lock retry delay.
        @raise ValueError 配置违反契约下限时抛出 / Raised when configuration violates the
            contract minimum.
        """
        if retention < _MINIMUM_RETENTION:
            raise ValueError("API V2 idempotency retention must be at least 24 hours")
        if retry_after_seconds < 1:
            raise ValueError("idempotency Retry-After must be positive")
        self._database = database
        self._store = PostgresV2IdempotencyStore(database)
        self._retention = retention
        self._clock = clock or _utc_now
        self._retry_after_seconds = retry_after_seconds

    async def execute(
        self,
        request: IdempotencyRequest,
        operation: IdempotentOperation,
    ) -> ReplayableResponse:
        """@brief 原子执行首次请求或逐字 replay / Atomically execute or byte-replay a request.

        @param request 规范化幂等请求 / Canonical idempotency request.
        @param operation 仅首次 claim 执行的领域命令 / Domain command executed only by the
            first claimant.
        @return 首次提交或已持久化响应 / Newly committed or persisted response.
        @raise IdempotencyConflict key 冲突或同 key 正在执行时抛出 / Raised for key reuse or
            an operation already in progress.
        """
        async with self._database.atomic_envelope():
            return await self._execute_in_current_envelope(request, operation)

    async def execute_prepared[PreparedT](
        self,
        request: IdempotencyRequest,
        prepare: IdempotentPrepare[PreparedT],
        commit: IdempotentCommit[PreparedT],
    ) -> ReplayableResponse:
        """@brief 无长事务地准备外部 I/O，再原子提交业务与 receipt / Prepare external I/O without a long transaction, then commit atomically.

        @param request 规范请求 / Canonical request.
        @param prepare 使用稳定 preparation ID 的外部准备 / External preparation using a stable ID.
        @param commit 不得发起网络 I/O 的领域提交 / Domain commit that must not perform network I/O.
        @return 首次提交或已持久化 response / Newly committed or persisted response.
        @raise IdempotencyConflict key 冲突或同 scope 正在执行时抛出 / Raised for key reuse or
            an operation in progress.

        @note 整个协调窗口只持有 PostgreSQL session advisory lock；preflight 与最终提交是
            两个短事务。``preparation_id`` 跨进程崩溃重试稳定，外部 adapter 必须用它实现
            provider-side idempotency / Only a PostgreSQL session advisory lock spans the coordination
            window; preflight and final commit are short transactions. External adapters must use the
            crash-stable ``preparation_id`` for provider-side idempotency.
        """

        async with self._database.coordination_lock(_scope_lock_key(request.scope)) as connection:
            if connection is None:
                raise IdempotencyConflict(
                    "idempotency.in_progress",
                    retry_after_seconds=self._retry_after_seconds,
                )
            decision = await self._store._preflight_on_connection(
                connection,
                request,
                now=self._clock(),
            )
            if decision is not None:
                if decision.kind is IdempotencyDecisionKind.REPLAY:
                    if decision.replay is None:
                        raise RuntimeError("replay decision has no response")
                    return decision.replay
                if decision.kind is IdempotencyDecisionKind.IN_PROGRESS:
                    raise IdempotencyConflict(
                        "idempotency.in_progress",
                        retry_after_seconds=self._retry_after_seconds,
                    )
                raise RuntimeError("prepared preflight cannot create a claim")
            prepared = await prepare(_preparation_id(request))
            async with self._database.atomic_envelope(connection=connection):
                return await self._execute_in_current_envelope(
                    request,
                    lambda: commit(prepared),
                )

    async def _execute_in_current_envelope(
        self,
        request: IdempotencyRequest,
        operation: IdempotentOperation,
    ) -> ReplayableResponse:
        """@brief 在已绑定原子信封内执行 claim、业务与 receipt / Run claim, business, and receipt in a bound atomic envelope.

        @param request 规范请求 / Canonical request.
        @param operation 只在 claim winner 执行的 callback / Callback run only by the claim winner.
        @return 首次提交或 replay / First commit or replay.
        """

        now = self._clock()
        expires_at = now + self._retention
        async with self._database.new_session() as session:
            async with session.begin():
                await self._install_scope(session, request.scope)
                decision = await self._store._claim_in_session(
                    session,
                    request,
                    now=now,
                    expires_at=expires_at,
                )
                if decision.kind is IdempotencyDecisionKind.REPLAY:
                    if decision.replay is None:
                        raise RuntimeError("replay decision has no response")
                    return decision.replay
                if decision.kind is IdempotencyDecisionKind.IN_PROGRESS:
                    raise IdempotencyConflict(
                        "idempotency.in_progress",
                        retry_after_seconds=self._retry_after_seconds,
                    )
                if decision.claim is None:
                    raise RuntimeError("claimed decision has no claim")
                response = await operation()
                if not isinstance(response, ReplayableResponse):
                    raise TypeError("idempotent operation must return ReplayableResponse")
                # Domain repositories may narrow app.workspace_id. Restore immutable HTTP scope.
                await self._install_scope(session, request.scope)
                completed_at = self._clock()
                completed = await self._store._complete_in_session(
                    session,
                    decision.claim,
                    response,
                    completed_at=completed_at,
                    expires_at=completed_at + self._retention,
                )
                if completed is None:
                    raise IdempotencyConflict(
                        "idempotency.in_progress",
                        retry_after_seconds=self._retry_after_seconds,
                    )
                return completed

    async def _install_scope(self, session: AsyncSession, scope: IdempotencyScope) -> None:
        """@brief 在当前短事务恢复不可变 HTTP RLS scope / Restore the immutable HTTP RLS scope.

        @param session 当前 session / Current session.
        @param scope 完整 HTTP scope / Complete HTTP scope.
        @return 无返回值 / No return value.
        """

        await self._database.install_v2_request_scope(
            session,
            actor_id=str(scope.user_id),
            workspace_id=None if scope.workspace_id is None else str(scope.workspace_id),
        )


class PostgresV2IdempotencyStore:
    """@brief 跨进程 PostgreSQL V2 幂等 store / Cross-process PostgreSQL V2 store.

    @param database composition root 管理的数据库 / Database managed by the composition root.

    @note 该 adapter 直接设置 V2 ``app.actor_id`` 与可空 ``app.workspace_id`` GUC，绝不
        构造历史 ``ActorScope``、``resource_owner_id``，也不机会式创建身份记录。
    """

    def __init__(self, database: AsyncDatabase) -> None:
        """@brief 绑定数据库资源 / Bind the database resource.

        @param database 应用生命周期数据库 / Application-lifetime database.
        """
        self._database = database

    async def claim(
        self,
        request: IdempotencyRequest,
        *,
        now: datetime,
        expires_at: datetime,
    ) -> IdempotencyDecision:
        """@brief 原子 claim、冲突或 replay / Atomically claim, conflict, or replay.

        @param request 完整作用域请求 / Fully scoped request.
        @param now 带时区当前时刻 / Timezone-aware current instant.
        @param expires_at receipt 初始保留边界 / Initial receipt retention boundary.
        @return claim、replay 或 in-progress / Claim, replay, or in-progress.
        """
        async with self._transaction(request.scope) as session:
            return await self._claim_in_session(
                session,
                request,
                now=now,
                expires_at=expires_at,
            )

    async def complete(
        self,
        claim: IdempotencyClaim,
        response: ReplayableResponse,
        *,
        completed_at: datetime,
        expires_at: datetime,
    ) -> ReplayableResponse | None:
        """@brief 原子保存逐字响应 receipt / Atomically persist a byte-exact response receipt.

        @param claim 首次请求的私有 claim / First request's private claim.
        @param response 原始响应快照 / Original response snapshot.
        @param completed_at 完成时刻 / Completion instant.
        @param expires_at 从完成计算的保留边界 / Retention boundary from completion.
        @return 保存/已保存响应，或 claim 所有权丢失时 ``None`` / Stored/already stored
            response, or ``None`` after ownership loss.
        """
        async with self._transaction(claim.scope) as session:
            return await self._complete_in_session(
                session,
                claim,
                response,
                completed_at=completed_at,
                expires_at=expires_at,
            )

    async def _claim_in_session(
        self,
        session: AsyncSession,
        request: IdempotencyRequest,
        *,
        now: datetime,
        expires_at: datetime,
    ) -> IdempotencyDecision:
        """@brief 在 caller 事务内 claim 或 replay / Claim or replay in the caller transaction.

        @param session 已安装相同 V2 RLS scope 的 Session / Session with matching V2 RLS scope.
        @param request 完整幂等请求 / Complete idempotency request.
        @param now 当前时刻 / Current instant.
        @param expires_at 初始保留边界 / Initial retention boundary.
        @return claim、replay 或 in-progress decision / Claim, replay, or in-progress decision.
        """
        _validate_time_range(now, expires_at)
        if not await self._try_scope_lock(session, request.scope):
            return IdempotencyDecision(IdempotencyDecisionKind.IN_PROGRESS)
        fingerprint = request.fingerprint
        row = await self._find_locked(session, request.scope)
        if row is not None:
            decision = self._decision_from_existing(row, fingerprint, now)
            if decision is not None:
                return decision
            await session.execute(
                delete(api_v2_idempotency_records).where(
                    api_v2_idempotency_records.c.id == str(row["id"])
                )
            )

        claim = self._new_claim(request.scope, fingerprint)
        inserted = await session.execute(
            postgresql_insert(api_v2_idempotency_records)
            .values(
                id=f"idemv2_{uuid.uuid4().hex}",
                user_id=str(request.scope.user_id),
                workspace_id=(
                    None if request.scope.workspace_id is None else str(request.scope.workspace_id)
                ),
                method=request.scope.method,
                canonical_path=request.scope.canonical_path,
                idempotency_key=request.scope.key,
                request_fingerprint=fingerprint,
                status=IdempotencyStatus.PENDING.value,
                claim_token_hash=_token_hash(claim.token),
                response_status=None,
                response_headers=None,
                response_body=None,
                expires_at=expires_at,
                created_at=now,
                updated_at=now,
            )
            .on_conflict_do_nothing(constraint="uq_api_v2_idempotency_scope")
            .returning(api_v2_idempotency_records.c.id)
        )
        if inserted.scalar_one_or_none() is not None:
            return IdempotencyDecision(IdempotencyDecisionKind.CLAIMED, claim=claim)

        concurrent = await self._find_locked(session, request.scope)
        if concurrent is None:
            raise RuntimeError("concurrent idempotency claim disappeared")
        decision = self._decision_from_existing(concurrent, fingerprint, now)
        if decision is None:
            raise RuntimeError("concurrent completed idempotency receipt expired")
        return decision

    async def _preflight_on_connection(
        self,
        connection: AsyncConnection,
        request: IdempotencyRequest,
        *,
        now: datetime,
    ) -> IdempotencyDecision | None:
        """@brief 在外部准备前用短事务读取 replay 或冲突 / Read replay or conflict in a short transaction before preparation.

        @param connection 已持有相同 scope session lock 的独占连接 / Exclusive connection
            holding the same scope's session lock.
        @param request 完整幂等请求 / Complete idempotency request.
        @param now 当前时刻 / Current instant.
        @return 已存在的 replay/in-progress；首次或到期时为空 / Existing replay or in-progress
            decision; ``None`` for a first or expired request.
        @raise IdempotencyConflict 同 key 不同指纹时抛出 / Raised for key reuse with a different
            fingerprint.

        @note 本方法绝不创建 pending；外部准备失败或进程崩溃不会留下永久占位。最终
            claim、领域提交与 receipt 仍在一个原子信封内 / This method never creates pending
            state. The final claim, domain commit, and receipt remain in one atomic envelope.
        """

        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("idempotency preflight time must be timezone-aware")
        async with AsyncSession(
            bind=connection,
            expire_on_commit=False,
            autoflush=False,
        ) as session:
            async with session.begin():
                await self._database.install_v2_request_scope(
                    session,
                    actor_id=str(request.scope.user_id),
                    workspace_id=(
                        None
                        if request.scope.workspace_id is None
                        else str(request.scope.workspace_id)
                    ),
                )
                row = await self._find_locked(session, request.scope)
                if row is None:
                    return None
                decision = self._decision_from_existing(row, request.fingerprint, now)
                if decision is not None:
                    return decision
                await session.execute(
                    delete(api_v2_idempotency_records).where(
                        api_v2_idempotency_records.c.id == str(row["id"])
                    )
                )
                return None

    async def _complete_in_session(
        self,
        session: AsyncSession,
        claim: IdempotencyClaim,
        response: ReplayableResponse,
        *,
        completed_at: datetime,
        expires_at: datetime,
    ) -> ReplayableResponse | None:
        """@brief 在 caller 事务内写入 receipt / Store a receipt in the caller transaction.

        @param session 已安装相同 V2 RLS scope 的 Session / Session with matching V2 RLS scope.
        @param claim 首次执行者私有 claim / First executor's private claim.
        @param response 逐字响应 / Byte-exact response.
        @param completed_at 完成时刻 / Completion instant.
        @param expires_at 保留边界 / Retention boundary.
        @return 保存的响应，或失去所有权时为空 / Stored response, or null after ownership loss.
        """
        _validate_time_range(completed_at, expires_at)
        if not await self._try_scope_lock(session, claim.scope):
            return None
        row = await self._find_locked(session, claim.scope)
        if row is None:
            return None
        if not hmac.compare_digest(str(row["request_fingerprint"]), claim.fingerprint):
            return None
        if str(row["status"]) == IdempotencyStatus.COMPLETED.value:
            return _response_from_row(row)
        stored_token_hash = row["claim_token_hash"]
        if not isinstance(stored_token_hash, str) or not hmac.compare_digest(
            stored_token_hash, _token_hash(claim.token)
        ):
            return None
        await session.execute(
            update(api_v2_idempotency_records)
            .where(api_v2_idempotency_records.c.id == str(row["id"]))
            .values(
                status=IdempotencyStatus.COMPLETED.value,
                claim_token_hash=None,
                response_status=response.status_code,
                response_headers=[
                    {"name": name, "value": value} for name, value in response.headers
                ],
                response_body=response.json_body,
                expires_at=expires_at,
                updated_at=completed_at,
            )
        )
        return response

    @asynccontextmanager
    async def _transaction(self, scope: IdempotencyScope) -> AsyncIterator[AsyncSession]:
        """@brief 打开安装 V2 RLS GUC 的短事务 / Open a short transaction with V2 RLS GUCs.

        @param scope 当前请求的签名 actor 与可空 Workspace / Current signed actor and nullable
            Workspace.
        @return 上下文内有效的独立 AsyncSession / Independent AsyncSession valid in context.
        """
        async with self._database.new_session() as session:
            async with session.begin():
                await self._database.install_v2_request_scope(
                    session,
                    actor_id=str(scope.user_id),
                    workspace_id=(None if scope.workspace_id is None else str(scope.workspace_id)),
                )
                yield session

    async def _try_scope_lock(
        self,
        session: AsyncSession,
        scope: IdempotencyScope,
    ) -> bool:
        """@brief 非阻塞获取 scope 事务级 advisory lock / Try a transaction advisory lock.

        @param session 当前事务 Session / Current transaction Session.
        @param scope 完整幂等 scope / Complete idempotency scope.
        @return 当前事务拥有锁时为真 / True when this transaction owns the lock.

        @note SHA-256 截断为有符号 64 位；碰撞只会产生一次可重试 409，不会绕过唯一约束。
            / SHA-256 is truncated to signed 64 bits; a collision only causes a retryable 409 and
            can never bypass the database uniqueness constraint.
        """
        value = await session.scalar(
            text("SELECT pg_try_advisory_xact_lock(:lock_key)"),
            {"lock_key": _scope_lock_key(scope)},
        )
        if not isinstance(value, bool):
            raise RuntimeError("PostgreSQL returned an invalid advisory-lock result")
        return value

    async def _find_locked(
        self,
        session: AsyncSession,
        scope: IdempotencyScope,
    ) -> Mapping[str, Any] | None:
        """@brief 按完整 scope 锁定一条记录 / Lock one record by its full scope.

        @param session 已安装相同 RLS scope 的 Session / Session with the same RLS scope.
        @param scope 完整幂等 scope / Full idempotency scope.
        @return mapping 行或不存在 / Mapping row or ``None``.
        """
        workspace_predicate = (
            api_v2_idempotency_records.c.workspace_id.is_(None)
            if scope.workspace_id is None
            else api_v2_idempotency_records.c.workspace_id == str(scope.workspace_id)
        )
        statement = (
            select(api_v2_idempotency_records)
            .where(
                api_v2_idempotency_records.c.user_id == str(scope.user_id),
                workspace_predicate,
                api_v2_idempotency_records.c.method == scope.method,
                api_v2_idempotency_records.c.canonical_path == scope.canonical_path,
                api_v2_idempotency_records.c.idempotency_key == scope.key,
            )
            .with_for_update()
        )
        return cast(
            Mapping[str, Any] | None,
            (await session.execute(statement)).mappings().first(),
        )

    def _new_claim(self, scope: IdempotencyScope, fingerprint: str) -> IdempotencyClaim:
        """@brief 生成高熵私有 claim / Generate a high-entropy private claim.

        @param scope 被 claim 的完整 scope / Fully claimed scope.
        @param fingerprint 请求指纹 / Request fingerprint.
        @return 仅保存在执行调用链中的 claim / Claim retained only in the execution call path.
        """
        return IdempotencyClaim(scope, fingerprint, secrets.token_urlsafe(32))

    def _decision_from_existing(
        self,
        row: Mapping[str, Any],
        fingerprint: str,
        now: datetime,
    ) -> IdempotencyDecision | None:
        """@brief 将已锁定行解释为 decision / Interpret a locked row as a decision.

        @param row 已锁定数据库行 / Locked database row.
        @param fingerprint 当前请求指纹 / Current request fingerprint.
        @param now 当前时刻 / Current instant.
        @return decision；过期 completed receipt 返回 ``None`` / Decision, or ``None`` for an
            expired completed receipt.
        @raise IdempotencyConflict 指纹不同时抛出 / Raised for a fingerprint mismatch.
        """
        status = str(row["status"])
        if status == IdempotencyStatus.PENDING.value:
            if not hmac.compare_digest(str(row["request_fingerprint"]), fingerprint):
                raise IdempotencyConflict("idempotency.key_reused")
            return IdempotencyDecision(IdempotencyDecisionKind.IN_PROGRESS)
        if status != IdempotencyStatus.COMPLETED.value:
            raise RuntimeError("database contains an unknown idempotency status")
        expires_at = row["expires_at"]
        if not isinstance(expires_at, datetime):
            raise RuntimeError("database idempotency expiry is not a datetime")
        if expires_at <= now:
            return None
        if not hmac.compare_digest(str(row["request_fingerprint"]), fingerprint):
            raise IdempotencyConflict("idempotency.key_reused")
        return IdempotencyDecision(
            IdempotencyDecisionKind.REPLAY,
            replay=_response_from_row(row),
        )


def _response_from_row(row: Mapping[str, Any]) -> ReplayableResponse:
    """@brief 从数据库行恢复逐字响应 / Restore a byte-exact response from a database row.

    @param row completed 数据库行 / Completed database row.
    @return 已验证 response value / Validated response value.
    @raise RuntimeError 持久化 receipt 形状损坏时抛出 / Raised for a corrupt receipt shape.
    """
    status = row["response_status"]
    body = row["response_body"]
    raw_headers = row["response_headers"]
    if (
        not isinstance(status, int)
        or not isinstance(body, bytes)
        or not isinstance(raw_headers, list)
    ):
        raise RuntimeError("completed idempotency receipt is incomplete")
    headers: list[tuple[str, str]] = []
    for item in raw_headers:
        if not isinstance(item, dict):
            raise RuntimeError("idempotency response header is malformed")
        name = item.get("name")
        value = item.get("value")
        if not isinstance(name, str) or not isinstance(value, str):
            raise RuntimeError("idempotency response header is malformed")
        headers.append((name, value))
    return ReplayableResponse(status, tuple(headers), body)


def _token_hash(token: str) -> str:
    """@brief 单向摘要私有 claim token / One-way hash a private claim token.

    @param token 高熵随机 token / High-entropy random token.
    @return SHA-256 十六进制摘要 / SHA-256 hexadecimal digest.
    """
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _scope_lock_key(scope: IdempotencyScope) -> int:
    """@brief 派生幂等 scope 的 advisory-lock key / Derive an advisory-lock key for a scope.

    @param scope 完整且已验证的幂等 scope / Complete validated idempotency scope.
    @return PostgreSQL 接受的有符号 64 位整数 / Signed 64-bit integer accepted by PostgreSQL.
    """
    fields = (
        str(scope.user_id),
        "" if scope.workspace_id is None else str(scope.workspace_id),
        scope.method,
        scope.canonical_path,
        scope.key,
    )
    digest = hashlib.sha256()
    for field in fields:
        encoded = field.encode("utf-8")
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
    return int.from_bytes(digest.digest()[:8], "big", signed=True)


def _preparation_id(request: IdempotencyRequest) -> IdempotencyPreparationId:
    """@brief 派生跨崩溃稳定且隐藏原始 key 的准备 ID / Derive a crash-stable preparation ID hiding the raw key.

    @param request 完整规范请求 / Complete canonical request.
    @return ``prep_`` 前缀的 SHA-256 标识 / SHA-256 identifier prefixed with ``prep_``.

    @note scope 与请求指纹均采用长度分帧；同 key 不同 body 会得到不同 ID，但在进入
        prepare 前已由 preflight 拒绝 / Scope fields and fingerprint are length-framed. A changed
        body would derive a different ID, but preflight rejects it before preparation.
    """

    fields = (
        str(request.scope.user_id),
        "" if request.scope.workspace_id is None else str(request.scope.workspace_id),
        request.scope.method,
        request.scope.canonical_path,
        request.scope.key,
        request.fingerprint,
    )
    digest = hashlib.sha256()
    for field in fields:
        encoded = field.encode("utf-8")
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
    return IdempotencyPreparationId(f"prep_{digest.hexdigest()}")


def _utc_now() -> datetime:
    """@brief 返回带时区 UTC 时间 / Return timezone-aware UTC time.

    @return 当前 UTC 时刻 / Current UTC instant.
    """
    return datetime.now(UTC)


def _validate_time_range(now: datetime, expires_at: datetime) -> None:
    """@brief 校验时间带 offset 且正向 / Validate timezone awareness and ordering.

    @param now 起始时刻 / Start instant.
    @param expires_at 保留边界 / Retention boundary.
    @raise ValueError 时间无 offset 或边界不在未来时抛出 / Raised for naive or unordered times.
    """
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("idempotency time must be timezone-aware")
    if expires_at.tzinfo is None or expires_at.utcoffset() is None:
        raise ValueError("idempotency expiry must be timezone-aware")
    if expires_at <= now:
        raise ValueError("idempotency expiry must be later than its start time")


__all__ = [
    "AtomicPostgresIdempotencyExecutor",
    "InMemoryIdempotencyExecutor",
    "InMemoryV2IdempotencyStore",
    "PostgresV2IdempotencyStore",
    "api_v2_idempotency_records",
]
