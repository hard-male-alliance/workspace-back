"""@brief API V2 PostgreSQL 原子 executor 编排测试 / Atomic executor orchestration tests."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import cast

import pytest

from backend.application.ports.v2_idempotency import (
    IdempotencyClaim,
    IdempotencyDecision,
    IdempotencyDecisionKind,
    IdempotencyRequest,
    IdempotencyScope,
    ReplayableResponse,
)
from backend.domain.principals import UserId, WorkspaceId
from backend.infrastructure.persistence.database import AsyncDatabase
from backend.infrastructure.v2_idempotency import (
    AtomicPostgresIdempotencyExecutor,
    PostgresV2IdempotencyStore,
)

_NOW = datetime(2026, 7, 23, 12, 0, tzinfo=UTC)
"""@brief 原子测试的固定时刻 / Fixed instant for atomic tests."""


@dataclass(slots=True)
class _Receipt:
    """@brief fake 事务中的 receipt / Receipt in the fake transaction.

    @param fingerprint 首次请求指纹 / First-request fingerprint.
    @param claim 首次执行者 claim / First executor claim.
    @param response completed 响应；pending 时为空 / Completed response, or null while pending.
    """

    fingerprint: str
    claim: IdempotencyClaim
    response: ReplayableResponse | None = None


@dataclass(slots=True)
class _State:
    """@brief 一份可提交的 fake 数据库状态 / One committable fake database state.

    @param effects 已提交业务副作用标签 / Committed business-effect labels.
    @param receipts scope 到 receipt 的映射 / Scope-to-receipt mapping.
    """

    effects: list[str] = field(default_factory=list)
    receipts: dict[IdempotencyScope, _Receipt] = field(default_factory=dict)


class _FakeTransaction:
    """@brief 不改变外层 fake 状态的 Session 事务 / No-op fake Session transaction."""

    async def __aenter__(self) -> _FakeTransaction:
        """@brief 进入 fake SAVEPOINT / Enter the fake SAVEPOINT.

        @return 当前事务 / This transaction.
        """
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> None:
        """@brief 离开 fake SAVEPOINT / Leave the fake SAVEPOINT.

        @param exc_type 异常类型 / Exception type.
        @param exc 异常对象 / Exception object.
        @param traceback traceback 对象 / Traceback object.
        @return 无返回值 / No return value.
        """


class _FakeSession:
    """@brief 满足 executor 上下文形状的 fake Session / Fake Session with executor shape."""

    async def __aenter__(self) -> _FakeSession:
        """@brief 进入 fake Session / Enter the fake Session.

        @return 当前 Session / This Session.
        """
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> None:
        """@brief 离开 fake Session / Leave the fake Session.

        @param exc_type 异常类型 / Exception type.
        @param exc 异常对象 / Exception object.
        @param traceback traceback 对象 / Traceback object.
        @return 无返回值 / No return value.
        """

    def begin(self) -> _FakeTransaction:
        """@brief 打开 fake SAVEPOINT / Open a fake SAVEPOINT.

        @return fake 事务 / Fake transaction.
        """
        return _FakeTransaction()


class _FakeAtomicDatabase:
    """@brief 模拟 commit/rollback 的事务信封 / Transaction envelope simulating commit and rollback."""

    def __init__(self) -> None:
        """@brief 初始化空 committed 状态 / Initialize empty committed state."""
        self.committed = _State()
        self.active: _State | None = None
        self.installed_scopes: list[tuple[str, str | None]] = []
        self.coordination_active = False

    @asynccontextmanager
    async def atomic_envelope(
        self,
        *,
        connection: object | None = None,
    ) -> AsyncIterator[None]:
        """@brief copy-on-write 执行外层事务 / Run a copy-on-write outer transaction.

        @return 正常退出提交、异常丢弃的上下文 / Context committing normally and discarding
            on an exception.
        """
        del connection
        if self.active is not None:
            raise RuntimeError("fake envelope cannot nest")
        self.active = deepcopy(self.committed)
        try:
            yield
        except BaseException:
            self.active = None
            raise
        else:
            self.committed = self._require_active()
            self.active = None

    @asynccontextmanager
    async def coordination_lock(self, lock_key: int) -> AsyncIterator[object | None]:
        """@brief 模拟仅持有 session lock、没有事务的协调窗口 / Simulate a transaction-free session-lock window.

        @param lock_key 未使用的确定性 lock key / Unused deterministic lock key.
        @return 独占 fake connection / Exclusive fake connection.
        """

        del lock_key
        if self.coordination_active:
            yield None
            return
        self.coordination_active = True
        connection = object()
        try:
            yield connection
        finally:
            self.coordination_active = False

    def new_session(self) -> _FakeSession:
        """@brief 创建加入外层事务的 fake Session / Create a fake joined Session.

        @return fake Session / Fake Session.
        """
        self._require_active()
        return _FakeSession()

    async def install_v2_request_scope(
        self,
        session: object,
        *,
        actor_id: str,
        workspace_id: str | None,
    ) -> None:
        """@brief 记录 executor 安装与恢复的 scope / Record installed and restored scopes.

        @param session 未使用的 fake Session / Unused fake Session.
        @param actor_id 已验证 actor / Verified actor.
        @param workspace_id 请求 Workspace / Request Workspace.
        @return 无返回值 / No return value.
        """
        del session
        self.installed_scopes.append((actor_id, workspace_id))

    def add_effect(self, label: str) -> None:
        """@brief 在活动事务添加业务副作用 / Add a business effect to the active transaction.

        @param label 唯一测试标签 / Unique test label.
        @return 无返回值 / No return value.
        """
        state = self._require_active()
        if label in state.effects:
            raise RuntimeError("business effect was executed twice")
        state.effects.append(label)

    def _require_active(self) -> _State:
        """@brief 返回活动事务状态 / Return active transaction state.

        @return 活动 copy-on-write 状态 / Active copy-on-write state.
        @raise RuntimeError 不在事务中时抛出 / Raised outside a transaction.
        """
        if self.active is None:
            raise RuntimeError("fake database has no active transaction")
        return self.active


def _request(key: str) -> IdempotencyRequest:
    """@brief 构造隔离测试请求 / Build an isolated test request.

    @param key 合法幂等 key / Valid idempotency key.
    @return 完整请求 / Complete request.
    """
    return IdempotencyRequest(
        IdempotencyScope(
            UserId("usr_atomic"),
            WorkspaceId("ws_atomic"),
            "POST",
            "/api/v2/workspaces/ws_atomic/resources",
            key,
        ),
        b"{}",
        "application/json",
        None,
    )


@pytest.mark.asyncio
async def test_atomic_executor_replays_and_rolls_back_claim_with_inner_effect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief receipt 可 replay，崩溃时 claim 与领域副作用一起回滚 / Replay and rollback are atomic.

    @param monkeypatch 临时替换 SQL store 为事务内 fake / Replaces SQL methods with an
        in-transaction fake.
    @return 无返回值 / No return value.
    """
    database = _FakeAtomicDatabase()

    async def claim_in_session(
        store: PostgresV2IdempotencyStore,
        session: object,
        request: IdempotencyRequest,
        *,
        now: datetime,
        expires_at: datetime,
    ) -> IdempotencyDecision:
        """@brief 在 fake 状态 claim 或 replay / Claim or replay in fake state.

        @param store 未使用的真实 adapter 壳 / Unused real-adapter shell.
        @param session 未使用的 fake Session / Unused fake Session.
        @param request 当前请求 / Current request.
        @param now 当前时间 / Current time.
        @param expires_at receipt 到期时间 / Receipt expiry.
        @return claim、pending 或 replay / Claim, pending, or replay.
        """
        del store, session, now, expires_at
        state = database._require_active()
        receipt = state.receipts.get(request.scope)
        if receipt is not None:
            if receipt.fingerprint != request.fingerprint:
                raise AssertionError("test unexpectedly reused a key with different input")
            if receipt.response is None:
                return IdempotencyDecision(IdempotencyDecisionKind.IN_PROGRESS)
            return IdempotencyDecision(
                IdempotencyDecisionKind.REPLAY,
                replay=receipt.response,
            )
        claim = IdempotencyClaim(request.scope, request.fingerprint, f"claim:{request.scope.key}")
        state.receipts[request.scope] = _Receipt(request.fingerprint, claim)
        return IdempotencyDecision(IdempotencyDecisionKind.CLAIMED, claim=claim)

    async def complete_in_session(
        store: PostgresV2IdempotencyStore,
        session: object,
        claim: IdempotencyClaim,
        response: ReplayableResponse,
        *,
        completed_at: datetime,
        expires_at: datetime,
    ) -> ReplayableResponse | None:
        """@brief 在 fake 状态完成 receipt / Complete a receipt in fake state.

        @param store 未使用的真实 adapter 壳 / Unused real-adapter shell.
        @param session 未使用的 fake Session / Unused fake Session.
        @param claim 首次 claim / First claim.
        @param response 待保存响应 / Response to store.
        @param completed_at 完成时间 / Completion time.
        @param expires_at receipt 到期时间 / Receipt expiry.
        @return 保存的响应或 claim 不匹配时为空 / Stored response, or null on mismatch.
        """
        del store, session, completed_at, expires_at
        receipt = database._require_active().receipts.get(claim.scope)
        if receipt is None or receipt.claim != claim:
            return None
        receipt.response = response
        return response

    monkeypatch.setattr(PostgresV2IdempotencyStore, "_claim_in_session", claim_in_session)
    monkeypatch.setattr(PostgresV2IdempotencyStore, "_complete_in_session", complete_in_session)
    executor = AtomicPostgresIdempotencyExecutor(
        cast(AsyncDatabase, database),
        clock=lambda: _NOW,
    )
    success_calls = 0

    async def success() -> ReplayableResponse:
        """@brief 模拟提交一个领域 UoW / Simulate one committed domain UoW.

        @return 成功响应 / Success response.
        """
        nonlocal success_calls
        success_calls += 1
        database.add_effect("success")
        return ReplayableResponse(201, (("Content-Type", "application/json"),), b'{"ok":true}')

    first = await executor.execute(_request("atomic-success-key-0001"), success)
    replay = await executor.execute(_request("atomic-success-key-0001"), success)
    assert replay == first
    assert success_calls == 1

    async def crash_after_inner_commit() -> ReplayableResponse:
        """@brief 模拟领域 UoW 后崩溃 / Simulate a crash after the domain UoW.

        @return 永不返回 / Never returns.
        @raise RuntimeError 固定故障注入 / Deterministic fault injection.
        """
        database.add_effect("retry")
        raise RuntimeError("crash after inner UoW commit")

    with pytest.raises(RuntimeError, match="crash after inner"):
        await executor.execute(_request("atomic-retry-key-00001"), crash_after_inner_commit)
    assert database.committed.effects == ["success"]

    async def retry() -> ReplayableResponse:
        """@brief 安全重试已回滚命令 / Safely retry the rolled-back command.

        @return 成功响应 / Success response.
        """
        database.add_effect("retry")
        return ReplayableResponse(
            201,
            (("Content-Type", "application/json"),),
            b'{"retried":true}',
        )

    await executor.execute(_request("atomic-retry-key-00001"), retry)

    assert database.committed.effects == ["success", "retry"]
    assert len(database.committed.receipts) == 2
    assert all(receipt.response is not None for receipt in database.committed.receipts.values())
    # Scope is restored after each operation before receipt RLS access.
    assert database.installed_scopes.count(("usr_atomic", "ws_atomic")) == 6


@pytest.mark.asyncio
async def test_prepared_executor_keeps_external_io_outside_transaction_and_replays(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@brief prepare 仅持 session lock，commit 与 receipt 才进入短事务 / Keep prepare transaction-free and commit atomically.

    @param monkeypatch 替换 SQL store 为可观察 fake / Replace SQL store methods with observable fakes.
    @return 无返回值 / No return value.
    """

    database = _FakeAtomicDatabase()

    async def preflight(
        store: PostgresV2IdempotencyStore,
        connection: object,
        request: IdempotencyRequest,
        *,
        now: datetime,
    ) -> IdempotencyDecision | None:
        """@brief 在无事务协调窗口读取已提交 receipt / Read a committed receipt without a transaction envelope."""

        del store, connection, now
        assert database.coordination_active
        assert database.active is None
        receipt = database.committed.receipts.get(request.scope)
        if receipt is None:
            return None
        if receipt.fingerprint != request.fingerprint:
            raise AssertionError("unexpected fingerprint mismatch")
        if receipt.response is None:
            return IdempotencyDecision(IdempotencyDecisionKind.IN_PROGRESS)
        return IdempotencyDecision(
            IdempotencyDecisionKind.REPLAY,
            replay=receipt.response,
        )

    async def claim_in_session(
        store: PostgresV2IdempotencyStore,
        session: object,
        request: IdempotencyRequest,
        *,
        now: datetime,
        expires_at: datetime,
    ) -> IdempotencyDecision:
        """@brief 在最终原子信封创建 fake claim / Create a fake claim in the final envelope."""

        del store, session, now, expires_at
        assert database.coordination_active
        state = database._require_active()
        claim = IdempotencyClaim(
            request.scope,
            request.fingerprint,
            f"claim:{request.scope.key}",
        )
        state.receipts[request.scope] = _Receipt(request.fingerprint, claim)
        return IdempotencyDecision(IdempotencyDecisionKind.CLAIMED, claim=claim)

    async def complete_in_session(
        store: PostgresV2IdempotencyStore,
        session: object,
        claim: IdempotencyClaim,
        response: ReplayableResponse,
        *,
        completed_at: datetime,
        expires_at: datetime,
    ) -> ReplayableResponse | None:
        """@brief 在同一原子信封写入 fake receipt / Store the fake receipt in the same envelope."""

        del store, session, completed_at, expires_at
        receipt = database._require_active().receipts[claim.scope]
        receipt.response = response
        return response

    monkeypatch.setattr(PostgresV2IdempotencyStore, "_preflight_on_connection", preflight)
    monkeypatch.setattr(PostgresV2IdempotencyStore, "_claim_in_session", claim_in_session)
    monkeypatch.setattr(PostgresV2IdempotencyStore, "_complete_in_session", complete_in_session)
    executor = AtomicPostgresIdempotencyExecutor(
        cast(AsyncDatabase, database),
        clock=lambda: _NOW,
    )
    preparation_ids: list[str] = []

    async def prepare(preparation_id: str) -> str:
        """@brief 断言外部准备没有活动事务 / Assert external preparation has no active transaction."""

        assert database.coordination_active
        assert database.active is None
        preparation_ids.append(preparation_id)
        return "provider-result"

    async def commit(prepared: str) -> ReplayableResponse:
        """@brief 在原子信封内提交业务效果 / Commit the business effect inside the envelope."""

        assert prepared == "provider-result"
        assert database.coordination_active
        database.add_effect("prepared-success")
        return ReplayableResponse(201, (("Content-Type", "application/json"),), b'{"ok":true}')

    request = _request("atomic-prepared-key-0001")
    first = await executor.execute_prepared(request, prepare, commit)
    replay = await executor.execute_prepared(request, prepare, commit)

    assert replay == first
    assert database.committed.effects == ["prepared-success"]
    assert len(preparation_ids) == 1
    assert preparation_ids[0].startswith("prep_")
    assert request.scope.key not in preparation_ids[0]
    assert database.coordination_active is False
