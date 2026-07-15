"""@brief PostgreSQL 幂等 pending claim 接管的无网络测试 / Network-free tests for PostgreSQL idempotency-claim takeover."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest

from backend.infrastructure.idempotency import IdempotentResponse
from backend.infrastructure.persistence.database import AsyncDatabase
from backend.infrastructure.persistence.runtime_repository import PostgresIdempotencyRegistry
from workspace_shared.tenancy import ActorScope

_SCOPE = ActorScope("usr_klee", "ws_alpha", "usr_klee")
"""@brief 定向测试使用的固定租户范围 / Fixed tenant scope for focused tests."""

_DIGEST = "stable-payload-digest"
"""@brief 定向测试使用的固定请求摘要 / Fixed request digest for focused tests."""


@dataclass
class _FakeIdempotencyRow:
    """@brief 仅覆盖 claim 逻辑字段的 ORM 行 fake / ORM-row fake covering only claim fields.

    @param request_hash 已持久化请求摘要 / Persisted request digest.
    @param response_status 可选完成状态 / Optional completed status.
    @param response_body 可选完成响应体 / Optional completed response body.
    @param expires_at 幂等记录总到期时间 / Overall idempotency-record expiry.
    @param created_at 行创建时间 / Row creation time.
    @param updated_at 行更新时间 / Row update time.
    @param revision 乐观版本号 / Optimistic version number.
    @param extensions 私有 runtime 令牌所在的 JSONB 形状 / JSONB shape holding the private runtime token.
    """

    request_hash: str
    response_status: int | None
    response_body: dict[str, object] | None
    expires_at: datetime
    created_at: datetime
    updated_at: datetime
    revision: int
    extensions: dict[str, object]


@dataclass
class _FakeScalarRows:
    """@brief 模拟 SQLAlchemy ``ScalarResult`` / Fake SQLAlchemy ``ScalarResult``.

    @param row 查询应返回的唯一行 / Single row returned by a query.
    """

    row: _FakeIdempotencyRow | None

    def first(self) -> _FakeIdempotencyRow | None:
        """@brief 返回唯一 fake 行 / Return the single fake row.

        @return 预设行或 ``None``。
        """
        return self.row


@dataclass
class _FakeSession:
    """@brief 只记录被删除行的最小异步 Session fake / Minimal async-session fake recording deleted rows.

    @param row 每次标量查询返回的当前行 / Current row returned by every scalar query.
    @param deleted 被 ``delete`` 的行 / Rows passed to ``delete``.
    """

    row: _FakeIdempotencyRow | None
    deleted: list[_FakeIdempotencyRow] = field(default_factory=list)

    async def scalars(self, _: object) -> _FakeScalarRows:
        """@brief 忽略 SQL 表达式并返回当前行 / Ignore SQL expression and return current row.

        @param _ 真实实现会传入 SQLAlchemy statement / SQLAlchemy statement in the real implementation.
        @return 包含当前行的标量结果。
        """
        return _FakeScalarRows(self.row)

    async def delete(self, row: _FakeIdempotencyRow) -> None:
        """@brief 记录删除请求 / Record a deletion request.

        @param row 请求删除的行 / Row requested for deletion.
        @return 无返回值。
        """
        self.deleted.append(row)


@dataclass
class _FakeDatabase:
    """@brief 只支持读写短事务的最小数据库 fake / Minimal database fake supporting write transactions.

    @param session 每一个事务交还的 session / Session yielded by every transaction.
    """

    session: _FakeSession

    @asynccontextmanager
    async def transaction(self, _: ActorScope) -> AsyncIterator[_FakeSession]:
        """@brief 返回预设 transaction session / Yield the configured transaction session.

        @param _ 调用方租户范围 / Caller tenant scope.
        @return 同一个受控 fake session。
        """
        yield self.session


def _replacement_pending_row(claim_token: str) -> _FakeIdempotencyRow:
    """@brief 构造接管后的新 pending 行 / Build a replacement pending row after takeover.

    @param claim_token 新 claimant 的私有令牌 / Private token of the new claimant.
    @return 尚未完成、但 token 已变更的行。
    """
    now = datetime.now(UTC)
    return _FakeIdempotencyRow(
        request_hash=_DIGEST,
        response_status=None,
        response_body=None,
        expires_at=now + timedelta(hours=1),
        created_at=now,
        updated_at=now,
        revision=1,
        extensions={
            "runtime": {
                "pending_claim_token": claim_token,
                "pending_until": (now + timedelta(minutes=5)).isoformat(),
            }
        },
    )


def _registry(session: _FakeSession) -> PostgresIdempotencyRegistry:
    """@brief 使用内存 fake 构造待测 registry / Construct registry under test with an in-memory fake.

    @param session 测试期望观察的 fake session / Fake session observed by the test.
    @return 仅用于私有 claim 方法的 registry。
    """
    return PostgresIdempotencyRegistry(cast(AsyncDatabase, _FakeDatabase(session)))


@pytest.mark.asyncio
async def test_stale_claimant_cannot_complete_replacement_pending_claim() -> None:
    """@brief 旧 claimant 不能把接管者的 pending 行完成 / Stale claimant cannot complete a replacement pending row."""
    replacement_token = "replacement-claim-token-secret"
    row = _replacement_pending_row(replacement_token)
    registry = _registry(_FakeSession(row))

    result = await registry._complete_claim(
        _SCOPE,
        "/api/v1/resumes",
        "idem-key",
        _DIGEST,
        "stale-claim-token-secret",
        IdempotentResponse(201, {"winner": "stale"}),
    )

    assert result is None
    assert row.response_status is None
    assert row.response_body is None
    assert row.revision == 1
    runtime = cast(dict[str, object], row.extensions["runtime"])
    assert row.extensions["runtime"] == {
        "pending_claim_token": replacement_token,
        "pending_until": runtime["pending_until"],
    }


@pytest.mark.asyncio
async def test_stale_claimant_cannot_release_replacement_pending_claim() -> None:
    """@brief 旧 claimant 失败时不能删除接管者的 pending 行 / Stale claimant failure cannot delete a replacement pending row."""
    row = _replacement_pending_row("replacement-claim-token-secret")
    session = _FakeSession(row)
    registry = _registry(session)

    await registry._release_pending_claim(
        _SCOPE,
        "/api/v1/resumes",
        "idem-key",
        _DIGEST,
        "stale-claim-token-secret",
    )

    assert session.deleted == []
    assert row.response_status is None


@pytest.mark.asyncio
async def test_current_claimant_completes_and_erases_private_pending_token() -> None:
    """@brief 当前 claimant 能完成，同时不保留私有 token / Current claimant can complete and does not retain its private token."""
    claim_token = "current-claim-token-secret"
    row = _replacement_pending_row(claim_token)
    registry = _registry(_FakeSession(row))

    result = await registry._complete_claim(
        _SCOPE,
        "/api/v1/resumes",
        "idem-key",
        _DIGEST,
        claim_token,
        IdempotentResponse(201, {"winner": "current"}),
    )

    assert result == IdempotentResponse(201, {"winner": "current"})
    assert row.response_status == 201
    assert row.response_body == {"winner": "current"}
    assert row.extensions["runtime"] == {"completed": True}
