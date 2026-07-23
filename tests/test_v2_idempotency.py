"""@brief API V2 幂等语义测试 / API V2 idempotency semantic tests."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest

from backend.application.ports.v2_idempotency import (
    IdempotencyClaim,
    IdempotencyConflict,
    IdempotencyRequest,
    IdempotencyScope,
    ReplayableResponse,
    request_fingerprint,
)
from backend.domain.principals import UserId, WorkspaceId
from backend.infrastructure.v2_idempotency import (
    InMemoryIdempotencyExecutor,
    InMemoryV2IdempotencyStore,
)

_NOW = datetime(2026, 7, 23, 8, 0, tzinfo=UTC)
"""@brief 测试使用的确定性 UTC 时刻 / Deterministic UTC instant used by tests."""


@dataclass(slots=True)
class _Clock:
    """@brief 可前移的同步测试时钟 / Advanceable synchronous test clock.

    @param value 当前返回时刻 / Currently returned instant.
    """

    value: datetime

    def __call__(self) -> datetime:
        """@brief 返回当前测试时刻 / Return the current test instant.

        @return 带时区 UTC 时间 / Timezone-aware UTC time.
        """
        return self.value


def _request(
    *,
    user_id: str = "usr_klee",
    workspace_id: str | None = "ws_alpha",
    method: str = "POST",
    path: str = "/api/v2/workspaces/{workspace_id}/invitations",
    key: str = "retry-safe-key-0001",
    body: bytes = b'{"email":"alice@example.com"}',
    content_type: str | None = "application/json",
    if_match: str | None = None,
) -> IdempotencyRequest:
    """@brief 构造可覆盖任一 scope 维度的请求 / Build a request varying any scope dimension.

    @param user_id 签名用户 ID / Signed user ID.
    @param workspace_id 可空 Workspace ID / Nullable Workspace ID.
    @param method HTTP method / HTTP method.
    @param path 规范 route path / Canonical route path.
    @param key 已校验 key / Validated key.
    @param body 规范请求字节 / Canonical request bytes.
    @param content_type 相关 Content-Type / Relevant Content-Type.
    @param if_match 相关 If-Match / Relevant If-Match.
    @return 幂等请求值 / Idempotency request value.
    """
    return IdempotencyRequest(
        IdempotencyScope(
            UserId(user_id),
            None if workspace_id is None else WorkspaceId(workspace_id),
            method,
            path,
            key,
        ),
        body,
        content_type,
        if_match,
    )


def _response(label: str = "first") -> ReplayableResponse:
    """@brief 构造带有强 ETag 与 Location 的原始响应 / Build an original response with ETag and Location.

    @param label JSON 中用于区分执行次数的标签 / Label distinguishing executions in JSON.
    @return 可逐字 replay 的 201 响应 / Byte-exact replayable 201 response.
    """
    return ReplayableResponse(
        201,
        (
            ("Content-Type", "application/json"),
            ("Location", "/api/v2/workspaces/ws_alpha/invitations/inv_1"),
            ("ETag", '"inv_1-r1"'),
        ),
        f'{{ "result": "{label}" }}\n'.encode(),
    )


def test_fingerprint_frames_body_and_relevant_headers_without_ambiguity() -> None:
    """@brief body、Content-Type 与 If-Match 都改变指纹 / Body and relevant headers alter fingerprints.

    @return 无返回值 / No return value.
    """
    baseline = request_fingerprint(b"abc", content_type="application/json", if_match='"r1"')
    assert baseline == request_fingerprint(b"abc", content_type="application/json", if_match='"r1"')
    assert baseline != request_fingerprint(
        b"abcd", content_type="application/json", if_match='"r1"'
    )
    assert baseline != request_fingerprint(
        b"abc", content_type="application/merge-patch+json", if_match='"r1"'
    )
    assert baseline != request_fingerprint(b"abc", content_type="application/json", if_match='"r2"')
    assert request_fingerprint(b"abc", content_type=None, if_match=None) != request_fingerprint(
        b"abc", content_type="", if_match=None
    )


@pytest.mark.asyncio
async def test_completed_request_replays_exact_status_headers_and_body_once() -> None:
    """@brief 同指纹只执行一次且 response 逐字 replay / Same fingerprint executes once and replays exactly.

    @return 无返回值 / No return value.
    """
    executor = InMemoryIdempotencyExecutor(InMemoryV2IdempotencyStore(), clock=_Clock(_NOW))
    request = _request()
    calls = 0

    async def operation() -> ReplayableResponse:
        """@brief 记录首次业务执行 / Record the first business execution.

        @return 原始 response / Original response.
        """
        nonlocal calls
        calls += 1
        return _response()

    first = await executor.execute(request, operation)
    replay = await executor.execute(request, operation)

    assert calls == 1
    assert replay == first
    assert replay.status_code == 201
    assert replay.headers == _response().headers
    assert replay.json_body == b'{ "result": "first" }\n'


@pytest.mark.asyncio
async def test_same_scope_and_key_with_different_request_is_key_reused() -> None:
    """@brief 同 scope/key 的不同规范输入返回冻结 409 code / Different input under one scope/key returns frozen 409 code.

    @return 无返回值 / No return value.
    """
    executor = InMemoryIdempotencyExecutor(InMemoryV2IdempotencyStore(), clock=_Clock(_NOW))

    async def operation() -> ReplayableResponse:
        """@brief 返回可缓存响应 / Return a cacheable response.

        @return 测试响应 / Test response.
        """
        return _response()

    await executor.execute(_request(), operation)

    with pytest.raises(IdempotencyConflict) as captured:
        await executor.execute(_request(body=b'{"email":"bob@example.com"}'), operation)

    assert captured.value.problem.status == 409
    assert captured.value.problem.code == "idempotency.key_reused"
    assert captured.value.retry_after_seconds is None


@pytest.mark.asyncio
async def test_each_scope_dimension_isolated_from_the_same_key() -> None:
    """@brief principal/workspace/method/path 都是唯一 scope 的一部分 / Every scope dimension isolates the same key.

    @return 无返回值 / No return value.
    """
    executor = InMemoryIdempotencyExecutor(InMemoryV2IdempotencyStore(), clock=_Clock(_NOW))
    requests = (
        _request(),
        _request(user_id="usr_alice"),
        _request(workspace_id="ws_beta"),
        _request(workspace_id=None, path="/api/v2/me/account-deletion-requests"),
        _request(method="PATCH"),
        _request(path="/api/v2/workspaces/{workspace_id}/knowledge-sources"),
    )
    calls = 0

    async def operation() -> ReplayableResponse:
        """@brief 为每一个隔离 scope 产生响应 / Produce a response for every isolated scope.

        @return 含执行序号的响应 / Response containing the execution ordinal.
        """
        nonlocal calls
        calls += 1
        return _response(str(calls))

    results = [await executor.execute(request, operation) for request in requests]

    assert calls == len(requests)
    assert len({result.json_body for result in results}) == len(requests)


@pytest.mark.asyncio
async def test_concurrent_duplicate_gets_in_progress_with_retry_after() -> None:
    """@brief callback 执行期间的重复请求立即 409 / A duplicate during callback execution gets immediate 409.

    @return 无返回值 / No return value.
    """
    executor = InMemoryIdempotencyExecutor(
        InMemoryV2IdempotencyStore(),
        clock=_Clock(_NOW),
        retry_after_seconds=3,
    )
    started = asyncio.Event()
    release = asyncio.Event()

    async def slow_operation() -> ReplayableResponse:
        """@brief 暴露 callback pending 窗口 / Expose the callback pending window.

        @return release 后的响应 / Response after release.
        """
        started.set()
        await release.wait()
        return _response()

    first = asyncio.create_task(executor.execute(_request(), slow_operation))
    await started.wait()
    try:
        with pytest.raises(IdempotencyConflict) as captured:
            await executor.execute(_request(), slow_operation)
        assert captured.value.problem.code == "idempotency.in_progress"
        assert captured.value.problem.retryable is True
        assert captured.value.retry_after_seconds == 3
    finally:
        release.set()
    assert await first == _response()


@pytest.mark.asyncio
async def test_callback_failure_keeps_pending_even_after_retention_window() -> None:
    """@brief 非原子 callback 失败后不删除或超时接管 pending / Failed non-atomic callback is never released or taken over.

    @return 无返回值 / No return value.
    """
    clock = _Clock(_NOW)
    executor = InMemoryIdempotencyExecutor(InMemoryV2IdempotencyStore(), clock=clock)
    calls = 0

    async def possibly_committed_operation() -> ReplayableResponse:
        """@brief 模拟业务提交后 receipt 前崩溃 / Simulate a crash after business commit but before receipt.

        @return 永不返回 / Never returns.
        @raise RuntimeError 模拟不确定提交结果 / Simulated uncertain commit outcome.
        """
        nonlocal calls
        calls += 1
        raise RuntimeError("connection lost after business commit")

    with pytest.raises(RuntimeError, match="connection lost"):
        await executor.execute(_request(), possibly_committed_operation)

    clock.value += timedelta(days=8)
    with pytest.raises(IdempotencyConflict) as captured:
        await executor.execute(_request(), possibly_committed_operation)

    assert calls == 1
    assert captured.value.problem.code == "idempotency.in_progress"


@pytest.mark.asyncio
async def test_completed_receipt_can_be_reclaimed_only_after_minimum_retention() -> None:
    """@brief completed receipt 保留满 24h 后才允许同 key 新执行 / Completed receipt permits reuse only after retention.

    @return 无返回值 / No return value.
    """
    clock = _Clock(_NOW)
    executor = InMemoryIdempotencyExecutor(InMemoryV2IdempotencyStore(), clock=clock)
    calls = 0

    async def operation() -> ReplayableResponse:
        """@brief 返回带调用序号的响应 / Return a response with the call ordinal.

        @return 序号响应 / Ordinal response.
        """
        nonlocal calls
        calls += 1
        return _response(str(calls))

    await executor.execute(_request(), operation)
    clock.value += timedelta(hours=23, minutes=59)
    await executor.execute(_request(), operation)
    assert calls == 1

    clock.value += timedelta(minutes=2)
    renewed = await executor.execute(
        _request(body=b'{"email":"new-generation@example.com"}'),
        operation,
    )
    assert calls == 2
    assert renewed.json_body == b'{ "result": "2" }\n'


@pytest.mark.asyncio
async def test_forged_claim_cannot_complete_another_callers_pending_record() -> None:
    """@brief 错误 token 不能完成 pending receipt / A wrong token cannot complete a pending receipt.

    @return 无返回值 / No return value.
    """
    store = InMemoryV2IdempotencyStore()
    request = _request()
    decision = await store.claim(
        request,
        now=_NOW,
        expires_at=_NOW + timedelta(hours=24),
    )
    assert decision.claim is not None
    forged = IdempotencyClaim(
        decision.claim.scope,
        decision.claim.fingerprint,
        "forged-private-token",
    )

    completed = await store.complete(
        forged,
        _response(),
        completed_at=_NOW,
        expires_at=_NOW + timedelta(hours=24),
    )
    duplicate = await store.claim(
        request,
        now=_NOW,
        expires_at=_NOW + timedelta(hours=24),
    )

    assert completed is None
    assert duplicate.kind.value == "in_progress"


def test_response_snapshot_rejects_tracking_and_credential_headers() -> None:
    """@brief receipt 不得缓存旧 request ID 或认证 material / Receipt rejects stale tracking and credential material.

    @return 无返回值 / No return value.
    """
    with pytest.raises(ValueError, match="not replayable"):
        ReplayableResponse(201, (("X-Request-Id", "request-first"),), b"{}")
    with pytest.raises(ValueError, match="not replayable"):
        ReplayableResponse(201, (("Set-Cookie", "secret=value"),), b"{}")
    with pytest.raises(ValueError, match="valid UTF-8 JSON"):
        ReplayableResponse(201, (), b"not-json")


def test_executor_enforces_contract_minimum_but_accepts_resume_retention() -> None:
    """@brief 普通命令不能低于 24h，Resume 可配置 30d / Normal commands require 24h and Resume may use 30d.

    @return 无返回值 / No return value.
    """
    with pytest.raises(ValueError, match="at least 24 hours"):
        InMemoryIdempotencyExecutor(
            InMemoryV2IdempotencyStore(), retention=timedelta(hours=23)
        )
    InMemoryIdempotencyExecutor(InMemoryV2IdempotencyStore(), retention=timedelta(days=30))
