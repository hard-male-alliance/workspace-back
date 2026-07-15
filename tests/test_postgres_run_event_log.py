"""@brief PostgreSQL Agent Run 仅追加事件日志的无数据库测试 / Database-free tests for PostgreSQL Agent Run append-only event logs."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest

from backend.domain.agent import AgentRunRecord, AgentRunStatus
from backend.infrastructure.persistence.runtime_repository import PostgresWorkspaceRepository
from workspace_shared.tenancy import ActorScope

_SCOPE = ActorScope("usr_klee", "ws_events", "usr_klee")
"""@brief 测试使用的完整租户范围 / Complete tenant scope used by these tests."""

_NOW = datetime(2026, 7, 15, tzinfo=UTC)
"""@brief 可重复的事件时间 / Deterministic timestamp for events."""


@dataclass(slots=True)
class _FakeScalars:
    """@brief 模拟 SQLAlchemy scalar 结果 / Fake SQLAlchemy scalar result."""

    rows: list[Any]

    def all(self) -> list[Any]:
        """@brief 返回固定结果行 / Return fixed result rows.

        @return 测试预置的 ORM 行列表 / Preconfigured ORM-row list for the test.
        """
        return self.rows


@dataclass(slots=True)
class _FakeSession:
    """@brief 仅实现事件写路径所需方法的 session 替身 / Session stand-in implementing only methods needed by event writes."""

    existing_events: list[Any]
    added: list[Any] = field(default_factory=list)

    async def scalars(self, statement: object) -> _FakeScalars:
        """@brief 忽略 SQL 表达式并返回现有事件 / Ignore the SQL expression and return existing events.

        @param statement 被测方法构造的 scope 限定查询 / Scoped query built by the method under test.
        @return 可枚举的固定事件行。
        """
        del statement
        return _FakeScalars(self.existing_events)

    def add(self, row: Any) -> None:
        """@brief 记录待插入的 ORM 行 / Record a pending ORM insert row.

        @param row 被测方法追加的事件 ORM 行 / Event ORM row appended by the method under test.
        @return 无返回值。
        """
        self.added.append(row)


def _run_row() -> SimpleNamespace:
    """@brief 构造最小锁定 Run ORM 行 / Construct a minimal locked Run ORM row.

    @return 能被 ``_write_run_locked`` 更新的可变行对象。
    """
    return SimpleNamespace(
        extensions={},
        status="queued",
        capability="general",
        response_locale="zh-CN",
        inference_intent={},
        effective_knowledge_selection={},
        error=None,
        started_at=None,
        finished_at=None,
        updated_at=_NOW,
        revision=1,
    )


def _run() -> AgentRunRecord:
    """@brief 构造最小可写领域 Run / Construct a minimal writable domain Run.

    @return 事件列表为空的 queued Run。
    """
    return AgentRunRecord(
        scope=_SCOPE,
        id="run_append_only",
        conversation_id="conv_append_only",
        input_message_id="msg_input",
        created_at=_NOW,
        updated_at=_NOW,
        request={"capability": "general", "response_locale": "zh-CN", "inference": {}, "knowledge": {}},
    )


def _event_row(event: dict[str, Any]) -> SimpleNamespace:
    """@brief 把领域事件投影为已持久化 ORM 行 / Project a domain event into a persisted ORM row.

    @param event 已追加的 SSE event envelope / Appended SSE event envelope.
    @return 仅带不可变比对字段的 ORM 行替身。
    """
    return SimpleNamespace(
        id=event["event_id"],
        sequence=event["sequence"],
        event_type=event["event_type"],
        payload=deepcopy(event["payload"]),
        trace_id=event["trace_id"],
    )


def _repository_without_database() -> PostgresWorkspaceRepository:
    """@brief 构造不触发数据库初始化的 repository / Construct a repository without database initialization.

    @return 只用于测试私有纯写逻辑的对象。
    """
    return object.__new__(PostgresWorkspaceRepository)


@pytest.mark.asyncio
async def test_write_run_locked_appends_only_the_new_event() -> None:
    """@brief 已持久化前缀不得 delete/reinsert / Persisted prefixes must not be deleted and reinserted."""
    run = _run()
    first = run.append_event("agent.run.started", {"state": "started"}, "req_first")
    existing = _event_row(first)
    run.status = AgentRunStatus.RUNNING
    run.append_event("agent.message.delta", {"delta": "你好"}, "req_second")
    session = _FakeSession([existing])

    await _repository_without_database()._write_run_locked(
        cast(Any, session), _SCOPE, _run_row(), run
    )

    assert [row.sequence for row in session.added] == [1]
    assert session.added[0].id == run.events[1]["event_id"]
    assert existing.id == first["event_id"]


@pytest.mark.asyncio
async def test_write_run_locked_rejects_a_stale_snapshot_that_omits_an_event() -> None:
    """@brief 陈旧 Run 不能省略数据库已经接受的事件 / A stale Run cannot omit an event already accepted by the database."""
    persisted_run = _run()
    persisted = persisted_run.append_event("agent.run.started", {"state": "started"}, None)
    stale_run = _run()
    session = _FakeSession([_event_row(persisted)])

    with pytest.raises(RuntimeError, match="omits a persisted immutable event"):
        await _repository_without_database()._write_run_locked(
            cast(Any, session), _SCOPE, _run_row(), stale_run
        )


@pytest.mark.asyncio
async def test_write_run_locked_rejects_non_contiguous_or_rewritten_events() -> None:
    """@brief sequence 空洞与同序列改写都必须失败 / Sequence holes and same-sequence rewrites must both fail."""
    run = _run()
    event = run.append_event("agent.run.started", {"state": "started"}, None)
    event["sequence"] = 1
    with pytest.raises(RuntimeError, match="contiguous immutable log"):
        await _repository_without_database()._write_run_locked(
            cast(Any, _FakeSession([])), _SCOPE, _run_row(), run
        )

    rewritten_run = _run()
    rewritten = rewritten_run.append_event("agent.run.started", {"state": "rewritten"}, None)
    session = _FakeSession([_event_row({**rewritten, "payload": {"state": "original"}})])
    with pytest.raises(RuntimeError, match="cannot rewrite a persisted immutable agent run event"):
        await _repository_without_database()._write_run_locked(
            cast(Any, session), _SCOPE, _run_row(), rewritten_run
        )


@pytest.mark.asyncio
async def test_write_run_locked_persists_agent_metering_json() -> None:
    """@brief PostgreSQL Run 写入必须保留 token/cost JSON 快照 / PostgreSQL Run writes must retain token/cost JSON snapshots.

    @return 无返回值 / No return value.

    @note 此处验证 ORM 写入边界而非 provider 账单准确性；完整公开投影由 Agent
    flow 测试覆盖。复制断言还防止未来把领域可变字典直接别名到 ORM 行。
    """
    run = _run()
    run.token_usage = {
        "input_tokens": 11,
        "output_tokens": 7,
        "total_tokens": 18,
        "estimated": True,
    }
    run.cost = {
        "currency": "USD",
        "unit": "microusd",
        "total_cost_microusd": 23,
        "estimated": True,
    }
    row = _run_row()

    await _repository_without_database()._write_run_locked(
        cast(Any, _FakeSession([])), _SCOPE, row, run
    )

    assert row.token_usage == run.token_usage
    assert row.cost == run.cost
    assert row.token_usage is not run.token_usage
    assert row.cost is not run.cost
