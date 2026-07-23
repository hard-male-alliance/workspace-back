"""@brief 已退休 PostgreSQL V1 持久化端口的 fail-closed 回归 / Fail-closed regressions for retired PostgreSQL V1 persistence ports."""

from __future__ import annotations

from collections.abc import Awaitable
from datetime import UTC, datetime
from typing import Never, cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from backend.domain.agent import AgentRunRecord, ConversationRecord, MessageRecord
from backend.domain.common import Job
from backend.domain.interview import InterviewSessionRecord
from backend.domain.proposal import ResumeProposalRecord
from backend.infrastructure.persistence.database import AsyncDatabase
from backend.infrastructure.persistence.runtime_repository import PostgresWorkspaceRepository
from workspace_shared.tenancy import ActorScope

_SCOPE = ActorScope("usr_retired", "ws_retired", "usr_retired")
"""@brief 不访问数据库的确定性租户范围 / Deterministic tenant scope that never reaches the database."""

_NOW = datetime(2026, 7, 23, tzinfo=UTC)
"""@brief 确定性测试时间 / Deterministic test time."""

_ERROR = "legacy PostgreSQL persistence surface is retired"
"""@brief 所有已退休端口共享的稳定错误前缀 / Stable error prefix shared by every retired port."""


class _ForbiddenDatabase:
    """@brief 任何数据库属性访问都会使测试失败 / Fail a test on any database attribute access."""

    def __getattr__(self, name: str) -> Never:
        """@brief 拒绝潜在数据库访问 / Reject a potential database access.

        @param name 被访问的属性 / Accessed attribute.
        @raise AssertionError fail-closed 边界触表时抛出。
        """
        raise AssertionError(f"retired persistence touched the database through {name}")


def _repository() -> PostgresWorkspaceRepository:
    """@brief 构造带数据库 tripwire 的旧 Repository / Build the legacy repository with a database tripwire.

    @return 任何 I/O 都会立即使测试失败的 Repository。
    """
    return PostgresWorkspaceRepository(cast(AsyncDatabase, _ForbiddenDatabase()))


async def _assert_retired(operation: Awaitable[object]) -> None:
    """@brief 断言操作以统一错误提前拒绝 / Assert that an operation rejects early with the unified error.

    @param operation 待执行的异步 Repository 操作 / Async repository operation to execute.
    """
    with pytest.raises(RuntimeError, match=_ERROR):
        await operation


@pytest.mark.asyncio
async def test_legacy_message_port_rejects_before_database_access() -> None:
    """@brief 可变 V1 Message 不得碰触 append-only V2 表 / Mutable V1 Messages must not touch the append-only V2 table."""
    repository = _repository()
    message = MessageRecord(
        id="msg_retired",
        conversation_id="conv_retired",
        created_at=_NOW,
        updated_at=_NOW,
        role="user",
        status="completed",
        content=[{"type": "text", "text": "retired"}],
    )

    await _assert_retired(repository.create_message(_SCOPE, message))
    await _assert_retired(repository.get_message(_SCOPE, message.id))
    await _assert_retired(repository.list_messages(_SCOPE, message.conversation_id))


@pytest.mark.asyncio
async def test_legacy_agent_port_rejects_before_database_or_outbox_access() -> None:
    """@brief 整个 V1 Agent 聚合不得污染 V2 表或统一 outbox / The whole V1 Agent aggregate must not pollute V2 tables or the unified outbox."""
    repository = _repository()
    conversation = ConversationRecord(
        scope=_SCOPE,
        id="conv_retired",
        created_at=_NOW,
        updated_at=_NOW,
        title="Retired",
        capability="general",
        context_refs=[],
    )
    run = AgentRunRecord(
        scope=_SCOPE,
        id="run_retired",
        conversation_id=conversation.id,
        input_message_id="msg_retired",
        created_at=_NOW,
        updated_at=_NOW,
        request={},
    )
    proposal = ResumeProposalRecord(
        scope=_SCOPE,
        id="prop_retired",
        created_at=_NOW,
        updated_at=_NOW,
        resume_id="res_retired",
        base_revision=1,
        source_run_id=run.id,
        title="Retired",
        summary="",
        operations=[],
    )
    await _assert_retired(repository.create_conversation(_SCOPE, conversation))
    await _assert_retired(repository.get_conversation(_SCOPE, conversation.id))
    await _assert_retired(repository.create_run(_SCOPE, run))
    await _assert_retired(repository.get_run(_SCOPE, run.id))
    await _assert_retired(repository.save_run(_SCOPE, run))
    await _assert_retired(repository.create_proposal(_SCOPE, proposal))
    await _assert_retired(repository.get_proposal(_SCOPE, proposal.id))
    await _assert_retired(repository.list_proposals(_SCOPE, proposal.resume_id))
    await _assert_retired(repository.save_proposal(_SCOPE, proposal))


@pytest.mark.asyncio
async def test_legacy_interview_port_rejects_before_database_access() -> None:
    """@brief V1 Interview 状态/事件不得投影到 V2 会话真相表 / V1 Interview state/events must not project into V2 truth tables."""
    repository = _repository()
    session = InterviewSessionRecord(
        scope=_SCOPE,
        id="int_retired",
        created_at=_NOW,
        updated_at=_NOW,
        request={},
    )
    report = {"id": "rpt_retired", "session_id": session.id}

    await _assert_retired(repository.create_session(_SCOPE, session))
    await _assert_retired(repository.get_session(_SCOPE, session.id))
    await _assert_retired(repository.list_sessions(_SCOPE))
    await _assert_retired(repository.save_session(_SCOPE, session))
    await _assert_retired(repository.save_report(_SCOPE, report))
    await _assert_retired(repository.get_report(_SCOPE, str(report["id"])))


@pytest.mark.asyncio
async def test_legacy_artifact_port_rejects_before_database_access() -> None:
    """@brief Resume 专用 V1 Artifact 不得写入统一 V2 Artifact 表 / Resume-specific V1 Artifacts must not write the unified V2 table."""
    repository = _repository()
    artifact = {
        "id": "art_retired",
        "resume_id": "res_retired",
        "resume_revision": 1,
    }
    job = Job(
        id="job_retired",
        job_type="resume.render",
        created_at=_NOW,
        request_id=None,
        extensions={"artifacts": [artifact]},
    )

    await _assert_retired(repository.save_artifact(_SCOPE, artifact, b"pdf", None))
    await _assert_retired(
        repository.save_artifact_and_job(_SCOPE, artifact, b"pdf", None, job)
    )
    await _assert_retired(repository.get_artifact(_SCOPE, str(artifact["id"])))
    await _assert_retired(repository.list_artifacts(_SCOPE, str(artifact["resume_id"])))
    await _assert_retired(
        repository._render_job_artifact_id(
            cast(AsyncSession, _ForbiddenDatabase()),
            _SCOPE,
            job,
            str(artifact["resume_id"]),
            "rrev_retired",
        )
    )
