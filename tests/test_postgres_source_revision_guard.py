"""@brief PostgreSQL 知识来源单调 revision 写入的无数据库测试 / Database-free tests for monotonic PostgreSQL knowledge-source revisions."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from backend.domain.knowledge import KnowledgeSourceRecord
from backend.infrastructure.persistence.runtime_repository import PostgresWorkspaceRepository
from workspace_shared.tenancy import ActorScope

_SCOPE = ActorScope("usr_klee", "ws_source_revision", "usr_klee")
"""@brief 测试使用的完整租户范围 / Complete tenant scope used by this test."""

_NOW = datetime(2026, 7, 15, tzinfo=UTC)
"""@brief 可重复测试时间 / Deterministic test timestamp."""


def _source(*, revision: int) -> KnowledgeSourceRecord:
    """@brief 构造最小可保存的 resume 知识来源 / Construct a minimal persistable resume knowledge source.

    @param revision 候选来源 revision / Candidate source revision.
    @return 供 PostgreSQL 写入边界消费的领域聚合 / Domain aggregate consumed by the PostgreSQL write boundary.
    """
    return KnowledgeSourceRecord(
        scope=_SCOPE,
        id="src_revision_guard",
        created_at=_NOW,
        updated_at=_NOW,
        name="Resume: Klee",
        source_type="resume",
        config={
            "source_type": "resume",
            "resume_id": "res_revision_guard",
            "revision_mode": "latest",
        },
        visibility={
            "policy_version": 1,
            "default_effect": "deny",
            "sensitivity": "confidential",
            "agent_grants": [],
            "session_override_allowed": True,
            "allow_external_model_processing": False,
            "allowed_model_regions": [],
            "retention_days": None,
        },
        revision=revision,
        mock_content="Klee Rust",
    )


def _source_row(*, revision: int) -> SimpleNamespace:
    """@brief 构造最小锁定 KnowledgeSource ORM 行 / Construct a minimal locked KnowledgeSource ORM row.

    @param revision 已持久化来源 revision / Persisted source revision.
    @return 能被 ``_write_source_locked`` 修改的 fake ORM 行 / Fake ORM row mutable by ``_write_source_locked``.
    """
    return SimpleNamespace(
        source_type="resume",
        title="Resume: old",
        config={"source_type": "resume", "resume_id": "res_revision_guard", "revision_mode": "latest"},
        ingestion_state="ready",
        updated_at=_NOW,
        revision=revision,
        extensions={},
    )


class _SourceWriteRepository(PostgresWorkspaceRepository):
    """@brief 跳过无关子表写入的来源写入测试替身 / Source-write test double skipping unrelated child-table writes."""

    async def _write_visibility_policy(
        self,
        session: AsyncSession,
        scope: ActorScope,
        record: KnowledgeSourceRecord,
    ) -> None:
        """@brief 忽略单调 revision 断言外的可见性子表 / Ignore visibility child tables outside the monotonic-revision assertion.

        @param session fake SQLAlchemy session / Fake SQLAlchemy session.
        @param scope tenant scope / Tenant scope.
        @param record source aggregate / Source aggregate.
        @return 无返回值 / No return value.
        """
        del session, scope, record


def _repository_without_database() -> _SourceWriteRepository:
    """@brief 构造只覆盖来源写入私有逻辑的 Repository / Construct a repository covering only private source-write logic.

    @return 不会初始化 AsyncDatabase 的测试 Repository / Test repository without initializing AsyncDatabase.
    """
    return object.__new__(_SourceWriteRepository)


@pytest.mark.asyncio
async def test_stale_source_revision_cannot_overwrite_a_newer_locked_row() -> None:
    """@brief 严格较旧的来源 revision 必须在写入前失败 / A strictly older source revision must fail before any write.

    @return 无返回值 / No return value.
    """
    row = _source_row(revision=7)
    source = _source(revision=6)

    with pytest.raises(RuntimeError, match="stale knowledge source write"):
        await _repository_without_database()._write_source_locked(
            cast(Any, object()), _SCOPE, row, source
        )

    assert row.revision == 7
    assert row.title == "Resume: old"


@pytest.mark.asyncio
async def test_same_source_revision_remains_idempotently_writable() -> None:
    """@brief 同 revision 重试仍可进入后续幂等写入逻辑 / A same-revision retry remains able to enter subsequent idempotent write logic.

    @return 无返回值 / No return value.
    """
    row = _source_row(revision=7)
    source = _source(revision=7)

    await _repository_without_database()._write_source_locked(
        cast(Any, object()), _SCOPE, row, source
    )

    assert row.revision == 7
    assert row.title == "Resume: Klee"
    assert row.ingestion_state == "new"
