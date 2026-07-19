"""Live PostgreSQL coverage for version persistence and pgvector ranking."""

from __future__ import annotations

import os
import secrets

import pytest
from sqlalchemy import delete

from backend.domain.common import utc_now
from backend.domain.knowledge import (
    EmbeddingSpace,
    KnowledgeChunk,
    KnowledgeClassification,
    KnowledgeSourceRecord,
)
from backend.infrastructure.persistence.database import AsyncDatabase, AsyncDatabaseOptions
from backend.infrastructure.persistence.models import (
    EmbeddingSpaceRecord,
    KnowledgeSourceVersionRecord,
    UserRecord,
    WorkspaceRecord,
)
from backend.infrastructure.persistence.models import (
    KnowledgeSourceRecord as KnowledgeSourceOrmRecord,
)
from backend.infrastructure.persistence.repositories import scoped_select
from backend.infrastructure.persistence.runtime_repository import PostgresWorkspaceRepository
from workspace_shared.tenancy import ActorScope


def _postgres_test_dsn() -> str:
    dsn = os.environ.get("AIWS_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("AIWS_TEST_POSTGRES_DSN is required for live PostgreSQL integration tests")
    return dsn


@pytest.mark.asyncio
async def test_source_version_and_pgvector_rank_are_persisted() -> None:
    suffix = secrets.token_hex(8)
    scope = ActorScope(
        actor_id=f"usr_knowledge_it_{suffix}",
        workspace_id=f"ws_knowledge_it_{suffix}",
        resource_owner_id=f"usr_knowledge_it_{suffix}",
    )
    source_id = f"src_knowledge_it_{suffix}"
    version_id = f"srcver_knowledge_it_{suffix}"
    space_id = f"embsp_knowledge_it_{suffix}"
    chunk_positive = f"chunk_positive_{suffix}"
    chunk_negative = f"chunk_negative_{suffix}"
    database = AsyncDatabase(
        AsyncDatabaseOptions(dsn=_postgres_test_dsn(), pool_size=1, max_overflow=0)
    )
    repository = PostgresWorkspaceRepository(database)
    now = utc_now()
    space = EmbeddingSpace(
        id=space_id,
        provider="deterministic",
        model="integration-test",
        model_revision="1",
        dimension=1024,
        distance_metric="cosine",
        normalization="l2",
        created_at=now,
    )
    positive = (1.0, *([0.0] * 1023))
    negative = (-1.0, *([0.0] * 1023))
    source = KnowledgeSourceRecord(
        scope=scope,
        id=source_id,
        created_at=now,
        updated_at=now,
        name="pgvector integration evidence",
        source_type="file",
        config={
            "source_type": "file",
            "file_id": f"file_{suffix}",
            "filename": "evidence.txt",
            "content_type": "text/plain",
            "sha256": "a" * 64,
        },
        visibility={
            "policy_version": 1,
            "default_effect": "deny",
            "agent_grants": [],
            "allow_external_model_processing": False,
        },
        ingestion_status="ready",
        source_version_id=version_id,
        classification=KnowledgeClassification(),
        private_metadata={"storage_key": "private-test-key"},
        source_metadata={"parser": {"parser": "plain_text"}},
        chunks=[
            KnowledgeChunk(
                chunk_positive,
                source_id,
                version_id,
                space_id,
                0,
                "positive vector",
                positive,
            ),
            KnowledgeChunk(
                chunk_negative,
                source_id,
                version_id,
                space_id,
                1,
                "negative vector",
                negative,
            ),
        ],
    )

    try:
        await repository.save_embedding_space(scope, space)
        await repository.create_source(scope, source)
        restored = await repository.get_source(scope, source_id)
        assert restored is not None
        assert restored.source_version_id == version_id
        assert restored.private_metadata == {"storage_key": "private-test-key"}
        assert [chunk.id for chunk in restored.chunks] == [chunk_positive, chunk_negative]
        async with database.read_session(scope) as session:
            version = (
                await session.scalars(
                    scoped_select(KnowledgeSourceVersionRecord, scope).where(
                        KnowledgeSourceVersionRecord.id == version_id
                    )
                )
            ).one()
            assert version.extensions["runtime"]["private_metadata"] == {
                "storage_key": "private-test-key"
            }

        ranked = await repository.rank_chunks_by_vector(
            scope,
            [chunk_negative, chunk_positive],
            space_id,
            positive,
            2,
        )
        assert [chunk_id for chunk_id, _ in ranked] == [chunk_positive, chunk_negative]
        assert ranked[0][1] > ranked[1][1]
    finally:
        async with database.transaction(scope) as session:
            await session.execute(
                delete(KnowledgeSourceOrmRecord).where(
                    KnowledgeSourceOrmRecord.id == source_id
                )
            )
            await session.execute(
                delete(EmbeddingSpaceRecord).where(EmbeddingSpaceRecord.id == space_id)
            )
            await session.execute(
                delete(WorkspaceRecord).where(WorkspaceRecord.id == scope.workspace_id)
            )
            await session.execute(delete(UserRecord).where(UserRecord.id == scope.actor_id))
        await database.aclose()
