"""Live PostgreSQL integration coverage for Resume render-job links."""

from __future__ import annotations

import hashlib
import os
import secrets
from copy import deepcopy

import pytest
from sqlalchemy import delete

from backend.domain.common import Job, iso_timestamp, utc_now
from backend.domain.resume import ResumeRecord, create_empty_document
from backend.infrastructure.persistence.database import AsyncDatabase, AsyncDatabaseOptions
from backend.infrastructure.persistence.models import (
    JobRecord,
    ResumeDocumentRecord,
    ResumeRenderJobRecord,
    ResumeRevisionRecord,
    ResumeTemplateRecord,
    UserRecord,
    WorkspaceRecord,
)
from backend.infrastructure.persistence.repositories import scoped_select
from backend.infrastructure.persistence.runtime_repository import PostgresWorkspaceRepository
from workspace_shared.tenancy import ActorScope


def _postgres_test_dsn() -> str:
    """Return the explicitly authorized live-test DSN or skip this integration test."""
    dsn = os.environ.get("AIWS_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("AIWS_TEST_POSTGRES_DSN is required for live PostgreSQL integration tests")
    return dsn


@pytest.mark.asyncio
async def test_render_job_link_tracks_revision_profile_and_artifact() -> None:
    """A unified Job must have one revision-bound Resume render-job association."""
    suffix = secrets.token_hex(8)
    scope = ActorScope(
        actor_id=f"usr_render_it_{suffix}",
        workspace_id=f"ws_render_it_{suffix}",
        resource_owner_id=f"usr_render_it_{suffix}",
    )
    resume_id = f"res_render_it_{suffix}"
    job_id = f"job_render_it_{suffix}"
    artifact_id = f"art_render_it_{suffix}"
    database = AsyncDatabase(
        AsyncDatabaseOptions(
            dsn=_postgres_test_dsn(),
            pool_size=1,
            max_overflow=0,
        )
    )
    repository = PostgresWorkspaceRepository(database)
    document = create_empty_document(
        scope,
        resume_id,
        "PostgreSQL render link integration",
        "en-US",
        "tpl_render_it",
        "1.0",
        f"sec_render_it_{suffix}",
    )
    resume = ResumeRecord(
        scope=scope,
        document=document,
        revisions={1: deepcopy(document)},
    )
    job = Job(
        id=job_id,
        job_type="resume.render",
        created_at=utc_now(),
        request_id=f"req_render_it_{suffix}",
        extensions={
            "resume_id": resume_id,
            "resume_revision": 1,
            "render_profile": "final",
            "artifacts": [],
            "diagnostics": [],
        },
    )

    try:
        await repository.create_resume(scope, resume)
        await repository.create_job(scope, job)

        async with database.read_session(scope) as session:
            link_statement = scoped_select(ResumeRenderJobRecord, scope).where(
                ResumeRenderJobRecord.job_id == job_id
            )
            link = (await session.scalars(link_statement)).one()
            revision_statement = scoped_select(ResumeRevisionRecord, scope).where(
                ResumeRevisionRecord.resume_id == resume_id,
                ResumeRevisionRecord.revision_no == 1,
            )
            revision = (await session.scalars(revision_statement)).one()
            assert link.resume_id == resume_id
            assert link.resume_revision_id == revision.id
            assert link.render_profile == "final"
            assert link.artifact_id is None
            assert link.diagnostics == {"items": []}

        job.start()
        await repository.save_job(scope, job)
        pdf = b"%PDF-1.4\n% integration test\n"
        timestamp = iso_timestamp(utc_now())
        artifact = {
            "id": artifact_id,
            "created_at": timestamp,
            "updated_at": timestamp,
            "revision": 1,
            "resume_id": resume_id,
            "resume_revision": 1,
            "artifact_kind": "rendered_resume",
            "format": "pdf",
            "content_type": "application/pdf",
            "size_bytes": len(pdf),
            "sha256": hashlib.sha256(pdf).hexdigest(),
            "download_url": f"/api/v1/render-artifacts/{artifact_id}/content",
            "expires_at": None,
            "page_count": 1,
            "source_map_artifact_id": None,
            "extensions": {},
        }
        await repository.save_artifact(
            scope,
            artifact,
            pdf,
            {"artifact_id": artifact_id, "pages": []},
        )
        diagnostic = {"severity": "info", "message": "integration render completed"}
        job.extensions["artifacts"] = [artifact]
        job.extensions["diagnostics"] = [diagnostic]
        job.completed_units = 1
        job.total_units = 1
        job.succeed()
        await repository.save_job(scope, job)

        async with database.read_session(scope) as session:
            link_statement = scoped_select(ResumeRenderJobRecord, scope).where(
                ResumeRenderJobRecord.job_id == job_id
            )
            link = (await session.scalars(link_statement)).one()
            assert link.artifact_id == artifact_id
            assert link.diagnostics == {"items": [diagnostic]}
            assert link.revision == 3
    finally:
        async with database.transaction(scope) as session:
            await session.execute(delete(JobRecord).where(JobRecord.id == job_id))
            await session.execute(
                delete(ResumeDocumentRecord).where(ResumeDocumentRecord.id == resume_id)
            )
            await session.execute(
                delete(ResumeTemplateRecord).where(
                    ResumeTemplateRecord.workspace_id == scope.workspace_id
                )
            )
            await session.execute(
                delete(WorkspaceRecord).where(WorkspaceRecord.id == scope.workspace_id)
            )
            await session.execute(delete(UserRecord).where(UserRecord.id == scope.actor_id))
        await database.aclose()
