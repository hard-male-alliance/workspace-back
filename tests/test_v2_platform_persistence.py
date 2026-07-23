"""@brief API V2 Platform persistence、migration 与事务门禁 / API V2 Platform persistence, migration, and transaction gates."""

from __future__ import annotations

import asyncio
import getpass
import shutil
import socket
import subprocess
from collections.abc import AsyncGenerator, AsyncIterator, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

import asyncpg  # type: ignore[import-untyped]
import psycopg
import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from psycopg.rows import dict_row

from backend.application.platform import PlatformApplicationService
from backend.application.ports.platform import (
    ByteRangeRequest,
    MutationContext,
    PlatformAuthorizationRequest,
    PlatformPermission,
    PlatformResourceTarget,
    PlatformTargetKind,
)
from backend.application.ports.v2_idempotency import (
    IdempotencyPreparationId,
    IdempotencyRequest,
    IdempotencyScope,
    ReplayableResponse,
)
from backend.domain.identity import IdentityUserRecord
from backend.domain.platform import (
    ApiArtifactContentUrl,
    ApiEvent,
    Artifact,
    ArtifactId,
    ArtifactKind,
    Job,
    JobId,
    JobStatus,
    PdfRect,
    PdfSourceMap,
    PdfSourceNode,
    ResourceRef,
)
from backend.domain.principals import (
    ClientId,
    MembershipId,
    ResourceMeta,
    Scope,
    Subject,
    TokenPrincipal,
    UserId,
    WorkspaceId,
)
from backend.domain.users import User
from backend.domain.workspaces import (
    DataRegion,
    Membership,
    MemberStatus,
    Workspace,
    WorkspacePlan,
    WorkspaceRole,
)
from backend.infrastructure.access import InMemoryAccessStore
from backend.infrastructure.hosted_identity import PostgresHostedIdentityRepository
from backend.infrastructure.persistence.database import (
    AsyncDatabase,
    AsyncDatabaseOptions,
)
from backend.infrastructure.persistence.models import (
    ArtifactContentRecord,
    ArtifactPdfSourceMapRecord,
    ArtifactRecord,
    AuditEventRecord,
    Base,
    JobRecord,
    OutboxEventRecord,
    ResumeRenderJobRecord,
    WorkspaceEventSequenceRecord,
)
from backend.infrastructure.platform import (
    InMemoryPlatformStore,
    InMemoryPlatformUnitOfWorkFactory,
    PostgresPlatformUnitOfWorkFactory,
)
from backend.infrastructure.v2_idempotency import AtomicPostgresIdempotencyExecutor

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 项目根目录 / Project root."""

MIGRATION = PROJECT_ROOT / "alembic" / "versions" / "20260723_0017_v2_platform_persistence.py"
"""@brief Platform V2 persistence migration / Platform V2 persistence migration."""

NOW = datetime(2026, 7, 23, 12, tzinfo=UTC)
"""@brief 固定测试时刻 / Fixed test instant."""

USER_ID = UserId("user_00000001")
"""@brief 测试用户 / Test user."""

WORKSPACE_ID = WorkspaceId("workspace_00000001")
"""@brief 测试 Workspace / Test Workspace."""

JOB_ID = JobId("job_00000001")
"""@brief 测试 Job / Test Job."""

ARTIFACT_ID = ArtifactId("artifact_00000001")
"""@brief 测试 Artifact / Test Artifact."""

CONTENT = b"platform-artifact-content"
"""@brief 测试 Artifact bytes / Test Artifact bytes."""


class _Clock:
    """@brief 固定应用与 replay 时钟 / Fixed application and replay clock."""

    def now(self) -> datetime:
        """@brief 返回固定时刻后一秒 / Return one second after the fixed instant."""
        return NOW + timedelta(seconds=1)


def _principal() -> TokenPrincipal:
    """@brief 构造 Platform 与 Resume cancellation scopes / Build Platform and Resume cancellation scopes."""
    return TokenPrincipal(
        USER_ID,
        Subject("subject_00000001"),
        ClientId("client_00000001"),
        frozenset(
            {
                Scope("workspace.read"),
                Scope("workspace.write"),
                Scope("resume.write"),
                Scope("resume.render"),
            }
        ),
    )


def _job() -> Job:
    """@brief 构造 queued Resume Job / Build a queued Resume Job."""
    return Job(
        ResourceMeta(JOB_ID, 1, NOW, NOW),
        WORKSPACE_ID,
        "resume.render",
        ResourceRef("resume", "resume_00000001", 7),
    )


def _artifact() -> Artifact:
    """@brief 构造同源 Resume PDF Artifact / Build a same-origin Resume PDF Artifact."""
    return Artifact(
        ResourceMeta(ARTIFACT_ID, 1, NOW, NOW),
        WORKSPACE_ID,
        ArtifactKind.RESUME_PDF,
        ResourceRef("resume", "resume_00000001", 7),
        "application/pdf",
        len(CONTENT),
        sha256(CONTENT).hexdigest(),
        ApiArtifactContentUrl.build(
            "https://api.hmalliances.org:8022",
            WORKSPACE_ID,
            ARTIFACT_ID,
        ),
        page_count=1,
        expires_at=NOW + timedelta(hours=1),
    )


def _source_map() -> PdfSourceMap:
    """@brief 构造可交叉验证 source map / Build a cross-verifiable source map."""
    return PdfSourceMap(
        ARTIFACT_ID,
        "resume_00000001",
        7,
        (
            PdfSourceNode(
                "entity_00000001",
                ("title",),
                1,
                (PdfRect(1.0, 2.0, 3.0, 4.0),),
            ),
        ),
    )


def _fixture() -> tuple[
    PlatformApplicationService,
    InMemoryPlatformUnitOfWorkFactory,
]:
    """@brief 组装集中授权与共享状态的内存 Platform slice / Assemble an authorized in-memory Platform slice."""
    access = InMemoryAccessStore()
    access.users[str(USER_ID)] = User(
        ResourceMeta(USER_ID, 1, NOW, NOW),
        Subject("subject_00000001"),
        "klee@example.com",
        True,
        "Klee",
        "zh-CN",
        WORKSPACE_ID,
    )
    access.workspaces[str(WORKSPACE_ID)] = Workspace(
        ResourceMeta(WORKSPACE_ID, 1, NOW, NOW),
        "Klee Lab",
        "klee-lab",
        WorkspacePlan.TEAM,
        DataRegion.CN,
    )
    membership = Membership(
        ResourceMeta(MembershipId("membership_00000001"), 1, NOW, NOW),
        WORKSPACE_ID,
        USER_ID,
        "Klee",
        WorkspaceRole.ADMIN,
        MemberStatus.ACTIVE,
    )
    access.memberships[str(membership.meta.id)] = membership
    store = InMemoryPlatformStore()
    store.seed_job(_job())
    store.seed_artifact(_artifact(), CONTENT, source_map=_source_map())
    factory = InMemoryPlatformUnitOfWorkFactory(
        access,
        store=store,
        clock=_Clock(),
    )
    service = PlatformApplicationService(
        factory,
        factory.content_store,
        factory.event_feed,
        clock=_Clock(),
    )
    return service, factory


async def _read(chunks: AsyncIterator[bytes]) -> bytes:
    """@brief 消费 Artifact async chunks / Consume Artifact async chunks."""
    return b"".join([chunk async for chunk in chunks])


async def test_memory_platform_cancel_commits_job_outbox_audit_and_live_event() -> None:
    """@brief cancellation 在一个 commit 中发布 Job、outbox、audit 与 live event / Cancellation publishes all state in one commit."""
    service, factory = _fixture()
    async with asyncio.timeout(1):
        stream = cast(
            AsyncGenerator[ApiEvent],
            await service.open_event_stream(_principal(), WORKSPACE_ID),
        )
        cancelled = await service.cancel_job(
            _principal(),
            WORKSPACE_ID,
            JOB_ID,
            MutationContext(
                "request_00000001",
                "0123456789abcdef0123456789abcdef",
            ),
        )
    event = await asyncio.wait_for(anext(stream), timeout=1)
    await stream.aclose()

    assert cancelled.status is JobStatus.CANCELLED
    assert cancelled.meta.revision == 2
    assert event.sequence == 1
    assert event.subject == ResourceRef("job", JOB_ID, 2)
    assert event.data["status"] == "cancelled"
    audits = await service.list_audit_events(_principal(), WORKSPACE_ID)
    assert len(audits.items) == 1
    assert audits.items[0].action == "job.cancel"
    assert factory.store.last_sequences[WORKSPACE_ID] == 1


async def test_memory_platform_uncommitted_cancel_rolls_back_every_projection() -> None:
    """@brief 未 commit cancellation 不泄漏 Job、event 或 audit / An uncommitted cancellation leaks no Job, event, or audit state."""
    _, factory = _fixture()
    target = PlatformResourceTarget(PlatformTargetKind.JOB, JOB_ID)
    async with factory() as uow:
        actor = await uow.authorizer.authenticate(_principal())
        access = await uow.authorizer.authorize(
            actor,
            WORKSPACE_ID,
            PlatformAuthorizationRequest(PlatformPermission.CANCEL_JOB, target),
        )
        before = await uow.repository.get_job(access, JOB_ID, for_update=True)
        assert before is not None
        after = before.cancel(at=NOW + timedelta(seconds=1))
        await uow.repository.save_job(access, after, expected_revision=1)
        await uow.journal.job_cancelled(
            access,
            before,
            after,
            MutationContext("request_00000001"),
        )
        # Deliberately do not commit.

    assert factory.store.jobs[JOB_ID].status is JobStatus.QUEUED
    assert factory.store.audit_events == {}
    assert factory.store.events == {}
    assert factory.store.last_sequences == {}


async def test_memory_artifact_content_range_digest_and_source_map_rehydrate() -> None:
    """@brief Artifact 内容支持 Range，metadata/SHA 与 source map 一致 / Artifact content supports Range with coherent metadata and source map."""
    service, _ = _fixture()
    full = await service.open_artifact_content(_principal(), WORKSPACE_ID, ARTIFACT_ID)
    assert await _read(full.chunks) == CONTENT
    partial = await service.open_artifact_content(
        _principal(),
        WORKSPACE_ID,
        ARTIFACT_ID,
        byte_range=ByteRangeRequest(first=2, last_inclusive=8),
    )
    assert await _read(partial.chunks) == CONTENT[2:9]
    source_map = await service.get_pdf_source_map(_principal(), WORKSPACE_ID, ARTIFACT_ID)
    assert source_map == _source_map()


def test_0017_is_linear_reuses_truth_tables_and_converts_legacy_artifacts() -> None:
    """@brief 0017 线性原位复用并在 retire 前转换 Artifact / 0017 is linear and converts before retirement."""
    configuration = Config()
    configuration.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))
    scripts = ScriptDirectory.from_config(configuration)
    script = scripts.get_revision("20260723_0017")
    assert script is not None
    assert script.down_revision == "20260723_0016"
    assert len(scripts.get_heads()) == 1
    assert len(script.nextrev) <= 1

    source = MIGRATION.read_text(encoding="utf-8")
    assert "def _migrate_legacy_artifacts()" in source
    assert source.index("_migrate_legacy_artifacts()") < source.index(
        "_retire_legacy_artifact_tables()"
    )
    assert "INSERT INTO agent.artifacts" in source
    assert "INSERT INTO agent.artifact_contents" in source
    assert "INSERT INTO agent.artifact_pdf_source_maps" in source
    assert "legacy_resource_owner_id" in source
    assert "encode(sha256(blob.content), 'hex')" in source
    assert "WHERE blob.id IS NULL" in source
    assert "def _assert_active_artifact_contents()" in source
    assert "cannot downgrade non-empty API V2 Platform persistence state" in source
    upgrade_source = source[
        source.index("def upgrade()") : source.index("def _preflight_downgrade()")
    ]
    assert upgrade_source.index("_preflight_upgrade()") < upgrade_source.index("_evolve_jobs()")
    assert upgrade_source.index("_assert_active_artifact_contents()") < upgrade_source.index(
        "_retire_legacy_artifact_tables()"
    )
    for truth in ("jobs", "outbox_events", "audit_events"):
        assert f'op.create_table(\n        "{truth}"' not in source
    for unified in (
        "workspace_event_sequences",
        "artifacts",
        "artifact_contents",
        "artifact_pdf_source_maps",
    ):
        assert f'"{unified}"' in source
    assert 'op.drop_table("render_artifacts"' in source
    assert 'op.drop_table("recording_artifacts"' in source


def test_0017_source_gates_transactional_sequence_rls_append_only_and_atomic_envelope() -> None:
    """@brief source gates 固定 transaction counter、RLS、append-only 与 new_session / Source gates pin sequence, RLS, append-only, and new_session."""
    migration = MIGRATION.read_text(encoding="utf-8")
    infrastructure = (
        PROJECT_ROOT / "src" / "backend" / "infrastructure" / "platform.py"
    ).read_text(encoding="utf-8")

    assert "ON CONFLICT (workspace_id) DO UPDATE" in migration
    assert "last_sequence + 1" in migration
    assert "BEFORE INSERT ON agent.outbox_events" in migration
    assert "NEW.sequence" in migration
    assert "NEW.replay_expires_at" in migration
    assert "FORCE ROW LEVEL SECURITY" in migration
    assert "GRANT SELECT, INSERT ON TABLE" in migration
    assert "GRANT UPDATE (status, published_at, attempt_count, updated_at)" in migration
    assert 'table = "identity.audit_events"' in migration
    assert 'f"GRANT SELECT, INSERT ON TABLE {table} TO {app_role}"' in migration
    assert "GRANT UPDATE, DELETE ON TABLE identity.audit_events" not in migration
    assert "self._database.new_session()" in infrastructure
    assert "self._database.session_factory()" not in infrastructure
    assert "_affected_rows(result) != 1" in infrastructure
    assert ".with_for_update()" in infrastructure
    assert "append_workspace_outbox_event" in infrastructure


def test_platform_orm_metadata_matches_0017_and_has_one_artifact_truth() -> None:
    """@brief ORM metadata 精确表达 0017 且仅保留一个 Artifact metadata truth / ORM metadata matches 0017 with one Artifact truth."""
    assert {
        "target_resource_revision",
        "progress_unit",
        "result_refs",
        "problem",
    } <= set(JobRecord.__table__.c.keys())
    assert {"subject_revision", "replay_expires_at"} <= set(OutboxEventRecord.__table__.c.keys())
    assert {"actor_type", "actor_revision", "resource_revision"} <= set(
        AuditEventRecord.__table__.c.keys()
    )
    assert cast(sa.String, JobRecord.__table__.c.job_type.type).length == 101
    assert cast(sa.String, ArtifactRecord.__table__.c.id.type).length == 160
    assert ResumeRenderJobRecord.__table__.c.artifact_id.references(ArtifactRecord.__table__.c.id)
    assert "agent.workspace_event_sequences" in Base.metadata.tables
    assert "agent.artifacts" in Base.metadata.tables
    assert "agent.artifact_contents" in Base.metadata.tables
    assert "agent.artifact_pdf_source_maps" in Base.metadata.tables
    for legacy in (
        "resume.render_artifacts",
        "resume.artifact_blobs",
        "resume.pdf_source_map_entries",
        "interview.recording_artifacts",
    ):
        assert legacy not in Base.metadata.tables

    outbox_table = cast(sa.Table, OutboxEventRecord.__table__)
    artifact_table = cast(sa.Table, ArtifactRecord.__table__)
    outbox_constraints = {item.name for item in outbox_table.constraints}
    assert "outbox_events_workspace_sequence" in outbox_constraints
    artifact_constraints = {item.name for item in artifact_table.constraints}
    assert "uq_artifacts_id_workspace" in artifact_constraints
    assert ArtifactContentRecord.__table__.c.artifact_id.primary_key
    assert ArtifactPdfSourceMapRecord.__table__.c.artifact_id.primary_key
    assert WorkspaceEventSequenceRecord.__table__.c.workspace_id.primary_key


_LEGACY_PLATFORM_FIXTURE_SQL = r"""
INSERT INTO identity.users (
    id, external_subject, display_name, email, email_verified, email_canonical,
    locale, account_status, created_at, updated_at, revision, extensions
) VALUES (
    'user_legacy0001', 'legacy-subject', 'Legacy Owner', 'legacy@example.com',
    true, 'legacy@example.com', 'en', 'active', '2026-07-01T00:00:00Z',
    '2026-07-02T00:00:00Z', 3, '{"legacy_user":true}'
);
INSERT INTO identity.workspaces (
    id, resource_owner_id, name, default_locale, slug, plan, data_region,
    created_at, updated_at, revision, extensions
) VALUES (
    'workspace_legacy0001', 'user_legacy0001', 'Legacy Workspace', 'en',
    'legacy-workspace', 'team', 'global', '2026-07-01T00:00:00Z',
    '2026-07-02T00:00:00Z', 2, '{"legacy_workspace":true}'
);
INSERT INTO identity.workspace_members (
    id, workspace_id, resource_owner_id, user_id, display_name, role, status,
    joined_at, created_at, updated_at, revision, extensions
) VALUES (
    'membership_legacy01', 'workspace_legacy0001', 'user_legacy0001',
    'user_legacy0001', 'Legacy Owner', 'owner', 'active',
    '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z',
    '2026-07-02T00:00:00Z', 1, '{}'
);
INSERT INTO resume.documents (
    id, workspace_id, resource_owner_id, title, locale, current_revision_no,
    template_id, template_version, created_at, updated_at, revision, extensions
) VALUES (
    'resume_legacy0001', 'workspace_legacy0001', 'user_legacy0001',
    'Legacy Resume', 'en', 7, 'template_legacy01', 'v1',
    '2026-07-03T00:00:00Z', '2026-07-04T00:00:00Z', 7,
    '{"legacy_document":true}'
);
INSERT INTO resume.revisions (
    id, workspace_id, resource_owner_id, resume_id, revision_no,
    semantic_document, content_hash, source, created_at, updated_at,
    revision, extensions, change_targets
) VALUES (
    'revision_legacy0007', 'workspace_legacy0001', 'user_legacy0001',
    'resume_legacy0001', 7, '{"id":"resume_legacy0001","revision":7}',
    repeat('a', 64), 'user', '2026-07-04T00:00:00Z',
    '2026-07-04T00:00:00Z', 1, '{"legacy_revision":true}', '[]'
);
INSERT INTO resume.render_artifacts (
    id, workspace_id, resource_owner_id, resume_id, resume_revision_id,
    artifact_kind, format, storage_key, content_sha256, content_bytes,
    expires_at, created_at, updated_at, revision, extensions
) VALUES
(
    'artifact_legacypdf1', 'workspace_legacy0001', 'user_legacy0001',
    'resume_legacy0001', 'revision_legacy0007', 'rendered_resume', 'pdf',
    'database://resume-artifacts/artifact_legacypdf1',
    encode(sha256(convert_to('%PDF-legacy-platform', 'UTF8')), 'hex'),
    octet_length(convert_to('%PDF-legacy-platform', 'UTF8')),
    '2026-08-03T00:00:00Z', '2026-07-05T00:00:00Z',
    '2026-07-06T00:00:00Z', 4,
    '{"runtime":{"public_metadata":{"page_count":2}},"legacy_metadata":"keep"}'
),
(
    'artifact_external01', 'workspace_legacy0001', 'user_legacy0001',
    'resume_legacy0001', 'revision_legacy0007', 'rendered_resume', 'json',
    's3://legacy-bucket/resume.json',
    encode(sha256(convert_to('{"legacy":true}', 'UTF8')), 'hex'),
    octet_length(convert_to('{"legacy":true}', 'UTF8')), NULL,
    '2026-07-05T01:00:00Z', '2026-07-06T01:00:00Z', 2,
    '{"external_provider":"s3"}'
);
INSERT INTO resume.artifact_blobs (
    id, workspace_id, resource_owner_id, artifact_id, content, source_map,
    created_at, updated_at, revision, extensions
) VALUES (
    'artifactblob_legacy1', 'workspace_legacy0001', 'user_legacy0001',
    'artifact_legacypdf1', convert_to('%PDF-legacy-platform', 'UTF8'),
    '{"schema_version":"1.0","resume_id":"resume_legacy0001",'
    '"resume_revision":7,"artifact_id":"artifact_legacypdf1",'
    '"page_count":2,"nodes":[{"node_kind":"section",'
    '"node_id":"entity_legacy0001","field_path":["title"],"page":1, '
    '"rects":[{"x":1.0,"y":2.0,"width":3.0,"height":4.0,"unit":"pt"}]}]}',
    '2026-07-05T00:00:01Z', '2026-07-06T00:00:01Z', 5,
    '{"blob_extension":"keep"}'
), (
    'artifactblob_external1', 'workspace_legacy0001', 'user_legacy0001',
    'artifact_external01', convert_to('{"legacy":true}', 'UTF8'), NULL,
    '2026-07-05T01:00:01Z', '2026-07-06T01:00:01Z', 3,
    '{"blob_extension":"external-key-inline-content"}'
);
INSERT INTO resume.pdf_source_map_entries (
    id, workspace_id, resource_owner_id, artifact_id, node_kind, node_id,
    field_path, page, rects, created_at, updated_at, revision, extensions
) VALUES (
    'sourcemapentry_0001', 'workspace_legacy0001', 'user_legacy0001',
    'artifact_legacypdf1', 'section', 'entity_legacy0001', ARRAY['title'], 1,
    '[{"x":1.0,"y":2.0,"width":3.0,"height":4.0,"unit":"pt"}]',
    '2026-07-05T00:00:02Z', '2026-07-06T00:00:02Z', 2,
    '{"entry_extension":"keep"}'
);
INSERT INTO agent.jobs (
    id, workspace_id, resource_owner_id, job_type, status, phase,
    completed_units, total_units, request_id, target_resource_type,
    target_resource_id, created_at, updated_at, revision, extensions,
    request_payload
) VALUES (
    'job_legacyrender1', 'workspace_legacy0001', 'user_legacy0001',
    'resume.render', 'queued', 'queued', 0, 1, 'request_legacy0001',
    'resume', 'resume_legacy0001', '2026-07-05T00:00:00Z',
    '2026-07-05T00:00:00Z', 1, '{"legacy_job":true}', '{}'
);
INSERT INTO resume.render_jobs (
    id, workspace_id, resource_owner_id, job_id, resume_id,
    resume_revision_id, artifact_id, render_profile, created_at,
    updated_at, revision, extensions
) VALUES (
    'renderjob_legacy01', 'workspace_legacy0001', 'user_legacy0001',
    'job_legacyrender1', 'resume_legacy0001', 'revision_legacy0007',
    'artifact_legacypdf1', 'production', '2026-07-05T00:00:00Z',
    '2026-07-06T00:00:00Z', 2, '{"render_job_extension":true}'
);
INSERT INTO interview.scenarios (
    id, workspace_id, resource_owner_id, title, locale, role_target,
    rubric, created_at, updated_at, revision, extensions
) VALUES (
    'scenario_legacy001', 'workspace_legacy0001', 'user_legacy0001',
    'Legacy Scenario', 'en', '{}', '{}', '2026-07-07T00:00:00Z',
    '2026-07-07T00:00:00Z', 1, '{"legacy_scenario":true}'
);
INSERT INTO interview.sessions (
    id, workspace_id, resource_owner_id, scenario_id, state, job_target,
    effective_knowledge_selection, inference_intent, media_capabilities,
    avatar_output_mode, consent, created_at, updated_at, revision, extensions
) VALUES (
    'session_legacy0001', 'workspace_legacy0001', 'user_legacy0001',
    'scenario_legacy001', 'completed', '{}', '{}', '{}', '{}', 'none', '{}',
    '2026-07-07T00:00:00Z', '2026-07-08T00:00:00Z', 3,
    '{"legacy_session":true}'
);
INSERT INTO interview.recording_artifacts (
    id, workspace_id, resource_owner_id, session_id, storage_key,
    content_sha256, content_bytes, media_kind, expires_at, deleted_at,
    created_at, updated_at, revision, extensions
) VALUES (
    'recording_legacy001', 'workspace_legacy0001', 'user_legacy0001',
    'session_legacy0001', 's3://legacy-bucket/interview.wav', repeat('c', 64),
    9876, 'audio/wav', '2026-09-01T00:00:00Z', '2026-07-10T00:00:00Z',
    '2026-07-07T00:00:00Z', '2026-07-10T00:00:00Z', 6,
    '{"recording_extension":"keep"}'
);
"""
"""@brief 在 0016 状态灌入的可证明 legacy Platform 数据 / Provable legacy Platform fixture seeded at 0016."""


@dataclass(frozen=True, slots=True)
class _PostgresHarness:
    """@brief 隔离 PostgreSQL 迁移测试环境 / Isolated PostgreSQL migration-test harness."""

    port: int
    socket_dir: Path
    superuser: str

    @property
    def migration_dsn(self) -> str:
        """@brief 返回 migrator asyncpg DSN / Return the migrator asyncpg DSN."""
        return self.migration_dsn_for("aiws")

    def migration_dsn_for(self, database: str) -> str:
        """@brief 返回指定数据库的 migrator DSN / Return a migrator DSN for one database.

        @param database 已验证测试数据库名 / Validated test database name.
        @return Alembic asyncpg DSN / Alembic asyncpg DSN.
        """
        if not database.replace("_", "").isalnum():
            raise ValueError("test database name must be alphanumeric with underscores")
        return f"postgresql+asyncpg://aiws_migrator@127.0.0.1:{self.port}/{database}"

    @property
    def app_dsn(self) -> str:
        """@brief 返回 app asyncpg DSN / Return the application asyncpg DSN."""
        return f"postgresql://aiws_app@127.0.0.1:{self.port}/aiws"

    @property
    def super_dsn(self) -> str:
        """@brief 返回超级用户 psycopg DSN / Return the superuser psycopg DSN."""
        return self.super_dsn_for("aiws")

    def super_dsn_for(self, database: str) -> str:
        """@brief 返回指定数据库的超级用户 DSN / Return a superuser DSN for one database."""
        return f"postgresql://{self.superuser}@127.0.0.1:{self.port}/{database}"

    def psql(self, binary: Path, sql: str, *, database: str = "aiws") -> None:
        """@brief 通过 psql 执行无参数 fixture SQL / Execute parameter-free fixture SQL through psql."""
        subprocess.run(
            [
                str(binary),
                "-h",
                str(self.socket_dir),
                "-p",
                str(self.port),
                "-d",
                database,
                "-v",
                "ON_ERROR_STOP=1",
            ],
            input=sql,
            text=True,
            check=True,
            capture_output=True,
        )

    def rows(
        self,
        statement: str,
        *,
        database: str = "aiws",
    ) -> list[dict[str, Any]]:
        """@brief 以超级用户读取验证行 / Read verification rows as the cluster superuser."""
        with psycopg.connect(self.super_dsn_for(database), row_factory=dict_row) as connection:
            return [dict(row) for row in connection.execute(statement).fetchall()]


def _postgres_binary(name: str) -> Path | None:
    """@brief 定位 PATH 或 Debian versioned PostgreSQL binary / Locate a PostgreSQL binary."""
    direct = shutil.which(name)
    if direct is not None:
        return Path(direct)
    candidates = sorted(Path("/usr/lib/postgresql").glob(f"*/bin/{name}"), reverse=True)
    return candidates[0] if candidates else None


def _migration_config(dsn: str) -> Config:
    """@brief 构建不经环境变量的 Alembic 配置 / Build an explicit Alembic configuration."""
    configuration = Config(str(PROJECT_ROOT / "alembic.ini"))
    configuration.attributes["aiws.migration_dsn"] = dsn
    for key, value in {
        "owner_role": "aiws_owner",
        "app_role": "aiws_app",
        "dashboard_role": "aiws_dashboard",
        "migrator_role": "aiws_migrator",
        "v2_legacy_workspace_plans": "{}",
    }.items():
        configuration.set_main_option(f"aiws.{key}", value)
    return configuration


def _create_0016_database(harness: _PostgresHarness, database: str) -> Config:
    """@brief 创建隔离的 0016 数据库用于破坏性 preflight 测试 / Create an isolated 0016 database for destructive-preflight tests.

    @param harness PostgreSQL 测试集群 / PostgreSQL test cluster.
    @param database 已验证的隔离数据库名 / Validated isolated database name.
    @return 指向隔离数据库的 Alembic 配置 / Alembic configuration targeting the database.
    """
    migration_dsn = harness.migration_dsn_for(database)
    psql = _postgres_binary("psql")
    assert psql is not None
    harness.psql(
        psql,
        f"""
        CREATE DATABASE {database} OWNER aiws_migrator;
        GRANT CREATE ON DATABASE {database} TO aiws_owner;
        """,
        database="postgres",
    )
    harness.psql(psql, "CREATE EXTENSION vector;", database=database)
    configuration = _migration_config(migration_dsn)
    command.upgrade(configuration, "20260723_0016")
    return configuration


@pytest.fixture(scope="module")
def platform_postgres(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[_PostgresHarness]:
    """@brief 启动有界临时 PostgreSQL 并执行非空迁移 / Start bounded PostgreSQL and migrate non-empty state."""
    initdb = _postgres_binary("initdb")
    pg_ctl = _postgres_binary("pg_ctl")
    psql = _postgres_binary("psql")
    if initdb is None or pg_ctl is None or psql is None:
        pytest.skip("PostgreSQL server binaries are unavailable")
    root = tmp_path_factory.mktemp("platform-postgres")
    data = root / "data"
    socket_dir = root / "socket"
    socket_dir.mkdir()
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = int(reservation.getsockname()[1])
    try:
        subprocess.run(
            [
                str(initdb),
                "-D",
                str(data),
                "-A",
                "trust",
                "--no-locale",
                "--encoding=UTF8",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as error:
        pytest.skip(f"initdb cannot initialize an unprivileged cluster: {error.stderr}")
    subprocess.run(
        [
            str(pg_ctl),
            "-D",
            str(data),
            "-o",
            f"-p {port} -k {socket_dir}",
            "-l",
            str(root / "postgres.log"),
            "-w",
            "start",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=20,
    )
    harness = _PostgresHarness(port, socket_dir, getpass.getuser())
    try:
        harness.psql(
            psql,
            """
            CREATE ROLE aiws_owner NOLOGIN;
            CREATE ROLE aiws_migrator LOGIN;
            CREATE ROLE aiws_app LOGIN;
            CREATE ROLE aiws_dashboard LOGIN;
            GRANT aiws_owner TO aiws_migrator;
            CREATE DATABASE aiws OWNER aiws_migrator;
            GRANT CREATE ON DATABASE aiws TO aiws_owner;
            """,
            database="postgres",
        )
        try:
            harness.psql(psql, "CREATE EXTENSION vector;")
        except subprocess.CalledProcessError:
            pytest.skip("the PostgreSQL vector extension is unavailable")
        configuration = _migration_config(harness.migration_dsn)
        command.upgrade(configuration, "20260723_0016")
        harness.psql(psql, _LEGACY_PLATFORM_FIXTURE_SQL)
        command.upgrade(configuration, "20260723_0017")
        yield harness
    finally:
        subprocess.run(
            [str(pg_ctl), "-D", str(data), "-w", "stop", "-m", "fast"],
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )


def test_0017_real_postgres_migrates_nonempty_artifact_truths(
    platform_postgres: _PostgresHarness,
) -> None:
    """@brief 真实 PG 仅在内容可证明时保留可读 Artifact / Real PG preserves readable Artifacts only with proven content."""
    artifacts = {
        row["id"]: row
        for row in platform_postgres.rows("SELECT * FROM agent.artifacts ORDER BY id")
    }
    pdf = artifacts["artifact_legacypdf1"]
    assert pdf["kind"] == "resume_pdf"
    assert pdf["subject_id"] == "resume_legacy0001"
    assert pdf["subject_revision"] == 7
    assert pdf["media_type"] == "application/pdf"
    assert pdf["page_count"] == 2
    assert pdf["revision"] == 4
    assert pdf["extensions"]["legacy_metadata"] == "keep"
    assert pdf["extensions"]["_migration_0017"]["legacy_resource_owner_id"] == "user_legacy0001"
    external = artifacts["artifact_external01"]
    assert external["kind"] == "resume_json"
    assert external["storage_key"] == "s3://legacy-bucket/resume.json"
    assert external["size_bytes"] == len(b'{"legacy":true}')
    assert external["sha256"] == sha256(b'{"legacy":true}').hexdigest()
    recording = artifacts["recording_legacy001"]
    assert recording["kind"] == "interview_audio"
    assert recording["subject_type"] == "interview_session"
    assert recording["media_type"] == "audio/wav"
    assert recording["deleted_at"] is not None
    assert (
        recording["extensions"]["_migration_0017"]["content_disposition"]
        == "deleted_tombstone_no_bytes"
    )

    contents = {
        row["artifact_id"]: row
        for row in platform_postgres.rows("SELECT * FROM agent.artifact_contents")
    }
    assert set(contents) == {"artifact_external01", "artifact_legacypdf1"}
    assert bytes(contents["artifact_legacypdf1"]["content"]) == b"%PDF-legacy-platform"
    assert contents["artifact_legacypdf1"]["revision"] == 5
    assert bytes(contents["artifact_external01"]["content"]) == b'{"legacy":true}'
    assert contents["artifact_external01"]["sha256"] == external["sha256"]
    assert contents["artifact_external01"]["size_bytes"] == external["size_bytes"]
    missing_active_content = platform_postgres.rows(
        """
        SELECT count(*) AS count
        FROM agent.artifacts AS artifact
        LEFT JOIN agent.artifact_contents AS content
          ON content.artifact_id = artifact.id
         AND content.workspace_id = artifact.workspace_id
        WHERE artifact.deleted_at IS NULL AND content.artifact_id IS NULL
        """
    )[0]
    assert missing_active_content == {"count": 0}
    source_map = platform_postgres.rows("SELECT * FROM agent.artifact_pdf_source_maps")[0]
    assert source_map["nodes"] == [
        {
            "entity_id": "entity_legacy0001",
            "field_path": ["title"],
            "page": 1,
            "rects": [{"x": 1.0, "y": 2.0, "width": 3.0, "height": 4.0, "unit": "pt"}],
        }
    ]
    migration_extension = source_map["extensions"]["_migration_0017"]
    assert migration_extension["source_tables"] == [
        "resume.artifact_blobs",
        "resume.pdf_source_map_entries",
    ]
    assert migration_extension["legacy_entries"][0]["node_kind"] == "section"

    legacy_tables = platform_postgres.rows(
        """
        SELECT to_regclass('resume.render_artifacts') AS render,
               to_regclass('resume.artifact_blobs') AS blob,
               to_regclass('resume.pdf_source_map_entries') AS source_map,
               to_regclass('interview.recording_artifacts') AS recording
        """
    )[0]
    assert set(legacy_tables.values()) == {None}
    job_link = platform_postgres.rows(
        "SELECT artifact_id FROM resume.render_jobs WHERE id = 'renderjob_legacy01'"
    )[0]
    assert job_link["artifact_id"] == "artifact_legacypdf1"
    foreign_keys = platform_postgres.rows(
        """
        SELECT conname, convalidated
        FROM pg_constraint
        WHERE conrelid = 'resume.render_jobs'::regclass
          AND conname LIKE 'fk_render_jobs_artifact%'
        ORDER BY conname
        """
    )
    assert foreign_keys == [
        {"conname": "fk_render_jobs_artifact_id_artifacts", "convalidated": True},
        {"conname": "fk_render_jobs_artifact_workspace", "convalidated": True},
    ]
    with pytest.raises(RuntimeError, match="cannot downgrade non-empty API V2 Platform"):
        command.downgrade(_migration_config(platform_postgres.migration_dsn), "20260723_0016")


@pytest.mark.parametrize(
    ("database", "unreadable_artifact_sql", "expected_error", "legacy_table", "legacy_id"),
    (
        (
            "aiws_unreadable_resume",
            r"""
            INSERT INTO resume.render_artifacts (
                id, workspace_id, resource_owner_id, resume_id, resume_revision_id,
                artifact_kind, format, storage_key, content_sha256, content_bytes,
                expires_at, created_at, updated_at, revision, extensions
            ) VALUES (
                'artifact_missingblob1', 'workspace_legacy0001', 'user_legacy0001',
                'resume_legacy0001', 'revision_legacy0007', 'rendered_resume', 'json',
                's3://legacy-bucket/missing-content.json', repeat('d', 64), 123,
                '2030-01-01T00:00:00Z', '2026-07-11T00:00:00Z',
                '2026-07-11T00:00:00Z', 1, '{"external_provider":"s3"}'
            );
            """,
            "legacy Resume Artifacts lack a verified inline blob",
            "resume.render_artifacts",
            "artifact_missingblob1",
        ),
        (
            "aiws_unreadable_interview",
            r"""
            INSERT INTO interview.recording_artifacts (
                id, workspace_id, resource_owner_id, session_id, storage_key,
                content_sha256, content_bytes, media_kind, expires_at, deleted_at,
                created_at, updated_at, revision, extensions
            ) VALUES (
                'recording_active001', 'workspace_legacy0001', 'user_legacy0001',
                'session_legacy0001', 's3://legacy-bucket/active-interview.wav',
                repeat('e', 64), 456, 'audio/wav', '2030-01-01T00:00:00Z', NULL,
                '2026-07-11T00:00:00Z', '2026-07-11T00:00:00Z', 1,
                '{"external_provider":"s3"}'
            );
            """,
            "legacy Interview recordings that are not deleted",
            "interview.recording_artifacts",
            "recording_active001",
        ),
    ),
)
def test_0017_real_postgres_preflight_rolls_back_unreadable_legacy_artifacts(
    platform_postgres: _PostgresHarness,
    database: str,
    unreadable_artifact_sql: str,
    expected_error: str,
    legacy_table: str,
    legacy_id: str,
) -> None:
    """@brief 不可读 legacy Artifact 令整个升级回滚并保留旧真相 / Unreadable legacy Artifacts roll back the whole upgrade and preserve old truth."""
    psql = _postgres_binary("psql")
    assert psql is not None
    configuration = _create_0016_database(platform_postgres, database)
    platform_postgres.psql(
        psql,
        _LEGACY_PLATFORM_FIXTURE_SQL + unreadable_artifact_sql,
        database=database,
    )

    with pytest.raises(RuntimeError, match=expected_error):
        command.upgrade(configuration, "20260723_0017")

    rollback_state = platform_postgres.rows(
        """
        SELECT
            (SELECT version_num FROM identity.alembic_version) AS version_num,
            to_regclass('resume.render_artifacts') IS NOT NULL AS resume_table,
            to_regclass('resume.artifact_blobs') IS NOT NULL AS blob_table,
            to_regclass('interview.recording_artifacts') IS NOT NULL AS interview_table,
            to_regclass('agent.artifacts') IS NULL AS no_v2_artifacts,
            NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'agent' AND table_name = 'jobs'
                  AND column_name = 'progress_unit'
            ) AS no_v2_job_ddl,
            NOT EXISTS (
                SELECT 1 FROM pg_policies
                WHERE policyname = 'platform_v2_owner_migration_0017'
            ) AS no_preflight_policy
        """,
        database=database,
    )[0]
    assert rollback_state == {
        "version_num": "20260723_0016",
        "resume_table": True,
        "blob_table": True,
        "interview_table": True,
        "no_v2_artifacts": True,
        "no_v2_job_ddl": True,
        "no_preflight_policy": True,
    }
    preserved = platform_postgres.rows(
        f"SELECT count(*) AS count FROM {legacy_table} WHERE id = '{legacy_id}'",
        database=database,
    )
    assert preserved == [{"count": 1}]


def test_0017_real_postgres_empty_round_trip(
    platform_postgres: _PostgresHarness,
) -> None:
    """@brief 空库 0016↔0017 可逆并可再升级 / An empty database supports 0016↔0017 round trips."""
    psql = _postgres_binary("psql")
    assert psql is not None
    platform_postgres.psql(
        psql,
        """
        CREATE DATABASE aiws_roundtrip OWNER aiws_migrator;
        GRANT CREATE ON DATABASE aiws_roundtrip TO aiws_owner;
        """,
        database="postgres",
    )
    platform_postgres.psql(psql, "CREATE EXTENSION vector;", database="aiws_roundtrip")
    dsn = f"postgresql+asyncpg://aiws_migrator@127.0.0.1:{platform_postgres.port}/aiws_roundtrip"
    configuration = _migration_config(dsn)
    command.upgrade(configuration, "20260723_0017")
    command.downgrade(configuration, "20260723_0016")
    command.upgrade(configuration, "20260723_0017")
    version = platform_postgres.rows(
        "SELECT version_num FROM identity.alembic_version",
        database="aiws_roundtrip",
    )
    assert version == [{"version_num": "20260723_0017"}]


async def test_0017_real_postgres_allocates_gap_free_workspace_sequence(
    platform_postgres: _PostgresHarness,
) -> None:
    """@brief 并发跨 subject 唯一分配，rollback 不留洞 / Concurrent subjects allocate uniquely without rollback gaps."""
    baseline = int(
        platform_postgres.rows(
            "SELECT COALESCE(MAX(sequence), 0) AS value FROM agent.outbox_events "
            "WHERE workspace_id = 'workspace_legacy0001'"
        )[0]["value"]
    )

    async def insert(event_id: str, subject_id: str, *, commit: bool = True) -> int:
        """@brief 在独立事务插入 event / Insert an event in an independent transaction."""
        connection = await asyncpg.connect(platform_postgres.app_dsn)
        transaction = connection.transaction()
        await transaction.start()
        try:
            await connection.execute(
                "SELECT set_config('app.actor_id', $1, true), "
                "set_config('app.workspace_id', $2, true)",
                "user_legacy0001",
                "workspace_legacy0001",
            )
            sequence = await connection.fetchval(
                """
                INSERT INTO agent.outbox_events (
                    id, workspace_id, resource_owner_id, aggregate_type,
                    aggregate_id, subject_revision, event_type, sequence,
                    occurred_at, payload, trace_id, replay_expires_at, status,
                    created_at, updated_at, revision, extensions
                ) VALUES (
                    $1, 'workspace_legacy0001', 'user_legacy0001', 'job', $2,
                    1, 'job.updated', 999, now(), '{}'::jsonb,
                    '0123456789abcdef0123456789abcdef', now() + interval '1 day',
                    'pending', now(), now(), 1, '{}'::jsonb
                ) RETURNING sequence
                """,
                event_id,
                subject_id,
            )
            if commit:
                await transaction.commit()
            else:
                await transaction.rollback()
            return int(sequence)
        finally:
            await connection.close()

    first = await asyncio.gather(
        insert("event_concurrent01", "job_subject00001"),
        insert("event_concurrent02", "job_subject00002"),
    )
    rolled_back = await insert("event_rollback001", "job_subject00003", commit=False)
    committed_after_rollback = await insert("event_afterroll01", "job_subject00004")
    assert sorted(first) == [baseline + 1, baseline + 2]
    assert rolled_back == baseline + 3
    assert committed_after_rollback == baseline + 3
    rows = platform_postgres.rows(
        "SELECT id, sequence FROM agent.outbox_events "
        "WHERE id IN ('event_concurrent01', 'event_concurrent02', "
        "'event_rollback001', 'event_afterroll01') ORDER BY sequence"
    )
    assert [row["sequence"] for row in rows] == [
        baseline + 1,
        baseline + 2,
        baseline + 3,
    ]
    assert all(row["id"] != "event_rollback001" for row in rows)


async def test_0017_real_postgres_enforces_rls_and_append_only_audit(
    platform_postgres: _PostgresHarness,
) -> None:
    """@brief 真实 runtime role 验证 RLS 与 append-only audit / Verify RLS and append-only audit as runtime role."""
    connection = await asyncpg.connect(platform_postgres.app_dsn)
    try:
        transaction = connection.transaction()
        await transaction.start()
        await connection.execute(
            "SELECT set_config('app.actor_id', $1, true), set_config('app.workspace_id', $2, true)",
            "user_legacy0001",
            "workspace_legacy0001",
        )
        assert await connection.fetchval("SELECT count(*) FROM agent.artifacts") == 3
        await connection.execute(
            """
            INSERT INTO identity.audit_events (
                id, workspace_id, resource_owner_id, occurred_at, actor_type,
                actor_id, action, resource_type, resource_id, request_id,
                outcome, details, created_at, updated_at, revision, extensions
            ) VALUES (
                'audit_security001', 'workspace_legacy0001', 'user_legacy0001',
                now(), 'user', 'user_legacy0001', 'artifact.read', 'artifact',
                'artifact_legacypdf1', 'request_security01', 'allowed', '{}',
                now(), now(), 1, '{}'
            )
            """
        )
        await transaction.commit()

        for statement in (
            "UPDATE identity.audit_events SET outcome = 'failed' WHERE id = 'audit_security001'",
            "DELETE FROM identity.audit_events WHERE id = 'audit_security001'",
            "UPDATE agent.artifacts SET media_type = 'text/plain' WHERE id = 'artifact_legacypdf1'",
        ):
            transaction = connection.transaction()
            await transaction.start()
            await connection.execute(
                "SELECT set_config('app.actor_id', $1, true), "
                "set_config('app.workspace_id', $2, true)",
                "user_legacy0001",
                "workspace_legacy0001",
            )
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await connection.execute(statement)
            await transaction.rollback()

        transaction = connection.transaction()
        await transaction.start()
        await connection.execute(
            "SELECT set_config('app.actor_id', $1, true), "
            "set_config('app.workspace_id', 'workspace_other001', true)",
            "user_legacy0001",
        )
        assert await connection.fetchval("SELECT count(*) FROM agent.artifacts") == 0
        await transaction.rollback()
    finally:
        await connection.close()

    security = platform_postgres.rows(
        """
        SELECT n.nspname AS schema_name, c.relname AS table_name,
               c.relrowsecurity, c.relforcerowsecurity
        FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE (n.nspname, c.relname) IN (
            ('agent', 'workspace_event_sequences'), ('agent', 'artifacts'),
            ('agent', 'artifact_contents'), ('agent', 'artifact_pdf_source_maps'),
            ('agent', 'jobs'), ('agent', 'outbox_events'),
            ('identity', 'audit_events')
        )
        """
    )
    assert len(security) == 7
    assert all(row["relrowsecurity"] and row["relforcerowsecurity"] for row in security)
    migration_policies = platform_postgres.rows(
        "SELECT policyname FROM pg_policies WHERE policyname = 'platform_v2_owner_migration_0017'"
    )
    assert migration_policies == []


async def test_postgres_platform_adapters_complete_authorized_product_flow(
    platform_postgres: _PostgresHarness,
) -> None:
    """@brief 真实 adapters 完成授权、内容、source map、取消与 live event / Real adapters complete authorization, content, source-map, cancellation, and live-event flows."""
    database = AsyncDatabase(
        AsyncDatabaseOptions(
            platform_postgres.app_dsn,
            pool_size=2,
            max_overflow=1,
            statement_timeout_ms=5_000,
            lock_timeout_ms=1_000,
        )
    )
    factory = PostgresPlatformUnitOfWorkFactory(
        database,
        api_origin="https://api.hmalliances.org:8022",
        event_poll_interval=0.01,
    )
    service = PlatformApplicationService(
        factory,
        factory.content_store,
        factory.event_feed,
        clock=_Clock(),
    )
    principal = TokenPrincipal(
        UserId("user_legacy0001"),
        Subject("legacy-subject"),
        ClientId("client_legacy0001"),
        frozenset(
            {
                Scope("workspace.read"),
                Scope("workspace.write"),
                Scope("resume.write"),
                Scope("resume.render"),
            }
        ),
    )
    workspace_id = WorkspaceId("workspace_legacy0001")
    job_id = JobId("job_legacyrender1")
    artifact_id = ArtifactId("artifact_legacypdf1")
    external_artifact_id = ArtifactId("artifact_external01")
    stream: AsyncGenerator[ApiEvent] | None = None
    try:
        jobs = await service.list_jobs(principal, workspace_id)
        assert [(str(job.meta.id), job.status) for job in jobs.items] == [
            (str(job_id), JobStatus.QUEUED)
        ]
        artifacts = await service.list_artifacts(principal, workspace_id)
        assert {str(artifact.meta.id) for artifact in artifacts.items} == {
            "artifact_external01",
            str(artifact_id),
        }

        download = await service.open_artifact_content(
            principal,
            workspace_id,
            artifact_id,
            byte_range=ByteRangeRequest(first=5, last_inclusive=10),
        )
        assert await _read(download.chunks) == b"legacy"
        assert download.content_length == 6
        external_content = b'{"legacy":true}'
        external_download = await service.open_artifact_content(
            principal,
            workspace_id,
            external_artifact_id,
        )
        assert await _read(external_download.chunks) == external_content
        assert external_download.artifact.sha256 == sha256(external_content).hexdigest()
        assert external_download.etag == f'"sha256-{sha256(external_content).hexdigest()}"'
        external_range = await service.open_artifact_content(
            principal,
            workspace_id,
            external_artifact_id,
            byte_range=ByteRangeRequest(first=2, last_inclusive=8),
        )
        assert await _read(external_range.chunks) == external_content[2:9]
        assert external_range.content_length == 7
        source_map = await service.get_pdf_source_map(
            principal,
            workspace_id,
            artifact_id,
        )
        assert source_map.resume_id == "resume_legacy0001"
        assert source_map.resume_revision == 7
        assert source_map.nodes[0].page == 1

        stream = cast(
            AsyncGenerator[ApiEvent],
            await asyncio.wait_for(
                service.open_event_stream(principal, workspace_id),
                timeout=1,
            ),
        )
        cancelled = await asyncio.wait_for(
            service.cancel_job(
                principal,
                workspace_id,
                job_id,
                MutationContext(
                    "request_adapter01",
                    "abcdef0123456789abcdef0123456789",
                ),
                expected_revision=1,
            ),
            timeout=1,
        )
        event = await asyncio.wait_for(anext(stream), timeout=1)
        assert cancelled.status is JobStatus.CANCELLED
        assert cancelled.meta.revision == 2
        assert event.subject == ResourceRef("job", str(job_id), 2)
        assert event.data == {"status": "cancelled"}
        assert event.trace_id == "abcdef0123456789abcdef0123456789"

        audits = await service.list_audit_events(principal, workspace_id)
        matching_audits = [
            audit
            for audit in audits.items
            if audit.action == "job.cancel" and audit.target.id == str(job_id)
        ]
        assert len(matching_audits) == 1
        assert matching_audits[0].request_id == "request_adapter01"
        persisted = platform_postgres.rows(
            "SELECT status, revision FROM agent.jobs WHERE id = 'job_legacyrender1'"
        )
        assert persisted == [{"status": "cancelled", "revision": 2}]
    finally:
        if stream is not None:
            await stream.aclose()
        await database.aclose()


def test_0018_real_postgres_invalidates_unbound_legacy_oauth_state(
    platform_postgres: _PostgresHarness,
) -> None:
    """@brief 真实 PG 对不可归属旧凭据 fail closed 并保留审计 / Real PG fails closed on unattributable credentials with audit."""

    psql = _postgres_binary("psql")
    assert psql is not None
    platform_postgres.psql(
        psql,
        """
        INSERT INTO identity.oauth_authorization_requests (
            id, client_id, redirect_uri, scope, state, nonce, code_challenge,
            code_challenge_method, prompt, status, created_at, expires_at
        ) VALUES (
            'authreq_legacybind01', 'client_legacy0001',
            'https://app.hmalliances.org/oauth/callback', 'openid offline_access',
            'legacy-state', 'legacy-nonce', repeat('z', 43), 'S256', 'consent',
            'code_issued', now(), now() + interval '5 minutes'
        );
        INSERT INTO identity.oauth_authorization_codes (
            id, code_hash, authorization_request_id, subject, user_id, client_id,
            redirect_uri, scope, nonce, code_challenge, auth_time, expires_at
        ) VALUES (
            'ac_legacybind0001', repeat('a', 64), 'authreq_legacybind01',
            'legacy-subject', 'user_legacy0001', 'client_legacy0001',
            'https://app.hmalliances.org/oauth/callback', 'openid offline_access',
            'legacy-nonce', repeat('z', 43), now(), now() + interval '1 minute'
        );
        INSERT INTO identity.oauth_refresh_token_families (
            id, subject, user_id, client_id, scope
        ) VALUES (
            'rtfam_legacybind01', 'legacy-subject', 'user_legacy0001',
            'client_legacy0001', 'openid offline_access'
        );
        """,
    )

    command.upgrade(_migration_config(platform_postgres.migration_dsn), "20260723_0018")

    state = platform_postgres.rows(
        """
        SELECT
            (SELECT consumed_at IS NOT NULL FROM identity.oauth_authorization_codes
             WHERE id = 'ac_legacybind0001') AS code_invalidated,
            (SELECT revoked_at IS NOT NULL FROM identity.oauth_refresh_token_families
             WHERE id = 'rtfam_legacybind01') AS family_revoked,
            (SELECT login_session_id IS NULL FROM identity.oauth_authorization_codes
             WHERE id = 'ac_legacybind0001') AS code_history_preserved,
            (SELECT login_session_id IS NULL FROM identity.oauth_refresh_token_families
             WHERE id = 'rtfam_legacybind01') AS family_history_preserved
        """
    )[0]
    assert set(state.values()) == {True}
    audit = platform_postgres.rows(
        """
        SELECT migration_id, details
        FROM identity.api_migration_audits
        WHERE id = 'audit_20260723_0018_identity_session_binding'
        """
    )
    assert audit == [
        {
            "migration_id": "20260723_0018",
            "details": {
                "rule": ("fail closed because historical login-session ownership is unknowable"),
                "invalidated_unbound_authorization_codes": 1,
                "revoked_unbound_refresh_families": 1,
            },
        }
    ]

    with psycopg.connect(platform_postgres.super_dsn) as connection:
        with pytest.raises(psycopg.errors.CheckViolation):
            connection.execute(
                """
                INSERT INTO identity.oauth_refresh_token_families (
                    id, subject, user_id, client_id, scope
                ) VALUES (
                    'rtfam_unboundactive', 'legacy-subject', 'user_legacy0001',
                    'client_legacy0001', 'openid offline_access'
                )
                """
            )

    platform_postgres.psql(
        psql,
        """
        INSERT INTO identity.identity_login_sessions (
            id, user_id, client_id, client_name, device_name, session_secret_hash,
            created_at, last_seen_at, idle_expires_at, absolute_expires_at
        ) VALUES
            ('idses_bound_a', 'user_legacy0001', 'client_legacy0001', 'Web', 'A',
             repeat('1', 64), now(), now(), now() + interval '1 hour', now() + interval '1 day'),
            ('idses_bound_b', 'user_legacy0001', 'client_legacy0001', 'Web', 'B',
             repeat('2', 64), now(), now(), now() + interval '1 hour', now() + interval '1 day');
        INSERT INTO identity.oauth_refresh_token_families (
            id, subject, user_id, client_id, login_session_id, scope
        ) VALUES
            ('rtfam_bound_a', 'legacy-subject', 'user_legacy0001',
             'client_legacy0001', 'idses_bound_a', 'openid offline_access'),
            ('rtfam_bound_b', 'legacy-subject', 'user_legacy0001',
             'client_legacy0001', 'idses_bound_b', 'openid offline_access');
        """,
    )


async def test_postgres_session_revocation_targets_only_its_bound_family(
    platform_postgres: _PostgresHarness,
) -> None:
    """@brief 真实 adapter 只撤销目标会话的 refresh family / Real adapter revokes only the target session's family."""

    database = AsyncDatabase(
        AsyncDatabaseOptions(
            platform_postgres.app_dsn,
            pool_size=1,
            max_overflow=0,
            statement_timeout_ms=5_000,
            lock_timeout_ms=1_000,
        )
    )
    repository = PostgresHostedIdentityRepository(database, data_region=DataRegion.GLOBAL)
    try:
        revoked_at = datetime.now(UTC)
        assert await repository.revoke_login_session("user_legacy0001", "idses_bound_a", revoked_at)
        assert not await repository.revoke_login_session(
            "user_legacy0001", "idses_bound_a", revoked_at
        )
    finally:
        await database.aclose()

    families = platform_postgres.rows(
        """
        SELECT id, revoked_at IS NOT NULL AS revoked
        FROM identity.oauth_refresh_token_families
        WHERE id IN ('rtfam_bound_a', 'rtfam_bound_b')
        ORDER BY id
        """
    )
    assert families == [
        {"id": "rtfam_bound_a", "revoked": True},
        {"id": "rtfam_bound_b", "revoked": False},
    ]


async def test_postgres_registration_flushes_workspace_before_owner_membership(
    platform_postgres: _PostgresHarness,
) -> None:
    """@brief 真实注册先落租户根再落 owner 成员 / Real registration persists the tenant root before its owner membership."""

    database = AsyncDatabase(
        AsyncDatabaseOptions(
            platform_postgres.app_dsn,
            pool_size=1,
            max_overflow=0,
            statement_timeout_ms=5_000,
            lock_timeout_ms=1_000,
        )
    )
    repository = PostgresHostedIdentityRepository(database, data_region=DataRegion.GLOBAL)
    user = IdentityUserRecord(
        id="usr_registration_pg1",
        subject="registration-postgres-subject-1",
        email="registration-pg1@example.com",
        email_verified=True,
        display_name="Registration PG",
        locale="en-SG",
    )
    try:
        assert await repository.create_user_with_password(
            user=user,
            password_authenticator_id="authn_registration_pg1",
            password_verifier="argon2id:postgres-registration-test",
            now=datetime.now(UTC),
        )
    finally:
        await database.aclose()

    rows = platform_postgres.rows(
        """
        SELECT usr.default_workspace_id AS workspace_id,
               workspace.resource_owner_id AS workspace_owner_id,
               member.user_id AS member_user_id,
               member.role,
               member.status
        FROM identity.users AS usr
        JOIN identity.workspaces AS workspace
          ON workspace.id = usr.default_workspace_id
        JOIN identity.workspace_members AS member
          ON member.workspace_id = workspace.id AND member.user_id = usr.id
        WHERE usr.id = 'usr_registration_pg1'
        """
    )
    assert rows == [
        {
            "workspace_id": rows[0]["workspace_id"],
            "workspace_owner_id": user.id,
            "member_user_id": user.id,
            "role": "owner",
            "status": "active",
        }
    ]


async def test_postgres_prepared_idempotency_holds_no_transaction_during_external_io(
    platform_postgres: _PostgresHarness,
) -> None:
    """@brief 真实 PG prepare 仅持 session lock，最终短事务原子保存 receipt / Real PG preparation holds only a session lock before atomic receipt commit.

    @param platform_postgres 临时真实 PostgreSQL / Temporary real PostgreSQL.
    @return 无返回值 / No return value.
    """

    database = AsyncDatabase(
        AsyncDatabaseOptions(
            platform_postgres.app_dsn,
            pool_size=1,
            max_overflow=0,
            statement_timeout_ms=5_000,
            lock_timeout_ms=1_000,
        )
    )
    executor = AtomicPostgresIdempotencyExecutor(database)
    request = IdempotencyRequest(
        IdempotencyScope(
            UserId("user_legacy0001"),
            WorkspaceId("workspace_legacy0001"),
            "POST",
            "/api/v2/workspaces/workspace_legacy0001/prepared-probe",
            "prepared-postgres-key-0001",
        ),
        b'{"probe":true}',
        "application/json",
        None,
    )
    preparation_ids: list[IdempotencyPreparationId] = []
    commit_calls = 0

    async def prepare(operation_id: IdempotencyPreparationId) -> str:
        """@brief 验证 advisory session 空闲且无 xact snapshot / Verify the advisory-lock session is idle without an xact snapshot."""

        assert database.in_atomic_envelope is False
        preparation_ids.append(operation_id)
        locks = platform_postgres.rows(
            """
            SELECT activity.state, activity.xact_start
            FROM pg_locks AS lock
            JOIN pg_stat_activity AS activity ON activity.pid = lock.pid
            WHERE lock.locktype = 'advisory'
              AND lock.granted
              AND activity.usename = 'aiws_app'
            """
        )
        assert any(row["state"] == "idle" and row["xact_start"] is None for row in locks)
        return "prepared-provider-result"

    async def commit(prepared: str) -> ReplayableResponse:
        """@brief 验证最终 callback 位于原子信封 / Verify the final callback runs in the atomic envelope."""

        nonlocal commit_calls
        assert prepared == "prepared-provider-result"
        assert database.in_atomic_envelope is True
        commit_calls += 1
        return ReplayableResponse(
            201,
            (("Content-Type", "application/json"),),
            b'{"prepared":true}',
        )

    try:
        first = await executor.execute_prepared(request, prepare, commit)
        replay = await executor.execute_prepared(request, prepare, commit)
    finally:
        await database.aclose()

    assert replay == first
    assert commit_calls == 1
    assert len(preparation_ids) == 1
    assert str(preparation_ids[0]).startswith("prep_")
    receipts = platform_postgres.rows(
        """
        SELECT status, response_status, response_body
        FROM identity.api_v2_idempotency_records
        WHERE canonical_path = '/api/v2/workspaces/workspace_legacy0001/prepared-probe'
        """
    )
    assert receipts == [
        {
            "status": "completed",
            "response_status": 201,
            "response_body": b'{"prepared":true}',
        }
    ]
