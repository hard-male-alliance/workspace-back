"""@brief API V2 Resume persistence、migration 与并发门禁 / API V2 Resume persistence, migration, and concurrency gates."""

from __future__ import annotations

import asyncio
import getpass
import hashlib
import json
import shutil
import socket
import subprocess
from collections.abc import AsyncIterator, Iterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, cast

import psycopg
import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from psycopg.rows import dict_row

from backend.application.outbox_dispatch import (
    OutboxDispatchService,
    OutboxDispatchSettings,
)
from backend.application.ports.access import AuthorizationDenied
from backend.application.ports.resume_worker import (
    RenderedResumeArtifact,
    ResumeCapabilityFailure,
    resume_worker_artifact_id,
)
from backend.application.resume_worker import (
    RESUME_WORK_EVENT_TYPES,
    ResumeJobOutboxHandler,
    ResumeJobWorkerService,
)
from backend.application.resumes import (
    CreateRenderJobCommand,
    CreateRestoreJobCommand,
    CreateResumeCommand,
    CreateResumeImportJobCommand,
    ResumeApplicationService,
    ResumePreconditionFailed,
    UpdateResumeMetadataCommand,
)
from backend.domain.platform import JobId
from backend.domain.principals import (
    ClientId,
    MembershipId,
    ResourceMeta,
    Scope,
    Subject,
    TokenPrincipal,
    UserId,
    WorkspaceAction,
    WorkspaceId,
)
from backend.domain.resume_jobs import RenderFormat, RenderMode
from backend.domain.resumes import (
    PageSize,
    ResumeDocument,
    ResumeId,
    ResumeSectionKind,
    TemplatePolicy,
    TemplateRef,
    TemplateZonePolicy,
)
from backend.domain.upload_sessions import UploadSessionId
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
from backend.infrastructure.outbox_dispatch import PostgresOutboxClaimRepository
from backend.infrastructure.persistence.database import AsyncDatabase, AsyncDatabaseOptions
from backend.infrastructure.persistence.models import (
    JobRecord,
    ResumeDocumentRecord,
    ResumeImportUploadSessionRecord,
    ResumeOperationBatchRecord,
    ResumeOperationRecord,
    ResumeProposalOperationRecord,
    ResumeProposalRecord,
    ResumeRevisionRecord,
    WorkspaceMemberRecord,
    WorkspaceRecord,
)
from backend.infrastructure.rendering import MockRenderer
from backend.infrastructure.resume_worker import (
    MultiFormatResumeRenderer,
    SafeResumeImporter,
)
from backend.infrastructure.resumes import (
    InMemoryResumeStore,
    InMemoryResumeUnitOfWorkFactory,
    MappingResumeTemplateCatalog,
    PostgresResumeUnitOfWorkFactory,
    PostgresResumeWorkerUnitOfWorkFactory,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 项目根目录 / Project root."""

MIGRATION = PROJECT_ROOT / "alembic" / "versions" / "20260723_0016_v2_resume_persistence.py"
"""@brief Resume V2 persistence migration / Resume V2 persistence migration."""

NOW = datetime(2026, 7, 23, 12, tzinfo=UTC)
"""@brief 固定测试时刻 / Fixed test instant."""

USER_ID = UserId("user_00000001")
"""@brief 测试用户 / Test user."""

WORKSPACE_ID = WorkspaceId("workspace_00000001")
"""@brief 测试 Workspace / Test Workspace."""

TEMPLATE_REF = TemplateRef("template_00000001", "1.0")
"""@brief 测试模板版本 / Test template version."""

_LEGACY_STYLE_JSON = r"""
{
  "style_contract_version":"1.0",
  "page":{"size":"A4","custom_width":null,"custom_height":null,
    "orientation":"portrait","margins":{
      "top":{"value":18.0,"unit":"mm"},"right":{"value":18.0,"unit":"mm"},
      "bottom":{"value":18.0,"unit":"mm"},"left":{"value":18.0,"unit":"mm"}},
    "max_pages":null,"show_page_numbers":false},
  "typography":{"font_family_token":"body.default","base_size_pt":10.5,
    "line_height":1.25,"heading_scale":1.2,"letter_spacing_em":0.0},
  "palette":{"primary":{"space":"srgb_hex","value":"#1F4E79"},
    "secondary":{"space":"srgb_hex","value":"#4F81BD"},
    "text":{"space":"srgb_hex","value":"#1A1A1A"},
    "muted_text":{"space":"srgb_hex","value":"#666666"},
    "background":{"space":"srgb_hex","value":"#FFFFFF"}},
  "density":0.5,"date_format_token":"yyyy_mm","bullet_style_token":"bullet.default",
  "section_layout":[],"template_settings":{},"extensions":{}
}
"""
"""@brief 可被 V1/V2 共同表示的 style fixture / Style fixture shared by V1 and V2."""

_LEGACY_SECTION_TITLE = "Legacy summary " + "x" * 130
"""@brief V1 合法但超过 V2 上限的 section 标题 / V1-valid section title above the V2 limit."""


def _canonical_sha256(value: object) -> str:
    """@brief 按 Resume V2 规则计算 JSON 指纹 / Compute a JSON fingerprint with Resume V2 rules."""
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _legacy_document_json(*, revision: int, headline: str, updated_at: str) -> str:
    """@brief 构造可证明 V1 ResumeDocument JSON / Build a provable V1 ResumeDocument JSON."""
    return f"""
    {{
      "id":"resume_legacy0001","created_at":"2026-07-01T00:00:00Z",
      "updated_at":"{updated_at}","revision":{revision},"schema_version":"1.0",
      "workspace_id":"workspace_legacy0001","title":" Legacy Resume ","locale":"en",
      "template":{{"template_id":"template_legacy01","template_version":"v1"}},
      "profile":{{"full_name":"Legacy Candidate","headline":"{headline}",
        "contacts":[],"summary":null}},
      "sections":[{{"section_id":"section_legacy0001","kind":"summary",
        "title":"{_LEGACY_SECTION_TITLE}","visible":true,"content":null,"items":[],"extensions":{{}}}}],
      "style_intent":{_LEGACY_STYLE_JSON},"knowledge_source_id":null,
      "extensions":{{"v1_only":{{"keep":"all content"}}}}
    }}
    """


_LEGACY_RESUME_FIXTURE_SQL = f"""
INSERT INTO identity.users (
    id, external_subject, display_name, email, email_verified, email_canonical,
    locale, account_status, created_at, updated_at, revision, extensions
) VALUES (
    'user_legacy0001', 'legacy-subject', 'Legacy Owner', 'legacy@example.com',
    true, 'legacy@example.com', 'en', 'active', '2026-07-01T00:00:00Z',
    '2026-07-02T00:00:00Z', 3, '{{}}'
);
INSERT INTO identity.workspaces (
    id, resource_owner_id, name, default_locale, slug, plan, data_region,
    created_at, updated_at, revision, extensions
) VALUES (
    'workspace_legacy0001', 'user_legacy0001', 'Legacy Workspace', 'en',
    'legacy-workspace', 'team', 'global', '2026-07-01T00:00:00Z',
    '2026-07-02T00:00:00Z', 2, '{{}}'
);
INSERT INTO identity.workspace_members (
    id, workspace_id, resource_owner_id, user_id, display_name, role, status,
    joined_at, created_at, updated_at, revision, extensions
) VALUES (
    'membership_legacy01', 'workspace_legacy0001', 'user_legacy0001',
    'user_legacy0001', 'Legacy Owner', 'owner', 'active', '2026-07-01T00:00:00Z',
    '2026-07-01T00:00:00Z', '2026-07-02T00:00:00Z', 1, '{{}}'
);
INSERT INTO resume.template_versions (
    id, workspace_id, resource_owner_id, template_id, template_version,
    manifest, renderer_binding, published_at, created_at, updated_at, revision, extensions
) VALUES (
    'tplver_legacy0001', 'workspace_legacy0001', 'user_legacy0001',
    'template_legacy01', 'v1', '{{}}', '{{}}', '2026-06-01T00:00:00Z',
    '2026-06-01T00:00:00Z', '2026-06-01T00:00:00Z', 1, '{{}}'
);
INSERT INTO resume.documents (
    id, workspace_id, resource_owner_id, template_version_id, title, locale,
    current_revision_no, created_at, updated_at, revision, extensions
) VALUES (
    'resume_legacy0001', 'workspace_legacy0001', 'user_legacy0001',
    'tplver_legacy0001', ' Legacy Resume ', 'en', 2, '2026-07-01T00:00:00Z',
    '2026-07-02T00:00:00Z', 2,
    '{{"runtime":{{"changed_targets":{{"2":[["profile","headline"]]}}}},"keep":"root"}}'
);
INSERT INTO resume.revisions (
    id, workspace_id, resource_owner_id, resume_id, revision_no, semantic_document,
    content_hash, created_by_actor_id, source, created_at, updated_at, revision, extensions
) VALUES
(
    'revision_legacy0001', 'workspace_legacy0001', 'user_legacy0001',
    'resume_legacy0001', 1, $json${_legacy_document_json(revision=1, headline="Engineer", updated_at="2026-07-01T00:00:00Z")}$json$::jsonb,
    repeat('a',64), 'user_legacy0001', 'user', '2026-07-01T00:00:00Z',
    '2026-07-01T00:00:00Z', 1, '{{"keep":"revision-one"}}'
),
(
    'revision_legacy0002', 'workspace_legacy0001', 'user_legacy0001',
    'resume_legacy0001', 2, $json${_legacy_document_json(revision=2, headline="Senior Engineer", updated_at="2026-07-02T00:00:00Z")}$json$::jsonb,
    repeat('b',64), 'user_legacy0001', 'user', '2026-07-02T00:00:00Z',
    '2026-07-02T00:00:00Z', 2, '{{"keep":"revision-two"}}'
);
INSERT INTO resume.operation_batches (
    id, workspace_id, resource_owner_id, resume_id, client_batch_id,
    base_revision_no, applied_revision_no, conflict_strategy, status,
    created_at, updated_at, revision, extensions
) VALUES (
    'batch_legacy00001', 'workspace_legacy0001', 'user_legacy0001',
    'resume_legacy0001', 'batch_client0001', 1, 2, 'reject', 'applied',
    '2026-07-02T00:00:00Z', '2026-07-02T00:00:00Z', 1, '{{"keep":"batch"}}'
);
INSERT INTO resume.operation_batches (
    id, workspace_id, resource_owner_id, resume_id, client_batch_id,
    base_revision_no, applied_revision_no, conflict_strategy, status,
    created_at, updated_at, revision, extensions
) VALUES (
    'batch_pending0001', 'workspace_legacy0001', 'user_legacy0001',
    'resume_legacy0001', 'batch_pending0001', 2, NULL, 'reject', 'received',
    '2026-07-03T00:00:00Z', '2026-07-03T00:00:00Z', 1, '{{"keep":"pending-batch"}}'
);
INSERT INTO resume.operations (
    id, workspace_id, resource_owner_id, batch_id, operation_id, ordinal,
    operation_type, payload, created_at, updated_at, revision, extensions
) VALUES (
    'operationrow_legacy1', 'workspace_legacy0001', 'user_legacy0001',
    'batch_legacy00001', 'operation_legacy01', 0, 'set_field',
    '{{"operation_id":"operation_legacy01","op":"set_field",
      "target":{{"entity_type":"profile"}},"field_path":["headline"],
      "value":"Senior Engineer"}}',
    '2026-07-02T00:00:00Z', '2026-07-02T00:00:00Z', 1, '{{"keep":"operation"}}'
);
INSERT INTO resume.proposals (
    id, workspace_id, resource_owner_id, resume_id, base_revision_no, status,
    decision_payload, expires_at, created_at, updated_at, revision, extensions
) VALUES (
    'proposal_legacy001', 'workspace_legacy0001', 'user_legacy0001',
    'resume_legacy0001', 2, 'pending',
    '{{"runtime":{{"title":"Promote headline","selected_operation_ids":[]}}}}',
    '2026-08-01T00:00:00Z', '2026-07-03T00:00:00Z',
    '2026-07-03T00:00:00Z', 1, '{{"keep":"proposal"}}'
);
INSERT INTO resume.proposal_operations (
    id, workspace_id, resource_owner_id, proposal_id, ordinal, operation_type,
    payload, created_at, updated_at, revision, extensions
) VALUES (
    'proposalop_legacy01', 'workspace_legacy0001', 'user_legacy0001',
    'proposal_legacy001', 0, 'set_field',
    '{{"operation":{{"operation_id":"operation_proposal1","op":"set_field",
       "target":{{"entity_type":"profile"}},"field_path":["headline"],
       "value":"Principal Engineer"}},"reason":"legacy rationale",
       "citations":[]}}', '2026-07-03T00:00:00Z',
    '2026-07-03T00:00:00Z', 1, '{{"keep":"proposal-operation"}}'
);
INSERT INTO agent.jobs (
    id, workspace_id, resource_owner_id, job_type, status, phase,
    completed_units, total_units, target_resource_type, target_resource_id,
    created_at, updated_at, revision, extensions
) VALUES (
    'job_legacyresume1', 'workspace_legacy0001', 'user_legacy0001',
    'resume.render', 'queued', 'queued', 0, 1, NULL, NULL,
    '2026-07-04T00:00:00Z', '2026-07-04T00:00:00Z', 1,
    '{{"runtime":{{"extensions":{{"resume_id":"resume_legacy0001",
      "resume_revision":2,"render_profile":"preview"}}}}}}'
);
INSERT INTO resume.render_jobs (
    id, workspace_id, resource_owner_id, job_id, resume_id, resume_revision_id,
    render_profile, created_at, updated_at, revision, extensions
) VALUES (
    'renderjob_legacy01', 'workspace_legacy0001', 'user_legacy0001',
    'job_legacyresume1', 'resume_legacy0001', 'revision_legacy0002', 'preview',
    '2026-07-04T00:00:00Z', '2026-07-04T00:00:00Z', 1, '{{}}'
);
INSERT INTO agent.outbox_events (
    id, workspace_id, resource_owner_id, aggregate_type, aggregate_id,
    event_type, sequence, occurred_at, payload, status,
    created_at, updated_at, revision, extensions
) VALUES (
    'event_legacyresume1', 'workspace_legacy0001', 'user_legacy0001',
    'resume', 'resume_legacy0001', 'resume.updated', 2,
    '2026-07-02T00:00:00Z', '{{"revision":2,"legacy":"keep"}}', 'pending',
    '2026-07-02T00:00:00Z', '2026-07-02T00:00:00Z', 1, '{{}}'
);
"""
"""@brief 0015 状态的非空 V1 Resume 业务 fixture / Non-empty V1 Resume fixture at revision 0015."""


class _Clock:
    """@brief 固定应用时钟 / Fixed application clock."""

    def now(self) -> datetime:
        """@brief 返回固定时刻 / Return the fixed instant."""
        return NOW


@dataclass(slots=True)
class _BytesUploadReader:
    """@brief 测试用隔离 upload object reader / Isolated upload-object reader for tests."""

    values: dict[tuple[WorkspaceId, UploadSessionId], bytes]

    @asynccontextmanager
    async def read(
        self,
        workspace_id: WorkspaceId,
        upload_id: UploadSessionId,
    ) -> AsyncIterator[AsyncIterator[bytes]]:
        """@brief 以多个 chunk 流式返回精确对象 / Stream the exact object in multiple chunks."""
        value = self.values[(workspace_id, upload_id)]

        async def chunks() -> AsyncIterator[bytes]:
            """@brief 产生有界 chunk / Yield bounded chunks."""
            midpoint = max(1, len(value) // 2)
            yield value[:midpoint]
            if midpoint < len(value):
                yield value[midpoint:]

        yield chunks()


class _FlakyResumeRenderer:
    """@brief 首次瞬态失败、随后成功的 crash-resume renderer / Crash-resume renderer failing transiently once, then succeeding."""

    def __init__(self, delegate: MultiFormatResumeRenderer) -> None:
        """@brief 绑定真实 delegate / Bind the real delegate."""
        self._delegate = delegate
        self.operation_ids: list[str] = []

    async def render_resume(
        self,
        document: ResumeDocument,
        formats: Sequence[RenderFormat],
        *,
        operation_id: str,
    ) -> tuple[RenderedResumeArtifact, ...]:
        """@brief 记录 operation ID 并只在第一次模拟瞬态故障 / Record the operation ID and simulate a transient first failure only."""
        self.operation_ids.append(operation_id)
        if len(self.operation_ids) == 1:
            raise ResumeCapabilityFailure("resume.renderer_unavailable", retryable=True)
        return await self._delegate.render_resume(
            document,
            formats,
            operation_id=operation_id,
        )


class _UnsupportedResumeRenderer:
    """@brief 模拟部署明确不支持的 renderer / Simulate an explicitly unsupported deployment renderer."""

    async def render_resume(
        self,
        document: ResumeDocument,
        formats: Sequence[RenderFormat],
        *,
        operation_id: str,
    ) -> tuple[RenderedResumeArtifact, ...]:
        """@brief 返回确定性非重试失败 / Return a deterministic non-retryable failure."""
        del document, formats, operation_id
        raise ResumeCapabilityFailure("resume.render_format_unsupported", retryable=False)


type _SourceMapDefect = Literal[
    "artifact_id",
    "resume_id",
    "resume_revision",
    "page_count",
    "page",
    "rect",
    "field_path",
]
"""@brief 定向 source-map 缺陷集合 / Directed source-map defect set."""


class _MalformedSourceMapPdfRenderer:
    """@brief 返回结构化但与 Artifact/SIR 不一致的 source map / Return a structured source map inconsistent with the Artifact/SIR."""

    def __init__(self, defect: _SourceMapDefect) -> None:
        """@brief 选择单一待验证缺陷 / Select one validation defect.

        @param defect 身份、页、矩形或字段绑定缺陷 / Identity, page, rectangle, or field-binding defect.
        """
        self._defect = defect
        self._delegate = MockRenderer()

    async def render(
        self,
        document: dict[str, Any],
    ) -> tuple[bytes, dict[str, Any]]:
        """@brief 生成有效 PDF 并只破坏 source map / Generate a valid PDF and corrupt only its source map.

        @param document 含 worker 预分配 Artifact ID 的 renderer 输入 / Renderer input carrying the worker-preallocated Artifact ID.
        @return 有效 PDF 与定向畸形 source map / Valid PDF and directed malformed source map.
        """
        content, source_map = await self._delegate.render(document)
        source_map["nodes"] = [
            {
                "entity_id": document["id"],
                "field_path": ["title"],
                "page": 1,
                "rects": [
                    {
                        "x": 1.0,
                        "y": 2.0,
                        "width": 3.0,
                        "height": 4.0,
                        "unit": "pt",
                    }
                ],
            }
        ]
        match self._defect:
            case "artifact_id":
                source_map["artifact_id"] = "artifact_wrong0001"
            case "resume_id":
                source_map["resume_id"] = "resume_wrong0001"
            case "resume_revision":
                source_map["resume_revision"] = cast(int, document["revision"]) + 1
            case "page_count":
                source_map["page_count"] = 2
            case "page":
                source_map["nodes"][0]["page"] = 2
            case "rect":
                source_map["nodes"][0]["rects"][0]["x"] = "not-a-number"
            case "field_path":
                source_map["nodes"][0]["field_path"] = ["does_not_exist"]
        return content, source_map


class _Ids:
    """@brief 并发安全的确定性 ID 序列 / Concurrency-safe deterministic ID sequence."""

    def __init__(self) -> None:
        """@brief 初始化计数器 / Initialize counters."""
        self._values: dict[str, int] = {}

    def __call__(self, prefix: str) -> str:
        """@brief 返回下一个契约有效 ID / Return the next contract-valid ID."""
        value = self._values.get(prefix, 0) + 1
        self._values[prefix] = value
        return f"{prefix}_{value:08d}"


def _policy() -> TemplatePolicy:
    """@brief 构造最小不可变模板策略 / Build a minimal immutable template policy."""
    kinds = frozenset(ResumeSectionKind)
    return TemplatePolicy(
        TEMPLATE_REF,
        frozenset({"zh-CN", "en-US"}),
        frozenset({PageSize.A4}),
        frozenset({"pdf", "json", "docx"}),
        kinds,
        (TemplateZonePolicy("main", kinds, 100),),
        frozenset({"body.default"}),
        frozenset({"yyyy_mm"}),
        frozenset({"bullet.default"}),
    )


def _fixture() -> tuple[ResumeApplicationService, InMemoryResumeUnitOfWorkFactory]:
    """@brief 组装复用集中 AccessAuthorizer 的生产形状内存 slice / Assemble a production-shaped memory slice using central authorization."""
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
        WorkspaceRole.EDITOR,
        MemberStatus.ACTIVE,
    )
    access.memberships[str(membership.meta.id)] = membership
    factory = InMemoryResumeUnitOfWorkFactory(
        access,
        store=InMemoryResumeStore(),
        templates=MappingResumeTemplateCatalog({TEMPLATE_REF: _policy()}),
    )
    service = ResumeApplicationService(factory, clock=_Clock(), id_factory=_Ids())
    return service, factory


def _principal() -> TokenPrincipal:
    """@brief 构造拥有 Resume read/write/render scopes 的 principal / Build a principal with Resume read/write/render scopes."""
    return TokenPrincipal(
        USER_ID,
        Subject("subject_00000001"),
        ClientId("client_00000001"),
        frozenset(
            {Scope("resume.read"), Scope("resume.write"), Scope("resume.render")}
        ),
    )


@pytest.mark.asyncio
async def test_memory_uow_serializes_concurrent_cas_and_rolls_back_uncommitted_work() -> None:
    """@brief 并发相同 If-Match 仅一项成功且未 commit 快照不泄漏 / Only one concurrent If-Match succeeds and uncommitted state never leaks."""
    service, factory = _fixture()
    created = await service.create_resume(
        _principal(),
        WORKSPACE_ID,
        CreateResumeCommand("Backend Resume", "zh-CN", TEMPLATE_REF),
    )

    results = await asyncio.gather(
        service.update_resume_metadata(
            _principal(),
            WORKSPACE_ID,
            created.meta.id,
            UpdateResumeMetadataCommand(title="Staff Resume"),
            expected_revision=1,
        ),
        service.update_resume_metadata(
            _principal(),
            WORKSPACE_ID,
            created.meta.id,
            UpdateResumeMetadataCommand(locale="en-US"),
            expected_revision=1,
        ),
        return_exceptions=True,
    )

    assert sum(not isinstance(item, BaseException) for item in results) == 1
    assert sum(isinstance(item, ResumePreconditionFailed) for item in results) == 1
    persisted = await service.get_resume(_principal(), WORKSPACE_ID, created.meta.id)
    assert persisted.meta.revision == 2

    before = dict(factory.store.uploads)
    factory.store.add_completed_upload(
        WORKSPACE_ID,
        "upload_00000001",
        completed_at=datetime.now(UTC),
        expires_at=datetime.now(UTC) + timedelta(hours=1),
    )
    async with factory() as uow:
        actor = await uow.authorizer.authenticate(_principal())
        await uow.authorizer.authorize(
            actor,
            WORKSPACE_ID,
            WorkspaceAction.CREATE_RESUME_IMPORT_JOB,
        )
        assert await uow.import_sources.claim(
            WORKSPACE_ID,
            "upload_00000001",
            JobId("job_00000001"),
        )
        # Deliberately do not commit.
    assert factory.store.uploads["upload_00000001"].claimed_by_job_id is None
    assert before == {}


@pytest.mark.asyncio
async def test_memory_reentry_rehydrates_revision_history_and_workspace_isolation() -> None:
    """@brief 跨 UoW 重入仍可读取完整 revision，跨 Workspace 不泄漏 / Re-entry preserves revisions and never leaks across Workspaces."""
    service, _ = _fixture()
    created = await service.create_resume(
        _principal(),
        WORKSPACE_ID,
        CreateResumeCommand("Backend Resume", "zh-CN", TEMPLATE_REF),
    )
    updated = await service.update_resume_metadata(
        _principal(),
        WORKSPACE_ID,
        created.meta.id,
        UpdateResumeMetadataCommand(title="Staff Resume"),
        expected_revision=1,
    )
    assert updated.meta.revision == 2
    history = await service.list_revisions(_principal(), WORKSPACE_ID, created.meta.id)
    assert [item.revision for item in history.items] == [1, 2]
    assert (
        await service.get_revision(_principal(), WORKSPACE_ID, created.meta.id, 1)
    ).document.title == "Backend Resume"
    with pytest.raises(AuthorizationDenied):
        await service.get_resume(
            _principal(),
            WorkspaceId("workspace_other_0001"),
            ResumeId(str(created.meta.id)),
        )


def test_0016_is_linear_audited_and_reuses_existing_business_tables() -> None:
    """@brief 0016 线性、非空可转换且不复制业务 truth / 0016 is linear, audited, and reuses business tables."""
    configuration = Config()
    configuration.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))
    scripts = ScriptDirectory.from_config(configuration)
    script = scripts.get_revision("20260723_0016")
    assert script is not None
    assert script.down_revision == "20260723_0015"
    heads = scripts.get_heads()
    assert len(heads) == 1
    assert script in scripts.iterate_revisions(heads[0], "base")

    source = MIGRATION.read_text(encoding="utf-8")
    assert "expand-backfill-validate-constrain" in source
    assert "resume-v1-to-v2/1" in source
    assert "v1_migration_archive" in source
    assert "payload_sha256" in source
    assert "reject_v1_migration_archive_mutation" in source
    assert "legacy Resume conversion failed at" in source
    assert "fixed template version FK" in source
    assert "cannot downgrade non-empty or irreversible API V2 Resume persistence state" in source
    for existing_table in (
        "documents",
        "revisions",
        "operation_batches",
        "operations",
        "proposals",
        "proposal_operations",
    ):
        assert f'op.create_table(\n        "{existing_table}"' not in source
    assert '"import_upload_sessions"' in source


def test_0016_encodes_rls_append_only_cas_receipts_and_conditional_upload_claim() -> None:
    """@brief source gates 固定 RLS、append-only、CAS、30 天 receipt 与条件 claim / Source gates pin RLS, append-only, CAS, retention, and conditional claim."""
    migration = MIGRATION.read_text(encoding="utf-8")
    infrastructure = (PROJECT_ROOT / "src" / "backend" / "infrastructure" / "resumes.py").read_text(
        encoding="utf-8"
    )

    assert "FORCE ROW LEVEL SECURITY" in migration
    assert "workspace_id = current_setting('app.workspace_id', true)" in migration
    assert "GRANT SELECT, INSERT ON TABLE" in migration
    assert "resume_v2_append_read" in migration
    assert "resume_v2_append_insert" in migration
    assert "resume_batches_v2_client_id" in migration
    assert "request_fingerprint" in migration
    assert "outcome" in migration
    assert "timedelta(days=30)" in infrastructure
    assert "current_revision_no == expected_revision" in infrastructure
    assert "_affected_rows(result) != 1" in infrastructure
    assert ".with_for_update()" in infrastructure
    assert 'status == "completed"' in infrastructure
    assert "claimed_by_job_id.is_(None)" in infrastructure
    assert "expires_at > now" in infrastructure
    assert "deferrable=True" in migration
    assert 'initially="DEFERRED"' in migration
    assert "self._database.new_session()" in infrastructure
    assert "self._database.session_factory()" not in infrastructure


def test_resume_orm_metadata_matches_0016_and_supports_160_character_v2_ids() -> None:
    """@brief ORM metadata 精确映射 0016 columns/constraints/indexes / ORM metadata exactly maps 0016 columns, constraints, and indexes."""
    assert cast(sa.String, ResumeDocumentRecord.__table__.c.id.type).length == 160
    assert cast(sa.String, JobRecord.__table__.c.id.type).length == 160
    assert cast(sa.String, ResumeProposalRecord.__table__.c.id.type).length == 160
    assert (
        cast(sa.String, ResumeProposalOperationRecord.__table__.c.operation_id.type).length == 160
    )
    assert cast(sa.String, ResumeImportUploadSessionRecord.__table__.c.id.type).length == 160
    assert {"template_id", "template_version"} <= set(ResumeDocumentRecord.__table__.c.keys())
    assert "change_targets" in ResumeRevisionRecord.__table__.c.keys()
    assert {"request_fingerprint", "outcome", "expires_at"} <= set(
        ResumeOperationBatchRecord.__table__.c.keys()
    )
    assert {"fingerprint", "applied_revision_no"} <= set(ResumeOperationRecord.__table__.c.keys())
    assert {"title", "evidence_refs"} <= set(ResumeProposalRecord.__table__.c.keys())
    assert "request_payload" in JobRecord.__table__.c.keys()
    receipt_table = cast(sa.Table, ResumeOperationBatchRecord.__table__)
    receipt_indexes = {item.name for item in receipt_table.indexes}
    assert "ix_resume_operation_batches_receipt_expiry" in receipt_indexes
    upload_table = cast(sa.Table, ResumeImportUploadSessionRecord.__table__)
    upload_indexes = {item.name for item in upload_table.indexes}
    assert {
        "ix_resume_import_upload_sessions_claimable",
        "ix_knowledge_upload_sessions_claimable",
    } & upload_indexes


def test_workspace_tenant_integrity_metadata_uses_real_database_names() -> None:
    """@brief Workspace metadata 声明真实 index/复合约束名称 / Workspace metadata declares real index and composite-constraint names."""
    workspace_table = cast(sa.Table, WorkspaceRecord.__table__)
    member_table = cast(sa.Table, WorkspaceMemberRecord.__table__)
    workspace_constraints = {item.name for item in workspace_table.constraints}
    workspace_indexes = {item.name for item in workspace_table.indexes}
    member_constraints = {item.name for item in member_table.constraints}
    member_indexes = {item.name for item in member_table.indexes}

    assert "uq_workspaces_id_resource_owner" in workspace_constraints
    assert "ix_workspaces_resource_owner_id_updated_at" in workspace_indexes
    assert "ix_workspaces_resource_owner_updated" not in workspace_indexes
    assert "uq_tnt_workspace_members_id_ws_owner" in member_constraints
    assert "fk_tnt_workspace_members_workspace_scope" in member_constraints
    assert "ix_workspace_members_workspace_id" in member_indexes


@dataclass(frozen=True, slots=True)
class _ResumePostgresHarness:
    """@brief 隔离 PostgreSQL V1→V2 Resume 迁移环境 / Isolated PostgreSQL Resume-migration environment."""

    port: int
    socket_dir: Path
    superuser: str

    @property
    def migration_dsn(self) -> str:
        """@brief 返回 Alembic asyncpg DSN / Return the Alembic asyncpg DSN."""
        return f"postgresql+asyncpg://aiws_migrator@127.0.0.1:{self.port}/aiws"

    @property
    def app_dsn(self) -> str:
        """@brief 返回 runtime asyncpg DSN / Return the runtime asyncpg DSN."""
        return f"postgresql+asyncpg://aiws_app@127.0.0.1:{self.port}/aiws"

    @property
    def super_dsn(self) -> str:
        """@brief 返回 superuser psycopg DSN / Return the superuser psycopg DSN."""
        return f"postgresql://{self.superuser}@127.0.0.1:{self.port}/aiws"

    def psql(self, binary: Path, sql: str, *, database: str = "aiws") -> None:
        """@brief 执行无参数 fixture SQL / Execute parameter-free fixture SQL."""
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

    def rows(self, statement: str) -> list[dict[str, Any]]:
        """@brief 以 superuser 读取验证行 / Read verification rows as superuser."""
        with psycopg.connect(self.super_dsn, row_factory=dict_row) as connection:
            return [dict(row) for row in connection.execute(statement).fetchall()]


def _postgres_binary(name: str) -> Path | None:
    """@brief 定位 PATH 或 Debian versioned PostgreSQL binary / Locate a PostgreSQL binary."""
    direct = shutil.which(name)
    if direct is not None:
        return Path(direct)
    candidates = sorted(Path("/usr/lib/postgresql").glob(f"*/bin/{name}"), reverse=True)
    return candidates[0] if candidates else None


def _migration_config(dsn: str) -> Config:
    """@brief 构造显式 Alembic 配置 / Build an explicit Alembic configuration."""
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


@pytest.fixture(scope="module")
def resume_postgres(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[_ResumePostgresHarness]:
    """@brief 启动有界 PostgreSQL，在 0015 灌数后升级 0016 / Start PostgreSQL and migrate seeded 0015 data."""
    initdb = _postgres_binary("initdb")
    pg_ctl = _postgres_binary("pg_ctl")
    psql = _postgres_binary("psql")
    if initdb is None or pg_ctl is None or psql is None:
        pytest.skip("PostgreSQL server binaries are unavailable")
    root = tmp_path_factory.mktemp("resume-postgres")
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
    harness = _ResumePostgresHarness(port, socket_dir, getpass.getuser())
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
            CREATE DATABASE aiws_empty OWNER aiws_migrator;
            GRANT CREATE ON DATABASE aiws_empty TO aiws_owner;
            CREATE DATABASE aiws_invalid OWNER aiws_migrator;
            GRANT CREATE ON DATABASE aiws_invalid TO aiws_owner;
            """,
            database="postgres",
        )
        try:
            harness.psql(psql, "CREATE EXTENSION vector;")
            harness.psql(psql, "CREATE EXTENSION vector;", database="aiws_empty")
            harness.psql(psql, "CREATE EXTENSION vector;", database="aiws_invalid")
        except subprocess.CalledProcessError:
            pytest.skip("the PostgreSQL vector extension is unavailable")
        configuration = _migration_config(harness.migration_dsn)
        command.upgrade(configuration, "20260723_0015")
        harness.psql(psql, _LEGACY_RESUME_FIXTURE_SQL)
        command.upgrade(configuration, "20260723_0016")
        yield harness
    finally:
        subprocess.run(
            [str(pg_ctl), "-D", str(data), "-w", "stop", "-m", "fast"],
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )


def test_0016_real_postgres_converts_nonempty_resume_truth_without_loss(
    resume_postgres: _ResumePostgresHarness,
) -> None:
    """@brief 真实 PostgreSQL 验证内容、关系、receipt、归档与审计 / Verify non-empty conversion truths."""
    document = resume_postgres.rows(
        "SELECT template_id, template_version, title, current_revision_no, revision, extensions "
        "FROM resume.documents WHERE id = 'resume_legacy0001'"
    )[0]
    assert document["template_id"] == "template_legacy01"
    assert document["template_version"] == "v1"
    assert document["title"] == "Legacy Resume"
    assert document["current_revision_no"] == document["revision"] == 2
    assert document["extensions"]["keep"] == "root"
    assert document["extensions"]["migration_0016"]["lossy_projection"] is True
    assert document["extensions"]["migration_0016"]["archive_id"].startswith("rv1arc_")

    revisions = resume_postgres.rows(
        "SELECT revision_no, semantic_document, content_hash, change_targets, extensions "
        "FROM resume.revisions WHERE resume_id = 'resume_legacy0001' ORDER BY revision_no"
    )
    assert [item["revision_no"] for item in revisions] == [1, 2]
    assert revisions[1]["semantic_document"]["meta"]["revision"] == 2
    assert revisions[1]["semantic_document"]["title"] == "Legacy Resume"
    assert revisions[1]["semantic_document"]["profile"]["headline"] == "Senior Engineer"
    assert revisions[1]["semantic_document"]["sections"][0]["kind"] == "custom"
    assert revisions[1]["semantic_document"]["sections"][0]["title"] == _LEGACY_SECTION_TITLE[:120]
    assert revisions[1]["extensions"]["migration_0016"]["lossy_projection"] is True
    assert revisions[1]["semantic_document"]["template"] == {
        "template_id": "template_legacy01",
        "version": "v1",
    }
    assert revisions[1]["change_targets"] == [{"entity_id": "resume_legacy0001", "field_path": []}]
    assert all(len(item["content_hash"]) == 64 for item in revisions)

    ledger = resume_postgres.rows(
        "SELECT operation_id, payload, fingerprint, applied_revision_no "
        "FROM resume.operations WHERE operation_id = 'operation_legacy01'"
    )[0]
    assert ledger["payload"]["entity_id"] == "resume_legacy0001"
    assert ledger["payload"]["field_path"] == ["profile", "headline"]
    assert ledger["applied_revision_no"] == 2
    assert ledger["fingerprint"] == _canonical_sha256(ledger["payload"])
    receipt = resume_postgres.rows(
        "SELECT request_fingerprint, outcome, expires_at FROM resume.operation_batches "
        "WHERE id = 'batch_legacy00001'"
    )[0]
    assert receipt["request_fingerprint"] == _canonical_sha256(
        {
            "client_batch_id": "batch_client0001",
            "base_revision": 1,
            "conflict_strategy": "reject",
            "operations": [ledger["payload"]],
            "render_hint": "none",
        }
    )
    assert receipt["outcome"]["resume"]["meta"]["revision"] == 2
    assert receipt["outcome"]["applied_operation_ids"] == ["operation_legacy01"]
    assert receipt["expires_at"] > datetime.now(UTC) + timedelta(days=29)
    quarantined = resume_postgres.rows(
        "SELECT client_batch_id, request_fingerprint, outcome, extensions "
        "FROM resume.operation_batches WHERE id = 'batch_pending0001'"
    )[0]
    assert quarantined["client_batch_id"].startswith("rblegacy_")
    assert quarantined["client_batch_id"] != "batch_pending0001"
    assert quarantined["request_fingerprint"] is None
    assert quarantined["outcome"] is None
    assert (
        quarantined["extensions"]["migration_0016"]["classification"]
        == "legacy_unapplied_quarantined"
    )

    proposal = resume_postgres.rows(
        "SELECT title, status, evidence_refs FROM resume.proposals WHERE id = 'proposal_legacy001'"
    )[0]
    assert proposal == {
        "title": "Promote headline",
        "status": "pending",
        "evidence_refs": [],
    }
    proposal_operation = resume_postgres.rows(
        "SELECT operation_id, payload, fingerprint FROM resume.proposal_operations "
        "WHERE id = 'proposalop_legacy01'"
    )[0]
    assert proposal_operation["operation_id"] == "operation_proposal1"
    assert proposal_operation["payload"]["field_path"] == ["profile", "headline"]
    assert len(proposal_operation["fingerprint"]) == 64

    job = resume_postgres.rows(
        "SELECT status, phase, target_resource_type, target_resource_id, request_payload "
        "FROM agent.jobs WHERE id = 'job_legacyresume1'"
    )[0]
    assert job["status"] == "failed"
    assert job["phase"] == "migration_terminal"
    assert (job["target_resource_type"], job["target_resource_id"]) == (
        "resume",
        "resume_legacy0001",
    )
    assert job["request_payload"]["migration"]["classification"] == "terminal_unreplayable"

    event = resume_postgres.rows(
        "SELECT payload FROM agent.outbox_events WHERE id = 'event_legacyresume1'"
    )[0]["payload"]
    assert event["data"] == {"legacy": "keep", "revision": 2}
    archives = resume_postgres.rows(
        "SELECT source_table, source_row_id, converter_version, payload_sha256, source_payload "
        "FROM resume.v1_migration_archive ORDER BY source_table, source_row_id"
    )
    assert len(archives) == 10
    assert {item["converter_version"] for item in archives} == {"resume-v1-to-v2/1"}
    assert all(
        item["payload_sha256"] == _canonical_sha256(item["source_payload"]) for item in archives
    )
    archived_revision = next(
        item
        for item in archives
        if item["source_table"] == "resume.revisions"
        and item["source_row_id"] == "revision_legacy0002"
    )
    assert archived_revision["source_payload"]["semantic_document"]["extensions"] == {
        "v1_only": {"keep": "all content"}
    }
    assert (
        archived_revision["source_payload"]["semantic_document"]["sections"][0]["title"]
        == _LEGACY_SECTION_TITLE
    )
    archived_pending_batch = next(
        item
        for item in archives
        if item["source_table"] == "resume.operation_batches"
        and item["source_row_id"] == "batch_pending0001"
    )
    assert archived_pending_batch["source_payload"]["client_batch_id"] == "batch_pending0001"
    audits = resume_postgres.rows(
        "SELECT event_type, phase, source_snapshot_sha256, details "
        "FROM identity.api_migration_audits "
        "WHERE migration_id = '20260723_0016_resume_v1_to_v2' ORDER BY phase"
    )
    assert [(item["event_type"], item["phase"]) for item in audits] == [
        ("backup_created", 0),
        ("started", 1),
        ("verified", 4),
        ("completed", 5),
    ]
    assert len({item["source_snapshot_sha256"] for item in audits}) == 1

    with psycopg.connect(resume_postgres.super_dsn) as connection:
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
            connection.execute(
                "UPDATE resume.v1_migration_archive SET converter_version = 'tampered'"
            )


@pytest.mark.asyncio
async def test_0016_migrated_resume_is_readable_through_v2_postgres_adapter(
    resume_postgres: _ResumePostgresHarness,
) -> None:
    """@brief 迁移后的 Resume 可由真实 V2 adapter 重建 / Rehydrate migrated data through the V2 adapter."""
    database = AsyncDatabase(
        AsyncDatabaseOptions(
            resume_postgres.app_dsn,
            pool_size=1,
            max_overflow=0,
        )
    )
    factory = PostgresResumeUnitOfWorkFactory(database)
    service = ResumeApplicationService(factory, clock=_Clock(), id_factory=_Ids())
    principal = TokenPrincipal(
        UserId("user_legacy0001"),
        Subject("legacy-subject"),
        ClientId("client_legacy0001"),
        frozenset({Scope("resume.read"), Scope("resume.write")}),
    )
    try:
        resume = await service.get_resume(
            principal,
            WorkspaceId("workspace_legacy0001"),
            ResumeId("resume_legacy0001"),
        )
        assert resume.meta.revision == 2
        assert resume.profile.headline == "Senior Engineer"
        assert resume.sections[0].id == "section_legacy0001"
        proposals = await service.list_proposals(
            principal,
            WorkspaceId("workspace_legacy0001"),
            ResumeId("resume_legacy0001"),
        )
        assert proposals.items[0].operations[0].operation_id == "operation_proposal1"
    finally:
        await database.aclose()


def test_0016_converted_nonempty_state_reaches_current_alembic_head(
    resume_postgres: _ResumePostgresHarness,
) -> None:
    """@brief 非空转换结果可继续升级到当前 head / Converted non-empty state upgrades to the current head."""
    configuration = _migration_config(resume_postgres.migration_dsn)
    command.upgrade(configuration, "head")
    scripts = ScriptDirectory.from_config(configuration)
    assert resume_postgres.rows("SELECT version_num FROM identity.alembic_version") == [
        {"version_num": scripts.get_current_head()}
    ]
    preserved = resume_postgres.rows(
        "SELECT semantic_document FROM resume.revisions WHERE id = 'revision_legacy0002'"
    )[0]["semantic_document"]
    assert preserved["profile"]["headline"] == "Senior Engineer"
    assert resume_postgres.rows("SELECT count(*) AS count FROM resume.v1_migration_archive") == [
        {"count": 10}
    ]


@pytest.mark.asyncio
async def test_postgres_resume_outbox_dispatch_renders_and_replays_terminal_job_idempotently(
    resume_postgres: _ResumePostgresHarness,
) -> None:
    """@brief 真实 PG 证明 Job-kind 分派、两段事务、Artifact 原子提交与终态重放 / Prove persisted-kind dispatch, two phases, atomic artifacts, and terminal replay on PostgreSQL."""
    configuration = _migration_config(resume_postgres.migration_dsn)
    await asyncio.to_thread(command.upgrade, configuration, "head")
    database = AsyncDatabase(
        AsyncDatabaseOptions(
            resume_postgres.app_dsn,
            pool_size=2,
            max_overflow=0,
        )
    )
    catalog = MappingResumeTemplateCatalog({TEMPLATE_REF: _policy()})
    identifiers = _Ids()
    service = ResumeApplicationService(
        PostgresResumeUnitOfWorkFactory(database, templates=catalog),
        clock=_Clock(),
        id_factory=identifiers,
    )
    principal = TokenPrincipal(
        UserId("user_legacy0001"),
        Subject("legacy-subject"),
        ClientId("client_legacy0001"),
        frozenset(
            {Scope("resume.read"), Scope("resume.write"), Scope("resume.render")}
        ),
    )
    upload_reader = _BytesUploadReader({})
    worker = ResumeJobWorkerService(
        PostgresResumeWorkerUnitOfWorkFactory(database, templates=catalog),
        SafeResumeImporter(upload_reader),
        MultiFormatResumeRenderer(MockRenderer()),
        clock=_Clock(),
    )
    settings = OutboxDispatchSettings(
        batch_size=10,
        lease_seconds=30,
        maximum_attempts=3,
        retry_base_seconds=1,
        retry_cap_seconds=2,
    )
    handler = ResumeJobOutboxHandler(
        worker,
        maximum_attempts=settings.maximum_attempts,
    )
    dispatcher = OutboxDispatchService(
        PostgresOutboxClaimRepository(
            database,
            event_types=RESUME_WORK_EVENT_TYPES,
        ),
        {event_type: handler for event_type in RESUME_WORK_EVENT_TYPES},
        required_event_types=RESUME_WORK_EVENT_TYPES,
        settings=settings,
    )
    try:
        document = await service.create_resume(
            principal,
            WorkspaceId("workspace_legacy0001"),
            CreateResumeCommand("Worker Resume", "en-US", TEMPLATE_REF),
        )
        job = await service.create_render_job(
            principal,
            WorkspaceId("workspace_legacy0001"),
            document.meta.id,
            CreateRenderJobCommand(
                1,
                RenderMode.FINAL,
                (RenderFormat.PDF, RenderFormat.JSON, RenderFormat.DOCX),
            ),
        )
        with psycopg.connect(resume_postgres.super_dsn) as connection:
            connection.execute(
                "UPDATE agent.outbox_events SET payload = "
                "jsonb_set(payload, '{data,kind}', '\"resume.import\"'::jsonb) "
                "WHERE aggregate_type = 'job' AND aggregate_id = %s",
                (str(job.meta.id),),
            )
            connection.commit()
        first = await dispatcher.run_once()
        assert (first.claimed, first.completed, first.retried, first.failed) == (1, 1, 0, 0), (
            resume_postgres.rows(
                "SELECT status, phase, revision, problem FROM agent.jobs "
                f"WHERE id = '{job.meta.id}'"
            ),
            resume_postgres.rows(
                "SELECT status, attempt_count, last_error_code FROM agent.outbox_events "
                f"WHERE aggregate_id = '{job.meta.id}'"
            ),
        )
        state = resume_postgres.rows(
            "SELECT status, phase, revision, result_refs, problem FROM agent.jobs "
            f"WHERE id = '{job.meta.id}'"
        )[0]
        assert state["status"] == "succeeded"
        assert state["phase"] == "completed"
        assert state["revision"] == 3
        assert state["problem"] is None
        assert len(state["result_refs"]) == 3
        artifacts = resume_postgres.rows(
            "SELECT kind, media_type, size_bytes, sha256, subject_id, subject_revision "
            "FROM agent.artifacts "
            f"WHERE subject_id = '{document.meta.id}' ORDER BY kind"
        )
        assert [item["kind"] for item in artifacts] == [
            "resume_docx",
            "resume_json",
            "resume_pdf",
        ]
        assert all(item["size_bytes"] > 0 and len(item["sha256"]) == 64 for item in artifacts)
        assert resume_postgres.rows(
            "SELECT count(*) AS count FROM agent.artifact_contents content "
            "JOIN agent.artifacts artifact ON artifact.id = content.artifact_id "
            f"WHERE artifact.subject_id = '{document.meta.id}'"
        ) == [{"count": 3}]
        assert resume_postgres.rows(
            "SELECT count(*) AS count FROM agent.artifact_pdf_source_maps source_map "
            "JOIN agent.artifacts artifact ON artifact.id = source_map.artifact_id "
            f"WHERE artifact.subject_id = '{document.meta.id}'"
        ) == [{"count": 1}]
        assert resume_postgres.rows(
            "SELECT count(*) AS count FROM resume.render_jobs "
            f"WHERE job_id = '{job.meta.id}'"
        ) == [{"count": 1}]

        event_id = resume_postgres.rows(
            "SELECT id FROM agent.outbox_events "
            f"WHERE aggregate_type = 'job' AND aggregate_id = '{job.meta.id}'"
        )[0]["id"]
        with psycopg.connect(resume_postgres.super_dsn) as connection:
            connection.execute(
                "UPDATE agent.outbox_events SET status = 'pending', published_at = NULL, "
                "lease_token_hash = NULL, lease_expires_at = NULL, "
                "next_attempt_at = statement_timestamp() "
                "WHERE id = %s",
                (event_id,),
            )
            connection.commit()
        replay = await dispatcher.run_once()
        assert (replay.claimed, replay.completed, replay.retried, replay.failed) == (1, 1, 0, 0)
        assert resume_postgres.rows(
            "SELECT count(*) AS count FROM agent.artifacts "
            f"WHERE subject_id = '{document.meta.id}'"
        ) == [{"count": 3}]
        assert resume_postgres.rows(
            "SELECT status, revision FROM agent.jobs "
            f"WHERE id = '{job.meta.id}'"
        ) == [{"status": "succeeded", "revision": 3}]

        upload_id = "upload_worker0001"
        imported_bytes = b"Klee Example\nDistributed systems engineer"
        imported_sha256 = hashlib.sha256(imported_bytes).hexdigest()
        upload_reader.values[
            (WorkspaceId("workspace_legacy0001"), UploadSessionId(upload_id))
        ] = imported_bytes
        with psycopg.connect(resume_postgres.super_dsn) as connection:
            connection.execute(
                """
                INSERT INTO knowledge.upload_sessions (
                    id, workspace_id, status, filename, media_type, expires_at,
                    completion_size_bytes, completion_sha256,
                    verification_operation_id, completed_at,
                    artifact_type, artifact_id, artifact_revision, legacy_payload,
                    created_at, updated_at, revision, extensions
                ) VALUES (
                    %s, %s, 'completed', 'resume.txt', 'text/plain',
                    statement_timestamp() + interval '1 day', %s, %s,
                    'verify_worker0001', statement_timestamp(),
                    'upload_artifact', 'upload_artifact_worker0001', 1, true,
                    statement_timestamp(), statement_timestamp(), 1, '{}'::jsonb
                )
                """,
                (
                    upload_id,
                    "workspace_legacy0001",
                    len(imported_bytes),
                    imported_sha256,
                ),
            )
            connection.commit()
        import_job = await service.create_import_job(
            principal,
            WorkspaceId("workspace_legacy0001"),
            CreateResumeImportJobCommand(
                upload_id,
                "Imported Worker Resume",
                "en-US",
                TEMPLATE_REF,
            ),
        )
        imported_dispatch = await dispatcher.run_once()
        assert (
            imported_dispatch.claimed,
            imported_dispatch.completed,
            imported_dispatch.retried,
            imported_dispatch.failed,
        ) == (1, 1, 0, 0)
        imported_job = resume_postgres.rows(
            "SELECT status, revision, result_refs FROM agent.jobs "
            f"WHERE id = '{import_job.meta.id}'"
        )[0]
        assert imported_job["status"] == "succeeded"
        assert imported_job["revision"] == 3
        imported_resume_id = imported_job["result_refs"][0]["id"]
        imported_document = resume_postgres.rows(
            "SELECT semantic_document FROM resume.revisions "
            f"WHERE resume_id = '{imported_resume_id}' AND revision_no = 1"
        )[0]["semantic_document"]
        assert imported_document["profile"]["full_name"] == "Klee Example"
        assert imported_document["profile"]["summary"]["text"] == imported_bytes.decode()

        updated = await service.update_resume_metadata(
            principal,
            WorkspaceId("workspace_legacy0001"),
            document.meta.id,
            UpdateResumeMetadataCommand(title="Changed after snapshot"),
            expected_revision=1,
        )
        assert updated.meta.revision == 2
        restore_job = await service.create_restore_job(
            principal,
            WorkspaceId("workspace_legacy0001"),
            document.meta.id,
            CreateRestoreJobCommand(1),
            expected_revision=2,
        )
        restored_dispatch = await dispatcher.run_once()
        assert (
            restored_dispatch.claimed,
            restored_dispatch.completed,
            restored_dispatch.retried,
            restored_dispatch.failed,
        ) == (1, 1, 0, 0)
        restored = resume_postgres.rows(
            "SELECT title, current_revision_no, revision FROM resume.documents "
            f"WHERE id = '{document.meta.id}'"
        )[0]
        assert restored == {
            "title": "Worker Resume",
            "current_revision_no": 3,
            "revision": 3,
        }
        assert resume_postgres.rows(
            "SELECT status, revision FROM agent.jobs "
            f"WHERE id = '{restore_job.meta.id}'"
        ) == [{"status": "succeeded", "revision": 3}]

        flaky_renderer = _FlakyResumeRenderer(MultiFormatResumeRenderer(MockRenderer()))
        flaky_worker = ResumeJobWorkerService(
            PostgresResumeWorkerUnitOfWorkFactory(database, templates=catalog),
            SafeResumeImporter(upload_reader),
            flaky_renderer,
            clock=_Clock(),
        )
        flaky_handler = ResumeJobOutboxHandler(
            flaky_worker,
            maximum_attempts=settings.maximum_attempts,
        )
        flaky_dispatcher = OutboxDispatchService(
            PostgresOutboxClaimRepository(
                database,
                event_types=RESUME_WORK_EVENT_TYPES,
            ),
            {event_type: flaky_handler for event_type in RESUME_WORK_EVENT_TYPES},
            required_event_types=RESUME_WORK_EVENT_TYPES,
            settings=settings,
        )
        crash_job = await service.create_render_job(
            principal,
            WorkspaceId("workspace_legacy0001"),
            document.meta.id,
            CreateRenderJobCommand(3, RenderMode.PREVIEW, (RenderFormat.PDF,)),
        )
        interrupted = await flaky_dispatcher.run_once()
        assert (
            interrupted.claimed,
            interrupted.completed,
            interrupted.retried,
            interrupted.failed,
        ) == (1, 0, 1, 0)
        assert resume_postgres.rows(
            "SELECT status, revision FROM agent.jobs "
            f"WHERE id = '{crash_job.meta.id}'"
        ) == [{"status": "running", "revision": 2}]
        with psycopg.connect(resume_postgres.super_dsn) as connection:
            connection.execute(
                "UPDATE agent.outbox_events SET next_attempt_at = statement_timestamp() "
                "WHERE aggregate_type = 'job' AND aggregate_id = %s",
                (str(crash_job.meta.id),),
            )
            connection.commit()
        resumed = await flaky_dispatcher.run_once()
        assert (resumed.claimed, resumed.completed, resumed.retried, resumed.failed) == (
            1,
            1,
            0,
            0,
        )
        expected_operation_id = f"resume.render:{crash_job.meta.id}"
        assert flaky_renderer.operation_ids == [expected_operation_id, expected_operation_id]
        assert resume_postgres.rows(
            "SELECT status, revision FROM agent.jobs "
            f"WHERE id = '{crash_job.meta.id}'"
        ) == [{"status": "succeeded", "revision": 3}]

        unsupported_worker = ResumeJobWorkerService(
            PostgresResumeWorkerUnitOfWorkFactory(database, templates=catalog),
            SafeResumeImporter(upload_reader),
            _UnsupportedResumeRenderer(),
            clock=_Clock(),
        )
        unsupported_handler = ResumeJobOutboxHandler(
            unsupported_worker,
            maximum_attempts=settings.maximum_attempts,
        )
        unsupported_dispatcher = OutboxDispatchService(
            PostgresOutboxClaimRepository(
                database,
                event_types=RESUME_WORK_EVENT_TYPES,
            ),
            {event_type: unsupported_handler for event_type in RESUME_WORK_EVENT_TYPES},
            required_event_types=RESUME_WORK_EVENT_TYPES,
            settings=settings,
        )
        unsupported_job = await service.create_render_job(
            principal,
            WorkspaceId("workspace_legacy0001"),
            document.meta.id,
            CreateRenderJobCommand(3, RenderMode.FINAL, (RenderFormat.PDF,)),
        )
        unsupported = await unsupported_dispatcher.run_once()
        assert (
            unsupported.claimed,
            unsupported.completed,
            unsupported.retried,
            unsupported.failed,
        ) == (1, 1, 0, 0)
        unsupported_state = resume_postgres.rows(
            "SELECT status, revision, problem FROM agent.jobs "
            f"WHERE id = '{unsupported_job.meta.id}'"
        )[0]
        assert unsupported_state["status"] == "failed"
        assert unsupported_state["revision"] == 3
        assert unsupported_state["problem"]["code"] == "resume.render_format_unsupported"
        assert unsupported_state["problem"]["retryable"] is False

        future_kind_job = await service.create_render_job(
            principal,
            WorkspaceId("workspace_legacy0001"),
            document.meta.id,
            CreateRenderJobCommand(3, RenderMode.FINAL, (RenderFormat.PDF,)),
        )
        with psycopg.connect(resume_postgres.super_dsn) as connection:
            connection.execute(
                "UPDATE agent.jobs SET job_type = 'resume.future_kind' WHERE id = %s",
                (str(future_kind_job.meta.id),),
            )
            connection.commit()
        future_kind_dispatch = await unsupported_dispatcher.run_once()
        assert (
            future_kind_dispatch.claimed,
            future_kind_dispatch.completed,
            future_kind_dispatch.retried,
            future_kind_dispatch.failed,
        ) == (1, 1, 0, 0)
        future_kind_state = resume_postgres.rows(
            "SELECT status, revision, problem FROM agent.jobs "
            f"WHERE id = '{future_kind_job.meta.id}'"
        )[0]
        assert future_kind_state["status"] == "failed"
        assert future_kind_state["revision"] == 3
        assert future_kind_state["problem"]["code"] == "resume.job_kind_unsupported"
        assert future_kind_state["problem"]["retryable"] is False

        source_map_defects: tuple[_SourceMapDefect, ...] = (
            "artifact_id",
            "resume_id",
            "resume_revision",
            "page_count",
            "page",
            "rect",
            "field_path",
        )
        for defect in source_map_defects:
            malformed_worker = ResumeJobWorkerService(
                PostgresResumeWorkerUnitOfWorkFactory(database, templates=catalog),
                SafeResumeImporter(upload_reader),
                MultiFormatResumeRenderer(_MalformedSourceMapPdfRenderer(defect)),
                clock=_Clock(),
            )
            malformed_handler = ResumeJobOutboxHandler(
                malformed_worker,
                maximum_attempts=settings.maximum_attempts,
            )
            malformed_dispatcher = OutboxDispatchService(
                PostgresOutboxClaimRepository(
                    database,
                    event_types=RESUME_WORK_EVENT_TYPES,
                ),
                {
                    event_type: malformed_handler
                    for event_type in RESUME_WORK_EVENT_TYPES
                },
                required_event_types=RESUME_WORK_EVENT_TYPES,
                settings=settings,
            )
            malformed_job = await service.create_render_job(
                principal,
                WorkspaceId("workspace_legacy0001"),
                document.meta.id,
                CreateRenderJobCommand(3, RenderMode.FINAL, (RenderFormat.PDF,)),
            )
            malformed = await malformed_dispatcher.run_once()
            assert (
                malformed.claimed,
                malformed.completed,
                malformed.retried,
                malformed.failed,
            ) == (1, 1, 0, 0), defect
            malformed_state = resume_postgres.rows(
                "SELECT status, revision, problem FROM agent.jobs "
                f"WHERE id = '{malformed_job.meta.id}'"
            )[0]
            assert malformed_state["status"] == "failed", defect
            assert malformed_state["revision"] == 3, defect
            assert malformed_state["problem"]["code"] == "resume.source_map_invalid", defect
            assert malformed_state["problem"]["retryable"] is False, defect
            malformed_artifact_id = resume_worker_artifact_id(
                f"resume.render:{malformed_job.meta.id}",
                RenderFormat.PDF,
            )
            assert resume_postgres.rows(
                "SELECT count(*) AS count FROM agent.artifacts "
                f"WHERE id = '{malformed_artifact_id}'"
            ) == [{"count": 0}], defect
            assert resume_postgres.rows(
                "SELECT count(*) AS count FROM agent.artifact_contents "
                f"WHERE artifact_id = '{malformed_artifact_id}'"
            ) == [{"count": 0}], defect
            assert resume_postgres.rows(
                "SELECT count(*) AS count FROM agent.artifact_pdf_source_maps "
                f"WHERE artifact_id = '{malformed_artifact_id}'"
            ) == [{"count": 0}], defect
            assert resume_postgres.rows(
                "SELECT count(*) AS count FROM resume.render_jobs "
                f"WHERE job_id = '{malformed_job.meta.id}'"
            ) == [{"count": 0}], defect

        malformed_event_job = await service.create_render_job(
            principal,
            WorkspaceId("workspace_legacy0001"),
            document.meta.id,
            CreateRenderJobCommand(3, RenderMode.FINAL, (RenderFormat.PDF,)),
        )
        with psycopg.connect(resume_postgres.super_dsn) as connection:
            connection.execute(
                "UPDATE agent.outbox_events SET payload = '{}'::jsonb "
                "WHERE aggregate_type = 'job' AND aggregate_id = %s",
                (str(malformed_event_job.meta.id),),
            )
            connection.commit()
        outcomes = []
        for attempt in range(settings.maximum_attempts):
            if attempt:
                with psycopg.connect(resume_postgres.super_dsn) as connection:
                    connection.execute(
                        "UPDATE agent.outbox_events "
                        "SET next_attempt_at = statement_timestamp() "
                        "WHERE aggregate_type = 'job' AND aggregate_id = %s",
                        (str(malformed_event_job.meta.id),),
                    )
                    connection.commit()
            outcomes.append(await dispatcher.run_once())
        assert [(item.retried, item.failed) for item in outcomes] == [
            (1, 0),
            (1, 0),
            (0, 1),
        ]
        assert resume_postgres.rows(
            "SELECT status, problem ->> 'code' AS code FROM agent.jobs "
            f"WHERE id = '{malformed_event_job.meta.id}'"
        ) == [
            {
                "status": "failed",
                "code": "resume.worker_attempts_exhausted",
            }
        ]
        assert resume_postgres.rows(
            "SELECT status FROM agent.outbox_events "
            f"WHERE aggregate_type = 'job' AND aggregate_id = '{malformed_event_job.meta.id}' "
            "AND event_type = 'resume.job_created'"
        ) == [{"status": "failed"}]
    finally:
        await database.aclose()


def test_0016_empty_upgrade_downgrade_restores_legacy_proposal_constraint(
    resume_postgres: _ResumePostgresHarness,
) -> None:
    """@brief 空库回退精确恢复 V1 conflicted 状态约束 / Empty rollback restores the V1 conflicted-status constraint."""
    migration_dsn = (
        f"postgresql+asyncpg://aiws_migrator@127.0.0.1:{resume_postgres.port}/aiws_empty"
    )
    configuration = _migration_config(migration_dsn)
    command.upgrade(configuration, "20260723_0016")
    command.downgrade(configuration, "20260723_0015")

    database_dsn = (
        f"postgresql://{resume_postgres.superuser}@127.0.0.1:{resume_postgres.port}/aiws_empty"
    )
    with psycopg.connect(database_dsn, row_factory=dict_row) as connection:
        version = connection.execute("SELECT version_num FROM identity.alembic_version").fetchone()
        constraint = connection.execute(
            "SELECT pg_get_constraintdef(oid) AS definition FROM pg_constraint "
            "WHERE conname = 'resume_proposals_status' "
            "AND conrelid = 'resume.proposals'::regclass"
        ).fetchone()
        archive = connection.execute("SELECT to_regclass('resume.v1_migration_archive')").fetchone()
    assert version is not None and version["version_num"] == "20260723_0015"
    assert constraint is not None and "conflicted" in constraint["definition"]
    assert archive is not None and archive["to_regclass"] is None


def test_0016_invalid_legacy_snapshot_fails_closed_with_exact_row_locator(
    resume_postgres: _ResumePostgresHarness,
) -> None:
    """@brief 非法 V1 快照定位到表、行与字段并整体回滚 / Invalid V1 snapshot is located and atomically rolled back."""
    migration_dsn = (
        f"postgresql+asyncpg://aiws_migrator@127.0.0.1:{resume_postgres.port}/aiws_invalid"
    )
    configuration = _migration_config(migration_dsn)
    command.upgrade(configuration, "20260723_0015")
    psql = _postgres_binary("psql")
    assert psql is not None
    resume_postgres.psql(psql, _LEGACY_RESUME_FIXTURE_SQL, database="aiws_invalid")
    resume_postgres.psql(
        psql,
        "UPDATE resume.revisions SET semantic_document = "
        "jsonb_set(semantic_document, '{schema_version}', '\"0.9\"'::jsonb) "
        "WHERE id = 'revision_legacy0002';",
        database="aiws_invalid",
    )

    with pytest.raises(
        RuntimeError,
        match=(
            r"resume\.revisions\[revision_legacy0002\]\."
            r"semantic_document\.schema_version"
        ),
    ):
        command.upgrade(configuration, "20260723_0016")

    database_dsn = (
        f"postgresql://{resume_postgres.superuser}@127.0.0.1:{resume_postgres.port}/aiws_invalid"
    )
    with psycopg.connect(database_dsn, row_factory=dict_row) as connection:
        version = connection.execute("SELECT version_num FROM identity.alembic_version").fetchone()
        archive = connection.execute("SELECT to_regclass('resume.v1_migration_archive')").fetchone()
    assert version is not None and version["version_num"] == "20260723_0015"
    assert archive is not None and archive["to_regclass"] is None
