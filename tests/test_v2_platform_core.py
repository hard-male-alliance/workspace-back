"""@brief API v2 5.6 通用平台领域与应用核心测试 / API v2 platform-core tests."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from types import TracebackType

import pytest

from backend.application.platform import (
    ArtifactContentIntegrityError,
    EventStreamInvariantError,
    PlatformApplicationService,
    PlatformConflict,
    PlatformIsolationViolation,
    PlatformPreconditionFailed,
)
from backend.application.ports.platform import (
    ArtifactContentStream,
    ArtifactDownload,
    ArtifactQuery,
    ByteRangeRequest,
    CollectionPage,
    ContentRange,
    EventReplayRequest,
    EventReplayWindowExpired,
    JobCasMismatch,
    JobQuery,
    MutationContext,
    PageRequest,
    PlatformAuthorizationRequest,
    PlatformPermission,
    PlatformResourceTarget,
    PlatformTargetKind,
    RangeNotSatisfiable,
    SubjectFilter,
)
from backend.domain.platform import (
    ApiArtifactContentUrl,
    ApiEvent,
    ApiEventId,
    Artifact,
    ArtifactId,
    ArtifactKind,
    AuditEvent,
    AuditEventId,
    AuditOutcome,
    Job,
    JobId,
    JobProgress,
    JobProgressUnit,
    JobStatus,
    JobTransitionError,
    PdfRect,
    PdfSourceMap,
    PdfSourceNode,
    PlatformDomainError,
    ProblemDetails,
    ResourceRef,
    SignedArtifactContentUrl,
)
from backend.domain.principals import (
    AuthenticatedActor,
    ClientId,
    MembershipId,
    ResourceMeta,
    Scope,
    Subject,
    TokenPrincipal,
    UserId,
    WorkspaceAccessContext,
    WorkspaceAction,
    WorkspaceId,
    _issue_workspace_access_context,
)
from backend.domain.workspaces import WorkspaceRole

NOW = datetime(2026, 7, 23, 12, 0, tzinfo=UTC)
"""@brief 测试固定时刻 / Fixed test instant."""

WORKSPACE_ID = WorkspaceId("ws_00000001")
"""@brief 主测试 Workspace / Primary test Workspace."""

OTHER_WORKSPACE_ID = WorkspaceId("ws_00000002")
"""@brief 隔离测试 Workspace / Isolation-test Workspace."""

USER_ID = UserId("user_00000001")
"""@brief 测试用户 / Test user."""

JOB_ID = JobId("job_00000001")
"""@brief 测试 Job / Test Job."""

ARTIFACT_ID = ArtifactId("artifact_00000001")
"""@brief 测试 Artifact / Test Artifact."""

RESUME_ID = "resume_00000001"
"""@brief 测试 Resume ID / Test Resume ID."""

PAYLOAD = b"contract-aligned-artifact"
"""@brief 测试 Artifact bytes / Test Artifact bytes."""

PAYLOAD_SHA256 = sha256(PAYLOAD).hexdigest()
"""@brief 测试 Artifact 摘要 / Test Artifact digest."""


class FixedClock:
    """@brief 可控测试时钟 / Controllable test clock."""

    def __init__(self, value: datetime = NOW + timedelta(seconds=1)) -> None:
        """@brief 初始化固定时刻 / Initialize the fixed instant.

        @param value 当前时刻 / Current instant.
        """
        self.value = value

    def now(self) -> datetime:
        """@brief 返回当前测试时刻 / Return the current test instant.

        @return 固定时刻 / Fixed instant.
        """
        return self.value


def _principal() -> TokenPrincipal:
    """@brief 构造已验证 principal / Build a verified principal.

    @return 测试 principal / Test principal.
    """
    return TokenPrincipal(
        USER_ID,
        Subject("subject_00000001"),
        ClientId("client_00000001"),
        frozenset({Scope("workspace.read"), Scope("resume.write")}),
    )


def _problem() -> ProblemDetails:
    """@brief 构造契约有效 Job problem / Build a contract-valid Job problem.

    @return 结构化 problem / Structured problem.
    """
    return ProblemDetails(
        "https://api.hmalliances.org:8022/problems/job/render-failed",
        "Render failed",
        503,
        "job.render_failed",
        "request_00000001",
        True,
        detail="The renderer did not complete.",
    )


def _job(
    *,
    workspace_id: WorkspaceId = WORKSPACE_ID,
    job_id: JobId = JOB_ID,
) -> Job:
    """@brief 构造 queued Job / Build a queued Job.

    @param workspace_id 所属 Workspace / Owning Workspace.
    @param job_id Job 标识 / Job identifier.
    @return queued Job / Queued Job.
    """
    return Job(
        ResourceMeta(job_id, 1, NOW, NOW),
        workspace_id,
        "resume.render",
        ResourceRef("resume", RESUME_ID, 7),
    )


def _artifact(
    *,
    workspace_id: WorkspaceId = WORKSPACE_ID,
    artifact_id: ArtifactId = ARTIFACT_ID,
    digest: str = PAYLOAD_SHA256,
) -> Artifact:
    """@brief 构造同源 PDF Artifact / Build a same-origin PDF Artifact.

    @param workspace_id 所属 Workspace / Owning Workspace.
    @param artifact_id Artifact 标识 / Artifact identifier.
    @param digest 内容摘要 / Content digest.
    @return Artifact metadata / Artifact metadata.
    """
    return Artifact(
        ResourceMeta(artifact_id, 1, NOW, NOW),
        workspace_id,
        ArtifactKind.RESUME_PDF,
        ResourceRef("resume", RESUME_ID, 7),
        "application/pdf",
        len(PAYLOAD),
        digest,
        ApiArtifactContentUrl.build(
            "https://api.hmalliances.org:8022",
            workspace_id,
            artifact_id,
        ),
        page_count=2,
        expires_at=NOW + timedelta(hours=1),
    )


def _source_map(*, page: int = 1) -> PdfSourceMap:
    """@brief 构造 PDF source map / Build a PDF source map.

    @param page node 页码 / Node page number.
    @return Source map / Source map.
    """
    return PdfSourceMap(
        ARTIFACT_ID,
        RESUME_ID,
        7,
        (
            PdfSourceNode(
                "item_00000001",
                ("title",),
                page,
                (PdfRect(10.0, 20.0, 100.0, 12.0),),
            ),
        ),
    )


def _event(sequence: int, *, event_id: str | None = None) -> ApiEvent:
    """@brief 构造 API event / Build an API event.

    @param sequence Workspace stream sequence / Workspace stream sequence.
    @param event_id 可选事件 ID / Optional event ID.
    @return API event / API event.
    """
    return ApiEvent(
        ApiEventId(event_id or f"event_{sequence:08d}"),
        sequence,
        "job.updated",
        NOW,
        ResourceRef("job", JOB_ID, sequence),
        {"status": "running", "hints": ("poll",)},
        "0123456789abcdef0123456789abcdef",
    )


def _audit_event(*, workspace_id: WorkspaceId = WORKSPACE_ID) -> AuditEvent:
    """@brief 构造审计事件 / Build an audit event.

    @param workspace_id 所属 Workspace / Owning Workspace.
    @return Audit event / Audit event.
    """
    return AuditEvent(
        AuditEventId("audit_00000001"),
        workspace_id,
        NOW,
        ResourceRef("user", USER_ID),
        "job.cancel",
        ResourceRef("job", JOB_ID, 1),
        AuditOutcome.ALLOWED,
        "request_00000001",
    )


def test_job_progress_and_state_machine_encode_all_contract_edges() -> None:
    """@brief 验证 Job 只有契约允许的迁移边 / Verify only contract Job transition edges."""
    with pytest.raises(PlatformDomainError, match="exceed total"):
        JobProgress("layout", 3, 2, JobProgressUnit.STEPS)

    queued = _job()
    running = queued.start(
        at=NOW + timedelta(seconds=1),
        progress=JobProgress("layout", 0, 2, JobProgressUnit.STEPS),
    )
    progressed = running.report_progress(
        JobProgress("layout", 1, 2, JobProgressUnit.STEPS),
        at=NOW + timedelta(seconds=2),
    )
    succeeded = progressed.succeed(
        (ResourceRef("artifact", ARTIFACT_ID, 1),),
        at=NOW + timedelta(seconds=3),
        progress=JobProgress("done", 2, 2, JobProgressUnit.STEPS),
    )

    assert succeeded.status is JobStatus.SUCCEEDED
    assert succeeded.meta.revision == 4
    assert succeeded.problem is None
    assert succeeded.finished_at == NOW + timedelta(seconds=3)
    assert succeeded.result_refs[0].id == ARTIFACT_ID
    with pytest.raises(JobTransitionError):
        succeeded.cancel(at=NOW + timedelta(seconds=4))

    failed = running.fail(_problem(), at=NOW + timedelta(seconds=2))
    assert failed.status is JobStatus.FAILED
    assert failed.problem is not None
    cancelled_queued = queued.cancel(at=NOW + timedelta(seconds=1))
    assert cancelled_queued.started_at is None
    assert running.cancel(at=NOW + timedelta(seconds=2)).started_at is not None
    assert queued.expire(at=NOW + timedelta(seconds=1)).status is JobStatus.EXPIRED
    with pytest.raises(JobTransitionError):
        running.expire(at=NOW + timedelta(seconds=2))


def test_job_constructor_rejects_every_illegal_state_association() -> None:
    """@brief 验证 status 关联字段不能组成非法状态 / Verify state-associated fields reject illegal states."""
    queued = _job()
    with pytest.raises(PlatformDomainError, match="queued job"):
        replace(queued, finished_at=NOW)
    with pytest.raises(PlatformDomainError, match="running job"):
        replace(queued, status=JobStatus.RUNNING)
    with pytest.raises(PlatformDomainError, match="succeeded job"):
        replace(
            queued,
            status=JobStatus.SUCCEEDED,
            meta=queued.meta.advance(NOW + timedelta(seconds=1)),
            finished_at=NOW + timedelta(seconds=1),
        )
    with pytest.raises(PlatformDomainError, match="failed job"):
        replace(
            queued,
            status=JobStatus.FAILED,
            meta=queued.meta.advance(NOW + timedelta(seconds=1)),
            started_at=NOW + timedelta(seconds=1),
            finished_at=NOW + timedelta(seconds=1),
        )
    with pytest.raises(PlatformDomainError, match="only succeeded"):
        replace(queued, result_refs=(ResourceRef("artifact", ARTIFACT_ID),))


def test_artifact_content_location_digest_media_and_source_map_are_coherent() -> None:
    """@brief 验证 Artifact 与 source map 的跨对象不变量 / Verify Artifact/source-map invariants."""
    artifact = _artifact()
    assert artifact.content_url.endswith(f"/{ARTIFACT_ID}/content")
    assert not artifact.is_expired(NOW)
    _source_map().validate_for(artifact)

    with pytest.raises(PlatformDomainError, match="exactly identify"):
        ApiArtifactContentUrl(
            "https://evil.example/api/v2/workspaces/ws_00000001/artifacts/artifact_00000001/content",
            "https://api.hmalliances.org:8022",
            WORKSPACE_ID,
            ARTIFACT_ID,
        )
    with pytest.raises(PlatformDomainError, match="SHA-256"):
        replace(artifact, sha256="ABC")
    with pytest.raises(PlatformDomainError, match="one GiB"):
        replace(artifact, size_bytes=1_073_741_825)
    with pytest.raises(PlatformDomainError, match="page count"):
        _source_map(page=3).validate_for(artifact)
    with pytest.raises(PlatformDomainError, match="Resume revision"):
        replace(_source_map(), resume_revision=8).validate_for(artifact)


def test_signed_artifact_url_requires_explicit_short_lived_single_use_grant() -> None:
    """@brief 验证跨域 URL 不能仅凭 HTTPS 获得信任 / Verify HTTPS alone does not trust cross-origin URLs."""
    signed = SignedArtifactContentUrl(
        "https://objects.example/file?signature=opaque",
        "signed_token_00000001",
        NOW,
        NOW + timedelta(minutes=5),
        timedelta(minutes=10),
    )
    artifact = replace(_artifact(), content_location=signed)
    assert artifact.content_url.startswith("https://objects.example/")

    with pytest.raises(PlatformDomainError, match="short-lived"):
        replace(signed, expires_at=NOW + timedelta(minutes=11))
    with pytest.raises(PlatformDomainError, match="single-use"):
        SignedArtifactContentUrl(
            signed.value,
            signed.token_id,
            signed.issued_at,
            signed.expires_at,
            signed.maximum_lifetime,
            single_use=False,  # type: ignore[arg-type]
        )
    with pytest.raises(PlatformDomainError, match="outlive"):
        replace(
            artifact,
            expires_at=NOW + timedelta(minutes=4),
        )


def test_api_and_audit_events_validate_schema_and_freeze_incremental_data() -> None:
    """@brief 验证事件 envelope、sequence 与深度不可变 data / Verify event envelopes and immutable data."""
    event = _event(1)
    assert event.data["hints"] == ("poll",)
    with pytest.raises(TypeError):
        event.data["status"] = "failed"  # type: ignore[index]
    with pytest.raises(PlatformDomainError, match="sequence"):
        replace(event, sequence=0)
    with pytest.raises(PlatformDomainError, match="trace id"):
        replace(event, trace_id="ABC")

    audit = _audit_event()
    assert audit.outcome is AuditOutcome.ALLOWED
    with pytest.raises(PlatformDomainError, match="audit action"):
        replace(audit, action="Cancel Job")


def test_queries_ranges_and_authorization_targets_are_contract_bounded() -> None:
    """@brief 验证过滤、分页、Range 与 permission-target 判别 / Verify query/range/authorization bounds."""
    query = JobQuery("resume.render", SubjectFilter("resume", RESUME_ID))
    assert query.cursor_binding == ("resume.render", "resume", RESUME_ID)
    assert ArtifactQuery(ArtifactKind.RESUME_PDF).cursor_binding[0] == "resume_pdf"
    with pytest.raises(ValueError, match="kind filter"):
        JobQuery("UPPER")
    with pytest.raises(ValueError, match="page limit"):
        PageRequest(limit=201)
    with pytest.raises(ValueError, match="Job target"):
        PlatformAuthorizationRequest(PlatformPermission.READ_JOB)
    with pytest.raises(ValueError, match="collection permission"):
        PlatformAuthorizationRequest(
            PlatformPermission.LIST_JOBS,
            PlatformResourceTarget(PlatformTargetKind.JOB, JOB_ID),
        )

    assert ByteRangeRequest(first=2, last_inclusive=99).resolve(10) == ContentRange(2, 9, 10)
    assert ByteRangeRequest(suffix_length=3).resolve(10) == ContentRange(7, 9, 10)
    with pytest.raises(RangeNotSatisfiable) as captured:
        ByteRangeRequest(first=10).resolve(10)
    assert captured.value.total_size_bytes == 10


class FakeAuthorizer:
    """@brief 记录精确平台授权请求的测试 authorizer / Test authorizer recording exact requests."""

    def __init__(self) -> None:
        """@brief 初始化授权记录 / Initialize authorization records."""
        self.requests: list[PlatformAuthorizationRequest] = []

    async def authenticate(self, principal: TokenPrincipal) -> AuthenticatedActor:
        """@brief 将 principal 绑定测试用户 / Bind principal to the test user."""
        return AuthenticatedActor(principal.user_id, principal)

    async def authorize(
        self,
        actor: AuthenticatedActor,
        workspace_id: WorkspaceId,
        request: PlatformAuthorizationRequest,
    ) -> WorkspaceAccessContext:
        """@brief 记录并签发密封 Workspace proof / Record and issue a sealed Workspace proof."""
        self.requests.append(request)
        return _issue_workspace_access_context(
            actor,
            workspace_id,
            MembershipId("membership_00000001"),
            WorkspaceRole.OWNER,
            WorkspaceAction.READ,
        )


class FakeRepository:
    """@brief 支持查询过滤和 Job CAS 的内存 repository / In-memory query and Job-CAS repository."""

    def __init__(self) -> None:
        """@brief 初始化测试记录 / Initialize test records."""
        self.jobs: dict[JobId, Job] = {JOB_ID: _job()}
        self.artifacts: dict[ArtifactId, Artifact] = {ARTIFACT_ID: _artifact()}
        self.source_maps: dict[ArtifactId, PdfSourceMap] = {ARTIFACT_ID: _source_map()}
        self.audit_events: tuple[AuditEvent, ...] = (_audit_event(),)
        self.cas_failure = False
        self.saved_expected_revision: int | None = None
        self.synchronized_cancellation: tuple[Job, datetime] | None = None

    async def list_jobs(
        self,
        access: WorkspaceAccessContext,
        query: JobQuery,
        page: PageRequest,
    ) -> CollectionPage[Job]:
        """@brief 应用 Job filters / Apply Job filters."""
        selected = tuple(
            job
            for job in self.jobs.values()
            if job.workspace_id == access.workspace_id
            and (query.kind is None or job.kind == query.kind)
            and (
                query.subject.subject_type is None
                or job.subject.resource_type == query.subject.subject_type
            )
            and (query.subject.subject_id is None or job.subject.id == query.subject.subject_id)
        )
        return CollectionPage(selected[: page.limit], None)

    async def get_job(
        self,
        access: WorkspaceAccessContext,
        job_id: JobId,
        *,
        for_update: bool = False,
    ) -> Job | None:
        """@brief 读取 Job；测试中保留跨租户缺陷供 service 检测 / Read a Job, retaining leak tests."""
        del access, for_update
        return self.jobs.get(job_id)

    async def save_job(
        self,
        access: WorkspaceAccessContext,
        job: Job,
        *,
        expected_revision: int,
    ) -> None:
        """@brief 模拟 revision CAS / Simulate revision CAS."""
        del access
        if self.cas_failure:
            raise JobCasMismatch
        current = self.jobs.get(job.meta.id)
        if current is None or current.meta.revision != expected_revision:
            raise JobCasMismatch
        self.saved_expected_revision = expected_revision
        self.jobs[job.meta.id] = job

    async def synchronize_cancellation(
        self,
        access: WorkspaceAccessContext,
        job: Job,
        *,
        at: datetime,
    ) -> None:
        """@brief 记录领域取消同步 / Record domain cancellation synchronization."""
        del access
        self.synchronized_cancellation = (job, at)

    async def list_artifacts(
        self,
        access: WorkspaceAccessContext,
        query: ArtifactQuery,
        page: PageRequest,
    ) -> CollectionPage[Artifact]:
        """@brief 应用 Artifact filters / Apply Artifact filters."""
        selected = tuple(
            artifact
            for artifact in self.artifacts.values()
            if artifact.workspace_id == access.workspace_id
            and (query.kind is None or artifact.kind is query.kind)
            and (
                query.subject.subject_type is None
                or artifact.subject.resource_type == query.subject.subject_type
            )
            and (
                query.subject.subject_id is None or artifact.subject.id == query.subject.subject_id
            )
        )
        return CollectionPage(selected[: page.limit], None)

    async def get_artifact(
        self,
        access: WorkspaceAccessContext,
        artifact_id: ArtifactId,
    ) -> Artifact | None:
        """@brief 读取 Artifact / Read an Artifact."""
        del access
        return self.artifacts.get(artifact_id)

    async def get_pdf_source_map(
        self,
        access: WorkspaceAccessContext,
        artifact_id: ArtifactId,
    ) -> PdfSourceMap | None:
        """@brief 读取 PDF source map / Read a PDF source map."""
        del access
        return self.source_maps.get(artifact_id)

    async def list_audit_events(
        self,
        access: WorkspaceAccessContext,
        page: PageRequest,
    ) -> CollectionPage[AuditEvent]:
        """@brief 返回审计页 / Return an audit page."""
        del access
        return CollectionPage(self.audit_events[: page.limit], None)


class FakeMutationJournal:
    """@brief 记录与 Job CAS 同事务 mutation 的测试 journal / Test transactional mutation journal."""

    def __init__(self) -> None:
        """@brief 初始化 cancellation 记录 / Initialize cancellation records."""
        self.cancellations: list[tuple[WorkspaceAccessContext, Job, Job, MutationContext]] = []

    async def job_cancelled(
        self,
        access: WorkspaceAccessContext,
        before: Job,
        after: Job,
        context: MutationContext,
    ) -> None:
        """@brief 记录成功 cancellation / Record a successful cancellation."""
        self.cancellations.append((access, before, after, context))


class FakeUnitOfWork:
    """@brief 平台测试工作单元 / Platform test unit of work."""

    def __init__(
        self,
        repository: FakeRepository,
        authorizer: FakeAuthorizer,
        journal: FakeMutationJournal,
    ) -> None:
        """@brief 绑定共享测试状态 / Bind shared test state."""
        self.repository = repository
        self.authorizer = authorizer
        self.journal = journal
        self.commits = 0

    async def __aenter__(self) -> FakeUnitOfWork:
        """@brief 进入工作单元 / Enter the unit of work."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        """@brief 不吞测试异常 / Do not suppress test exceptions."""
        del exc_type, exc, traceback
        return None

    async def commit(self) -> None:
        """@brief 记录提交 / Record a commit."""
        self.commits += 1

    async def rollback(self) -> None:
        """@brief 模拟幂等回滚 / Simulate idempotent rollback."""


class FakeUnitOfWorkFactory:
    """@brief 重用内存状态的工作单元工厂 / Unit-of-work factory sharing in-memory state."""

    def __init__(
        self,
        repository: FakeRepository,
        authorizer: FakeAuthorizer,
        journal: FakeMutationJournal,
    ) -> None:
        """@brief 初始化依赖 / Initialize dependencies."""
        self.repository = repository
        self.authorizer = authorizer
        self.journal = journal
        self.created: list[FakeUnitOfWork] = []

    def __call__(self) -> FakeUnitOfWork:
        """@brief 创建新工作单元 / Create a new unit of work."""
        uow = FakeUnitOfWork(self.repository, self.authorizer, self.journal)
        self.created.append(uow)
        return uow


class FakeContentStore:
    """@brief 可注入 metadata/body 缺陷的 Artifact store / Artifact store with injectable defects."""

    def __init__(self, payload: bytes = PAYLOAD) -> None:
        """@brief 初始化内容 / Initialize content."""
        self.payload = payload
        self.mode = "valid"

    async def open(
        self,
        access: WorkspaceAccessContext,
        artifact: Artifact,
        selected_range: ContentRange | None,
    ) -> ArtifactContentStream:
        """@brief 打开测试内容 / Open test content."""
        del access
        payload = self.payload
        if selected_range is not None:
            payload = payload[selected_range.first : selected_range.last_inclusive + 1]
        if self.mode == "truncated":
            payload = payload[:-1]
        elif self.mode == "changed":
            payload = b"x" * len(payload)

        async def chunks() -> AsyncIterator[bytes]:
            """@brief 分两块流式返回 / Stream in two chunks."""
            split = len(payload) // 2
            if payload[:split]:
                yield payload[:split]
            if payload[split:]:
                yield payload[split:]

        return ArtifactContentStream(
            chunks(),
            "text/plain" if self.mode == "metadata" else artifact.media_type,
            artifact.size_bytes,
            artifact.sha256,
            selected_range,
        )


class FakeEventFeed:
    """@brief 可验证 replay 时机与顺序的 event feed / Event feed for replay timing and order tests."""

    def __init__(self) -> None:
        """@brief 初始化事件和请求记录 / Initialize events and request records."""
        self.events: tuple[ApiEvent, ...] = (_event(1), _event(2))
        self.replays: list[EventReplayRequest] = []
        self.expire_replay = False

    async def open(
        self,
        access: WorkspaceAccessContext,
        replay: EventReplayRequest,
    ) -> AsyncIterator[ApiEvent]:
        """@brief 在返回 iterator 前决定 replay 是否有效 / Decide replay validity before return."""
        del access
        self.replays.append(replay)
        if self.expire_replay and replay.after_event_id is not None:
            raise EventReplayWindowExpired(replay.after_event_id)

        async def stream() -> AsyncIterator[ApiEvent]:
            """@brief 产生配置事件 / Yield configured events."""
            for event in self.events:
                yield event

        return stream()


def _service(
    *,
    repository: FakeRepository | None = None,
    content_store: FakeContentStore | None = None,
    event_feed: FakeEventFeed | None = None,
) -> tuple[
    PlatformApplicationService,
    FakeRepository,
    FakeAuthorizer,
    FakeUnitOfWorkFactory,
    FakeContentStore,
    FakeEventFeed,
]:
    """@brief 组装平台测试服务 / Assemble a platform test service."""
    selected_repository = repository or FakeRepository()
    authorizer = FakeAuthorizer()
    journal = FakeMutationJournal()
    factory = FakeUnitOfWorkFactory(selected_repository, authorizer, journal)
    selected_content = content_store or FakeContentStore()
    selected_events = event_feed or FakeEventFeed()
    service = PlatformApplicationService(
        factory,
        selected_content,
        selected_events,
        clock=FixedClock(),
    )
    return (
        service,
        selected_repository,
        authorizer,
        factory,
        selected_content,
        selected_events,
    )


async def _read(download: ArtifactDownload) -> bytes:
    """@brief 消费并触发下载后置校验 / Consume a download and trigger post-validation."""
    return b"".join([chunk async for chunk in download.chunks])


async def test_application_filters_lists_and_authorizes_exact_operations() -> None:
    """@brief 验证列表过滤与精确 permission / Verify list filtering and exact permissions."""
    service, _, authorizer, _, _, _ = _service()
    jobs = await service.list_jobs(
        _principal(),
        WORKSPACE_ID,
        query=JobQuery("resume.render", SubjectFilter("resume", RESUME_ID)),
    )
    artifacts = await service.list_artifacts(
        _principal(),
        WORKSPACE_ID,
        query=ArtifactQuery(ArtifactKind.RESUME_PDF, SubjectFilter("resume", RESUME_ID)),
    )

    assert jobs.items == (_job(),)
    assert artifacts.items == (_artifact(),)
    assert [request.permission for request in authorizer.requests] == [
        PlatformPermission.LIST_JOBS,
        PlatformPermission.LIST_ARTIFACTS,
    ]


async def test_job_cancellation_uses_exact_target_cas_and_rejects_terminal_state() -> None:
    """@brief 验证 cancellation 的目标授权、CAS 与终态冲突 / Verify cancellation authorization and CAS."""
    service, repository, authorizer, factory, _, _ = _service()
    mutation = MutationContext(
        "request_00000001",
        "0123456789abcdef0123456789abcdef",
    )
    snapshot = await service.get_job_for_cancellation(_principal(), WORKSPACE_ID, JOB_ID)
    assert snapshot.meta.revision == 1
    assert authorizer.requests[-1].permission is PlatformPermission.CANCEL_JOB
    with pytest.raises(PlatformPreconditionFailed):
        await service.cancel_job(
            _principal(),
            WORKSPACE_ID,
            JOB_ID,
            mutation,
            expected_revision=2,
        )

    cancelled = await service.cancel_job(
        _principal(),
        WORKSPACE_ID,
        JOB_ID,
        mutation,
        expected_revision=snapshot.meta.revision,
    )

    assert cancelled.status is JobStatus.CANCELLED
    assert cancelled.meta.revision == 2
    assert repository.saved_expected_revision == 1
    assert repository.synchronized_cancellation is not None
    synchronized_job, synchronized_at = repository.synchronized_cancellation
    assert synchronized_job.status is JobStatus.QUEUED
    assert synchronized_at == cancelled.meta.updated_at
    assert factory.created[-1].commits == 1
    assert factory.journal.cancellations[0][3] == mutation
    assert factory.journal.cancellations[0][1].status is JobStatus.QUEUED
    assert factory.journal.cancellations[0][2].status is JobStatus.CANCELLED
    request = authorizer.requests[-1]
    assert request.permission is PlatformPermission.CANCEL_JOB
    assert request.target == PlatformResourceTarget(PlatformTargetKind.JOB, JOB_ID)

    with pytest.raises(PlatformConflict) as terminal:
        await service.cancel_job(_principal(), WORKSPACE_ID, JOB_ID, mutation)
    assert terminal.value.code == "job.not_cancellable"

    repository.jobs[JOB_ID] = _job()
    repository.cas_failure = True
    with pytest.raises(PlatformConflict) as concurrent:
        await service.cancel_job(_principal(), WORKSPACE_ID, JOB_ID, mutation)
    assert concurrent.value.code == "job.concurrent_transition"


async def test_application_fails_loudly_on_repository_workspace_leaks() -> None:
    """@brief 验证 repository 跨租户缺陷不会伪装成成功 / Verify repository tenant leaks fail loudly."""
    repository = FakeRepository()
    repository.jobs[JOB_ID] = _job(workspace_id=OTHER_WORKSPACE_ID)
    service, _, _, _, _, _ = _service(repository=repository)
    with pytest.raises(PlatformIsolationViolation):
        await service.get_job(_principal(), WORKSPACE_ID, JOB_ID)


async def test_artifact_download_supports_range_and_validates_metadata_and_body() -> None:
    """@brief 验证 ETag、Range、metadata 与完整 body 摘要 / Verify ETag, Range, metadata, and digest."""
    service, _, authorizer, _, store, _ = _service()
    full = await service.open_artifact_content(_principal(), WORKSPACE_ID, ARTIFACT_ID)
    assert full.etag == f'"sha256-{PAYLOAD_SHA256}"'
    assert full.content_length == len(PAYLOAD)
    assert await _read(full) == PAYLOAD
    assert authorizer.requests[-1].permission is PlatformPermission.READ_ARTIFACT_CONTENT

    partial = await service.open_artifact_content(
        _principal(),
        WORKSPACE_ID,
        ARTIFACT_ID,
        byte_range=ByteRangeRequest(first=2, last_inclusive=7),
    )
    assert partial.selected_range == ContentRange(2, 7, len(PAYLOAD))
    assert partial.content_length == 6
    assert await _read(partial) == PAYLOAD[2:8]

    store.mode = "metadata"
    with pytest.raises(ArtifactContentIntegrityError):
        await service.open_artifact_content(_principal(), WORKSPACE_ID, ARTIFACT_ID)
    store.mode = "changed"
    changed = await service.open_artifact_content(_principal(), WORKSPACE_ID, ARTIFACT_ID)
    with pytest.raises(ArtifactContentIntegrityError):
        await _read(changed)
    store.mode = "truncated"
    truncated = await service.open_artifact_content(_principal(), WORKSPACE_ID, ARTIFACT_ID)
    with pytest.raises(ArtifactContentIntegrityError):
        await _read(truncated)


async def test_source_map_and_audit_reads_apply_workspace_authorization() -> None:
    """@brief 验证 source-map/audit 查询授权与交叉验证 / Verify source-map and audit authorization."""
    service, repository, authorizer, _, _, _ = _service()
    source_map = await service.get_pdf_source_map(_principal(), WORKSPACE_ID, ARTIFACT_ID)
    audits = await service.list_audit_events(_principal(), WORKSPACE_ID)

    assert source_map == _source_map()
    assert audits.items == (_audit_event(),)
    assert [request.permission for request in authorizer.requests] == [
        PlatformPermission.READ_ARTIFACT_SOURCE_MAP,
        PlatformPermission.LIST_AUDIT_EVENTS,
    ]

    repository.audit_events = (_audit_event(workspace_id=OTHER_WORKSPACE_ID),)
    with pytest.raises(PlatformIsolationViolation):
        await service.list_audit_events(_principal(), WORKSPACE_ID)


async def test_event_replay_is_prevalidated_and_delivery_is_at_least_once_ordered() -> None:
    """@brief 验证 replay 409 时机、重复容忍与 sequence 防倒退 / Verify replay timing and ordering."""
    service, _, authorizer, _, _, feed = _service()
    first = _event(1)
    feed.events = (first, first, _event(2))
    stream = await service.open_event_stream(
        _principal(),
        WORKSPACE_ID,
        after_event_id=ApiEventId("event_00000000"),
    )
    assert [event.sequence async for event in stream] == [1, 1, 2]
    assert authorizer.requests[-1].permission is PlatformPermission.READ_EVENTS
    assert feed.replays[-1].after_event_id == ApiEventId("event_00000000")

    feed.events = (_event(2), _event(1))
    invalid = await service.open_event_stream(_principal(), WORKSPACE_ID)
    with pytest.raises(EventStreamInvariantError, match="backwards"):
        [event async for event in invalid]

    feed.expire_replay = True
    with pytest.raises(EventReplayWindowExpired) as expired:
        await service.open_event_stream(
            _principal(),
            WORKSPACE_ID,
            after_event_id=ApiEventId("event_00000000"),
        )
    assert expired.value.code == "event.replay_window_expired"
