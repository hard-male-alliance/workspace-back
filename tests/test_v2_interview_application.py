"""API v2 Interview 应用核心测试。"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from types import TracebackType

import pytest

from backend.application.interview_v2 import (
    V2_INTERVIEW_ENDPOINT_METHODS,
    CreateInterviewReportJobCommand,
    CreateInterviewScenarioCommand,
    CreateInterviewSessionCommand,
    EndInterviewSessionCommand,
    InterviewApplicationService,
    InterviewConflict,
    InterviewMutationContext,
    InterviewPortProtocolError,
    InterviewPreconditionFailed,
    InterviewWorkerError,
    InterviewWorkerRetry,
    InterviewWorkerService,
    InvalidInterviewCommand,
)
from backend.application.ports.interview_v2 import (
    EndSessionOutput,
    InterviewCasMismatch,
    InterviewPage,
    InterviewPageRequest,
    InterviewPermission,
    InterviewPermissionGrant,
    InterviewPermissionRequest,
    InterviewSessionPolicyRequest,
    InterviewWorkerOperationId,
    InterviewWorkerPortFailure,
    RealtimeInputKeyReused,
    ReportGenerationRequest,
    TranscriptSequenceReservation,
)
from backend.domain.interview_v2 import (
    INTERVIEW_REPORT_JOB_KIND,
    AvatarOutputMode,
    CandidateUtteranceInput,
    CreateRealtimeConnectionSpec,
    EndInterviewReason,
    EphemeralToken,
    FallbackTransport,
    InterviewAvatarPreferences,
    InterviewCommunicationMetrics,
    InterviewDifficulty,
    InterviewEvidence,
    InterviewExecutionGrant,
    InterviewMediaPreferences,
    InterviewReport,
    InterviewReportDraft,
    InterviewReportId,
    InterviewRichText,
    InterviewRubric,
    InterviewScenario,
    InterviewScenarioId,
    InterviewScenarioPatch,
    InterviewScenarioSpec,
    InterviewScenarioStatus,
    InterviewSession,
    InterviewSessionId,
    InterviewSessionStatus,
    JobTarget,
    RealtimeConnection,
    RealtimeConnectionId,
    RealtimeConnectionLease,
    RealtimeControl,
    RealtimeControlInput,
    RealtimeInputEnvelope,
    RealtimeInputId,
    RealtimeInputLedgerRecord,
    RealtimeInputReceipt,
    RealtimeTransport,
    RecordingConsent,
    RubricDimension,
    RubricScore,
    ScoreScale,
    TranscriptSegment,
    realtime_input_fingerprint,
)
from backend.domain.knowledge_retrieval import (
    InferenceCostTier,
    InferenceIntent,
    InferenceQualityTier,
    KnowledgeSelection,
    KnowledgeSelectionMode,
)
from backend.domain.knowledge_sources import ModelRegion
from backend.domain.platform import Artifact, AuditEvent, Job, JobId, JobStatus
from backend.domain.principals import (
    ClientId,
    ResourceMeta,
    Scope,
    Subject,
    TokenPrincipal,
    UserId,
    WorkspaceId,
)
from backend.domain.resources import ResourceRef
from backend.infrastructure.access import InMemoryAccessStore
from backend.infrastructure.interview import (
    InMemoryInterviewUnitOfWorkFactory,
    StaticInterviewSessionPolicy,
)

NOW = datetime(2026, 7, 23, 4, 0, tzinfo=UTC)
WORKSPACE = WorkspaceId("workspace_0001")
OTHER_WORKSPACE = WorkspaceId("workspace_0002")
PRINCIPAL = TokenPrincipal(
    UserId("user_actor_0001"),
    Subject("subject_actor_0001"),
    ClientId("client_actor_0001"),
    frozenset({Scope("interview.read"), Scope("interview.write")}),
)
CONTEXT = InterviewMutationContext("request_0001")


class FixedClock:
    """@brief 测试固定时钟 / Fixed test clock."""

    def now(self) -> datetime:
        """@brief 返回固定时刻 / Return the fixed instant."""
        return NOW


class DeterministicIds:
    """@brief 确定性 opaque-ID 工厂 / Deterministic opaque-ID factory."""

    def __init__(self) -> None:
        """@brief 初始化分前缀计数 / Initialize per-prefix counters."""
        self._counts: dict[str, int] = {}

    def __call__(self, prefix: str) -> str:
        """@brief 生成下一个确定性 ID / Generate the next deterministic ID."""
        count = self._counts.get(prefix, 0) + 1
        self._counts[prefix] = count
        return f"{prefix}_{count:08d}"


@dataclass
class State:
    """@brief 内存假持久化状态 / In-memory fake persistence state."""

    scenarios: dict[InterviewScenarioId, InterviewScenario] = field(default_factory=dict)
    sessions: dict[InterviewSessionId, InterviewSession] = field(default_factory=dict)
    leases: dict[RealtimeConnectionId, RealtimeConnectionLease] = field(default_factory=dict)
    realtime_inputs: dict[
        tuple[WorkspaceId, InterviewSessionId, RealtimeInputId], tuple[str, int]
    ] = field(default_factory=dict)
    next_input_sequence: dict[InterviewSessionId, int] = field(default_factory=dict)
    transcript: dict[InterviewSessionId, list[TranscriptSegment]] = field(default_factory=dict)
    next_transcript_sequence: dict[InterviewSessionId, int] = field(default_factory=dict)
    reports: dict[InterviewReportId, InterviewReport] = field(default_factory=dict)
    jobs: dict[JobId, Job] = field(default_factory=dict)
    job_specs: dict[JobId, object] = field(default_factory=dict)
    artifacts: list[Artifact] = field(default_factory=list)
    outbox: list[object] = field(default_factory=list)
    audits: list[AuditEvent] = field(default_factory=list)
    permissions: list[InterviewPermissionRequest] = field(default_factory=list)
    active_transactions: int = 0
    malicious_scenario: InterviewScenario | None = None
    media_finalize_calls: int = 0
    report_generate_calls: int = 0
    media_operation_ids: list[InterviewWorkerOperationId] = field(default_factory=list)
    report_operation_ids: list[InterviewWorkerOperationId] = field(default_factory=list)


class FakeAuthorizer:
    """@brief 记录精确权限请求的 fake / Fake recording exact permission requests."""

    def __init__(self, state: State) -> None:
        self.state = state

    async def authorize(
        self,
        principal: TokenPrincipal,
        request: InterviewPermissionRequest,
    ) -> InterviewPermissionGrant:
        self.state.permissions.append(request)
        return InterviewPermissionGrant(principal.user_id, request)


class FakePolicy:
    """@brief 返回与创建快照精确对齐的 grant / Fake returning an exact creation-snapshot grant."""

    async def authorize_session(
        self,
        request: InterviewSessionPolicyRequest,
    ) -> InterviewExecutionGrant:
        return InterviewExecutionGrant(
            scenario_ref=ResourceRef(
                "interview_scenario",
                request.scenario.meta.id,
                request.scenario.meta.revision,
            ),
            resume_ref=request.spec.resume_ref,
            agent_scope=request.spec.knowledge.agent_scope,
            model_ref=ResourceRef("model", "model_policy_0001", 1),
            model_region=request.spec.inference.data_region,
            external_model_processing=False,
            knowledge_contexts=(),
            policy_version=1,
        )


class FakeRepository:
    """@brief Workspace-first 且 CAS/sequence-aware 的 fake repository / Workspace-first CAS/sequence-aware fake repository."""

    def __init__(self, state: State) -> None:
        self.state = state

    async def list_scenarios(
        self,
        workspace_id: WorkspaceId,
        page: InterviewPageRequest,
    ) -> InterviewPage[InterviewScenario]:
        items = [item for item in self.state.scenarios.values() if item.workspace_id == workspace_id]
        items.sort(key=lambda item: (item.meta.created_at, item.meta.id))
        if self.state.malicious_scenario is not None:
            items.append(self.state.malicious_scenario)
        return InterviewPage(tuple(items[: page.limit]), None)

    async def get_scenario(
        self,
        workspace_id: WorkspaceId,
        scenario_id: InterviewScenarioId,
        *,
        for_update: bool = False,
    ) -> InterviewScenario | None:
        del for_update
        item = self.state.scenarios.get(scenario_id)
        return item if item is not None and item.workspace_id == workspace_id else None

    async def add_scenario(self, scenario: InterviewScenario) -> None:
        self.state.scenarios[scenario.meta.id] = scenario

    async def save_scenario(
        self,
        scenario: InterviewScenario,
        *,
        expected_revision: int,
    ) -> None:
        current = self.state.scenarios.get(scenario.meta.id)
        if current is None or current.meta.revision != expected_revision:
            raise InterviewCasMismatch
        self.state.scenarios[scenario.meta.id] = scenario

    async def list_sessions(
        self,
        workspace_id: WorkspaceId,
        page: InterviewPageRequest,
    ) -> InterviewPage[InterviewSession]:
        items = [item for item in self.state.sessions.values() if item.workspace_id == workspace_id]
        items.sort(key=lambda item: (item.meta.created_at, item.meta.id))
        return InterviewPage(tuple(items[: page.limit]), None)

    async def get_session(
        self,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
        *,
        for_update: bool = False,
    ) -> InterviewSession | None:
        del for_update
        item = self.state.sessions.get(session_id)
        return item if item is not None and item.workspace_id == workspace_id else None

    async def add_session(self, session: InterviewSession) -> None:
        self.state.sessions[session.meta.id] = session
        self.state.next_input_sequence[session.meta.id] = 1
        self.state.next_transcript_sequence[session.meta.id] = 1
        self.state.transcript[session.meta.id] = []

    async def save_session(
        self,
        session: InterviewSession,
        *,
        expected_revision: int,
    ) -> None:
        current = self.state.sessions.get(session.meta.id)
        if current is None or current.meta.revision != expected_revision:
            raise InterviewCasMismatch
        self.state.sessions[session.meta.id] = session

    async def add_connection_lease(self, lease: RealtimeConnectionLease) -> None:
        self.state.leases[lease.id] = lease

    async def get_connection_lease(
        self,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
        connection_id: RealtimeConnectionId,
    ) -> RealtimeConnectionLease | None:
        lease = self.state.leases.get(connection_id)
        if (
            lease is None
            or lease.workspace_id != workspace_id
            or lease.session_id != session_id
        ):
            return None
        return lease

    async def append_realtime_input(
        self,
        record: RealtimeInputLedgerRecord,
    ) -> RealtimeInputReceipt:
        key = (record.workspace_id, record.session_id, record.input_id)
        prior = self.state.realtime_inputs.get(key)
        if prior is not None:
            if prior[0] != record.fingerprint_sha256:
                raise RealtimeInputKeyReused
            return RealtimeInputReceipt(prior[1], True)
        sequence = self.state.next_input_sequence[record.session_id]
        self.state.next_input_sequence[record.session_id] = sequence + 1
        self.state.realtime_inputs[key] = (record.fingerprint_sha256, sequence)
        return RealtimeInputReceipt(sequence, False)

    async def allocate_transcript_sequence(
        self,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
    ) -> TranscriptSequenceReservation:
        session = self.state.sessions.get(session_id)
        if session is None or session.workspace_id != workspace_id:
            raise AssertionError("missing Session")
        sequence = self.state.next_transcript_sequence[session_id]
        self.state.next_transcript_sequence[session_id] = sequence + 1
        return TranscriptSequenceReservation(sequence)

    async def add_transcript_segment(self, segment: TranscriptSegment) -> None:
        self.state.transcript[segment.session_id].append(segment)

    async def list_transcript(
        self,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
        page: InterviewPageRequest,
    ) -> InterviewPage[TranscriptSegment]:
        items = [
            item
            for item in self.state.transcript.get(session_id, [])
            if item.workspace_id == workspace_id
        ]
        items.sort(key=lambda item: (item.sequence, item.id))
        return InterviewPage(tuple(items[: page.limit]), None)

    async def load_transcript_snapshot(
        self,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
        *,
        maximum_segments: int,
    ) -> tuple[TranscriptSegment, ...]:
        items = tuple(
            item
            for item in self.state.transcript.get(session_id, [])
            if item.workspace_id == workspace_id
        )
        if len(items) > maximum_segments:
            raise ValueError("Transcript exceeds worker bound")
        return tuple(sorted(items, key=lambda item: (item.sequence, item.id)))

    async def get_report(
        self,
        workspace_id: WorkspaceId,
        report_id: InterviewReportId,
    ) -> InterviewReport | None:
        report = self.state.reports.get(report_id)
        return report if report is not None and report.workspace_id == workspace_id else None

    async def add_report(self, report: InterviewReport) -> None:
        self.state.reports[report.meta.id] = report

    async def has_live_report_job(
        self,
        workspace_id: WorkspaceId,
        session_id: InterviewSessionId,
    ) -> bool:
        return any(
            job.workspace_id == workspace_id
            and job.kind == INTERVIEW_REPORT_JOB_KIND
            and job.subject.id == session_id
            and not job.is_terminal
            for job in self.state.jobs.values()
        )


class FakeJobs:
    """@brief 统一 Job store fake / Unified Job-store fake."""

    def __init__(self, state: State) -> None:
        self.state = state

    async def add(self, job: Job, spec: object) -> None:
        self.state.jobs[job.meta.id] = job
        self.state.job_specs[job.meta.id] = spec

    async def get(
        self,
        workspace_id: WorkspaceId,
        job_id: JobId,
        *,
        for_update: bool = False,
    ) -> Job | None:
        del for_update
        job = self.state.jobs.get(job_id)
        return job if job is not None and job.workspace_id == workspace_id else None

    async def get_owned(
        self,
        workspace_id: WorkspaceId,
        actor_id: UserId,
        job_id: JobId,
        *,
        for_update: bool = False,
    ) -> Job | None:
        """@brief 按测试 creator 精确读取 Job / Read a Job by the test creator exactly."""
        del for_update
        if actor_id != PRINCIPAL.user_id:
            return None
        return await self.get(workspace_id, job_id)

    async def save(self, job: Job, *, expected_revision: int) -> None:
        current = self.state.jobs.get(job.meta.id)
        if current is None or current.meta.revision != expected_revision:
            raise InterviewCasMismatch
        self.state.jobs[job.meta.id] = job


class FakeArtifacts:
    """@brief 统一 Artifact store fake / Unified Artifact-store fake."""

    def __init__(self, state: State) -> None:
        self.state = state

    async def add(self, artifact: Artifact, content: bytes) -> None:
        assert len(content) == artifact.size_bytes
        self.state.artifacts.append(artifact)


class FakeOutbox:
    """@brief 统一 outbox fake / Unified outbox fake."""

    def __init__(self, state: State) -> None:
        self.state = state

    async def add(self, record: object) -> None:
        self.state.outbox.append(record)


class FakeAudit:
    """@brief 统一 audit fake / Unified audit fake."""

    def __init__(self, state: State) -> None:
        self.state = state

    async def add(self, event: AuditEvent) -> None:
        self.state.audits.append(event)


class FakeUow:
    """@brief 跟踪活动事务数的 fake UoW / Fake UoW tracking active transactions."""

    def __init__(self, state: State) -> None:
        self.state = state
        self.authorizer = FakeAuthorizer(state)
        self.policy = FakePolicy()
        self.repository = FakeRepository(state)
        self.jobs = FakeJobs(state)
        self.artifacts = FakeArtifacts(state)
        self.outbox = FakeOutbox(state)
        self.audit = FakeAudit(state)

    async def __aenter__(self) -> FakeUow:
        self.state.active_transactions += 1
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        del exc_type, exc, traceback
        self.state.active_transactions -= 1
        return None

    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
        return None


class FakeUowFactory:
    """@brief fake UoW 工厂 / Fake UoW factory."""

    def __init__(self, state: State) -> None:
        self.state = state

    def __call__(self) -> FakeUow:
        return FakeUow(self.state)


class FakeRealtimeGateway:
    """@brief 断言无活动事务的 signaling fake / Signaling fake asserting no active transaction."""

    def __init__(self, state: State) -> None:
        self.state = state
        self.revoked: list[RealtimeConnectionId] = []
        self.mutate_after_issue = False

    async def issue(
        self,
        workspace_id: WorkspaceId,
        session: InterviewSession,
        audience: ResourceRef,
        spec: CreateRealtimeConnectionSpec,
        *,
        issued_at: datetime,
    ) -> RealtimeConnection:
        assert self.state.active_transactions == 0
        connection = RealtimeConnection(
            RealtimeConnectionId("connection_0001"),
            workspace_id,
            session.meta.id,
            audience,
            spec.supported_transports[0],
            "wss://realtime.example.com/interview",
            EphemeralToken("secret-ephemeral-token-0001"),
            (),
            issued_at,
            issued_at + timedelta(minutes=5),
            5_000,
        )
        if self.mutate_after_issue:
            current = self.state.sessions[session.meta.id]
            self.state.sessions[session.meta.id] = current.mark_connecting(at=issued_at)
        return connection

    async def revoke(self, connection_id: RealtimeConnectionId) -> None:
        assert self.state.active_transactions == 0
        self.revoked.append(connection_id)


class FakeMediaFinalizer:
    """@brief 断言无活动事务的 media fake / Media fake asserting no active transaction."""

    def __init__(self, state: State, *, fail: bool = False) -> None:
        self.state = state
        self.fail = fail

    async def finalize(
        self,
        session: InterviewSession,
        *,
        operation_id: InterviewWorkerOperationId,
    ) -> EndSessionOutput:
        assert self.state.active_transactions == 0
        assert session.view.status is InterviewSessionStatus.ENDING
        self.state.media_finalize_calls += 1
        self.state.media_operation_ids.append(operation_id)
        if self.fail:
            raise RuntimeError("private provider failure payload")
        return EndSessionOutput(())


class RetryableMediaFinalizer:
    """@brief 返回可重试分类失败的 media fake / Media fake returning a classified retryable failure."""

    def __init__(self, state: State) -> None:
        """@brief 绑定调用计数状态 / Bind call-count state."""
        self.state = state

    async def finalize(
        self,
        session: InterviewSession,
        *,
        operation_id: InterviewWorkerOperationId,
    ) -> EndSessionOutput:
        """@brief 模拟无副作用的瞬态 provider 不可用 / Simulate side-effect-free transient provider unavailability."""
        assert self.state.active_transactions == 0
        assert session.view.status is InterviewSessionStatus.ENDING
        self.state.media_finalize_calls += 1
        self.state.media_operation_ids.append(operation_id)
        raise InterviewWorkerPortFailure(
            "interview.media_provider_temporarily_unavailable",
            retryable=True,
        )


class FakeReportProvider:
    """@brief 仅返回公开安全草稿的 report fake / Report fake returning only a public-safe draft."""

    def __init__(self, state: State, *, invalid: bool = False) -> None:
        self.state = state
        self.invalid = invalid

    async def generate(
        self,
        request: ReportGenerationRequest,
        *,
        operation_id: InterviewWorkerOperationId,
    ) -> InterviewReportDraft:
        assert self.state.active_transactions == 0
        self.state.report_generate_calls += 1
        self.state.report_operation_ids.append(operation_id)
        dimension = request.rubric.dimensions[0]
        evidence: tuple[InterviewEvidence, ...] = ()
        if request.transcript:
            segment = request.transcript[0]
            evidence = (
                InterviewEvidence(
                    segment.id,
                    segment.start_ms,
                    segment.end_ms,
                    segment.text,
                ),
            )
        rubric_version = "wrong-version" if self.invalid else request.rubric.rubric_version
        return InterviewReportDraft(
            report_version="1",
            rubric_id=request.rubric.rubric_id,
            rubric_version=rubric_version,
            engine_version="fake-engine-1",
            overall_score=80,
            overall_confidence=0.8,
            executive_summary=InterviewRichText("总体表现稳健。"),
            rubric_scores=(
                RubricScore(
                    dimension.dimension_id,
                    80,
                    0.8,
                    InterviewRichText("证据充分。"),
                    evidence,
                    ("继续练习",),
                ),
            ),
            strengths=(InterviewRichText("结构清晰。"),),
            improvements=(InterviewRichText("增加定量分析。"),),
            communication_metrics=InterviewCommunicationMetrics(
                2_000,
                2_000,
                120,
                0,
                0,
                0,
                (),
            ),
            action_plan=(),
            limitations=(),
        )


def _scenario_spec() -> InterviewScenarioSpec:
    rubric = InterviewRubric(
        "rubric_0001",
        "1",
        "System design",
        (
            RubricDimension(
                "dimension_0001",
                "Consistency",
                "Explain consistency trade-offs",
                1,
                ("Defines linearizability",),
                ScoreScale(0, 100),
            ),
        ),
        ScoreScale(0, 100),
    )
    return InterviewScenarioSpec(
        "Distributed systems",
        "A systems interview",
        "zh-CN",
        "system_design",
        InterviewDifficulty.ADVANCED,
        45,
        8,
        ("consistency",),
        True,
        True,
        rubric,
    )


def _session_command(scenario_id: InterviewScenarioId) -> CreateInterviewSessionCommand:
    media = InterviewMediaPreferences(
        True,
        False,
        False,
        1920,
        1080,
        30,
        InterviewAvatarPreferences(
            AvatarOutputMode.AUDIO_ONLY,
            None,
            "voice_0001",
            ("opus",),
            (),
            False,
            False,
        ),
        FallbackTransport.WEBSOCKET,
    )
    return CreateInterviewSessionCommand(
        scenario_id,
        None,
        JobTarget(
            "Senior Engineer",
            "HM Alliances",
            None,
            None,
            None,
            "senior",
            ("distributed-systems",),
        ),
        KnowledgeSelection(
            KnowledgeSelectionMode.NONE,
            (),
            (),
            (),
            "interview_agent",
        ),
        "zh-CN",
        media,
        RecordingConsent(False, False, True, 30, NOW, "consent-1"),
        InferenceIntent(
            InferenceQualityTier.BALANCED,
            10_000,
            InferenceCostTier.STANDARD,
            ModelRegion.CN,
            False,
            False,
        ),
    )


def _services(
    state: State,
    *,
    media_fail: bool = False,
    report_invalid: bool = False,
) -> tuple[
    InterviewApplicationService,
    InterviewWorkerService,
    FakeRealtimeGateway,
]:
    factory = FakeUowFactory(state)
    gateway = FakeRealtimeGateway(state)
    ids = DeterministicIds()
    application = InterviewApplicationService(
        factory,
        gateway,
        clock=FixedClock(),
        id_factory=ids,
    )
    worker = InterviewWorkerService(
        factory,
        FakeMediaFinalizer(state, fail=media_fail),
        FakeReportProvider(state, invalid=report_invalid),
        service_actor=ResourceRef("service", "interview_worker_service01"),
        clock=FixedClock(),
        id_factory=ids,
    )
    return application, worker, gateway


@pytest.mark.asyncio
async def test_session_creation_rejects_unconfigured_recording_before_persistence() -> None:
    """@brief 无 media adapter 时不接受最终必失败的录音 Session / Session creation rejects recording that cannot be finalized by this deployment."""

    state = State()
    service, _worker, _gateway = _services(state)
    scenario = await service.create_scenario(
        PRINCIPAL,
        WORKSPACE,
        CreateInterviewScenarioCommand(_scenario_spec()),
        CONTEXT,
    )
    active = await service.update_scenario(
        PRINCIPAL,
        WORKSPACE,
        scenario.meta.id,
        InterviewScenarioPatch({"status": InterviewScenarioStatus.ACTIVE}),
        expected_revision=1,
        context=CONTEXT,
    )
    command = _session_command(active.meta.id)
    recording = replace(
        command.recording,
        record_audio=True,
    )

    with pytest.raises(InvalidInterviewCommand) as raised:
        await service.create_session(
            PRINCIPAL,
            WORKSPACE,
            replace(command, recording=recording),
            CONTEXT,
        )

    assert raised.value.code == "interview.recording_unavailable"
    assert state.sessions == {}


@pytest.mark.asyncio
async def test_all_twelve_routes_and_workers_form_one_strict_lifecycle() -> None:
    state = State()
    service, worker, _gateway = _services(state)
    page = InterviewPageRequest()

    assert (await service.list_scenarios(PRINCIPAL, WORKSPACE, page)).items == ()
    scenario = await service.create_scenario(
        PRINCIPAL,
        WORKSPACE,
        CreateInterviewScenarioCommand(_scenario_spec()),
        CONTEXT,
    )
    assert await service.get_scenario(PRINCIPAL, WORKSPACE, scenario.meta.id) == scenario
    active = await service.update_scenario(
        PRINCIPAL,
        WORKSPACE,
        scenario.meta.id,
        InterviewScenarioPatch({"status": InterviewScenarioStatus.ACTIVE}),
        expected_revision=1,
        context=CONTEXT,
    )
    assert (await service.list_scenarios(PRINCIPAL, WORKSPACE, page)).items == (active,)

    created = await service.create_session(
        PRINCIPAL,
        WORKSPACE,
        _session_command(active.meta.id),
        CONTEXT,
    )
    session_id = created.meta.id
    assert await service.get_session(PRINCIPAL, WORKSPACE, session_id) == created
    assert (await service.list_sessions(PRINCIPAL, WORKSPACE, page)).items == (created,)

    connection = await service.create_realtime_connection(
        PRINCIPAL,
        WORKSPACE,
        session_id,
        CreateRealtimeConnectionSpec((RealtimeTransport.WEBRTC,), ("opus",), ()),
        CONTEXT,
    )
    audience = ResourceRef("user", PRINCIPAL.user_id)
    media_started = RealtimeControlInput(RealtimeControl.MEDIA_STARTED)
    await service.ingest_realtime_input(
        audience,
        RealtimeInputEnvelope(
            RealtimeInputId("input_media_0001"),
            WORKSPACE,
            session_id,
            connection.id,
            NOW,
            media_started,
            realtime_input_fingerprint(media_started),
        ),
    )
    utterance = CandidateUtteranceInput("线性一致性要求操作有单一全局顺序。", 0, 2_000)
    envelope = RealtimeInputEnvelope(
        RealtimeInputId("input_text_0001"),
        WORKSPACE,
        session_id,
        connection.id,
        NOW,
        utterance,
        realtime_input_fingerprint(utterance),
    )
    first_receipt = await service.ingest_realtime_input(audience, envelope)
    replay_receipt = await service.ingest_realtime_input(audience, envelope)
    assert first_receipt == RealtimeInputReceipt(2, False)
    assert replay_receipt == RealtimeInputReceipt(2, True)
    assert len(state.transcript[session_id]) == 1
    assert utterance.text not in repr(state.realtime_inputs)
    transcript = await service.get_transcript(PRINCIPAL, WORKSPACE, session_id, page)
    assert transcript.items == tuple(state.transcript[session_id])

    active_session = state.sessions[session_id]
    end_job = await service.create_end_request(
        PRINCIPAL,
        WORKSPACE,
        session_id,
        EndInterviewSessionCommand(EndInterviewReason.COMPLETED),
        expected_revision=active_session.meta.revision,
        context=CONTEXT,
    )
    assert await service.ingest_realtime_input(audience, envelope) == RealtimeInputReceipt(2, True)
    await worker.execute_queued_job(
        WORKSPACE,
        session_id,
        end_job.meta.id,
        attempt_count=1,
        maximum_attempts=12,
    )
    ended = state.sessions[session_id].view
    assert ended.status is InterviewSessionStatus.COMPLETED
    assert state.jobs[end_job.meta.id].status is JobStatus.SUCCEEDED
    await worker.execute_queued_job(
        WORKSPACE,
        session_id,
        end_job.meta.id,
        attempt_count=2,
        maximum_attempts=12,
    )
    assert state.media_finalize_calls == 1
    assert state.media_operation_ids == [
        InterviewWorkerOperationId(f"interview.end:{end_job.meta.id}")
    ]

    report_job = await service.create_report_job(
        PRINCIPAL,
        WORKSPACE,
        session_id,
        CreateInterviewReportJobCommand("1"),
        CONTEXT,
    )
    await worker.execute_queued_job(
        WORKSPACE,
        session_id,
        report_job.meta.id,
        attempt_count=1,
        maximum_attempts=12,
    )
    report_id = state.sessions[session_id].view.report_id
    assert report_id is not None
    report = state.reports[report_id]
    assert await service.get_report(PRINCIPAL, WORKSPACE, report.meta.id) == report
    assert state.sessions[session_id].view.report_id == report.meta.id
    assert state.jobs[report_job.meta.id].status is JobStatus.SUCCEEDED
    await worker.execute_queued_job(
        WORKSPACE,
        session_id,
        report_job.meta.id,
        attempt_count=2,
        maximum_attempts=12,
    )
    assert state.report_generate_calls == 1
    assert state.report_operation_ids == [
        InterviewWorkerOperationId(f"interview.report:{report_job.meta.id}")
    ]

    assert len(V2_INTERVIEW_ENDPOINT_METHODS) == 12
    assert len(set(V2_INTERVIEW_ENDPOINT_METHODS)) == 12
    assert {request.permission for request in state.permissions} == set(InterviewPermission)
    assert all(request.workspace_id == WORKSPACE for request in state.permissions)
    assert state.active_transactions == 0


@pytest.mark.asyncio
async def test_workers_resume_running_jobs_and_replay_succeeded_deliveries() -> None:
    """@brief Worker 在首段提交后崩溃可恢复且成功消息可重放 / Workers resume after a post-first-commit crash and replay successful deliveries."""
    state = State()
    service, worker, _gateway = _services(state)
    scenario = await service.create_scenario(
        PRINCIPAL,
        WORKSPACE,
        CreateInterviewScenarioCommand(_scenario_spec()),
        CONTEXT,
    )
    active = await service.update_scenario(
        PRINCIPAL,
        WORKSPACE,
        scenario.meta.id,
        InterviewScenarioPatch({"status": InterviewScenarioStatus.ACTIVE}),
        expected_revision=1,
        context=CONTEXT,
    )
    created = await service.create_session(
        PRINCIPAL,
        WORKSPACE,
        _session_command(active.meta.id),
        CONTEXT,
    )
    connection = await service.create_realtime_connection(
        PRINCIPAL,
        WORKSPACE,
        created.meta.id,
        CreateRealtimeConnectionSpec((RealtimeTransport.WEBRTC,), (), ()),
        CONTEXT,
    )
    control = RealtimeControlInput(RealtimeControl.MEDIA_STARTED)
    await service.ingest_realtime_input(
        ResourceRef("user", PRINCIPAL.user_id),
        RealtimeInputEnvelope(
            RealtimeInputId("input_resume_0001"),
            WORKSPACE,
            created.meta.id,
            connection.id,
            NOW,
            control,
            realtime_input_fingerprint(control),
        ),
    )
    active_session = state.sessions[created.meta.id]
    end_job = await service.create_end_request(
        PRINCIPAL,
        WORKSPACE,
        created.meta.id,
        EndInterviewSessionCommand(EndInterviewReason.COMPLETED),
        expected_revision=active_session.meta.revision,
        context=CONTEXT,
    )

    # 模拟 worker 在 queued→running 提交后、provider 调用前崩溃。
    # Simulate a crash after queued-to-running commit and before provider I/O.
    state.jobs[end_job.meta.id] = end_job.start(at=NOW)
    first_end = await worker.execute_end_job(WORKSPACE, created.meta.id, end_job.meta.id)
    replayed_end = await worker.execute_end_job(WORKSPACE, created.meta.id, end_job.meta.id)
    assert first_end == replayed_end
    assert state.media_finalize_calls == 1

    report_job = await service.create_report_job(
        PRINCIPAL,
        WORKSPACE,
        created.meta.id,
        CreateInterviewReportJobCommand("1"),
        CONTEXT,
    )
    state.jobs[report_job.meta.id] = report_job.start(at=NOW)
    first_report = await worker.execute_report_job(
        WORKSPACE,
        created.meta.id,
        report_job.meta.id,
    )
    replayed_report = await worker.execute_report_job(
        WORKSPACE,
        created.meta.id,
        report_job.meta.id,
    )
    assert first_report == replayed_report
    assert state.report_generate_calls == 1
    assert state.active_transactions == 0


@pytest.mark.asyncio
async def test_in_memory_uow_copies_immutable_domain_values_and_rolls_back() -> None:
    """@brief 内存 UoW 可复制 MappingProxy 领域值且未提交变更回滚 / The in-memory UoW copies immutable mapping-backed values and rolls back uncommitted changes."""
    factory = InMemoryInterviewUnitOfWorkFactory(
        InMemoryAccessStore(),
        StaticInterviewSessionPolicy(
            model_ref=ResourceRef("model", "model_memory_0001", 1),
            allowed_regions=frozenset({ModelRegion.CN}),
            allow_external_model_processing=False,
        ),
    )
    scenario = InterviewScenario(
        ResourceMeta(InterviewScenarioId("scenario_memory01"), 1, NOW, NOW),
        WORKSPACE,
        _scenario_spec(),
    )
    async with factory() as first:
        await first.repository.add_scenario(scenario)
        await first.commit()

    async with factory() as rolled_back:
        current = await rolled_back.repository.get_scenario(
            WORKSPACE,
            scenario.meta.id,
        )
        assert current == scenario
        changed = current.update(
            InterviewScenarioPatch({"name": "Uncommitted name"}),
            at=NOW,
        )
        await rolled_back.repository.save_scenario(changed, expected_revision=1)

    async with factory() as verification:
        persisted = await verification.repository.get_scenario(
            WORKSPACE,
            scenario.meta.id,
        )
        assert persisted == scenario
        await verification.commit()


@pytest.mark.asyncio
async def test_stale_scenario_revision_maps_to_precondition_failed() -> None:
    state = State()
    service, _worker, _gateway = _services(state)
    scenario = await service.create_scenario(
        PRINCIPAL,
        WORKSPACE,
        CreateInterviewScenarioCommand(_scenario_spec()),
        CONTEXT,
    )

    with pytest.raises(InterviewPreconditionFailed):
        await service.update_scenario(
            PRINCIPAL,
            WORKSPACE,
            scenario.meta.id,
            InterviewScenarioPatch({"name": "stale"}),
            expected_revision=999,
            context=CONTEXT,
        )


@pytest.mark.asyncio
async def test_repository_cross_workspace_rows_are_rejected() -> None:
    state = State()
    service, _worker, _gateway = _services(state)
    scenario = await service.create_scenario(
        PRINCIPAL,
        WORKSPACE,
        CreateInterviewScenarioCommand(_scenario_spec()),
        CONTEXT,
    )
    state.malicious_scenario = replace(scenario, workspace_id=OTHER_WORKSPACE)

    with pytest.raises(InterviewPortProtocolError):
        await service.list_scenarios(PRINCIPAL, WORKSPACE, InterviewPageRequest())


@pytest.mark.asyncio
async def test_connection_grant_is_revoked_when_second_phase_detects_a_race() -> None:
    state = State()
    service, _worker, gateway = _services(state)
    scenario = await service.create_scenario(
        PRINCIPAL,
        WORKSPACE,
        CreateInterviewScenarioCommand(_scenario_spec()),
        CONTEXT,
    )
    active = await service.update_scenario(
        PRINCIPAL,
        WORKSPACE,
        scenario.meta.id,
        InterviewScenarioPatch({"status": InterviewScenarioStatus.ACTIVE}),
        expected_revision=1,
        context=CONTEXT,
    )
    session = await service.create_session(
        PRINCIPAL,
        WORKSPACE,
        _session_command(active.meta.id),
        CONTEXT,
    )
    gateway.mutate_after_issue = True

    with pytest.raises(InterviewConflict, match="changed"):
        await service.create_realtime_connection(
            PRINCIPAL,
            WORKSPACE,
            session.meta.id,
            CreateRealtimeConnectionSpec((RealtimeTransport.WEBRTC,), (), ()),
            CONTEXT,
        )
    assert gateway.revoked == [RealtimeConnectionId("connection_0001")]


@pytest.mark.asyncio
async def test_realtime_input_id_cannot_be_reused_with_different_content() -> None:
    state = State()
    service, _worker, _gateway = _services(state)
    scenario = await service.create_scenario(
        PRINCIPAL,
        WORKSPACE,
        CreateInterviewScenarioCommand(_scenario_spec()),
        CONTEXT,
    )
    active = await service.update_scenario(
        PRINCIPAL,
        WORKSPACE,
        scenario.meta.id,
        InterviewScenarioPatch({"status": InterviewScenarioStatus.ACTIVE}),
        expected_revision=1,
        context=CONTEXT,
    )
    session = await service.create_session(
        PRINCIPAL,
        WORKSPACE,
        _session_command(active.meta.id),
        CONTEXT,
    )
    connection = await service.create_realtime_connection(
        PRINCIPAL,
        WORKSPACE,
        session.meta.id,
        CreateRealtimeConnectionSpec((RealtimeTransport.WEBRTC,), (), ()),
        CONTEXT,
    )
    audience = ResourceRef("user", PRINCIPAL.user_id)
    first = RealtimeControlInput(RealtimeControl.MEDIA_STARTED)
    second = RealtimeControlInput(RealtimeControl.HEARTBEAT)
    input_id = RealtimeInputId("input_same_0001")
    await service.ingest_realtime_input(
        audience,
        RealtimeInputEnvelope(
            input_id,
            WORKSPACE,
            session.meta.id,
            connection.id,
            NOW,
            first,
            realtime_input_fingerprint(first),
        ),
    )
    with pytest.raises(RealtimeInputKeyReused):
        await service.ingest_realtime_input(
            audience,
            RealtimeInputEnvelope(
                input_id,
                WORKSPACE,
                session.meta.id,
                connection.id,
                NOW,
                second,
                realtime_input_fingerprint(second),
            ),
        )


@pytest.mark.asyncio
async def test_media_provider_failure_durably_fails_session_and_job_without_leakage() -> None:
    state = State()
    service, worker, _gateway = _services(state, media_fail=True)
    scenario = await service.create_scenario(
        PRINCIPAL,
        WORKSPACE,
        CreateInterviewScenarioCommand(_scenario_spec()),
        CONTEXT,
    )
    active = await service.update_scenario(
        PRINCIPAL,
        WORKSPACE,
        scenario.meta.id,
        InterviewScenarioPatch({"status": InterviewScenarioStatus.ACTIVE}),
        expected_revision=1,
        context=CONTEXT,
    )
    session = await service.create_session(
        PRINCIPAL,
        WORKSPACE,
        _session_command(active.meta.id),
        CONTEXT,
    )
    connection = await service.create_realtime_connection(
        PRINCIPAL,
        WORKSPACE,
        session.meta.id,
        CreateRealtimeConnectionSpec((RealtimeTransport.WEBRTC,), (), ()),
        CONTEXT,
    )
    control = RealtimeControlInput(RealtimeControl.MEDIA_STARTED)
    await service.ingest_realtime_input(
        ResourceRef("user", PRINCIPAL.user_id),
        RealtimeInputEnvelope(
            RealtimeInputId("input_start_0001"),
            WORKSPACE,
            session.meta.id,
            connection.id,
            NOW,
            control,
            realtime_input_fingerprint(control),
        ),
    )
    current = state.sessions[session.meta.id]
    job = await service.create_end_request(
        PRINCIPAL,
        WORKSPACE,
        session.meta.id,
        EndInterviewSessionCommand(EndInterviewReason.COMPLETED),
        expected_revision=current.meta.revision,
        context=CONTEXT,
    )

    with pytest.raises(InterviewWorkerError) as captured:
        await worker.execute_end_job(WORKSPACE, session.meta.id, job.meta.id)
    assert "private provider" not in captured.value.detail
    assert state.sessions[session.meta.id].view.status is InterviewSessionStatus.FAILED
    assert state.jobs[job.meta.id].status is JobStatus.FAILED
    assert state.jobs[job.meta.id].problem is not None
    assert "private provider" not in repr(state.jobs[job.meta.id].problem)


@pytest.mark.asyncio
async def test_retryable_worker_failure_keeps_running_then_fails_at_attempt_cap() -> None:
    """@brief 瞬态失败可恢复，最后一次则原子终结领域状态 / A transient failure is recoverable, then atomically terminal at the cap."""
    state = State()
    factory = FakeUowFactory(state)
    gateway = FakeRealtimeGateway(state)
    ids = DeterministicIds()
    service = InterviewApplicationService(
        factory,
        gateway,
        clock=FixedClock(),
        id_factory=ids,
    )
    worker = InterviewWorkerService(
        factory,
        RetryableMediaFinalizer(state),
        FakeReportProvider(state),
        service_actor=ResourceRef("service", "interview_worker_service01"),
        clock=FixedClock(),
        id_factory=ids,
    )
    scenario = await service.create_scenario(
        PRINCIPAL,
        WORKSPACE,
        CreateInterviewScenarioCommand(_scenario_spec()),
        CONTEXT,
    )
    active = await service.update_scenario(
        PRINCIPAL,
        WORKSPACE,
        scenario.meta.id,
        InterviewScenarioPatch({"status": InterviewScenarioStatus.ACTIVE}),
        expected_revision=1,
        context=CONTEXT,
    )
    session = await service.create_session(
        PRINCIPAL,
        WORKSPACE,
        _session_command(active.meta.id),
        CONTEXT,
    )
    connection = await service.create_realtime_connection(
        PRINCIPAL,
        WORKSPACE,
        session.meta.id,
        CreateRealtimeConnectionSpec((RealtimeTransport.WEBRTC,), (), ()),
        CONTEXT,
    )
    control = RealtimeControlInput(RealtimeControl.MEDIA_STARTED)
    await service.ingest_realtime_input(
        ResourceRef("user", PRINCIPAL.user_id),
        RealtimeInputEnvelope(
            RealtimeInputId("input_retry_0001"),
            WORKSPACE,
            session.meta.id,
            connection.id,
            NOW,
            control,
            realtime_input_fingerprint(control),
        ),
    )
    current = state.sessions[session.meta.id]
    job = await service.create_end_request(
        PRINCIPAL,
        WORKSPACE,
        session.meta.id,
        EndInterviewSessionCommand(EndInterviewReason.COMPLETED),
        expected_revision=current.meta.revision,
        context=CONTEXT,
    )

    with pytest.raises(InterviewWorkerRetry) as retry:
        await worker.execute_queued_job(
            WORKSPACE,
            session.meta.id,
            job.meta.id,
            attempt_count=1,
            maximum_attempts=2,
        )
    assert retry.value.code == "interview.media_provider_temporarily_unavailable"
    assert state.sessions[session.meta.id].view.status is InterviewSessionStatus.ENDING
    assert state.jobs[job.meta.id].status is JobStatus.RUNNING

    await worker.execute_queued_job(
        WORKSPACE,
        session.meta.id,
        job.meta.id,
        attempt_count=2,
        maximum_attempts=2,
    )
    failed_job = state.jobs[job.meta.id]
    assert state.sessions[session.meta.id].view.status is InterviewSessionStatus.FAILED
    assert failed_job.status is JobStatus.FAILED
    assert failed_job.problem is not None
    assert failed_job.problem.code == "interview.worker_attempts_exhausted"
    assert not failed_job.problem.retryable
    assert state.media_finalize_calls == 2
    assert state.media_operation_ids == [
        InterviewWorkerOperationId(f"interview.end:{job.meta.id}"),
        InterviewWorkerOperationId(f"interview.end:{job.meta.id}"),
    ]


@pytest.mark.asyncio
async def test_header_only_exhaustion_closes_queued_end_job_and_session_idempotently() -> None:
    """@brief payload 前失败仍以 creator+Job 持久绑定原子闭合 / A pre-payload failure still closes creator-and-Job-bound state atomically."""
    state = State()
    service, worker, _ = _services(state)
    scenario = await service.create_scenario(
        PRINCIPAL,
        WORKSPACE,
        CreateInterviewScenarioCommand(_scenario_spec()),
        CONTEXT,
    )
    active = await service.update_scenario(
        PRINCIPAL,
        WORKSPACE,
        scenario.meta.id,
        InterviewScenarioPatch({"status": InterviewScenarioStatus.ACTIVE}),
        expected_revision=1,
        context=CONTEXT,
    )
    session = await service.create_session(
        PRINCIPAL,
        WORKSPACE,
        _session_command(active.meta.id),
        CONTEXT,
    )
    job = await service.create_end_request(
        PRINCIPAL,
        WORKSPACE,
        session.meta.id,
        EndInterviewSessionCommand(EndInterviewReason.TECHNICAL_FAILURE),
        expected_revision=session.meta.revision,
        context=CONTEXT,
    )

    await worker.fail_exhausted(WORKSPACE, PRINCIPAL.user_id, job.meta.id)
    await worker.fail_exhausted(WORKSPACE, PRINCIPAL.user_id, job.meta.id)

    failed = state.jobs[job.meta.id]
    assert failed.status is JobStatus.FAILED
    assert failed.problem is not None
    assert failed.problem.code == "interview.worker_attempts_exhausted"
    assert state.sessions[session.meta.id].view.status is InterviewSessionStatus.FAILED
